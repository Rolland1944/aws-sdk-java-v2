#!/usr/bin/env python3
"""P2: D1+D2+D4 fixed-combo matrix on the Track2 candidate.

Joint oracle = the pre-registered cell with the lowest wall-clock *median*
across >=5 independent Spark processes. GET/bytes/cache/merge explain the
result; they do not rank it.

Each (cell, repeat) is its own ``run_benchmark.py`` process so D1 starts cold
and factory singletons do not leak across cells. Repeats are interleaved and
the cell order rotates each round so one config cannot own a quiet or noisy
window.

Default cells (d1,d2,d4):

  000  passthrough factory
  100  D1 only
  010  D2 only, wait 0 (queue/switch cost)
  011  D2+D4, wait 200us (measured batching knee)
  110  D1+D2, wait 0
  111  D1+D2+D4, wait 200us

D1 admission/budget sweep cells (D1 only, everything else off):

  100        cap 256 KiB, budget 256 MiB (only 2.6% of bytes are cacheable)
  100-4m1g   cap 4 MiB,   budget 1 GiB
  100-8m2g   cap 8 MiB,   budget 2 GiB

The cap and the budget are one knob, not two: a trace replay of the measured
GET sequence shows cap 16 MiB on a 2 GiB budget is *worse* than cap 8 MiB
because the large scans evict the small reads that supply most of the hits.

Usage:
  python3 tools/track2/run_track1_matrix.py \\
      --data s3a://home-haoyue/track2/clickbench_sf1_e0_uc1_joint2 \\
      --out docs/adaptive-range-reader/results/track2/track1_p2_matrix
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from typing import NamedTuple

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_QUERIES = os.path.join(HERE, "clickbench_queries.json")
DEFAULT_DATA = "s3a://home-haoyue/track2/clickbench_sf1_e0_uc1_joint2"

MIB = 1024 * 1024


class Cell(NamedTuple):
    id: str
    d1: bool = False
    d2: bool = False
    d4: bool = False
    wait_us: int = 0
    d1_admit_bytes: int = 256 * 1024
    d1_cache_mib: int = 256
    d1_adaptive: bool = False
    d1_hard_mib: int = 0
    d1_coverage: float = 1.0

    def block_bytes(self):
        """Keep an admitted range in one cache block so a repeat read of the
        same range hits on the exact-key fast path instead of stitching."""
        return max(MIB, self.d1_admit_bytes)


CELLS = (
    Cell("000"),
    Cell("100", d1=True),
    Cell("010", d2=True),
    Cell("011", d2=True, d4=True, wait_us=200),
    Cell("110", d1=True, d2=True),
    Cell("111", d1=True, d2=True, d4=True, wait_us=200),
    # D1 admission/budget sweep: the cap and the budget have to move together.
    Cell("100-4m1g", d1=True, d1_admit_bytes=4 * MIB, d1_cache_mib=1024),
    Cell("100-8m2g", d1=True, d1_admit_bytes=8 * MIB, d1_cache_mib=2048),
)


def schedule(cells, runs):
    """Round-robin repeats, rotate cell order each round."""
    slots = []
    ids = [c[0] if isinstance(c, tuple) else c["id"] for c in cells]
    by_id = {c[0]: c for c in cells}
    for rnd in range(1, runs + 1):
        rot = ids[rnd - 1:] + ids[:rnd - 1]
        for cid in rot:
            slots.append({"cell": cid, "run": rnd, "spec": by_id[cid]})
    return slots


def cell_dir(out, cell_id, run_id):
    return os.path.join(out, "cells", cell_id, f"run-{run_id}")


def cell_report_path(out, cell_id, run_id):
    return os.path.join(cell_dir(out, cell_id, run_id), "report.json")


def load(path):
    with open(path) as fh:
        return json.load(fh)


def run_ok(path):
    if not os.path.isfile(path):
        return False
    try:
        report = load(path)
    except (OSError, json.JSONDecodeError):
        return False
    runs = report.get("runs") or []
    if not runs:
        return False
    return not any(q.get("error") for q in runs[0].get("queries") or [])


def bench_argv(args, spec, dest):
    argv = [
        sys.executable, os.path.join(HERE, "run_benchmark.py"),
        "--data", args.data,
        "--out", dest,
        "--queries-file", args.queries_file,
        "--tables", *args.tables,
        "--runs", "1",
        "--layout-id", f"track1-{spec.id}",
        "--master", args.master,
        "--driver-memory", args.driver_memory,
        "--track1-s3a",
    ]
    if spec.d1:
        argv.append("--track1-d1")
    if spec.d2:
        argv.append("--track1-d2")
    if spec.d4:
        argv.append("--track1-d4")
    argv.extend(["--track1-d2-wait-us", str(spec.wait_us)])
    argv.extend(["--track1-d1-admit-bytes", str(spec.d1_admit_bytes)])
    argv.extend(["--track1-d1-cache-mib", str(spec.d1_cache_mib)])
    argv.extend(["--track1-d1-block-bytes", str(spec.block_bytes())])
    if spec.d1_adaptive:
        argv.append("--track1-d1-adaptive")
        if spec.d1_hard_mib:
            argv.extend(["--track1-d1-hard-mib", str(spec.d1_hard_mib)])
        argv.extend(["--track1-d1-coverage", str(spec.d1_coverage)])
    if args.queries:
        argv.extend(["--queries", args.queries])
    return argv


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def summarize_cell(cell_id, spec, reports):
    totals = [r["runs"][0]["query_sum_s"] for r in reports]
    mean = statistics.mean(totals)
    stdev = statistics.stdev(totals) if len(totals) > 1 else 0.0
    cv = (stdev / mean) if mean else float("inf")
    by_q = {}
    for report in reports:
        for q in report["runs"][0]["queries"]:
            by_q.setdefault(q["query"], []).append(q)
    per_query = []
    for qnr, samples in sorted(by_q.items()):
        walls = [s["wall_s"] for s in samples]
        errors = [s["error"] for s in samples if s["error"]]
        per_query.append({
            "query": qnr,
            "n": len(walls),
            "median_s": statistics.median(walls),
            "mean_s": statistics.mean(walls),
            "p50_s": percentile(walls, 0.50),
            "p95_s": percentile(walls, 0.95),
            "p99_s": percentile(walls, 0.99),
            "min_s": min(walls),
            "max_s": max(walls),
            "errors": errors,
        })
    ios = [r["runs"][0].get("io") or {} for r in reports]
    t1s = [r["runs"][0].get("track1") or {} for r in reports]

    def median_field(rows, key):
        vals = [row.get(key) for row in rows if row.get(key) is not None]
        return statistics.median(vals) if vals else None

    return {
        "id": cell_id,
        "d1": spec.d1,
        "d2": spec.d2,
        "d4": spec.d4,
        "d2_wait_us": spec.wait_us,
        "d1_admit_bytes": spec.d1_admit_bytes,
        "d1_cache_mib": spec.d1_cache_mib,
        "d1_adaptive": spec.d1_adaptive,
        "d1_target_budget": median_field(t1s, "d1_target_budget"),
        "d1_u_h": median_field(t1s, "d1_u_h"),
        "d1_r_h": median_field(t1s, "d1_r_h"),
        "d1_r_admit": median_field(t1s, "d1_r_admit"),
        "d1_pollution": median_field(t1s, "d1_pollution"),
        "d1_rd_p90": median_field(t1s, "d1_rd_p90"),
        "d1_heap_ratio": median_field(t1s, "d1_heap_ratio"),
        "d1_observations": median_field(t1s, "d1_observations"),
        "d1_shrinks": median_field(t1s, "d1_shrinks"),
        "d1_bypasses": median_field(t1s, "d1_bypasses"),
        "d1_mode": next((row.get("d1_mode") for row in reversed(t1s)
                         if row.get("d1_mode")), None),
        "n_runs": len(totals),
        "end_to_end_s": totals,
        "median_s": statistics.median(totals),
        "mean_s": mean,
        "stdev_s": stdev,
        "cv": cv,
        "p50_s": percentile(totals, 0.50),
        "p95_s": percentile(totals, 0.95),
        "p99_s": percentile(totals, 0.99),
        "stable": cv < 0.05 and len(totals) >= 5 and not any(q["errors"] for q in per_query),
        "gets": median_field(ios, "gets"),
        "ranged_gets": median_field(ios, "ranged_gets"),
        "remote_bytes": median_field(ios, "remote_bytes"),
        "cache_hits": median_field(t1s, "cache_hits"),
        "cache_useful_bytes": median_field(t1s, "cache_useful_bytes"),
        "cached_bytes": median_field(t1s, "cached_bytes"),
        "peak_cached_bytes": median_field(t1s, "peak_cached_bytes"),
        "evicted_bytes": median_field(t1s, "evicted_bytes"),
        "evicted_blocks": median_field(t1s, "evicted_blocks"),
        "admit_rejected": median_field(t1s, "admit_rejected"),
        "teed_gets": median_field(t1s, "teed_gets"),
        "teed_bytes": median_field(t1s, "teed_bytes"),
        "gc_ms": median_field(t1s, "gc_ms"),
        "gc_count": median_field(t1s, "gc_count"),
        "d1_admit_max_bytes": median_field(t1s, "d1_admit_max_bytes"),
        "merged_gets": median_field(t1s, "merged_gets"),
        "wasted_bytes": median_field(t1s, "wasted_bytes"),
        "queue_wait_ns": median_field(t1s, "queue_wait_ns"),
        "queue_submissions": median_field(t1s, "queue_submissions"),
        "queue_batches": median_field(t1s, "queue_batches"),
        "queue_singleton_batches": median_field(t1s, "queue_singleton_batches"),
        "same_object_multi_ticket_groups": median_field(t1s, "same_object_multi_ticket_groups"),
        "mergeable_groups": median_field(t1s, "mergeable_groups"),
        "merge_budget_fallbacks": median_field(t1s, "merge_budget_fallbacks"),
        "max_batch_size": median_field(t1s, "max_batch_size"),
        "max_same_object_group_size": median_field(t1s, "max_same_object_group_size"),
        "peak_heap_bytes": median_field(t1s, "peak_heap_bytes"),
        "g_star_bytes": median_field(t1s, "g_star_bytes"),
        "fallbacks": median_field(t1s, "fallbacks"),
        "per_query": per_query,
        "reports": [r.get("_path") for r in reports],
    }


def query_regressions(cells, baseline_id="000", frac=0.15, floor_s=1.0):
    by_id = {c["id"]: c for c in cells}
    base = by_id.get(baseline_id)
    if not base:
        return []
    base_q = {q["query"]: q["median_s"] for q in base["per_query"]}
    flags = []
    for cell in cells:
        if cell["id"] == baseline_id:
            continue
        for q in cell["per_query"]:
            ref = base_q.get(q["query"])
            if ref is None or ref <= 0:
                continue
            if q["median_s"] > ref * (1.0 + frac) and (q["median_s"] - ref) >= floor_s:
                flags.append({
                    "cell": cell["id"],
                    "query": q["query"],
                    "median_s": q["median_s"],
                    "baseline_s": ref,
                    "rel": q["median_s"] / ref - 1.0,
                })
    return flags


def build_oracle(cell_summaries, runs):
    ranked = sorted(cell_summaries, key=lambda c: (c["median_s"], c["id"]))
    winner = ranked[0] if ranked else None
    regressions = query_regressions(cell_summaries)
    bytes_000 = next((c["remote_bytes"] for c in cell_summaries if c["id"] == "000"), None)
    heap_cap = 32 * 1024 * 1024 * 1024
    bounded = []
    for c in cell_summaries:
        amp = None
        if bytes_000 and c["remote_bytes"]:
            amp = c["remote_bytes"] / bytes_000
        heap_ok = c["peak_heap_bytes"] is None or c["peak_heap_bytes"] < heap_cap
        amp_ok = amp is None or amp <= 1.25
        bounded.append({
            "id": c["id"],
            "bytes_vs_000": amp,
            "bytes_ok": amp_ok,
            "heap_ok": heap_ok,
        })
    eligible = [c for c in ranked if c["stable"]]
    return {
        "rank_metric": "wall_clock_median_s",
        "ranked": [c["id"] for c in ranked],
        "winner": winner["id"] if winner else None,
        "winner_stable": bool(winner and winner["stable"]),
        "stable_winner": eligible[0]["id"] if eligible else None,
        "all_stable": all(c["stable"] for c in cell_summaries) and len(cell_summaries) > 0,
        "n_runs_required": 5,
        "n_runs": runs,
        "query_regressions": regressions,
        "bounds": bounded,
    }


def write_report(args, cells, slots, summaries):
    oracle = build_oracle(summaries, args.runs)
    report = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "experiment": "Track1 P2 D1+D2+D4 joint oracle",
        "data": args.data,
        "queries_file": args.queries_file,
        "master": args.master,
        "driver_memory": args.driver_memory,
        "runs": args.runs,
        "cells": [c._asdict() for c in cells],
        "schedule": [{"cell": s["cell"], "run": s["run"]} for s in slots],
        "results": summaries,
        "oracle": oracle,
    }
    dest = os.path.join(args.out, "p2_report.json")
    os.makedirs(args.out, exist_ok=True)
    with open(dest, "w") as fh:
        json.dump(report, fh, indent=2)
    return report, dest


def parse_cells(raw):
    wanted = [x.strip() for x in raw.split(",") if x.strip()]
    by_id = {c[0]: c for c in CELLS}
    missing = [c for c in wanted if c not in by_id]
    if missing:
        raise SystemExit(f"unknown cells {missing}; known {list(by_id)}")
    return [by_id[c] for c in wanted]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=DEFAULT_DATA)
    ap.add_argument("--out", required=True)
    ap.add_argument("--queries-file", default=DEFAULT_QUERIES)
    ap.add_argument("--tables", nargs="*", default=["hits"])
    ap.add_argument("--queries", default=None,
                    help="comma-separated query numbers; default all 43")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--cells", default="000,100,010,011,110,111")
    ap.add_argument("--master", default="local[16]")
    ap.add_argument("--driver-memory", default="32g")
    ap.add_argument("--resume", action="store_true", default=True)
    ap.add_argument("--no-resume", dest="resume", action="store_false")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cells = parse_cells(args.cells)
    slots = schedule(cells, args.runs)
    os.makedirs(args.out, exist_ok=True)
    print(f"# Track1 P2 matrix  cells={[c[0] for c in cells]}  "
          f"runs={args.runs}  slots={len(slots)}  data={args.data}", flush=True)

    for i, slot in enumerate(slots, 1):
        dest = cell_dir(args.out, slot["cell"], slot["run"])
        report_path = cell_report_path(args.out, slot["cell"], slot["run"])
        label = f"{slot['cell']} run {slot['run']}"
        if args.resume and run_ok(report_path):
            print(f"\n=== {i}/{len(slots)} {label} (resume) ===", flush=True)
            continue
        argv = bench_argv(args, slot["spec"], dest)
        print(f"\n=== {i}/{len(slots)} {label} ===", flush=True)
        print("  " + " ".join(argv), flush=True)
        if args.dry_run:
            continue
        rc = subprocess.call(argv)
        if rc not in (0, 1):
            return rc
        if not run_ok(report_path):
            print(f"  FAIL {report_path} missing or has query errors", flush=True)
            return 2

    if args.dry_run:
        write_report(args, cells, slots, [])
        return 0

    summaries = []
    for spec in cells:
        cid = spec[0]
        reports = []
        for rnd in range(1, args.runs + 1):
            path = cell_report_path(args.out, cid, rnd)
            rec = load(path)
            rec["_path"] = path
            reports.append(rec)
        summaries.append(summarize_cell(cid, spec, reports))

    report, dest = write_report(args, cells, slots, summaries)
    oracle = report["oracle"]
    print("\n# joint oracle", flush=True)
    for cell in summaries:
        flag = "STABLE" if cell["stable"] else "unstable"
        print(f"  {cell['id']}  median={cell['median_s']:.2f}s  "
              f"cv={cell['cv']*100:.2f}%  gets={cell['gets']}  {flag}",
              flush=True)
    print(f"  winner         {oracle['winner']}  "
          f"(stable={oracle['winner_stable']})", flush=True)
    print(f"  stable_winner  {oracle['stable_winner']}", flush=True)
    print(f"  regressions    {len(oracle['query_regressions'])}", flush=True)
    print(f"  report         {dest}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
