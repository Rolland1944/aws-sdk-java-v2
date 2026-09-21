#!/usr/bin/env python3
"""P3: D1 adaptive cache on canonical SF1 / SF8.

P3-0 characterization cells are the fixed D1 bins. The online cell is the
soft controller with one coverage/heap-guard setting for every scale.

  python3 tools/track2/run_track1_p3.py \\
      --data s3a://home-haoyue/track2/clickbench_sf1 \\
      --out docs/adaptive-range-reader/results/track2/track1_p3_sf1 \\
      --phase characterize --runs 1
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import run_track1_matrix as m  # noqa: E402
import track1_cost_ledger as cost  # noqa: E402
import track1_dynamic_oracle as oracle  # noqa: E402

MIB = m.MIB
DEFAULT_QUERIES = m.DEFAULT_QUERIES
SF1 = "s3a://home-haoyue/track2/clickbench_sf1"
SF8 = "s3a://home-haoyue/track2/clickbench_sf8"

FIXED = (
    m.Cell("000"),
    m.Cell("100", d1=True),
    m.Cell("100-4m1g", d1=True, d1_admit_bytes=4 * MIB, d1_cache_mib=1024),
    m.Cell("100-8m2g", d1=True, d1_admit_bytes=8 * MIB, d1_cache_mib=2048),
)
# Same 256 KiB admit as `100`; only the soft/hard budget moves. Isolates
# cache capacity from the scan-pollution that 4 MiB / 8 MiB admission adds.
HOLD_ADMIT = (
    m.Cell("100-1g", d1=True, d1_admit_bytes=256 * 1024, d1_cache_mib=1024),
    m.Cell("100-2g", d1=True, d1_admit_bytes=256 * 1024, d1_cache_mib=2048),
)
KIB = 1024
# Hold budget at 1 GiB and only move the filter. 256 KiB is the request-count
# mode, not a proven admit optimum: SF1 `000` reuse is *higher* in
# 256KiB–2MiB than below 256KiB. 4MiB/8MiB were never an admit-only test.
HOLD_BUDGET = (
    m.Cell("64k-1g", d1=True, d1_admit_bytes=64 * KIB, d1_cache_mib=1024),
    m.Cell("128k-1g", d1=True, d1_admit_bytes=128 * KIB, d1_cache_mib=1024),
    m.Cell("256k-1g", d1=True, d1_admit_bytes=256 * KIB, d1_cache_mib=1024),
    m.Cell("512k-1g", d1=True, d1_admit_bytes=512 * KIB, d1_cache_mib=1024),
    m.Cell("1m-1g", d1=True, d1_admit_bytes=MIB, d1_cache_mib=1024),
    m.Cell("2m-1g", d1=True, d1_admit_bytes=2 * MIB, d1_cache_mib=1024),
)
ONLINE = m.Cell(
    "online",
    d1=True,
    d1_adaptive=True,
    d1_hard_mib=4096,
    d1_coverage=1.0,
    d1_admit_bytes=256 * 1024,
    d1_cache_mib=256,
)

PHASES = {
    "characterize": FIXED,
    "budget-only": (
        m.Cell("000"),
        m.Cell("100", d1=True),
    ) + HOLD_ADMIT,
    "online": (
        m.Cell("000"),
        m.Cell("100", d1=True),
        ONLINE,
    ),
    "fixed-vs-online": (
        m.Cell("000"),
        m.Cell("100", d1=True),
        ONLINE,
    ),
    "p3-2-sf8": (
        m.Cell("000"),
        m.Cell("100", d1=True),
        HOLD_ADMIT[0],
        ONLINE,
    ),
    "admit-only": (
        m.Cell("000"),
        m.Cell("100", d1=True),
    ) + HOLD_BUDGET,
}
CATALOG = {c.id: c for c in FIXED + HOLD_ADMIT + HOLD_BUDGET + (ONLINE,)}


def cells_for(phase):
    if phase not in PHASES:
        raise SystemExit(f"unknown phase {phase}; known {list(PHASES)}")
    return PHASES[phase]


def write_report(args, cells, slots, summaries):
    fixed_oracle = oracle.from_cell_summaries(summaries)
    report = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "experiment": "Track1 P3 D1 adaptive cache",
        "phase": args.phase,
        "data": args.data,
        "queries_file": args.queries_file,
        "master": args.master,
        "driver_memory": args.driver_memory,
        "runs": args.runs,
        "cells": [c._asdict() for c in cells],
        "schedule": [{"cell": s["cell"], "run": s["run"]} for s in slots],
        "results": summaries,
        "fixed_oracle": fixed_oracle,
        "joint_oracle": m.build_oracle(summaries, args.runs),
        "cost_ledger": cost.from_cell_summaries(
            summaries, driver_memory=args.driver_memory),
    }
    dest = os.path.join(args.out, "p3_report.json")
    os.makedirs(args.out, exist_ok=True)
    with open(dest, "w") as fh:
        json.dump(report, fh, indent=2)
    return report, dest


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=SF1)
    ap.add_argument("--out", required=True)
    ap.add_argument("--queries-file", default=DEFAULT_QUERIES)
    ap.add_argument("--tables", nargs="*", default=["hits"])
    ap.add_argument("--queries", default=None)
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--phase", default="characterize",
                    choices=sorted(PHASES))
    ap.add_argument("--cells", default=None,
                    help="override phase cell list, comma-separated")
    ap.add_argument("--master", default="local[16]")
    ap.add_argument("--driver-memory", default="32g")
    ap.add_argument("--resume", action="store_true", default=True)
    ap.add_argument("--no-resume", dest="resume", action="store_false")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.cells:
        wanted = [x.strip() for x in args.cells.split(",") if x.strip()]
        by_id = CATALOG
        missing = [c for c in wanted if c not in by_id]
        if missing:
            raise SystemExit(f"unknown cells {missing}")
        cells = [by_id[c] for c in wanted]
    else:
        cells = list(cells_for(args.phase))

    slots = m.schedule(cells, args.runs)
    os.makedirs(args.out, exist_ok=True)
    print(f"# Track1 P3  phase={args.phase}  cells={[c.id for c in cells]}  "
          f"runs={args.runs}  slots={len(slots)}  data={args.data}", flush=True)

    for i, slot in enumerate(slots, 1):
        dest = m.cell_dir(args.out, slot["cell"], slot["run"])
        report_path = m.cell_report_path(args.out, slot["cell"], slot["run"])
        label = f"{slot['cell']} run {slot['run']}"
        if args.resume and m.run_ok(report_path):
            print(f"\n=== {i}/{len(slots)} {label} (resume) ===", flush=True)
            continue
        argv = m.bench_argv(args, slot["spec"], dest)
        print(f"\n=== {i}/{len(slots)} {label} ===", flush=True)
        print("  " + " ".join(argv), flush=True)
        if args.dry_run:
            continue
        rc = subprocess.call(argv)
        if rc not in (0, 1):
            return rc
        if not m.run_ok(report_path):
            print(f"  FAIL {report_path} missing or has query errors", flush=True)
            return 2

    if args.dry_run:
        write_report(args, cells, slots, [])
        return 0

    summaries = []
    for spec in cells:
        reports = []
        for rnd in range(1, args.runs + 1):
            path = m.cell_report_path(args.out, spec.id, rnd)
            rec = m.load(path)
            rec["_path"] = path
            reports.append(rec)
        summaries.append(m.summarize_cell(spec.id, spec, reports))

    report, dest = write_report(args, cells, slots, summaries)
    print("\n# P3 cells", flush=True)
    for cell in summaries:
        flag = "STABLE" if cell["stable"] else "screen"
        print(f"  {cell['id']}  median={cell['median_s']:.2f}s  "
              f"gets={cell['gets']}  u_h={cell.get('d1_u_h')}  "
              f"r_h={cell.get('d1_r_h')}  {flag}", flush=True)
    fo = report["fixed_oracle"]
    print(f"  fixed_oracle   {fo.get('winner')}  "
          f"optimistic_gap_vs_winner={fo.get('optimistic_gap_vs_winner')}",
          flush=True)
    print(f"  report         {dest}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
