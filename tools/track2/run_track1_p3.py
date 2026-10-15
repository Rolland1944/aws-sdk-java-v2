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

import phase3_meta as meta  # noqa: E402
import s3a_session  # noqa: E402
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
WTLFU_ONLINE = m.Cell(
    "wtlfu-online",
    d1=True,
    d1_adaptive=True,
    d1_hard_mib=4096,
    d1_coverage=1.0,
    d1_admit_bytes=0,
    d1_cache_mib=256,
)
# Capacity-control ablation: start at the 4 GiB hard limit. Admission and
# W-TinyLFU replacement remain online, but the target has no room to grow.
WTLFU_FIXED_4G = m.Cell(
    "wtlfu-fixed-4g",
    d1=True,
    d1_adaptive=True,
    d1_fixed_capacity=True,
    d1_hard_mib=4096,
    d1_coverage=1.0,
    d1_admit_bytes=0,
    d1_cache_mib=4096,
)
WTLFU_FIXED_8G = m.Cell(
    "wtlfu-fixed-8g",
    d1=True,
    d1_adaptive=True,
    d1_fixed_capacity=True,
    d1_hard_mib=8192,
    d1_coverage=1.0,
    d1_admit_bytes=0,
    d1_cache_mib=8192,
)
WTLFU_FIXED_2G = m.Cell(
    "wtlfu-fixed-2g",
    d1=True,
    d1_adaptive=True,
    d1_fixed_capacity=True,
    d1_hard_mib=2048,
    d1_coverage=1.0,
    d1_admit_bytes=0,
    d1_cache_mib=2048,
)
WTLFU_2M_FIXED_2G = m.Cell(
    "wtlfu-2m-fixed-2g",
    d1=True,
    d1_adaptive=True,
    d1_fixed_capacity=True,
    d1_fixed_admission=True,
    d1_hard_mib=2048,
    d1_coverage=1.0,
    d1_admit_bytes=2 * MIB,
    d1_cache_mib=2048,
)
HOTPATH_2G = (
    m.Cell("hotpath-baseline-2g", d1=True, d1_adaptive=True, d1_fixed_capacity=True,
           d1_hard_mib=2048, d1_cache_mib=2048, d1_admit_bytes=0, d1_profile=True,
           d1_zero_copy=False, d1_doorkeeper=False, d1_shared_backing=False),
    m.Cell("hotpath-deferred-copy-2g", d1=True, d1_adaptive=True, d1_fixed_capacity=True,
           d1_hard_mib=2048, d1_cache_mib=2048, d1_admit_bytes=0, d1_profile=True,
           d1_zero_copy=False, d1_doorkeeper=False, d1_shared_backing=True),
    m.Cell("hotpath-zero-copy-2g", d1=True, d1_adaptive=True, d1_fixed_capacity=True,
           d1_hard_mib=2048, d1_cache_mib=2048, d1_admit_bytes=0, d1_profile=True,
           d1_zero_copy=True, d1_doorkeeper=False),
    m.Cell("hotpath-full-2g", d1=True, d1_adaptive=True, d1_fixed_capacity=True,
           d1_hard_mib=2048, d1_cache_mib=2048, d1_admit_bytes=0, d1_profile=True),
)
HOTPATH_4G = tuple(
    cell._replace(id=cell.id.replace("-2g", "-4g"), d1_hard_mib=4096, d1_cache_mib=4096)
    for cell in HOTPATH_2G
)

# Replacement-policy comparison (Phase 3 §7). Every cell is fixed-capacity,
# admit.bytes=0 (live 8 MiB limit), doorkeeper explicitly off, so only the
# replacement/admission policy differs. `000` (no cache) is added by the phase.
# AV stands for the size-aware class already compared with GDSF, AdaptSize, LHD
# and LRB (Einziger et al., TOS 2022). The other cells are one representative of
# each remaining class: W-TinyLFU (admission, size-oblivious), SIEVE (FIFO),
# SLRU (segmented recency) and LRU. GDSF stays selectable but is not in this matrix.
POLICIES = ("wtinylfu-av", "wtinylfu", "sieve", "slru", "lru")
POLICY_SIZES = {"128m": 128, "256m": 256, "512m": 512, "1g": 1024, "2g": 2048,
                "4g": 4096, "8g": 8192, "16g": 16384}

# Capacity curve (Phase 3 §8.1, RQ2). The policy is fixed to W-TinyLFU-AV (the
# current admission/replacement algorithm); only the capacity moves. SF1 sizes are
# SF8 sizes scaled by 14.1/112.0 so both grids cover the same working-set ratios;
# `16g` on SF1 exceeds the data (approximate upper bound: requests <= 8 MiB only).
CAP_POLICY = "wtinylfu-av"
CAP_SF1_SIZES = ("128m", "256m", "512m", "1g", "2g", "4g", "16g")
CAP_SF8_SIZES = ("1g", "2g", "4g", "8g", "16g")


def policy_cell(policy, size):
    mib = POLICY_SIZES[size]
    return m.Cell(
        f"pol-{policy}-{size}",
        d1=True,
        d1_adaptive=True,
        d1_fixed_capacity=True,
        d1_hard_mib=mib,
        d1_cache_mib=mib,
        d1_admit_bytes=0,
        d1_coverage=1.0,
        d1_doorkeeper=False,
        d1_policy=policy,
    )


POLICY_CELLS = tuple(policy_cell(p, s) for s in POLICY_SIZES for p in POLICIES)

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
    "policy-smoke": (
        m.Cell("000"),
        HOLD_BUDGET[-1],
        HOLD_ADMIT[0],
        WTLFU_ONLINE,
    ),
    "hotpath": HOTPATH_2G,
    "policy-512m": (m.Cell("000"),) + tuple(policy_cell(p, "512m") for p in POLICIES),
    "policy-4g": (m.Cell("000"),) + tuple(policy_cell(p, "4g") for p in POLICIES),
    "cap-sf1": (m.Cell("000"),) + tuple(policy_cell(CAP_POLICY, s) for s in CAP_SF1_SIZES),
    "cap-sf8": (m.Cell("000"),) + tuple(policy_cell(CAP_POLICY, s) for s in CAP_SF8_SIZES),
    # n=1 heap-feasibility probe for the largest SF8 arm; reports no percentages.
    "cap-sf8-smoke": (policy_cell(CAP_POLICY, "16g"),),
    # AV vs the finalized W-TinyLFU on the finalized Track2 layout (SF8 joint2),
    # fixed 4 GiB, same JAR, only -Dtrack1.d1.policy differs. Run once per cloud.
    # `wtinylfu` here is the exact config of the 2x2 `joint2-track1-final` cell.
    "av-vs-wtlfu-sf8-joint2": (policy_cell("wtinylfu-av", "4g"), policy_cell("wtinylfu", "4g")),
}
# Dataset and driver heap are tied to the phase so a capacity grid cannot be run
# against the wrong scale, and every arm of one grid shares one Xmx (P-5).
SF8_JOINT2 = "s3a://home-haoyue/track2/clickbench_sf8_e0_uc1_joint2"
PHASE_DATA = {"cap-sf1": SF1, "cap-sf8": SF8, "cap-sf8-smoke": SF8,
              "av-vs-wtlfu-sf8-joint2": SF8_JOINT2}
# The AV-vs-W-TinyLFU phase keeps the finalized 32g: the 2x2 results were taken at
# 32g, and AV's higher peak heap must be measured under the same limit.
PHASE_DRIVER_MEMORY = {"cap-sf1": "48g", "cap-sf8": "48g", "cap-sf8-smoke": "48g",
                       "av-vs-wtlfu-sf8-joint2": "32g"}
DEFAULT_DRIVER_MEMORY = "32g"
# SF8 shuffle scratch peaked at ~46 GiB (2026-10-15 run that hit ENOSPC); start a
# run only with that much headroom plus margin on the scratch disk.
DEFAULT_MIN_FREE_GIB = 50.0
CATALOG = {c.id: c for c in FIXED + HOLD_ADMIT + HOLD_BUDGET
           + (ONLINE, WTLFU_ONLINE, WTLFU_FIXED_4G, WTLFU_FIXED_8G,
              WTLFU_FIXED_2G, WTLFU_2M_FIXED_2G) + HOTPATH_2G + HOTPATH_4G
           + POLICY_CELLS}


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
    ap.add_argument("--data", default=None,
                    help=f"default: per-phase ({PHASE_DATA}), else {SF1}")
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
    ap.add_argument("--driver-memory", default=None,
                    help=f"default: per-phase ({PHASE_DRIVER_MEMORY}), else {DEFAULT_DRIVER_MEMORY}")
    ap.add_argument("--resume", action="store_true", default=True)
    ap.add_argument("--no-resume", dest="resume", action="store_false")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--allow-stale-jar", action="store_true",
                    help="skip the P-2 check that the JAR is newer than every source file")
    ap.add_argument("--min-free-gib", type=float, default=DEFAULT_MIN_FREE_GIB,
                    help="refuse to start a run when the scratch disk has less free space "
                         f"(default {DEFAULT_MIN_FREE_GIB:g}; 0 disables the guard)")
    args = ap.parse_args()
    args.data = args.data or PHASE_DATA.get(args.phase, SF1)
    args.driver_memory = args.driver_memory or PHASE_DRIVER_MEMORY.get(
        args.phase, DEFAULT_DRIVER_MEMORY)

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
          f"runs={args.runs}  slots={len(slots)}  data={args.data}  "
          f"driver={args.driver_memory}", flush=True)

    if not args.dry_run:
        problems = meta.preflight(allow_stale_jar=args.allow_stale_jar)
        if problems:
            for p in problems:
                print(f"  PREFLIGHT FAIL: {p}", flush=True)
            return 3
    record = meta.collect(args, cells, slots, probe_dataset=not args.dry_run)
    meta_path = meta.write(args.out, record, dry_run=args.dry_run)
    print(f"  p-1 metadata   {meta_path}", flush=True)

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
        scratch = s3a_session.spark_scratch()
        removed = meta.scratch_clean(scratch)
        ok, free = meta.disk_guard(scratch, args.min_free_gib)
        print(f"  scratch        cleaned {removed} leftover dirs; free {free:.1f} GiB "
              f"(need >= {args.min_free_gib:g})", flush=True)
        if args.min_free_gib and not ok:
            print(f"  DISK GUARD: only {free:.1f} GiB free on the scratch disk "
                  f"({scratch}); free space or lower --min-free-gib. Not starting "
                  f"{label}.", flush=True)
            return 4
        with meta.DiskSampler(scratch) as sampler:
            rc = subprocess.call(argv)
        os.makedirs(dest, exist_ok=True)
        with open(os.path.join(dest, "disk.json"), "w") as fh:
            json.dump({"scratch": scratch, "free_gib_before": round(free, 2),
                       "free_gib_after": round(meta.free_gib(scratch), 2),
                       "min_free_gib_during": (round(sampler.min_free, 2)
                                               if sampler.min_free is not None else None),
                       "samples": sampler.samples, "exit": rc}, fh, indent=2)
        print(f"  disk           min free during run "
              f"{sampler.min_free if sampler.min_free is None else round(sampler.min_free, 1)} GiB",
              flush=True)
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
