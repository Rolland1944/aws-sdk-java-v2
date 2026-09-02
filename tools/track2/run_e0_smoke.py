#!/usr/bin/env python3
"""E-0: does a plan built from bytes alone make ClickBench SF1 faster?

*** This is the gate every other v2 experiment waits behind. ***

E-0 asks one question and reports one number. If the wall clock does not come
down on a 14 GiB ClickBench, there is no point ablating six dimensions on a
larger one, and certainly no point asking an LLM to produce a plan that the
rules could not. So the pass condition is deliberately weak and deliberately
singular: median wall clock lower than the same environment's baseline. No 10%
threshold, no composite score, no GET count standing in for time.

GETs and remote bytes are recorded because they explain the result, not because
they are the result. A run where bytes fall and wall clock does not is a real
outcome that says the workload was not I/O-bound, and hiding it behind an I/O
metric would be exactly the substitution the contract forbids.

Eight stages, each skippable so a re-run does not repeat what already succeeded:

  1  baseline benchmark, SDK interceptor on   -> wall clock, io/*.ndjson
  2  parse footers of the baseline layout      -> chunk byte ranges
  3  correlate                                 -> observations + episodes
  4  access profile                            -> co-access, request shape
  5  dataset snapshot                          -> geometry, physical types
  6  deterministic plan                        -> the six-dimension plan
  7  rewrite (UC1 PyArrow or UC2 Spark)        -> the candidate layout
  8  candidate benchmark, same queries/config  -> wall clock again

Stage 8 must use the same query set, master and reader configuration as stage
1. Nothing here changes them between the two, and the report records both so a
mismatch is visible afterwards.

Usage:
  python3 tools/track2/run_e0_smoke.py \
      --source s3a://bucket/track2/clickbench_sf1 \
      --candidate-out s3a://bucket/track2/clickbench_sf1_e0 \
      --out docs/adaptive-range-reader/results/track2/e0_smoke \
      --sysconst .../sysconst.json --runs 5
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_QUERIES = os.path.join(HERE, "queries", "clickbench.sql")


def run(step, argv, dry_run=False):
    print(f"\n=== {step} ===", flush=True)
    print("  " + " ".join(argv), flush=True)
    if dry_run:
        return 0
    return subprocess.call(argv)


def py(script, *args):
    return [sys.executable, os.path.join(HERE, script), *[str(a) for a in args]]


def load(path):
    with open(path) as fh:
        return json.load(fh)


def benchmark_summary(report_path):
    if not os.path.exists(report_path):
        return None
    summary = load(report_path).get("summary") or {}
    runs = load(report_path).get("runs") or []
    io = (runs[0].get("io") or {}) if runs else {}
    return {
        "median_s": summary.get("median_s"),
        "mean_s": summary.get("mean_s"),
        "cv": summary.get("cv"),
        "stable": summary.get("gate_pass"),
        "end_to_end_s": summary.get("end_to_end_s"),
        "ranged_gets_run1": io.get("ranged_gets"),
        "remote_bytes_run1": io.get("remote_bytes"),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True, help="baseline layout root")
    ap.add_argument("--candidate-out", required=True, help="where to write the new layout")
    ap.add_argument("--out", required=True, help="results directory")
    ap.add_argument("--sysconst", required=True)
    ap.add_argument("--queries-file", default=DEFAULT_QUERIES)
    ap.add_argument("--tables", nargs="*", default=["hits"])
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--master", default="local[16]")
    ap.add_argument("--driver-memory", default="32g")
    ap.add_argument("--regime", default="measured_cross_cloud")
    ap.add_argument("--use-case", choices=("uc1", "uc2"), default="uc2",
                    help="uc1 writes with PyArrow, uc2 with Spark")
    ap.add_argument("--compression-probe", action="store_true",
                    help="measure per-column compressibility first (slow, UC1 only)")
    ap.add_argument("--skip", nargs="*", default=[],
                    choices=("baseline", "footer", "correlate", "profile",
                             "snapshot", "plan", "rewrite", "candidate"),
                    help="stages already done; their outputs are reused")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    out = os.path.abspath(args.out)
    os.makedirs(out, exist_ok=True)
    baseline_dir = os.path.join(out, "baseline")
    candidate_dir = os.path.join(out, "candidate")
    footer = os.path.join(out, "footer.parquet")
    observations = os.path.join(out, "observations.parquet")
    coverage = os.path.join(out, "coverage.json")
    profile = os.path.join(out, "access_profile.json")
    snapshot = os.path.join(out, "dataset_snapshot.json")
    probe = os.path.join(out, "compression_probe.json")
    plan = os.path.join(out, "plan_deterministic.json")
    table = args.tables[0]

    def skipped(stage):
        return stage in args.skip

    # 1. baseline: wall clock and the SDK trace the whole pipeline is built on
    if not skipped("baseline"):
        rc = run("1/8 baseline benchmark", py(
            "run_benchmark.py", "--data", args.source, "--out", baseline_dir,
            "--queries-file", args.queries_file, "--tables", *args.tables,
            "--runs", args.runs, "--layout-id", "baseline",
            "--master", args.master, "--driver-memory", args.driver_memory,
        ), args.dry_run)
        # A failing stability gate is not fatal here: E-0 compares medians, and
        # a noisy baseline shows up as a wide CV in the report rather than a
        # refusal to continue.
        if rc not in (0, 1):
            return rc

    # 2-3. footers, then the geometric join that replaced the semantic one
    if not skipped("footer"):
        if run("2/8 parse footers", py(
            "parse_footer.py", "--input", f"{args.source.rstrip('/')}/{table}",
            "--out", footer,
        ), args.dry_run):
            return 1
    if not skipped("correlate"):
        if run("3/8 correlate", py(
            "correlate.py", "--io", os.path.join(baseline_dir, "io", "*.ndjson"),
            "--footer", footer, "--out", observations, "--report", coverage,
        ), args.dry_run) not in (0, 1):
            return 1

    # 4-5. the two documents the planner reads
    if not skipped("snapshot"):
        if run("4/8 dataset snapshot", py(
            "dataset_snapshot.py", "--layout", args.source, "--out", snapshot,
        ), args.dry_run):
            return 1
    if not skipped("profile"):
        if run("5/8 access profile", py(
            "access_profile.py", "--observations", observations,
            "--dataset-snapshot", snapshot, "--sysconst", args.sysconst,
            "--regime", args.regime, "--out", profile,
        ), args.dry_run):
            return 1

    if args.compression_probe and not skipped("plan"):
        run("5b/8 compression probe", py(
            "compression_probe.py", "--layout", args.source,
            "--tables", *args.tables, "--out", probe,
        ), args.dry_run)

    # 6. the plan
    if not skipped("plan"):
        argv = py("plan_deterministic.py",
                  "--dataset-snapshot", snapshot, "--access-profile", profile,
                  "--sysconst", args.sysconst, "--regime", args.regime,
                  "--dataset", "clickbench", "--plan-id", "e0-deterministic",
                  "--out", plan)
        if args.compression_probe and os.path.exists(probe):
            argv += ["--compression-probe", probe]
        if run("6/8 deterministic plan", argv, args.dry_run):
            return 1

    # 7. materialise it through whichever writer the use case owns
    if not skipped("rewrite"):
        if args.use_case == "uc1":
            argv = py("write_layout_pyarrow.py", "--source", args.source,
                      "--out", args.candidate_out, "--plan", plan,
                      "--tables", *args.tables, "--verify")
        else:
            argv = py("write_layout.py", "--source", args.source,
                      "--out", args.candidate_out, "--candidate", plan,
                      "--tables", *args.tables, "--dataset", "clickbench",
                      "--master", args.master,
                      "--driver-memory", args.driver_memory, "--verify")
        if run(f"7/8 rewrite ({args.use_case.upper()})", argv, args.dry_run):
            return 1

    # 8. the same queries, the same configuration, the new bytes
    if not skipped("candidate"):
        rc = run("8/8 candidate benchmark", py(
            "run_benchmark.py", "--data", args.candidate_out, "--out", candidate_dir,
            "--queries-file", args.queries_file, "--tables", *args.tables,
            "--runs", args.runs, "--layout-id", "e0-deterministic",
            "--master", args.master, "--driver-memory", args.driver_memory,
        ), args.dry_run)
        if rc not in (0, 1):
            return rc

    if args.dry_run:
        print("\n(dry run: no report written)")
        return 0

    base = benchmark_summary(os.path.join(baseline_dir, "report.json"))
    cand = benchmark_summary(os.path.join(candidate_dir, "report.json"))
    if not base or not cand:
        print("\nmissing a benchmark report; cannot decide E-0")
        return 1

    delta = cand["median_s"] - base["median_s"]
    improve = (1.0 - cand["median_s"] / base["median_s"]) if base["median_s"] else None
    passed = delta < 0

    plan_doc = load(plan) if os.path.exists(plan) else {}
    report = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "experiment": "E-0 ClickBench SF1 wall-clock smoke test",
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md r5 §0.1",
        "criterion": ("median wall clock lower than the same-environment "
                      "baseline. No threshold: this is a direction check, not "
                      "a speed-up claim."),
        "use_case": args.use_case,
        "source": args.source,
        "candidate": args.candidate_out,
        "runs": args.runs,
        "master": args.master,
        "queries_file": args.queries_file,
        "plan_id": plan_doc.get("plan_id"),
        "plan_dimensions": plan_doc.get("dimensions"),
        "plan_explain": plan_doc.get("explain"),
        "baseline": base,
        "candidate_run": cand,
        "delta_s": round(delta, 3),
        "improve_frac": None if improve is None else round(improve, 4),
        "pass": passed,
        "supporting": {
            "note": ("GETs and bytes explain the result; they are not the "
                     "criterion. Bytes down with wall clock flat means the "
                     "workload was not I/O-bound."),
            "baseline_gets": base.get("ranged_gets_run1"),
            "candidate_gets": cand.get("ranged_gets_run1"),
            "baseline_remote_bytes": base.get("remote_bytes_run1"),
            "candidate_remote_bytes": cand.get("remote_bytes_run1"),
        },
        "stability": {
            "baseline_cv": base.get("cv"),
            "candidate_cv": cand.get("cv"),
            "note": ("a median difference smaller than either CV is not "
                     "evidence; re-run with more --runs before reading it"),
        },
        "next": ("E-A..E-F ablations may start" if passed else
                 "STOP: fix collection, planning or the writer path. Do not "
                 "ablate, and do not run plan_llm.py."),
    }
    path = os.path.join(out, "e0_report.json")
    with open(path, "w") as fh:
        json.dump(report, fh, indent=2)

    print("\n# E-0 result")
    print(f"  baseline   {base['median_s']:.2f}s median  (cv {base['cv']*100:.1f}%)")
    print(f"  candidate  {cand['median_s']:.2f}s median  (cv {cand['cv']*100:.1f}%)")
    print(f"  delta      {delta:+.2f}s  ({(improve or 0) * 100:+.1f}%)")
    print(f"  GETs       {base.get('ranged_gets_run1')} -> {cand.get('ranged_gets_run1')}")
    print(f"  verdict    {'PASS' if passed else 'FAIL'}")
    print(f"  {report['next']}")
    print(f"  report     {path}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
