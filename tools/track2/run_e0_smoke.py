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

  1  baseline benchmark, SDK interceptor on   -> wall clock, io/run-N/*.ndjson
  2  parse footers of the baseline layout      -> chunk byte ranges
  3  correlate                                 -> observations + episodes
  4  access profile                            -> co-access, request shape
  5  dataset snapshot                          -> geometry, physical types
  6  deterministic plan                        -> the six-dimension plan
  7  rewrite (UC1 PyArrow or UC2 Spark)        -> the candidate layout
  8  candidate benchmark, same queries/config  -> wall clock again

`--paired` replaces stages 1 and 8 with alternating one-run pairs so the
two layouts share a time window. Planning traces must already exist (skip
those stages) or be collected in a separate invocation.

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
import statistics
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


def io_globs_from_report(report_path):
    """Only the NDJSON directories this benchmark recorded.

    A flat `io/*.ndjson` glob re-reads leftover traces from earlier
    rewrites and invents a GET count nobody ran.
    """
    if not os.path.exists(report_path):
        return []
    out = []
    for run in (load(report_path).get("runs") or []):
        io_dir = run.get("io_dir")
        if io_dir and os.path.isdir(io_dir):
            out.append(os.path.join(io_dir, "*.ndjson"))
    return out


def check_written_layout(args, out, plan_path, table):
    """Parse the candidate, hard-check geometry, reprice; return 0 or 1."""
    sys.path.insert(0, HERE)
    import advisor_catalog
    import virtual_footer as vf
    import whatif

    cand_snap_path = os.path.join(out, "candidate_snapshot.json")
    print("\n=== 7b/8 written geometry ===", flush=True)
    rc = run("7b/8 candidate snapshot", py(
        "dataset_snapshot.py", "--layout", args.candidate_out,
        "--tables", table, "--sample-files", "0",
        "--out", cand_snap_path))
    if rc:
        return rc
    plan = load(plan_path)
    measured = load(cand_snap_path)
    cat = advisor_catalog.load(
        os.path.join(out, "dataset_snapshot.json"),
        os.path.join(out, "access_profile.json"))
    whatif.bind_catalog(cat)
    probe_path = os.path.join(out, "layout_probe.json")
    if os.path.exists(probe_path):
        vf.bind_probe(load(probe_path))
    predicted = vf.predict_geometry(table, plan)
    geom = (measured.get("geometry") or {}).get(table) or {}
    ok, failures = whatif.geometry_matches(predicted, geom)
    report = {
        "predicted": {k: predicted.get(k) for k in (
            "n_files", "n_rg", "compressed_bytes", "n_scan_units")},
        "measured": {k: geom.get(k) for k in (
            "files", "n_rg", "compressed_bytes", "n_scan_units")},
        "failures": failures,
        "pass": ok,
        "probe_bound": os.path.exists(probe_path),
    }
    with open(args.sysconst) as fh:
        sysc = json.load(fh)
    regime = sysc["regimes"][args.regime]
    vectored = sysc.get("vectored") or {}
    planned = whatif.evaluate_workload(plan, cat.patterns_for(table), regime, vectored)
    measured_cand = {
        "candidate_id": "written",
        "actions": [],
        "tables": {table: {
            "file_bytes": None,
            "rg_bytes": None,
            "column_order": (measured.get("column_order") or {}).get(table),
        }},
    }
    # Overlay measured geometry by temporarily swapping baseline? Better:
    # reprice the original plan (virtual) vs a synthetic candidate that uses
    # measured file/rg counts via tables overlay is incomplete. Report both
    # virtual t_io and note measured n_files/n_rg instead.
    report["virtual_t_io_s"] = planned["t_io_s"]
    report["virtual_ranged_gets"] = planned["ranged_gets"]
    winner = ((plan.get("search") or {}).get(table) or {}).get("winner") or {}
    report["planned_winner"] = winner
    path = os.path.join(out, "written_geometry.json")
    with open(path, "w") as fh:
        json.dump(report, fh, indent=2)
    print(f"  files  pred={predicted.get('n_files')}  "
          f"measured={geom.get('files')}")
    print(f"  n_rg   pred={predicted.get('n_rg')}  measured={geom.get('n_rg')}")
    print(f"  bytes  pred={predicted.get('compressed_bytes')}  "
          f"measured={geom.get('compressed_bytes')}")
    print(f"  gate   {'PASS' if ok else 'FAIL'}")
    print(f"  wrote  {path}")
    if not ok:
        print("  STOP: written geometry exceeds virtual-candidate tolerance")
        for failure in failures:
            print(f"    - {failure}")
        return 1
    return 0


def collect_candidate_io(args, out, plan_path, table):
    """Correlate candidate IO and check L1 components. Returns (report, rc)."""
    sys.path.insert(0, HERE)
    import advisor_catalog
    import whatif

    cand_footer = os.path.join(out, "candidate_footer.parquet")
    cand_obs = os.path.join(out, "candidate_observations.parquet")
    cand_cov = os.path.join(out, "candidate_coverage.json")
    cand_prof = os.path.join(out, "candidate_access_profile.json")
    cand_snap = os.path.join(out, "candidate_snapshot.json")
    io_globs = io_globs_from_report(os.path.join(out, "candidate", "report.json"))
    if not io_globs:
        io_globs = [os.path.join(out, "candidate", "io", "run-*", "*.ndjson")]
    # L1 validate is a per-run comparison. Folding five isolated traces
    # together multiplies observed GETs and looks like a model failure.
    if len(io_globs) > 1:
        print(f"  correlating first of {len(io_globs)} isolated run traces")
        io_globs = io_globs[:1]

    rc = run("8b/8 candidate footer", py(
        "parse_footer.py", "--input", f"{args.candidate_out.rstrip('/')}/{table}",
        "--out", cand_footer))
    if rc:
        return None, rc
    rc = run("8c/8 candidate correlate", py(
        "correlate.py", "--io", *io_globs, "--footer", cand_footer,
        "--out", cand_obs, "--report", cand_cov))
    if rc not in (0, 1):
        return None, rc
    if not os.path.exists(cand_snap):
        rc = run("8d/8 candidate snapshot", py(
            "dataset_snapshot.py", "--layout", args.candidate_out,
            "--tables", table, "--sample-files", "0", "--out", cand_snap))
        if rc:
            return None, rc
    rc = run("8e/8 candidate profile", py(
        "access_profile.py", "--observations", cand_obs,
        "--dataset-snapshot", cand_snap, "--sysconst", args.sysconst,
        "--regime", args.regime, "--out", cand_prof))
    if rc:
        return None, rc

    cat = advisor_catalog.load(
        os.path.join(out, "dataset_snapshot.json"),
        os.path.join(out, "access_profile.json"))
    whatif.bind_catalog(cat)
    probe_path = os.path.join(out, "layout_probe.json")
    if os.path.exists(probe_path):
        import virtual_footer as vf
        vf.bind_probe(load(probe_path))
    with open(args.sysconst) as fh:
        sysc = json.load(fh)
    regime = sysc["regimes"][args.regime]
    vectored = sysc.get("vectored") or {}
    plan = load(plan_path)
    report, _ev, _ok = whatif.run_validate_candidate(
        regime, vectored, load(os.path.join(out, "access_profile.json")),
        load(cand_prof), plan)
    path = os.path.join(out, "candidate_validate.json")
    with open(path, "w") as fh:
        json.dump(report, fh, indent=2)
    print(f"  data  pred={report['predicted']['data_gets']} "
          f"observed={report['observed']['data_gets']} "
          f"err={report.get('data_rel_error')}")
    print(f"  meta  pred={(report['predicted']['file_meta_gets'] or 0) + (report['predicted']['scan_meta_gets'] or 0)} "
          f"observed={report['observed']['meta_gets']} "
          f"err={report.get('meta_rel_error')}")
    print(f"  gate  {'PASS' if report.get('pass') else 'FAIL'}")
    print(f"  wrote {path}")
    return report, 0


def write_l1_ablation(args, out, plan_path):
    sys.path.insert(0, HERE)
    import advisor_catalog
    import whatif

    cat = advisor_catalog.load(
        os.path.join(out, "dataset_snapshot.json"),
        os.path.join(out, "access_profile.json"))
    whatif.bind_catalog(cat)
    probe_path = os.path.join(out, "layout_probe.json")
    if os.path.exists(probe_path):
        import virtual_footer as vf
        vf.bind_probe(load(probe_path))
    with open(args.sysconst) as fh:
        sysc = json.load(fh)
    report = whatif.run_l1_ablations(
        sysc["regimes"][args.regime], sysc.get("vectored") or {}, load(plan_path))
    path = os.path.join(out, "l1_ablation.json")
    with open(path, "w") as fh:
        json.dump(report, fh, indent=2)
    print("\n=== L1 ablation (identity / file / order) ===", flush=True)
    for row in report.get("variants") or []:
        print(f"  {row['id']:12} files={row['n_files']} rg={row['n_rg']} "
              f"splits={row['n_scan_units']} data={row['data_gets']} "
              f"scanM={row['scan_meta_gets']} gets={row['ranged_gets']} "
              f"t_io={row['t_io_s']}", flush=True)
    print(f"  wrote {path}", flush=True)
    return report


def _median_io(runs, key):
    vals = [(r.get("io") or {}).get(key) for r in runs]
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return statistics.median(vals)


def benchmark_summary(report_path):
    if not os.path.exists(report_path):
        return None
    doc = load(report_path)
    summary = doc.get("summary") or {}
    runs = doc.get("runs") or []
    io = (runs[0].get("io") or {}) if runs else {}
    return {
        "median_s": summary.get("median_s"),
        "mean_s": summary.get("mean_s"),
        "cv": summary.get("cv"),
        "stable": summary.get("gate_pass"),
        "end_to_end_s": summary.get("end_to_end_s"),
        "ranged_gets_run1": io.get("ranged_gets"),
        "remote_bytes_run1": io.get("remote_bytes"),
        "ranged_gets_median": _median_io(runs, "ranged_gets"),
        "remote_bytes_median": _median_io(runs, "remote_bytes"),
        "paired": bool(doc.get("paired")),
        "io_dirs": [r.get("io_dir") for r in runs if r.get("io_dir")],
    }


def merge_benchmark_reports(paths, dest_dir, layout_id, data):
    """Fold one-run paired reports into the directory E-0 already reads."""
    import run_benchmark
    runs = []
    for path in paths:
        for rec in (load(path).get("runs") or []):
            runs.append(rec)
    for i, rec in enumerate(runs, 1):
        rec["run"] = i
    os.makedirs(dest_dir, exist_ok=True)
    summary = run_benchmark.summarize(runs)
    report = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "layout_id": layout_id,
        "data": data,
        "paired": True,
        "runs": runs,
        "summary": summary,
    }
    dest = os.path.join(dest_dir, "report.json")
    with open(dest, "w") as fh:
        json.dump(report, fh, indent=2)
    return summary


def run_paired(args, out, baseline_dir, candidate_dir):
    """Alternate one baseline run and one candidate run, same window."""
    work = os.path.join(out, "paired")
    os.makedirs(work, exist_ok=True)
    base_paths, cand_paths = [], []
    for i in range(1, args.runs + 1):
        bdir = os.path.join(work, f"baseline_{i}")
        cdir = os.path.join(work, f"candidate_{i}")
        rc = run(f"pair {i}/{args.runs} baseline", py(
            "run_benchmark.py", "--data", args.source, "--out", bdir,
            "--queries-file", args.queries_file, "--tables", *args.tables,
            "--runs", 1, "--layout-id", "baseline",
            "--master", args.master, "--driver-memory", args.driver_memory,
        ), args.dry_run)
        if rc not in (0, 1):
            return rc
        rc = run(f"pair {i}/{args.runs} candidate", py(
            "run_benchmark.py", "--data", args.candidate_out, "--out", cdir,
            "--queries-file", args.queries_file, "--tables", *args.tables,
            "--runs", 1, "--layout-id", "e0-deterministic",
            "--master", args.master, "--driver-memory", args.driver_memory,
        ), args.dry_run)
        if rc not in (0, 1):
            return rc
        base_paths.append(os.path.join(bdir, "report.json"))
        cand_paths.append(os.path.join(cdir, "report.json"))
    if args.dry_run:
        return 0
    merge_benchmark_reports(base_paths, baseline_dir, "baseline", args.source)
    merge_benchmark_reports(
        cand_paths, candidate_dir, "e0-deterministic", args.candidate_out)
    return 0


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
                    help="run the layout probe first: per-column (codec, "
                         "encoding) tuples and the page ladder. Without it the "
                         "compression, encoding and page axes stay at baseline "
                         "because nothing measured them (slow, UC1 only)")
    ap.add_argument("--paired", action="store_true",
                    help="alternate one baseline run and one candidate run "
                         "--runs times after rewrite; replaces stages 1 and 8")
    ap.add_argument("--skip", nargs="*", default=[],
                    choices=("baseline", "footer", "correlate", "profile",
                             "snapshot", "plan", "rewrite", "candidate",
                             "candidate-io"),
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
    probe = os.path.join(out, "layout_probe.json")
    plan = os.path.join(out, "plan_deterministic.json")
    table = args.tables[0]

    def skipped(stage):
        return stage in args.skip

    # 1. baseline: wall clock and the SDK trace the whole pipeline is built on.
    # `--paired` collects this wall clock after rewrite, in the same window
    # as the candidate; a planning trace still has to come from a prior run.
    if not args.paired and not skipped("baseline"):
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
        base_io = io_globs_from_report(os.path.join(baseline_dir, "report.json"))
        if not base_io:
            base_io = [os.path.join(baseline_dir, "io", "run-*", "*.ndjson")]
        if run("3/8 correlate", py(
            "correlate.py", "--io", *base_io, "--footer", footer,
            "--out", observations, "--report", coverage,
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

    if args.compression_probe and not skipped("plan") and not os.path.exists(probe):
        # The page pass is sized from the snapshot's measured rows per row
        # group. Without that it writes a sample too small for the page limit
        # to bind, every ladder point looks like a no-op, and the page axis
        # comes back unpriced.
        run("5b/8 layout probe (codec x encoding tuples, page ladder)", py(
            "compression_probe.py", "--layout", args.source,
            "--tables", *args.tables, "--dataset-snapshot", snapshot,
            "--out", probe,
        ), args.dry_run)

    # 6. the plan. plan_deterministic.py refuses to emit if L1 cannot replay
    # the baseline GET/byte counts (self-consistency hard gate).
    if not skipped("plan"):
        argv = py("plan_deterministic.py",
                  "--dataset-snapshot", snapshot, "--access-profile", profile,
                  "--sysconst", args.sysconst, "--regime", args.regime,
                  "--dataset", "clickbench", "--plan-id", "e0-deterministic",
                  # L0 filters candidates against the renderer that will
                  # actually write them, during the search. Planning for
                  # PyArrow and then executing on Spark hands UC2 a layout
                  # strip_for_spark silently turns into a different one.
                  "--writer", "pyarrow" if args.use_case == "uc1" else "parquet-mr",
                  "--out", plan)
        # Bind an existing probe even when this invocation did not collect it.
        # Otherwise a re-plan silently drops codec/encoding from the search.
        if os.path.exists(probe):
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
        if not args.dry_run:
            if check_written_layout(args, out, plan, table):
                return 1

    # 8. the same queries, the same configuration, the new bytes
    if args.paired and (not skipped("baseline") or not skipped("candidate")):
        rc = run_paired(args, out, baseline_dir, candidate_dir)
        if rc not in (0, 1):
            return rc
    elif not skipped("candidate"):
        rc = run("8/8 candidate benchmark", py(
            "run_benchmark.py", "--data", args.candidate_out, "--out", candidate_dir,
            "--queries-file", args.queries_file, "--tables", *args.tables,
            "--runs", args.runs, "--layout-id", "e0-deterministic",
            "--master", args.master, "--driver-memory", args.driver_memory,
        ), args.dry_run)
        if rc not in (0, 1):
            return rc

    l1_ablation = None
    if os.path.exists(plan) and not args.dry_run:
        l1_ablation = write_l1_ablation(args, out, plan)

    cand_l1 = None
    cand_report = os.path.join(candidate_dir, "report.json")
    if (not skipped("candidate-io") and not args.dry_run
            and os.path.exists(plan)
            and (os.path.isdir(os.path.join(candidate_dir, "io"))
                 or io_globs_from_report(cand_report))):
        cand_l1, crc = collect_candidate_io(args, out, plan, table)
        if crc not in (0, 1):
            return crc

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
        "paired": bool(args.paired),
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
                     "workload was not I/O-bound. L1 candidate-component "
                     "validation is a separate model check."),
            "baseline_gets": base.get("ranged_gets_median") or base.get("ranged_gets_run1"),
            "candidate_gets": cand.get("ranged_gets_median") or cand.get("ranged_gets_run1"),
            "baseline_remote_bytes": base.get("remote_bytes_median") or base.get("remote_bytes_run1"),
            "candidate_remote_bytes": cand.get("remote_bytes_median") or cand.get("remote_bytes_run1"),
            "l1_ablation": l1_ablation,
            "l1_candidate_validate": cand_l1,
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
    print(f"  GETs       {base.get('ranged_gets_median') or base.get('ranged_gets_run1')} "
          f"-> {cand.get('ranged_gets_median') or cand.get('ranged_gets_run1')}")
    print(f"  verdict    {'PASS' if passed else 'FAIL'}")
    print(f"  {report['next']}")
    print(f"  report     {path}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
