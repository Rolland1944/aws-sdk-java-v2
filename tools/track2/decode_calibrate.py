#!/usr/bin/env python3
"""Calibrate advisor_policy.DECODE_RATE_SCALE against measured executor CPU.

The decode probe times a single-threaded PyArrow read of a tmpfs file. The
benchmark runs sixteen Spark tasks over S3 through parquet-mr. Those are not
the same number, and the gap between them is one scalar in the cost model.
This fits it.

What makes the fit possible at all is that the model's natural output is
core-seconds, which is exactly what `Executor CPU Time` in a Spark event log
measures. No parallelism assumption enters on either side.

What makes the fit *weak* is that the event log reports total task CPU, not
decode CPU:

    CPU = s x D_model + N

`D_model` is predicted decode core-seconds and `N` is everything else --
filters, aggregation, shuffle, serialisation, scheduling. Two runs give two
equations and there are three unknowns, so `N` needs an assumption, and the
answer is only as good as that assumption. Two are offered here and both are
reported, because the spread between them is the honest error bar:

  * `tasks`  -- N scales with task count. A candidate with larger files runs
    fewer, fatter tasks, and per-task overhead leaves with them.
  * `fixed`  -- N is unchanged. On the ClickBench pair this one is refuted
    rather than uncertain: it can only be satisfied by a negative scale, and
    saying so is more useful than averaging it in.

A scale fitted this way is a consistency check, not a measurement, and
`DECODE_MODELLED` should stay False on the strength of it alone. The
experiment that would identify `s` directly is a scan-only Spark job over the
real layout with `spark.sql.parquet.filterPushdown=false`, one column at a
time: decode dominates the task CPU, and the ratio to encoded bytes is the
reader's rate with no `N` to model.

Usage:
  python3 tools/track2/decode_calibrate.py \
      --results docs/.../e0_smoke_uc1 \
      --sysconst docs/.../sysconst.json --regime same_region_m5d
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import access_profile  # noqa: E402
import advisor_catalog  # noqa: E402
import dataset_snapshot  # noqa: E402
import virtual_footer as vf  # noqa: E402
import whatif  # noqa: E402

# Spark reports these in different units in the same event. Getting it wrong
# is a factor of a million, so both are named rather than inlined.
CPU_TIME_NS = "Executor CPU Time"
RUN_TIME_MS = "Executor Run Time"


def read_eventlog(path):
    """Task metric totals for one run: core-seconds, run-seconds, task count.

    Event logs are written zstd-compressed in a versioned directory. `zstd -dc`
    is used rather than a Python decompressor so this works on a host where
    only the CLI is installed, which is the host the benchmark ran on.
    """
    events = None
    for root, _dirs, files in os.walk(path):
        for name in sorted(files):
            if name.startswith("events_"):
                events = os.path.join(root, name)
                break
        if events:
            break
    if not events:
        return None
    if events.endswith(".zstd"):
        raw = subprocess.run(["zstd", "-dc", events], capture_output=True,
                             check=True).stdout.decode()
    else:
        with open(events) as fh:
            raw = fh.read()

    cpu_ns = run_ms = 0
    tasks = 0
    input_bytes = 0
    for line in raw.splitlines():
        if '"SparkListenerTaskEnd"' not in line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        metrics = rec.get("Task Metrics") or {}
        if not metrics:
            continue
        tasks += 1
        cpu_ns += metrics.get(CPU_TIME_NS) or 0
        run_ms += metrics.get(RUN_TIME_MS) or 0
        input_bytes += (metrics.get("Input Metrics") or {}).get("Bytes Read") or 0
    if not tasks:
        return None
    return {
        "cpu_core_s": cpu_ns / 1e9,
        "run_core_s": run_ms / 1e3,
        # Run time minus CPU time is the task's blocked time, most of it S3.
        "io_wait_core_s": run_ms / 1e3 - cpu_ns / 1e9,
        "tasks": tasks,
        "input_bytes": input_bytes,
    }


def measure_side(directory, side, runs):
    """Event-log totals for every run of one side, plus their medians."""
    per_run = []
    for i in range(1, runs + 1):
        logs = os.path.join(directory, "paired", f"{side}_{i}", "eventlogs")
        if not os.path.isdir(logs):
            continue
        rec = read_eventlog(logs)
        if rec:
            rec["run"] = i
            per_run.append(rec)
    if not per_run:
        return None
    keys = ("cpu_core_s", "run_core_s", "io_wait_core_s", "tasks", "input_bytes")
    return {
        "per_run": per_run,
        "n_runs": len(per_run),
        **{k: statistics.median([r[k] for r in per_run]) for k in keys},
        "cpu_cv": (statistics.stdev([r["cpu_core_s"] for r in per_run])
                   / statistics.mean([r["cpu_core_s"] for r in per_run])
                   if len(per_run) > 1 else 0.0),
    }


def predict_decode(directory, sysconst, regime_name, reader):
    """Predicted decode core-seconds for the baseline and for the plan."""
    snap = dataset_snapshot.DatasetSnapshot(
        json.load(open(os.path.join(directory, "dataset_snapshot.json"))))
    prof = access_profile.AccessProfile(
        json.load(open(os.path.join(directory, "access_profile.json"))))
    cat = advisor_catalog.AdvisorCatalog(snap, prof)
    whatif.bind_catalog(cat)
    with open(os.path.join(directory, "layout_probe.json")) as fh:
        vf.bind_probe(json.load(fh))
    with open(os.path.join(directory, "decode_probe.json")) as fh:
        # scale 1.0: this function produces the quantity the scale multiplies.
        vf.bind_decode_profile(json.load(fh), reader=reader, rate_scale=1.0)
    with open(os.path.join(directory, "plan_deterministic.json")) as fh:
        plan = json.load(fh)
    with open(sysconst) as fh:
        sysc = json.load(fh)
    regime = sysc["regimes"][regime_name]
    vectored = sysc.get("vectored") or {}

    out = {}
    for side, cand in (("baseline", {"candidate_id": "baseline", "actions": []}),
                       ("candidate", plan)):
        ev = whatif.evaluate_workload(cand, cat.PATTERNS, regime, vectored)
        out[side] = {
            "decode_core_s": ev["decode_core_s"],
            "t_io_s": ev["t_io_s"],
            "priced": ev["decode_priced"],
            "unpriced_columns": ev["decode_unpriced_columns"],
        }
    return out


def fit_scale(cpu_base, cpu_cand, d_base, d_cand, ratio):
    """Solve CPU = s x D + N for s, given N_cand = ratio x N_base.

    `s` here is a *cost* multiplier on predicted core-seconds. The knob it
    feeds, advisor_policy.DECODE_RATE_SCALE, multiplies the rate instead, so
    it takes 1/s. Both are reported rather than one, because plugging the cost
    multiplier into the rate knob makes decode 6x cheaper instead of 6x
    dearer and every number downstream still looks reasonable.

    Returns (cost_scale, decode_share_of_baseline_cpu) or None when the system
    has no positive solution -- which is itself a result about the assumption,
    not a numerical failure.
    """
    # cpu_base = s*d_base + n
    # cpu_cand = s*d_cand + ratio*n
    denom = d_cand - ratio * d_base
    if abs(denom) < 1e-9:
        return None
    scale = (cpu_cand - ratio * cpu_base) / denom
    n_base = cpu_base - scale * d_base
    if scale <= 0 or n_base < 0:
        return None
    return scale, (scale * d_base) / cpu_base


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", required=True,
                    help="E-0 results directory containing paired/ and the probes")
    ap.add_argument("--sysconst", required=True)
    ap.add_argument("--regime", default="same_region_m5d")
    ap.add_argument("--reader", choices=vf.DECODE_READERS, default="pyarrow")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    print(f"# decode rate calibration ({args.reader} rates)")
    sides = {}
    for side in ("baseline", "candidate"):
        rec = measure_side(args.results, side, args.runs)
        if not rec:
            raise SystemExit(f"no event logs for {side} under {args.results}/paired")
        sides[side] = rec
        print(f"  {side:9} n={rec['n_runs']} cpu={rec['cpu_core_s']:8.1f} core-s "
              f"(cv {100 * rec['cpu_cv']:.1f}%)  io_wait={rec['io_wait_core_s']:8.1f}  "
              f"tasks={int(rec['tasks'])}")

    pred = predict_decode(args.results, args.sysconst, args.regime, args.reader)
    for side, rec in pred.items():
        if not rec["priced"]:
            raise SystemExit(f"{side} decode is unpriced: "
                             f"{rec['unpriced_columns'][:10]}")
        print(f"  {side:9} predicted decode {rec['decode_core_s']:8.1f} core-s "
              f"at scale 1.0")

    d_base = pred["baseline"]["decode_core_s"]
    d_cand = pred["candidate"]["decode_core_s"]
    cpu_base = sides["baseline"]["cpu_core_s"]
    cpu_cand = sides["candidate"]["cpu_core_s"]
    task_ratio = sides["candidate"]["tasks"] / sides["baseline"]["tasks"]

    print(f"\n  measured   CPU {cpu_base:.1f} -> {cpu_cand:.1f} core-s "
          f"({100 * (cpu_cand / cpu_base - 1):+.1f}%)")
    print(f"  predicted  decode {d_base:.1f} -> {d_cand:.1f} core-s "
          f"({100 * (d_cand / d_base - 1):+.1f}%) at scale 1.0")
    print(f"  task count {int(sides['baseline']['tasks'])} -> "
          f"{int(sides['candidate']['tasks'])} (ratio {task_ratio:.4f})")

    assumptions = {"tasks": task_ratio, "fixed": 1.0}
    fits = {}
    print("\n  non-decode CPU assumption -> fitted scale")
    for name, ratio in assumptions.items():
        got = fit_scale(cpu_base, cpu_cand, d_base, d_cand, ratio)
        if got is None:
            fits[name] = None
            print(f"    {name:8} (N_cand = {ratio:.4f} x N_base)  no positive "
                  f"solution: the measured CPU drop cannot be explained with "
                  f"this much non-decode CPU held constant")
            continue
        scale, share = got
        fits[name] = {"ratio": ratio,
                      "cost_scale": round(scale, 3),
                      "decode_rate_scale": round(1.0 / scale, 4),
                      "decode_share_of_baseline_cpu": round(share, 4),
                      "implied_decode_core_s": {
                          "baseline": round(scale * d_base, 1),
                          "candidate": round(scale * d_cand, 1)}}
        print(f"    {name:8} (N_cand = {ratio:.4f} x N_base)  "
              f"cost x{scale:5.2f}  -> DECODE_RATE_SCALE={1.0 / scale:.4f}  "
              f"(decode is {100 * share:.0f}% of baseline task CPU)")

    # Sensitivity on the one assumption that produced a number: how much does
    # the fitted scale move for a 10% error in the task-count ratio?
    if fits.get("tasks"):
        print("\n  sensitivity of the `tasks` fit to that ratio being wrong")
        for bump in (0.9, 1.0, 1.1):
            got = fit_scale(cpu_base, cpu_cand, d_base, d_cand, task_ratio * bump)
            label = f"ratio x {bump:.1f}"
            print(f"    {label:12} -> "
                  + (f"cost x{got[0]:5.2f}  DECODE_RATE_SCALE={1.0 / got[0]:.4f}"
                     if got else "no positive solution"))

    doc = {
        "measured": sides,
        "predicted_at_scale_1": pred,
        "task_ratio": task_ratio,
        "fits": fits,
        "reader": args.reader,
        "regime": args.regime,
        "verdict": (
            "DECODE_RATE_SCALE is identified only up to an assumption about "
            "non-decode CPU, because the event log measures total task CPU. "
            "The `fixed` assumption has no positive solution, so the fit rests "
            "entirely on non-decode CPU falling with task count. Treat the "
            "number as a consistency check and keep DECODE_MODELLED False "
            "until a scan-only job (filterPushdown=false, one column at a "
            "time, over the real layout) measures the reader's rate directly."),
    }
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(doc, fh, indent=2)
        print(f"\n  out {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
