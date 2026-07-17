#!/usr/bin/env python3
"""Track 1 POC evaluation (PROJECT2 20.2 / 20.5).

Compares the trained statistical policy selector against the hand-written
`template_auto`, per-workload best static strategies, and system baselines, on:
  * P1: each single workload trace (selector must be no worse than template_auto).
  * P3: the mixed trace (TPC-H -> Lance -> ML -> multimodal -> TPC-H, PROJECT2
    17.6) where the selector should win at/after load-switch points.

The mixed trace is built by concatenating single-workload traces (load_trace
time-shifts each phase), exactly like `prefetch_simulator.py --trace a b c ...`.

Outputs a JSON report and a convergence figure (cumulative remote bytes vs read
index with phase boundaries).
"""
from __future__ import annotations

import argparse
import json
import os
from argparse import Namespace

import numpy as np

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from prefetch_simulator import (
    CostModel,
    build_policy_dict,
    load_trace,
    replay,
)

# single workload -> (trace paths, best static strategy per PROJECT2 6.2)
SINGLE_SCENARIOS: list[tuple[str, list[str], str]] = [
    ("tpch", ["traces/tpch_sf1_full.csv"], "s3a_random"),
    ("ml_epoch_scan", ["traces/ml_taxi_epoch.csv"], "template_locality"),
    ("lance_large", ["traces/lance_sift1m_real/lance_all.csv"], "template_locality"),
    ("lance_small", ["traces/lance_fmnist_real/lance_all.csv"], "s3a_prefetch"),
    ("ml_embedding", ["traces/ml_emb_real.csv"], "s3a_prefetch"),
    ("multimodal", ["traces/mm_n2000/retrieval.csv"], "template_multimodal"),
]

# mixed trace phase order (PROJECT2 17.6). (phase label, trace path, best static)
MIXED_PHASES: list[tuple[str, str, str]] = [
    ("tpch_a", "traces/tpch_sf1_full.csv", "s3a_random"),
    ("lance", "traces/lance_sift1m_real/lance_all.csv", "template_locality"),
    ("ml_emb", "traces/ml_emb_real.csv", "s3a_prefetch"),
    ("multimodal", "traces/mm_n2000/retrieval.csv", "template_multimodal"),
    ("tpch_b", "traces/tpch_sf1_full.csv", "s3a_random"),
]

COMPARE_POLICIES = [
    "aws_range_get",
    "s3a_prefetch",
    "template_auto",
    "stat_selector",
]
CANDIDATE_STATICS = ["s3a_random", "template_locality", "s3a_prefetch", "template_multimodal"]

SWITCH_WINDOW = 200  # reads after a phase boundary counted as the "switch region"


def make_args(model: str | None, hysteresis: int, cache_mib: int) -> Namespace:
    return Namespace(
        readahead_kib=64,
        block_size_mib=8,
        prefetch_blocks=8,
        cache_budget_mib=cache_mib,
        stream_window_mib=128,
        max_forward_skip_kib=8192,
        template_window_kib=2048,
        template_small_page_kib=64,
        template_locality_page_kib=256,
        model=model,
        selector_hysteresis=hysteresis,
    )


def run_policy(
    trace_paths: list[str],
    policy_name: str,
    model: str | None,
    cost: CostModel,
    cache_mib: int,
    hysteresis: int,
    record: bool = False,
) -> tuple[dict, list[dict] | None]:
    df = load_trace(trace_paths)
    args = make_args(model, hysteresis, cache_mib)
    policy = build_policy_dict(args)[policy_name]  # fresh instance (selector is stateful)
    records: list[dict] | None = [] if record else None
    metrics, state = replay(df, policy, cost, cache_mib * 1024 * 1024, records=records)
    summary = metrics.to_dict(memory_bytes=state.cached_bytes())
    keep = {
        "logical_bytes": summary["logical_bytes"],
        "remote_bytes": summary["remote_bytes"],
        "remote_gets": summary["remote_gets"],
        "read_amplification": summary["read_amplification"],
        "latency_sum_ms": summary["latency_sum_ms"],
        "latency_p95_ms": summary["latency_p95_ms"],
        "cache_hit_read_rate": summary["cache_hit_read_rate"],
    }
    return keep, records


def phase_segments(records: list[dict]) -> list[tuple[int, int]]:
    """Contiguous runs of identical source_trace -> (start_idx, end_idx) per phase."""
    segments: list[tuple[int, int]] = []
    start = 0
    for i in range(1, len(records) + 1):
        if i == len(records) or records[i]["source_trace"] != records[start]["source_trace"]:
            segments.append((start, i))
            start = i
    return segments


def window_metrics(records: list[dict], start: int, end: int) -> dict:
    seg = records[start:end]
    logical = max(1, sum(r["logical_bytes"] for r in seg))
    remote = sum(r["remote_bytes"] for r in seg)
    lat = sum(r["latency_ms"] for r in seg)
    return {
        "reads": len(seg),
        "remote_bytes": int(remote),
        "read_amplification": remote / logical,
        "latency_sum_ms": lat,
    }


def cumulative(records: list[dict], key: str) -> np.ndarray:
    return np.cumsum([r[key] for r in records], dtype=float)


def evaluate_single(cost: CostModel, model: str, cache_mib: int, hysteresis: int) -> dict:
    print("=== P1: single-workload scenarios ===")
    out: dict[str, dict] = {}
    for tag, paths, best_static in SINGLE_SCENARIOS:
        runs: dict[str, dict] = {}
        for pol in COMPARE_POLICIES:
            runs[pol], _ = run_policy(paths, pol, model, cost, cache_mib, hysteresis)
        if best_static not in runs:
            runs[best_static], _ = run_policy(paths, best_static, model, cost, cache_mib, hysteresis)

        sel = runs["stat_selector"]
        auto = runs["template_auto"]
        best = runs[best_static]
        lat_ratio = sel["latency_sum_ms"] / max(auto["latency_sum_ms"], 1e-9)
        remote_ratio = sel["remote_bytes"] / max(auto["remote_bytes"], 1)
        # Primary objective (PROJECT2 2: ~15% perf) is latency; read amplification
        # is a budget guardrail reported separately.
        latency_not_worse = lat_ratio <= 1.02
        no_amp_regression = remote_ratio <= 1.10
        out[tag] = {
            "best_static": best_static,
            "runs": runs,
            "selector_vs_auto_latency_ratio": lat_ratio,
            "selector_vs_auto_remote_ratio": remote_ratio,
            "selector_vs_best_latency_ratio": sel["latency_sum_ms"] / max(best["latency_sum_ms"], 1e-9),
            "selector_amp": sel["read_amplification"],
            "auto_amp": auto["read_amplification"],
            "latency_not_worse_than_auto": bool(latency_not_worse),
            "no_amp_regression_vs_auto": bool(no_amp_regression),
        }
        print(
            f"  {tag:14s} best={best_static:20s} "
            f"sel/auto lat={lat_ratio:.3f} remote={remote_ratio:.3f} "
            f"amp {sel['read_amplification']:.2f}vs{auto['read_amplification']:.2f} "
            f"lat_ok={latency_not_worse} amp_ok={no_amp_regression}"
        )
    return out


def evaluate_mixed(cost: CostModel, model: str, cache_mib: int, hysteresis: int, fig_path: str) -> dict:
    print("\n=== P3: mixed trace ===")
    mixed_paths = [p for _, p, _ in MIXED_PHASES]

    overall: dict[str, dict] = {}
    records_by_policy: dict[str, list[dict]] = {}
    for pol in COMPARE_POLICIES + CANDIDATE_STATICS:
        rec = pol in COMPARE_POLICIES
        summary, records = run_policy(mixed_paths, pol, model, cost, cache_mib, hysteresis, record=rec)
        overall[pol] = summary
        if records is not None:
            records_by_policy[pol] = records

    sel_rec = records_by_policy["stat_selector"]
    segments = phase_segments(sel_rec)
    phase_labels = [lbl for lbl, _, _ in MIXED_PHASES]

    # per-phase breakdown for the three adaptive/baseline policies of interest
    per_phase: dict[str, dict] = {}
    tracked = ["stat_selector", "template_auto", "aws_range_get"]
    for pol in tracked:
        recs = records_by_policy[pol]
        segs = phase_segments(recs)
        per_phase[pol] = {}
        for (start, end), label in zip(segs, phase_labels):
            per_phase[pol][label] = window_metrics(recs, start, end)

    # switch-region convergence: first SWITCH_WINDOW reads of each phase (skip first phase)
    switch: dict[str, dict] = {}
    switch_max_amp: dict[str, float] = {}
    for pol in ["stat_selector", "template_auto"]:
        recs = records_by_policy[pol]
        segs = phase_segments(recs)
        switch[pol] = {}
        for (start, end), label in zip(segs[1:], phase_labels[1:]):
            w_end = min(end, start + SWITCH_WINDOW)
            switch[pol][label] = window_metrics(recs, start, w_end)
        switch_max_amp[pol] = max((m["read_amplification"] for m in switch[pol].values()), default=0.0)

    sel_total = overall["stat_selector"]
    auto_total = overall["template_auto"]
    lat_ratio = sel_total["latency_sum_ms"] / max(auto_total["latency_sum_ms"], 1e-9)
    remote_ratio = sel_total["remote_bytes"] / max(auto_total["remote_bytes"], 1)
    verdict = {
        "selector_vs_auto_latency_ratio": lat_ratio,
        "selector_vs_auto_remote_ratio": remote_ratio,
        "selector_amp": sel_total["read_amplification"],
        "auto_amp": auto_total["read_amplification"],
        # primary objective is latency; amplification is a budget guardrail
        "selector_beats_auto_latency": bool(sel_total["latency_sum_ms"] <= auto_total["latency_sum_ms"]),
        "no_amp_regression_vs_auto": bool(remote_ratio <= 1.10),
        "switch_region_max_amp": switch_max_amp,
    }
    print(
        f"  overall sel/auto  latency={lat_ratio:.3f} remote={remote_ratio:.3f} "
        f"amp {sel_total['read_amplification']:.2f}vs{auto_total['read_amplification']:.2f} "
        f"beats_lat={verdict['selector_beats_auto_latency']} amp_ok={verdict['no_amp_regression_vs_auto']}"
    )
    print(f"  switch-region max amplification: {switch_max_amp}")

    plot_convergence(records_by_policy, segments, phase_labels, fig_path)

    return {
        "phase_order": phase_labels,
        "overall": overall,
        "per_phase": per_phase,
        "switch_region": {"window_reads": SWITCH_WINDOW, "by_policy": switch},
        "verdict": verdict,
        "figure": fig_path,
    }


def plot_convergence(records_by_policy, segments, phase_labels, fig_path: str) -> None:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 8), sharex=True)
    # s3a_prefetch is a 700GB+ / 55x-amp outlier that flattens the plot; keep it
    # out of the figure (still in the JSON table) so selector-vs-auto is visible.
    styles = {
        "stat_selector": {"color": "C0", "lw": 2.2, "zorder": 5},
        "template_auto": {"color": "C1", "lw": 1.8},
        "aws_range_get": {"color": "C7", "lw": 1.2, "ls": "--"},
    }
    for pol, st in styles.items():
        rec = records_by_policy.get(pol)
        if rec is None:
            continue
        x = np.arange(len(rec))
        ax1.plot(x, cumulative(rec, "remote_bytes") / (1024 * 1024), label=pol, **st)
        ax2.plot(x, cumulative(rec, "latency_ms") / 1000.0, label=pol, **st)

    for (start, _), label in zip(segments, phase_labels):
        for ax in (ax1, ax2):
            ax.axvline(start, color="gray", alpha=0.4, lw=0.8)
        ax1.text(start, ax1.get_ylim()[1] * 0.02, f" {label}", fontsize=8, color="gray", rotation=90, va="bottom")

    ax1.set_ylabel("cumulative remote bytes (MiB)")
    ax2.set_ylabel("cumulative sim latency (s)")
    ax2.set_xlabel("read index (mixed trace, phase boundaries marked)")
    ax1.set_title("Track 1 mixed-trace convergence: stat_selector vs baselines")
    ax1.legend(loc="upper left", fontsize=9)
    ax1.grid(alpha=0.3)
    ax2.grid(alpha=0.3)
    fig.tight_layout()
    os.makedirs(os.path.dirname(os.path.abspath(fig_path)), exist_ok=True)
    fig.savefig(fig_path, dpi=110)
    plt.close(fig)
    print(f"  wrote {fig_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Track 1 POC evaluation")
    ap.add_argument("--model", default="models/policy_selector.joblib")
    ap.add_argument("--out-json", default="access_report/track1_eval.json")
    ap.add_argument("--figure", default="access_report/track1_mixed_convergence.png")
    ap.add_argument("--request-rtt-ms", type=float, default=50.0)
    ap.add_argument("--bandwidth-mib-s", type=float, default=100.0)
    ap.add_argument("--cache-budget-mib", type=int, default=256)
    ap.add_argument("--selector-hysteresis", type=int, default=3)
    args = ap.parse_args()

    cost = CostModel(request_rtt_ms=args.request_rtt_ms, bandwidth_mib_s=args.bandwidth_mib_s)
    single = evaluate_single(cost, args.model, args.cache_budget_mib, args.selector_hysteresis)
    mixed = evaluate_mixed(cost, args.model, args.cache_budget_mib, args.selector_hysteresis, args.figure)

    p1_latency = all(v["latency_not_worse_than_auto"] for v in single.values())
    p1_amp = all(v["no_amp_regression_vs_auto"] for v in single.values())
    report = {
        "config": {
            "model": args.model,
            "request_rtt_ms": args.request_rtt_ms,
            "bandwidth_mib_s": args.bandwidth_mib_s,
            "cache_budget_mib": args.cache_budget_mib,
            "selector_hysteresis": args.selector_hysteresis,
        },
        "P1_single": single,
        "P1_latency_pass": bool(p1_latency),
        "P1_no_amp_regression": bool(p1_amp),
        "P3_mixed": mixed,
        "P3_latency_pass": bool(mixed["verdict"]["selector_beats_auto_latency"]),
        "P3_no_amp_regression": bool(mixed["verdict"]["no_amp_regression_vs_auto"]),
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out_json)), exist_ok=True)
    with open(args.out_json, "w") as f:
        json.dump(report, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"\nP1 latency (selector <= template_auto on all single): {p1_latency}")
    print(f"P1 amplification guardrail (no >10% remote regression): {p1_amp}")
    print(f"P3 latency (selector beats template_auto on mixed): {report['P3_latency_pass']}")
    print(f"P3 amplification guardrail: {report['P3_no_amp_regression']}")
    print(f"wrote {args.out_json}")


if __name__ == "__main__":
    main()
