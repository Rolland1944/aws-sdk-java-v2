#!/usr/bin/env python3
"""Does the per-workload BEST policy (= the training label) change with cache budget?

Why: PROJECT2 6.2 labelled every workload with its best static strategy at
cache_budget=256MiB, and the decision tree was trained on those labels. The SDK
benchmark then ran at 64MiB, where the learned selector beat the hand rule by only
5.3% on bytes (and was slightly WORSE at 8/32MiB) - see PROJECT2 8.4. Hypothesis:
the tree is applying 256MiB-optimal policies in a regime they were not chosen for.

This is the cheap precondition for "should we retrain at 64MiB?":
  * if NO label flips  -> retraining learns the same mapping and changes nothing;
  * if labels DO flip  -> retraining has real substance, and this says for which
    workloads and how big the miss is.

Only the FOUR labels the Java side can actually execute are considered (the SDK has
exactly four policy executors), so the answer is directly actionable.

"Best" = minimum simulated latency_sum_ms, matching eval_track1's stated objective
("Primary objective is latency; read amplification is a budget guardrail").

Usage:
  PYTHONPATH=tools .venv/bin/python tools/label_flip_check.py [--budgets 64 256]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

from argparse import Namespace

from prefetch_simulator import CostModel, build_policy_dict, load_trace, replay

# Mirrors eval_track1.SINGLE_SCENARIOS / make_args / run_policy. Inlined rather than
# imported because eval_track1 pulls in matplotlib at module scope (figure output),
# which this check does not need. Keep these in sync with eval_track1 if it changes.
SINGLE_SCENARIOS: list[tuple[str, list[str], str]] = [
    ("tpch", ["traces/tpch_sf1_full.csv"], "s3a_random"),
    ("ml_epoch_scan", ["traces/ml_taxi_epoch.csv"], "template_locality"),
    ("lance_large", ["traces/lance_sift1m_real/lance_all.csv"], "template_locality"),
    ("lance_small", ["traces/lance_fmnist_real/lance_all.csv"], "s3a_prefetch"),
    ("ml_embedding", ["traces/ml_emb_real.csv"], "s3a_prefetch"),
    ("multimodal", ["traces/mm_n2000/retrieval.csv"], "template_multimodal"),
]

# The four policies the SDK actually implements as executors.
CANDIDATES = ["s3a_random", "template_locality", "s3a_prefetch", "template_multimodal"]


def make_args(cache_mib: int) -> Namespace:
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
        model=None,
        selector_hysteresis=3,
    )


def run_policy(trace_paths: list[str], policy_name: str, cost: CostModel, cache_mib: int) -> dict:
    df = load_trace(trace_paths)
    policy = build_policy_dict(make_args(cache_mib))[policy_name]  # fresh instance (policies are stateful)
    metrics, state = replay(df, policy, cost, cache_mib * 1024 * 1024)
    summary = metrics.to_dict(memory_bytes=state.cached_bytes())
    return {
        "logical_bytes": summary["logical_bytes"],
        "remote_bytes": summary["remote_bytes"],
        "remote_gets": summary["remote_gets"],
        "read_amplification": summary["read_amplification"],
        "latency_sum_ms": summary["latency_sum_ms"],
        "latency_p95_ms": summary["latency_p95_ms"],
        "cache_hit_read_rate": summary["cache_hit_read_rate"],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Per-workload best-policy flip check across cache budgets")
    ap.add_argument("--budgets", type=int, nargs="+", default=[64, 256])
    ap.add_argument("--request-rtt-ms", type=float, default=50.0)
    ap.add_argument("--bandwidth-mib-s", type=float, default=100.0)
    ap.add_argument("--json", default="access_report/label_flip_check.json")
    args = ap.parse_args()

    cost = CostModel(request_rtt_ms=args.request_rtt_ms, bandwidth_mib_s=args.bandwidth_mib_s)
    results: dict = {}

    for tag, paths, label_ref in SINGLE_SCENARIOS:
        results[tag] = {"reference_label_256": label_ref, "by_budget": {}}
        for budget in args.budgets:
            runs: dict[str, dict] = {}
            for pol in CANDIDATES:
                t0 = time.time()
                summary = run_policy(paths, pol, cost, budget)
                runs[pol] = summary
                print(f"  [{tag} @{budget}MiB] {pol:22s} "
                      f"lat={summary['latency_sum_ms'] / 1000:9.1f}s "
                      f"remote={summary['remote_bytes'] / 2**20:9.1f}MiB "
                      f"amp={summary['read_amplification']:6.3f}  ({time.time() - t0:.0f}s)",
                      flush=True)
            best = min(CANDIDATES, key=lambda p: runs[p]["latency_sum_ms"])
            results[tag]["by_budget"][str(budget)] = {"best": best, "runs": runs}
            print(f"  => {tag} @{budget}MiB best={best}", flush=True)
        print(flush=True)

    print("\n" + "=" * 96)
    print("PER-WORKLOAD BEST POLICY (by simulated latency), across cache budgets")
    print("=" * 96)
    header = f"{'workload':16s} {'label(ref,256)':22s}" + "".join(f" {str(b) + 'MiB':>22s}" for b in args.budgets)
    print(header)
    print("-" * len(header))
    flips: list[str] = []
    for tag, _, label_ref in SINGLE_SCENARIOS:
        row = f"{tag:16s} {label_ref:22s}"
        bests = []
        for b in args.budgets:
            best = results[tag]["by_budget"][str(b)]["best"]
            bests.append(best)
            mark = "" if best == label_ref else " *"
            row += f" {best + mark:>22s}"
        print(row)
        if len(set(bests)) > 1:
            flips.append(f"{tag}: " + " -> ".join(f"{b}MiB={p}" for b, p in zip(args.budgets, bests)))

    print("\nFLIPS ACROSS BUDGETS (the label a retrain would actually change):")
    if flips:
        for f in flips:
            print(f"  * {f}")
    else:
        print("  (none - the best policy is budget-invariant for every workload)")

    # How much does using the WRONG-regime label cost, at the smallest budget?
    small = min(args.budgets)
    print(f"\ncost of keeping the 256MiB label at {small}MiB (latency, lower=better):")
    for tag, _, label_ref in SINGLE_SCENARIOS:
        runs = results[tag]["by_budget"][str(small)]["runs"]
        best = results[tag]["by_budget"][str(small)]["best"]
        ref_lat = runs[label_ref]["latency_sum_ms"]
        best_lat = runs[best]["latency_sum_ms"]
        gap = (ref_lat - best_lat) / max(best_lat, 1e-9)
        flag = "  <-- retrain would fix this" if best != label_ref else ""
        print(f"  {tag:16s} label={label_ref:22s} {ref_lat / 1000:8.1f}s   "
              f"best={best:22s} {best_lat / 1000:8.1f}s   gap={gap:+7.1%}{flag}")

    os.makedirs(os.path.dirname(os.path.abspath(args.json)), exist_ok=True)
    with open(args.json, "w") as f:
        json.dump({"budgets": args.budgets, "candidates": CANDIDATES,
                   "cost_model": {"rtt_ms": args.request_rtt_ms, "bw_mib_s": args.bandwidth_mib_s},
                   "results": results}, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"\nwrote {args.json}")


if __name__ == "__main__":
    sys.exit(main())
