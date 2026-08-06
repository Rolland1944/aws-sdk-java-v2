#!/usr/bin/env python3
"""Measure how often each selector picks the workload's BEST policy (oracle agreement).

Motivation: on the SDK benchmark the learned decision tree only beats the hand-written
`template_auto` by a few percent. Before blaming the model class (tree vs GBDT vs
bandit vs ...), we need to know whether there is any headroom left at all: if the hand
rule ALREADY routes ~all reads to the per-workload best policy, then no model - however
fancy - can gain much, and the remaining gap must come from somewhere else (executor
parameters, cache/budget, prefetch shape).

So for every read we compare each selector's CHOICE against the oracle label (that
workload's best static strategy, PROJECT2 6.2 / train_policy_selector.DEFAULT_TRAINING_SET)
and report the agreement rate + which policy it picks instead.

Selectors compared (all label-free unless noted):
  ta(java)   - the rule ACTUALLY shipped/benchmarked: internal.RuleBasedPolicySelector.
               Label-free, and page-visit counts advance on EVERY read.
  ta(sim)    - the original prefetch_simulator.TemplateAutoPolicy: additionally peeks at
               `query_id.startswith("retrieval")` (workload-oracle LEAK) and only advances
               page counts inside the locality branch. Included to quantify how much of
               the hand rule's apparent skill came from the leak / the counter quirk.
  tree+hyst  - the deployed learned selector: decision tree over the 10-dim feature window
               + hysteresis(3), i.e. AdaptivePolicySelector.
  tree(raw)  - same tree WITHOUT hysteresis, to isolate what smoothing costs/buys.

IMPORTANT caveats when reading the numbers:
  * The oracle is CONSTANT per workload (one best policy per trace). So "agreement" means
    "did the selector converge to this workload's best policy", NOT "was this individual
    read routed optimally". A non-oracle choice is not automatically harmful (e.g. routing
    a 1MiB read to multimodal inside a mostly-random trace can be fine).
  * The tree was TRAINED on the in-sample traces with exactly these labels, so its
    in-sample agreement is optimistically biased. Held-out rows are reported separately -
    trust those.
  Agreement is therefore a HEADROOM diagnostic, not a performance metric; the performance
  truth stays the simulator replay / AdaptiveReaderSystemBenchmark.

Usage:
  python3 tools/policy_agreement.py [--model models/policy_selector.joblib]
                                    [--max-reads 20000] [--json access_report/...]
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter, deque

import numpy as np

from prefetch_simulator import (
    Request,
    align_down,
    extract_features,
    load_trace,
    row_to_request,
)

HISTORY_WINDOW = 64
RECENT_MAX = 8
LOCALITY_PAGE = 256 * 1024
LARGE_READ = 128 * 1024
SEQ_MIN_READ = 32 * 1024
SMALL_READ = 16 * 1024
FORWARD_THRESHOLD = 0.7

# Traces the tree was TRAINED on (label = that workload's best static strategy).
IN_SAMPLE: list[tuple[str, str, str]] = [
    ("traces/tpch_sf1_full.csv", "s3a_random", "tpch"),
    ("traces/ml_taxi_epoch.csv", "template_locality", "ml_epoch_scan"),
    ("traces/lance_sift1m_real/lance_all.csv", "template_locality", "lance_large"),
    ("traces/lance_fmnist_real/lance_all.csv", "s3a_prefetch", "lance_small"),
    ("traces/ml_emb_real.csv", "s3a_prefetch", "ml_embedding"),
    ("traces/mm_n2000/retrieval.csv", "template_multimodal", "multimodal"),
]

# Never trained on: same five workload CLASSES, different sources/phases.
HELD_OUT: list[tuple[str, str, str]] = [
    ("traces/clickbench.csv", "s3a_random", "clickbench"),
    ("traces/ml_lastfm_emb.csv", "s3a_prefetch", "ml_lastfm_emb"),
    ("traces/mm_n2000/mm_all.csv", "template_multimodal", "mm_all"),
]

# mixed_holdout.csv carries query_id = "<class>:<source>:<orig>" (build_mixed_trace.py),
# so the oracle is resolved PER ROW from the source tag - this also disambiguates the
# lance class, whose two members have different best policies (fmnist vs sift).
MIXED_TRACE = "traces/mixed_holdout.csv"
SOURCE_ORACLE: dict[str, str] = {
    "tpch": "s3a_random",
    "clickbench": "s3a_random",
    "fmnist": "s3a_prefetch",
    "sift": "template_locality",
    "emb_real": "s3a_prefetch",
    "lastfm": "s3a_prefetch",
    "taxi": "template_locality",
    "mm": "template_multimodal",
}

SELECTORS = ["ta(java)", "ta(sim)", "tree+hyst", "tree(raw)"]


class ObjState:
    __slots__ = ("recent", "seen_pages")

    def __init__(self) -> None:
        self.recent: deque[Request] = deque(maxlen=RECENT_MAX)
        self.seen_pages: dict[int, int] = {}


def _forward_ratio(recent: deque[Request]) -> float:
    pairs = list(zip(recent, list(recent)[1:]))
    if not pairs:
        return 0.0
    forward = sum(1 for a, b in pairs if b.offset >= a.offset)
    return forward / len(pairs)


def rule_choice(obj: ObjState, req: Request, *, leak_query_id: bool) -> str:
    """The template_auto decision (which sub-policy), without executing any fetch."""
    if leak_query_id and req.query_id.startswith("retrieval"):
        return "template_multimodal"
    if req.length >= LARGE_READ:
        return "template_multimodal"
    if len(obj.recent) >= 2 and _forward_ratio(obj.recent) >= FORWARD_THRESHOLD \
            and req.length >= SEQ_MIN_READ:
        return "s3a_prefetch"
    if obj.seen_pages.get(align_down(req.offset, LOCALITY_PAGE), 0) >= 1:
        return "template_locality"
    if req.length <= SMALL_READ:
        return "s3a_random"
    return "s3a_prefetch"


class Hysteresis:
    """1:1 port of StatisticalPolicySelector._apply_hysteresis / Java Hysteresis."""

    def __init__(self, threshold: int = 3) -> None:
        self.threshold = threshold
        self.current: str | None = None
        self.pending: str | None = None
        self.pending_n = 0

    def apply(self, label: str) -> str:
        if self.current is None:
            self.current = label
            return self.current
        if label == self.current:
            self.pending = None
            self.pending_n = 0
            return self.current
        if label == self.pending:
            self.pending_n += 1
        else:
            self.pending = label
            self.pending_n = 1
        if self.pending_n >= self.threshold:
            self.current = label
            self.pending = None
            self.pending_n = 0
        return self.current


class TreePredictor:
    """Decision tree with the simulator's quantized prediction cache (per-read sklearn is slow)."""

    def __init__(self, model) -> None:
        self.model = model
        self.cache: dict[tuple[int, ...], str] = {}

    def predict(self, feats: np.ndarray) -> str:
        key = tuple(int(round(v * 100)) for v in feats)
        label = self.cache.get(key)
        if label is None:
            label = str(self.model.predict(feats.reshape(1, -1))[0])
            self.cache[key] = label
        return label


def oracle_for(req: Request, fixed: str | None) -> str | None:
    if fixed is not None:
        return fixed
    # mixed_holdout: "<class>:<source>:<orig>"
    parts = req.query_id.split(":")
    return SOURCE_ORACLE.get(parts[1]) if len(parts) >= 2 else None


def evaluate(path: str, fixed_oracle: str | None, tree: TreePredictor,
             max_reads: int) -> dict:
    df = load_trace([path])
    history: deque[Request] = deque(maxlen=HISTORY_WINDOW)
    # Separate per-object state per rule variant: the java variant advances page counts on
    # every read, the sim variant only inside its locality branch, so they must not share.
    state_java: dict[str, ObjState] = {}
    state_sim: dict[str, ObjState] = {}
    hyst = Hysteresis(3)

    hits = Counter()
    chosen: dict[str, Counter] = {s: Counter() for s in SELECTORS}
    # What ta(java) picks when it MISSES the oracle.
    ta_mistakes = Counter()
    total = 0
    oracle_counts = Counter()

    for row in df.itertuples(index=False):
        req = row_to_request(row)
        oracle = oracle_for(req, fixed_oracle)
        if oracle is None:
            continue

        obj_j = state_java.setdefault(req.object_key, ObjState())
        obj_s = state_sim.setdefault(req.object_key, ObjState())

        picks = {
            "ta(java)": rule_choice(obj_j, req, leak_query_id=False),
            "ta(sim)": rule_choice(obj_s, req, leak_query_id=True),
        }
        feats = extract_features(list(history), req)
        raw = tree.predict(feats)
        picks["tree(raw)"] = raw
        picks["tree+hyst"] = hyst.apply(raw)

        for name, pick in picks.items():
            chosen[name][pick] += 1
            if pick == oracle:
                hits[name] += 1
        if picks["ta(java)"] != oracle:
            ta_mistakes[picks["ta(java)"]] += 1
        oracle_counts[oracle] += 1
        total += 1

        # advance state AFTER the decision (matches both implementations)
        page = align_down(req.offset, LOCALITY_PAGE)
        obj_j.recent.append(req)
        obj_j.seen_pages[page] = obj_j.seen_pages.get(page, 0) + 1
        obj_s.recent.append(req)
        if picks["ta(sim)"] == "template_locality":
            obj_s.seen_pages[page] = obj_s.seen_pages.get(page, 0) + 1
        history.append(req)

        if max_reads and total >= max_reads:
            break

    return {
        "reads": total,
        "agreement": {s: (hits[s] / total if total else float("nan")) for s in SELECTORS},
        "chosen": {s: dict(chosen[s]) for s in SELECTORS},
        "ta_java_mistakes": dict(ta_mistakes),
        "oracle_counts": dict(oracle_counts),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Oracle-agreement of hand rule vs learned tree")
    ap.add_argument("--model", default="models/policy_selector.joblib")
    ap.add_argument("--max-reads", type=int, default=20000,
                    help="cap reads per trace (contiguous prefix; 0 = all). Traces are "
                         "single-workload so a prefix is representative; mixed is < cap.")
    ap.add_argument("--json", default="access_report/policy_agreement.json")
    args = ap.parse_args()

    import joblib

    bundle = joblib.load(args.model)
    print(f"model: {args.model} type={bundle.get('model_type')} labels={bundle.get('labels')}\n")

    groups: list[tuple[str, list[tuple[str, str | None, str]]]] = [
        ("in-sample", [(p, lab, tag) for p, lab, tag in IN_SAMPLE]),
        ("held-out", [(p, lab, tag) for p, lab, tag in HELD_OUT]
         + [(MIXED_TRACE, None, "mixed_holdout")]),
    ]

    results: dict[str, dict] = {}
    print(f"{'group':10s} {'trace':16s} {'oracle':20s} {'reads':>7s}"
          + "".join(f" {s:>10s}" for s in SELECTORS))
    print("-" * (10 + 1 + 16 + 1 + 20 + 8 + 11 * len(SELECTORS)))

    for group, entries in groups:
        for path, fixed, tag in entries:
            if not os.path.exists(path):
                print(f"{group:10s} {tag:16s} {'(missing)':20s}")
                continue
            tree = TreePredictor(bundle["model"])  # fresh cache/state per trace
            res = evaluate(path, fixed, tree, args.max_reads)
            res["group"] = group
            res["trace"] = path
            res["oracle"] = fixed or "per-row(source)"
            results[tag] = res
            print(f"{group:10s} {tag:16s} {res['oracle']:20s} {res['reads']:7d}"
                  + "".join(f" {res['agreement'][s]:9.1%}" for s in SELECTORS))

    # Macro (per-trace unweighted) and micro (read-weighted) summaries per group.
    summary: dict[str, dict] = {}
    print()
    for group, _ in groups:
        rs = [r for r in results.values() if r["group"] == group]
        if not rs:
            continue
        reads = sum(r["reads"] for r in rs)
        macro = {s: float(np.mean([r["agreement"][s] for r in rs])) for s in SELECTORS}
        micro = {s: sum(r["agreement"][s] * r["reads"] for r in rs) / reads for s in SELECTORS}
        summary[group] = {"traces": len(rs), "reads": reads, "macro": macro, "micro": micro}
        print(f"{group}: {len(rs)} traces, {reads} reads")
        print("  macro (per-trace avg): "
              + "  ".join(f"{s}={macro[s]:.1%}" for s in SELECTORS))
        print("  micro (read-weighted): "
              + "  ".join(f"{s}={micro[s]:.1%}" for s in SELECTORS))

    # The headroom question the user actually asked.
    if "held-out" in summary:
        ta = summary["held-out"]["micro"]["ta(java)"]
        tr = summary["held-out"]["micro"]["tree+hyst"]
        print(f"\nheadroom (held-out, read-weighted):")
        print(f"  hand rule ta(java) hits the best policy on {ta:.1%} of reads"
              f"  -> at most {1 - ta:.1%} of reads could be improved by ANY better brain")
        print(f"  the deployed tree already hits {tr:.1%} ({tr - ta:+.1%} vs hand rule)")
        print("  => if this gap is small, changing MODEL CLASS is not where the win is;")
        print("     look at executor params / cache-budget / prefetch shape instead.")

    print("\nta(java) choices when it MISSES the oracle (held-out traces):")
    for tag, r in results.items():
        if r["group"] != "held-out" or not r["ta_java_mistakes"]:
            continue
        top = sorted(r["ta_java_mistakes"].items(), key=lambda kv: -kv[1])[:4]
        miss = sum(r["ta_java_mistakes"].values())
        print(f"  {tag:16s} misses={miss:6d}  "
              + "  ".join(f"{k}={v}" for k, v in top))

    print("\nNOTE: oracle is CONSTANT per workload, so agreement = 'converged to this "
          "workload's best policy',\n      not 'this read was routed optimally'. "
          "in-sample rows are biased (the tree trained on them).")

    os.makedirs(os.path.dirname(os.path.abspath(args.json)), exist_ok=True)
    with open(args.json, "w") as f:
        json.dump({"model": args.model, "max_reads": args.max_reads,
                   "per_trace": results, "summary": summary}, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"\nwrote {args.json}")


if __name__ == "__main__":
    main()
