#!/usr/bin/env python3
"""Generate golden vectors for the Java <-> Python inference consistency test.

PROJECT2 7.3 S1 step 3: emit >=1000 (request-stream -> features -> label ->
hysteresis-current) samples so the Java port of extract_features +
DecisionTreePolicySelector can be checked for:

  * feature parity (within tolerance), and
  * predicted label + post-hysteresis current-policy 100% agreement.

Faithfulness rules:
  * Features/labels come from the SAME code path the model was trained/served
    with: prefetch_simulator.extract_features over a bounded 64-read rolling
    window, and the raw DecisionTreeClassifier.predict (NOT the selector's
    quantized prediction cache -> no rounding drift).
  * Each "segment" is a contiguous read stream replayed from an EMPTY window;
    Java resets its FeatureWindow + hysteresis per segment, so both sides see
    identical inputs and reset points and must agree exactly.
  * Real traces (already in traces/) drive the bulk; a synthetic "mixed"
    segment forces workload switches (hysteresis), and a hand-built "edge"
    segment covers boundaries real traces may miss.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from prefetch_simulator import (  # noqa: E402
    FEATURE_NAMES,
    Request,
    extract_features,
    load_trace,
    row_to_request,
)

WINDOW = 64
HYSTERESIS = 3

TRAINING_TRACES = [
    ("tpch", "traces/tpch_sf1_full.csv"),
    ("ml_epoch_scan", "traces/ml_taxi_epoch.csv"),
    ("lance_large", "traces/lance_sift1m_real/lance_all.csv"),
    ("lance_small", "traces/lance_fmnist_real/lance_all.csv"),
    ("ml_embedding", "traces/ml_emb_real.csv"),
    ("multimodal", "traces/mm_n2000/retrieval.csv"),
]


class Hysteresis:
    """Exact port of StatisticalPolicySelector._apply_hysteresis."""

    def __init__(self, k: int = HYSTERESIS) -> None:
        self.current: str | None = None
        self.pending: str | None = None
        self.pending_n = 0
        self.k = k

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
        if self.pending_n >= self.k:
            self.current = label
            self.pending = None
            self.pending_n = 0
        return self.current


def _file_size_json(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def replay_segment(name: str, requests: list[Request], model) -> dict:
    """Roll the 64-window over `requests`, emit features/label/current each read."""
    history: list[Request] = []
    feats_matrix = []
    for req in requests:
        feats_matrix.append(extract_features(history[-WINDOW:], req))
        history.append(req)

    if feats_matrix:
        labels = [str(v) for v in model.predict(np.asarray(feats_matrix, dtype=float))]
    else:
        labels = []

    hyst = Hysteresis()
    reads = []
    for req, feats, label in zip(requests, feats_matrix, labels):
        current = hyst.apply(label)
        reads.append(
            {
                "object_key": req.object_key,
                "offset": int(req.offset),
                "length": int(req.length),
                "file_size": _file_size_json(req.file_size),
                "expected_features": [float(x) for x in feats],
                "expected_label": label,
                "expected_current": current,
            }
        )
    return {"name": name, "reads": reads}


def trace_requests(path: str, limit: int) -> list[Request]:
    df = load_trace([path])
    if limit and len(df) > limit:
        df = df.iloc[:limit]
    return [row_to_request(row) for row in df.itertuples(index=False)]


def build_mixed(chunk: int) -> list[Request]:
    """Concatenate a small prefix of each trace -> forces workload switches."""
    reqs: list[Request] = []
    for _, path in TRAINING_TRACES:
        reqs.extend(trace_requests(path, chunk))
    return reqs


def build_edge_cases() -> list[Request]:
    kb = 1024
    small = 16 * kb  # frac_small boundary (<=)
    large = 128 * kb  # frac_large boundary (>=)
    seq_gap = 64 * kb  # sequentiality boundary (<=)
    page = 256 * kb  # page-revisit granularity
    t = 0.0
    reqs: list[Request] = []

    def add(obj, off, length, fsize):
        nonlocal t
        t += 1e-3
        reqs.append(Request(timestamp=t, object_key=obj, offset=off, length=length, file_size=fsize))

    # First read (empty history), file_size None -> size_ratio 0.
    add("edge/a", 0, 4 * kb, None)
    # Exact small boundary, valid file_size -> size_ratio branch.
    add("edge/a", 4 * kb, small, 1_000_000)
    # Exact large boundary.
    add("edge/a", 100 * kb, large, 1_000_000)
    # Sequential: gap exactly seq_gap (still counts as sequential, <=).
    prev_end = 100 * kb + large
    add("edge/a", prev_end + seq_gap, 8 * kb, 1_000_000)
    # Backward jump (forward_ratio drops).
    add("edge/a", 0, 8 * kb, 1_000_000)
    # Page revisit: two reads into the same 256KB page on a fresh object.
    add("edge/b", page * 3 + 10, 512, 5_000_000)
    add("edge/b", page * 3 + 2000, 512, 5_000_000)
    # file_size == 0 -> treated invalid -> size_ratio 0.
    add("edge/b", page * 5, 2 * kb, 0)
    # length larger than file_size -> size_ratio clamps to 1.0.
    add("edge/c", 0, 10 * kb, 4 * kb)
    # A run of tiny reads across many distinct objects (distinct_obj_ratio high).
    for i in range(6):
        add(f"edge/obj{i}", i * 512, 384, 200_000)
    # A run of big sequential reads (sequentiality + frac_large high).
    base = 0
    for i in range(6):
        add("edge/seqobj", base, 512 * kb, 50_000_000)
        base += 512 * kb
    return reqs


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate Java<->Python golden vectors")
    ap.add_argument("--model", default="models/policy_selector.joblib")
    ap.add_argument("--tree-json", default="models/policy_selector_v1.json")
    ap.add_argument(
        "--out",
        default="services-custom/s3-adaptive-range-reader/src/test/resources/golden/policy_golden_v1.json",
    )
    ap.add_argument("--per-trace", type=int, default=300)
    ap.add_argument("--mixed-chunk", type=int, default=90)
    args = ap.parse_args()

    import warnings

    import joblib

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        bundle = joblib.load(args.model)
    model = bundle["model"]

    model_checksum = None
    if os.path.exists(args.tree_json):
        with open(args.tree_json) as f:
            model_checksum = json.load(f).get("sha256_checksum")

    segments = []
    for name, path in TRAINING_TRACES:
        reqs = trace_requests(path, args.per_trace)
        segments.append(replay_segment(name, reqs, model))
        print(f"  segment {name:16s} reads={len(reqs)}")

    mixed = build_mixed(args.mixed_chunk)
    segments.append(replay_segment("mixed_switch", mixed, model))
    print(f"  segment {'mixed_switch':16s} reads={len(mixed)}")

    edge = build_edge_cases()
    segments.append(replay_segment("edge_cases", edge, model))
    print(f"  segment {'edge_cases':16s} reads={len(edge)}")

    total = sum(len(s["reads"]) for s in segments)
    payload = {
        "schema_version": 1,
        "feature_version": "v1",
        "model_checksum": model_checksum,
        "feature_names": list(FEATURE_NAMES),
        "window": WINDOW,
        "hysteresis": HYSTERESIS,
        "total_reads": total,
        "segments": segments,
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(payload, f, indent=1)
        f.write("\n")

    switches = 0
    for s in segments:
        prev = None
        for r in s["reads"]:
            if prev is not None and r["expected_current"] != prev:
                switches += 1
            prev = r["expected_current"]
    print(f"wrote {args.out}")
    print(f"  segments={len(segments)} total_reads={total} hysteresis_switches={switches}")
    if total < 1000:
        raise SystemExit(f"golden too small: {total} < 1000")


if __name__ == "__main__":
    main()
