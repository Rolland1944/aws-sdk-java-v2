#!/usr/bin/env python3
"""Fit object-store system constants (RTT, BW, K) from SdkIoCollector NDJSON.

Contract role (TRACK2_M0_CONTRACT.md r4 / what-if L1): the cost model is
    t_io = Σ_req (RTT + bytes/BW) / K
RTT is a first-class input so the same advisor can rank layouts under the
measured cross-cloud regime (~228 ms) and the contracted same-region regime
(Track 1, ~25 ms). This module is the only place those numbers are fitted.

K is average in-flight concurrency during IO-busy time:
    K = sum(request latency) / union(in-flight intervals)
It is NOT Spark's local[N]. Peak inflight can be ~N × vectored-active-reads
while the busy-time average is much lower, because footer/HEAD work and
small-file scans occupy long stretches with 1–2 requests in flight.

Usage:
  python3 tools/track2/sysconst.py \
      --io docs/adaptive-range-reader/results/track2/e2_baseline/io/*.ndjson \
      --runs 5 \
      --out docs/adaptive-range-reader/results/track2/e5_whatif/sysconst.json
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import statistics as st
from collections import Counter, defaultdict
from datetime import datetime, timezone


BUCKETS = (
    ("<64KiB", 0, 65536),
    ("64K-1M", 65536, 1048576),
    ("1-8M", 1048576, 8388608),
    (">=8M", 8388608, 1 << 62),
)

# Track 1 same-region measurement (contract D-8 / r4 sensitivity regime).
SAME_REGION_RTT_S = 0.025


def _bucket(length):
    for name, lo, hi in BUCKETS:
        if lo <= length < hi:
            return name
    return BUCKETS[-1][0]


def load_records(patterns):
    records = []
    files = []
    for pattern in patterns:
        files.extend(sorted(glob.glob(pattern)))
    for path in files:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                records.append(rec)
    return records, files


def _median(xs):
    return st.median(xs) if xs else None


def _pctl(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    i = min(len(xs) - 1, max(0, int(round(p * (len(xs) - 1)))))
    return xs[i]


def union_busy_ns(intervals):
    """Length of the union of [start, end) intervals, plus the span."""
    if not intervals:
        return 0, 0
    intervals = sorted(intervals)
    span = intervals[-1][1] - intervals[0][0]
    union = 0
    cs, ce = intervals[0]
    for s, e in intervals[1:]:
        if s > ce:
            union += ce - cs
            cs, ce = s, e
        else:
            ce = max(ce, e)
    union += ce - cs
    return union, span


def fit_rtt_bw(ranged):
    """Robust RTT/BW from size-bucket medians, not a least-squares fit.

    Small GETs are RTT-dominated; the <64KiB median latency is RTT.
    Large GETs pay RTT + size/BW; subtract the small-GET median to isolate BW.
    """
    by_bucket = defaultdict(list)
    for rec in ranged:
        length = rec["range_length"]
        lat_s = rec["latency_ns"] / 1e9
        by_bucket[_bucket(length)].append((lat_s, length))

    bucket_stats = {}
    for name, lo, hi in BUCKETS:
        pairs = by_bucket.get(name, [])
        if not pairs:
            continue
        lats = [p[0] for p in pairs]
        lens = [p[1] for p in pairs]
        bucket_stats[name] = {
            "n": len(pairs),
            "median_latency_s": _median(lats),
            "p90_latency_s": _pctl(lats, 0.9),
            "median_bytes": _median(lens),
            "mean_bytes": st.mean(lens),
        }

    small = bucket_stats.get("<64KiB")
    large = bucket_stats.get(">=8M") or bucket_stats.get("1-8M")
    rtt_s = small["median_latency_s"] if small else None
    bw_bps = None
    if small and large and large["median_latency_s"] > small["median_latency_s"]:
        d_bytes = large["median_bytes"] - small["median_bytes"]
        d_s = large["median_latency_s"] - small["median_latency_s"]
        if d_s > 0 and d_bytes > 0:
            bw_bps = d_bytes / d_s
    return rtt_s, bw_bps, bucket_stats


def concurrency_report(records):
    """Explain why busy-time K is far below local[N]."""
    intervals = []
    by_op = Counter()
    by_thread_kind = Counter()
    inflight = []
    tiny_get = 0
    data_get = 0
    head = 0
    ranged = []
    high_inflight_intervals = []
    meta_intervals = []
    data_intervals = []

    for rec in records:
        start, end = rec.get("ts_start_ns"), rec.get("ts_end_ns")
        if not start or not end or end <= start:
            continue
        intervals.append((start, end))
        op = rec.get("audit_op") or rec.get("method") or "?"
        by_op[op] += 1
        thread = rec.get("thread") or ""
        if "readingParquetFooters" in thread:
            kind = "footer_thread"
        elif "Executor task launch" in thread:
            kind = "task"
        elif "s3a-transfer" in thread:
            kind = "s3a_transfer"
        elif "checkPathsExist" in thread or "ForkJoinPool" in thread:
            kind = "listing"
        else:
            kind = "other"
        by_thread_kind[kind] += 1
        inf = rec.get("inflight_at_issue") or 0
        inflight.append(inf)
        method = rec.get("method")
        length = rec.get("range_length") or 0
        if method == "HEAD":
            head += 1
            meta_intervals.append((start, end))
        elif method == "GET" and length:
            ranged.append(rec)
            if length < 65536:
                tiny_get += 1
                meta_intervals.append((start, end))
            else:
                data_get += 1
                data_intervals.append((start, end))
        if inf >= 16:
            high_inflight_intervals.append((start, end))

    union, span = union_busy_ns(intervals)
    lat_sum = sum(e - s for s, e in intervals)
    k_busy = (lat_sum / union) if union else None
    meta_union, _ = union_busy_ns(meta_intervals)
    data_union, _ = union_busy_ns(data_intervals)
    high_union, _ = union_busy_ns(high_inflight_intervals)
    meta_lat = sum(e - s for s, e in meta_intervals)
    data_lat = sum(e - s for s, e in data_intervals)

    k_meta = (meta_lat / meta_union) if meta_union else None
    k_data = (data_lat / data_union) if data_union else None

    return {
        "n_records": len(records),
        "n_head": head,
        "n_ranged_get_tiny": tiny_get,
        "n_ranged_get_data": data_get,
        "wall_span_s": span / 1e9 if span else None,
        "union_busy_s": union / 1e9 if union else None,
        "busy_fraction_of_span": (union / span) if span else None,
        "sum_latency_s": lat_sum / 1e9,
        "K_busy": k_busy,
        "K_metadata": k_meta,
        "K_data": k_data,
        "inflight_p50": _median(inflight),
        "inflight_p90": _pctl(inflight, 0.9),
        "inflight_max": max(inflight) if inflight else None,
        "high_inflight_union_s": high_union / 1e9 if high_union else 0.0,
        "high_inflight_fraction_of_busy": (high_union / union) if union else None,
        "ops": dict(by_op.most_common()),
        "thread_kinds": dict(by_thread_kind),
        "explanation": (
            "K_busy is average concurrency only while at least one request is in "
            "flight, so inter-query and inter-run idle time is already excluded. "
            "It is still far below local[16] because most of the busy union is "
            "metadata: HEAD + sub-64KiB footer/page-index GETs, which run at "
            "K_metadata ≈ 1–4. Peak inflight (~60) occurs only during vectored "
            "column-chunk scans (K_data, local[16] × up to 4 ranged reads). "
            "L1 must use K_data for chunk transfers and K_metadata for per-file "
            "open cost; a single global K=6.8 would mix the two regimes."
        ),
    }


def per_run_slice(records, n_runs):
    """NDJSON is cumulative across runs. Split into n_runs equal-count chunks
    only as a fallback; prefer timestamp gaps if they exist."""
    if n_runs <= 1 or not records:
        return [records]
    dated = [r for r in records if r.get("ts_start_ns")]
    dated.sort(key=lambda r: r["ts_start_ns"])
    # large gaps between runs (JVM restart): split on gaps > 30s
    gaps = []
    for i in range(1, len(dated)):
        dt = dated[i]["ts_start_ns"] - dated[i - 1]["ts_end_ns"]
        if dt > 30e9:
            gaps.append(i)
    if len(gaps) == n_runs - 1:
        bounds = [0] + gaps + [len(dated)]
        return [dated[bounds[i]:bounds[i + 1]] for i in range(n_runs)]
    # equal count fallback
    chunk = len(dated) // n_runs
    return [dated[i * chunk:(i + 1) * chunk if i < n_runs - 1 else len(dated)]
            for i in range(n_runs)]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--io", nargs="+", required=True)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    records, files = load_records(args.io)
    if not records:
        raise SystemExit("no IO records")

    ranged = [r for r in records
              if r.get("method") == "GET" and r.get("range_length")
              and r.get("latency_ns")]
    rtt_s, bw_bps, buckets = fit_rtt_bw(ranged)
    conc = concurrency_report(records)

    slices = per_run_slice(records, args.runs)
    per_run = []
    for i, sl in enumerate(slices, 1):
        n_head = sum(1 for r in sl if r.get("method") == "HEAD")
        n_get = sum(1 for r in sl if r.get("method") == "GET" and r.get("range_length"))
        nbytes = sum((r.get("range_length") or 0) for r in sl
                     if r.get("method") == "GET" and r.get("range_length"))
        per_run.append({
            "run": i,
            "n_records": len(sl),
            "n_head": n_head,
            "n_ranged_get": n_get,
            "remote_bytes": nbytes,
            "remote_gib": nbytes / 2 ** 30,
        })

    measured = {
        "name": "measured_cross_cloud",
        "rtt_s": rtt_s,
        "bw_bps": bw_bps,
        "bw_mib_s": (bw_bps / 2 ** 20) if bw_bps else None,
        "K_metadata": conc["K_metadata"],
        "K_data": conc["K_data"],
        "K_busy": conc["K_busy"],
        "note": "Fitted from E2 NDJSON on the Tencent Cloud VM (contract r4).",
    }
    same_region = {
        "name": "same_region_m5d",
        "rtt_s": SAME_REGION_RTT_S,
        "bw_bps": bw_bps,
        "bw_mib_s": (bw_bps / 2 ** 20) if bw_bps else None,
        "K_metadata": conc["K_metadata"],
        "K_data": conc["K_data"],
        "K_busy": conc["K_busy"],
        "note": (
            "RTT replaced with Track 1 same-region ~25 ms; BW/K kept from this "
            "host until E12 re-fits them on m5d.4xlarge. Sensitivity only — not "
            "a gate number (contract r4 / E12)."
        ),
    }

    out = {
        "fitted_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md r4",
        "io_files": files,
        "n_runs": args.runs,
        "regimes": {"measured_cross_cloud": measured, "same_region_m5d": same_region},
        "default_regime": "measured_cross_cloud",
        "size_buckets": buckets,
        "concurrency": conc,
        "per_run": per_run,
        "vectored": {
            "min_seek_bytes": 131072,
            "max_merged_bytes": 2097152,
            "active_ranged_reads": 4,
            "source": "s3a_session.apply_frozen_reader (contract 1.4)",
        },
    }

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(out, fh, indent=2)

    print("# sysconst")
    print(f"  records          {len(records)}")
    print(f"  RTT measured     {rtt_s*1e3:.1f} ms" if rtt_s else "  RTT  n/a")
    print(f"  BW               {bw_bps/2**20:.1f} MiB/s" if bw_bps else "  BW   n/a")
    print(f"  K_busy           {conc['K_busy']:.2f}")
    print(f"  K_metadata       {conc['K_metadata']:.2f}" if conc["K_metadata"] else "")
    print(f"  K_data           {conc['K_data']:.2f}" if conc["K_data"] else "")
    print(f"  inflight p50/max {conc['inflight_p50']}/{conc['inflight_max']}")
    print(f"  high-inflight %  {100*(conc['high_inflight_fraction_of_busy'] or 0):.1f}% of busy")
    if per_run:
        print(f"  per-run GETs     {[r['n_ranged_get'] for r in per_run]}")
        print(f"  per-run GiB      {[round(r['remote_gib'], 2) for r in per_run]}")
    if args.out:
        print(f"  out              {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
