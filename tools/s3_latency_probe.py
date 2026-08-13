#!/usr/bin/env python3
"""
E1 scout: bare S3 latency / bandwidth / concurrency probe (no SDK adaptive layer).

Uses an existing object in the bucket (default: a tpch300 lineitem part). Writes a
JSON summary + optional raw CSV under --out-dir.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config


def pct(xs, p):
    if not xs:
        return None
    ys = sorted(xs)
    # nearest-rank, matches AdaptiveReaderSystemBenchmark.percentileNanos
    rank = max(1, int(__import__("math").ceil(p / 100.0 * len(ys))))
    return ys[min(len(ys) - 1, rank - 1)]


def get_range(client, bucket, key, offset, length):
    t0 = time.perf_counter()
    resp = client.get_object(
        Bucket=bucket,
        Key=key,
        Range=f"bytes={offset}-{offset + length - 1}",
    )
    body = resp["Body"].read()
    t1 = time.perf_counter()
    if len(body) != length:
        raise RuntimeError(f"short read: got {len(body)} want {length}")
    return t1 - t0


def size_sweep(client, bucket, key, sizes, n, offset_base):
    rows = []
    summary = []
    for size in sizes:
        lats = []
        for i in range(n):
            # stride offsets to avoid serving entirely from the same hot page
            off = offset_base + (i * max(size, 4096)) % (64 * 1024 * 1024)
            lat = get_range(client, bucket, key, off, size)
            lats.append(lat)
            rows.append({"phase": "size", "size": size, "i": i, "latency_s": lat})
        rtt_proxy = pct(lats, 50)
        summary.append(
            {
                "size": size,
                "n": n,
                "p50_s": pct(lats, 50),
                "p95_s": pct(lats, 95),
                "p99_s": pct(lats, 99),
                "p999_s": pct(lats, 99.9) if n >= 200 else None,
                "mean_s": statistics.fmean(lats),
                "p99_over_p50": (pct(lats, 99) / pct(lats, 50)) if pct(lats, 50) else None,
            }
        )
        print(
            f"[size] {size:>10} B  n={n}  p50={summary[-1]['p50_s']*1e3:.2f}ms  "
            f"p99={summary[-1]['p99_s']*1e3:.2f}ms  "
            f"p99/p50={summary[-1]['p99_over_p50']:.2f}",
            flush=True,
        )
    return rows, summary


def fit_rtt_bw(size_summary):
    """latency ≈ a + size/b  via two-point fit on p50 of smallest & largest sizes."""
    pts = sorted(
        [(s["size"], s["p50_s"]) for s in size_summary if s["p50_s"] and s["size"] >= 64 * 1024],
        key=lambda x: x[0],
    )
    if len(pts) < 2:
        return None
    (s0, t0), (s1, t1) = pts[0], pts[-1]
    if s1 == s0 or t1 <= t0:
        return {"rtt_s": t0, "bw_Bps": None, "g_star_B": None, "note": "degenerate fit"}
    bw = (s1 - s0) / (t1 - t0)
    rtt = t0 - s0 / bw
    if rtt < 0:
        rtt = min(t0, t1) * 0.5
    g_star = rtt * bw
    return {
        "rtt_s": rtt,
        "bw_Bps": bw,
        "bw_MiBps": bw / (1024 * 1024),
        "g_star_B": g_star,
        "g_star_MiB": g_star / (1024 * 1024),
        "fit_points": [{"size": s0, "p50_s": t0}, {"size": s1, "p50_s": t1}],
    }


def concurrency_sweep(client_factory, bucket, key, concs, per_worker, size):
    rows = []
    summary = []
    for c in concs:
        lats = []
        t_wall0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=c) as pool:
            futs = []
            for i in range(c * per_worker):
                client = client_factory()
                off = (i * size) % (64 * 1024 * 1024)
                futs.append(pool.submit(get_range, client, bucket, key, off, size))
            for fut in as_completed(futs):
                lat = fut.result()
                lats.append(lat)
                rows.append({"phase": "conc", "concurrency": c, "size": size, "latency_s": lat})
        wall = time.perf_counter() - t_wall0
        total_bytes = len(lats) * size
        summary.append(
            {
                "concurrency": c,
                "n": len(lats),
                "size": size,
                "wall_s": wall,
                "agg_MiBps": (total_bytes / wall) / (1024 * 1024),
                "p50_s": pct(lats, 50),
                "p95_s": pct(lats, 95),
                "p99_s": pct(lats, 99),
                "p99_over_p50": (pct(lats, 99) / pct(lats, 50)) if pct(lats, 50) else None,
            }
        )
        print(
            f"[conc] c={c:<3} size={size}  wall={wall:.1f}s  "
            f"agg={summary[-1]['agg_MiBps']:.1f} MiB/s  "
            f"p50={summary[-1]['p50_s']*1e3:.2f}ms  p99={summary[-1]['p99_s']*1e3:.2f}ms",
            flush=True,
        )
    return rows, summary


def hedge_upper_bound(lats, trigger_q=0.95):
    """Offline: if a second independent sample from the same dist is issued at
    trigger_q quantile wait, take min(original, hedge). Extra request rate = P(lat>trigger)."""
    if len(lats) < 50:
        return None
    ys = sorted(lats)
    trigger = pct(ys, int(trigger_q * 100))
    # simulate: for each sample, with prob p_slow we'd have waited trigger and drawn a twin
    import random

    rng = random.Random(0)
    improved = []
    extras = 0
    for lat in ys:
        if lat > trigger:
            extras += 1
            twin = ys[rng.randrange(len(ys))]
            # hedge issued after `trigger` wait; completion = trigger + twin
            improved.append(min(lat, trigger + twin))
        else:
            improved.append(lat)
    return {
        "trigger_q": trigger_q,
        "trigger_s": trigger,
        "extra_request_rate": extras / len(ys),
        "orig_p99_s": pct(ys, 99),
        "hedged_p99_s": pct(improved, 99),
        "p99_improvement": 1.0 - pct(improved, 99) / pct(ys, 99) if pct(ys, 99) else None,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", default="home-haoyue")
    ap.add_argument("--key", default="tpch300/lineitem/part-0000.parquet")
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--out-dir", default="docs/adaptive-range-reader/results/aws-s3_scout_e1")
    ap.add_argument("--n-per-size", type=int, default=300)
    ap.add_argument("--sizes", default="4096,65536,1048576,16777216")
    ap.add_argument("--concs", default="1,4,16,64")
    ap.add_argument("--conc-size", type=int, default=1048576)
    ap.add_argument("--conc-per-worker", type=int, default=20)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    sizes = [int(x) for x in args.sizes.split(",") if x.strip()]
    concs = [int(x) for x in args.concs.split(",") if x.strip()]

    def client_factory():
        return boto3.client(
            "s3",
            region_name=args.region,
            config=Config(max_pool_connections=128, retries={"max_attempts": 3}),
        )

    client = client_factory()
    head = client.head_object(Bucket=args.bucket, Key=args.key)
    obj_size = head["ContentLength"]
    print(f"probe object s3://{args.bucket}/{args.key} size={obj_size}", flush=True)

    all_rows = []
    size_rows, size_summary = size_sweep(client, args.bucket, args.key, sizes, args.n_per_size, 0)
    all_rows.extend(size_rows)
    fit = fit_rtt_bw(size_summary)
    print(f"[fit] {json.dumps(fit, indent=2)}", flush=True)

    # use 1MiB latencies for hedge sim if present
    one_mib = [r["latency_s"] for r in size_rows if r["size"] == 1048576]
    hedges = {
        f"p{int(q*100)}": hedge_upper_bound(one_mib, q) for q in (0.90, 0.95, 0.99)
    }

    conc_rows, conc_summary = concurrency_sweep(
        client_factory, args.bucket, args.key, concs, args.conc_per_worker, args.conc_size
    )
    all_rows.extend(conc_rows)

    csv_path = os.path.join(args.out_dir, "raw_latencies.csv")
    with open(csv_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=sorted({k for r in all_rows for k in r}))
        w.writeheader()
        w.writerows(all_rows)

    report = {
        "bucket": args.bucket,
        "key": args.key,
        "region": args.region,
        "object_size": obj_size,
        "size_summary": size_summary,
        "rtt_bw_fit": fit,
        "hedge_upper_bound_1MiB": hedges,
        "concurrency_summary": conc_summary,
        "d5_gate": {
            "p99_over_p50_1MiB": next(
                (s["p99_over_p50"] for s in size_summary if s["size"] == 1048576), None
            ),
            "keep_d5_if_gt_3": None,
        },
    }
    p = report["d5_gate"]["p99_over_p50_1MiB"]
    report["d5_gate"]["keep_d5_if_gt_3"] = (p is not None and p > 3.0)

    out_json = os.path.join(args.out_dir, "e1_summary.json")
    with open(out_json, "w") as fh:
        json.dump(report, fh, indent=2)
    print(f"\nWrote {out_json}")
    print(f"Wrote {csv_path}")
    print(f"D5 keep (p99/p50>3 on 1MiB)? {report['d5_gate']['keep_d5_if_gt_3']} "
          f"(ratio={p})")


if __name__ == "__main__":
    main()
