#!/usr/bin/env python3
"""Criteo-CTR-FE-v1: one-shot deterministic profile of the Criteo click-log layout.

This is *not* part of the measured 2x2 workload. It runs once, before the query
generator, and answers the questions the generator must not invent:

  * how many rows exist per day, so the Train/Validation/Test day split is
    frozen against real counts instead of a guess;
  * the fixed quantiles of the 13 integer features, so the bucket edges in the
    final SQL are measured constants, not hand-picked round numbers;
  * the approximate cardinality of the 26 categorical features, so the
    low / mid / high representatives are chosen by a deterministic rule
    (sort by cardinality, tie-break on column index) instead of by whoever
    writes the query file;
  * the Top-K values of those representatives, as frozen SQL constants;
  * a schema SHA-256 and the object inventory, so a later run can prove it is
    reading the same dataset.

Nothing here looks at any measured latency, so the profile cannot leak Track1
or Track2 performance into the choice of queries.

Usage:
  python3 tools/track2/criteo_profile.py \
      --data s3a://home-haoyue/track2/criteo_clicklogs --table criteo \
      --out /mnt/ssd-cache/criteo_local/work/criteo_profile.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import s3a_session  # noqa: E402

DEFAULT_DATA = "s3a://home-haoyue/track2/criteo_clicklogs"
DEFAULT_TABLE = "criteo"
INT_COLS = [f"integer_feature_{i}" for i in range(1, 14)]
CAT_COLS = [f"categorical_feature_{i}" for i in range(1, 27)]
# Fixed, declared once. The generator writes these same points into the SQL
# comments so a reader knows what "median bucket" meant.
QUANTILES = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
# Bucket edges for the integer bucketing query: quartiles, three edges,
# four buckets. Chosen here so no one picks them while looking at results.
BUCKET_QUANTILES = [0.25, 0.5, 0.75]
DEFAULT_TOPK = 20
PROFILE_VERSION = "criteo-ctr-fe-v1-profile-1"


def object_inventory(uri, region):
    """Object count and bytes of the table, straight from S3, no Spark."""
    import pyarrow.fs as pfs

    path = uri.replace("s3a://", "").replace("s3://", "").rstrip("/")
    fs = pfs.S3FileSystem(region=region)
    infos = fs.get_file_info(pfs.FileSelector(path, recursive=True))
    files = [i for i in infos if i.is_file and i.path.endswith(".parquet")]
    per_day = {}
    for f in files:
        parts = f.path.split("/")
        day = next((p for p in parts if p.startswith("day=")), "unknown")
        d = per_day.setdefault(day, {"files": 0, "bytes": 0})
        d["files"] += 1
        d["bytes"] += f.size
    return {
        "n_files": len(files),
        "total_bytes": sum(f.size for f in files),
        "per_day": {k: per_day[k] for k in sorted(per_day)},
    }


def schema_signature(spark, table):
    """SHA-256 over column index/name/type, so a re-run can prove same schema."""
    cols = spark.sql(f"SELECT * FROM {table} LIMIT 0").columns
    types = [f.dataType.simpleString()
             for f in spark.sql(f"SELECT * FROM {table} LIMIT 0").schema.fields]
    payload = "\n".join(f"{i}:{n}:{t}" for i, (n, t) in enumerate(zip(cols, types)))
    return hashlib.sha256(payload.encode()).hexdigest(), [
        {"index": i, "name": n, "type": t} for i, (n, t) in enumerate(zip(cols, types))
    ]


def build_spark(args):
    from pyspark.sql import SparkSession

    builder = (SparkSession.builder
               .master(args.master)
               .appName("criteo-ctr-fe-v1-profile")
               .config("spark.driver.memory", args.driver_memory)
               .config("spark.local.dir", s3a_session.spark_scratch()))
    ak, sk = s3a_session.load_creds()
    s3a_session.export_aws_env(ak, sk)
    builder = s3a_session.apply_frozen_reader(
        builder, ak, sk,
        interceptor=False,      # profiling must not emit IO traces
        track1_s3a=False,       # and must not engage Track1 at all
        track1_d1=False, track1_d2=False, track1_d4=False)
    for item in args.conf:
        key, _, value = item.partition("=")
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def pass_one(spark, table):
    """One scan, all the aggregates that only need a single row back."""
    exprs = ["count(*) AS n_rows",
             "sum(CAST(label AS BIGINT)) AS n_clicks",
             "sum(CAST(label = 0 AS BIGINT)) AS n_nonclicks"]
    for i, c in enumerate(INT_COLS):
        exprs += [f"count({c}) AS nn_{i}", f"min({c}) AS min_{i}",
                  f"max({c}) AS max_{i}", f"avg({c}) AS avg_{i}",
                  f"stddev({c}) AS std_{i}",
                  f"percentile_approx({c}, array({', '.join(str(q) for q in QUANTILES)})) AS q_{i}"]
    for i, c in enumerate(CAT_COLS):
        exprs += [f"count({c}) AS cnn_{i}",
                  f"approx_count_distinct({c}) AS cd_{i}"]
    row = spark.sql(f"SELECT {', '.join(exprs)} FROM {table}").collect()[0]
    return row


def per_day_rows(spark, table):
    rows = spark.sql(
        f"SELECT day, count(*) AS n_rows, sum(CAST(label AS BIGINT)) AS n_clicks "
        f"FROM {table} GROUP BY day ORDER BY day").collect()
    # `day` is a partition column and Spark types it DATE, not STRING. JSON
    # object keys must be strings, so normalise here; the generator then emits
    # DATE 'YYYY-MM-DD' literals rather than comparing a date to a string.
    return {_day_key(r["day"]): {"rows": r["n_rows"],
                                 "clicks": r["n_clicks"],
                                 "ctr": (r["n_clicks"] / r["n_rows"]) if r["n_rows"] else None}
            for r in rows}


def _day_key(value):
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def topk(spark, table, col, k):
    rows = spark.sql(
        f"SELECT {col} AS value, count(*) AS c, sum(CAST(label AS BIGINT)) AS clicks "
        f"FROM {table} WHERE {col} IS NOT NULL "
        f"GROUP BY {col} ORDER BY c DESC, value ASC LIMIT {k}").collect()
    return [{"value": r["value"], "count": r["c"], "clicks": r["clicks"]} for r in rows]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=DEFAULT_DATA)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--out", required=True)
    ap.add_argument("--topk", type=int, default=DEFAULT_TOPK)
    ap.add_argument("--split-train", nargs="*",
                    default=["2015-02-15", "2015-02-16"])
    ap.add_argument("--split-val", nargs="*", default=["2015-02-17"])
    ap.add_argument("--split-test", nargs="*", default=["2015-02-18"])
    ap.add_argument("--master", default="local[16]")
    ap.add_argument("--driver-memory", default="48g")
    ap.add_argument("--scratch", default="/mnt/ssd-spare/track2-scratch")
    ap.add_argument("--region", default=os.environ.get("AWS_DEFAULT_REGION", "us-east-2"))
    ap.add_argument("--conf", action="append", default=[])
    args = ap.parse_args()

    os.environ["SPARK_LOCAL_DIRS"] = args.scratch
    if not args.conf:
        # Same reason as the 2x2 grid: a large heap defers GC, and a profiling
        # job with 26 sketches plus group-bys accumulates shuffle.
        args.conf.append("spark.cleaner.periodicGC.interval=30s")

    print("# criteo profile", flush=True)
    inv = object_inventory(f"{args.data.rstrip('/')}/{args.table}", args.region)
    print(f"  objects {inv['n_files']}  bytes {inv['total_bytes']}", flush=True)
    for day, d in inv["per_day"].items():
        print(f"    {day}: {d['files']} files {d['bytes']/2**30:.2f} GiB", flush=True)

    spark = build_spark(args)
    try:
        path = f"{args.data.rstrip('/')}/{args.table}"
        spark.read.parquet(path).createOrReplaceTempView(args.table)
        sig, columns = schema_signature(spark, args.table)
        print(f"  schema sha256 {sig[:16]}…  cols {len(columns)}", flush=True)

        print("  pass 1: row counts, integer stats, categorical cardinality", flush=True)
        row = pass_one(spark, args.table)
        n_rows = row["n_rows"]
        n_clicks = row["n_clicks"]
        print(f"    rows {n_rows}  clicks {n_clicks}  ctr {n_clicks/n_rows:.6f}", flush=True)

        ints = {}
        for i, c in enumerate(INT_COLS):
            q = list(row[f"q_{i}"])
            quantiles = {str(p): v for p, v in zip(QUANTILES, q)}
            ints[c] = {
                "non_null": row[f"nn_{i}"],
                "null_fraction": 1 - (row[f"nn_{i}"] / n_rows) if n_rows else None,
                "min": row[f"min_{i}"], "max": row[f"max_{i}"],
                "avg": row[f"avg_{i}"], "stddev": row[f"std_{i}"],
                "quantiles": quantiles,
                "bucket_edges": [quantiles[str(p)] for p in BUCKET_QUANTILES],
            }
        cats = {}
        for i, c in enumerate(CAT_COLS):
            cats[c] = {
                "non_null": row[f"cnn_{i}"],
                "null_fraction": 1 - (row[f"cnn_{i}"] / n_rows) if n_rows else None,
                "approx_distinct": row[f"cd_{i}"],
            }

        # Deterministic representative choice: sort by cardinality, then by
        # column index. Nobody gets to pick the column that flatters a result.
        order = sorted(range(len(CAT_COLS)),
                       key=lambda i: (cats[CAT_COLS[i]]["approx_distinct"] or 0, i))
        rep_low = CAT_COLS[order[0]]
        rep_mid = CAT_COLS[order[len(order) // 2]]
        rep_high = CAT_COLS[order[-1]]
        reps = {"low": rep_low, "mid": rep_mid, "high": rep_high}
        print(f"    representatives low={rep_low} mid={rep_mid} high={rep_high}", flush=True)

        topk_vals = {}
        for tier, col in reps.items():
            print(f"  pass 2: Top-{args.topk} of {col} ({tier})", flush=True)
            topk_vals[col] = topk(spark, args.table, col, args.topk)

        per_day = per_day_rows(spark, args.table)
        for day, d in per_day.items():
            print(f"    {day}: rows {d['rows']} ctr {d['ctr']:.6f}", flush=True)
    finally:
        spark.stop()

    days = sorted(per_day)
    if len(days) < 4:
        print(f"  WARNING: only {len(days)} day partitions present; the intended "
              f"20/2/2 day split is not available. Split recorded as given.",
              flush=True)

    profile = {
        "profile_version": PROFILE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_uri": f"{args.data.rstrip('/')}/{args.table}",
        "table": args.table,
        "split": {"train": args.split_train, "validation": args.split_val,
                  "test": args.split_test, "note":
                  "day-based; Train/Validation/Test are disjoint and ordered by time"},
        "objects": inv,
        "rows": {"total": n_rows, "clicks": n_clicks,
                 "nonclicks": row["n_nonclicks"],
                 "ctr": n_clicks / n_rows if n_rows else None,
                 "per_day": per_day},
        "schema": {"columns": columns, "sha256": sig},
        "integer_features": ints,
        "categorical_features": cats,
        "cardinality_order": [CAT_COLS[i] for i in order],
        "representative_columns": reps,
        "topk": {"k": args.topk, "values": topk_vals},
        "constants": {
            "quantile_points": QUANTILES,
            "bucket_quantiles": BUCKET_QUANTILES,
        },
        "engine": {"master": args.master, "driver_memory": args.driver_memory,
                   "scratch": args.scratch, "conf": args.conf},
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    # Write through a temp file: the profile is the input the generator freezes
    # against, and a half-written JSON would silently produce a different
    # workload next time.
    tmp = args.out + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(profile, fh, indent=2, default=str)
    os.replace(tmp, args.out)
    print(f"  out {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
