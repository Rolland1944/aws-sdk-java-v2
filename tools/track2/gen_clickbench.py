#!/usr/bin/env python3
"""Expand official ClickBench hits.parquet and write a Spark baseline to S3.

The public ClickBench corpus is one month of Yandex.Metrica hits: ~1e8 rows,
105 columns, a single ~14 GiB Parquet file. Track 2's external bar is ≥100 GiB
(TRACK2_M0_CONTRACT.md D-5 / E2), so this script repeats that month `--copies`
times (default 8 ≈ 110 GiB if Spark's Snappy re-encode stays near the source).

It is not a second generator of new users. UserID / WatchID / FUniqID already
span nearly the full int64 range, so they are left unchanged: the result is the
same population observed over consecutive 30-day windows. EventDate / EventTime
are shifted by 30 days per copy so month-bounded official queries (Q38–Q43)
remain selective, and a later sort on EventDate can prune the extra months.

EventDate is stored in the source as uint16 days-since-1970 and EventTime as
unix seconds. Both are converted to Spark Date / Timestamp so the official
ClickBench Spark query texts (extract, DATE_TRUNC, date literals) run without
a per-query rewrite.

The write itself is contract 2.3: plain `df.write.parquet(...)` per copy,
appended under <out>/hits. That directory is the canonical Spark baseline,
not a layout candidate. Do not point this at an existing TPC-H prefix.

Usage:
  python3 tools/track2/gen_clickbench.py --dry-run

  python3 tools/track2/gen_clickbench.py \\
      --source /data/home/haoyueli/hitmap_test/data/raw/clickbench/hits.parquet \\
      --out s3a://home-haoyue/track2/clickbench_sf8
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import s3a_session  # noqa: E402
import write_layout  # noqa: E402

SOURCE_ROWS = 99_997_497
SOURCE_BYTES = 14_779_976_446
# Footer stats: EventDate 15888–15917 (2013-07-02 .. 2013-07-31), 30 distinct days.
DAYS_PER_COPY = 30


def build_spark(args):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master(args.master)
        .appName("track2-gen-clickbench")
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.local.dir", s3a_session.spark_scratch())
        # Source EventTime is unix seconds; EventDate is days since 1970-01-01 UTC.
        .config("spark.sql.session.timeZone", "UTC")
    )
    needs_s3 = args.out.startswith("s3a://") or args.out.startswith("s3://")
    if needs_s3:
        ak, sk = s3a_session.load_creds()
        s3a_session.export_aws_env(ak, sk)
        builder = s3a_session.apply_frozen_reader(builder, ak, sk, interceptor=False)
    for item in args.conf:
        key, _, value = item.partition("=")
        builder = builder.config(key, value)
    return builder.getOrCreate()


def shift_month(df, copy_id):
    """Promote temporal columns and optionally shift them by 30-day windows."""
    from pyspark.sql import functions as F

    # Source EventDate is days since 1970-01-01 (uint16). EventTime is unix seconds.
    day_shift = copy_id * DAYS_PER_COPY
    event_date = F.date_add(
        F.date_add(F.lit("1970-01-01").cast("date"), F.col("EventDate").cast("int")),
        day_shift)
    event_time = F.from_unixtime(
        F.col("EventTime").cast("long") + day_shift * 86400).cast("timestamp")
    return df.withColumn("EventDate", event_date).withColumn("EventTime", event_time)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--source",
        default="/data/home/haoyueli/hitmap_test/data/raw/clickbench/hits.parquet")
    ap.add_argument("--out", default="s3a://home-haoyue/track2/clickbench_sf8")
    ap.add_argument("--copies", type=int, default=8,
                    help="30-day windows; 8 ≈ 100 GiB at source compression")
    ap.add_argument("--master", default="local[16]")
    ap.add_argument("--driver-memory", default="32g")
    ap.add_argument("--conf", action="append", default=[])
    ap.add_argument("--dry-run", action="store_true",
                    help="print schema after the date/time promotion; do not write")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--verify-sample", type=int, default=20)
    ap.add_argument("--manifest", default=None)
    args = ap.parse_args()

    if args.copies < 1:
        sys.exit("--copies must be >= 1")
    if "tpch" in args.out or "baseline_sf100" in args.out or "cand_" in args.out:
        sys.exit(f"refusing to write ClickBench onto a TPC-H prefix: {args.out}")

    spark = build_spark(args)
    destination = f"{args.out.rstrip('/')}/hits"
    print(f"# clickbench scale-up  copies={args.copies}  src={args.source}", flush=True)
    print(f"# dest {destination}", flush=True)

    raw = spark.read.parquet(args.source)
    print(f"# source schema ({len(raw.columns)} cols):", flush=True)
    raw.printSchema()

    sample = shift_month(raw, 0)
    print("# after EventDate/EventTime promotion:", flush=True)
    sample.printSchema()
    if args.dry_run:
        row = sample.limit(1).collect()[0]
        print(f"# sample EventDate={row['EventDate']} EventTime={row['EventTime']}",
              flush=True)
        spark.stop()
        return 0

    copies = []
    t_all = time.time()
    for copy_id in range(args.copies):
        t0 = time.time()
        df = shift_month(spark.read.parquet(args.source), copy_id)
        writer = df.write.mode("overwrite" if copy_id == 0 else "append")
        writer.parquet(destination)
        elapsed = round(time.time() - t0, 1)
        copies.append({"copy_id": copy_id, "day_offset": copy_id * DAYS_PER_COPY,
                       "elapsed_seconds": elapsed})
        print(f"  copy {copy_id}/{args.copies - 1}  +{copy_id * DAYS_PER_COPY}d  "
              f"{elapsed:.1f}s", flush=True)

    verified = None
    if args.verify:
        verified = write_layout.verify_footers(destination, args.verify_sample)
        print(f"# verify {verified}", flush=True)

    manifest = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "dataset": "clickbench",
        "scale": f"sf{args.copies}",
        "source": {
            "path": args.source,
            "rows": SOURCE_ROWS,
            "bytes": SOURCE_BYTES,
            "note": "official ClickBench hits.parquet (parquet-cpp 1.5.1-SNAPSHOT)",
        },
        "replication": {
            "copies": args.copies,
            "days_per_copy": DAYS_PER_COPY,
            "user_keys_offset": False,
            "user_keys_reason": "UserID/WatchID/FUniqID already span int64; "
                                "copies are additional months of the same users",
            "event_date_promoted_to": "date",
            "event_time_promoted_to": "timestamp",
        },
        "output": args.out,
        "hits_path": destination,
        "expected_rows": SOURCE_ROWS * args.copies,
        "copies": copies,
        "elapsed_seconds": round(time.time() - t_all, 1),
        "verified": verified,
        "role": "canonical Spark baseline (contract 2.3 plain write.parquet); "
                "not a layout candidate",
    }
    manifest_path = args.manifest or os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../../docs/adaptive-range-reader/results/track2/clickbench",
        f"_manifest_sf{args.copies}.json")
    manifest_path = os.path.abspath(manifest_path)
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, "w") as fh:
        json.dump(manifest, fh, indent=2)
    print(f"\nmanifest: {manifest_path}", flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
