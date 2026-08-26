#!/usr/bin/env python3
"""E2 baseline / layout benchmark: TPC-H 22 queries, cold-cache, N repeats.

Contract (TRACK2_M0_CONTRACT.md 2.1 / 4.2 / 7 E2):
  * all 22 DuckDB-frozen TPC-H texts, uniform weight
  * cold-cache: new Spark session per run (JVM / S3A buffer die with the session)
  * >=5 independent runs; report median and distribution, never the fastest run
  * gate: CV of end-to-end wall-clock across runs < 5%
  * per-query median archived (guardrail against hiding a regression)

Reader settings are the frozen 1.4 set (s3a_session.apply_frozen_reader).
The layout under --data must already be a Spark/parquet-mr rewrite, not the
DuckDB source: DuckDB files have no page index and are not the baseline
defined in contract 2.3.

Usage:
  # dialect smoke on local SF1 (seconds)
  python3 tools/track2/run_benchmark.py \\
      --data /data/home/haoyueli/track2-data/tpch_sf1_smoke \\
      --runs 1 --queries 6 --no-s3 --out /tmp/bench_sf1

  # E2 gate on the S3 baseline
  python3 tools/track2/run_benchmark.py \\
      --data s3a://home-haoyue/track2/baseline_sf100 \\
      --runs 5 --out docs/adaptive-range-reader/results/track2/e2_baseline
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import s3a_session  # noqa: E402

TPCH_TABLES = ["customer", "lineitem", "nation", "orders",
               "part", "partsupp", "region", "supplier"]
DEFAULT_QUERIES = os.path.expanduser("~/track2-data/tpch_sf100/_tpch_queries.json")


def load_queries(path, only=None):
    with open(path) as fh:
        raw = json.load(fh)
    queries = {int(k): v for k, v in raw.items() if str(k).isdigit()}
    nrs = sorted(queries)
    if only:
        nrs = [n for n in only if n in queries]
    return [(n, to_spark_sql(queries[n])) for n in nrs]


def to_spark_sql(sql):
    """Minimal dialect fixes so DuckDB TPC-H text runs on Spark SQL.

    Only rewrite constructs Spark rejects; keep the query text otherwise identical
    to the frozen DuckDB strings (contract 2.1).
    """
    return (sql
            .replace("substring(c_phone FROM 1 FOR 2)", "substring(c_phone, 1, 2)")
            .replace("AS c_orders (c_custkey,\n        c_count)", "AS c_orders")
            .replace("count(o_orderkey)\n    FROM", "count(o_orderkey) AS c_count\n    FROM")
            .replace("extract(minute FROM EventTime)", "minute(EventTime)"))


def drop_os_page_cache():
    """Best-effort; S3A coldness comes from killing the JVM, not this."""
    try:
        with open("/proc/sys/vm/drop_caches", "w") as fh:
            fh.write("3\n")
        return True
    except OSError:
        return False


def io_stats(collector_dir, since_mtime):
    """Sum interceptor records written after since_mtime."""
    gets, ranged, nbytes = 0, 0, 0
    if not collector_dir or not os.path.isdir(collector_dir):
        return {"gets": None, "ranged_gets": None, "remote_bytes": None}
    for name in os.listdir(collector_dir):
        if not name.endswith(".ndjson"):
            continue
        path = os.path.join(collector_dir, name)
        if os.path.getmtime(path) < since_mtime - 1:
            continue
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                # The interceptor writes NDJSON asynchronously; a record may be
                # truncated if read while the JVM is tearing down after spark.stop().
                # Skip those few lines rather than aborting the whole run.
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("method") != "GET":
                    continue
                gets += 1
                length = rec.get("range_length") or rec.get("bytes_expected") or 0
                if rec.get("range_offset") is not None:
                    ranged += 1
                    nbytes += length
                elif length:
                    nbytes += length
    return {"gets": gets, "ranged_gets": ranged, "remote_bytes": nbytes}


def build_session(args, run_id, collector_dir, eventlog_dir):
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder
               .master(args.master)
               .appName(f"track2-e2-{args.layout_id}-run{run_id}")
               .config("spark.driver.memory", args.driver_memory)
               .config("spark.local.dir", s3a_session.spark_scratch()))
    if args.s3:
        ak, sk = s3a_session.load_creds()
        s3a_session.export_aws_env(ak, sk)
        builder = s3a_session.apply_frozen_reader(
            builder, ak, sk,
            collector_dir=collector_dir,
            eventlog_dir=eventlog_dir,
            interceptor=not args.no_interceptor)
    if "hits" in getattr(args, "tables", []):
        # gen_clickbench.py wrote EventDate/EventTime in UTC.
        builder = builder.config("spark.sql.session.timeZone", "UTC")
    for item in args.conf:
        key, _, value = item.partition("=")
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def register_tables(spark, data_root, tables):
    for table in tables:
        path = f"{data_root.rstrip('/')}/{table}"
        spark.read.parquet(path).createOrReplaceTempView(table)


def run_once(args, run_id, queries, collector_dir, eventlog_dir):
    dropped = drop_os_page_cache()
    t_session = time.time()
    spark = build_session(args, run_id, collector_dir, eventlog_dir)
    register_tables(spark, args.data, args.tables)
    rows = []
    for qnr, sql in queries:
        t0 = time.time()
        err = None
        nout = None
        # Stamp the query number into the event log. Spark records this as the
        # SQL execution's description, which makes the log self-describing:
        # workload_snapshot.py can then read scans back per query instead of
        # assuming the k-th execution is the k-th query.
        spark.sparkContext.setLocalProperty("callSite.short", f"track2:q{qnr}")
        try:
            result = spark.sql(sql).collect()
            nout = len(result)
        except Exception as exc:  # keep going so one query does not kill the run
            err = f"{type(exc).__name__}: {exc}"
        elapsed = time.time() - t0
        rows.append({
            "run": run_id,
            "query": qnr,
            "wall_s": round(elapsed, 3),
            "rows": nout,
            "error": err,
        })
        status = "ERR" if err else "ok"
        print(f"  run {run_id}  Q{qnr:02d}  {elapsed:8.2f}s  {status}", flush=True)
        if err:
            print(f"           {err[:300]}", flush=True)
    spark.stop()
    session_s = time.time() - t_session
    io = io_stats(collector_dir, t_session)
    return {
        "run": run_id,
        "session_s": round(session_s, 3),
        "query_sum_s": round(sum(r["wall_s"] for r in rows), 3),
        "drop_caches": dropped,
        "queries": rows,
        "io": io,
    }


def summarize(runs):
    totals = [r["query_sum_s"] for r in runs]
    mean = statistics.mean(totals)
    stdev = statistics.stdev(totals) if len(totals) > 1 else 0.0
    cv = (stdev / mean) if mean else float("inf")
    by_q = {}
    for r in runs:
        for q in r["queries"]:
            by_q.setdefault(q["query"], []).append(q)
    per_query = []
    for qnr, samples in sorted(by_q.items()):
        walls = [s["wall_s"] for s in samples]
        errors = [s["error"] for s in samples if s["error"]]
        per_query.append({
            "query": qnr,
            "n": len(walls),
            "median_s": statistics.median(walls),
            "mean_s": statistics.mean(walls),
            "min_s": min(walls),
            "max_s": max(walls),
            "stdev_s": statistics.stdev(walls) if len(walls) > 1 else 0.0,
            "errors": errors,
        })
    return {
        "n_runs": len(runs),
        "end_to_end_s": totals,
        "median_s": statistics.median(totals),
        "mean_s": mean,
        "stdev_s": stdev,
        "cv": cv,
        "gate_threshold": 0.05,
        "gate_pass": cv < 0.05 and len(runs) >= 5 and not any(
            q["errors"] for q in per_query),
        "per_query": per_query,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, help="layout root (local or s3a://)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--queries-file", default=DEFAULT_QUERIES)
    ap.add_argument("--tables", nargs="*", default=None,
                    help="temp views to register under --data/<table>; "
                         "default TPC-H eight, or [hits] for ClickBench")
    ap.add_argument("--queries", default=None,
                    help="comma-separated query numbers; default all in --queries-file")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--layout-id", default="baseline")
    ap.add_argument("--master", default="local[16]")
    ap.add_argument("--driver-memory", default="32g")
    ap.add_argument("--conf", action="append", default=[])
    ap.add_argument("--no-s3", dest="s3", action="store_false")
    ap.add_argument("--no-interceptor", action="store_true")
    args = ap.parse_args()
    args.s3 = args.s3 and (args.data.startswith("s3a://") or args.data.startswith("s3://"))

    if args.tables is None:
        args.tables = ["hits"] if "clickbench" in args.queries_file else TPCH_TABLES
    only = [int(x) for x in args.queries.split(",")] if args.queries else None
    queries = load_queries(args.queries_file, only)
    os.makedirs(args.out, exist_ok=True)
    collector_dir = os.path.join(args.out, "io")
    eventlog_dir = os.path.join(args.out, "eventlogs")

    print(f"# E2 benchmark  layout={args.layout_id}  runs={args.runs}  "
          f"queries={[n for n,_ in queries]}  data={args.data}")
    runs = []
    for i in range(1, args.runs + 1):
        print(f"\n== run {i}/{args.runs} ==", flush=True)
        runs.append(run_once(args, i, queries, collector_dir, eventlog_dir))

    summary = summarize(runs)
    report = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 7 E2 / 4.2",
        "layout_id": args.layout_id,
        "data": args.data,
        "master": args.master,
        "driver_memory": args.driver_memory,
        "runs": runs,
        "summary": summary,
    }
    with open(os.path.join(args.out, "report.json"), "w") as fh:
        json.dump(report, fh, indent=2)
    with open(os.path.join(args.out, "per_query.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["query", "n", "median_s", "mean_s",
                                           "min_s", "max_s", "stdev_s", "n_errors"])
        w.writeheader()
        for q in summary["per_query"]:
            w.writerow({**{k: q[k] for k in ("query", "n", "median_s", "mean_s",
                                             "min_s", "max_s", "stdev_s")},
                        "n_errors": len(q["errors"])})

    print(f"\n# summary")
    print(f"  end-to-end s   {summary['end_to_end_s']}")
    print(f"  median         {summary['median_s']:.2f}s")
    print(f"  cv             {summary['cv']*100:.2f}%   (gate < 5%, n>=5)")
    print(f"  gate           {'PASS' if summary['gate_pass'] else 'FAIL / pending'}")
    print(f"  report         {os.path.join(args.out, 'report.json')}")
    if args.runs < 5:
        print(f"  note           n={args.runs} < 5; gate cannot pass yet")
    return 0 if (summary["gate_pass"] or args.runs < 5) else 1


if __name__ == "__main__":
    sys.exit(main())
