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
import hashlib
import json
import math
import os
import shutil
import socket
import statistics
import subprocess
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


LOCAL_HOST_ALIASES = {
    "localhost",
    "127.0.0.1",
    socket.gethostname(),
    socket.getfqdn(),
}


def parse_worker_hosts(raw):
    if not raw:
        return []
    return [h.strip() for h in raw.split(",") if h.strip()]


def is_remote_host(host):
    return host not in LOCAL_HOST_ALIASES


def drop_os_page_cache(worker_hosts=None):
    """Drop Linux page cache locally (via direct write or passwordless sudo) and on remote workers."""
    ok = False
    try:
        with open("/proc/sys/vm/drop_caches", "w") as fh:
            fh.write("3\n")
        ok = True
    except OSError:
        res = subprocess.run(
            ["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        ok = res.returncode == 0
    for host in worker_hosts or []:
        if is_remote_host(host):
            subprocess.run(
                [
                    "ssh",
                    "-o",
                    "BatchMode=yes",
                    host,
                    "sudo -n sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches' || true",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    return ok


def prepare_remote_workers(worker_hosts, run_io_dir):
    probe_dir = os.path.join(run_io_dir, "track1-probe")
    for host in worker_hosts:
        if is_remote_host(host):
            subprocess.check_call(
                [
                    "ssh",
                    "-o",
                    "BatchMode=yes",
                    host,
                    f"rm -rf {run_io_dir!r} && mkdir -p {probe_dir!r}",
                ]
            )


def pull_remote_workers(worker_hosts, run_io_dir):
    for host in worker_hosts:
        if not is_remote_host(host):
            continue
        dest = os.path.join(run_io_dir, "workers", host)
        os.makedirs(dest, exist_ok=True)
        subprocess.run(
            [
                "rsync",
                "-az",
                f"{host}:{run_io_dir.rstrip('/')}/",
                f"{dest}/",
            ],
            check=False,
        )


def io_stats(collector_dir, since_mtime):
    """Sum interceptor records written after since_mtime across driver + workers."""
    gets, ranged, nbytes = 0, 0, 0
    if not collector_dir or not os.path.isdir(collector_dir):
        return {"gets": None, "ranged_gets": None, "remote_bytes": None}
    seen_files = set()
    for root, _dirs, files in os.walk(collector_dir):
        for name in files:
            if not name.endswith(".ndjson") or name.startswith("track1-s3a-probe"):
                continue
            if name in seen_files:
                continue
            path = os.path.join(root, name)
            if os.path.getmtime(path) < since_mtime - 5:
                continue
            seen_files.add(name)
            with open(path) as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
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
            interceptor=not args.no_interceptor,
            track1_s3a=getattr(args, "track1_s3a", False),
            track1_d1=getattr(args, "track1_d1", False),
            track1_d2=getattr(args, "track1_d2", False),
            track1_d4=getattr(args, "track1_d4", False),
            track1_d2_wait_us=getattr(args, "track1_d2_wait_us", 0),
            track1_d1_admit_bytes=getattr(args, "track1_d1_admit_bytes", 256 * 1024),
            track1_d1_cache_mib=getattr(args, "track1_d1_cache_mib", 256),
            track1_d1_block_bytes=getattr(args, "track1_d1_block_bytes", None),
            track1_d1_adaptive=getattr(args, "track1_d1_adaptive", False),
            track1_d1_fixed_capacity=getattr(args, "track1_d1_fixed_capacity", False),
            track1_d1_fixed_admission=getattr(args, "track1_d1_fixed_admission", False),
            track1_d1_profile=getattr(args, "track1_d1_profile", False),
            track1_d1_zero_copy=getattr(args, "track1_d1_zero_copy", True),
            track1_d1_doorkeeper=getattr(args, "track1_d1_doorkeeper", False),
            track1_d1_shared_backing=getattr(args, "track1_d1_shared_backing", True),
            track1_d1_policy=getattr(args, "track1_d1_policy", ""),
            track1_d1_hard_mib=getattr(args, "track1_d1_hard_mib", None),
            track1_d1_coverage=getattr(args, "track1_d1_coverage", None),
            track1_d1_observe_gets=getattr(args, "track1_d1_observe_gets", None),
            track1_d1_min_mib=getattr(args, "track1_d1_min_mib", None))
    if getattr(args, "executor_memory", None):
        builder = builder.config("spark.executor.memory", args.executor_memory)
    if getattr(args, "executor_cores", None):
        builder = builder.config("spark.executor.cores", str(args.executor_cores))
    if getattr(args, "num_executors", None):
        builder = builder.config("spark.executor.instances", str(args.num_executors))
        if args.executor_cores:
            builder = builder.config(
                "spark.cores.max", str(int(args.num_executors) * int(args.executor_cores))
            )
    if "hits" in getattr(args, "tables", []):
        # gen_clickbench.py wrote EventDate/EventTime in UTC.
        builder = builder.config("spark.sql.session.timeZone", "UTC")
    for item in args.conf:
        key, _, value = item.partition("=")
        builder = builder.config(key, value)
    if getattr(args, "periodic_gc_ms", 0):
        gc_flags = (
            f"-XX:+UseG1GC -XX:G1PeriodicGCInterval={int(args.periodic_gc_ms)} "
            "-XX:-G1PeriodicGCInvokesConcurrent"
        )
        for opt_key in ("spark.driver.extraJavaOptions", "spark.executor.extraJavaOptions"):
            existing = builder._options.get(opt_key, "")
            merged = f"{existing} {gc_flags}".strip()
            builder = builder.config(opt_key, merged)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def register_tables(spark, data_root, tables):
    for table in tables:
        path = f"{data_root.rstrip('/')}/{table}"
        spark.read.parquet(path).createOrReplaceTempView(table)


def result_sha256(rows):
    """Stable content hash for a collected SQL result, insensitive to row order."""
    def normalise(value):
        if hasattr(value, "asDict"):
            return {k: normalise(v) for k, v in value.asDict(recursive=True).items()}
        if isinstance(value, dict):
            return {str(k): normalise(v) for k, v in value.items()}
        if isinstance(value, (tuple, list)):
            return [normalise(v) for v in value]
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value

    encoded = sorted(
        json.dumps(normalise(row), sort_keys=True, separators=(",", ":"), default=str)
        for row in rows
    )
    digest = hashlib.sha256()
    for row in encoded:
        digest.update(row.encode())
        digest.update(b"\n")
    return digest.hexdigest()


def run_once(args, run_id, queries, collector_dir, eventlog_dir):
    workers = parse_worker_hosts(getattr(args, "worker_hosts", ""))
    dropped = drop_os_page_cache(workers)
    t_session = time.time()
    run_io_dir = os.path.join(collector_dir, f"run-{run_id}")
    os.makedirs(run_io_dir, exist_ok=True)
    if workers:
        prepare_remote_workers(workers, run_io_dir)
    spark = build_session(args, run_id, run_io_dir, eventlog_dir)
    register_tables(spark, args.data, args.tables)
    rows = []
    for qnr, sql in queries:
        t0 = time.time()
        err = None
        nout = None
        result_hash = None
        spark.sparkContext.setLocalProperty("callSite.short", f"track2:q{qnr}")
        try:
            result = spark.sql(sql).collect()
            nout = len(result)
            result_hash = result_sha256(result)
        except Exception as exc:  # keep going so one query does not kill the run
            err = f"{type(exc).__name__}: {exc}"
        elapsed = time.time() - t0
        rows.append({
            "run": run_id,
            "query": qnr,
            "wall_s": round(elapsed, 3),
            "rows": nout,
            "result_sha256": result_hash,
            "error": err,
        })
        status = "ERR" if err else "ok"
        print(f"  run {run_id}  Q{qnr:02d}  {elapsed:8.2f}s  {status}", flush=True)
        if err:
            print(f"           {err[:300]}", flush=True)
    driver_track1 = read_driver_track1_stats(spark)
    spark.stop()
    if not args.master.startswith("local"):
        # Allow Standalone executor JVM shutdown hooks to flush stats & NDJSON.
        time.sleep(1.0)
    if workers:
        pull_remote_workers(workers, run_io_dir)
    track1 = aggregate_track1_stats(run_io_dir, driver_track1)
    session_s = time.time() - t_session
    io = io_stats(run_io_dir, t_session)
    return {
        "run": run_id,
        "session_s": round(session_s, 3),
        "query_sum_s": round(sum(r["wall_s"] for r in rows), 3),
        "drop_caches": dropped,
        "queries": rows,
        "io_dir": run_io_dir,
        "io": io,
        "track1": track1,
    }


ADDITIVE_TRACK1_KEYS = (
    "cache_hits",
    "cache_misses",
    "cache_useful_bytes",
    "cached_bytes",
    "peak_cached_bytes",
    "evicted_bytes",
    "evicted_blocks",
    "d1_admitted_blocks",
    "d1_rejected_blocks",
    "d1_ghost_hits",
    "d1_reject_ghost_hits",
    "d1_reject_request_ghost_hits",
    "d1_eviction_ghost_hits",
    "remote_gets",
    "merged_gets",
    "merged_members",
    "wasted_bytes",
    "fallbacks",
    "admit_rejected",
    "teed_gets",
    "teed_bytes",
    "gc_ms",
    "gc_count",
)


def read_driver_track1_stats(spark):
    """Snapshot D1/D2/D4 counters from the live driver JVM."""
    try:
        raw = spark._jvm.software.amazon.awssdk.s3.adaptive.s3a.Track1S3aRuntime.shared().snapshotJson()
        return json.loads(str(raw))
    except Exception:
        return None


def aggregate_track1_stats(run_io_dir, driver_fallback):
    """Collect per-JVM Track1 stats files across driver + Standalone executors."""
    snaps = {}
    for root, _dirs, files in os.walk(run_io_dir):
        for name in sorted(files):
            if name.startswith("track1-s3a-stats-") and name.endswith(".json"):
                if name in snaps:
                    continue
                path = os.path.join(root, name)
                try:
                    with open(path) as fh:
                        snaps[name] = json.load(fh)
                except Exception:
                    continue
    if not snaps:
        if driver_fallback is not None:
            return driver_fallback
        legacy = os.path.join(run_io_dir, "track1-probe", "track1-s3a-stats.json")
        if os.path.isfile(legacy):
            with open(legacy) as fh:
                return json.load(fh)
        return None
    items = list(snaps.values())
    if len(items) == 1:
        return items[0]
    # Multi-JVM cluster run: prefer executor JVMs that actually issued/served reads
    # when computing per-node max heap, while summing all additive cache counters.
    active_items = [
        s for s in items
        if (s.get("cache_hits", 0) + s.get("cache_misses", 0) + s.get("remote_gets", 0)) > 0
    ] or items
    merged = dict(active_items[0])
    for key in ADDITIVE_TRACK1_KEYS:
        if all(isinstance(s.get(key), (int, float)) for s in items if key in s):
            merged[key] = sum(s.get(key, 0) for s in items)
    merged["peak_heap_bytes"] = max(s.get("peak_heap_bytes", 0) for s in active_items)
    merged["sum_peak_heap_bytes"] = sum(s.get("peak_heap_bytes", 0) for s in active_items)
    merged["max_node_peak_cached_bytes"] = max(
        s.get("peak_cached_bytes", 0) for s in active_items
    )
    merged["jvm_count"] = len(items)
    merged["active_executor_jvms"] = len(active_items)
    return merged


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
    ap.add_argument("--executor-memory", default=None,
                    help="Spark executor memory in cluster mode (e.g. 48g)")
    ap.add_argument("--executor-cores", type=int, default=None,
                    help="Spark cores per executor in cluster mode (e.g. 16)")
    ap.add_argument("--num-executors", type=int, default=None,
                    help="Number of active executors to use in Standalone cluster mode")
    ap.add_argument("--worker-hosts", default="",
                    help="Comma-separated worker hosts for remote drop_caches & telemetry pull")
    ap.add_argument("--periodic-gc-ms", type=int, default=0,
                    help="Enable G1 periodic GC interval in ms (e.g. 30000)")
    ap.add_argument("--conf", action="append", default=[])
    ap.add_argument("--no-s3", dest="s3", action="store_false")
    ap.add_argument("--no-interceptor", action="store_true")
    ap.add_argument("--track1-s3a", action="store_true",
                    help="route S3A AWS clients through Track1S3ClientFactory "
                         "(passthrough unless a D1/D2/D4 flag is also set)")
    ap.add_argument("--track1-d1", action="store_true",
                    help="enable D1 range cache (requires --track1-s3a)")
    ap.add_argument("--track1-d2", action="store_true",
                    help="enable D2 sync-to-async queue (requires --track1-s3a)")
    ap.add_argument("--track1-d4", action="store_true",
                    help="enable D4 merge; ignored unless --track1-d2 is also set")
    ap.add_argument("--track1-d2-wait-us", type=int, default=0,
                    help="D2 batch wait window in microseconds (0 / 50 / 200)")
    ap.add_argument("--track1-d1-admit-bytes", type=int, default=256 * 1024,
                    help="D1 admission cap in bytes; 0 = always admit")
    ap.add_argument("--track1-d1-cache-mib", type=int, default=256,
                    help="D1 cache budget in MiB; raise it with the admission "
                         "cap, never alone")
    ap.add_argument("--track1-d1-block-bytes", type=int, default=None,
                    help="D1 cache block size in bytes (default 1 MiB)")
    ap.add_argument("--track1-d1-adaptive", action="store_true",
                    help="enable D1 soft controller (observe/track/bypass/shrink)")
    ap.add_argument("--track1-d1-fixed-capacity", action="store_true",
                    help="pin adaptive D1 soft target at the hard cache limit")
    ap.add_argument("--track1-d1-fixed-admission", action="store_true",
                    help="apply d1 admission bytes as a fixed filter in adaptive mode")
    ap.add_argument("--track1-d1-profile", action="store_true",
                    help="emit D1 local cache overhead counters")
    ap.add_argument("--no-track1-d1-zero-copy", action="store_false", dest="track1_d1_zero_copy",
                    help="materialize cache hits for A/B comparison")
    ap.add_argument("--track1-d1-doorkeeper", action="store_true",
                    help="enable the first-touch doorkeeper (off by default)")
    ap.add_argument("--no-track1-d1-doorkeeper", action="store_false", dest="track1_d1_doorkeeper",
                    default=False,
                    help="disable the doorkeeper (already the default)")
    ap.add_argument("--track1-d1-policy", default="",
                    choices=["", "lru", "slru", "wtinylfu", "wtinylfu-test", "wtinylfu-plain",
                             "wtinylfu-av", "gdsf", "gdsf-unit", "sieve"],
                    help="D1 replacement policy; empty derives it from --track1-d1-adaptive "
                         "(W-TinyLFU when adaptive, LRU otherwise)")
    ap.add_argument("--no-track1-d1-shared-backing", action="store_false", dest="track1_d1_shared_backing",
                    help="copy admitted request payloads for A/B comparison")
    ap.add_argument("--track1-d1-hard-mib", type=int, default=None,
                    help="D1 hard GlobalBudget cap in MiB when adaptive")
    ap.add_argument("--track1-d1-coverage", type=float, default=None,
                    help="target reusable-working-set coverage (0-1)")
    ap.add_argument("--track1-d1-observe-gets", type=int, default=None,
                    help="GET count before leaving OBSERVE")
    ap.add_argument("--track1-d1-min-mib", type=int, default=None,
                    help="soft target floor in MiB when adaptive")
    ap.add_argument("--keep-io", action="store_true",
                    help="leave previous io/ traces in --out; default wipes "
                         "them so a later correlate cannot mix invocations")
    args = ap.parse_args()
    args.s3 = args.s3 and (args.data.startswith("s3a://") or args.data.startswith("s3://"))

    if args.tables is None:
        args.tables = ["hits"] if "clickbench" in args.queries_file else TPCH_TABLES
    only = [int(x) for x in args.queries.split(",")] if args.queries else None
    queries = load_queries(args.queries_file, only)
    os.makedirs(args.out, exist_ok=True)
    collector_dir = os.path.join(args.out, "io")
    eventlog_dir = os.path.join(args.out, "eventlogs")
    if not args.keep_io and os.path.isdir(collector_dir):
        shutil.rmtree(collector_dir)
    os.makedirs(collector_dir, exist_ok=True)

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
