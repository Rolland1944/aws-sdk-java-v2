#!/usr/bin/env python3
"""Trino benchmark runner for RECAS multi-engine evaluation (Experiment 2).

Mirrors `tools/track2/run_benchmark.py` protocol & report.json schema so all
downstream aggregation, CV gates, and plotting tools work identically:
  * Fresh Trino server JVM per run (cold JVM & cold 4 GiB Track1 cache)
  * Linux OS page cache dropped before each run
  * Zero Trino source modifications: attaches RECAS via `-javaagent`
    (`Track1SdkAgent` + `Track1SdkAutoHook`) on Trino's native `S3Client`
  * Reuses exact frozen `canonical` and Spark-planned `joint` Parquet files
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run_benchmark as rb  # noqa: E402
import s3a_session  # noqa: E402

TRINO_HOME_DEFAULT = os.path.expanduser("~/track2-env/trino")
JDK17_DEFAULT = os.path.expanduser("~/track2-env/jdk-17")
AWS_BIN = os.path.expanduser("~/.local/bin/aws")
AGENT_JAR_IN_PLUGIN = "plugin/hive/aws-sdk-java-s3-adaptive-range-reader-2.25.70.jar"


def parse_s3_uri(uri: str) -> tuple[str, str]:
    for prefix in ("s3a://", "s3://"):
        if uri.startswith(prefix):
            rest = uri[len(prefix):]
            bucket, _, key_prefix = rest.partition("/")
            return bucket, key_prefix.strip("/")
    raise ValueError(f"Expected s3:// or s3a:// URI, got {uri!r}")


def arrow_type_to_trino(dtype: pa.DataType) -> str:
    if pa.types.is_boolean(dtype):
        return "BOOLEAN"
    if pa.types.is_int8(dtype):
        return "TINYINT"
    if pa.types.is_int16(dtype) or pa.types.is_uint8(dtype):
        return "SMALLINT"
    if pa.types.is_int32(dtype) or pa.types.is_uint16(dtype):
        return "INTEGER"
    if pa.types.is_int64(dtype) or pa.types.is_uint32(dtype) or pa.types.is_uint64(dtype):
        return "BIGINT"
    if pa.types.is_float32(dtype):
        return "REAL"
    if pa.types.is_float64(dtype):
        return "DOUBLE"
    if pa.types.is_decimal(dtype):
        return f"DECIMAL({dtype.precision}, {dtype.scale})"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "VARCHAR"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return "VARBINARY"
    raise ValueError(f"Unsupported Arrow column type for Trino DDL: {dtype}")


def infer_parquet_ddl(
    bucket: str,
    table_prefix: str,
    table_name: str,
    endpoint_url: str,
    ak: str,
    sk: str,
    region: str,
) -> tuple[str, bool]:
    """Inspect one Parquet object under s3://bucket/table_prefix to build Trino CREATE TABLE."""
    env = dict(os.environ)
    env["AWS_ACCESS_KEY_ID"] = ak
    env["AWS_SECRET_ACCESS_KEY"] = sk
    env["AWS_DEFAULT_REGION"] = region
    ls_cmd = [
        AWS_BIN,
        "s3",
        "ls",
        f"s3://{bucket}/{table_prefix.rstrip('/')}/",
        "--recursive",
        "--endpoint-url",
        endpoint_url,
    ]
    proc = subprocess.run(ls_cmd, env=env, capture_output=True, text=True, check=True)
    parquet_key = None
    has_day_partition = False
    for line in proc.stdout.splitlines():
        parts = line.split(None, 3)
        if len(parts) == 4 and parts[3].endswith(".parquet"):
            parquet_key = parts[3]
            if "/day=" in parquet_key or parquet_key.startswith("day="):
                has_day_partition = True
            break
    if not parquet_key:
        raise RuntimeError(f"No .parquet object found under s3://{bucket}/{table_prefix}/")

    with tempfile.TemporaryDirectory() as tmp:
        # Download a single file (or use s3api range if huge? Wait: single parquet file is ~30-200MB on LAN,
        # or fetch only tail 1 MiB + PAR1 magic to read Parquet footer in <0.1s!)
        local_tail = os.path.join(tmp, "sample.parquet")
        schema = fetch_parquet_schema_fast(bucket, parquet_key, endpoint_url, env, local_tail)

    cols = []
    for field in schema:
        if has_day_partition and field.name == "day":
            continue
        cols.append(f'"{field.name}" {arrow_type_to_trino(field.type)}')
    if has_day_partition:
        cols.append('"day" DATE')
    col_sql = ",\n  ".join(cols)
    ext_loc = f"s3://{bucket}/{table_prefix.rstrip('/')}"
    with_props = [f"external_location = '{ext_loc}'", "format = 'PARQUET'"]
    if has_day_partition:
        with_props.append("partitioned_by = ARRAY['day']")
    ddl = f"CREATE TABLE hive.bench.{table_name} (\n  {col_sql}\n) WITH (\n  " + ",\n  ".join(with_props) + "\n)"
    return ddl, has_day_partition


def fetch_parquet_schema_fast(bucket: str, key: str, endpoint_url: str, env: dict, tmp_path: str) -> pa.Schema:
    """Read Parquet schema by fetching only the last 2 MiB (or full object if <= 2 MiB) via s3api."""
    head = subprocess.run(
        [AWS_BIN, "s3api", "head-object", "--bucket", bucket, "--key", key, "--endpoint-url", endpoint_url],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    size = int(json.loads(head.stdout)["ContentLength"])
    tail_Window = 2 * 1024 * 1024
    if size <= tail_Window:
        subprocess.run(
            [AWS_BIN, "s3api", "get-object", "--bucket", bucket, "--key", key, "--endpoint-url", endpoint_url, tmp_path],
            env=env,
            stdout=subprocess.DEVNULL,
            check=True,
        )
        return pq.read_schema(tmp_path)

    # Fetch last 2 MiB, reconstruct minimal valid Parquet file [PAR1 + padding? No: footer offsets in PageIndex
    # are not dereferenced by pq.read_schema, only the footer length at [-8:-4] and magic PAR1 at [0:4] & [-4:]!]
    tail_file = tmp_path + ".tail"
    start = size - tail_Window
    subprocess.run(
        [
            AWS_BIN,
            "s3api",
            "get-object",
            "--bucket",
            bucket,
            "--key",
            key,
            "--range",
            f"bytes={start}-{size - 1}",
            "--endpoint-url",
            endpoint_url,
            tail_file,
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        check=True,
    )
    with open(tail_file, "rb") as fh:
        tail_bytes = fh.read()
    footer_len = int.from_bytes(tail_bytes[-8:-4], "little")
    if footer_len + 8 <= len(tail_bytes):
        minimal = b"PAR1" + tail_bytes[-(footer_len + 8):]
        with open(tmp_path, "wb") as out:
            out.write(minimal)
        return pq.read_schema(tmp_path)
    # Fallback if footer > 2 MiB
    subprocess.run(
        [AWS_BIN, "s3api", "get-object", "--bucket", bucket, "--key", key, "--endpoint-url", endpoint_url, tmp_path],
        env=env,
        stdout=subprocess.DEVNULL,
        check=True,
    )
    return pq.read_schema(tmp_path)


def _rewrite_pmod_hash(sql: str) -> str:
    # Convert Spark `pmod(hash(EXPR), MOD)` -> positive modulo on xxhash64 in Trino
    pattern = re.compile(r"pmod\(\s*hash\((.+?)\)\s*,\s*(\d+)\s*\)")
    while True:
        m = pattern.search(sql)
        if not m:
            break
        expr, mod = m.group(1), m.group(2)
        repl = f"(((from_big_endian_64(xxhash64(CAST({expr} AS VARBINARY))) % {mod}) + {mod}) % {mod})"
        sql = sql[:m.start()] + repl + sql[m.end():]
    # Remaining `hash(EXPR)` (e.g. Criteo Q20-24 hash_check)
    hpat = re.compile(r"\bhash\((concat_ws\([^;]+?\)\s*)\)")
    sql = hpat.sub(r"from_big_endian_64(xxhash64(CAST(\1 AS VARBINARY)))", sql)
    return sql


def to_trino_sql(sql: str) -> str:
    """Minimal ANSI dialect fixes so frozen benchmark queries execute identically on Trino."""
    s = sql.strip()
    if s.endswith(";"):
        s = s[:-1].strip()
    # ClickBench SELECT-alias in GROUP BY -> ANSI ordinal positions
    s = s.replace("GROUP BY UserID, m, SearchPhrase", "GROUP BY 1, 2, 3")
    s = s.replace("GROUP BY k HAVING", "GROUP BY 1 HAVING")
    s = s.replace(
        "GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst",
        "GROUP BY 1, 2, 3, 4, 5",
    )
    # ClickBench EventDate string literal comparisons -> ANSI DATE literals
    s = re.sub(
        r"\bEventDate\s*(>=|<=|=|<|>)\s*'(\d{4}-\d{2}-\d{2})'",
        r"EventDate \1 DATE '\2'",
        s,
    )
    # ANSI SQL requires OFFSET before LIMIT
    s = re.sub(r"\bLIMIT\s+(\d+)\s+OFFSET\s+(\d+)\b", r"OFFSET \2 LIMIT \1", s)
    # Spark STRING cast -> ANSI VARCHAR
    s = re.sub(r"\bAS\s+STRING\b", "AS VARCHAR", s)
    if "pmod(" in s or "hash(concat_ws" in s:
        s = _rewrite_pmod_hash(s)
    return s


def load_trino_queries(path: str, only=None) -> list[tuple[int, str]]:
    with open(path) as fh:
        raw = json.load(fh)
    queries = {int(k): v for k, v in raw.items() if str(k).isdigit()}
    nrs = sorted(queries)
    if only:
        nrs = [n for n in only if n in queries]
    return [(n, to_trino_sql(queries[n])) for n in nrs]


def trino_exec(port: int, sql: str, schema: str = "bench") -> list[list]:
    url = f"http://127.0.0.1:{port}/v1/statement"
    req = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        headers={
            "X-Trino-User": "benchmark",
            "X-Trino-Catalog": "hive",
            "X-Trino-Schema": schema,
            "X-Trino-Time-Zone": "UTC",
        },
        method="POST",
    )
    rows = []
    with urllib.request.urlopen(req, timeout=600) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    while True:
        if "error" in payload:
            err = payload["error"]
            raise RuntimeError(f"Trino {err.get('errorName')}: {err.get('message')}")
        if "data" in payload and payload["data"]:
            rows.extend(payload["data"])
        next_uri = payload.get("nextUri")
        if not next_uri:
            break
        with urllib.request.urlopen(next_uri, timeout=600) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    return rows


def wait_trino_ready(port: int, proc_dir: str, timeout_s: int = 90) -> None:
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/info", timeout=3) as resp:
                info = json.loads(resp.read().decode("utf-8"))
                if not info.get("starting", True):
                    active = trino_exec(
                        port,
                        "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'",
                        schema="default",
                    )
                    if active and active[0][0] >= 1:
                        # Force a distributed connector announcement check (not coordinator-local)
                        trino_exec(port, "SELECT count(*) FROM tpch.tiny.region", schema="default")
                        return
        except Exception as exc:
            last_err = exc
        time.sleep(1.0)
    log_file = os.path.join(proc_dir, "data", "var", "log", "server.log")
    tail = ""
    if os.path.isfile(log_file):
        with open(log_file, errors="replace") as fh:
            tail = "".join(fh.readlines()[-40:])
    raise RuntimeError(f"Trino failed to become ready in {timeout_s}s ({last_err})\n{tail}")


def configure_trino_instance(
    inst_dir: str,
    trino_home: str,
    port: int,
    server_memory_gb: int,
    endpoint_url: str,
    region: str,
    path_style: bool,
    ak: str,
    sk: str,
    run_io_dir: str,
    args,
) -> None:
    if os.path.exists(inst_dir):
        shutil.rmtree(inst_dir)
    os.makedirs(os.path.join(inst_dir, "etc", "catalog"), exist_ok=True)
    os.makedirs(os.path.join(inst_dir, "data"), exist_ok=True)
    os.makedirs(os.path.join(inst_dir, "metastore"), exist_ok=True)

    with open(os.path.join(inst_dir, "etc", "node.properties"), "w") as fh:
        fh.write(
            "node.environment=recasbench\n"
            f"node.id=trino-bench-node-1\n"
            f"node.data-dir={os.path.join(inst_dir, 'data')}\n"
        )

    max_query_mem = max(4, int(server_memory_gb * 0.65))
    with open(os.path.join(inst_dir, "etc", "config.properties"), "w") as fh:
        fh.write(
            "coordinator=true\n"
            "node-scheduler.include-coordinator=true\n"
            f"http-server.http.port={port}\n"
            f"discovery.uri=http://127.0.0.1:{port}\n"
            f"query.max-memory={max_query_mem}GB\n"
            f"query.max-memory-per-node={max_query_mem}GB\n"
        )

    probe_dir = os.path.join(run_io_dir, "track1-probe")
    os.makedirs(probe_dir, exist_ok=True)
    agent_jar = os.path.join(trino_home, AGENT_JAR_IN_PLUGIN)
    jvm_lines = [
        "-server",
        f"-Xmx{server_memory_gb}G",
        "-XX:+UseG1GC",
        "-XX:G1HeapRegionSize=32M",
        "-XX:+ExplicitGCInvokesConcurrent",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-Djdk.attach.allowAttachSelf=true",
        "-XX:+UnlockDiagnosticVMOptions",
        f"-javaagent:{agent_jar}",
        "-Dtrack1.sdk.auto=true",
        f"-Dtrack2.collector.dir={run_io_dir}",
        f"-Dtrack1.s3a.probe.dir={probe_dir}",
        f"-Dtrack1.d1={str(bool(args.track1_d1)).lower()}",
        f"-Dtrack1.d1.adaptive={str(bool(args.track1_d1_adaptive)).lower()}",
        f"-Dtrack1.d1.fixed.capacity={str(bool(args.track1_d1_fixed_capacity)).lower()}",
        f"-Dtrack1.d1.admit.bytes={int(args.track1_d1_admit_bytes)}",
        f"-Dtrack1.d1.cache.mib={int(args.track1_d1_cache_mib)}",
        f"-Dtrack1.d1.zero.copy={str(bool(args.track1_d1_zero_copy)).lower()}",
        f"-Dtrack1.d1.doorkeeper={str(bool(args.track1_d1_doorkeeper)).lower()}",
        f"-Dtrack1.d1.shared.backing={str(bool(args.track1_d1_shared_backing)).lower()}",
    ]
    if args.track1_d1_hard_mib is not None:
        jvm_lines.append(f"-Dtrack1.d1.hard.mib={int(args.track1_d1_hard_mib)}")
    if args.track1_d1_policy:
        jvm_lines.append(f"-Dtrack1.d1.policy={args.track1_d1_policy}")
    if args.periodic_gc_ms > 0:
        jvm_lines.append(f"-XX:G1PeriodicGCInterval={int(args.periodic_gc_ms)}")
        jvm_lines.append("-XX:-G1PeriodicGCInvokesConcurrent")

    with open(os.path.join(inst_dir, "etc", "jvm.config"), "w") as fh:
        fh.write("\n".join(jvm_lines) + "\n")

    with open(os.path.join(inst_dir, "etc", "catalog", "tpch.properties"), "w") as fh:
        fh.write("connector.name=tpch\n")

    metastore_uri = f"file://{os.path.join(inst_dir, 'metastore')}"
    with open(os.path.join(inst_dir, "etc", "catalog", "hive.properties"), "w") as fh:
        fh.write(
            "connector.name=hive\n"
            "hive.metastore=file\n"
            f"hive.metastore.catalog.dir={metastore_uri}\n"
            "hive.non-managed-table-creates-enabled=true\n"
            "hive.parquet.use-column-names=true\n"
            "hive.parquet.time-zone=UTC\n"
            "parquet.use-column-index=true\n"
            "parquet.use-bloom-filter=true\n"
            "fs.hadoop.enabled=true\n"
            "fs.native-s3.enabled=true\n"
            f"s3.endpoint={endpoint_url}\n"
            f"s3.region={region}\n"
            f"s3.aws-access-key={ak}\n"
            f"s3.aws-secret-key={sk}\n"
            f"s3.path-style-access={str(bool(path_style)).lower()}\n"
            "s3.max-connections=500\n"
        )


def trino_launcher(trino_home: str, inst_dir: str, jdk_home: str, action: str) -> None:
    env = dict(os.environ)
    env["JAVA_HOME"] = jdk_home
    env["PATH"] = f"{os.path.join(jdk_home, 'bin')}:{env.get('PATH', '')}"
    launcher = os.path.join(trino_home, "bin", "launcher")
    cmd = [
        launcher,
        f"--etc-dir={os.path.join(inst_dir, 'etc')}",
        f"--data-dir={os.path.join(inst_dir, 'data')}",
        action,
    ]
    subprocess.run(cmd, env=env, check=(action == "start"))


def run_once(args, run_id: int, queries: list[tuple[int, str]], ddls: list[tuple[str, str, bool]], collector_dir: str):
    dropped = rb.drop_os_page_cache()
    run_io_dir = os.path.join(collector_dir, f"run-{run_id}")
    os.makedirs(run_io_dir, exist_ok=True)
    inst_dir = os.path.join(args.scratch_dir, f"trino-inst-{args.port}")

    ak, sk = s3a_session.load_creds()
    configure_trino_instance(
        inst_dir=inst_dir,
        trino_home=args.trino_home,
        port=args.port,
        server_memory_gb=args.server_memory_gb,
        endpoint_url=args.endpoint_url,
        region=args.region,
        path_style=args.path_style,
        ak=ak,
        sk=sk,
        run_io_dir=run_io_dir,
        args=args,
    )

    trino_launcher(args.trino_home, inst_dir, args.jdk_home, "start")
    rows = []
    try:
        wait_trino_ready(args.port, inst_dir)
        trino_exec(args.port, "CREATE SCHEMA IF NOT EXISTS hive.bench", schema="default")
        for table_name, ddl, partitioned in ddls:
            trino_exec(args.port, ddl, schema="default")
            if partitioned:
                trino_exec(
                    args.port,
                    f"CALL hive.system.sync_partition_metadata('bench', '{table_name}', 'FULL')",
                    schema="bench",
                )
        # Mark t_session AFTER schema/DDL registration so table setup S3 checks are excluded
        t_session = time.time()
        # Wipe any setup-time NDJSON lines if created before queries start
        for f in os.listdir(run_io_dir):
            if f.endswith(".ndjson"):
                open(os.path.join(run_io_dir, f), "w").close()

        for qnr, sql in queries:
            t0 = time.time()
            err = None
            nout = None
            result_hash = None
            for attempt in range(15):
                try:
                    t0 = time.time()
                    result = trino_exec(args.port, sql, schema="bench")
                    nout = len(result)
                    result_hash = rb.result_sha256(result)
                    err = None
                    break
                except Exception as exc:
                    msg = f"{type(exc).__name__}: {exc}"
                    if "NO_NODES_AVAILABLE" in msg and attempt < 14:
                        time.sleep(1.0)
                        continue
                    err = msg
                    break
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
            print(f"  run {run_id}  Q{qnr:02d}  {elapsed:8.2f}s  rows={nout}  {status}", flush=True)
            if err:
                print(f"           {err[:300]}", flush=True)
        session_s = time.time() - t_session
    finally:
        trino_launcher(args.trino_home, inst_dir, args.jdk_home, "stop")
        time.sleep(1.0)

    track1 = rb.aggregate_track1_stats(run_io_dir, None)
    io = rb.io_stats(run_io_dir, 0)
    shutil.rmtree(inst_dir, ignore_errors=True)
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


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True, help="S3/S3A layout root (e.g. s3a://bucket/sf8)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--queries-file", default=rb.DEFAULT_QUERIES)
    ap.add_argument("--tables", nargs="*", default=None)
    ap.add_argument("--queries", default=None, help="comma-separated query numbers")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--layout-id", default="baseline")
    ap.add_argument("--server-memory-gb", type=int, default=48)
    ap.add_argument("--port", type=int, default=18080)
    ap.add_argument("--endpoint-url", default="https://cos.ap-guangzhou.myqcloud.com")
    ap.add_argument("--region", default="ap-guangzhou")
    ap.add_argument("--path-style", action="store_true")
    ap.add_argument("--trino-home", default=TRINO_HOME_DEFAULT)
    ap.add_argument("--jdk-home", default=JDK17_DEFAULT)
    ap.add_argument("--scratch-dir", default=os.path.expanduser("~/track2-scratch"))
    ap.add_argument("--periodic-gc-ms", type=int, default=30000)
    ap.add_argument("--track1-d1", action="store_true")
    ap.add_argument("--track1-d1-adaptive", action="store_true")
    ap.add_argument("--track1-d1-fixed-capacity", action="store_true")
    ap.add_argument("--track1-d1-admit-bytes", type=int, default=0)
    ap.add_argument("--track1-d1-cache-mib", type=int, default=4096)
    ap.add_argument("--track1-d1-hard-mib", type=int, default=4096)
    ap.add_argument("--track1-d1-policy", default="")
    ap.add_argument("--no-track1-d1-zero-copy", action="store_false", dest="track1_d1_zero_copy")
    ap.add_argument("--track1-d1-doorkeeper", action="store_true", default=False)
    ap.add_argument("--no-track1-d1-shared-backing", action="store_false", dest="track1_d1_shared_backing")
    ap.add_argument("--keep-io", action="store_true")
    args = ap.parse_args()

    if args.tables is None:
        if "clickbench" in args.queries_file:
            args.tables = ["hits"]
        elif "criteo" in args.queries_file:
            args.tables = ["criteo"]
        elif "novomcp" in args.queries_file:
            args.tables = ["compounds"]
        else:
            args.tables = rb.TPCH_TABLES

    only = [int(x) for x in args.queries.split(",")] if args.queries else None
    queries = load_trino_queries(args.queries_file, only)
    bucket, root_prefix = parse_s3_uri(args.data)
    ak, sk = s3a_session.load_creds()

    print(f"# Inferring Parquet DDLs under s3://{bucket}/{root_prefix} ...", flush=True)
    ddls = []
    for tbl in args.tables:
        t_prefix = f"{root_prefix}/{tbl}" if root_prefix else tbl
        ddl, partitioned = infer_parquet_ddl(
            bucket, t_prefix, tbl, args.endpoint_url, ak, sk, args.region
        )
        ddls.append((tbl, ddl, partitioned))

    os.makedirs(args.out, exist_ok=True)
    collector_dir = os.path.join(args.out, "io")
    if not args.keep_io and os.path.isdir(collector_dir):
        shutil.rmtree(collector_dir)
    os.makedirs(collector_dir, exist_ok=True)

    print(
        f"# Trino benchmark  layout={args.layout_id}  runs={args.runs}  "
        f"queries={[n for n, _ in queries]}  data={args.data}  d1={args.track1_d1}",
        flush=True,
    )
    runs = []
    for i in range(1, args.runs + 1):
        print(f"\n== run {i}/{args.runs} ==", flush=True)
        runs.append(run_once(args, i, queries, ddls, collector_dir))

    summary = rb.summarize(runs)
    report = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "engine": "trino-435",
        "layout_id": args.layout_id,
        "data": args.data,
        "server_memory_gb": args.server_memory_gb,
        "runs": runs,
        "summary": summary,
    }
    with open(os.path.join(args.out, "report.json"), "w") as fh:
        json.dump(report, fh, indent=2)
    with open(os.path.join(args.out, "per_query.csv"), "w", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=["query", "n", "median_s", "mean_s", "min_s", "max_s", "stdev_s", "n_errors"],
        )
        w.writeheader()
        for q in summary["per_query"]:
            w.writerow({
                **{k: q[k] for k in ("query", "n", "median_s", "mean_s", "min_s", "max_s", "stdev_s")},
                "n_errors": len(q["errors"]),
            })

    print(f"\n# summary (Trino)")
    print(f"  end-to-end s   {summary['end_to_end_s']}")
    print(f"  median         {summary['median_s']:.2f}s")
    print(f"  cv             {summary['cv']*100:.2f}%")
    print(f"  report         {os.path.join(args.out, 'report.json')}")
    return 0 if not any(q["errors"] for q in summary["per_query"]) else 1


if __name__ == "__main__":
    sys.exit(main())
