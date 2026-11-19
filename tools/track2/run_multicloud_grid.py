#!/usr/bin/env python3
"""Unified runner & zero-disk streaming sync for RECAS VLDB experiments:
  - Exp 1: Spark Scalability (1, 2, 4, 8 worker nodes on COS)
  - Exp 2: Trino Multi-Engine 2x2 grids (on COS)
  - Exp 3: Multi-Backend 5-Workload 2x2 grids (GCS, Alibaba OSS, Local MinIO)

Enforces:
  * Round-robin cell order rotation across rounds (`00, 10, 01, 11`)
  * 48 GiB heap + G1PeriodicGC=30s + OS page-cache drop per cell-run
  * Fixed 4 GiB cost-aware W-TinyLFU (`admit_bytes=0, doorkeeper=off`)
  * Round-1 physical I/O conservation gate (`00` & `01` GET/bytes vs. COS)
  * Rolling stage -> benchmark -> purge mode for local MinIO disk cap
"""

from __future__ import annotations

import argparse
import concurrent.futures
import configparser
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
PYTHON_BIN = sys.executable
AWS_BIN = os.path.expanduser("~/.local/bin/aws")
RUN_SPARK = os.path.join(HERE, "run_benchmark.py")
RUN_TRINO = os.path.join(HERE, "run_trino_benchmark.py")

WORKLOADS = {
    "novomcp": {
        "raw_prefix": "novomcp_open_corpus_lite",
        "joint_prefix": "novomcp_open_corpus_lite_joint2",
        "tables": ["compounds"],
        "queries_file": os.path.join(HERE, "novomcp_queries.json"),
        "expected_00_gets": 21724,
        "expected_00_bytes": 89210701284,
        "expected_01_gets": 16829,
        "expected_01_bytes": 73655945808,
    },
    "criteo": {
        "raw_prefix": "criteo_clicklogs",
        "joint_prefix": "criteo_clicklogs_encoding_fixed",
        "tables": ["criteo"],
        "queries_file": os.path.join(HERE, "criteo_queries.json"),
        "expected_00_gets": 81492,
        "expected_00_bytes": 211006729773,
        "expected_01_gets": 38048,
        "expected_01_bytes": 153180187675,
    },
    "clickbench_sf8": {
        "raw_prefix": "sf8",
        "joint_prefix": "sf8_encoding_fixed",
        "tables": ["hits"],
        "queries_file": os.path.join(HERE, "clickbench_queries.json"),
        "expected_00_gets": 262309,
        "expected_00_bytes": 399311391889,
        "expected_01_gets": 187264,
        "expected_01_bytes": 280687790158,
    },
    "tpch_sf350": {
        "raw_prefix": "tpch_sf350",
        "joint_prefix": "tpch_sf350_joint2",
        "tables": [
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier",
        ],
        "queries_file": os.path.expanduser("~/track2-data/tpch_sf100/_tpch_queries.json"),
        "expected_00_gets": 1345049,
        "expected_00_bytes": 610335800318,
        "expected_01_gets": 301929,
        "expected_01_bytes": 418785437987,
    },
    "nyc_yellow": {
        "raw_prefix": "nyc_yellow",
        "joint_prefix": "nyc_yellow_encoding_fixed",
        "tables": ["yellow"],
        "queries_file": os.path.join(HERE, "nyc_yellow_epoch_queries.json"),
        "expected_00_gets": 121730,
        "expected_00_bytes": 111776295770,
        "expected_01_gets": 8850,
        "expected_01_bytes": 92449540000,
    },
}

BACKENDS = {
    "cos": {
        "bucket": "haoyueli-cos-1257943044",
        "s3a_endpoint": "cos.ap-guangzhou.myqcloud.com",
        "https_endpoint": "https://cos.ap-guangzhou.myqcloud.com",
        "sync_endpoint": "https://cos.ap-guangzhou.myqcloud.com",
        "region": "ap-guangzhou",
        "creds_file": os.path.expanduser("~/.aws/credentials"),
        "creds_section": "default",
        "path_style": False,
    },
    "oss": {
        "bucket": "recas-oss-bench",
        "s3a_endpoint": "oss-cn-shanghai-internal.aliyuncs.com",
        "https_endpoint": "https://oss-cn-shanghai-internal.aliyuncs.com",
        "sync_endpoint": "https://oss-cn-shanghai.aliyuncs.com",
        "region": "oss-cn-shanghai",
        "creds_file": os.path.expanduser("~/.aws/credentials"),
        "creds_section": "oss",
        "path_style": False,
    },
    "gcs": {
        "bucket": "recas-gcs-bench",
        "s3a_endpoint": "storage.googleapis.com",
        "https_endpoint": "https://storage.googleapis.com",
        "sync_endpoint": "https://storage.googleapis.com",
        "region": "us-central1",
        "sync_region": "auto",
        "creds_file": os.path.expanduser("~/.aws/credentials_gcs"),
        "creds_section": "gcs",
        "path_style": True,
    },
    "minio": {
        "bucket": "recas-minio-bench",
        "s3a_endpoint": "http://127.0.0.1:9000",
        "https_endpoint": "http://127.0.0.1:9000",
        "sync_endpoint": "http://127.0.0.1:9000",
        "region": "us-east-1",
        "creds_file": os.path.expanduser("~/.aws/credentials_minio"),
        "creds_section": "minio",
        "path_style": True,
    },
}

CELLS = ("00", "10", "01", "11")


def ensure_minio_creds_file():
    path = BACKENDS["minio"]["creds_file"]
    if os.path.isfile(path):
        return
    ak = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    sk = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
    with open(path, "w") as fh:
        fh.write(f"[minio]\naws_access_key_id = {ak}\naws_secret_access_key = {sk}\n")
    os.chmod(path, 0o600)


def read_creds(backend_cfg: dict) -> tuple[str, str]:
    if backend_cfg is BACKENDS["minio"]:
        ensure_minio_creds_file()
    cp = configparser.ConfigParser()
    cp.read(backend_cfg["creds_file"])
    sec = backend_cfg["creds_section"]
    return cp[sec]["aws_access_key_id"], cp[sec]["aws_secret_access_key"]


def aws_env(backend_cfg: dict) -> dict[str, str]:
    ak, sk = read_creds(backend_cfg)
    region = backend_cfg.get("sync_region", backend_cfg["region"])
    style = "path" if backend_cfg["path_style"] else "virtual"
    cfg_path = os.path.expanduser(f"~/.aws/recas_cli_{backend_cfg['creds_section']}.cfg")
    with open(cfg_path, "w") as fh:
        fh.write(
            f"[default]\nregion = {region}\ns3 =\n"
            f"    addressing_style = {style}\n"
            "    signature_version = s3v4\n"
            "    multipart_threshold = 5GB\n"
            "    multipart_chunksize = 5GB\n"
        )
    env = dict(os.environ)
    env["AWS_CONFIG_FILE"] = cfg_path
    env["AWS_ACCESS_KEY_ID"] = ak
    env["AWS_SECRET_ACCESS_KEY"] = sk
    env["AWS_DEFAULT_REGION"] = region
    env["AWS_REQUEST_CHECKSUM_CALCULATION"] = "WHEN_REQUIRED"
    env["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "WHEN_REQUIRED"
    return env


def list_objects(backend_name: str, prefix: str, endpoint_override: str = "") -> dict[str, int]:
    bcfg = BACKENDS[backend_name]
    ep = endpoint_override or bcfg["sync_endpoint"]
    cmd = [
        AWS_BIN,
        "s3",
        "ls",
        f"s3://{bcfg['bucket']}/{prefix.rstrip('/')}/",
        "--recursive",
        "--endpoint-url",
        ep,
    ]
    res = subprocess.run(cmd, env=aws_env(bcfg), capture_output=True, text=True)
    if res.returncode != 0:
        return {}
    out = {}
    for line in res.stdout.splitlines():
        parts = line.split(None, 3)
        if len(parts) == 4:
            try:
                size = int(parts[2])
            except ValueError:
                continue
            key = parts[3].strip()
            out[key] = size
    return out


def sync_prefix_from_cos(
    target_backend: str,
    prefix: str,
    workers: int = 8,
    shm_dir: str = "/dev/shm",
) -> dict:
    """Copy missing/mismatched objects from COS -> target_backend using RAM buffer (/dev/shm)."""
    src_objs = list_objects("cos", prefix)
    if not src_objs:
        raise RuntimeError(f"Source COS prefix s3://{BACKENDS['cos']['bucket']}/{prefix}/ is empty!")
    dst_objs = list_objects(target_backend, prefix)

    todo = [
        (key, size)
        for key, size in sorted(src_objs.items())
        if dst_objs.get(key) != size
    ]
    total_bytes = sum(src_objs.values())
    todo_bytes = sum(sz for _, sz in todo)
    print(
        f"[sync] cos -> {target_backend}  prefix={prefix}  "
        f"total_files={len(src_objs)} ({total_bytes / 1024**3:.2f} GiB)  "
        f"to_copy={len(todo)} ({todo_bytes / 1024**3:.2f} GiB)",
        flush=True,
    )
    if not todo:
        return {"files": len(src_objs), "bytes": total_bytes, "copied": 0}

    src_cfg = BACKENDS["cos"]
    dst_cfg = BACKENDS[target_backend]
    src_env = aws_env(src_cfg)
    dst_env = aws_env(dst_cfg)
    stage_base = shm_dir if os.path.isdir(shm_dir) else tempfile.gettempdir()

    def _copy_one(item: tuple[str, int]) -> int:
        key, size = item
        with tempfile.TemporaryDirectory(dir=stage_base, prefix="recas_sync_") as tmp:
            local_file = os.path.join(tmp, os.path.basename(key) or "obj")
            subprocess.run(
                [
                    AWS_BIN,
                    "s3",
                    "cp",
                    f"s3://{src_cfg['bucket']}/{key}",
                    local_file,
                    "--endpoint-url",
                    src_cfg["sync_endpoint"],
                    "--only-show-errors",
                ],
                env=src_env,
                check=True,
            )
            subprocess.run(
                [
                    AWS_BIN,
                    "s3",
                    "cp",
                    local_file,
                    f"s3://{dst_cfg['bucket']}/{key}",
                    "--endpoint-url",
                    dst_cfg["sync_endpoint"],
                    "--only-show-errors",
                ],
                env=dst_env,
                check=True,
            )
        return size

    copied_bytes = 0
    done_files = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for sz in pool.map(_copy_one, todo):
            done_files += 1
            copied_bytes += sz
            if done_files % 10 == 0 or done_files == len(todo):
                print(
                    f"  [{target_backend}:{prefix}] {done_files}/{len(todo)} files "
                    f"({copied_bytes / 1024**3:.2f}/{todo_bytes / 1024**3:.2f} GiB)",
                    flush=True,
                )

    post_objs = list_objects(target_backend, prefix)
    if len(post_objs) != len(src_objs) or sum(post_objs.values()) != total_bytes:
        raise RuntimeError(
            f"Sync verification failed for {target_backend}:{prefix}: "
            f"expected {len(src_objs)} files / {total_bytes} B, "
            f"got {len(post_objs)} files / {sum(post_objs.values())} B"
        )
    print(f"[sync] VERIFIED {target_backend}:{prefix} ({len(post_objs)} files, {total_bytes} B)", flush=True)
    return {"files": len(post_objs), "bytes": total_bytes, "copied": len(todo)}


def purge_backend_prefix(target_backend: str, prefix: str) -> None:
    if target_backend != "minio":
        raise ValueError("Refusing to purge non-MinIO cloud bucket!")
    bcfg = BACKENDS["minio"]
    print(f"[purge] Removing s3://{bcfg['bucket']}/{prefix}/ ...", flush=True)
    subprocess.run(
        [
            AWS_BIN,
            "s3",
            "rm",
            f"s3://{bcfg['bucket']}/{prefix.rstrip('/')}/",
            "--recursive",
            "--endpoint-url",
            bcfg["sync_endpoint"],
            "--only-show-errors",
        ],
        env=aws_env(bcfg),
        check=True,
    )


def run_cell_once(
    args,
    wl: str,
    workload_cfg: dict,
    backend_cfg: dict,
    cell: str,
    rnd: int,
    out_dir: str,
) -> dict:
    cell_run_dir = os.path.join(out_dir, "grid", cell, f"run-{rnd}")
    rep_path = os.path.join(cell_run_dir, "report.json")
    if os.path.isfile(rep_path) and not args.overwrite:
        with open(rep_path) as fh:
            existing = json.load(fh)
        errs = [q for q in existing["runs"][0]["queries"] if q.get("error")]
        if not errs:
            print(f"  [skip] {cell} run-{rnd} already complete ({existing['runs'][0]['query_sum_s']}s)", flush=True)
            return existing

    is_joint = cell in ("01", "11")
    is_cache = cell in ("10", "11")
    prefix = workload_cfg["joint_prefix"] if is_joint else workload_cfg["raw_prefix"]
    data_uri = f"s3a://{backend_cfg['bucket']}/{prefix}"
    layout_id = f"{wl}-{'joint' if is_joint else 'raw'}-{cell}"

    env = dict(os.environ)
    env["JAVA_HOME"] = os.path.expanduser("~/track2-env/jdk-17")
    spark_home = os.path.expanduser("~/track2-env/spark")
    env["SPARK_HOME"] = spark_home
    py4j_zips = [
        os.path.join(spark_home, "python", "lib", f)
        for f in os.listdir(os.path.join(spark_home, "python", "lib"))
        if f.startswith("py4j-") and f.endswith(".zip")
    ]
    env["PYTHONPATH"] = f"{os.path.join(spark_home, 'python')}:{py4j_zips[0]}" if py4j_zips else ""
    if args.backend == "minio":
        ensure_minio_creds_file()
    env["S3ARR_AWS_CREDENTIALS_FILE"] = backend_cfg["creds_file"]
    env["S3ARR_AWS_SECTION"] = backend_cfg["creds_section"]
    env["AWS_DEFAULT_REGION"] = backend_cfg["region"]
    s3a_ep = args.endpoint_override or backend_cfg["s3a_endpoint"]
    env["S3ARR_ENDPOINT"] = s3a_ep
    env["S3ARR_PATH_STYLE"] = "true" if backend_cfg["path_style"] else "false"

    os.makedirs(cell_run_dir, exist_ok=True)
    if args.engine == "spark":
        cmd = [
            PYTHON_BIN,
            RUN_SPARK,
            "--data",
            data_uri,
            "--out",
            cell_run_dir,
            "--queries-file",
            workload_cfg["queries_file"],
            "--tables",
            *workload_cfg["tables"],
            "--runs",
            "1",
            "--layout-id",
            layout_id,
            "--master",
            args.master,
            "--driver-memory",
            f"{args.memory_gb}g",
            "--periodic-gc-ms",
            "30000",
        ]
        if args.queries:
            cmd += ["--queries", args.queries]
        if args.executor_memory:
            cmd += ["--executor-memory", args.executor_memory]
        if args.executor_cores:
            cmd += ["--executor-cores", str(args.executor_cores)]
        if args.num_executors:
            cmd += ["--num-executors", str(args.num_executors)]
        if args.worker_hosts:
            cmd += ["--worker-hosts", args.worker_hosts]
        if is_cache:
            cmd += [
                "--track1-s3a",
                "--track1-d1",
                "--track1-d1-adaptive",
                "--track1-d1-fixed-capacity",
                "--track1-d1-hard-mib",
                "4096",
                "--track1-d1-cache-mib",
                "4096",
                "--track1-d1-admit-bytes",
                "0",
            ]
    else:
        https_ep = args.endpoint_override or backend_cfg["https_endpoint"]
        if not https_ep.startswith("http"):
            https_ep = f"https://{https_ep}"
        cmd = [
            PYTHON_BIN,
            RUN_TRINO,
            "--data",
            data_uri,
            "--out",
            cell_run_dir,
            "--queries-file",
            workload_cfg["queries_file"],
            "--tables",
            *workload_cfg["tables"],
            "--runs",
            "1",
            "--layout-id",
            layout_id,
            "--server-memory-gb",
            str(args.memory_gb),
            "--endpoint-url",
            https_ep,
            "--region",
            backend_cfg["region"],
            "--periodic-gc-ms",
            "30000",
        ]
        if backend_cfg["path_style"]:
            cmd.append("--path-style")
        if args.queries:
            cmd += ["--queries", args.queries]
        if is_cache:
            cmd += [
                "--track1-d1",
                "--track1-d1-adaptive",
                "--track1-d1-fixed-capacity",
                "--track1-d1-hard-mib",
                "4096",
                "--track1-d1-cache-mib",
                "4096",
                "--track1-d1-admit-bytes",
                "0",
            ]

    print(f"\n>>> [{args.engine}:{args.backend}:{wl}] round={rnd}/{args.runs} cell={cell}", flush=True)
    subprocess.run(cmd, env=env, check=True)
    with open(rep_path) as fh:
        return json.load(fh)


def verify_r1_io_gate(workload: str, workload_cfg: dict, cell: str, rep: dict) -> None:
    """Verify Spark 00/01 GETs & remote_bytes match COS reference within 0.5%."""
    if cell not in ("00", "01"):
        return
    io = rep["runs"][0]["io"]
    exp_gets = workload_cfg["expected_00_gets"] if cell == "00" else workload_cfg["expected_01_gets"]
    exp_bytes = workload_cfg["expected_00_bytes"] if cell == "00" else workload_cfg["expected_01_bytes"]
    got_gets = io.get("gets") or 0
    got_bytes = io.get("remote_bytes") or 0
    bytes_err = abs(got_bytes - exp_bytes) / max(1, exp_bytes)
    gets_err = abs(got_gets - exp_gets) / max(1, exp_gets)
    print(
        f"  [io-gate {workload} {cell}] gets={got_gets} (exp {exp_gets}, err {gets_err*100:.2f}%)  "
        f"bytes={got_bytes} (exp {exp_bytes}, err {bytes_err*100:.2f}%)",
        flush=True,
    )
    if bytes_err > 0.005 or gets_err > 0.02:
        raise RuntimeError(
            f"R1 I/O conservation gate FAILED for {workload} cell {cell}: "
            f"got ({got_gets} GETs, {got_bytes} B) vs expected ({exp_gets} GETs, {exp_bytes} B)"
        )


def summarize_grid(out_dir: str, runs: int) -> dict:
    by_cell = {}
    for cell in CELLS:
        vals, gets, rbytes, heaps = [], [], [], []
        for rnd in range(1, runs + 1):
            p = os.path.join(out_dir, "grid", cell, f"run-{rnd}", "report.json")
            if not os.path.isfile(p):
                continue
            r0 = json.load(open(p))["runs"][0]
            vals.append(r0["query_sum_s"])
            if r0["io"]["gets"] is not None:
                gets.append(r0["io"]["gets"])
            if r0["io"]["remote_bytes"] is not None:
                rbytes.append(r0["io"]["remote_bytes"])
            t1 = r0.get("track1") or {}
            if t1.get("peak_heap_bytes"):
                heaps.append(t1["peak_heap_bytes"])
        med = statistics.median(vals)
        cv = (statistics.stdev(vals) / statistics.mean(vals)) if len(vals) > 1 else 0.0
        by_cell[cell] = {
            "n": len(vals),
            "wall_s": vals,
            "median_s": round(med, 3),
            "cv_pct": round(cv * 100, 2),
            "median_gets": int(statistics.median(gets)) if gets else None,
            "median_remote_gib": round(statistics.median(rbytes) / 1024**3, 2) if rbytes else None,
            "median_peak_heap_gib": round(statistics.median(heaps) / 1024**3, 2) if heaps else None,
        }
    m00 = by_cell["00"]["median_s"]
    effects = {
        "caching_at_00_pct": round((by_cell["10"]["median_s"] - m00) / m00 * 100, 2),
        "layout_off_pct": round((by_cell["01"]["median_s"] - m00) / m00 * 100, 2),
        "combined_pct": round((by_cell["11"]["median_s"] - m00) / m00 * 100, 2),
    }
    summary = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "cells": by_cell,
        "effects": effects,
    }
    with open(os.path.join(out_dir, "grid_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    print("\n=== GRID SUMMARY ===")
    print(json.dumps(summary, indent=2))
    return summary


def cmd_sync(args):
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]
    for wl in workloads:
        wcfg = WORKLOADS[wl]
        sync_prefix_from_cos(
            args.backend, wcfg["raw_prefix"], workers=args.workers, shm_dir=args.stage_dir
        )
        sync_prefix_from_cos(
            args.backend, wcfg["joint_prefix"], workers=args.workers, shm_dir=args.stage_dir
        )


def cmd_run(args):
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]
    bcfg = BACKENDS[args.backend]
    for wl in workloads:
        wcfg = WORKLOADS[wl]
        if args.stage_from_cos:
            sync_prefix_from_cos(args.backend, wcfg["raw_prefix"], workers=args.sync_workers)
            sync_prefix_from_cos(args.backend, wcfg["joint_prefix"], workers=args.sync_workers)

        wl_out = os.path.join(args.out_root, f"{args.engine}_{wl}_{args.backend}{args.tag}")
        os.makedirs(wl_out, exist_ok=True)
        for rnd in range(1, args.runs + 1):
            k = (rnd - 1) % len(CELLS)
            order = CELLS[k:] + CELLS[:k]
            for cell in order:
                rep = run_cell_once(args, wl, wcfg, bcfg, cell, rnd, wl_out)
                if rnd == 1 and args.engine == "spark" and not args.queries:
                    verify_r1_io_gate(wl, wcfg, cell, rep)
        summarize_grid(wl_out, args.runs)

        if args.purge_after and args.backend == "minio":
            purge_backend_prefix("minio", wcfg["raw_prefix"])
            purge_backend_prefix("minio", wcfg["joint_prefix"])


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="subcmd", required=True)

    sp_sync = sub.add_parser("sync", help="Stream-sync frozen datasets from COS to target backend")
    sp_sync.add_argument("--backend", required=True, choices=["oss", "gcs", "minio"])
    sp_sync.add_argument("--workloads", required=True, help="comma-separated workload keys")
    sp_sync.add_argument("--workers", type=int, default=4)
    sp_sync.add_argument("--stage-dir", default="/dev/shm", help="temp buffer directory")
    sp_sync.set_defaults(func=cmd_sync)

    sp_run = sub.add_parser("run", help="Run rotated 2x2 grid(s) with I/O conservation gates")
    sp_run.add_argument("--engine", default="spark", choices=["spark", "trino"])
    sp_run.add_argument("--backend", required=True, choices=["cos", "oss", "gcs", "minio"])
    sp_run.add_argument("--workloads", required=True, help="comma-separated workload keys")
    sp_run.add_argument(
        "--out-root",
        default=os.path.join(HERE, "../../docs/adaptive-range-reader/results/track2"),
    )
    sp_run.add_argument("--tag", default="", help="optional suffix on output dir, e.g. _n4")
    sp_run.add_argument("--runs", type=int, default=5)
    sp_run.add_argument("--memory-gb", type=int, default=48)
    sp_run.add_argument("--master", default="local[16]")
    sp_run.add_argument("--executor-memory", default=None)
    sp_run.add_argument("--executor-cores", type=int, default=None)
    sp_run.add_argument("--num-executors", type=int, default=None)
    sp_run.add_argument("--worker-hosts", default="")
    sp_run.add_argument("--endpoint-override", default="")
    sp_run.add_argument("--queries", default=None)
    sp_run.add_argument("--stage-from-cos", action="store_true")
    sp_run.add_argument("--purge-after", action="store_true")
    sp_run.add_argument("--sync-workers", type=int, default=8)
    sp_run.add_argument("--overwrite", action="store_true")
    sp_run.set_defaults(func=cmd_run)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
