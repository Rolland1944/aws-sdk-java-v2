#!/usr/bin/env python3
"""
generate_tpch.py —— 用 DuckDB dbgen 生成 TPC-H Parquet 数据集（PROJECT3 §6.3 / E5）

设计要点：
  * **分步生成**：用 dbgen 的 (children, step) 把大表切成 N 份，每份单独 COPY 成一个
    parquet 文件后立刻丢掉表。这样 (a) 内存占用与 SF 无关，(b) 产出天然是"一个表多个
    文件"的真实湖仓布局，(c) 中断可续跑。
  * **row group 粒度默认交给 DuckDB**：PROJECT2 §5 刻画的 SF1 lineitem 是 49 个 row
    group，对应 DuckDB 默认的 122880 行/组。保持默认才能和上一阶段的热力图口径可比。
  * 采 trace 不需要连 S3：parquet 的 offset 由格式决定，本地读出来的访问 pattern 与
    字节躺在 S3 上一致（见 logging_fs.py 开头的说明）。

用法：
    # 标定：SF1 各压缩格式实际字节数，据此推算达到 100GB 需要多大 SF
    python scripts/data_collect/generate_tpch.py --sf 1 --children 4 \
        --out /mnt/nvme/data/tpch/calib_snappy --compression snappy

    # 正式生成
    python scripts/data_collect/generate_tpch.py --sf 300 --children 100 \
        --out /mnt/nvme/data/tpch/sf300 --compression snappy \
        --temp-dir /mnt/nvme/tmp --memory-limit 32GB
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time

TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]


def human(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0 or unit == "TiB":
            return f"{n:.2f}{unit}" if unit != "B" else f"{n}B"
        n /= 1024.0
    return str(n)


def dir_bytes(path: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def step_outputs(out: str, step: int, children: int) -> dict[str, str]:
    width = max(4, len(str(children)))
    return {t: os.path.join(out, t, f"part-{step:0{width}d}.parquet") for t in TABLES}


def generate_step(args, step: int) -> dict[str, int]:
    """跑一个 dbgen step，把非空表各写成一个 parquet 文件。返回 {table: bytes}。"""
    import duckdb

    targets = step_outputs(args.out, step, args.children)
    if not args.force and all(
        os.path.exists(p) or t in ("nation", "region") and step > 0
        for t, p in targets.items()
    ):
        existing = {t: os.path.getsize(p) for t, p in targets.items() if os.path.exists(p)}
        if existing:
            print(f"[step {step}] skip (already present)", flush=True)
            return existing

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"SET threads TO {args.threads}")
        con.execute(f"SET memory_limit = '{args.memory_limit}'")
        if args.temp_dir:
            os.makedirs(args.temp_dir, exist_ok=True)
            con.execute(f"SET temp_directory = '{args.temp_dir}'")
        # 保留 dbgen 的自然行序：lineitem 按 orderkey 递增，决定 row group 的 min/max
        # 统计，进而决定谓词下推能裁掉哪些块。打乱会让访问 pattern 失真。
        con.execute("SET preserve_insertion_order = true")
        con.execute("INSTALL tpch")
        con.execute("LOAD tpch")

        t0 = time.time()
        if args.children > 1:
            con.execute(f"CALL dbgen(sf={args.sf}, children={args.children}, step={step})")
        else:
            con.execute(f"CALL dbgen(sf={args.sf})")
        gen_s = time.time() - t0

        written: dict[str, int] = {}
        rowgroup = (
            f", ROW_GROUP_SIZE {args.row_group_size}" if args.row_group_size else ""
        )
        for table in TABLES:
            rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            if rows == 0:
                continue
            dest = targets[table]
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            tmp = dest + ".partial"
            con.execute(
                f"COPY {table} TO '{tmp}' "
                f"(FORMAT PARQUET, COMPRESSION {args.compression}{rowgroup})"
            )
            os.replace(tmp, dest)
            written[table] = os.path.getsize(dest)

        total = sum(written.values())
        print(
            f"[step {step}/{args.children}] dbgen {gen_s:.1f}s, "
            f"wrote {len(written)} tables, {human(total)} "
            f"({', '.join(f'{t}={human(b)}' for t, b in sorted(written.items()))})",
            flush=True,
        )
        return written
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sf", type=float, required=True, help="TPC-H scale factor")
    ap.add_argument("--out", required=True, help="output dataset root")
    ap.add_argument("--children", type=int, default=1,
                    help="split the generation into N steps (also => N files per big table)")
    ap.add_argument("--steps", default=None,
                    help="only run a subset, e.g. '0-9' or '3'")
    ap.add_argument("--compression", default="snappy",
                    choices=["snappy", "zstd", "gzip", "uncompressed"])
    ap.add_argument("--row-group-size", type=int, default=0,
                    help="rows per row group; 0 = DuckDB default (122880), keeps "
                         "PROJECT2 layout comparability")
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 8)
    ap.add_argument("--memory-limit", default="32GB")
    ap.add_argument("--temp-dir", default=None)
    ap.add_argument("--force", action="store_true", help="regenerate existing steps")
    ap.add_argument("--clean", action="store_true", help="remove --out first")
    args = ap.parse_args()

    if args.clean and os.path.exists(args.out):
        shutil.rmtree(args.out)
    os.makedirs(args.out, exist_ok=True)

    if args.steps:
        if "-" in args.steps:
            lo, hi = args.steps.split("-", 1)
            steps = list(range(int(lo), int(hi) + 1))
        else:
            steps = [int(args.steps)]
    else:
        steps = list(range(args.children)) if args.children > 1 else [0]

    t0 = time.time()
    per_table: dict[str, int] = {}
    for step in steps:
        written = generate_step(args, step)
        for t, b in written.items():
            per_table[t] = per_table.get(t, 0) + b

    elapsed = time.time() - t0
    total = dir_bytes(args.out)
    manifest = {
        "sf": args.sf,
        "children": args.children,
        "steps": steps,
        "compression": args.compression,
        "row_group_size": args.row_group_size or "duckdb-default(122880)",
        "total_bytes": total,
        "total_human": human(total),
        "bytes_per_sf": total / args.sf if args.sf else None,
        "per_table_bytes": per_table,
        "file_count": sum(len(files) for _r, _d, files in os.walk(args.out)),
        "elapsed_seconds": round(elapsed, 1),
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    with open(os.path.join(args.out, "_manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=2)

    print(f"\n=== done in {elapsed:.1f}s ===")
    print(f"total on disk : {human(total)} ({total} bytes)")
    print(f"bytes per SF  : {human(int(total / args.sf))}")
    print(f"file count    : {manifest['file_count']}")
    print(f"manifest      : {os.path.join(args.out, '_manifest.json')}")
    if args.sf:
        need_100gb = 100 * 1024 ** 3 / (total / args.sf)
        print(f"=> SF needed for 100GiB with COMPRESSION {args.compression}: {need_100gb:.1f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
