#!/usr/bin/env python3
"""Generate the canonical TPC-H source data frozen in TRACK2_M0_CONTRACT.md 2.2.

This produces ONE dataset. Every layout candidate is later rewritten from it by
write_layout.py, changing only writer parameters and row order. Regenerating per
candidate is forbidden by the contract: it would vary the data itself and destroy
the causal attribution the whole track depends on.

Generation is chunked with dbgen's own `children`/`step` partitioning rather than
one `CALL dbgen(sf = 100)`. At SF100 lineitem is ~600M rows, and a single call
materialises the whole table inside the database before anything can be exported,
which needs far more space than the instance has. Chunking bounds peak database
size to roughly one step and makes a multi-hour run observable.

Note on row order: the contract defines the baseline layout as "no sort, dbgen's
original row order" (2.3), so this script deliberately does NOT set
`preserve_insertion_order = false`. That setting is the usual advice for large
DuckDB exports, but it silently permutes rows, which would quietly redefine the
baseline. Chunking is what keeps memory bounded instead.

Because `--children` determines how rows are split across files, it is recorded
in the archive and must stay fixed for the whole study.

Usage:
  # smoke test first -- same code path, seconds instead of hours
  python3 tools/track2/gen_tpch.py --sf 1 --out /mnt/scratch/tpch_sf1

  # the real thing (contract 2.2)
  python3 tools/track2/gen_tpch.py \
      --sf 100 \
      --out /mnt/scratch/tpch_sf100 \
      --temp-dir /mnt/scratch/duckdb_tmp \
      --memory-limit 48GB
"""

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
import time
from datetime import datetime, timezone

# TPC-H standard 8 tables, all in scope (contract 2.2).
TPCH_TABLES = ["customer", "lineitem", "nation", "orders",
               "part", "partsupp", "region", "supplier"]

MASK64 = (1 << 64) - 1


def connect(args):
    try:
        import duckdb
    except ModuleNotFoundError:
        sys.exit("duckdb is not importable; install it with: pip install duckdb")

    conn = duckdb.connect(args.db) if args.db else duckdb.connect()
    conn.execute("INSTALL tpch")
    conn.execute("LOAD tpch")
    if args.temp_dir:
        os.makedirs(args.temp_dir, exist_ok=True)
        conn.execute(f"SET temp_directory = '{args.temp_dir}'")
    if args.memory_limit:
        conn.execute(f"SET memory_limit = '{args.memory_limit}'")
    if args.threads:
        conn.execute(f"SET threads = {args.threads}")
    return conn


def probe_versions(conn):
    versions = {"duckdb": conn.execute("SELECT version()").fetchone()[0]}
    row = conn.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'tpch'"
    ).fetchone()
    versions["tpch_extension"] = row[0] if row and row[0] else "<unreported>"
    versions["python"] = sys.version.split()[0]
    return versions


def checksum_expression(conn, table):
    """Order-independent content checksum over every column of a table.

    Summing per-row hashes is commutative, so a chunked export produces the same
    value as a single-shot one; that is what makes the checksum comparable across
    different `--children` settings.
    """
    columns = [r[1] for r in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]
    quoted = ", ".join(f'"{c}"' for c in columns)
    return f"hash({quoted})"


def export_step(conn, args, step, state):
    """Append one dbgen partition, flush every table to Parquet, then empty them."""
    if args.children > 1:
        conn.execute(f"CALL dbgen(sf = {args.sf}, children = {args.children}, step = {step})")
    else:
        conn.execute(f"CALL dbgen(sf = {args.sf})")

    for table in TPCH_TABLES:
        rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if rows == 0:
            # with children set, the small tables only appear in one step
            continue

        if args.checksum:
            expr = state["checksum_expr"].setdefault(table, checksum_expression(conn, table))
            chunk_sum = conn.execute(f"SELECT SUM({expr}) FROM {table}").fetchone()[0] or 0
        else:
            chunk_sum = 0

        table_dir = os.path.join(args.out, table)
        os.makedirs(table_dir, exist_ok=True)
        target = os.path.join(table_dir, f"part-{step:05d}.parquet")
        conn.execute(
            f"COPY (SELECT * FROM {table}) TO '{target}' "
            f"(FORMAT PARQUET, COMPRESSION {args.compression})"
        )

        stats = state["tables"].setdefault(
            table, {"rows": 0, "checksum": 0, "files": 0, "bytes": 0})
        stats["rows"] += rows
        stats["checksum"] = (stats["checksum"] + int(chunk_sum)) & MASK64
        stats["files"] += 1
        stats["bytes"] += os.path.getsize(target)

        conn.execute(f"DELETE FROM {table}")


def export_queries(conn, path):
    """Freeze the exact 22 query texts used, so E2 cannot silently drift (contract 2.1)."""
    rows = conn.execute("SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr").fetchall()
    queries = {str(nr): text for nr, text in rows}
    with open(path, "w") as fh:
        json.dump(queries, fh, indent=2)
    digest = hashlib.sha256(
        "".join(queries[k] for k in sorted(queries, key=int)).encode("utf-8")).hexdigest()
    return len(queries), digest


def human(num_bytes):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if abs(num_bytes) < 1024 or unit == "TiB":
            return f"{num_bytes:.1f}{unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f}TiB"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sf", type=float, default=100, help="scale factor (default 100)")
    ap.add_argument("--out", required=True, help="output directory for the Parquet source")
    ap.add_argument("--children", type=int, default=0,
                    help="dbgen partitions; 0 picks ceil(sf), which keeps each step ~1 SF")
    ap.add_argument("--db", default=None,
                    help="persistent DuckDB file; default in-memory, which is fine because "
                         "chunking keeps only one step resident")
    ap.add_argument("--temp-dir", default=None, help="DuckDB spill directory; put this on NVMe")
    ap.add_argument("--memory-limit", default=None, help="e.g. 48GB")
    ap.add_argument("--threads", type=int, default=0, help="0 leaves the DuckDB default")
    ap.add_argument("--compression", default="ZSTD", choices=["ZSTD", "SNAPPY", "UNCOMPRESSED"],
                    help="source-only codec; the source is never measured, so favour size")
    ap.add_argument("--no-checksum", dest="checksum", action="store_false",
                    help="skip content checksums (contract 2.2 requires them for the real run)")
    ap.add_argument("--archive", default=None,
                    help="archive JSON path (default <out>/_gen_archive.json)")
    ap.add_argument("--force", action="store_true", help="overwrite a non-empty --out")
    args = ap.parse_args()

    if args.children <= 0:
        args.children = max(1, math.ceil(args.sf))
    if not args.archive:
        args.archive = os.path.join(args.out, "_gen_archive.json")

    if os.path.isdir(args.out) and os.listdir(args.out):
        if not args.force:
            sys.exit(f"{args.out} is not empty; pass --force to overwrite")
        shutil.rmtree(args.out)
    os.makedirs(args.out, exist_ok=True)

    conn = connect(args)
    versions = probe_versions(conn)
    print(f"# TPC-H SF{args.sf:g} source generation (contract r3)")
    for key, value in versions.items():
        print(f"  {key:16s} {value}")
    print(f"  {'children':16s} {args.children}")
    print(f"  {'out':16s} {args.out}\n")

    state = {"tables": {}, "checksum_expr": {}}
    started = time.time()
    for step in range(args.children):
        step_started = time.time()
        export_step(conn, args, step, state)
        done = step + 1
        elapsed = time.time() - started
        eta = elapsed / done * (args.children - done)
        written = sum(t["bytes"] for t in state["tables"].values())
        print(f"  step {done:4d}/{args.children}  "
              f"{time.time() - step_started:6.1f}s  "
              f"written {human(written):>9s}  "
              f"eta {eta / 60:6.1f}m")

    queries_path = os.path.join(args.out, "_tpch_queries.json")
    query_count, query_digest = export_queries(conn, queries_path)

    archive = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 2.2",
        "contract_revision": "r3",
        "versions": versions,
        "scale_factor": args.sf,
        "children": args.children,
        "compression": args.compression,
        "output": os.path.abspath(args.out),
        "elapsed_seconds": round(time.time() - started, 1),
        "tables": {t: {"rows": s["rows"],
                       "checksum": s["checksum"] if args.checksum else None,
                       "files": s["files"],
                       "bytes": s["bytes"]}
                   for t, s in sorted(state["tables"].items())},
        "queries": {"path": queries_path, "count": query_count, "sha256": query_digest},
    }
    with open(args.archive, "w") as fh:
        json.dump(archive, fh, indent=2)

    print(f"\n{'table':10s} {'rows':>14s} {'files':>7s} {'bytes':>11s}  checksum")
    for table, stats in archive["tables"].items():
        checksum = f"{stats['checksum']:#018x}" if stats["checksum"] is not None else "-"
        print(f"{table:10s} {stats['rows']:>14,d} {stats['files']:>7d} "
              f"{human(stats['bytes']):>11s}  {checksum}")
    total = sum(s["bytes"] for s in archive["tables"].values())
    print(f"\ntotal {human(total)} in {archive['elapsed_seconds'] / 60:.1f}m")
    print(f"queries: {query_count} frozen at {queries_path} (sha256 {query_digest[:16]}...)")
    print(f"archive: {args.archive}")

    missing = [t for t in TPCH_TABLES if t not in archive["tables"]]
    if missing:
        print(f"\nWARNING: no rows generated for {', '.join(missing)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
