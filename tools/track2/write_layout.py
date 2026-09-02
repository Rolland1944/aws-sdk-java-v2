#!/usr/bin/env python3
"""UC2: materialise a layout plan through Spark SQL and writer parameters.

*** This file is one of the two second-layer renderers (TRACK2_V2_PLAN.md §7). ***

The use case it models is the common one: the user does not own the Parquet
writer. Data arrives through Spark SQL, the writer is buried inside the engine,
and the only surfaces available from outside are the query text and the writer
options. Nothing here patches or recompiles Spark, Hadoop or parquet-mr -- that
was the constraint that shaped the whole action space.

What that leaves reachable, and how:

    column order    df.select(*order)   -- or an explicit SELECT column list
    row group size  parquet.block.size
    page geometry   parquet.page.size, parquet.page.row.count.limit
    compression     parquet.compression (global only)
    file size       repartition(n)

Two of the six dimensions do not make it. Per-column compression and specific
encoding families have no parquet-mr property (contract §6.2 M-5), so
`strip_for_spark` drops them and records a warning. Those warnings are the
point of running the same plan through both renderers, so they are written into
the manifest rather than only printed.

Column order is the reason this renderer is interesting at all. Reordering a
schema is one `select`, and readers match columns by name, so the change is
invisible above the file. `--emit-sql` prints the equivalent SQL for a user who
would rather paste a statement than run this script.

Writer options are verified, never assumed. Spark reaches the Hadoop
configuration through `newHadoopConfWithOptions`, so `.option()` values do
arrive at parquet-mr, but a name it does not recognise is ignored with no
error. `--verify` reads the footers back and reports what was actually
produced; a silently dropped knob would otherwise surface much later as an
unexplained null result.

Usage:
  # baseline (contract 2.3)
  python3 tools/track2/write_layout.py \
      --source /mnt/scratch/clickbench_sf1 --out s3a://bucket/track2/baseline

  # a plan
  python3 tools/track2/write_layout.py \
      --source /mnt/scratch/clickbench_sf1 --out s3a://bucket/track2/plan-001 \
      --candidate plans/hits-v2-001.json --dataset clickbench --verify
"""

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from layout_actions import (  # noqa: E402
    BASELINE_CANDIDATE, check_l0, render, strip_for_spark)

TPCH_TABLES = ["customer", "lineitem", "nation", "orders",
               "part", "partsupp", "region", "supplier"]


# ------------------------------------------------------------------------- spark

def build_spark(args):
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError:
        sys.exit("pyspark is not importable; run this with spark-submit or install pyspark")

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import s3a_session
    builder = (SparkSession.builder
               .master(args.master)
               .appName(f"track2-write-{args.candidate_id}")
               .config("spark.driver.memory", args.driver_memory)
               .config("spark.local.dir", s3a_session.spark_scratch()))
    needs_s3 = (args.out.startswith("s3a://") or args.out.startswith("s3://")
                or args.source.startswith("s3a://") or args.source.startswith("s3://"))
    if needs_s3:
        ak, sk = s3a_session.load_creds()
        s3a_session.export_aws_env(ak, sk)
        builder = s3a_session.apply_frozen_reader(builder, ak, sk, interceptor=False)
    for item in args.conf:
        key, _, value = item.partition("=")
        builder = builder.config(key, value)
    return builder.getOrCreate()


def resolve_column_order(requested, available, table, notes):
    """The write order: requested columns first, anything unnamed appended.

    L0 already rejects a plan whose order is not a permutation of the schema,
    so this only has to be defensive about a plan written against a different
    scale factor. Appending rather than dropping matters: silently losing a
    column would turn a layout experiment into a correctness bug.
    """
    if not requested:
        return None
    known = [c for c in requested if c in available]
    missing = [c for c in requested if c not in available]
    tail = [c for c in available if c not in set(known)]
    if missing:
        notes.append(f"{table}: column order names {len(missing)} absent "
                     f"column(s) {missing[:5]}; ignored")
    if tail:
        notes.append(f"{table}: column order omits {len(tail)} column(s); "
                     f"appended in source order to preserve the schema")
    return known + tail


def apply_transforms(df, rendered, table, source_bytes, notes):
    """Reorder columns, then size files. Sizing shuffles, ordering does not.

    Ordering is a projection and survives a shuffle, so unlike v1 (where the
    sort had to come last) the two steps here are independent.
    """
    order = resolve_column_order(rendered.column_order, list(df.columns), table, notes)
    if order:
        df = df.select(*order)

    file_count = None
    if rendered.target_file_size and source_bytes:
        file_count = max(1, math.ceil(source_bytes / rendered.target_file_size))
        df = df.repartition(file_count)

    return df, file_count, order


def emit_sql(rendered, table, order, file_count, destination):
    """The equivalent SQL + SET statements, for a user who cannot run this script.

    This is the literal UC2 deliverable: the plan expressed as things a person
    can paste into a SQL session.
    """
    lines = []
    for key, value in sorted(rendered.writer_options.items()):
        lines.append(f"SET spark.hadoop.{key}={value};")
    columns = ",\n       ".join(order) if order else "*"
    hint = f"/*+ REPARTITION({file_count}) */ " if file_count else ""
    lines.append(
        f"INSERT OVERWRITE DIRECTORY '{destination}'\n"
        f"USING parquet\n"
        f"SELECT {hint}{columns}\n"
        f"  FROM {table};")
    return "\n".join(lines)


# ------------------------------------------------------------------ verification

def verify_footers(path, sample, expect_order=None):
    """Read written footers back and report what parquet-mr actually produced."""
    try:
        import pyarrow.parquet as pq
        import pyarrow.fs as pafs
    except ModuleNotFoundError:
        return {"status": "skipped", "detail": "pyarrow not importable"}

    try:
        uri = path.replace("s3a://", "s3://", 1) if path.startswith("s3a://") else path
        filesystem, resolved = pafs.FileSystem.from_uri(uri) if "://" in uri \
            else (pafs.LocalFileSystem(), uri)
        selector = pafs.FileSelector(resolved, recursive=True)
        files = [f for f in filesystem.get_file_info(selector)
                 if f.is_file and f.path.endswith(".parquet")]
        if not files:
            return {"status": "fail", "detail": f"no parquet files under {path}"}

        row_groups, sizes, page_index, bloom = 0, [], 0, 0
        codecs, encodings, order = set(), set(), None
        for info in files[:sample]:
            with filesystem.open_input_file(info.path) as handle:
                metadata = pq.ParquetFile(handle).metadata
            if order is None:
                order = [metadata.schema.column(i).name
                         for i in range(metadata.num_columns)]
            row_groups += metadata.num_row_groups
            for i in range(metadata.num_row_groups):
                group = metadata.row_group(i)
                sizes.append(group.total_byte_size)
                for c in range(group.num_columns):
                    column = group.column(c)
                    codecs.add(str(column.compression))
                    encodings.update(str(e) for e in column.encodings)
                column = group.column(0)
                page_index += 1 if column.has_offset_index else 0
                bloom += 1 if getattr(column, "bloom_filter_offset", None) else 0

        result = {
            "status": "pass",
            "files": len(files),
            "files_sampled": min(len(files), sample),
            "row_groups_sampled": row_groups,
            "row_group_bytes_min": min(sizes) if sizes else None,
            "row_group_bytes_median": sorted(sizes)[len(sizes) // 2] if sizes else None,
            "row_group_bytes_max": max(sizes) if sizes else None,
            "column_order": order,
            "compression_codecs": sorted(codecs),
            "encodings": sorted(encodings),
            "offset_index_present": page_index > 0,
            "bloom_filter_present": bloom > 0,
        }
        # Page index is a pinned constraint, not an action (contract r5 §0.1).
        # parquet-mr always writes it, so its absence means the write itself is
        # not comparable with the PyArrow path.
        if page_index == 0:
            result["status"] = "fail"
            result["detail"] = ("no OffsetIndex in the written footers; the "
                                "page-index constraint is violated")
        if expect_order and order and order != list(expect_order):
            result["status"] = "fail"
            result["detail"] = ("written column order does not match the plan; "
                                f"wanted {list(expect_order)[:5]}..., got {order[:5]}...")
        return result
    except Exception as exc:  # verification must never mask the write itself
        return {"status": "unknown", "detail": f"{type(exc).__name__}: {exc}"}


# -------------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True, help="canonical source layout root")
    ap.add_argument("--out", required=True, help="output root for this layout")
    ap.add_argument("--candidate", default=None,
                    help="layout plan JSON (contract 3.3); omit for the baseline")
    ap.add_argument("--tables", nargs="*", default=None)
    ap.add_argument("--dataset", choices=("tpch", "clickbench"), default="clickbench")
    ap.add_argument("--source-archive", default=None,
                    help="_manifest.json, used for per-table sizes; "
                         "defaults to <source>/_manifest.json")
    ap.add_argument("--conf", action="append", default=[],
                    help="extra Spark conf as key=value, repeatable")
    ap.add_argument("--master", default="local[16]",
                    help="Spark master; local[16] matches the frozen E2 client")
    ap.add_argument("--driver-memory", default="32g")
    ap.add_argument("--verify", action="store_true", help="read footers back after writing")
    ap.add_argument("--verify-sample", type=int, default=20)
    ap.add_argument("--emit-sql", action="store_true",
                    help="print the equivalent SQL + SET statements and exit")
    ap.add_argument("--manifest", default=None, help="default <out>/_layout_manifest.json")
    args = ap.parse_args()
    os.environ["TRACK2_DATASET"] = args.dataset
    if args.tables is None:
        args.tables = ["hits"] if args.dataset == "clickbench" else TPCH_TABLES

    candidate = BASELINE_CANDIDATE
    if args.candidate:
        with open(args.candidate) as fh:
            candidate = json.load(fh)
    args.candidate_id = candidate.get("candidate_id") or candidate.get("plan_id", "unnamed")

    actions = candidate.get("actions", [])

    archive_path = args.source_archive or os.path.join(args.source, "_manifest.json")
    table_bytes = {}
    if os.path.exists(archive_path):
        with open(archive_path) as fh:
            archive = json.load(fh)
        if "per_table_bytes" in archive:
            table_bytes = dict(archive["per_table_bytes"])
        elif "tables" in archive:
            table_bytes = {t: s["bytes"] for t, s in archive["tables"].items()}
    elif any(a.get("canonical") == "write.target-file-size-bytes" for a in actions):
        sys.exit(f"--source-archive is required for target-file-size plans "
                 f"(looked for {archive_path})")

    # Render per table, strip what Spark cannot express, then L0-check what is
    # left. Checking before stripping would reject every UC1-authored plan.
    per_table_rendered = {}
    violations = []
    for table in args.tables:
        rendered = strip_for_spark(render(actions, table=table))
        per_table_rendered[table] = rendered
        src = table_bytes.get(table)
        # Tiny tables cannot satisfy "several row groups"; only check
        # monotonicity. Large tables keep the structural-fidelity checks.
        if src is not None and src < 2 * 1024 ** 3:
            src = None
        violations.extend(check_l0(rendered, src, writer="parquet-mr"))
    if violations:
        print(f"L0 check rejected plan '{args.candidate_id}' (contract 5.3):")
        for violation in violations:
            print(f"  - {violation}")
        return 2

    # Warnings are collected across the per-table renders, not read off the
    # global one. A plan from plan_deterministic scopes every per-column action
    # to a table, so a global render sees none of them and would report that
    # nothing was dropped -- hiding the exact UC1/UC2 difference this renderer
    # exists to measure.
    rendered_global = strip_for_spark(render(actions))
    warnings = list(rendered_global.warnings)
    for rendered in per_table_rendered.values():
        for warning in rendered.warnings:
            if warning not in warnings:
                warnings.append(warning)
    for warning in warnings:
        print(f"WARNING: {warning}")

    if args.emit_sql:
        for table in args.tables:
            rendered = per_table_rendered[table]
            file_count = None
            if rendered.target_file_size and table_bytes.get(table):
                file_count = max(1, math.ceil(
                    table_bytes[table] / rendered.target_file_size))
            print(f"\n-- {table}")
            print(emit_sql(rendered, table, rendered.column_order, file_count,
                           f"{args.out.rstrip('/')}/{table}"))
        return 0

    spark = build_spark(args)
    versions = {
        "spark": spark.version,
        "hadoop": spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
        "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
    }
    print(f"\n# writing layout '{args.candidate_id}' (UC2: Spark + parquet-mr)", flush=True)
    for key, value in versions.items():
        print(f"  {key:8s} {value}", flush=True)
    print(f"  options  {rendered_global.writer_options or '<parquet-mr defaults>'}\n",
          flush=True)

    notes, results = [], {}
    for table in args.tables:
        rendered = per_table_rendered[table]
        started = time.time()
        df = spark.read.parquet(os.path.join(args.source, table))
        df, file_count, order = apply_transforms(
            df, rendered, table, table_bytes.get(table), notes)

        writer = df.write.mode("overwrite")
        for key, value in rendered.writer_options.items():
            writer = writer.option(key, value)
        destination = f"{args.out.rstrip('/')}/{table}"
        writer.parquet(destination)

        results[table] = {
            "path": destination,
            "requested_files": file_count,
            "column_order": order,
            "elapsed_seconds": round(time.time() - started, 1),
            "rendered": rendered.as_dict(),
            "warnings": rendered.warnings,
        }
        if args.verify:
            results[table]["verified"] = verify_footers(
                destination, args.verify_sample, order)
        print(f"  {table:10s} {results[table]['elapsed_seconds']:7.1f}s  {destination}",
              flush=True)

    manifest = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md r5",
        "use_case": "UC2 (Spark SQL + writer parameters; engine treated as a black box)",
        "candidate_id": args.candidate_id,
        "actions": actions,
        "rendered": {
            "writer": "parquet-mr",
            "global": rendered_global.as_dict(),
            "per_table": {t: r.as_dict() for t, r in per_table_rendered.items()},
        },
        "versions": versions,
        "source": args.source,
        "output": args.out,
        "tables": results,
        "warnings": warnings,
        "uc1_only_actions_dropped": bool(warnings),
        "notes": notes,
    }
    remote_out = args.out.startswith("s3a://") or args.out.startswith("s3://")
    if args.manifest:
        manifest_path = args.manifest
    elif remote_out:
        # s3a:// cannot take a local open(); keep a sidecar next to the source so
        # the run is still archivable even if the S3 put of the JSON is skipped
        manifest_path = os.path.join(
            os.path.dirname(os.path.abspath(archive_path)),
            f"_layout_manifest_{args.candidate_id}.json")
    else:
        manifest_path = os.path.join(args.out, "_layout_manifest.json")
    try:
        parent = os.path.dirname(manifest_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(manifest_path, "w") as fh:
            json.dump(manifest, fh, indent=2)
        print(f"\nmanifest: {manifest_path}", flush=True)
    except OSError:
        # last resort: emit JSON so the run is still archivable from the log
        print("\nmanifest (could not write locally):", flush=True)
        print(json.dumps(manifest, indent=2), flush=True)

    for note in notes:
        print(f"NOTE: {note}")
    failed = [t for t, r in results.items()
              if (r.get("verified") or {}).get("status") == "fail"]
    spark.stop()
    if failed:
        print(f"VERIFY FAILED for {failed}")
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
