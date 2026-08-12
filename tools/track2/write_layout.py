#!/usr/bin/env python3
"""Rewrite the canonical TPC-H source into one Parquet layout candidate.

*** This file is the target of the generated Code Diff (TRACK2_M0_CONTRACT.md 1.3). ***

A candidate is described only by engine-neutral canonical names (contract 11.2,
aligned with Iceberg table properties). Rendering canonical names into parquet-mr
configuration and DataFrame transforms happens here and nowhere else, so analysis,
what-if and constraint checking never learn what engine is in use.

The baseline needs no actions at all. Contract 2.3 defines it as plain
`df.write.parquet(...)`, i.e. parquet-mr's own defaults, so an empty action list
reproduces it exactly and there is no hand-tuned "aligned baseline" to defend.

Two behaviours worth knowing before reading results:

  * Writer options are verified, never assumed. Spark reaches the Hadoop
    configuration through `newHadoopConfWithOptions`, so `.option()` values do
    arrive at parquet-mr, but a name that parquet-mr does not recognise is simply
    ignored with no error. `--verify` reads the footers back and reports what was
    actually produced; a silently dropped knob would otherwise surface much later
    as an unexplained null result.

  * Partitioning on a derived column does not prune the way it looks like it
    should. `partition.spec = l_shipdate:year` adds a `l_shipdate_year` column and
    partitions on it, but Spark cannot infer `year(l_shipdate) = 1995` from a
    predicate on `l_shipdate`, which is what TPC-H actually filters on. Such a
    candidate therefore costs a rewrite and buys nothing unless the queries are
    rewritten too. Hidden partitioning is exactly the gap Iceberg exists to fill
    (contract 11), so this is recorded as a warning rather than silently applied.

Usage:
  # baseline (contract 2.3)
  python3 tools/track2/write_layout.py \
      --source /mnt/scratch/tpch_sf100 --out s3a://bucket/track2/baseline

  # a candidate
  python3 tools/track2/write_layout.py \
      --source /mnt/scratch/tpch_sf100 --out s3a://bucket/track2/cand-rg32 \
      --candidate candidates/rg32_sort_shipdate.json --verify
"""

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, timezone

TPCH_TABLES = ["customer", "lineitem", "nation", "orders",
               "part", "partsupp", "region", "supplier"]

# contract 11.2: canonical (Iceberg vocabulary) -> parquet-mr property
CANONICAL_TO_PARQUET_MR = {
    "write.parquet.row-group-size-bytes": "parquet.block.size",
    "write.parquet.page-size-bytes": "parquet.page.size",
    "write.parquet.page-row-limit": "parquet.page.row.count.limit",
    "write.parquet.compression-codec": "parquet.compression",
    "write.parquet.writer-version": "parquet.writer.version",
}

# per-column properties; the column name is appended to both sides
CANONICAL_COLUMN_PREFIXES = {
    "write.parquet.bloom-filter-enabled.column.": "parquet.bloom.filter.enabled#",
    "write.parquet.bloom-filter-ndv.column.": "parquet.bloom.filter.expected.ndv#",
    "write.parquet.bloom-filter-fpp.column.": "parquet.bloom.filter.fpp#",
    "write.parquet.dict-encoding-enabled.column.": "parquet.enable.dictionary#",
    "write.parquet.stats-enabled.column.": "parquet.column.statistics.enabled#",
}

# canonical names that are DataFrame transforms rather than writer properties
TRANSFORM_CANONICALS = {"write.target-file-size-bytes", "sort.columns", "partition.spec"}

BASELINE_CANDIDATE = {"candidate_id": "baseline", "actions": []}


# --------------------------------------------------------------------- rendering

class Rendered(object):
    def __init__(self):
        self.writer_options = {}
        self.target_file_size = None
        self.sort_columns = []
        self.sort_mode = "global"
        self.partition = None
        self.warnings = []


def render(actions):
    """Render canonical actions into parquet-mr options and transform parameters."""
    out = Rendered()
    for action in actions:
        canonical = action.get("canonical")
        value = action.get("value")
        if not canonical:
            raise ValueError(f"action without a canonical name: {action}")

        if canonical in CANONICAL_TO_PARQUET_MR:
            out.writer_options[CANONICAL_TO_PARQUET_MR[canonical]] = str(value)
            continue

        column_property = next(
            ((prefix, rendered) for prefix, rendered in CANONICAL_COLUMN_PREFIXES.items()
             if canonical.startswith(prefix)), None)
        if column_property:
            prefix, rendered_prefix = column_property
            column = canonical[len(prefix):]
            out.writer_options[rendered_prefix + column] = str(value).lower() \
                if isinstance(value, bool) else str(value)
            continue

        if canonical == "write.target-file-size-bytes":
            out.target_file_size = int(value)
        elif canonical == "sort.columns":
            if isinstance(value, dict):
                out.sort_columns = list(value.get("columns", []))
                out.sort_mode = value.get("mode", "global")
            else:
                out.sort_columns = list(value or [])
        elif canonical == "partition.spec":
            out.partition = parse_partition_spec(value)
        else:
            raise ValueError(
                f"unrenderable canonical name '{canonical}'. Every candidate action must be "
                f"expressible on the frozen writer (contract 5.3 L0 check 1); add a mapping in "
                f"CANONICAL_TO_PARQUET_MR or reject the action.")

    if out.partition and out.partition["transform"] != "identity":
        out.warnings.append(
            f"partition.spec {out.partition['column']}:{out.partition['transform']} partitions on a "
            f"derived column; Spark cannot prune it from a predicate on {out.partition['column']} "
            f"itself, so expect no pruning benefit for unmodified TPC-H queries")
    return out


def parse_partition_spec(value):
    """Parse `none`, `col`, or `col:transform` as used in contract 5.1."""
    if not value or value == "none":
        return None
    if isinstance(value, dict):
        return {"column": value["column"], "transform": value.get("transform", "identity")}
    column, _, transform = str(value).partition(":")
    return {"column": column, "transform": transform or "identity"}


# ------------------------------------------------------------------- L0 checking

def check_l0(rendered, source_bytes):
    """Static candidate legality, contract 5.3.

    Returns a list of violations. A non-empty list must stop the run: the point of
    L0 is to reject a candidate before spending machine time on an experiment whose
    negative result would be an artefact of the candidate being unrealisable.
    """
    violations = []
    options = rendered.writer_options

    row_group = int(options.get("parquet.block.size", 0)) or None
    page = int(options.get("parquet.page.size", 0)) or None
    target_file = rendered.target_file_size

    # check 5: monotonicity
    if row_group and target_file and row_group > target_file:
        violations.append(
            f"parquet.block.size ({row_group}) > target file size ({target_file})")
    if page and row_group and page > row_group:
        violations.append(f"parquet.page.size ({page}) > parquet.block.size ({row_group})")

    # check 3: structural fidelity -- at least 2 files and 4 row groups, or the
    # candidate says nothing about a layout at SF100 scale
    if source_bytes:
        effective_file = target_file or source_bytes
        if effective_file and source_bytes / effective_file < 2:
            violations.append(
                f"target file size {effective_file} yields < 2 files over {source_bytes} bytes")
        effective_rg = row_group or 128 * 1024 * 1024
        if source_bytes / effective_rg < 4:
            violations.append(
                f"row group size {effective_rg} yields < 4 row groups over {source_bytes} bytes")

    # check 2: reader capability -- page index is not an action at all (contract 5.1)
    for name in options:
        if "columnindex" in name.lower() or "page.write-checksum" in name:
            violations.append(f"{name} is not a candidate action on parquet-mr (contract 6.2 M-1)")

    return violations


# ------------------------------------------------------------------------- spark

def build_spark(args):
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError:
        sys.exit("pyspark is not importable; run this with spark-submit or install pyspark")

    builder = SparkSession.builder.appName(f"track2-write-{args.candidate_id}")
    for item in args.conf:
        key, _, value = item.partition("=")
        builder = builder.config(key, value)
    return builder.getOrCreate()


def apply_transforms(df, rendered, table, source_bytes, notes):
    """Apply partitioning, file sizing and sorting, in that order.

    Order matters: repartitioning shuffles and would destroy any sort applied
    before it, so sorting is always last.
    """
    from pyspark.sql import functions as F

    partition_columns = []
    if rendered.partition:
        column = rendered.partition["column"]
        if column not in df.columns:
            notes.append(f"{table}: skipped partition.spec, no column {column}")
        else:
            transform = rendered.partition["transform"]
            if transform == "identity":
                partition_columns = [column]
            else:
                derived = f"{column}_{transform}"
                expression = {"year": F.year, "month": F.month, "day": F.dayofmonth}[transform]
                df = df.withColumn(derived, expression(F.col(column)))
                partition_columns = [derived]

    sort_columns = [c for c in rendered.sort_columns if c in df.columns]
    if rendered.sort_columns and not sort_columns:
        notes.append(f"{table}: skipped sort.columns, none of "
                     f"{rendered.sort_columns} present")

    file_count = None
    if rendered.target_file_size and source_bytes:
        file_count = max(1, math.ceil(source_bytes / rendered.target_file_size))

    if sort_columns and rendered.sort_mode == "global":
        # range partitioning gives disjoint value ranges per file, which is what makes
        # a global sort prune; a plain repartition would interleave them again
        df = df.repartitionByRange(file_count, *sort_columns) if file_count \
            else df.repartitionByRange(*sort_columns)
    elif file_count:
        df = df.repartition(file_count)

    if sort_columns:
        df = df.sortWithinPartitions(*sort_columns)

    return df, partition_columns, file_count


# ------------------------------------------------------------------ verification

def verify_footers(path, sample):
    """Read written footers back and report what parquet-mr actually produced."""
    try:
        import pyarrow.parquet as pq
        import pyarrow.fs as pafs
    except ModuleNotFoundError:
        return {"status": "skipped", "detail": "pyarrow not importable"}

    try:
        filesystem, resolved = pafs.FileSystem.from_uri(path) if "://" in path \
            else (pafs.LocalFileSystem(), path)
        selector = pafs.FileSelector(resolved, recursive=True)
        files = [f for f in filesystem.get_file_info(selector)
                 if f.is_file and f.path.endswith(".parquet")]
        if not files:
            return {"status": "fail", "detail": f"no parquet files under {path}"}

        row_groups, sizes, page_index, bloom = 0, [], 0, 0
        for info in files[:sample]:
            with filesystem.open_input_file(info.path) as handle:
                metadata = pq.ParquetFile(handle).metadata
            row_groups += metadata.num_row_groups
            for i in range(metadata.num_row_groups):
                group = metadata.row_group(i)
                sizes.append(group.total_byte_size)
                column = group.column(0)
                page_index += 1 if column.has_offset_index else 0
                bloom += 1 if getattr(column, "bloom_filter_offset", None) else 0

        return {
            "status": "pass",
            "files": len(files),
            "files_sampled": min(len(files), sample),
            "row_groups_sampled": row_groups,
            "row_group_bytes_min": min(sizes) if sizes else None,
            "row_group_bytes_median": sorted(sizes)[len(sizes) // 2] if sizes else None,
            "row_group_bytes_max": max(sizes) if sizes else None,
            "offset_index_present": page_index > 0,
            "bloom_filter_present": bloom > 0,
        }
    except Exception as exc:  # verification must never mask the write itself
        return {"status": "unknown", "detail": f"{type(exc).__name__}: {exc}"}


# -------------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True, help="canonical source from gen_tpch.py")
    ap.add_argument("--out", required=True, help="output root for this layout")
    ap.add_argument("--candidate", default=None,
                    help="LayoutCandidate JSON (contract 3.3); omit for the baseline")
    ap.add_argument("--tables", nargs="*", default=TPCH_TABLES)
    ap.add_argument("--source-archive", default=None,
                    help="_gen_archive.json, used for per-table sizes; "
                         "defaults to <source>/_gen_archive.json")
    ap.add_argument("--conf", action="append", default=[],
                    help="extra Spark conf as key=value, repeatable")
    ap.add_argument("--verify", action="store_true", help="read footers back after writing")
    ap.add_argument("--verify-sample", type=int, default=20)
    ap.add_argument("--manifest", default=None, help="default <out>/_layout_manifest.json")
    args = ap.parse_args()

    candidate = BASELINE_CANDIDATE
    if args.candidate:
        with open(args.candidate) as fh:
            candidate = json.load(fh)
    args.candidate_id = candidate.get("candidate_id", "unnamed")

    rendered = render(candidate.get("actions", []))

    archive_path = args.source_archive or os.path.join(args.source, "_gen_archive.json")
    table_bytes = {}
    if os.path.exists(archive_path):
        with open(archive_path) as fh:
            table_bytes = {t: s["bytes"] for t, s in json.load(fh)["tables"].items()}
    elif rendered.target_file_size:
        sys.exit(f"--source-archive is required for target-file-size candidates "
                 f"(looked for {archive_path})")

    violations = check_l0(rendered, table_bytes.get("lineitem"))
    if violations:
        print(f"L0 check rejected candidate '{args.candidate_id}' (contract 5.3):")
        for violation in violations:
            print(f"  - {violation}")
        return 2

    for warning in rendered.warnings:
        print(f"WARNING: {warning}")

    spark = build_spark(args)
    versions = {
        "spark": spark.version,
        "hadoop": spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
        "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
    }
    print(f"\n# writing layout '{args.candidate_id}' (contract r3)")
    for key, value in versions.items():
        print(f"  {key:8s} {value}")
    print(f"  options  {rendered.writer_options or '<parquet-mr defaults>'}\n")

    notes, results = [], {}
    for table in args.tables:
        started = time.time()
        df = spark.read.parquet(os.path.join(args.source, table))
        df, partition_columns, file_count = apply_transforms(
            df, rendered, table, table_bytes.get(table), notes)

        writer = df.write.mode("overwrite")
        for key, value in rendered.writer_options.items():
            writer = writer.option(key, value)
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        destination = f"{args.out.rstrip('/')}/{table}"
        writer.parquet(destination)

        results[table] = {
            "path": destination,
            "requested_files": file_count,
            "partition_by": partition_columns,
            "elapsed_seconds": round(time.time() - started, 1),
        }
        if args.verify:
            results[table]["verified"] = verify_footers(destination, args.verify_sample)
        print(f"  {table:10s} {results[table]['elapsed_seconds']:7.1f}s  {destination}")

    manifest = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 1.3 / 5",
        "contract_revision": "r3",
        "candidate_id": args.candidate_id,
        "actions": candidate.get("actions", []),
        "rendered": {
            "writer": "parquet-mr",
            "writer_options": rendered.writer_options,
            "target_file_size": rendered.target_file_size,
            "sort_columns": rendered.sort_columns,
            "sort_mode": rendered.sort_mode,
            "partition": rendered.partition,
        },
        "versions": versions,
        "source": args.source,
        "output": args.out,
        "tables": results,
        "warnings": rendered.warnings,
        "notes": notes,
    }
    manifest_path = args.manifest or os.path.join(args.out, "_layout_manifest.json")
    try:
        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        with open(manifest_path, "w") as fh:
            json.dump(manifest, fh, indent=2)
        print(f"\nmanifest: {manifest_path}")
    except OSError:
        # a remote --out cannot take a local manifest; emit it so the run is still archivable
        print("\nmanifest (could not write locally):")
        print(json.dumps(manifest, indent=2))

    for note in notes:
        print(f"NOTE: {note}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
