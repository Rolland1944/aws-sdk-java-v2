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


def render(actions, table=None):
    """Render canonical actions into parquet-mr options and transform parameters.

    `table` selects per-table actions (contract 3.3 `scope.table`). An action
    with `"table": "lineitem"` applies only when writing lineitem; an action
    without `table` is global. Table-scoped transforms override global ones.
    """
    out = Rendered()
    file_global = file_table = None
    sort_global, sort_mode_global = None, None
    sort_table, sort_mode_table = None, None
    part_global = part_table = None

    def _sort_value(value):
        if isinstance(value, dict):
            return list(value.get("columns", [])), value.get("mode", "global")
        return list(value or []), "global"

    for action in actions:
        canonical = action.get("canonical")
        value = action.get("value")
        action_table = action.get("table")
        if not canonical:
            raise ValueError(f"action without a canonical name: {action}")
        if table and action_table and action_table != table:
            continue
        if table is None and action_table:
            # Global-only render (L0 / analyze): skip table-scoped transforms
            # so a lineitem 1GB action does not look like a global 1GB.
            if canonical in TRANSFORM_CANONICALS:
                continue

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

        scoped = action_table == table if table else False
        if canonical == "write.target-file-size-bytes":
            if scoped:
                file_table = int(value)
            elif action_table is None:
                file_global = int(value)
        elif canonical == "sort.columns":
            cols, mode = _sort_value(value)
            if scoped:
                sort_table, sort_mode_table = cols, mode
            elif action_table is None:
                sort_global, sort_mode_global = cols, mode
        elif canonical == "partition.spec":
            spec = parse_partition_spec(value)
            if scoped:
                part_table = spec
            elif action_table is None:
                part_global = spec
        else:
            raise ValueError(
                f"unrenderable canonical name '{canonical}'. Every candidate action must be "
                f"expressible on the frozen writer (contract 5.3 L0 check 1); add a mapping in "
                f"CANONICAL_TO_PARQUET_MR or reject the action.")

    out.target_file_size = file_table if file_table is not None else file_global
    if sort_table is not None:
        out.sort_columns, out.sort_mode = sort_table, sort_mode_table or "global"
    elif sort_global is not None:
        out.sort_columns, out.sort_mode = sort_global, sort_mode_global or "global"
    out.partition = part_table if part_table is not None else part_global

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

    # check 3: structural fidelity -- a target-file-size candidate must still produce
    # several files, and any candidate must produce several row groups, or it says
    # nothing about layout at SF100 scale. File count is only checked when the caller
    # actually requested a file size; with no target the engine's own parallelism
    # decides the file count, which is not this candidate's doing.
    if source_bytes:
        if target_file and source_bytes / target_file < 2:
            violations.append(
                f"target file size {target_file} yields < 2 files over {source_bytes} bytes")
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


def baseline_geometry_for(table):
    """Measured geometry of the layout being rewritten, or None.

    Set TRACK2_DATASET_SNAPSHOT to a dataset_snapshot.py document. The writer
    runs inside spark-submit and takes its candidate as JSON, so an environment
    variable is the least intrusive way to hand it one more file; nothing here
    fails if it is absent, the sort just does not pin a file count.
    """
    path = os.environ.get("TRACK2_DATASET_SNAPSHOT")
    if not path or not os.path.exists(path):
        return None
    with open(path) as fh:
        return (json.load(fh).get("geometry") or {}).get(table)


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
    elif sort_columns:
        # Per-table sort with no target-file-size: keep the measured baseline
        # file count so sort does not silently change scan parallelism
        # (E8 Q11/Q18 regressed when it did). The count comes from the dataset
        # snapshot of the layout being rewritten, via TRACK2_DATASET_SNAPSHOT,
        # rather than a per-dataset module that had to be edited by hand.
        base = baseline_geometry_for(table)
        if base:
            file_count = base["files"]
            notes.append(
                f"{table}: sort with no TFS; keeping baseline {file_count} files")

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
        uri = path.replace("s3a://", "s3://", 1) if path.startswith("s3a://") else path
        filesystem, resolved = pafs.FileSystem.from_uri(uri) if "://" in uri \
            else (pafs.LocalFileSystem(), uri)
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
    ap.add_argument("--tables", nargs="*", default=None)
    ap.add_argument("--dataset", choices=("tpch", "clickbench"), default="tpch")
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
    ap.add_argument("--manifest", default=None, help="default <out>/_layout_manifest.json")
    ap.add_argument("--dataset-snapshot", default=None,
                    help="dataset_snapshot.py output for the source layout; a "
                         "sort with no target file size keeps its file count")
    args = ap.parse_args()
    os.environ["TRACK2_DATASET"] = args.dataset
    if args.dataset_snapshot:
        os.environ["TRACK2_DATASET_SNAPSHOT"] = os.path.abspath(args.dataset_snapshot)
    if args.tables is None:
        args.tables = ["hits"] if args.dataset == "clickbench" else TPCH_TABLES

    candidate = BASELINE_CANDIDATE
    if args.candidate:
        with open(args.candidate) as fh:
            candidate = json.load(fh)
    args.candidate_id = candidate.get("candidate_id", "unnamed")

    actions = candidate.get("actions", [])
    rendered_global = render(actions)

    archive_path = args.source_archive or os.path.join(args.source, "_manifest.json")
    table_bytes = {}
    if os.path.exists(archive_path):
        with open(archive_path) as fh:
            archive = json.load(fh)
        if "per_table_bytes" in archive:
            table_bytes = dict(archive["per_table_bytes"])
        elif "tables" in archive:
            table_bytes = {t: s["bytes"] for t, s in archive["tables"].items()}
    elif rendered_global.target_file_size or any(
            a.get("canonical") == "write.target-file-size-bytes" for a in actions):
        sys.exit(f"--source-archive is required for target-file-size candidates "
                 f"(looked for {archive_path})")

    violations = []
    for table in args.tables:
        rendered_t = render(actions, table=table)
        if (not rendered_t.target_file_size
                and "parquet.block.size" not in rendered_t.writer_options):
            continue
        src = table_bytes.get(table)
        # Tiny tables cannot satisfy "several row groups"; only check
        # monotonicity. Large tables keep the structural-fidelity checks.
        if src is not None and src < 2 * 1024 ** 3:
            src = None
        violations.extend(check_l0(rendered_t, src))
    if violations:
        print(f"L0 check rejected candidate '{args.candidate_id}' (contract 5.3):")
        for violation in violations:
            print(f"  - {violation}")
        return 2

    for warning in rendered_global.warnings:
        print(f"WARNING: {warning}")

    spark = build_spark(args)
    versions = {
        "spark": spark.version,
        "hadoop": spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
        "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
    }
    print(f"\n# writing layout '{args.candidate_id}' (contract r3)", flush=True)
    for key, value in versions.items():
        print(f"  {key:8s} {value}", flush=True)
    print(f"  options  {rendered_global.writer_options or '<parquet-mr defaults>'}\n",
          flush=True)

    notes, results = [], {}
    per_table_rendered = {}
    for table in args.tables:
        rendered = render(actions, table=table)
        per_table_rendered[table] = {
            "writer_options": rendered.writer_options,
            "target_file_size": rendered.target_file_size,
            "sort_columns": rendered.sort_columns,
            "sort_mode": rendered.sort_mode,
            "partition": rendered.partition,
        }
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
        print(f"  {table:10s} {results[table]['elapsed_seconds']:7.1f}s  {destination}",
              flush=True)

    manifest = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 1.3 / 5",
        "contract_revision": "r3",
        "candidate_id": args.candidate_id,
        "actions": candidate.get("actions", []),
        "rendered": {
            "writer": "parquet-mr",
            "writer_options": rendered_global.writer_options,
            "target_file_size": rendered_global.target_file_size,
            "sort_columns": rendered_global.sort_columns,
            "sort_mode": rendered_global.sort_mode,
            "partition": rendered_global.partition,
            "per_table": per_table_rendered,
        },
        "versions": versions,
        "source": args.source,
        "output": args.out,
        "tables": results,
        "warnings": rendered_global.warnings,
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
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
