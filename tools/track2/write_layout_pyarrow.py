#!/usr/bin/env python3
"""UC1: materialise a layout plan by calling the Parquet writer directly.

*** This file is one of the two second-layer renderers (TRACK2_V2_PLAN.md §7). ***

The use case it models is the one where the user owns the ingest code: a script
that reads batches from somewhere and calls a Parquet writer. There is no
engine in the way, so every dimension of the plan is reachable -- including the
two Spark cannot express, per-column compression and specific encoding
families.

This is emphatically not a fork of Arrow. Every knob below is a keyword
argument of `pyarrow.parquet.ParquetWriter` that has been public for years:

    schema=            reordered   -> column order
    row_group_size=                -> row group size
    compression=       dict         -> per-column codec
    column_encoding=   dict         -> per-column encoding family
    data_page_size=                -> page size
    write_page_index=True          -> pinned, never read from the plan

File size is the only dimension with no argument, because it is not a property
of a file -- it is a decision about when to stop writing one. The writer closes
the current file and opens the next once the bytes on disk pass the target,
which is exactly what "the application controls file size" means in UC1.

Page index is pinned rather than planned. PyArrow defaults it *off* while
parquet-mr always writes it, so leaving it to the plan would make the two
renderers produce structurally different files for reasons unrelated to the
layout under test, and every UC1-vs-UC2 comparison would be measuring that
instead. `--verify` reads the footers back and fails the run if the index is
missing.

Usage:
  python3 tools/track2/write_layout_pyarrow.py \
      --source /mnt/scratch/clickbench_sf1 --out /mnt/scratch/plan-001 \
      --plan plans/hits-v2-001.json --tables hits --verify
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from layout_actions import BASELINE_CANDIDATE, check_l0, render  # noqa: E402

DEFAULT_BATCH_ROWS = 65536
# PyArrow's row_group_size is a row count, not a byte budget. Converting needs
# the average row width, which the source footers give exactly.
DEFAULT_ROW_GROUP_BYTES = 128 * 1024 * 1024


def open_fs(path):
    import pyarrow.fs as pafs
    if "://" in path:
        uri = path.replace("s3a://", "s3://", 1) if path.startswith("s3a://") else path
        return pafs.FileSystem.from_uri(uri)
    return pafs.LocalFileSystem(), path


def list_parquet(fs, base):
    import pyarrow.fs as pafs
    info = fs.get_file_info(base)
    if info.type == pafs.FileType.File:
        return [base]
    return sorted(f.path for f in fs.get_file_info(pafs.FileSelector(base, recursive=True))
                  if f.type == pafs.FileType.File and f.path.endswith(".parquet"))


def source_row_width(fs, files, sample=4):
    """Mean uncompressed bytes per row, for the byte -> row-count conversion."""
    import pyarrow.parquet as pq
    total_bytes = total_rows = 0
    for path in files[:sample]:
        with fs.open_input_file(path) as handle:
            md = pq.ParquetFile(handle).metadata
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            total_bytes += rg.total_byte_size
            total_rows += rg.num_rows
    return (total_bytes / total_rows) if total_rows else None


def resolve_column_order(requested, available, notes, table):
    """Requested order first, anything unnamed appended in source order."""
    if not requested:
        return list(available)
    known = [c for c in requested if c in available]
    missing = [c for c in requested if c not in available]
    tail = [c for c in available if c not in set(known)]
    if missing:
        notes.append(f"{table}: plan names {len(missing)} absent column(s) "
                     f"{missing[:5]}; ignored")
    if tail:
        notes.append(f"{table}: plan omits {len(tail)} column(s); appended in "
                     f"source order to preserve the schema")
    return known + tail


def writer_kwargs(rendered, order, notes, table):
    """Translate a rendered plan into ParquetWriter keyword arguments."""
    kwargs = {"write_page_index": True}

    # Per-column compression: PyArrow accepts a dict keyed by column name and
    # falls back to the global codec for anything absent. This is the dimension
    # UC2 cannot express at all.
    if rendered.column_compression:
        default = rendered.compression or "snappy"
        compression = {c: _codec(default) for c in order}
        compression.update({c: _codec(v)
                            for c, v in rendered.column_compression.items()
                            if c in set(order)})
        kwargs["compression"] = compression
        notes.append(f"{table}: per-column compression on "
                     f"{len(rendered.column_compression)} column(s) "
                     f"(not expressible in UC2)")
    elif rendered.compression:
        kwargs["compression"] = _codec(rendered.compression)

    encodings = {c: e for c, e in rendered.column_encoding.items() if c in set(order)}
    if encodings:
        # column_encoding and use_dictionary are mutually exclusive per column:
        # the dictionary path ignores an explicit encoding, so a column asking
        # for DELTA_* has to have the dictionary turned off or the request is
        # silently dropped.
        explicit = {c: e for c, e in encodings.items() if e != "RLE_DICTIONARY"}
        dictionary = {c: (encodings.get(c) == "RLE_DICTIONARY") for c in order
                      if c in encodings}
        if explicit:
            kwargs["column_encoding"] = explicit
        if dictionary:
            kwargs["use_dictionary"] = [c for c, on in dictionary.items() if on]
        notes.append(f"{table}: encoding family set on {len(encodings)} column(s) "
                     f"(not expressible in UC2)")

    if rendered.page_size:
        kwargs["data_page_size"] = int(rendered.page_size)
    if rendered.column_dictionary and "use_dictionary" not in kwargs:
        kwargs["use_dictionary"] = [c for c, on in rendered.column_dictionary.items()
                                    if on and c in set(order)]
    return kwargs


def _codec(value):
    v = str(value).lower()
    return "NONE" if v in {"uncompressed", "none", ""} else v


class RotatingWriter:
    """Write batches, closing and reopening once a file passes the target size.

    Target file size has no writer argument because it is not a property of a
    file. In UC1 the application decides when to stop, and this is that
    decision in the smallest honest form: check the bytes written so far after
    each row group, and roll over when they exceed the target. The check is
    after the row group rather than inside it because a row group cannot be
    split across files.
    """

    def __init__(self, fs, out_dir, schema, target_bytes, kwargs):
        self.fs = fs
        self.out_dir = out_dir
        self.schema = schema
        self.target_bytes = target_bytes
        self.kwargs = kwargs
        self.index = 0
        self.writer = None
        self.handle = None
        self.paths = []

    def _open(self):
        import pyarrow.parquet as pq
        path = f"{self.out_dir}/part-{self.index:05d}.parquet"
        self.handle = self.fs.open_output_stream(path)
        self.writer = pq.ParquetWriter(self.handle, self.schema, **self.kwargs)
        self.paths.append(path)
        self.index += 1

    def write(self, table, row_group_size=None):
        if self.writer is None:
            self._open()
        self.writer.write_table(table, row_group_size=row_group_size)
        if self.target_bytes and self.handle.tell() >= self.target_bytes:
            self.close()

    def close(self):
        if self.writer is not None:
            self.writer.close()
            self.handle.close()
            self.writer = None
            self.handle = None


def write_table(source_fs, source_files, out_fs, out_dir, rendered, table,
                notes, batch_rows=DEFAULT_BATCH_ROWS):
    import pyarrow as pa
    import pyarrow.parquet as pq

    with source_fs.open_input_file(source_files[0]) as handle:
        source_schema = pq.ParquetFile(handle).schema_arrow
    order = resolve_column_order(rendered.column_order, list(source_schema.names),
                                 notes, table)
    schema = pa.schema([source_schema.field(name) for name in order],
                       metadata=source_schema.metadata)

    row_group_size = None
    rg_bytes = rendered.row_group_size or DEFAULT_ROW_GROUP_BYTES
    width = source_row_width(source_fs, source_files)
    if width:
        row_group_size = max(1024, int(rg_bytes / width))
        notes.append(f"{table}: row group {rg_bytes // 2 ** 20} MiB -> "
                     f"{row_group_size} rows at {width:.1f} B/row measured")
    else:
        notes.append(f"{table}: could not measure row width; row group size "
                     f"left to the writer default")

    kwargs = writer_kwargs(rendered, order, notes, table)
    writer = RotatingWriter(out_fs, out_dir, schema, rendered.target_file_size, kwargs)
    rows = 0
    try:
        for path in source_files:
            with source_fs.open_input_file(path) as handle:
                pf = pq.ParquetFile(handle)
                for batch in pf.iter_batches(batch_size=batch_rows, columns=order):
                    chunk = pa.Table.from_batches([batch]).select(order)
                    writer.write(chunk.cast(schema), row_group_size)
                    rows += batch.num_rows
    finally:
        writer.close()
    return {"rows": rows, "files": len(writer.paths), "column_order": order,
            "row_group_size_rows": row_group_size,
            "writer_kwargs": {k: v for k, v in kwargs.items()
                              if k != "column_encoding" or len(str(v)) < 2000}}


# ------------------------------------------------------------------ verification

def verify_footers(fs, out_dir, sample, expect):
    """Read the written footers back and check every dimension actually landed.

    Verification is not optional decoration here. Three of the six dimensions
    fail *silently* when misapplied: an encoding the type does not support
    downgrades to PLAIN, a codec name the build does not have raises only at
    write time for some codecs, and `write_page_index` defaults off. A run that
    did not verify cannot distinguish "this layout does not help" from "this
    layout was never written".
    """
    import pyarrow.parquet as pq
    try:
        files = list_parquet(fs, out_dir)
    except Exception as exc:
        return {"status": "unknown", "detail": f"{type(exc).__name__}: {exc}"}
    if not files:
        return {"status": "fail", "detail": f"no parquet files under {out_dir}"}

    order = None
    row_groups, sizes = 0, []
    codecs, encodings = set(), set()
    offset_index = 0
    chunks = 0
    for path in files[:sample]:
        with fs.open_input_file(path) as handle:
            md = pq.ParquetFile(handle).metadata
        if order is None:
            order = [md.schema.column(i).name for i in range(md.num_columns)]
        row_groups += md.num_row_groups
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            sizes.append(rg.total_byte_size)
            for c in range(rg.num_columns):
                col = rg.column(c)
                chunks += 1
                codecs.add(str(col.compression))
                encodings.update(str(e) for e in col.encodings)
                offset_index += 1 if col.has_offset_index else 0

    result = {
        "status": "pass",
        "files": len(files),
        "files_sampled": min(len(files), sample),
        "row_groups_sampled": row_groups,
        "row_group_bytes_median": sorted(sizes)[len(sizes) // 2] if sizes else None,
        "row_group_bytes_max": max(sizes) if sizes else None,
        "column_order": order,
        "compression_codecs": sorted(codecs),
        "encodings": sorted(encodings),
        "chunks_with_offset_index": offset_index,
        "chunks_sampled": chunks,
        "page_index_present": offset_index == chunks and chunks > 0,
    }
    failures = []
    if not result["page_index_present"]:
        failures.append(f"OffsetIndex missing on {chunks - offset_index}/{chunks} "
                        f"chunks; the page-index constraint is violated")
    wanted_order = (expect or {}).get("column_order")
    if wanted_order and order != list(wanted_order):
        failures.append(f"column order mismatch: wanted {list(wanted_order)[:5]}..., "
                        f"got {(order or [])[:5]}...")
    wanted_encodings = (expect or {}).get("encodings") or {}
    absent = sorted({e for e in wanted_encodings.values() if e not in encodings})
    if absent:
        failures.append(f"requested encoding(s) {absent} do not appear in the "
                        f"written footers; the writer fell back silently")
    wanted_codecs = {c.upper() for c in ((expect or {}).get("codecs") or [])}
    missing_codecs = sorted(wanted_codecs - {c.upper() for c in codecs})
    if missing_codecs:
        failures.append(f"requested codec(s) {missing_codecs} do not appear in "
                        f"the written footers")
    if failures:
        result["status"] = "fail"
        result["failures"] = failures
    return result


# -------------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True, help="source layout root")
    ap.add_argument("--out", required=True, help="output root for this layout")
    ap.add_argument("--plan", default=None, help="layout plan JSON; omit for baseline")
    ap.add_argument("--tables", nargs="*", default=None,
                    help="subdirectories to rewrite; default is every table found")
    ap.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS)
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--verify-sample", type=int, default=20)
    ap.add_argument("--manifest", default=None)
    args = ap.parse_args()

    plan = BASELINE_CANDIDATE
    if args.plan:
        with open(args.plan) as fh:
            plan = json.load(fh)
    plan_id = plan.get("plan_id") or plan.get("candidate_id", "baseline")
    actions = plan.get("actions", [])

    import pyarrow.fs as pafs
    source_fs, source_base = open_fs(args.source)
    out_fs, out_base = open_fs(args.out)

    tables = args.tables
    if not tables:
        info = source_fs.get_file_info(source_base)
        if info.type == pafs.FileType.Directory:
            tables = sorted(
                f.base_name for f in source_fs.get_file_info(
                    pafs.FileSelector(source_base, recursive=False))
                if f.type == pafs.FileType.Directory and not f.base_name.startswith("_"))
        tables = tables or [os.path.basename(source_base.rstrip("/"))]

    notes, results = [], {}
    violations = []
    per_table_rendered = {}
    for table in tables:
        rendered = render(actions, table=table)
        per_table_rendered[table] = rendered
        violations.extend(check_l0(rendered, writer="pyarrow"))
    if violations:
        print(f"L0 check rejected plan '{plan_id}':")
        for violation in violations:
            print(f"  - {violation}")
        return 2

    print(f"\n# writing layout '{plan_id}' (UC1: PyArrow ParquetWriter)", flush=True)
    for table in tables:
        rendered = per_table_rendered[table]
        src = source_base if len(tables) == 1 and not args.tables else f"{source_base}/{table}"
        files = list_parquet(source_fs, src)
        if not files:
            notes.append(f"{table}: no parquet files under {src}; skipped")
            continue
        dest = f"{out_base.rstrip('/')}/{table}"
        out_fs.create_dir(dest, recursive=True)
        started = time.time()
        rec = write_table(source_fs, files, out_fs, dest, rendered, table,
                          notes, args.batch_rows)
        rec["path"] = dest
        rec["elapsed_seconds"] = round(time.time() - started, 1)
        if args.verify:
            rec["verified"] = verify_footers(out_fs, dest, args.verify_sample, {
                "column_order": rec["column_order"],
                "encodings": rendered.column_encoding,
                "codecs": ([rendered.compression] if rendered.compression else [])
                          + list(rendered.column_compression.values()),
            })
        results[table] = rec
        print(f"  {table:10s} {rec['elapsed_seconds']:7.1f}s  "
              f"{rec['rows']} rows -> {rec['files']} file(s)  {dest}", flush=True)

    manifest = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md r5",
        "use_case": "UC1 (hand-written code calling pyarrow.parquet.ParquetWriter)",
        "writer": "pyarrow",
        "page_index": "pinned on; not read from the plan",
        "candidate_id": plan_id,
        "actions": actions,
        "rendered": {t: r.as_dict() for t, r in per_table_rendered.items()},
        "source": args.source,
        "output": args.out,
        "tables": results,
        "notes": notes,
    }
    manifest_path = args.manifest or (
        os.path.join(args.out, "_layout_manifest_pyarrow.json")
        if "://" not in args.out else
        f"_layout_manifest_pyarrow_{plan_id}.json")
    try:
        parent = os.path.dirname(os.path.abspath(manifest_path))
        os.makedirs(parent, exist_ok=True)
        with open(manifest_path, "w") as fh:
            json.dump(manifest, fh, indent=2)
        print(f"\nmanifest: {manifest_path}", flush=True)
    except OSError:
        print("\nmanifest (could not write locally):", flush=True)
        print(json.dumps(manifest, indent=2), flush=True)

    for note in notes:
        print(f"NOTE: {note}")
    failed = {t: r["verified"] for t, r in results.items()
              if (r.get("verified") or {}).get("status") == "fail"}
    for table, rec in failed.items():
        print(f"VERIFY FAILED {table}:")
        for failure in rec.get("failures", [rec.get("detail")]):
            print(f"  - {failure}")
    return 3 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
