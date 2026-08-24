#!/usr/bin/env python3
"""FormatMetadataCollector: parse Parquet footers into a byte-range map.

Contract role (TRACK2_M0_CONTRACT.md 3.1/3.2, TRACK2_PLAN.md 2.1): produce the
mapping from (file, column, row_group) to the byte range [start, end) that the
column chunk occupies in the object. correlate.py intersects that range with the
(offset, length) of every physical GET to attribute bytes to a column chunk --
this is the layer that turns "a GET happened" into "query Q read column C of
row group R".

Two things are computed rather than read, because pyarrow does not expose them
directly:

  * chunk byte range. A column chunk's bytes span from its first page (the
    dictionary page if present, else the data page) up to start + compressed
    size. `file_offset` on the chunk is the dictionary/data page offset, not a
    clean chunk start, so the start is taken as min(dictionary_page_offset,
    data_page_offset) and the end as start + total_compressed_size.
  * row group byte range. The union of its column chunks' ranges. Row groups
    are laid out contiguously by every writer we use, so this is exact.

Page-index and bloom presence are recorded per chunk (has_column_index,
has_offset_index, bloom_filter_offset) because the capability matrix
(contract 6) makes claims about them that this collector is expected to
confirm on real layouts.

Read-only: footers are parsed with pyarrow and nothing is written back to the
data. Works on a local path or an s3:// URI (via pyarrow.fs).

Usage:
  # one table of the canonical source
  python3 tools/track2/parse_footer.py \
      --input /data/home/haoyueli/track2-data/tpch_sf100/lineitem \
      --out docs/adaptive-range-reader/results/track2/footer_lineitem.parquet

  # a single file, JSON instead of parquet
  python3 tools/track2/parse_footer.py --input .../part-0000.parquet --format json
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone


def _open_filesystem(path):
    """Return (filesystem, base_path). Local paths use LocalFileSystem."""
    if "://" in path:
        import pyarrow.fs as pafs
        fs, resolved = pafs.FileSystem.from_uri(path)
        return fs, resolved
    import pyarrow.fs as pafs
    return pafs.LocalFileSystem(), path


def list_parquet_files(fs, base):
    """All .parquet files under base (a directory) or base itself (a file)."""
    import pyarrow.fs as pafs
    info = fs.get_file_info(base)
    if info.type == pafs.FileType.File:
        return [base] if base.endswith(".parquet") else []
    selector = pafs.FileSelector(base, recursive=True)
    out = []
    for f in fs.get_file_info(selector):
        if f.type == pafs.FileType.File and f.path.endswith(".parquet"):
            out.append(f.path)
    return sorted(out)


def parse_file(fs, path):
    """Parse one file's footer into a list of column-chunk records."""
    import pyarrow.parquet as pq
    records = []
    with fs.open_input_file(path) as handle:
        pf = pq.ParquetFile(handle)
        md = pf.metadata
        schema_names = [md.schema.column(i).name for i in range(md.num_columns)]
        for rg_idx in range(md.num_row_groups):
            rg = md.row_group(rg_idx)
            rg_start = None
            rg_end = None
            for col_idx in range(rg.num_columns):
                col = rg.column(col_idx)
                dict_off = col.dictionary_page_offset
                data_off = col.data_page_offset
                starts = [o for o in (dict_off, data_off) if o is not None]
                if not starts:
                    continue
                start = min(starts)
                end = start + col.total_compressed_size
                rg_start = start if rg_start is None else min(rg_start, start)
                rg_end = end if rg_end is None else max(rg_end, end)

                stats = col.statistics if col.is_stats_set else None
                records.append({
                    "file": path,
                    "row_group": rg_idx,
                    "rg_num_rows": rg.num_rows,
                    "column_index": col_idx,
                    "column": col.path_in_schema,
                    "physical_type": col.physical_type,
                    "compression": str(col.compression),
                    "byte_start": start,
                    "byte_end": end,
                    "compressed_bytes": col.total_compressed_size,
                    "uncompressed_bytes": col.total_uncompressed_size,
                    "num_values": col.num_values,
                    "encodings": list(col.encodings),
                    "has_column_index": bool(col.has_column_index),
                    "has_offset_index": bool(col.has_offset_index),
                    "has_bloom_filter": col.bloom_filter_offset is not None,
                    "min": _safe_stat(stats, "min"),
                    "max": _safe_stat(stats, "max"),
                    "null_count": stats.null_count if stats and stats.has_null_count else None,
                })
    return records, schema_names


def _safe_stat(stats, attr):
    """min/max as a string, or None.

    Column types differ (int, decimal-as-str, date), so a single output column
    must hold one type. Stringifying keeps the value comparable for the
    clustering/overlap analysis in analyze_layout.py without a per-type schema.
    """
    if stats is None or not stats.has_min_max:
        return None
    value = getattr(stats, attr)
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        return repr(value)
    return str(value)


def _stat_ordinal(value):
    """Map a Parquet min/max to a real so rg_span is dimensionless."""
    if value is None:
        return None
    from datetime import date, datetime
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, date):
        return value.toordinal()
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value)
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        try:
            return date.fromisoformat(text[:10]).toordinal()
        except ValueError:
            return None
    try:
        return float(text)
    except ValueError:
        return None


def clustering_from_path(path, columns=None):
    """Per-column baseline clustering from row-group min/max.

    `rg_span` = avg(rg.max − rg.min) / (global.max − global.min).
    1.0 means every row group covers the full domain (no prune on the
    baseline). Near 0 means the column is already ordered. Strings with
    no numeric/date interpretation are omitted.
    """
    import pyarrow.parquet as pq
    want = set(columns) if columns else None
    fs, base = _open_filesystem(path)
    files = list_parquet_files(fs, base)
    spans = {}  # column -> list of (lo, hi)
    n_rg = 0
    for fpath in files:
        with fs.open_input_file(fpath) as handle:
            md = pq.ParquetFile(handle).metadata
        names = [md.schema.column(i).name for i in range(md.num_columns)]
        for rg_idx in range(md.num_row_groups):
            rg = md.row_group(rg_idx)
            n_rg += 1
            for col_idx, name in enumerate(names):
                if want is not None and name not in want:
                    continue
                stats = rg.column(col_idx).statistics
                if not stats or not stats.has_min_max:
                    continue
                lo = _stat_ordinal(stats.min)
                hi = _stat_ordinal(stats.max)
                if lo is None or hi is None:
                    continue
                spans.setdefault(name, []).append((lo, hi))
    out = {}
    for name, pairs in spans.items():
        glo = min(p[0] for p in pairs)
        ghi = max(p[1] for p in pairs)
        domain = ghi - glo
        avg = sum(p[1] - p[0] for p in pairs) / len(pairs)
        out[name] = {
            "rg_span": round((avg / domain) if domain else 0.0, 4),
            "n_rg_with_stats": len(pairs),
            "n_rg": n_rg,
        }
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", required=True, help="parquet file or directory (local or s3://)")
    ap.add_argument("--out", default=None, help="output path; default stdout summary only")
    ap.add_argument("--format", choices=["parquet", "json", "none"], default="parquet")
    ap.add_argument("--limit", type=int, default=0, help="cap number of files (0 = all)")
    ap.add_argument("--clustering", action="store_true",
                    help="print per-column rg_span from row-group min/max and exit")
    ap.add_argument("--clustering-columns", nargs="*", default=None)
    args = ap.parse_args()

    if args.clustering:
        recs = clustering_from_path(args.input, args.clustering_columns)
        print(f"# clustering: {args.input}")
        for name, rec in sorted(recs.items(), key=lambda kv: kv[1]["rg_span"]):
            print(f"  {name:24s} rg_span={rec['rg_span']:.4f}  "
                  f"rgs={rec['n_rg_with_stats']}/{rec['n_rg']}")
        if args.out:
            os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
            with open(args.out, "w") as fh:
                json.dump({"input": args.input, "columns": recs}, fh, indent=2)
            print(f"  out              {args.out}")
        return 0

    try:
        import pyarrow.parquet as pq  # noqa: F401
    except ModuleNotFoundError:
        sys.exit("pyarrow is not importable; install it or use the track2 venv")

    fs, base = _open_filesystem(args.input)
    files = list_parquet_files(fs, base)
    if args.limit:
        files = files[:args.limit]
    if not files:
        sys.exit(f"no parquet files under {args.input}")

    all_records = []
    per_file = []
    started = datetime.now(timezone.utc)
    for path in files:
        records, schema = parse_file(fs, path)
        all_records.extend(records)
        rgs = {r["row_group"] for r in records}
        per_file.append({
            "file": path,
            "row_groups": len(rgs),
            "columns": len(schema),
            "column_chunks": len(records),
            "bytes": sum(r["compressed_bytes"] for r in records),
        })

    summary = {
        "parsed_at": started.isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 3.1/3.2",
        "input": args.input,
        "files": len(files),
        "column_chunks": len(all_records),
        "row_groups": len({(r["file"], r["row_group"]) for r in all_records}),
        "total_compressed_bytes": sum(r["compressed_bytes"] for r in all_records),
        "with_column_index": sum(1 for r in all_records if r["has_column_index"]),
        "with_offset_index": sum(1 for r in all_records if r["has_offset_index"]),
        "with_bloom_filter": sum(1 for r in all_records if r["has_bloom_filter"]),
        "per_file": per_file,
    }

    if args.out and args.format != "none":
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        if args.format == "json":
            with open(args.out, "w") as fh:
                json.dump({"summary": summary, "records": all_records}, fh, indent=2)
        else:
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pa.Table.from_pylist(all_records)
            pq.write_table(table, args.out)

    print(f"# footer parse: {args.input}")
    print(f"  files            {summary['files']}")
    print(f"  row groups       {summary['row_groups']}")
    print(f"  column chunks    {summary['column_chunks']}")
    print(f"  compressed bytes {summary['total_compressed_bytes']:,}")
    print(f"  column index     {summary['with_column_index']}/{summary['column_chunks']}")
    print(f"  offset index     {summary['with_offset_index']}/{summary['column_chunks']}")
    print(f"  bloom filter     {summary['with_bloom_filter']}/{summary['column_chunks']}")
    if args.out and args.format != "none":
        print(f"  out              {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
