#!/usr/bin/env python3
"""Dataset snapshot: the *current* physical layout, measured, not transcribed.

`workload.BASELINE_GEOMETRY`, `COLUMN_ORDER`, `COLUMN_SHARE` and
`SYNTHETIC_STATS` used to be hand-copied Python literals. That was wrong twice
over. They are not workload: a workload is what the queries ask for, while
these describe how one particular directory happens to be written today. And
they are not general: the moment a candidate layout is materialised, or the
scale factor changes, or someone re-runs the writer, the transcription is
stale and nothing in the pipeline notices.

So this module measures them instead, from the three things a Parquet dataset
already tells you:

  * object listing  -> file count and compressed bytes, exactly.
  * footers (sampled) -> row groups per file, row-group bytes, physical column
    order, per-column byte share, and the baseline clustering `rg_span` that
    Gate A reads.
  * column_stats.json -> NDV, null fraction and the quantile CDF that L1's
    selectivity estimate needs. This one is a separate DuckDB pass because it
    needs the data, not the metadata.

`rg_span` = avg(rg.max - rg.min) / (global.max - global.min). 1.0 means every
row group covers the whole domain, so the baseline cannot prune on that column
and a sort has headroom. Near 0 means it is already ordered. This single number
is what separates TPC-H (l_shipdate 0.999, sorting was a 37% win) from
ClickBench (CounterID 0.080, sorting was a loss), and it used to be a literal
someone typed in.

Usage:
  python3 tools/track2/dataset_snapshot.py \
      --layout s3a://home-haoyue/track2/baseline_sf100 \
      --column-stats docs/.../column_stats.json \
      --out docs/.../dataset_snapshot.json

  # geometry only, no DuckDB pass, sample 4 files per table
  python3 tools/track2/dataset_snapshot.py --layout ./baseline --sample-files 4
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parse_footer import _open_filesystem, _stat_ordinal  # noqa: E402

DEFAULT_SAMPLE_FILES = 8
# Spark's default requested row-group size. Used only as the "what would the
# engine do with no action" reference when a candidate leaves rg unset.
DEFAULT_RG_BYTES = 128 * 1024 * 1024


def _normalise(path):
    """s3a:// is a Hadoop scheme; pyarrow.fs speaks s3://."""
    if path.startswith("s3a://"):
        return "s3://" + path[len("s3a://"):]
    return path


def list_tables(fs, base):
    """Immediate subdirectories of the layout root, each treated as a table."""
    import pyarrow.fs as pafs
    info = fs.get_file_info(base)
    if info.type == pafs.FileType.File:
        return {}
    out = {}
    for f in fs.get_file_info(pafs.FileSelector(base, recursive=False)):
        if f.type == pafs.FileType.Directory and not f.base_name.startswith("_"):
            out[f.base_name] = f.path
    return out


def list_files(fs, base):
    """(path, size) for every parquet object under base, recursively.

    Recursive because an identity-partitioned table is `table/col=v/part.parquet`
    and the file count that Gate D cares about is the total, not the top level.
    """
    import pyarrow.fs as pafs
    out = []
    for f in fs.get_file_info(pafs.FileSelector(base, recursive=True)):
        if f.type == pafs.FileType.File and f.path.endswith(".parquet"):
            out.append((f.path, f.size))
    return sorted(out)


def _sample(items, n):
    """Evenly spaced sample, so a partitioned table is not all one directory."""
    if n <= 0 or len(items) <= n:
        return list(items)
    step = len(items) / n
    return [items[int(i * step)] for i in range(n)]


def scan_footers(fs, files, sample_n):
    """Row-group geometry, column order/share and rg_span from sampled footers."""
    import pyarrow.parquet as pq
    sampled = _sample(files, sample_n)
    rg_counts, rg_bytes, rows_per_file = [], [], []
    order = []
    share = {}
    spans = {}  # column -> [(lo, hi)]
    for path, _size in sampled:
        with fs.open_input_file(path) as handle:
            md = pq.ParquetFile(handle).metadata
        names = [md.schema.column(i).name for i in range(md.num_columns)]
        if not order:
            order = names
        rg_counts.append(md.num_row_groups)
        rows_per_file.append(md.num_rows)
        for rg_idx in range(md.num_row_groups):
            rg = md.row_group(rg_idx)
            rg_bytes.append(rg.total_byte_size)
            for col_idx, name in enumerate(names):
                col = rg.column(col_idx)
                share[name] = share.get(name, 0) + col.total_compressed_size
                stats = col.statistics if col.is_stats_set else None
                if not stats or not stats.has_min_max:
                    continue
                lo = _stat_ordinal(stats.min)
                hi = _stat_ordinal(stats.max)
                if lo is not None and hi is not None:
                    spans.setdefault(name, []).append((lo, hi))

    clustering = {}
    for name, pairs in spans.items():
        glo = min(p[0] for p in pairs)
        ghi = max(p[1] for p in pairs)
        domain = ghi - glo
        avg = sum(p[1] - p[0] for p in pairs) / len(pairs)
        clustering[name] = {
            "rg_span": round((avg / domain) if domain else 0.0, 4),
            "n_rg_sampled": len(pairs),
        }
    # rg_bytes is the *mean*, not the median, because L1 consumes it only as
    # `n_rg x rg_bytes = total uncompressed bytes` when rescaling row-group
    # counts. Spark's lineitem output is trimodal (1 / 161 / 236 MiB row
    # groups, 1.5 per file), so a median over the first N files reports 236 MiB
    # and a median over an even spread reports 161 MiB. Both are legitimate
    # medians and neither reconstructs the total. The median is kept alongside
    # for the readability bound, which really is about the largest sizes seen.
    return {
        "files_sampled": len(sampled),
        "rg_per_file": (len(rg_bytes) / len(sampled)) if sampled else 1.0,
        "rg_bytes": int(statistics.mean(rg_bytes)) if rg_bytes else None,
        "rg_bytes_median": int(statistics.median(rg_bytes)) if rg_bytes else None,
        "rg_bytes_max": max(rg_bytes) if rg_bytes else None,
        "rows_per_file": (statistics.mean(rows_per_file)) if rows_per_file else 0.0,
        "column_order": order,
        "column_share": share,
        "clustering": clustering,
    }


def build(layout, column_stats_path=None, sample_files=DEFAULT_SAMPLE_FILES,
          tables=None):
    """Measure one layout root into a snapshot document."""
    resolved = _normalise(layout)
    fs, base = _open_filesystem(resolved)
    found = list_tables(fs, base)
    if not found:
        # A single-table layout (ClickBench `hits`) pointed at directly.
        found = {os.path.basename(base.rstrip("/")): base}
    if tables:
        found = {t: p for t, p in found.items() if t in set(tables)}

    geometry, column_order, column_share, clustering = {}, {}, {}, {}
    for table, path in sorted(found.items()):
        files = list_files(fs, path)
        if not files:
            continue
        footer = scan_footers(fs, files, sample_files)
        n_rg = max(len(files), int(round(footer["rg_per_file"] * len(files))))
        geometry[table] = {
            "files": len(files),
            "rg_per_file": round(footer["rg_per_file"], 3),
            "rg_bytes": footer["rg_bytes"],
            "rg_bytes_median": footer["rg_bytes_median"],
            "rg_bytes_max": footer["rg_bytes_max"],
            "compressed_bytes": sum(s for _p, s in files),
            "n_rg": n_rg,
            "n_rows": int(round(footer["rows_per_file"] * len(files))),
            "files_sampled": footer["files_sampled"],
            "path": path,
        }
        column_order[table] = footer["column_order"]
        column_share[table] = footer["column_share"]
        clustering[table] = footer["clustering"]

    stats = merge_column_stats(clustering, column_stats_path)
    return {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 3.1/3.2",
        "layout": layout,
        "sample_files": sample_files,
        "source": {
            "geometry": "object listing + sampled parquet footers",
            "column_stats": column_stats_path,
        },
        "geometry": geometry,
        "column_order": column_order,
        "column_share": column_share,
        "column_stats": stats,
    }


def merge_column_stats(clustering, column_stats_path):
    """rg_span (from footers) plus ndv/cdf/null_frac (from the DuckDB pass)."""
    out = {}
    for table, cols in clustering.items():
        out[table] = {"columns": {name: dict(rec) for name, rec in cols.items()}}
    if not column_stats_path or not os.path.exists(column_stats_path):
        return out
    with open(column_stats_path) as fh:
        data = json.load(fh)
    for table, rec in (data.get("tables") or data).items():
        entry = out.setdefault(table, {"columns": {}})
        if rec.get("n_rows") is not None:
            entry["n_rows"] = rec["n_rows"]
        for name, cs in (rec.get("columns") or {}).items():
            merged = dict(entry["columns"].get(name) or {})
            for key, value in cs.items():
                if value is not None:
                    merged[key] = value
            entry["columns"][name] = merged
    return out


class DatasetSnapshot:
    """Measured layout, exposing the names the L1 model already reads.

    Deliberately duck-typed against the old `workload` module: `virtual_footer`
    and `whatif` keep saying `BASELINE_GEOMETRY` and `COLUMN_ORDER`, but the
    values now come from the data instead of a literal someone maintained.
    """

    def __init__(self, doc):
        self.doc = doc
        self.layout = doc.get("layout")
        self.BASELINE_GEOMETRY = doc["geometry"]
        self.COLUMN_ORDER = doc.get("column_order") or {}
        self.COLUMN_SHARE = doc.get("column_share") or {}
        self.ALL_COLUMNS = {t: list(c) for t, c in self.COLUMN_ORDER.items()}
        self.COLUMN_STATS = doc.get("column_stats") or {}
        self.BASELINE_RG_BYTES = DEFAULT_RG_BYTES

    def largest_table(self):
        return max(self.BASELINE_GEOMETRY,
                   key=lambda t: self.BASELINE_GEOMETRY[t]["compressed_bytes"])

    def large_tables(self, min_bytes):
        sizes = {t: g["compressed_bytes"] for t, g in self.BASELINE_GEOMETRY.items()}
        return sorted((t for t, b in sizes.items() if b >= min_bytes),
                      key=lambda t: sizes[t], reverse=True)

    def ndv(self, table, column):
        rec = ((self.COLUMN_STATS.get(table) or {}).get("columns") or {}).get(column)
        return (rec or {}).get("ndv")

    def rg_span(self, table, column):
        rec = ((self.COLUMN_STATS.get(table) or {}).get("columns") or {}).get(column)
        return (rec or {}).get("rg_span")


def load(path):
    with open(path) as fh:
        return DatasetSnapshot(json.load(fh))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--layout", required=True,
                    help="layout root (local, s3:// or s3a://)")
    ap.add_argument("--column-stats", default=None,
                    help="column_stats.json for ndv/cdf; footers give rg_span")
    ap.add_argument("--tables", nargs="*", default=None)
    ap.add_argument("--sample-files", type=int, default=DEFAULT_SAMPLE_FILES,
                    help="footers read per table (0 = all)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    doc = build(args.layout, args.column_stats, args.sample_files, args.tables)
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(doc, fh, indent=2)

    print(f"# dataset snapshot: {args.layout}")
    for table, g in sorted(doc["geometry"].items(),
                           key=lambda kv: -kv[1]["compressed_bytes"]):
        span = doc["column_stats"].get(table, {}).get("columns", {})
        best = sorted(((r.get("rg_span"), c) for c, r in span.items()
                       if r.get("rg_span") is not None))[:1]
        hint = f"  min rg_span {best[0][1]}={best[0][0]:.4f}" if best else ""
        print(f"  {table:12s} files={g['files']:5d} rg/file={g['rg_per_file']:5.2f} "
              f"rg_bytes={(g['rg_bytes'] or 0)/2**20:8.1f}MiB "
              f"{g['compressed_bytes']/2**30:8.2f}GiB{hint}")
    if args.out:
        print(f"  out          {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
