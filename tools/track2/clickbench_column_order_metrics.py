#!/usr/bin/env python3
"""Measure exact order-only geometry and range-merge mechanisms for ClickBench.

The input is a materialised local layout.  This tool never reads table data:
all measurements come from Parquet footers, so chunk span and predicted range
coalescing can be reported independently of benchmark timings.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import defaultdict

MIN_SEEK_BYTES = 128 * 1024
MAX_MERGED_BYTES = 2 * 1024 * 1024


def digest(value):
    return hashlib.sha256(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def files(root):
    return sorted(
        os.path.join(dirpath, name)
        for dirpath, _dirs, names in os.walk(root)
        for name in names
        if name.endswith(".parquet")
    )


def chunk_range(column):
    starts = [x for x in (column.dictionary_page_offset, column.data_page_offset)
              if x is not None and x >= 0]
    if not starts:
        return None
    start = min(starts)
    return start, start + column.total_compressed_size


def merged_ranges(chunks):
    """Apply the frozen 128 KiB / 2 MiB vector range merge policy."""
    spans = sorted(chunks)
    merged = []
    for start, end in spans:
        if not merged:
            merged.append([start, end])
            continue
        previous = merged[-1]
        if start - previous[1] <= MIN_SEEK_BYTES and end - previous[0] <= MAX_MERGED_BYTES:
            previous[1] = max(previous[1], end)
        else:
            merged.append([start, end])
    return merged


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--layout", required=True, help="local layout root")
    ap.add_argument("--protocol", required=True)
    ap.add_argument("--arm", choices=("co-located", "separated"), required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with open(args.protocol) as fh:
        protocol = json.load(fh)
    expected_order = protocol["orders"][args.arm]
    primary = {
        int(q): columns
        for q, columns in protocol["queries"]["primary_query_columns"].items()
    }
    table_root = os.path.join(args.layout, "hits")
    parquet_files = files(table_root)
    if not parquet_files:
        raise SystemExit(f"no parquet files under {table_root}")

    import pyarrow.parquet as pq

    file_rg_counts, all_rg_rows, schemas = [], [], []
    aggregate = {
        q: {"row_groups": 0, "span_bytes": 0, "ranges": 0, "range_bytes": 0}
        for q in primary
    }
    for path in parquet_files:
        pf = pq.ParquetFile(path)
        md = pf.metadata
        names = [md.schema.column(i).name for i in range(md.num_columns)]
        schemas.append(names)
        file_rg_counts.append(md.num_row_groups)
        index = {name: i for i, name in enumerate(names)}
        for q, columns in primary.items():
            missing = [c for c in columns if c not in index]
            if missing:
                raise SystemExit(f"{path}: Q{q} columns absent from footer: {missing}")
        for rg_index in range(md.num_row_groups):
            rg = md.row_group(rg_index)
            all_rg_rows.append(rg.num_rows)
            for q, columns in primary.items():
                chunks = []
                for name in columns:
                    item = chunk_range(rg.column(index[name]))
                    if item is None:
                        raise SystemExit(f"{path}: RG {rg_index} {name} has no chunk offset")
                    chunks.append(item)
                merged = merged_ranges(chunks)
                rec = aggregate[q]
                rec["row_groups"] += 1
                rec["span_bytes"] += max(end for _start, end in chunks) - min(
                    start for start, _end in chunks
                )
                rec["ranges"] += len(merged)
                rec["range_bytes"] += sum(end - start for start, end in merged)

    if any(schema != expected_order for schema in schemas):
        bad = next(schema for schema in schemas if schema != expected_order)
        raise SystemExit(
            f"footer schema differs from frozen {args.arm} order: "
            f"wanted {expected_order[:5]}, got {bad[:5]}"
        )

    mechanism = {}
    for q, rec in aggregate.items():
        n = rec.pop("row_groups")
        mechanism[str(q)] = {
            "row_groups": n,
            "mean_chunk_span_bytes_per_rg": rec["span_bytes"] / n,
            "mean_predicted_ranges_per_rg": rec["ranges"] / n,
            "mean_predicted_range_bytes_per_rg": rec["range_bytes"] / n,
        }
    summary = {
        key: sum(item[key] for item in mechanism.values()) / len(mechanism)
        for key in (
            "mean_chunk_span_bytes_per_rg",
            "mean_predicted_ranges_per_rg",
            "mean_predicted_range_bytes_per_rg",
        )
    }
    out = {
        "layout": os.path.abspath(args.layout),
        "arm": args.arm,
        "frozen_order_sha256": digest(expected_order),
        "geometry": {
            "files": len(parquet_files),
            "row_groups": len(all_rg_rows),
            "rows": sum(all_rg_rows),
            "file_rg_counts": file_rg_counts,
            "file_rg_counts_sha256": digest(file_rg_counts),
            "rg_row_counts_sha256": digest(all_rg_rows),
        },
        "mechanism_by_primary_query": mechanism,
        "mechanism_primary_mean": summary,
        "merge_policy": {
            "min_seek_bytes": MIN_SEEK_BYTES,
            "max_merged_bytes": MAX_MERGED_BYTES,
        },
    }
    with open(args.out, "w") as fh:
        json.dump(out, fh, indent=2)
        fh.write("\n")
    print(json.dumps({
        "arm": args.arm,
        **out["geometry"],
        **summary,
    }, indent=2))


if __name__ == "__main__":
    main()
