#!/usr/bin/env python3
"""Correlate the three telemetry layers into an ObservationBundle.

Contract role (TRACK2_M0_CONTRACT.md 3.1/3.2, TRACK2_PLAN.md 6 step 2): join

  * physical  -- SdkIoCollector NDJSON: one record per GET with ts_wall_ms,
                 range_offset, range_length, path, audit_* fields
  * format    -- parse_footer.py output: (file, column, row_group) -> [byte_start, byte_end)
  * semantic  -- collect_semantic.py output: per execution_id a time window,
                 scanned files, projected columns, pushed filters

into one ObservationBundle per GET, and report the attribution coverage that the
M1 gate measures (contract 1.2: >=95% of GET bytes map to query -> object ->
row group/column chunk).

Two independent joins happen here:

  * physical -> format: a GET's [offset, offset+length) is intersected with every
    column-chunk range of the same object; each overlapping chunk gets the
    overlapping byte count. This is the layer that must be exact, and it is what
    the coverage gate measures.
  * physical -> semantic: the GET's ts_wall_ms is placed inside a query's time
    window. The MVP runs queries serially, so the window is an exact assignment.
    When the collector carried an audit_sqlid (CommonAuditContext injection), that
    is preferred over the window because it survives concurrency.

A GET is counted as attributed when it maps to at least one column chunk AND to a
query. The coverage metric is bytes-weighted, not request-count-weighted, because
a handful of large footer/metadata reads would otherwise dominate a count.

Usage:
  python3 tools/track2/correlate.py \
      --io track2-io-*.ndjson \
      --footer footer_lineitem.parquet \
      --semantic semantic.json \
      --out observation_bundle.parquet --report coverage.json
"""

import argparse
import bisect
import glob
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone


def load_io_records(patterns):
    """Read one or more NDJSON files of SdkIoCollector records."""
    records = []
    files = []
    for pattern in patterns:
        files.extend(sorted(glob.glob(pattern)))
    for path in files:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # only ranged GETs carry layout information
                if rec.get("range_offset") is None or rec.get("range_length") is None:
                    continue
                records.append(rec)
    return records, files


def _norm_path(p):
    """Normalise a path/URI so footer and IO records agree on the object key."""
    if p is None:
        return None
    for prefix in ("s3a://", "s3://", "file://", "file:"):
        if p.startswith(prefix):
            p = p[len(prefix):]
            break
    return p.lstrip("/")


def load_footer(path):
    """footer records grouped by normalised object key, with a per-object interval index."""
    if path.endswith(".parquet"):
        import pyarrow.parquet as pq
        rows = pq.read_table(path).to_pylist()
    else:
        with open(path) as fh:
            rows = json.load(fh)["records"]

    by_object = defaultdict(list)
    for r in rows:
        key = _norm_path(r["file"])
        by_object[key].append(r)

    # per-object sorted interval index for fast point/range lookup
    index = {}
    for key, chunks in by_object.items():
        chunks.sort(key=lambda c: c["byte_start"])
        starts = [c["byte_start"] for c in chunks]
        index[key] = (starts, chunks)
    return index


def load_semantic(path):
    """Per-execution time windows + scan fragments, plus a sorted window list."""
    with open(path) as fh:
        data = json.load(fh)
    executions = data.get("executions", [])
    windows = []
    for e in executions:
        if e.get("start_ms") is not None and e.get("end_ms") is not None and e.get("scans"):
            windows.append((e["start_ms"], e["end_ms"], e))
    windows.sort(key=lambda w: w[0])
    return windows


def resolve_object(index, object_key):
    """Map an IO path onto a footer index key."""
    key = _norm_path(object_key)
    if not key:
        return None
    if key in index:
        return key
    candidates = [k for k in index if k.endswith(key) or key.endswith(k)]
    if not candidates:
        # last path component match (bucket-qualified vs not)
        base = key.rsplit("/", 1)[-1]
        candidates = [k for k in index if k.endswith("/" + base) or k.endswith(base)]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        return max(candidates, key=len)
    return None


def find_chunks(index, object_key, offset, length):
    """Column chunks of object_key overlapping [offset, offset+length)."""
    key = resolve_object(index, object_key)
    if key is None:
        return []
    starts, chunks = index[key]
    end = offset + length
    out = []
    lo = bisect.bisect_left(starts, offset)
    for i in range(max(0, lo - 1), len(chunks)):
        c = chunks[i]
        if c["byte_start"] >= end:
            break
        if c["byte_end"] > offset:
            overlap = min(end, c["byte_end"]) - max(offset, c["byte_start"])
            out.append((c, overlap))
    return out


def object_data_end(index, object_key):
    """Exclusive end of the last column chunk; GETs past this are footer/metadata."""
    key = resolve_object(index, object_key)
    if key is None:
        return None
    _starts, chunks = index[key]
    return max(c["byte_end"] for c in chunks) if chunks else None


def find_query(windows, ts_ms):
    """The execution whose [start_ms, end_ms] contains ts_ms, or None."""
    starts = [w[0] for w in windows]
    i = bisect.bisect_right(starts, ts_ms) - 1
    if i >= 0:
        start, end, e = windows[i]
        if start <= ts_ms <= end:
            return e
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--io", nargs="+", required=True, help="SdkIoCollector NDJSON file(s)/glob")
    ap.add_argument("--footer", required=True, help="parse_footer.py output (parquet or json)")
    ap.add_argument("--semantic", required=True, help="collect_semantic.py output JSON")
    ap.add_argument("--out", default=None, help="ObservationBundle parquet output")
    ap.add_argument("--report", default=None, help="coverage report JSON")
    args = ap.parse_args()

    io_records, io_files = load_io_records(args.io)
    if not io_records:
        sys.exit("no ranged-GET records found in the IO input")
    footer_index = load_footer(args.footer)
    windows = load_semantic(args.semantic)

    observations = []
    bytes_total = 0
    bytes_overlap = 0
    bytes_metadata = 0
    bytes_attributed = 0
    reqs_attributed = 0
    page_index_chunks = 0
    page_index_total = 0
    unattributed_reasons = defaultdict(int)

    for rec in io_records:
        offset = rec["range_offset"]
        length = rec["range_length"]
        path = rec.get("audit_path") or rec.get("path")
        ts = rec.get("ts_wall_ms")
        bytes_total += length

        chunks = find_chunks(footer_index, path, offset, length)
        overlap = sum(ov for _c, ov in chunks)
        bytes_overlap += overlap
        data_end = object_data_end(footer_index, path)
        is_metadata = overlap == 0 and data_end is not None and offset >= data_end
        if is_metadata:
            bytes_metadata += length

        query = None
        sqlid = rec.get("audit_sqlid")
        if sqlid is not None:
            query = {"execution_id": sqlid, "via": "sqlid"}
        elif ts is not None:
            match = find_query(windows, ts)
            if match:
                query = {"execution_id": match["execution_id"], "via": "time_window"}

        # gate: GET bytes that land in a column chunk AND a query
        attributed = overlap > 0 and query is not None
        if attributed:
            bytes_attributed += overlap
            reqs_attributed += 1
        else:
            if overlap == 0 and not is_metadata:
                unattributed_reasons["no_chunk_match"] += 1
            if is_metadata:
                unattributed_reasons["format_metadata"] += 1
            if query is None:
                unattributed_reasons["no_query_match"] += 1

        for c, _ov in chunks:
            page_index_total += 1
            if c.get("has_offset_index") or c.get("has_column_index"):
                page_index_chunks += 1

        observations.append({
            "ts_wall_ms": ts,
            "object": _norm_path(path),
            "range_offset": offset,
            "range_length": length,
            "overlap_bytes": overlap,
            "is_metadata": is_metadata,
            "latency_ns": rec.get("latency_ns"),
            "http_status": rec.get("http_status"),
            "execution_id": query["execution_id"] if query else None,
            "attribution_via": query["via"] if query else None,
            "chunks": [
                {"column": c["column"], "row_group": c["row_group"],
                 "overlap_bytes": ov, "byte_start": c["byte_start"], "byte_end": c["byte_end"]}
                for c, ov in chunks
            ],
            "attributed": attributed,
        })

    coverage = bytes_attributed / bytes_total if bytes_total else 0.0
    page_index_frac = (page_index_chunks / page_index_total) if page_index_total else 0.0
    page_level_claimed = page_index_frac >= 0.95
    report = {
        "correlated_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 1.2 (M1 gate)",
        "io_files": io_files,
        "ranged_gets": len(io_records),
        "bytes_total": bytes_total,
        "bytes_chunk_overlap": bytes_overlap,
        "bytes_format_metadata": bytes_metadata,
        "bytes_attributed": bytes_attributed,
        "requests_attributed": reqs_attributed,
        "coverage_bytes": round(coverage, 4),
        "coverage_requests": round(reqs_attributed / len(io_records), 4) if io_records else 0.0,
        "gate_threshold": 0.95,
        "gate_pass": coverage >= 0.95,
        "unattributed_reasons": dict(unattributed_reasons),
        "page_coverage": {
            "chunks_with_page_index": page_index_chunks,
            "chunks_touched": page_index_total,
            "fraction": round(page_index_frac, 4),
            "page_level_attribution_claimed": page_level_claimed,
            "note": ("page index present on touched chunks; page-range intersection not yet "
                     "implemented — do not claim page-level attribution")
                     if page_index_frac > 0 else
                     ("no ColumnIndex/OffsetIndex on touched chunks; page-level attribution "
                      "is not claimed (contract: report separately, do not claim)"),
        },
    }

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            pq.write_table(pa.Table.from_pylist(observations), args.out)
        except ModuleNotFoundError:
            with open(args.out + ".json", "w") as fh:
                json.dump(observations, fh, indent=2)
    if args.report:
        os.makedirs(os.path.dirname(os.path.abspath(args.report)) or ".", exist_ok=True)
        with open(args.report, "w") as fh:
            json.dump(report, fh, indent=2)

    print(f"# correlate: {len(io_records)} ranged GETs from {len(io_files)} file(s)")
    print(f"  bytes attributed   {bytes_attributed:,} / {bytes_total:,}")
    print(f"  chunk overlap      {bytes_overlap:,}")
    print(f"  format metadata    {bytes_metadata:,}")
    print(f"  coverage (bytes)   {coverage*100:.2f}%   (gate >= 95%)")
    print(f"  coverage (requests){report['coverage_requests']*100:6.2f}%")
    print(f"  page index         {page_index_chunks}/{page_index_total} "
          f"claimed={page_level_claimed}")
    print(f"  gate               {'PASS' if report['gate_pass'] else 'FAIL'}")
    if unattributed_reasons:
        print(f"  unattributed       {dict(unattributed_reasons)}")
    if args.report:
        print(f"  report             {args.report}")
    return 0 if report["gate_pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
