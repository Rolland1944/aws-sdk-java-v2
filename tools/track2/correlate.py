#!/usr/bin/env python3
"""Correlate SDK byte access with Parquet footers into an ObservationBundle.

Contract role (TRACK2_M0_CONTRACT.md r5 §0.1, TRACK2_V2_PLAN.md §3.2): join

  * physical  -- SdkIoCollector NDJSON: one record per GET with ts_wall_ms,
                 range_offset, range_length, path, thread
  * format    -- parse_footer.py output: (file, column, row_group) -> [byte_start, byte_end)

into one ObservationBundle per GET, and report chunk-attribution coverage.

r5 removed the third join. v1 also placed each GET inside a Spark execution
window, which made every downstream conclusion conditional on there *being* a
Spark event log -- so the advisor could only ever advise a SQL engine on a
layout, and a Lance or ML data loader got nothing. The geometric join is the
part that carries layout information, and it needs no engine at all:

    GET [offset, offset+length) ∩ chunk [byte_start, byte_end)

What the execution id was actually used for downstream was grouping: which
columns get read *together*. That is recoverable without semantics. An
**access episode** is a run of requests on the same (thread, object) with no
gap longer than --episode-gap-ms. One Spark task reading one file is one
episode; so is one PyArrow `read_table`. Columns co-occurring in an episode is
the evidence the column-order action is built on (access_profile.py).

Two things episodes are not. They are not queries: a query that scans 40 files
across 16 threads is 640 episodes, not 1, so episode *counts* mean nothing on
their own. And they are not exact under thread reuse -- a pool thread that
picks up a new task within the gap threshold merges two episodes. The gap is
therefore a knob, and --report prints the episode size distribution so a
degenerate setting (everything one episode, or every GET its own) is visible
rather than silent.

Usage:
  python3 tools/track2/correlate.py \
      --io 'track2-io-*.ndjson' \
      --footer footer_hits.parquet \
      --out observation_bundle.parquet --report coverage.json
"""

import argparse
import bisect
import glob
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone

# One task's requests on one object arrive back to back. 2s is far above the
# inter-request spacing inside a vectored read even at cross-cloud RTT (228ms
# measured), and far below the gap between two Spark tasks on the same pool
# thread. Both ends are visible in the episode histogram the report prints.
DEFAULT_EPISODE_GAP_MS = 2000

# Chunk attribution is the only gate left after the semantic join was removed.
DEFAULT_COVERAGE_GATE = 0.95


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


def assign_episodes(records, gap_ms=DEFAULT_EPISODE_GAP_MS):
    """Group requests into access episodes; returns a list of episode ids.

    An episode is a maximal run of requests on one (thread, object) whose
    consecutive timestamps differ by at most `gap_ms`. Records with no usable
    timestamp fall back to one episode per (thread, object), which keeps the
    co-access signal rather than dropping the request.

    Returned ids are positional and parallel to `records`.
    """
    buckets = defaultdict(list)
    for i, rec in enumerate(records):
        thread = rec.get("thread") or "?"
        obj = _norm_path(rec.get("audit_path") or rec.get("path")) or "?"
        buckets[(thread, obj)].append(i)

    episode_of = [None] * len(records)
    next_id = 0
    for (thread, obj), idxs in sorted(buckets.items()):
        idxs.sort(key=lambda i: (records[i].get("ts_wall_ms") or 0,
                                 records[i].get("ts_start_ns") or 0))
        prev_ts = None
        current = None
        for i in idxs:
            ts = records[i].get("ts_wall_ms")
            if current is None or (ts is not None and prev_ts is not None
                                   and ts - prev_ts > gap_ms):
                current = next_id
                next_id += 1
            episode_of[i] = current
            if ts is not None:
                prev_ts = ts
    return episode_of, next_id


def episode_summary(observations, n_episodes):
    """Size distribution, so a degenerate --episode-gap-ms is visible."""
    sizes = defaultdict(int)
    columns = defaultdict(set)
    for obs in observations:
        eid = obs["episode_id"]
        if eid is None:
            continue
        sizes[eid] += 1
        for chunk in obs["chunks"]:
            columns[eid].add(chunk["column"])
    counts = sorted(sizes.values())
    col_counts = sorted(len(c) for c in columns.values()) or [0]
    if not counts:
        return {"n_episodes": 0}
    return {
        "n_episodes": n_episodes,
        "requests_per_episode": {
            "min": counts[0],
            "median": int(statistics.median(counts)),
            "max": counts[-1],
            "mean": round(sum(counts) / len(counts), 2),
        },
        "columns_per_episode": {
            "min": col_counts[0],
            "median": int(statistics.median(col_counts)),
            "max": col_counts[-1],
        },
        "note": ("an episode is one (thread, object) run within the gap "
                 "threshold; it is not a query. Degenerate settings show up "
                 "here as median=1 (gap too small) or n_episodes≈n_objects "
                 "(gap too large)."),
    }


def build(io_records, footer_index, gap_ms=DEFAULT_EPISODE_GAP_MS):
    """Attribute every ranged GET to column chunks and an access episode."""
    episode_of, n_episodes = assign_episodes(io_records, gap_ms)

    observations = []
    stats = {
        "bytes_total": 0,
        "bytes_overlap": 0,
        "bytes_metadata": 0,
        "requests_attributed": 0,
        "page_index_chunks": 0,
        "page_index_total": 0,
    }
    unattributed = defaultdict(int)

    for i, rec in enumerate(io_records):
        offset = rec["range_offset"]
        length = rec["range_length"]
        path = rec.get("audit_path") or rec.get("path")
        ts = rec.get("ts_wall_ms")
        stats["bytes_total"] += length

        chunks = find_chunks(footer_index, path, offset, length)
        overlap = sum(ov for _c, ov in chunks)
        stats["bytes_overlap"] += overlap
        data_end = object_data_end(footer_index, path)
        is_metadata = overlap == 0 and data_end is not None and offset >= data_end
        if is_metadata:
            stats["bytes_metadata"] += length

        # r5: attribution is chunk-only. A GET either lands in a column chunk
        # (layout-relevant) or it is footer/page-index traffic (accounted
        # separately, and priced by L1 as per-open overhead).
        attributed = overlap > 0
        if attributed:
            stats["requests_attributed"] += 1
        elif is_metadata:
            unattributed["format_metadata"] += 1
        else:
            unattributed["no_chunk_match"] += 1

        for c, _ov in chunks:
            stats["page_index_total"] += 1
            if c.get("has_offset_index") or c.get("has_column_index"):
                stats["page_index_chunks"] += 1

        observations.append({
            "ts_wall_ms": ts,
            "thread": rec.get("thread"),
            "episode_id": episode_of[i],
            "object": _norm_path(path),
            "range_offset": offset,
            "range_length": length,
            "overlap_bytes": overlap,
            "is_metadata": is_metadata,
            "latency_ns": rec.get("latency_ns"),
            "http_status": rec.get("http_status"),
            "chunks": [
                {"column": c["column"], "row_group": c["row_group"],
                 "overlap_bytes": ov, "byte_start": c["byte_start"],
                 "byte_end": c["byte_end"]}
                for c, ov in chunks
            ],
            "attributed": attributed,
        })

    return observations, stats, dict(unattributed), n_episodes


def coverage_report(io_files, io_records, observations, stats, unattributed,
                    n_episodes, gap_ms, gate=DEFAULT_COVERAGE_GATE):
    total = stats["bytes_total"]
    # Denominator excludes footer/page-index reads: they are real traffic but
    # by construction cannot land in a column chunk, so counting them as
    # unattributed would cap coverage below 100% no matter how exact the join.
    data_bytes = total - stats["bytes_metadata"]
    coverage = stats["bytes_overlap"] / data_bytes if data_bytes else 0.0
    page_total = stats["page_index_total"]
    page_frac = (stats["page_index_chunks"] / page_total) if page_total else 0.0
    return {
        "correlated_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md r5 §0.1",
        "layers": ["sdk_io", "parquet_footer"],
        "io_files": io_files,
        "ranged_gets": len(io_records),
        "bytes_total": total,
        "bytes_chunk_overlap": stats["bytes_overlap"],
        "bytes_format_metadata": stats["bytes_metadata"],
        "bytes_data": data_bytes,
        "requests_attributed": stats["requests_attributed"],
        "coverage_bytes": round(coverage, 4),
        "coverage_requests": (round(stats["requests_attributed"] / len(io_records), 4)
                              if io_records else 0.0),
        "gate_threshold": gate,
        "gate_pass": coverage >= gate,
        "gate_note": ("chunk attribution only; the query-attribution half of the "
                      "v1 gate was removed with the Semantic layer (r5)"),
        "unattributed_reasons": unattributed,
        "episodes": dict(episode_summary(observations, n_episodes),
                         gap_ms=gap_ms),
        "page_coverage": {
            "chunks_with_page_index": stats["page_index_chunks"],
            "chunks_touched": page_total,
            "fraction": round(page_frac, 4),
            "note": ("page index present on touched chunks; page-range "
                     "intersection not implemented — do not claim page-level "
                     "attribution"),
        },
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--io", nargs="+", required=True,
                    help="SdkIoCollector NDJSON file(s)/glob")
    ap.add_argument("--footer", required=True,
                    help="parse_footer.py output (parquet or json)")
    ap.add_argument("--episode-gap-ms", type=int, default=DEFAULT_EPISODE_GAP_MS,
                    help="max gap within one (thread, object) access episode")
    ap.add_argument("--coverage-gate", type=float, default=DEFAULT_COVERAGE_GATE,
                    help="minimum chunk-attributed fraction of data bytes")
    ap.add_argument("--out", default=None, help="ObservationBundle parquet output")
    ap.add_argument("--report", default=None, help="coverage report JSON")
    args = ap.parse_args()

    io_records, io_files = load_io_records(args.io)
    if not io_records:
        sys.exit("no ranged-GET records found in the IO input")
    footer_index = load_footer(args.footer)

    observations, stats, unattributed, n_episodes = build(
        io_records, footer_index, args.episode_gap_ms)
    report = coverage_report(io_files, io_records, observations, stats,
                             unattributed, n_episodes, args.episode_gap_ms,
                             args.coverage_gate)

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

    ep = report["episodes"]
    print(f"# correlate: {len(io_records)} ranged GETs from {len(io_files)} file(s)")
    print(f"  chunk overlap      {stats['bytes_overlap']:,} / {report['bytes_data']:,} data bytes")
    print(f"  format metadata    {stats['bytes_metadata']:,}")
    print(f"  coverage (bytes)   {report['coverage_bytes']*100:.2f}%   "
          f"(gate >= {args.coverage_gate*100:.0f}%)")
    print(f"  coverage (requests){report['coverage_requests']*100:6.2f}%")
    if ep.get("n_episodes"):
        print(f"  episodes           {ep['n_episodes']} "
              f"(median {ep['requests_per_episode']['median']} req, "
              f"{ep['columns_per_episode']['median']} cols)")
    print(f"  page index         {stats['page_index_chunks']}/{stats['page_index_total']}")
    print(f"  gate               {'PASS' if report['gate_pass'] else 'FAIL'}")
    if unattributed:
        print(f"  unattributed       {unattributed}")
    if args.report:
        print(f"  report             {args.report}")
    return 0 if report["gate_pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
