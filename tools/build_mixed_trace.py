#!/usr/bin/env python3
"""Build a balanced, structure-preserving mixed holdout trace for S2/S3 evaluation.

Motivation (see docs/adaptive-range-reader/PROJECT2.md 8.1): evaluating the
adaptive reader on TPC-H alone is misleading - TPC-H is dominated by s3a_random
and never exercises the prefetch / locality paths, so S3 async prefetch shows no
gain. This tool mixes the FIVE workload classes we characterised so both S2 and
S3 can be judged on a representative, class-balanced workload.

Composition (one contiguous SEGMENT per class, concatenated - NOT interleaved,
so each class keeps its own temporal access pattern and the mix naturally
exercises cross-segment policy switching + hysteresis):

  tpch        = tpch_sf1_full (1/2) + clickbench (1/2)
  lance       = lance_fmnist_real (1/2) + lance_sift1m_real (1/2)
  ml_emb      = ml_emb_real (1/2) + ml_lastfm_emb (1/2)
  ml_epoch    = ml_taxi_epoch (whole; only 659 reads, epochs kept intact)
  multi_model = mm_n2000 (whole; keeps train_load/retrieval phases)

Design rules:
  * Structure-preserving down-sampling (never drop random rows):
      - 'query' : keep WHOLE query_id groups in first-seen order until the
                  per-source target is reached (preserves each query's
                  backward-seek / footer-then-columns structure). Used for
                  tpch + clickbench, which carry real query markers.
      - 'block' : keep contiguous 64-read blocks spread evenly across the whole
                  timeline (preserves local sequential / page-revisit runs that
                  the 64-read feature window and template_locality depend on).
                  Used for the strace-derived lance / ml_emb sources.
      - 'all'   : keep every row (ml_epoch, multi_model).
  * object_key is namespaced with the SOURCE tag (e.g. "fmnist/...") because
    several sources share identical keys (all lance/mm sources emit
    "_versions/18446744073709551613.manifest"); without this the reader would
    treat distinct objects as one and mix their sizes/contents.
  * query_id is rewritten to "<class>:<source>:<orig>" so a benchmark can later
    report per-class breakdowns (the benchmark parser ignores this column).
  * timestamps are rebased to be globally monotonic across segments (relative
    timing within a segment is preserved); the benchmark ignores timestamps but
    the heatmap tooling expects monotonicity.

Usage:
  python3 tools/build_mixed_trace.py [--per-source N] [--block B] \
      [--out traces/mixed_holdout.csv] [--traces-dir traces]
"""

import argparse
import csv
import os
import sys
from collections import OrderedDict

# (class, [(source_tag, relative_csv, target_rows_or_None, method), ...])
# target is per-source; None means "keep whole". Class total ~= sum of targets.
PLAN = [
    ("tpch", [
        ("tpch",       "tpch_sf1_full.csv",              1200, "query"),
        ("clickbench", "clickbench.csv",                 1200, "query"),
    ]),
    ("lance", [
        ("fmnist",     "lance_fmnist_real/lance_all.csv", 1200, "block"),
        ("sift",       "lance_sift1m_real/lance_all.csv", 1200, "block"),
    ]),
    ("ml_emb", [
        ("emb_real",   "ml_emb_real.csv",                1200, "block"),
        ("lastfm",     "ml_lastfm_emb.csv",              1200, "block"),
    ]),
    ("ml_epoch", [
        ("taxi",       "ml_taxi_epoch.csv",              None, "all"),
    ]),
    ("multi_model", [
        ("mm",         "mm_n2000/mm_all.csv",            None, "all"),
    ]),
]

HEADER = ["timestamp", "object_key", "offset", "length", "file_size", "query_id"]


class Row:
    __slots__ = ("ts", "key", "offset", "length", "size", "qid")

    def __init__(self, ts, key, offset, length, size, qid):
        self.ts = ts
        self.key = key
        self.offset = offset
        self.length = length
        self.size = size
        self.qid = qid


def read_source(path):
    rows = []
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for f_ in reader:
            if len(f_) < 5 or not f_[1]:
                continue
            ts = float(f_[0]) if f_[0] else 0.0
            qid = f_[5] if len(f_) > 5 else "?"
            rows.append(Row(ts, f_[1], int(f_[2]), int(f_[3]), int(f_[4]), qid))
    return rows


def sample_query(rows, target):
    """Keep whole query_id groups (first-seen order) until >= target rows."""
    groups = OrderedDict()
    for r in rows:
        groups.setdefault(r.qid, []).append(r)
    out = []
    for _, grp in groups.items():
        if out and len(out) >= target:
            break
        out.extend(grp)
    return out


def sample_block(rows, target, block):
    """Keep contiguous `block`-sized runs spread evenly across the timeline."""
    n = len(rows)
    if n <= target:
        return list(rows)
    total_blocks = (n + block - 1) // block
    keep_blocks = max(1, (target + block - 1) // block)
    if keep_blocks >= total_blocks:
        return list(rows)
    out = []
    # Evenly spaced block start indices across [0, total_blocks).
    for i in range(keep_blocks):
        b = (i * total_blocks) // keep_blocks
        start = b * block
        out.extend(rows[start:start + block])
    return out


def select(rows, target, method, block):
    if method == "all" or target is None:
        return list(rows)
    if method == "query":
        return sample_query(rows, target)
    if method == "block":
        return sample_block(rows, target, block)
    raise ValueError("unknown method: " + method)


def main():
    ap = argparse.ArgumentParser(description="Build a mixed holdout trace.")
    ap.add_argument("--traces-dir", default="traces")
    ap.add_argument("--out", default="traces/mixed_holdout.csv")
    ap.add_argument("--per-source", type=int, default=None,
                    help="override per-source target row count for sampled sources")
    ap.add_argument("--block", type=int, default=64,
                    help="block size for 'block' sampling (matches feature window)")
    args = ap.parse_args()

    base = args.block
    summary = []
    out_rows = []
    ts_base = 0.0

    for cls, sources in PLAN:
        cls_count = 0
        for tag, rel, target, method in sources:
            path = os.path.join(args.traces_dir, rel)
            if not os.path.exists(path):
                print("[warn] missing source, skipped: " + path, file=sys.stderr)
                continue
            if args.per_source is not None and method != "all":
                target = args.per_source
            rows = read_source(path)
            picked = select(rows, target, method, base)

            # Rebase timestamps to stay globally monotonic; namespace keys; label.
            seg_min = picked[0].ts if picked else 0.0
            seg_max = seg_min
            for r in picked:
                seg_max = max(seg_max, r.ts)
            for r in picked:
                out_rows.append((
                    round(ts_base + (r.ts - seg_min), 9),
                    tag + "/" + r.key,
                    r.offset,
                    r.length,
                    r.size,
                    cls + ":" + tag + ":" + r.qid,
                ))
            ts_base += (seg_max - seg_min) + 1.0  # 1s gap between segments
            summary.append((cls, tag, len(rows), len(picked), method))
            cls_count += len(picked)
        summary.append((cls, "* TOTAL", "", cls_count, ""))

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(out_rows)

    # Composition summary (stdout + sidecar file).
    lines = []
    lines.append("mixed holdout trace: " + args.out)
    lines.append("total reads: %d" % len(out_rows))
    lines.append("")
    lines.append("%-12s %-12s %10s %10s  %s" %
                 ("class", "source", "src_rows", "picked", "method"))
    lines.append("-" * 60)
    for cls, tag, src_rows, picked, method in summary:
        lines.append("%-12s %-12s %10s %10s  %s" %
                     (cls, tag, src_rows, picked, method))
    report = "\n".join(lines)
    print(report)
    with open(os.path.splitext(args.out)[0] + ".summary.txt", "w") as f:
        f.write(report + "\n")


if __name__ == "__main__":
    main()
