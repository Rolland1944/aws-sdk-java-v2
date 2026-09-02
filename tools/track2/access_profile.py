#!/usr/bin/env python3
"""Access profile: what the workload reads, derived from bytes rather than plans.

This replaces `workload_snapshot.py` (TRACK2_M0_CONTRACT.md r5 §0.1). The
snapshot answered "which columns does query 14 project and what does it filter
on", read out of a Spark event log. That was the right question for a sort/
partition advisor and the wrong dependency for anything else: no event log, no
advice. The v2 action space needs neither predicates nor query identity. It
needs to know which columns are read, how big they are, and which ones are read
*together* -- all three of which are visible in the bytes.

Four things come out of a correlated observation bundle:

  * column weight -- bytes actually transferred per column, and the fraction of
    episodes that touch it. Cold columns fall out of this for free, and are
    reported, never dropped (DB2 §5.5).
  * co-access matrix -- for each column pair, the weight of episodes reading
    both. This is the sole evidence behind the column-order action, and it is
    the reason episodes had to exist at all.
  * access patterns -- episodes grouped by (table, set of columns read). These
    play the role `QUERIES` played in v1: a projection to price, and a count
    saying how often it happens. ClickBench's 43 queries collapse to a few
    dozen distinct column sets, so L1 prices patterns, not files.
  * request shape -- size histogram, per-file open overhead, columns touched
    per row group. This is what the page-size and file-size rules read.

The co-access weight is *episode-weighted, not byte-weighted*. Two columns read
together in 900 short episodes matter more to the layout than two read together
once in a 2 GiB scan, because the layout knob being decided (adjacency, hence
range merging) pays off per request, not per byte.

Usage:
  python3 tools/track2/access_profile.py \
      --observations observation_bundle.parquet \
      --sysconst sysconst.json \
      --out access_profile.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sysconst import BUCKETS, _bucket  # noqa: E402

# Pairs kept in the emitted matrix. The seriation algorithm reads the full
# in-memory matrix; this only bounds what lands in JSON and in an LLM prompt.
DEFAULT_TOP_PAIRS = 400


def table_of(object_key):
    """Infer the table from an object path.

    Layouts are `<root>/<table>/[<col>=<val>/]*part-*.parquet`, so the table is
    the last path component that is not a partition directory. Nothing here
    needs a catalog: the advisor only uses the table name to keep per-table
    geometry apart, and a wrong guess degrades to "one table", not to a wrong
    recommendation.
    """
    if not object_key:
        return "unknown"
    parts = [p for p in object_key.split("/") if p]
    parts = parts[:-1]  # drop the file name
    while parts and "=" in parts[-1]:
        parts.pop()
    return parts[-1] if parts else "unknown"


def load_observations(path):
    """Rows from correlate.py, either parquet or its JSON fallback."""
    if path.endswith(".parquet"):
        import pyarrow.parquet as pq
        return pq.read_table(path).to_pylist()
    with open(path) as fh:
        return json.load(fh)


def build_episodes(observations):
    """Fold per-request observations into per-episode records.

    An episode carries the set of columns it touched, the bytes it moved, the
    row groups it entered and how many requests it took to do that. Requests
    that hit no chunk (footer, page index) are counted as open overhead on the
    episode rather than discarded -- they are exactly the cost the file-size
    action trades against.
    """
    episodes = {}
    for obs in observations:
        eid = obs.get("episode_id")
        if eid is None:
            continue
        ep = episodes.get(eid)
        if ep is None:
            ep = episodes[eid] = {
                "episode_id": eid,
                "object": obs.get("object"),
                "table": table_of(obs.get("object")),
                "columns": set(),
                "row_groups": set(),
                "data_bytes": 0,
                "meta_bytes": 0,
                "data_requests": 0,
                "meta_requests": 0,
                "column_bytes": defaultdict(int),
            }
        chunks = obs.get("chunks") or []
        if chunks:
            ep["data_requests"] += 1
            ep["data_bytes"] += obs.get("overlap_bytes") or 0
            for chunk in chunks:
                ep["columns"].add(chunk["column"])
                ep["row_groups"].add(chunk["row_group"])
                ep["column_bytes"][chunk["column"]] += chunk.get("overlap_bytes") or 0
        else:
            ep["meta_requests"] += 1
            ep["meta_bytes"] += obs.get("range_length") or 0
    return list(episodes.values())


def column_weights(episodes, all_columns=None):
    """Per-column bytes and episode counts, plus the cold-column list."""
    by_table = defaultdict(lambda: defaultdict(
        lambda: {"bytes": 0, "episodes": 0}))
    table_episodes = Counter()
    for ep in episodes:
        table = ep["table"]
        if not ep["columns"]:
            continue
        table_episodes[table] += 1
        for col in ep["columns"]:
            rec = by_table[table][col]
            rec["episodes"] += 1
            rec["bytes"] += ep["column_bytes"].get(col, 0)

    out = {}
    cold = []
    for table, cols in by_table.items():
        total_b = sum(r["bytes"] for r in cols.values()) or 1
        n_ep = table_episodes[table] or 1
        out[table] = {
            col: {
                "bytes": rec["bytes"],
                "byte_share": round(rec["bytes"] / total_b, 6),
                "episodes": rec["episodes"],
                "episode_share": round(rec["episodes"] / n_ep, 6),
            }
            for col, rec in sorted(cols.items(), key=lambda kv: -kv[1]["bytes"])
        }
    # Cold columns need the schema, which observations alone cannot supply:
    # a column nobody read produces no bytes and therefore no record.
    for table, schema in (all_columns or {}).items():
        touched = set(out.get(table) or {})
        for col in schema:
            if col not in touched:
                cold.append({"table": table, "column": col,
                             "action": "report_only_do_not_drop"})
    return out, cold, dict(table_episodes)


def coaccess(episodes):
    """Episode-weighted co-access weight per unordered column pair.

    Weighted by episode count rather than bytes: adjacency pays off per merged
    request, so the pair read together in many small episodes is the one worth
    placing side by side.
    """
    pairs = defaultdict(Counter)
    for ep in episodes:
        cols = sorted(ep["columns"])
        if len(cols) < 2:
            continue
        for a, b in combinations(cols, 2):
            pairs[ep["table"]][(a, b)] += 1
    return pairs


def access_patterns(episodes, min_episodes=1):
    """Episodes grouped by (table, column set) -- the v2 stand-in for QUERIES.

    L1 prices one representative of each pattern and multiplies by the episode
    count, which is both cheaper and more honest than pricing per file: the
    thing that varies across a workload is the projection, not which part-file
    happened to serve it.
    """
    groups = defaultdict(lambda: {"n_episodes": 0, "data_bytes": 0,
                                  "data_requests": 0, "meta_requests": 0,
                                  "rg_touched": 0, "objects": set()})
    for ep in episodes:
        if not ep["columns"]:
            continue
        key = (ep["table"], tuple(sorted(ep["columns"])))
        g = groups[key]
        g["n_episodes"] += 1
        g["data_bytes"] += ep["data_bytes"]
        g["data_requests"] += ep["data_requests"]
        g["meta_requests"] += ep["meta_requests"]
        g["rg_touched"] += len(ep["row_groups"])
        g["objects"].add(ep["object"])

    out = []
    for (table, cols), g in sorted(groups.items(), key=lambda kv: -kv[1]["n_episodes"]):
        if g["n_episodes"] < min_episodes:
            continue
        n = g["n_episodes"]
        out.append({
            "pattern_id": f"{table}/{len(out):04d}",
            "table": table,
            "columns": list(cols),
            "n_columns": len(cols),
            "n_episodes": n,
            "n_objects": len(g["objects"]),
            "data_bytes": g["data_bytes"],
            "bytes_per_episode": int(g["data_bytes"] / n),
            "requests_per_episode": round(g["data_requests"] / n, 2),
            "meta_requests_per_episode": round(g["meta_requests"] / n, 2),
            "rg_per_episode": round(g["rg_touched"] / n, 2),
        })
    return out


def request_shape(observations, episodes):
    """Size histogram and per-open overhead: what the page/file rules read."""
    buckets = Counter()
    bucket_bytes = Counter()
    data_reqs = meta_reqs = 0
    data_bytes = meta_bytes = 0
    for obs in observations:
        length = obs.get("range_length") or 0
        if not length:
            continue
        buckets[_bucket(length)] += 1
        bucket_bytes[_bucket(length)] += length
        if obs.get("chunks"):
            data_reqs += 1
            data_bytes += obs.get("overlap_bytes") or 0
        else:
            meta_reqs += 1
            meta_bytes += length

    total_reqs = data_reqs + meta_reqs
    objects = {ep["object"] for ep in episodes}
    n_ep = max(len(episodes), 1)
    # A chunk split across several requests means the reader could not merge
    # it -- either the page granularity or the column adjacency is wrong. This
    # ratio is what the page-size rule keys on.
    splits = sum(ep["data_requests"] for ep in episodes)
    chunks_touched = sum(len(ep["columns"]) for ep in episodes) or 1
    return {
        "buckets": {name: buckets.get(name, 0) for name, _lo, _hi in BUCKETS},
        "bucket_bytes": {name: bucket_bytes.get(name, 0) for name, _lo, _hi in BUCKETS},
        "tiny_get_fraction": round(buckets.get("<64KiB", 0) / max(total_reqs, 1), 4),
        "data_requests": data_reqs,
        "meta_requests": meta_reqs,
        "data_bytes": data_bytes,
        "meta_bytes": meta_bytes,
        "meta_request_fraction": round(meta_reqs / max(total_reqs, 1), 4),
        "unique_objects": len(objects),
        "meta_requests_per_object": round(meta_reqs / max(len(objects), 1), 2),
        "requests_per_episode": round(total_reqs / n_ep, 2),
        "requests_per_chunk_touched": round(splits / chunks_touched, 3),
    }


def build(observations, all_columns=None, sysconst=None, regime=None,
          top_pairs=DEFAULT_TOP_PAIRS, min_episodes=1):
    episodes = build_episodes(observations)
    if not episodes:
        raise SystemExit("no episodes in the observation bundle; re-run "
                         "correlate.py (episode_id must be present)")
    weights, cold, table_episodes = column_weights(episodes, all_columns)
    pairs = coaccess(episodes)
    patterns = access_patterns(episodes, min_episodes)

    matrix = {}
    for table, counter in pairs.items():
        matrix[table] = [
            {"a": a, "b": b, "weight": w}
            for (a, b), w in counter.most_common(top_pairs)
        ]

    doc = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md r5 §0.1",
        "source": {"layers": ["sdk_io", "parquet_footer"],
                   "semantic_layer": "removed (r5); no query plans are read"},
        "n_episodes": len(episodes),
        "episodes_per_table": table_episodes,
        "column_weight": weights,
        "cold_columns": cold,
        "cold_policy": "DB2 §5.5: evidence only, never a drop recommendation",
        "coaccess": matrix,
        "coaccess_weighting": "episodes (not bytes): adjacency pays per request",
        "patterns": patterns,
        "request_shape": request_shape(observations, episodes),
    }
    if sysconst:
        name = regime or sysconst.get("default_regime") or "measured_cross_cloud"
        doc["regime"] = {"name": name, **(sysconst.get("regimes") or {}).get(name, {})}
        doc["vectored"] = sysconst.get("vectored") or {}
    return doc


class AccessProfile:
    """The evidence the advisor reads, duck-typed where v1 read a workload.

    `PATTERNS` occupies the slot `QUERIES` used to, but the unit changed from a
    query to an access pattern, so the name changed with it: anything still
    asking for `QUERIES` is asking for predicates that no longer exist.
    """

    def __init__(self, doc):
        self.doc = doc
        self.COLUMN_WEIGHT = doc.get("column_weight") or {}
        self.COLD_COLUMNS = doc.get("cold_columns") or []
        self.PATTERNS = doc.get("patterns") or []
        self.REQUEST_SHAPE = doc.get("request_shape") or {}
        self.n_episodes = doc.get("n_episodes") or 0
        self._coaccess = doc.get("coaccess") or {}

    def tables(self):
        return sorted({p["table"] for p in self.PATTERNS})

    def patterns_for(self, table):
        return [p for p in self.PATTERNS if p["table"] == table]

    def columns_read(self, table):
        """Columns with any observed traffic, hottest first."""
        return list((self.COLUMN_WEIGHT.get(table) or {}).keys())

    def coaccess_matrix(self, table):
        """{(a, b): weight} with both orderings, for the seriation pass."""
        out = {}
        for rec in self._coaccess.get(table) or []:
            out[(rec["a"], rec["b"])] = rec["weight"]
            out[(rec["b"], rec["a"])] = rec["weight"]
        return out

    def column_bytes_share(self, table):
        return {c: r["byte_share"]
                for c, r in (self.COLUMN_WEIGHT.get(table) or {}).items()}

    def provenance(self):
        return {
            "collected_at": self.doc.get("collected_at"),
            "n_episodes": self.n_episodes,
            "n_patterns": len(self.PATTERNS),
            "layers": (self.doc.get("source") or {}).get("layers"),
        }


def load(path):
    with open(path) as fh:
        return AccessProfile(json.load(fh))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--observations", required=True,
                    help="correlate.py ObservationBundle (parquet or json)")
    ap.add_argument("--dataset-snapshot", default=None,
                    help="supplies the full schema so cold columns can be listed")
    ap.add_argument("--sysconst", default=None, help="sysconst.py output")
    ap.add_argument("--regime", default=None)
    ap.add_argument("--top-pairs", type=int, default=DEFAULT_TOP_PAIRS)
    ap.add_argument("--min-episodes", type=int, default=1,
                    help="drop access patterns seen fewer times than this")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    observations = load_observations(args.observations)
    all_columns = None
    if args.dataset_snapshot and os.path.exists(args.dataset_snapshot):
        with open(args.dataset_snapshot) as fh:
            all_columns = json.load(fh).get("column_order") or {}
    sysc = None
    if args.sysconst and os.path.exists(args.sysconst):
        with open(args.sysconst) as fh:
            sysc = json.load(fh)

    doc = build(observations, all_columns, sysc, args.regime,
                args.top_pairs, args.min_episodes)
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(doc, fh, indent=2)

    shape = doc["request_shape"]
    print(f"# access profile: {doc['n_episodes']} episodes, "
          f"{len(doc['patterns'])} patterns")
    for table, cols in doc["column_weight"].items():
        top = list(cols.items())[:5]
        hot = ", ".join(f"{c}({r['episode_share']*100:.0f}%)" for c, r in top)
        print(f"  {table:12s} {len(cols):4d} columns read   hot: {hot}")
    print(f"  tiny GETs          {shape['tiny_get_fraction']*100:.1f}%")
    print(f"  meta req/object    {shape['meta_requests_per_object']}")
    print(f"  req/chunk touched  {shape['requests_per_chunk_touched']}")
    if doc["cold_columns"]:
        print(f"  cold columns       {len(doc['cold_columns'])} (reported, never dropped)")
    if args.out:
        print(f"  out                {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
