#!/usr/bin/env python3
"""Freeze the real-ClickBench column-order positive-control protocol.

The experiment has one fixed primary query cohort and two deliberately opposed
physical orders.  It does not generate rows or modify query texts: both arms
rewrite the same official ClickBench SF1 input with the same parent actions.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from layout_actions import check_l0, render  # noqa: E402

PRIMARY_QUERIES = [15, 17, 18, 22, 23, 28, 31, 32, 33, 37, 38]
PRIMARY_RULE = (
    "Official ClickBench queries with 2-8 catalogued columns; exclude SELECT *, "
    "REGEXP_REPLACE, Q30's large SUM chain, and projection-only ORDER BY queries."
)
# This is the union of the primary cohort's projected/filter columns, in an
# order that keeps the two URL/text paths and the shared grouping/filter paths
# together.  It is intentionally frozen before any new timing run.
HOT_COLUMNS = [
    "SearchPhrase", "URL", "Title", "UserID", "WatchID", "ClientIP",
    "SearchEngineID", "IsRefresh", "ResolutionWidth", "CounterID",
    "EventDate", "DontCountHits",
]
# Wide, non-primary columns chosen before rewriting from the existing SF1
# footprint.  They create a deterministic anti-seriation order without
# inventing data or selecting based on the new experiment's measurements.
COLD_SEPARATORS = [
    "OriginalURL", "HID", "URLHash", "RefererHash", "ClientEventTime",
    "LocalEventTime", "SocialSourcePage", "OpenstatServiceName", "Params",
    "UserAgent", "MobilePhone", "MobilePhoneModel",
]
PRIMARY_QUERY_COLUMNS = {
    15: ["SearchEngineID", "SearchPhrase"],
    17: ["UserID", "SearchPhrase"],
    18: ["UserID", "SearchPhrase"],
    22: ["SearchPhrase", "URL"],
    23: ["SearchPhrase", "URL", "Title", "UserID"],
    28: ["CounterID", "URL"],
    31: ["SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth",
         "SearchPhrase"],
    32: ["WatchID", "ClientIP", "IsRefresh", "ResolutionWidth",
         "SearchPhrase"],
    33: ["WatchID", "ClientIP", "IsRefresh", "ResolutionWidth"],
    37: ["URL", "CounterID", "EventDate", "DontCountHits", "IsRefresh"],
    38: ["Title", "CounterID", "EventDate", "DontCountHits", "IsRefresh"],
}
# These official Top-N texts order only by an aggregate and leave tied rows
# unspecified. Spark may return different, equally valid tied rows after an
# otherwise equivalent rewrite, so their hashes are archival evidence rather
# than equality gates.  Row count remains a gate for every query.
HASH_TIE_EXEMPT_QUERIES = {
    25: "ORDER BY EventTime LIMIT 10 has no tie-breaker for equal timestamps",
    32: "ORDER BY COUNT(*) DESC LIMIT 10 has no tie-breaker",
    33: "ORDER BY COUNT(*) DESC LIMIT 10 has no tie-breaker",
    40: "ORDER BY PageViews DESC LIMIT 10 OFFSET 1000 has no tie-breaker",
}


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def source_order(source, table):
    import pyarrow.parquet as pq
    root = os.path.join(source, table)
    files = sorted(
        os.path.join(dirpath, name)
        for dirpath, _dirs, names in os.walk(root)
        for name in names
        if name.endswith(".parquet")
    )
    if not files:
        raise SystemExit(f"no parquet files under {root}")
    return list(pq.ParquetFile(files[0]).schema_arrow.names)


def full_order(head, baseline):
    if len(head) != len(set(head)):
        raise ValueError("protocol order has duplicate columns")
    missing = [c for c in head if c not in baseline]
    if missing:
        raise ValueError(f"protocol columns absent from source: {missing}")
    return head + [c for c in baseline if c not in set(head)]


def order_span(columns, order):
    pos = {c: i for i, c in enumerate(order)}
    selected = [pos[c] for c in columns]
    return max(selected) - min(selected)


def parent_actions(path):
    with open(path) as fh:
        doc = json.load(fh)
    actions = doc.get("actions")
    if not actions:
        raise ValueError(f"{path} has no actions")
    return actions


def canonical(action):
    return action.get("canonical", "")


def build_actions(parent, order, row_group_bytes=None):
    # The parent freezes codecs, encodings, page/index policy and target-file
    # size.  Replace only order and (for the small-RG tier) row-group size.
    actions = [
        a for a in parent
        if canonical(a) not in {
            "write.parquet.column-order",
            "write.parquet.row-group-size-bytes",
        }
    ]
    actions.append({
        "canonical": "write.parquet.column-order",
        "value": order,
        "table": "hits",
    })
    if row_group_bytes:
        actions.append({
            "canonical": "write.parquet.row-group-size-bytes",
            "value": row_group_bytes,
            "table": "hits",
        })
    return actions


def write_json(path, doc):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(doc, fh, indent=2)
        fh.write("\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--parent", required=True, help="frozen UC1 parent manifest")
    ap.add_argument("--source", required=True, help="official ClickBench SF1 root")
    ap.add_argument("--queries", required=True, help="official query JSON")
    ap.add_argument("--out", required=True, help="protocol and plan directory")
    ap.add_argument("--small-rg-mib", type=int, default=64)
    args = ap.parse_args()

    if args.small_rg_mib <= 0:
        raise SystemExit("--small-rg-mib must be positive")
    baseline = source_order(args.source, "hits")
    co_located = full_order(HOT_COLUMNS, baseline)
    separated_head = [
        part
        for pair in zip(HOT_COLUMNS, COLD_SEPARATORS)
        for part in pair
    ]
    separated = full_order(separated_head, baseline)
    parent = parent_actions(args.parent)
    small_rg_bytes = args.small_rg_mib * 1024 * 1024

    protocol = {
        "protocol_version": 1,
        "frozen_at": datetime.now(timezone.utc).isoformat(),
        "data": {
            "source": os.path.abspath(args.source),
            "table": "hits",
            "source_schema_order": baseline,
            "source_schema_order_sha256": hashlib.sha256(
                "\n".join(baseline).encode()
            ).hexdigest(),
        },
        "queries": {
            "file": os.path.abspath(args.queries),
            "sha256": sha256(args.queries),
            "primary_ids": PRIMARY_QUERIES,
            "selection_rule": PRIMARY_RULE,
            "secondary_ids": list(range(1, 44)),
            "primary_query_columns": PRIMARY_QUERY_COLUMNS,
            "hash_tie_exempt_queries": HASH_TIE_EXEMPT_QUERIES,
        },
        "orders": {
            "co-located": co_located,
            "separated": separated,
            "hot_columns": HOT_COLUMNS,
            "cold_separators": COLD_SEPARATORS,
        },
        "tiers": {
            "production-rg": {
                "row_group_size_bytes": None,
                "fixed_row_groups_per_file": 8,
            },
            "small-rg": {
                "row_group_size_bytes": small_rg_bytes,
                "fixed_row_groups_per_file": 8,
            },
        },
        "acceptance": {
            "rounds": 5,
            "max_cv": 0.05,
            "minimum_paired_median_speedup": 0.02,
            "minimum_get_reduction": 0.05,
            "minimum_span_or_predicted_range_reduction": 0.20,
            "require_all_five_primary_paired_signs": True,
            "require_zero_query_errors": True,
        },
    }
    protocol["order_span_by_query"] = {
        arm: {str(q): order_span(columns, order)
              for q, columns in PRIMARY_QUERY_COLUMNS.items()}
        for arm, order in (("co-located", co_located), ("separated", separated))
    }
    write_json(os.path.join(args.out, "protocol.json"), protocol)

    for tier, tier_doc in protocol["tiers"].items():
        for arm, order in (("co-located", co_located), ("separated", separated)):
            actions = build_actions(
                parent, order, row_group_bytes=tier_doc["row_group_size_bytes"]
            )
            rendered = render(actions, table="hits")
            problems = check_l0(rendered, writer="pyarrow")
            if problems:
                raise SystemExit(f"{tier}/{arm} violates L0: {problems}")
            plan = {
                "plan_id": f"{tier}-{arm}",
                "target": {"tables": ["hits"]},
                "actions": actions,
                "column_order_protocol": {
                    "protocol": "protocol.json",
                    "tier": tier,
                    "arm": arm,
                    "parent": os.path.abspath(args.parent),
                },
            }
            write_json(os.path.join(args.out, f"{tier}-{arm}.json"), plan)
            print(f"{tier:14s} {arm:12s} actions={len(actions):3d} L0=0")


if __name__ == "__main__":
    main()
