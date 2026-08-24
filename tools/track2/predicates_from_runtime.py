#!/usr/bin/env python3
"""Turn Spark event logs into the advisor's sort and partition key candidates.

This closes a loop that used to be open. `collect_semantic.py` has always
recorded what the engine actually pushed down, but nothing on the RECOMMEND
side read it: `analyze_layout.sort_from_predicates` ranked columns out of the
hand-transcribed `workload.QUERIES` catalogue, and the candidate grid came from
two hand-written constants (`SORT_GRID`, `PER_TABLE_SORT`) that never consulted
the ranking at all. The keys were therefore an assumption dressed as evidence.

Here the keys are derived instead:

    eventlog -> Scan parquet blocks -> (table, column, op, literal)
             -> weight by the measured wall time of the executions that pushed it
             -> sort candidates   (range predicates: ge/gt/le/lt)
             -> partition candidates (equality on a low-NDV column)

Weighting uses the execution's own `end_ms - start_ms` from the event log, so
no query-id mapping and no per_query.csv are needed -- the ranking is a pure
function of the runtime record. A column filtered by several scans of the same
execution is counted once for that execution, matching the old convention of
adding one `median_s` per (table, column) hit.

Why range and equality are split. A range predicate is what a *sort* key turns
into row-group skipping. An equality predicate on a low-NDV column prunes far
better as an identity *partition*, because Spark drops whole directories before
it opens a single footer. Ranking them together would let a 2-value column win
a sort key it cannot use.

Only identity partitions are proposed. `l_shipdate:year` and friends partition
on a derived column that no TPC-H predicate mentions, and Spark will not infer
`year(l_shipdate) = 1995` from a filter on `l_shipdate`, so those candidates
cost a rewrite and prune nothing (see write_layout.py and TRACK2_PROJECT.md).
Making them work needs hidden partitioning, i.e. Iceberg, not a writer flag.

Usage:
  python3 tools/track2/predicates_from_runtime.py \
      --eventlog docs/adaptive-range-reader/results/track2/e2_baseline/eventlogs \
      --column-stats .../e5_whatif/column_stats.json \
      --out .../e5_whatif/runtime_predicates.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import collect_semantic as cs  # noqa: E402

# An equality column is a partition candidate only below this many distinct
# values. 64 keeps o_orderstatus (2) and c_mktsegment (4) and rejects
# p_type (141); it is a starting cut left for ablation, like the L0 gates.
MAX_PARTITION_NDV = 64
# A composite sort key is only proposed when the second column is filtered
# alongside the first often enough to be worth the extra ordering constraint.
COMPOSITE_MIN_SHARE = 0.5
# Columns whose predicates are structural rather than selective. IsNotNull is
# emitted by Spark for every join and filter column and says nothing about
# layout; it would otherwise dominate every ranking.
IGNORED_OPS = {"isnotnull", "isnull"}


def load_executions(eventlog):
    """All SQL executions with at least one Scan parquet, across all logs."""
    logs = cs._list_eventlogs(eventlog)
    if not logs:
        raise SystemExit(f"no event logs under {eventlog}")
    executions = []
    for path in logs:
        parsed, _task_bytes = cs.parse_eventlog(path)
        for execution in parsed:
            execution["eventlog"] = path
            executions.append(execution)
    return logs, executions


def execution_seconds(execution):
    start, end = execution.get("start_ms"), execution.get("end_ms")
    if start is None or end is None or end < start:
        return 0.0
    return (end - start) / 1000.0


def build_catalogue(executions):
    """Per (table, column) runtime evidence, split into range and equality."""
    tables = defaultdict(lambda: {
        "n_scans": 0,
        "scan_seconds": 0.0,
        "columns": defaultdict(lambda: {
            "range": {"n_executions": 0, "weight_s": 0.0, "ops": defaultdict(int)},
            "equality": {"n_executions": 0, "weight_s": 0.0,
                         "ops": defaultdict(int), "literals": []},
        }),
    })
    # (table, col_a, col_b) -> seconds where both carry a range predicate, used
    # to decide whether a composite sort key is justified.
    cooccurrence = defaultdict(float)

    for execution in executions:
        seconds = execution_seconds(execution)
        # per execution, not per scan: one query filtering the same column in
        # two scans must not count its own runtime twice
        range_hit = defaultdict(set)
        eq_hit = defaultdict(set)
        for scan in execution.get("scans") or []:
            table = scan.get("table")
            if not table:
                continue
            tables[table]["n_scans"] += 1
            tables[table]["scan_seconds"] += seconds
            for pred in scan.get("predicates") or []:
                op = pred["op"]
                if op in IGNORED_OPS:
                    continue
                column = pred["column"]
                entry = tables[table]["columns"][column]
                if op in cs.RANGE_OPS:
                    entry["range"]["ops"][op] += 1
                    range_hit[table].add(column)
                elif op in {"eq", "in"}:
                    entry["equality"]["ops"][op] += 1
                    literal = pred.get("literal")
                    if literal is not None and literal not in entry["equality"]["literals"]:
                        entry["equality"]["literals"].append(literal)
                    eq_hit[table].add(column)

        for table, columns in range_hit.items():
            for column in columns:
                slot = tables[table]["columns"][column]["range"]
                slot["n_executions"] += 1
                slot["weight_s"] += seconds
            for a in sorted(columns):
                for b in sorted(columns):
                    if a < b:
                        cooccurrence[(table, a, b)] += seconds
        for table, columns in eq_hit.items():
            for column in columns:
                slot = tables[table]["columns"][column]["equality"]
                slot["n_executions"] += 1
                slot["weight_s"] += seconds

    return _undefault(tables), cooccurrence


def _undefault(tables):
    out = {}
    for table, rec in tables.items():
        columns = {}
        for column, entry in rec["columns"].items():
            columns[column] = {
                "range": {
                    "n_executions": entry["range"]["n_executions"],
                    "weight_s": round(entry["range"]["weight_s"], 1),
                    "ops": dict(entry["range"]["ops"]),
                },
                "equality": {
                    "n_executions": entry["equality"]["n_executions"],
                    "weight_s": round(entry["equality"]["weight_s"], 1),
                    "ops": dict(entry["equality"]["ops"]),
                    "literals": entry["equality"]["literals"][:8],
                },
            }
        out[table] = {
            "n_scans": rec["n_scans"],
            "scan_seconds": round(rec["scan_seconds"], 1),
            "columns": columns,
        }
    return out


def column_ndv(col_stats, table, column):
    if not col_stats:
        return None
    tables = col_stats.get("tables") or col_stats
    rec = (tables.get(table) or {}).get("columns") or {}
    entry = rec.get(column) or {}
    return entry.get("ndv")


def sort_key_candidates(catalogue, cooccurrence, top_k, col_stats=None):
    """Ranked single-column prefixes, plus one composite when it is earned."""
    out = {}
    for table, rec in catalogue.items():
        ranked = sorted(
            ((c, e["range"]) for c, e in rec["columns"].items()
             if e["range"]["weight_s"] > 0),
            key=lambda item: (-item[1]["weight_s"], item[0]))
        if not ranked:
            continue
        keys = [{
            "columns": [column],
            "weight_s": entry["weight_s"],
            "n_executions": entry["n_executions"],
            "ops": entry["ops"],
            # repartitionByRange cannot open more ranges than the key has
            # distinct values, so this bounds the file count of a global sort
            "ndv": column_ndv(col_stats, table, column),
        } for column, entry in ranked[:top_k]]
        if len(ranked) >= 2:
            first, second = ranked[0][0], ranked[1][0]
            pair = tuple(sorted((first, second)))
            shared = cooccurrence.get((table, pair[0], pair[1]), 0.0)
            leader = ranked[0][1]["weight_s"]
            # only worth ordering by both if the second column is filtered in
            # the same executions as the first; otherwise it is dead weight in
            # the sort and just costs a wider comparison
            if leader and shared / leader >= COMPOSITE_MIN_SHARE:
                keys.append({
                    "columns": [first, second],
                    "weight_s": round(shared, 1),
                    "n_executions": ranked[1][1]["n_executions"],
                    "ops": {},
                    "composite_share": round(shared / leader, 3),
                    # a composite key's range count is the product, so it is
                    # never the binding constraint if either part is high-NDV
                    "ndv": None,
                })
        out[table] = keys
    return out


def partition_key_candidates(catalogue, col_stats, max_ndv):
    """Filtered columns whose cardinality is small enough to be directories.

    Both predicate kinds count. Spark's partition filter runs on the directory
    values, so an identity partition prunes `EventDate BETWEEN a AND b` just as
    it prunes `o_orderstatus = 'F'` -- unlike a *sort* key, which needs an
    ordering to skip row groups. NDV is the whole gate: it is the directory
    count, and a column with too many values shreds the table into files far
    below the target size.
    """
    out = {}
    for table, rec in catalogue.items():
        keys = []
        for column, entry in rec["columns"].items():
            weight = entry["equality"]["weight_s"] + entry["range"]["weight_s"]
            if weight <= 0:
                continue
            executions = max(entry["equality"]["n_executions"],
                             entry["range"]["n_executions"])
            driver = "equality" if entry["equality"]["weight_s"] >= entry["range"]["weight_s"] \
                else "range"
            key = {"column": column, "weight_s": round(weight, 1),
                   "n_executions": executions, "driver": driver,
                   "ndv": column_ndv(col_stats, table, column)}
            if key["ndv"] is None:
                key["rejected"] = "no ndv in column stats"
            elif key["ndv"] > max_ndv:
                key["rejected"] = f"ndv {key['ndv']} > max {max_ndv}"
            else:
                key["n_partitions"] = key["ndv"]
            keys.append(key)
        keys.sort(key=lambda k: (bool(k.get("rejected")), -k["weight_s"], k["column"]))
        if keys:
            out[table] = keys
    return out


def build(eventlog, col_stats=None, top_k=2, max_ndv=MAX_PARTITION_NDV):
    logs, executions = load_executions(eventlog)
    catalogue, cooccurrence = build_catalogue(executions)
    with_scan = [e for e in executions if e.get("scans")]
    return {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 3.1/3.2 -> 5.1",
        "eventlog": eventlog,
        "n_eventlogs": len(logs),
        "n_executions": len(executions),
        "n_executions_with_scan": len(with_scan),
        "weighting": "execution end_ms - start_ms from the event log",
        "max_partition_ndv": max_ndv,
        "tables": catalogue,
        "sort_keys": sort_key_candidates(catalogue, cooccurrence, top_k, col_stats),
        "partition_keys": partition_key_candidates(catalogue, col_stats, max_ndv),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--eventlog", required=True, help="event log file or directory")
    ap.add_argument("--column-stats", default=None,
                    help="column_stats.json, for the NDV gate on partition keys")
    ap.add_argument("--top-k", type=int, default=2,
                    help="single-column sort prefixes to propose per table")
    ap.add_argument("--max-partition-ndv", type=int, default=MAX_PARTITION_NDV)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    col_stats = None
    if args.column_stats and os.path.exists(args.column_stats):
        with open(args.column_stats) as fh:
            col_stats = json.load(fh)

    result = build(args.eventlog, col_stats, args.top_k, args.max_partition_ndv)

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(result, fh, indent=2)

    print("# predicates from runtime")
    print(f"  event logs        {result['n_eventlogs']}")
    print(f"  executions        {result['n_executions']} "
          f"({result['n_executions_with_scan']} with scan)")
    print(f"  tables            {len(result['tables'])}")
    print("  sort keys (range predicates, weighted by execution wall time)")
    for table, keys in sorted(result["sort_keys"].items(),
                              key=lambda kv: -kv[1][0]["weight_s"]):
        for key in keys:
            ops = ",".join(f"{o}x{n}" for o, n in sorted(key["ops"].items()))
            print(f"    {table:>10} {'+'.join(key['columns']):<28} "
                  f"{key['weight_s']:>9.1f}s  n={key['n_executions']:<3} {ops}")
    print("  partition keys (identity only; NDV is the directory count)")
    for table, keys in sorted(result["partition_keys"].items()):
        for key in keys:
            note = key.get("rejected") or f"{key['n_partitions']} partitions"
            print(f"    {table:>10} {key['column']:<28} "
                  f"{key['weight_s']:>9.1f}s  {key['driver']:<8} "
                  f"ndv={key['ndv']}  {note}")
    if args.out:
        print(f"  out               {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
