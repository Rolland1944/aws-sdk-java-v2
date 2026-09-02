#!/usr/bin/env python3
"""Workload snapshot: the scan catalogue L1 prices, read from event logs.

`workload.QUERIES` was 150 lines of hand-transcribed scans -- which columns
each TPC-H query projects and which predicates it pushes down. Two problems
with that, and only the second one is obvious.

The obvious one: it is labour that does not transfer. A new benchmark means
someone reads 43 more SQL texts and types out the projections, and the
ClickBench port skipped it entirely (`clickbench_workload.QUERIES` was
generated, then trusted).

The real one: it is the *wrong source*. The transcription records what the SQL
text says, but L1 prices what Spark actually reads, and those differ. Spark
prunes projections through joins, pushes only some predicates into the source,
splits a self-join into separate scans, and rewrites filters. Every one of
those gaps is a silent modelling error that a hand catalogue cannot expose,
because the hand catalogue is not measuring anything. The event log is.

    eventlog -> SQLExecutionStart.physicalPlanDescription
             -> Scan parquet blocks: ReadSchema + PushedFilters + Location
             -> per query: scans[] -> L1 prices these directly

Query identity. The event log has no query number, so this module resolves it
two ways, preferring the first:

  * `track2:q<N>` in the execution description. run_benchmark.py sets this
    per query, so any new run is self-describing.
  * positional. Queries run serially, so within one log the k-th scan-bearing
    execution is the k-th query of the run. A short log is a truncated run and
    covers a prefix. This is what the archived E2 logs need, and it is checked:
    every full-length run must agree with every other on the scan shape of each
    query, and a disagreement is reported rather than averaged away.

Usage:
  python3 tools/track2/workload_snapshot.py \
      --eventlog docs/.../e2_baseline/eventlogs \
      --per-query docs/.../e2_baseline/per_query.csv \
      --out docs/.../workload_snapshot.json

  # cross-check a run against the retired hand catalogue
  python3 tools/track2/workload_snapshot.py --eventlog ... --compare tpch
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import collect_semantic as cs  # noqa: E402
import advisor_policy  # noqa: E402

QUERY_TAG_RE = re.compile(r"track2:q(\d+)\b")
# Predicates that carry no selectivity information. Kept out of the snapshot so
# a scan's predicate list means "things that can prune".
INERT_OPS = {"isnotnull", "unknown"}


def _literal(op, raw):
    """PushedFilters literal -> a value L1 can compare against a CDF.

    Left as text on purpose: virtual_footer._coerce casts against a sample of
    the column's own CDF, which knows the type. Guessing here would mean
    guessing twice. `In` is the exception, because its selectivity is
    len(values)/ndv and that needs the list.
    """
    if raw is None:
        return None
    if op in ("in", "notin"):
        body = raw.strip()
        if body.startswith("[") and body.endswith("]"):
            body = body[1:-1]
        return [v.strip() for v in cs.split_top_level(body) if v.strip()]
    return raw.strip()


def scan_from_fragment(frag):
    """A collect_semantic scan fragment -> the shape virtual_footer reads."""
    predicates = []
    for leaf in frag.get("predicates") or []:
        op = leaf["op"]
        if op in INERT_OPS:
            continue
        predicates.append({
            "column": leaf["column"],
            "op": op,
            "value": _literal(op, leaf.get("literal")),
        })
    return {
        "table": frag.get("table"),
        "columns": list(frag.get("read_columns") or []),
        "predicates": predicates,
    }


def _scan_shape(scans):
    """Comparable fingerprint of a query's scans, for cross-run agreement."""
    return sorted((s["table"], len(s["columns"]),
                   tuple(sorted((p["column"], p["op"]) for p in s["predicates"])))
                  for s in scans)


def load_runs(eventlog):
    """[(log path, [executions with scans, in start order])] per event log."""
    runs = []
    for path in cs._list_eventlogs(eventlog):
        executions, _task_bytes = cs.parse_eventlog(path)
        executions.sort(key=lambda e: (e.get("start_ms") or 0))
        with_scan = [e for e in executions if e.get("scans")]
        if with_scan:
            runs.append((path, with_scan))
    return runs


def assign_query_ids(runs, query_ids):
    """[(query id, execution)] across all runs, plus how the id was resolved."""
    tagged = []
    for _path, executions in runs:
        for ex in executions:
            m = QUERY_TAG_RE.search(ex.get("description") or "")
            if m:
                tagged.append((int(m.group(1)), ex))
    if tagged:
        return tagged, "description_tag"

    assigned = []
    for _path, executions in runs:
        if len(executions) > len(query_ids):
            raise SystemExit(
                f"event log has {len(executions)} scan-bearing executions but "
                f"only {len(query_ids)} query ids were supplied; positional "
                f"alignment is unsafe. Re-run the benchmark so run_benchmark.py "
                f"tags executions with track2:q<N>, or pass --query-ids.")
        for i, ex in enumerate(executions):
            assigned.append((query_ids[i], ex))
    return assigned, "positional"


def build(eventlog, query_ids=None, per_query=None):
    runs = load_runs(eventlog)
    if not runs:
        raise SystemExit(f"no scan-bearing executions under {eventlog}")

    medians = advisor_policy.load_per_query_medians(per_query) if per_query else {}
    if query_ids is None:
        query_ids = (sorted(medians) if medians
                     else list(range(1, max(len(e) for _p, e in runs) + 1)))

    assigned, method = assign_query_ids(runs, query_ids)

    observations = {}
    for qid, ex in assigned:
        scans = [scan_from_fragment(f) for f in ex["scans"]]
        scans = [s for s in scans if s["table"]]
        if scans:
            observations.setdefault(qid, []).append(scans)

    queries, disagreements = {}, []
    for qid, seen in sorted(observations.items()):
        shapes = {json.dumps(_scan_shape(s)) for s in seen}
        if len(shapes) > 1:
            disagreements.append({"query": qid, "n_runs": len(seen),
                                  "n_distinct_shapes": len(shapes)})
        # Widest observation wins: a run that lost a scan to AQE or a cached
        # exchange under-reports, and under-reporting is what makes L1 too
        # optimistic.
        queries[qid] = max(seen, key=lambda s: (len(s), sum(len(x["columns"]) for x in s)))

    tables = sorted({s["table"] for scans in queries.values() for s in scans})
    n_pred = sum(len(s["predicates"]) for scans in queries.values() for s in scans)
    return {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 3.1/3.2 -> 5.1",
        "eventlog": eventlog,
        "n_eventlogs": len(runs),
        "query_id_source": method,
        "runs": [{"eventlog": p, "n_executions_with_scan": len(e)} for p, e in runs],
        "n_queries": len(queries),
        "n_scans": sum(len(s) for s in queries.values()),
        "n_predicates": n_pred,
        "tables": tables,
        "disagreements": disagreements,
        "measured_median_s": {str(q): v for q, v in medians.items()},
        "queries": {str(q): scans for q, scans in sorted(queries.items())},
    }


class WorkloadSnapshot:
    """Observed scan catalogue, duck-typed against the old workload module."""

    def __init__(self, doc):
        self.doc = doc
        self.QUERIES = {int(q): scans for q, scans in doc["queries"].items()}
        self.measured_median_s = {int(q): v for q, v
                                  in (doc.get("measured_median_s") or {}).items()}
        self.query_id_source = doc.get("query_id_source")

    def tables(self):
        return sorted({s["table"] for scans in self.QUERIES.values() for s in scans})


def load(path):
    with open(path) as fh:
        return WorkloadSnapshot(json.load(fh))


def compare_to_hand_catalogue(doc, dataset):
    """Agreement with the retired hand catalogue, per query.

    Only meaningful while both exist. It is the evidence that positional
    alignment put the right scans under the right query number, and it is also
    where the pushdown gaps show up: a query whose hand catalogue lists a
    predicate the plan never pushed was being priced against a filter Spark
    does not apply.
    """
    if dataset == "clickbench":
        import hand_catalog_clickbench as hand
    else:
        import hand_catalog_tpch as hand
    rows = []
    for qid, scans in sorted(doc["queries"].items(), key=lambda kv: int(kv[0])):
        ref = hand.QUERIES.get(int(qid)) or []
        obs_t = sorted(s["table"] for s in scans)
        ref_t = sorted(s["table"] for s in ref)
        obs_p = {(s["table"], p["column"]) for s in scans for p in s["predicates"]}
        ref_p = {(s["table"], p["column"]) for s in ref for p in s["predicates"]}
        rows.append({
            "query": int(qid),
            "tables_match": obs_t == ref_t,
            "observed_tables": obs_t,
            "hand_tables": ref_t,
            "predicates_only_in_plan": sorted(f"{t}.{c}" for t, c in obs_p - ref_p),
            "predicates_only_by_hand": sorted(f"{t}.{c}" for t, c in ref_p - obs_p),
        })
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--eventlog", required=True, help="event log file or directory")
    ap.add_argument("--per-query", default=None,
                    help="per_query.csv; supplies query ids and measured medians")
    ap.add_argument("--query-ids", nargs="*", type=int, default=None,
                    help="explicit query order for positional alignment")
    ap.add_argument("--compare", choices=("tpch", "clickbench"), default=None,
                    help="cross-check against the retired hand catalogue")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    doc = build(args.eventlog, args.query_ids, args.per_query)
    if args.compare:
        doc["hand_catalogue_comparison"] = compare_to_hand_catalogue(doc, args.compare)

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(doc, fh, indent=2)

    print(f"# workload snapshot: {args.eventlog}")
    print(f"  event logs     {doc['n_eventlogs']}  "
          f"({', '.join(str(r['n_executions_with_scan']) for r in doc['runs'])} queries each)")
    print(f"  query ids from {doc['query_id_source']}")
    print(f"  queries        {doc['n_queries']}")
    print(f"  scans          {doc['n_scans']}  predicates {doc['n_predicates']}")
    print(f"  tables         {', '.join(doc['tables'])}")
    if doc["disagreements"]:
        print(f"  DISAGREEMENT   {doc['disagreements']}")
    if args.compare:
        rows = doc["hand_catalogue_comparison"]
        bad = [r for r in rows if not r["tables_match"]]
        print(f"  vs hand        {len(rows) - len(bad)}/{len(rows)} queries "
              f"scan the same tables")
        for r in rows:
            if r["predicates_only_by_hand"] or r["predicates_only_in_plan"]:
                print(f"    Q{r['query']:<3} plan-only={r['predicates_only_in_plan']} "
                      f"hand-only={r['predicates_only_by_hand']}")
    if args.out:
        print(f"  out            {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
