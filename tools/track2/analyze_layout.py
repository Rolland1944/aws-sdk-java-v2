#!/usr/bin/env python3
"""RECOMMEND side of the Track 2 advisor (DB2 Design Advisor analogue).

Reads E2 telemetry plus the two measured snapshots and emits:
  1. a layout-problem report (request-size buckets, per-file overhead, co-access,
     cold columns -- cold columns are evidence, never a drop recommendation)
  2. a compressed-workload schedule (DB2 6.2; search only, the gate uses all)
  3. the candidate grid as LayoutCandidate JSON files

Every axis of that grid now comes from measurement rather than a literal:

    sort keys, partition keys  <- predicates_from_runtime (pushed-down filters)
    file counts, row groups    <- adaptive_physical_options (measured geometry)
    which tables are in scope  <- measured size vs the policy threshold

There is deliberately no hand-written fallback. A run with no event log used to
fall back to `workload.PER_TABLE_SORT`, which meant the advisor's headline
result -- that it picks l_shipdate and o_orderdate -- was reading back the two
keys a human had typed into that constant. Removing the fallback is what makes
the choice evidence.

Usage:
  python3 tools/track2/analyze_layout.py --emit-per-table \
      --dataset-snapshot .../dataset_snapshot.json \
      --workload-snapshot .../workload_snapshot.json \
      --runtime-predicates .../runtime_predicates.json \
      --out docs/adaptive-range-reader/results/track2/e5_whatif
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations, product

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import adaptive_physical_options  # noqa: E402
import advisor_catalog  # noqa: E402
import advisor_policy  # noqa: E402
from sysconst import BUCKETS, load_records, _bucket  # noqa: E402

# Installed by main() or by whatif.bind_catalog.
catalog = None


def io_problem_report(records, n_runs):
    tiny = data = head = 0
    bytes_tiny = bytes_data = 0
    files = set()
    by_bucket = Counter()
    for rec in records:
        path = rec.get("audit_path") or rec.get("path") or ""
        if path:
            files.add(path.split("?")[0])
        method = rec.get("method")
        length = rec.get("range_length") or 0
        if method == "HEAD":
            head += 1
        elif method == "GET" and length:
            by_bucket[_bucket(length)] += 1
            if length < 65536:
                tiny += 1
                bytes_tiny += length
            else:
                data += 1
                bytes_data += length
    scale = 1.0 / max(n_runs, 1)
    n_files = len({p for p in files if p.endswith(".parquet")}) or len(files)
    return {
        "per_run": {
            "head": round(head * scale),
            "ranged_get_tiny": round(tiny * scale),
            "ranged_get_data": round(data * scale),
            "ranged_get_total": round((tiny + data) * scale),
            "bytes_tiny": round(bytes_tiny * scale),
            "bytes_data": round(bytes_data * scale),
            "bytes_data_gib": round(bytes_data * scale / 2 ** 30, 2),
        },
        "buckets_per_run": {k: round(v * scale) for k, v in by_bucket.items()},
        "unique_parquet_paths": n_files,
        "head_per_file_per_run": round(head * scale / max(n_files, 1), 2),
        "tiny_get_fraction": tiny / max(tiny + data, 1),
        "note": (
            "81%+ of GETs are RTT-dominated (<1 MiB) in the measured regime. "
            "Per-file HEAD+footer opens are the layout-sensitive part of that."
        ),
    }


def coaccess_and_cold(medians):
    pair_w = Counter()
    col_w = Counter()
    used = defaultdict(set)
    for q, scans in catalog.QUERIES.items():
        w = medians.get(q, 1.0)
        for scan in scans:
            cols = scan["columns"]
            table = scan["table"]
            for c in cols:
                col_w[(table, c)] += w
                used[table].add(c)
            for a, b in combinations(sorted(set(cols)), 2):
                pair_w[(table, a, b)] += w
    cold = []
    for table, cols in catalog.ALL_COLUMNS.items():
        for c in cols:
            if c not in used.get(table, ()):
                cold.append({"table": table, "column": c,
                             "action": "report_only_do_not_drop"})
    top_pairs = [
        {"table": t, "a": a, "b": b, "weight_s": round(w, 1)}
        for (t, a, b), w in pair_w.most_common(20)
    ]
    return {
        "coaccess_top20": top_pairs,
        "column_weight_s": [
            {"table": t, "column": c, "weight_s": round(w, 1)}
            for (t, c), w in col_w.most_common()
        ],
        "cold_columns": cold,
        "cold_policy": "DB2 §5.5 / TRACK2_PLAN §1.3: never recommend deletion",
    }


def sort_from_predicates(medians, runtime=None):
    """Sort-key evidence: range-predicate columns weighted by query time.

    Two views of the same event logs. `runtime` weights each column by the wall
    time of the executions that pushed a range filter on it, needing no query
    numbering. The fallback weights by measured per-query median instead, which
    needs the query ids the workload snapshot resolved but lines the ranking up
    with the numbers in the acceptance report.
    """
    if runtime and runtime.get("sort_keys"):
        ranked = []
        for table, keys in runtime["sort_keys"].items():
            for key in keys:
                if len(key["columns"]) != 1:
                    continue
                ranked.append({"table": table, "column": key["columns"][0],
                               "weight_s": key["weight_s"],
                               "n_executions": key["n_executions"],
                               "ops": key["ops"], "source": "runtime_pushdown"})
        ranked.sort(key=lambda r: -r["weight_s"])
        return ranked
    w = Counter()
    for q, scans in catalog.QUERIES.items():
        for scan in scans:
            for pred in scan["predicates"]:
                if pred["op"] in {"ge", "gt", "le", "lt", "between"}:
                    w[(scan["table"], pred["column"])] += medians.get(q, 1.0)
    return [{"table": t, "column": c, "weight_s": round(wt, 1),
             "source": "workload_snapshot"}
            for (t, c), wt in w.most_common()]


def make_candidate(partition, file_label, file_bytes, rg_label, rg_bytes,
                   sort_label, sort_cols):
    actions = []
    if partition and partition != "none":
        actions.append({"canonical": "partition.spec", "value": partition})
    if file_bytes:
        actions.append({"canonical": "write.target-file-size-bytes",
                        "value": file_bytes})
    if rg_bytes:
        actions.append({"canonical": "write.parquet.row-group-size-bytes",
                        "value": rg_bytes})
    if sort_cols:
        actions.append({"canonical": "sort.columns", "value": sort_cols})
    cid = f"p-{partition}_f-{file_label}_rg-{rg_label}_s-{sort_label}"
    return {"candidate_id": cid, "actions": actions,
            "partition": partition, "file_label": file_label,
            "file_bytes": file_bytes, "rg_label": rg_label, "rg_bytes": rg_bytes,
            "sort_label": sort_label, "sort_columns": sort_cols}


def generate_grid(options=None):
    """The old global grid: one file size / row group / sort for every table.

    Kept as the `--grid global` ablation, because E8's headline finding is that
    it loses -- a target file size chosen for a 21 GiB fact table starves a
    4 GiB one. It is no longer a hand-written product: the axes are the largest
    table's adaptive options and the runtime sort keys, so the ablation
    compares two ways of *using* the same evidence rather than a measured grid
    against a typed one.
    """
    if options is None:
        raise SystemExit("generate_grid needs table options; pass "
                         "--runtime-predicates so the axes come from evidence")
    primary = catalog.largest_table()
    opt = options.get(primary) or {}
    files = opt.get("file") or [("baseline", None)]
    rgs = catalog.rg_options(primary)
    sorts = opt.get("sort") or [("none", [], None)]
    cands = []
    for flab, fbytes in files:
        for rlab, rbytes in rgs:
            for sort_option in sorts:
                slab, scols = sort_option[0], sort_option[1]
                cands.append(make_candidate(
                    "none", flab, fbytes, rlab, rbytes, slab, scols))
    # explicit baseline: empty actions (contract 2.3)
    cands.append({
        "candidate_id": "baseline",
        "actions": [],
        "partition": "none",
        "file_label": "spark_default",
        "file_bytes": None,
        "rg_label": "spark_default",
        "rg_bytes": None,
        "sort_label": "none",
        "sort_columns": [],
    })
    return cands


def _table_spec(file_label, file_bytes, sort_label, sort_cols,
                partition_label="none", partition_column=None, partition_n=None,
                sort_ndv=None):
    spec = {
        "file_label": file_label,
        "file_bytes": file_bytes,
        "sort_label": sort_label,
        "sort_columns": list(sort_cols),
        # distinct values of the sort prefix: repartitionByRange cannot emit
        # more files than this, whatever target file size asks for
        "sort_ndv": sort_ndv,
        "partition_label": partition_label,
        "partition": partition_column or "none",
        "partition_n": partition_n,
    }
    if file_bytes:
        # Asking for a target file size without also pinning the row group
        # lets parquet-mr keep its own default, which on a re-sorted table is
        # what produced the oversized row groups the M2 canary timed out on.
        spec["rg_label"] = "engine_default"
        spec["rg_bytes"] = catalog.BASELINE_RG_BYTES
    else:
        spec["rg_label"] = "baseline"
        spec["rg_bytes"] = None
    return spec


def key_label(columns):
    """Short candidate-id label for a key: `l_shipdate` -> `shipdate`."""
    parts = []
    for column in columns:
        head, sep, tail = column.partition("_")
        parts.append((tail if sep and tail and len(head) <= 2 else column).lower())
    return "_".join(parts) or "none"


def large_tables():
    """Tables the per-table search may touch, largest first.

    Measured size is the only criterion, so this is the same
    lineitem/orders/partsupp set the old hand-written product enumerated,
    without naming them -- and on ClickBench it is `hits` with no code change.
    """
    return catalog.large_tables()


def runtime_table_options(runtime, sort_top_k=2, partition_top_k=2):
    """Sort and partition axes derived from runtime_predicates.json.

    Every key here was pushed down by the engine during E2. Nothing is filtered
    for plausibility: a column the workload filters on is proposed, and L0/L1
    decide. That is the DB2 discipline -- RECOMMEND enumerates, EVALUATE
    rejects -- and it is what the hand-written SORT_GRID short-circuited.
    """
    sort_keys = (runtime or {}).get("sort_keys") or {}
    partition_keys = (runtime or {}).get("partition_keys") or {}
    options = {}
    for table in large_tables():
        keys = sort_keys.get(table) or []
        singles = [k for k in keys if len(k["columns"]) == 1][:sort_top_k]
        composites = [k for k in keys if len(k["columns"]) > 1]
        sorts = [("none", [], None)]
        for key in singles + composites:
            sorts.append((key_label(key["columns"]), list(key["columns"]),
                          key.get("ndv")))

        partitions = [("none", None, None)]
        eligible = [k for k in (partition_keys.get(table) or [])
                    if not k.get("rejected")]
        for key in eligible[:partition_top_k]:
            partitions.append((key_label([key["column"]]), key["column"],
                               key["n_partitions"]))

        options[table] = {"file": catalog.file_options(table),
                          "sort": sorts, "partition": partitions}
    return options


def make_per_table_candidate(picks):
    """One workload layout: independent file size / sort / partition per table.

    `picks` maps table -> (file_option, sort_option, partition_option).
    """
    tables = {}
    actions = []
    axes = []
    for table in picks:
        file_opt, sort_opt, part_opt = picks[table]
        spec = _table_spec(file_opt[0], file_opt[1], sort_opt[0], sort_opt[1],
                           part_opt[0], part_opt[1], part_opt[2],
                           sort_ndv=sort_opt[2])
        tables[table] = spec
        if spec["file_bytes"]:
            actions.append({"canonical": "write.target-file-size-bytes",
                            "value": spec["file_bytes"], "table": table})
            actions.append({"canonical": "write.parquet.row-group-size-bytes",
                            "value": spec["rg_bytes"], "table": table})
        if spec["sort_columns"]:
            actions.append({"canonical": "sort.columns",
                            "value": spec["sort_columns"], "table": table})
        if spec["partition"] != "none":
            # identity only; write_layout partitions on the column itself
            actions.append({"canonical": "partition.spec",
                            "value": spec["partition"], "table": table})
        axes.extend([f"{table}.sort", f"{table}.file", f"{table}.partition"])

    parts = []
    for table, spec in tables.items():
        piece = f"{table}-{spec['file_label']}-{spec['sort_label']}"
        if spec["partition"] != "none":
            piece += f"-p{spec['partition_label']}"
        parts.append(piece)

    candidate = {
        "candidate_id": "_".join(parts),
        "scope": "per_table",
        "actions": actions,
        "tables": tables,
        "axes": axes,
        "partition": "none",
        "file_label": None,
        "file_bytes": None,
        "rg_label": None,
        "rg_bytes": None,
        "sort_label": None,
        "sort_columns": [],
    }
    for table, spec in tables.items():
        candidate[f"{table}.sort"] = spec["sort_label"]
        candidate[f"{table}.file"] = spec["file_label"]
        candidate[f"{table}.partition"] = spec["partition_label"]
    return candidate


def per_table_baseline(tables=None):
    tables = tables if tables is not None else large_tables()
    candidate = {
        "candidate_id": "baseline",
        "scope": "per_table",
        "actions": [],
        "tables": {t: _table_spec("baseline", None, "none", []) for t in tables},
        "axes": [f"{t}.{axis}" for t in tables
                 for axis in ("sort", "file", "partition")],
        "partition": "none",
        "file_label": "spark_default",
        "file_bytes": None,
        "rg_label": "spark_default",
        "rg_bytes": None,
        "sort_label": "none",
        "sort_columns": [],
    }
    for table in tables:
        candidate[f"{table}.sort"] = "none"
        candidate[f"{table}.file"] = "baseline"
        candidate[f"{table}.partition"] = "none"
    return candidate


def generate_per_table_grid(options=None):
    """Cartesian product over the per-table axes.

    Table set and axes both come from data now: `large_tables()` from measured
    geometry, sort/partition keys from the runtime predicate catalogue.
    """
    if options is None:
        raise SystemExit(
            "no table options: pass --runtime-predicates. There is no hand-"
            "written grid to fall back on any more, on purpose.")
    tables = list(options)
    per_table = []
    for table in tables:
        opt = options[table]
        per_table.append([(f, s, p) for f in opt["file"]
                          for s in opt["sort"] for p in opt["partition"]])
    cands = []
    for combo in product(*per_table):
        if all(f[0] == "baseline" and s[0] == "none" and p[0] == "none"
               for f, s, p in combo):
            continue
        cands.append(make_per_table_candidate(dict(zip(tables, combo))))
    cands.append(per_table_baseline(tables))
    return cands


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--io", nargs="+", required=False)
    ap.add_argument("--per-query", required=False)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--out", required=True)
    advisor_catalog.add_arguments(ap)
    ap.add_argument("--emit-per-table", action="store_true",
                    help="write candidates_per_table.json (no IO traces needed)")
    ap.add_argument("--runtime-predicates", required=True,
                    help="predicates_from_runtime.py output; the sort and "
                         "partition axes are derived from it")
    ap.add_argument("--sort-top-k", type=int, default=2,
                    help="single-column sort prefixes proposed per table")
    ap.add_argument("--partition-top-k", type=int, default=2,
                    help="identity partition columns proposed per table")
    args = ap.parse_args()

    global catalog
    catalog = advisor_catalog.from_args(args)

    with open(args.runtime_predicates) as fh:
        runtime = json.load(fh)
    options = runtime_table_options(runtime, args.sort_top_k, args.partition_top_k)

    if args.emit_per_table and not args.io:
        os.makedirs(args.out, exist_ok=True)
        ptable = generate_per_table_grid(options)
        path = os.path.join(args.out, "candidates_per_table.json")
        with open(path, "w") as fh:
            json.dump(ptable, fh, indent=2)
        cand_dir = os.path.join(args.out, "candidates_per_table")
        os.makedirs(cand_dir, exist_ok=True)
        for c in ptable:
            with open(os.path.join(cand_dir, c["candidate_id"] + ".json"), "w") as fh:
                json.dump({"candidate_id": c["candidate_id"],
                           "scope": c.get("scope"),
                           "actions": c["actions"],
                           "tables": c.get("tables")}, fh, indent=2)
        with open(os.path.join(args.out, "action_space.json"), "w") as fh:
            json.dump({"provenance": catalog.provenance(),
                       "physical": adaptive_physical_options.describe(
                           catalog.dataset, catalog.parallelism),
                       "sort_and_partition": {
                           t: {"sort": [s[0] for s in o["sort"]],
                               "partition": [p[0] for p in o["partition"]]}
                           for t, o in options.items()}}, fh, indent=2)
        print("# analyze_layout --emit-per-table")
        for table, opt in options.items():
            print(f"  {table:12s} file={[f[0] for f in opt['file']]} "
                  f"sort={[s[0] for s in opt['sort']]} "
                  f"partition={[p[0] for p in opt['partition']]}")
        print(f"  candidates  {len(ptable)} -> {path}")
        return 0

    if not args.io or not args.per_query:
        raise SystemExit("--io and --per-query are required unless --emit-per-table")

    records, _files = load_records(args.io)
    medians = advisor_policy.load_per_query_medians(args.per_query)
    problems = io_problem_report(records, args.runs)
    access = coaccess_and_cold(medians)
    sort_ev = sort_from_predicates(medians, runtime)

    levels = {}
    for name, x in (("low", 0.60), ("medium", 0.25), ("high", 0.05)):
        kept, frac = advisor_policy.compress_workload(medians, x)
        levels[name] = {"x": x, "queries": kept, "kept_cost_frac": round(frac, 4),
                        "n": len(kept)}

    candidates = generate_grid(options)
    ptable = generate_per_table_grid(options)
    os.makedirs(args.out, exist_ok=True)
    cand_dir = os.path.join(args.out, "candidates")
    os.makedirs(cand_dir, exist_ok=True)
    index = []
    for c in candidates:
        path = os.path.join(cand_dir, c["candidate_id"] + ".json")
        with open(path, "w") as fh:
            json.dump({"candidate_id": c["candidate_id"],
                       "actions": c["actions"]}, fh, indent=2)
        index.append(c)

    report = {
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 5.1 / 7 E5",
        "problems": problems,
        "coaccess": access,
        "provenance": catalog.provenance(),
        "sort_key_evidence": sort_ev,
        "key_source": "runtime_pushdown",
        "runtime_predicates": args.runtime_predicates,
        "physical_options": adaptive_physical_options.describe(
            catalog.dataset, catalog.parallelism),
        "partition_key_candidates": (runtime or {}).get("partition_keys"),
        "workload_compression": levels,
        "n_candidates": len(candidates),
        "candidate_index": os.path.join(args.out, "candidates.json"),
    }
    with open(os.path.join(args.out, "analyze_report.json"), "w") as fh:
        json.dump(report, fh, indent=2)
    with open(os.path.join(args.out, "candidates.json"), "w") as fh:
        json.dump(index, fh, indent=2)
    with open(os.path.join(args.out, "candidates_per_table.json"), "w") as fh:
        json.dump(ptable, fh, indent=2)

    print("# analyze_layout")
    print(f"  GET/run          {problems['per_run']['ranged_get_total']}")
    print(f"  tiny GET frac    {problems['tiny_get_fraction']*100:.1f}%")
    print(f"  HEAD/file/run    {problems['head_per_file_per_run']}")
    print(f"  cold columns     {len(access['cold_columns'])} (report only)")
    print(f"  sort evidence    {sort_ev[:3]}")
    print(f"  compression low  {levels['low']}")
    print(f"  candidates       {len(candidates)} -> {cand_dir}")
    print(f"  per-table        {len(ptable)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
