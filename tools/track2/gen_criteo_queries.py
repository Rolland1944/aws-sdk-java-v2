#!/usr/bin/env python3
"""Generate the frozen Criteo-CTR-FE-v1 workload from criteo_profile.json.

The generator is deliberately blind: its only inputs are the dataset schema and
the one-shot profile. It never opens a benchmark report, never sees a layout
candidate, and never looks at a wall-clock number. That is what makes the
resulting 24 queries a workload rather than a set of queries tuned to whatever
the candidate happens to be good at.

Everything variable is frozen as a constant here:

  * day split          from the profile's per-day row counts
  * integer buckets    from measured quartiles, not round numbers
  * clip caps          from measured p99
  * categorical reps   by (cardinality, column index), never by hand
  * hash / null token / bucket count   declared once, used everywhere
  * Top-K ordering     count DESC, value ASC  (no LIMIT without ORDER BY)
  * epochs             the same SQL text repeated, on purpose

Outputs:
  tools/track2/criteo_queries.json            {"_comment":..., "1":..., ... "24":...}
  tools/track2/criteo_workload_manifest.json  everything needed to reproduce it

Usage:
  python3 tools/track2/gen_criteo_queries.py \
      --profile /mnt/ssd-cache/criteo_local/work/criteo_profile.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

WORKLOAD_ID = "Criteo-CTR-FE-v1"
GENERATOR_VERSION = "1"
INT_COLS = [f"integer_feature_{i}" for i in range(1, 14)]
CAT_COLS = [f"categorical_feature_{i}" for i in range(1, 27)]
NULL_TOKEN = "__NULL__"
HASH_FN = "spark_murmur3_hash"          # Spark SQL built-in hash()
BUCKETS = 4096                          # Q13 / Q14 / Q15
BUCKETS_JOINT = 16384                   # Q17
RESULT_ROW_CAP = 1000                   # every query returns <= this many rows
TOPK = 20
N_INT_REPS = 3                          # Q06 / Q15 integer representatives


def day_literal(day):
    return f"DATE '{day}'"


def day_filter(days):
    """Inclusive filter over a contiguous day list."""
    if len(days) == 1:
        return f"day = {day_literal(days[0])}"
    return f"day BETWEEN {day_literal(days[0])} AND {day_literal(days[-1])}"


def split_case(split):
    """Train / validation / test labelling, derived from the frozen day lists."""
    train, val, test = split["train"], split["validation"], split["test"]
    parts = []
    if train:
        if len(train) == 1:
            cond = f"day = {day_literal(train[0])}"
        else:
            cond = f"day BETWEEN {day_literal(train[0])} AND {day_literal(train[-1])}"
        parts.append(f"WHEN {cond} THEN 'train'")
    if val:
        cond = ("day = " + day_literal(val[0])) if len(val) == 1 else \
               f"day BETWEEN {day_literal(val[0])} AND {day_literal(val[-1])}"
        parts.append(f"WHEN {cond} THEN 'validation'")
    if test:
        cond = ("day = " + day_literal(test[0])) if len(test) == 1 else \
               f"day BETWEEN {day_literal(test[0])} AND {day_literal(test[-1])}"
        parts.append(f"WHEN {cond} THEN 'test'")
    return "CASE " + " ".join(parts) + " ELSE 'unassigned' END"


def bucket_case(col, edges):
    """Quartile buckets; NULL gets its own bucket (-1) instead of being dropped."""
    e0, e1, e2 = edges
    return (f"CASE WHEN {col} IS NULL THEN -1"
            f" WHEN {col} <= {e0} THEN 0"
            f" WHEN {col} <= {e1} THEN 1"
            f" WHEN {col} <= {e2} THEN 2"
            f" ELSE 3 END")


def pick_int_reps(profile, n):
    """Deterministic rule: fewest missing values, tie-break on column index.

    Chosen from data quality, not from how a column behaves under a layout.
    """
    ranked = sorted(INT_COLS,
                    key=lambda c: (profile["integer_features"][c]["null_fraction"] or 1.0,
                                   INT_COLS.index(c)))
    return ranked[:n]


def str_of(col):
    """Hash input for a column, with a frozen NULL representation.

    The integer cast has to be explicit: `coalesce(int_col, '__NULL__')` leaves
    Spark to widen INT and STRING on its own, and when it picks BIGINT the NULL
    branch blows up with CAST_INVALID_INPUT -- but only on the day partitions
    that actually contain a NULL, which is how this survived a train-only run.
    """
    if col.startswith("integer_feature_"):
        return f"coalesce(CAST({col} AS STRING), '{NULL_TOKEN}')"
    return f"coalesce({col}, '{NULL_TOKEN}')"


def hash_check(low, int_col):
    return (f"sum(CAST(hash(concat_ws('|', {str_of(low)}, {str_of(int_col)})) "
            f"AS BIGINT)) AS hash_check")


def dense_sums(prefix):
    """Force-decode the 13 dense columns: sums, so the reader must materialise them."""
    return [f"sum(CAST(coalesce({c}, 0) AS BIGINT)) AS {prefix}_{c}"
            for c in INT_COLS]


def sparse_sums(prefix):
    """Force-decode the 26 sparse columns via their string lengths."""
    return [f"sum(length(coalesce({c}, '{NULL_TOKEN}'))) AS {prefix}_{c}"
            for c in CAT_COLS]


def build(profile, args):
    table = args.table
    split = profile["split"]
    ints = profile["integer_features"]
    cats = profile["categorical_features"]
    reps = profile["representative_columns"]
    low, mid, high = reps["low"], reps["mid"], reps["high"]
    int_reps = pick_int_reps(profile, N_INT_REPS)
    caps = {c: int(round(ints[c]["quantiles"]["0.99"])) for c in INT_COLS}
    train_f = day_filter(split["train"])
    val_f = day_filter(split["validation"])
    test_f = day_filter(split["test"])
    q = {}

    # --- 1. data integrity -------------------------------------------------
    q[1] = (f"SELECT count(*) AS n_rows, sum(CAST(label AS BIGINT)) AS n_clicks, "
            f"avg(CAST(label AS DOUBLE)) AS ctr FROM {table}")
    q[2] = (f"SELECT day, count(*) AS n_rows, sum(CAST(label AS BIGINT)) AS n_clicks, "
            f"avg(CAST(label AS DOUBLE)) AS ctr FROM {table} "
            f"GROUP BY day ORDER BY day")
    q[3] = (f"SELECT {split_case(split)} AS split_name, label, count(*) AS n_rows "
            f"FROM {table} GROUP BY 1, 2 ORDER BY split_name, label")

    # --- 2. dense feature processing --------------------------------------
    q[4] = ("SELECT count(*) AS n_rows, " + ", ".join(
        f"1.0 - count({c}) / count(*) AS null_rate_{c}" for c in INT_COLS) +
        f" FROM {table}")
    q[5] = ("SELECT " + ", ".join(
        f"min({c}) AS min_{c}, max({c}) AS max_{c}, "
        f"avg({c}) AS avg_{c}, stddev({c}) AS std_{c}" for c in INT_COLS) +
        f" FROM {table}")
    # Q06: measured quartile buckets, one UNION ALL branch per representative.
    branches = []
    for c in int_reps:
        branches.append(
            f"SELECT '{c}' AS feature, {bucket_case(c, ints[c]['bucket_edges'])} AS bucket, "
            f"count(*) AS n_rows, avg(CAST(label AS DOUBLE)) AS ctr "
            f"FROM {table} GROUP BY 1, 2")
    q[6] = ("SELECT * FROM (" + " UNION ALL ".join(branches) +
            ") ORDER BY feature, bucket")
    q[7] = ("SELECT " + ", ".join(
        f"sum(ln(CAST(coalesce({c}, 0) AS DOUBLE) + 1.0)) AS log1p_sum_{c}, "
        f"sum(CASE WHEN coalesce({c}, 0) > {caps[c]} THEN {caps[c]} "
        f"ELSE coalesce({c}, 0) END) AS clipped_sum_{c}" for c in INT_COLS) +
        f" FROM {table}")

    # --- 3. categorical feature processing --------------------------------
    q[8] = ("SELECT count(*) AS n_rows, " + ", ".join(
        f"1.0 - count({c}) / count(*) AS null_rate_{c}" for c in CAT_COLS) +
        f" FROM {table}")
    q[9] = (f"SELECT approx_count_distinct({low}) AS low_cardinality, "
            f"approx_count_distinct({mid}) AS mid_cardinality, "
            f"approx_count_distinct({high}) AS high_cardinality FROM {table}")
    for idx, col in ((10, low), (11, mid), (12, high)):
        q[idx] = (f"SELECT {col} AS feature_value, count(*) AS n_rows, "
                  f"avg(CAST(label AS DOUBLE)) AS ctr FROM {table} "
                  f"GROUP BY {col} ORDER BY n_rows DESC, feature_value ASC LIMIT {TOPK}")
    q[13] = (f"SELECT pmod(hash(coalesce({mid}, '{NULL_TOKEN}')), {BUCKETS}) AS bucket, "
             f"count(*) AS n_rows, avg(CAST(label AS DOUBLE)) AS ctr FROM {table} "
             f"GROUP BY 1 ORDER BY n_rows DESC, bucket ASC LIMIT {RESULT_ROW_CAP}")

    # --- 4. feature crosses ------------------------------------------------
    q[14] = (f"SELECT pmod(hash(concat_ws('|', coalesce({mid}, '{NULL_TOKEN}'), "
             f"coalesce({low}, '{NULL_TOKEN}'))), {BUCKETS}) AS bucket, "
             f"count(*) AS n_rows, avg(CAST(label AS DOUBLE)) AS ctr FROM {table} "
             f"GROUP BY 1 ORDER BY n_rows DESC, bucket ASC LIMIT {RESULT_ROW_CAP}")
    q[15] = (f"SELECT {bucket_case(int_reps[0], ints[int_reps[0]]['bucket_edges'])} "
             f"AS int_bucket, {low} AS feature_value, count(*) AS n_rows, "
             f"avg(CAST(label AS DOUBLE)) AS ctr FROM {table} "
             f"GROUP BY 1, 2 ORDER BY n_rows DESC, int_bucket ASC, feature_value ASC "
             f"LIMIT {RESULT_ROW_CAP}")
    q[16] = (f"SELECT day, {low} AS feature_value, count(*) AS n_rows, "
             f"avg(CAST(label AS DOUBLE)) AS ctr FROM {table} "
             f"GROUP BY 1, 2 ORDER BY day ASC, n_rows DESC, feature_value ASC "
             f"LIMIT {RESULT_ROW_CAP}")
    q[17] = (f"SELECT pmod(hash(concat_ws('|', coalesce({low}, '{NULL_TOKEN}'), "
             f"coalesce({mid}, '{NULL_TOKEN}'), coalesce({high}, '{NULL_TOKEN}'))), "
             f"{BUCKETS_JOINT}) AS bucket, count(*) AS n_rows, "
             f"avg(CAST(label AS DOUBLE)) AS ctr FROM {table} "
             f"GROUP BY 1 ORDER BY n_rows DESC, bucket ASC LIMIT {RESULT_ROW_CAP}")

    # --- 5. model input / epoch scans --------------------------------------
    head = "SELECT count(*) AS n_rows, sum(CAST(label AS BIGINT)) AS label_sum"
    q[18] = (f"{head}, " + ", ".join(dense_sums("s")) + f" FROM {table} WHERE {train_f}")
    q[19] = (f"{head}, " + ", ".join(sparse_sums("len")) + f" FROM {table} WHERE {train_f}")
    # Q20-Q22 are byte-identical: three training epochs over the same input.
    epoch = (f"{head}, " + ", ".join(dense_sums("s") + sparse_sums("len")) +
             f", {hash_check(low, int_reps[0])} "
             f"FROM {table} WHERE {train_f}")
    for i in range(args.epochs):
        q[20 + i] = epoch
    q[23] = (f"{head}, " + ", ".join(dense_sums("s") + sparse_sums("len")) +
             f", {hash_check(low, int_reps[0])} "
             f"FROM {table} WHERE {val_f}")
    q[24] = (f"{head}, " + ", ".join(dense_sums("s") + sparse_sums("len")) +
             f", {hash_check(low, int_reps[0])} "
             f"FROM {table} WHERE {test_f}")

    doc = {"_comment": (f"{WORKLOAD_ID}: Criteo CTR data-quality checks, feature "
                        f"statistics, feature engineering and multi-epoch training "
                        f"input scans. Generated from criteo_profile.json; not an "
                        f"official Criteo benchmark and not a full DLRM training run.")}
    for k in sorted(q):
        doc[str(k)] = q[k]
    return doc, {
        "int_representatives": int_reps,
        "clip_caps": caps,
        "bucket_edges": {c: ints[c]["bucket_edges"] for c in int_reps},
        "train_filter": train_f, "val_filter": val_f, "test_filter": test_f,
    }


def git_commit():
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))),
            capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or None
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--table", default="criteo")
    ap.add_argument("--out-queries",
                    default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         "criteo_queries.json"))
    ap.add_argument("--out-manifest",
                    default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         "criteo_workload_manifest.json"))
    ap.add_argument("--epochs", type=int, default=3)
    args = ap.parse_args()

    profile = json.load(open(args.profile))
    queries, derived = build(profile, args)
    text = json.dumps(queries, indent=2, ensure_ascii=False) + "\n"
    with open(args.out_queries, "w") as fh:
        fh.write(text)

    manifest = {
        "workload_id": WORKLOAD_ID,
        "generator_version": GENERATOR_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_uri": profile["dataset_uri"],
        "table": args.table,
        "objects": {"n_files": profile["objects"]["n_files"],
                    "total_bytes": profile["objects"]["total_bytes"]},
        "rows": {"total": profile["rows"]["total"],
                 "clicks": profile["rows"]["clicks"],
                 "ctr": profile["rows"]["ctr"],
                 "per_day": {d: v["rows"] for d, v in profile["rows"]["per_day"].items()}},
        "schema_sha256": profile["schema"]["sha256"],
        "profile_uri": os.path.abspath(args.profile),
        "profile_version": profile.get("profile_version"),
        "split": {"train": profile["split"]["train"],
                  "validation": profile["split"]["validation"],
                  "test": profile["split"]["test"],
                  "sql_filters": {k: derived[f"{k}_filter"]
                                  for k in ("train", "val", "test")}},
        "dense": {"representatives": derived["int_representatives"],
                  "bucket_edges": derived["bucket_edges"],
                  "clip_caps_p99": derived["clip_caps"],
                  "bucket_source": "measured quartiles (0.25/0.5/0.75)"},
        "categorical": {
            "representatives": profile["representative_columns"],
            "selection_rule": "sort by approx cardinality, tie-break on column index",
            "cardinality_of_representatives": {
                k: profile["categorical_features"][v]["approx_distinct"]
                for k, v in profile["representative_columns"].items()},
            "topk": {"k": TOPK, "ordering": "count DESC, value ASC",
                     "measured_head": {c: [d["value"] for d in v[:5]]
                                       for c, v in profile["topk"]["values"].items()}},
        },
        "hash": {"function": HASH_FN, "null_token": NULL_TOKEN,
                 "buckets": BUCKETS, "buckets_joint": BUCKETS_JOINT},
        "epochs": args.epochs,
        "result_row_cap": RESULT_ROW_CAP,
        "query_count": len([k for k in queries if k.isdigit()]),
        "query_order": [int(k) for k in sorted((k for k in queries if k.isdigit()),
                                               key=int)],
        "queries_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "generator_git_commit": git_commit(),
        "constraints": {
            "no_random": True, "no_current_timestamp": True, "no_tablesample": True,
            "no_limit_without_order_by": True, "no_spark_cache": True,
            "one_spark_process_per_cell_and_round": True,
            "query_choice_inputs": ["schema", "criteo_profile.json"],
            "query_choice_excludes": ["baseline timing", "candidate timing",
                                      "any layout geometry beyond the profile"],
        },
        "notes": [
            "Q20-Q22 are byte-identical on purpose: three training epochs.",
            "No query uses SELECT *: runner collects to the driver, so columns are "
            "force-decoded with aggregates instead.",
            "`day` is a DATE partition column; all day predicates use DATE literals.",
            "Simulates training-input decoding and multi-epoch scan, not end-to-end "
            "CTR model training (that needs a separate Spark ML / DLRM runner).",
        ],
    }
    with open(args.out_manifest, "w") as fh:
        json.dump(manifest, fh, indent=2)
    print(f"# {WORKLOAD_ID}")
    print(f"  queries  {manifest['query_count']} -> {args.out_queries}")
    print(f"  sha256   {manifest['queries_sha256']}")
    print(f"  manifest {args.out_manifest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
