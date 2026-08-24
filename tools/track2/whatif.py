#!/usr/bin/env python3
"""EVALUATE side: L0 static checks + L1 analytical cost (virtual footer).

DB2 Design Advisor compiles each candidate in EVALUATE mode against the
optimizer. We don't have that optimizer, so a candidate is a virtual footer
and the cost is the frozen Reader's request plan:

    t_io  = Σ_scan (RTT + bytes/BW) / K_eff
            K_eff = min(K_busy, n_files_opened, local[N])
    t_exec_residual = residual_base(q) * rows_c / rows_base
            * (n_files_base / n_files)^α   if join/agg and no prune
    t_e2e = t_io + t_exec_residual

`t_exec_residual` is *not* a CPU model. It is the calibration residual
`measured_median(q) - predicted_baseline_io(q)`, so it absorbs decode,
aggregation, shuffle, scheduling, JVM/GC *and* any error in the I/O term,
and it is then rescaled linearly in rows scanned. Linear-in-rows is wrong
for shuffle/join stages and for the fixed per-query overhead; the name is
meant to keep that visible rather than imply the model knows what CPU did.

L0 additionally refuses two sort-specific layouts (both knobs are for
later ablation; defaults below):

  * Gate A: sort prefix whose baseline rg_span is within
    --cluster-headroom-min of the span a sort could actually reach
    (column already clustered; rewriting it has no prune headroom).
  * Gate C: a pruned scan whose surviving row groups AND files both
    fall below --prune-parallelism-floor (the remaining work serializes
    onto one or two connections). K_eff stays in the cost model as the
    ranking term among candidates that pass.
  * Gate D: a partition on a derived column (cannot prune at all), or an
    identity partition past --max-partitions / under
    --min-partition-bytes per directory.

Search prunes any L0-legal candidate whose predicted per-query t_e2e
regresses more than 10% vs the *same L1* on the baseline (contract §4.1
as a feasibility cut, not a speed-up). Default grid is per-table: every
table above the size threshold chooses file size, sort and partition
independently. The old global grid is `--grid global`.

`--validate` is the self-consistency gate: replaying the baseline candidate
must reproduce the ranged GETs and bytes the E2 report actually recorded,
within 10%. Both the geometry it predicts from and the scans it prices come
from measured snapshots -- see advisor_catalog.py.

Usage:
  python3 tools/track2/whatif.py --search --validate \
      --sysconst .../sysconst.json --analyze-dir .../e5_whatif \
      --dataset-snapshot .../dataset_snapshot.json \
      --workload-snapshot .../workload_snapshot.json \
      --e2-report .../e2_baseline/report.json \
      --regime measured_cross_cloud
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import advisor_catalog  # noqa: E402
import advisor_policy  # noqa: E402
import virtual_footer as vf  # noqa: E402
from advisor_policy import (  # noqa: E402
    CLUSTER_HEADROOM_MIN, FOOTER_BYTES, GUARDRAIL_REGRESSION, HEAD_PER_OPEN,
    JOIN_AGG_FILE_ALPHA, JOIN_AGG_NO_PRUNE, LARGE_TABLE_BYTES, MAX_PARTITIONS,
    MAX_READABLE_RG_BYTES, META_GETS_PER_OPEN, MIN_PARTITION_BYTES,
    PARALLELISM, PRUNE_PARALLELISM_FLOOR, VALIDATE_TOL)
from write_layout import render, check_l0  # noqa: E402

# The measured catalog. bind_catalog installs it here and in virtual_footer.
catalog = None


def bind_catalog(cat):
    global catalog
    catalog = cat
    vf.catalog = cat
    import analyze_layout
    analyze_layout.catalog = cat
    return cat


def _primary_table():
    return max(catalog.BASELINE_GEOMETRY,
               key=lambda t: catalog.BASELINE_GEOMETRY[t]["compressed_bytes"])


def _column_stat(col_stats, table, column, key):
    cols = ((col_stats or {}).get(table) or {}).get("columns") or {}
    return (cols.get(column) or {}).get(key)


def achievable_rg_span(table, column, col_stats):
    """Smallest rg_span a global sort on `column` could reach.

    A range sort splits the table into n_rg row groups, so each covers about
    1/n_rg of the domain -- unless the column has fewer distinct values than
    row groups, in which case ties pin the floor at 1/ndv. This is the same
    NDV bound `predict_geometry` uses to cap the file count, applied one level
    down.
    """
    geom = catalog.BASELINE_GEOMETRY[table]
    n_rg = geom.get("n_rg") or max(1, int(round(geom["files"] * geom["rg_per_file"])))
    ndv = _column_stat(col_stats, table, column, "ndv")
    floor = 1.0 / max(n_rg, 1)
    if ndv:
        floor = max(floor, 1.0 / float(ndv))
    return floor


def cluster_headroom_violations(candidate, col_stats, headroom_min):
    """Gate A: refuse to re-sort a column that is already as clustered as a
    sort could make it. See advisor_policy.CLUSTER_HEADROOM_MIN."""
    if not headroom_min:
        return []
    viol = []
    for table in catalog.BASELINE_GEOMETRY:
        sort_cols = vf.layout_for(table, candidate)["sort_columns"]
        if not sort_cols:
            continue
        prefix = sort_cols[0]
        span = _column_stat(col_stats, table, prefix, "rg_span")
        if span is None:
            continue
        span = float(span)
        floor = achievable_rg_span(table, prefix, col_stats)
        headroom = span / floor if floor else float("inf")
        if headroom < headroom_min:
            viol.append(
                f"{table}.{prefix} rg_span {span:.4f} is only {headroom:.2f}x "
                f"the {floor:.4f} a sort could reach (< cluster-headroom-min "
                f"{headroom_min}); baseline is already clustered")
    return viol


def prune_parallelism_violations(candidate, col_stats, empirical_corr,
                                 parallelism_floor):
    """Gate C: hard-reject a sort that serializes a pruned scan.

    Both surviving row groups and surviving files must fall below the floor.
    Spark splits large files by row group, so a 2 GiB file with 16 RGs is
    still 16 tasks; a 3-month date range that keeps 5 files / 5 RGs is legal
    at the default floor of 4.
    """
    if not parallelism_floor:
        return []
    for qnr, scans in catalog.QUERIES.items():
        for scan in scans:
            prune_sel, branch = vf.prune_fraction(
                scan, candidate, col_stats, empirical_corr)
            if prune_sel >= 0.999:
                continue
            geom = vf.predict_geometry(scan["table"], candidate)
            n_rg, n_files = vf.surviving_rg_and_files(geom, prune_sel)
            if n_rg < parallelism_floor and n_files < parallelism_floor:
                return [f"Q{qnr} {scan['table']} prune leaves {n_rg} RGs / "
                        f"{n_files} files < parallelism floor "
                        f"{parallelism_floor} ({branch.get('branch')}, "
                        f"sel={prune_sel:.4g})"]
    return []


def partition_violations(candidate, max_partitions, min_partition_bytes):
    """Gate D: refuse partitions that cannot prune or that shred the table."""
    viol = []
    for table in catalog.BASELINE_GEOMETRY:
        spec = vf.partition_spec(vf.layout_for(table, candidate))
        if not spec:
            continue
        column, transform, n_parts = spec
        if transform != "identity":
            viol.append(
                f"{table} partition {column}:{transform} is a derived column; "
                f"Spark cannot prune it from a predicate on {column} itself, so "
                f"it costs a rewrite and prunes nothing (needs Iceberg hidden "
                f"partitioning)")
            continue
        if max_partitions and n_parts > max_partitions:
            viol.append(f"{table} partition {column} -> {n_parts} directories "
                        f"> max-partitions {max_partitions}")
        if min_partition_bytes:
            per_part = vf.table_bytes(table) / max(n_parts, 1)
            if per_part < min_partition_bytes:
                viol.append(
                    f"{table} partition {column} leaves {per_part / 2 ** 20:.0f} "
                    f"MiB per directory < floor "
                    f"{min_partition_bytes / 2 ** 20:.0f} MiB")
    return viol


def l0_check(candidate, source_bytes=None, col_stats=None, empirical_corr=True,
             cluster_headroom_min=CLUSTER_HEADROOM_MIN,
             prune_parallelism_floor=PRUNE_PARALLELISM_FLOOR,
             max_partitions=MAX_PARTITIONS,
             min_partition_bytes=MIN_PARTITION_BYTES):
    """Contract 5.3 plus the measured vectored-timeout readability bound."""
    primary = _primary_table()
    rendered = render(candidate.get("actions") or [], table=primary)
    table_b = source_bytes or vf.table_bytes(primary)
    violations = list(check_l0(rendered, table_b))
    # Writer capability: every action already had to render (else render() raises).
    # Reader capability: page index is not an action; bloom is M5-only.
    # Structure: evaluate predicted primary-table geometry.
    geom = vf.predict_geometry(primary, candidate)
    if geom["n_files"] < 2:
        violations.append(f"{primary} files {geom['n_files']} < 2")
    if geom["n_rg"] < 4:
        violations.append(f"{primary} row groups {geom['n_rg']} < 4")
    # Budget: SF100 rewrite is in budget; reject only absurd blow-ups.
    if geom["n_files"] > 20000:
        violations.append(f"{primary} files {geom['n_files']} exceed rewrite budget")
    for table in catalog.BASELINE_GEOMETRY:
        rg = vf.layout_for(table, candidate)["rg_bytes"]
        if rg and rg > MAX_READABLE_RG_BYTES:
            violations.append(
                f"{table} requested row-group {rg} B exceeds readable bound "
                f"{MAX_READABLE_RG_BYTES} B (parquet vectored wait is hardcoded "
                f"300s; M2 canary 35–182 MiB ranges timed out)")
    # Parallelism floor: each large table is checked on its own predicted
    # file count. A per-table 1GB on lineitem is legal; the same size on
    # orders is not. nation/region/customer stay exempt (< 2 GiB).
    for table, base in catalog.BASELINE_GEOMETRY.items():
        if base["compressed_bytes"] < LARGE_TABLE_BYTES:
            continue
        g = vf.predict_geometry(table, candidate)
        floor = min(PARALLELISM, base["files"])
        if g["n_files"] < floor:
            violations.append(
                f"{table} files {g['n_files']} < parallelism floor {floor} "
                f"(baseline {base['files']})")
    violations.extend(cluster_headroom_violations(
        candidate, col_stats, cluster_headroom_min))
    violations.extend(prune_parallelism_violations(
        candidate, col_stats, empirical_corr, prune_parallelism_floor))
    violations.extend(partition_violations(
        candidate, max_partitions, min_partition_bytes))
    return (not violations), violations, geom


def _load_json(path):
    with open(path) as fh:
        return json.load(fh)


def load_column_stats(path=None):
    """Column facts for L1, from the dataset snapshot.

    The snapshot already merges the two sources that matter -- `rg_span` from
    the footers it read and `ndv`/`cdf` from the DuckDB pass -- so there is no
    synthetic tier any more. `workload.SYNTHETIC_STATS` used to supply uniform
    TPC-H date CDFs so the model could prune before column_stats.json landed;
    those were the spec's domain, not the data's, and the measured values
    differ (l_shipdate NDV 2505, not the 2526 the spec implies).

    `path` overrides with a freshly collected column_stats.json, which is what
    a candidate layout needs after it is materialised.
    """
    stats = {t: {"columns": dict(rec.get("columns") or {})}
             for t, rec in (catalog.COLUMN_STATS or {}).items()}
    if path and os.path.exists(path):
        data = _load_json(path)
        for table, rec in (data.get("tables") or data).items():
            entry = stats.setdefault(table, {"columns": {}})
            for name, cs in (rec.get("columns") or rec).items():
                merged = dict(entry["columns"].get(name) or {})
                for key, value in cs.items():
                    if value is not None:
                        merged[key] = value
                entry["columns"][name] = merged
    return stats


def evaluate_scan(scan, candidate, col_stats, empirical_corr, vectored):
    table = scan["table"]
    geom = vf.predict_geometry(table, candidate)
    prune_sel, branch = vf.prune_fraction(scan, candidate, col_stats, empirical_corr)
    n_rg, n_files = vf.surviving_rg_and_files(geom, prune_sel)
    min_seek = vectored.get("min_seek_bytes", 131072)
    max_merged = vectored.get("max_merged_bytes", 2097152)
    gets_per_rg, bytes_per_rg = vf.merge_gets(
        table, scan["columns"], geom, min_seek, max_merged)
    data_gets = n_rg * gets_per_rg
    data_bytes = n_rg * bytes_per_rg
    meta_gets = n_files * META_GETS_PER_OPEN
    meta_bytes = n_files * FOOTER_BYTES * META_GETS_PER_OPEN
    heads = n_files * HEAD_PER_OPEN
    return {
        "table": table,
        "n_files_opened": n_files,
        "n_rg_surviving": n_rg,
        "n_rg_total": geom["n_rg"],
        "prune_sel": prune_sel,
        "branch": branch["branch"],
        "data_gets": data_gets,
        "data_bytes": data_bytes,
        "meta_gets": meta_gets,
        "meta_bytes": meta_bytes,
        "heads": heads,
        "rows_frac": n_rg / max(geom["n_rg"], 1),
    }


def evaluate_query(qnr, candidate, col_stats, empirical_corr, vectored):
    scans = []
    tot = {"data_gets": 0, "data_bytes": 0, "meta_gets": 0, "meta_bytes": 0,
           "heads": 0, "rows_frac_sum": 0.0, "n_scans": 0}
    for scan in catalog.QUERIES[qnr]:
        rec = evaluate_scan(scan, candidate, col_stats, empirical_corr, vectored)
        scans.append(rec)
        for k in ("data_gets", "data_bytes", "meta_gets", "meta_bytes", "heads"):
            tot[k] += rec[k]
        tot["rows_frac_sum"] += rec["rows_frac"]
        tot["n_scans"] += 1
    tot["ranged_gets"] = tot["data_gets"] + tot["meta_gets"]
    tot["scans"] = scans
    return tot


def t_io_from_counts(tot, regime):
    """Sum per-scan IO. K_eff cannot exceed files actually opened.

    On the baseline every large table has n_files > K_busy, so this equals
    the old (Σ cost) / K_busy formula used by the GET/byte validate gate.
    """
    rtt = regime["rtt_s"]
    bw = regime["bw_bps"]
    k_busy = regime["K_busy"] or 1.0
    n_req = tot["ranged_gets"] + tot["heads"]
    bytes_ = tot["data_bytes"] + tot["meta_bytes"]
    scans = tot.get("scans") or []
    if not scans:
        serial = n_req * rtt + (bytes_ / bw if bw else 0.0)
        return serial / k_busy, n_req, bytes_
    t_io = 0.0
    for rec in scans:
        req = rec["data_gets"] + rec["meta_gets"] + rec["heads"]
        b = rec["data_bytes"] + rec["meta_bytes"]
        k = max(1.0, min(k_busy, rec["n_files_opened"], PARALLELISM))
        rec["k_eff"] = round(k, 3)
        t_io += (req * rtt + (b / bw if bw else 0.0)) / k
    return t_io, n_req, bytes_


def join_agg_residual_scale(tot, candidate):
    """Penalize unpruned multi-scan queries when the fact table coalesces.

    Not a join cardinality model: (n_files_base / n_files)^α with α=0.5,
    so Q18's two full lineitem scans + GROUP BY get more expensive as files
    drop 200→22, while pruned scans (Q15) keep scale 1.
    """
    if tot["n_scans"] < 2:
        return 1.0, False
    tables = {rec["table"] for rec in tot["scans"]}
    fact = max(tables, key=lambda t: vf.table_bytes(t))
    rows_frac = max(rec["rows_frac"] for rec in tot["scans"] if rec["table"] == fact)
    if rows_frac < JOIN_AGG_NO_PRUNE:
        return 1.0, False
    n_c = max(vf.predict_geometry(fact, candidate)["n_files"], 1)
    n_b = catalog.BASELINE_GEOMETRY[fact]["files"]
    if n_c >= n_b:
        return 1.0, True
    return (n_b / n_c) ** JOIN_AGG_FILE_ALPHA, True


def evaluate_workload(candidate, queries, medians, col_stats, regime,
                      empirical_corr, vectored, residual_base=None, rows_base=None):
    per_q = {}
    sum_t_io = sum_t_exec_residual = 0.0
    sum_gets = sum_bytes = 0
    for q in queries:
        tot = evaluate_query(q, candidate, col_stats, empirical_corr, vectored)
        t_io, n_req, nbytes = t_io_from_counts(tot, regime)
        rows = tot["rows_frac_sum"]
        if residual_base is not None and rows_base and rows_base.get(q):
            t_exec_residual = max(0.0, residual_base.get(q, 0.0) * (rows / rows_base[q]))
        else:
            t_exec_residual = 0.0
        residual_scale, join_unpruned = join_agg_residual_scale(tot, candidate)
        t_exec_residual *= residual_scale
        t_e2e = t_io + t_exec_residual
        per_q[str(q)] = {
            "t_io_s": round(t_io, 3),
            "t_exec_residual_s": round(t_exec_residual, 3),
            "t_e2e_s": round(t_e2e, 3),
            "ranged_gets": tot["ranged_gets"],
            "heads": tot["heads"],
            "bytes": nbytes,
            "rows_frac_sum": round(rows, 4),
            "residual_scale": round(residual_scale, 4),
            "join_agg_unpruned": join_unpruned,
            "measured_median_s": medians.get(q),
            "scans": tot["scans"],
        }
        sum_t_io += t_io
        sum_t_exec_residual += t_exec_residual
        sum_gets += tot["ranged_gets"]
        sum_bytes += nbytes
    return {
        "candidate_id": candidate.get("candidate_id"),
        "t_io_s": round(sum_t_io, 3),
        "t_exec_residual_s": round(sum_t_exec_residual, 3),
        "t_e2e_s": round(sum_t_io + sum_t_exec_residual, 3),
        "ranged_gets": sum_gets,
        "bytes": sum_bytes,
        "bytes_gib": round(sum_bytes / 2 ** 30, 2),
        "per_query": per_q,
    }


def calibrate_execution_residual(baseline_eval, medians):
    """measured - predicted_io on the baseline, per query.

    Everything the L1 I/O term fails to explain lands here, including its own
    error. Clamped at 0 so an over-predicting I/O term cannot hand out a
    negative execution cost.
    """
    residual = {}
    rows = {}
    for q, rec in baseline_eval["per_query"].items():
        qn = int(q)
        residual[qn] = max(0.0, (medians.get(qn) or 0.0) - rec["t_io_s"])
        rows[qn] = rec["rows_frac_sum"] or 1.0
    return residual, rows


def query_regressions(ev, baseline_per_q):
    """Per-query predicted t_e2e vs baseline L1 (same model)."""
    out = []
    for q, rec in ev["per_query"].items():
        base_rec = baseline_per_q.get(q) or baseline_per_q.get(str(int(q)))
        if not base_rec:
            continue
        base = base_rec["t_e2e_s"]
        if not base:
            continue
        out.append({
            "query": int(q),
            "t_e2e_s": rec["t_e2e_s"],
            "baseline_t_e2e_s": base,
            "regression": round(rec["t_e2e_s"] / base - 1.0, 4),
        })
    return out


def guardrail_regressions(ev, baseline_per_q, limit=GUARDRAIL_REGRESSION):
    """§4.1: predicted t_e2e must not exceed 1.10 × baseline L1 t_e2e."""
    return [r for r in query_regressions(ev, baseline_per_q)
            if r["regression"] > limit]


def load_candidates(analyze_dir, grid="per-table"):
    name = "candidates_per_table.json" if grid == "per-table" else "candidates.json"
    path = os.path.join(analyze_dir, name)
    if not os.path.exists(path):
        raise SystemExit(
            f"{path} not found. Run analyze_layout.py --emit-per-table with "
            f"--runtime-predicates first: EVALUATE no longer regenerates the "
            f"grid, because doing so hid which evidence it came from.")
    return _load_json(path)


def iterative_search(legal, score_key="t_e2e_s"):
    """DB2-style one-pass iteration over the candidate's coordinate keys."""
    def pick(cands, key, current):
        grouped = {}
        for c in cands:
            grouped.setdefault(c["candidate"][key], []).append(c)
        # score a value by the candidate that matches `current` on other keys
        best_val, best_score = None, None
        for val, group in grouped.items():
            matching = [g for g in group if all(
                g["candidate"][k] == current[k] for k in current if k != key)]
            pool = matching or group
            winner = min(pool, key=lambda g: g[score_key])
            if best_score is None or winner[score_key] < best_score:
                best_val, best_score = val, winner[score_key]
        return best_val

    sample = legal[0]["candidate"]
    axes = sample.get("axes")
    if axes:
        # `<table>.<axis>` coordinates, largest table first, sort before file
        # before partition. Start every axis at its baseline value, which is
        # what makes this a one-pass descent from the do-nothing layout.
        baseline_value = {"file": "baseline", "sort": "none", "partition": "none"}
        order = list(axes)
        current = {a: baseline_value[a.rsplit(".", 1)[1]] for a in order}
    elif sample.get("scope") == "per_table" and "li_sort" in sample:
        current = {"li_sort": "none", "li_file": "baseline",
                   "o_sort": "none", "o_file": "baseline",
                   "ps_file": "baseline"}
        order = ["li_sort", "li_file", "o_sort", "o_file", "ps_file"]
    elif sample.get("scope") == "per_table":
        current = {"sort_label": "none", "file_label": "baseline"}
        order = ["sort_label", "file_label"]
    else:
        current = {"sort_label": "none", "rg_label": "128MB",
                   "file_label": "128MB", "partition": "none"}
        order = ["sort_label", "rg_label", "file_label", "partition"]
    for key in order:
        current[key] = pick(legal, key, current)
    # resolve to an actual candidate
    for c in legal:
        cand = c["candidate"]
        if all(cand.get(k) == current[k] for k in current):
            return current, c
    return current, min(legal, key=lambda g: g[score_key])


def e2_observed_io(path):
    """(ranged GETs, GiB) for one pass of the workload, from an E2 report.

    The first run only. The S3A counters in the report are cumulative over the
    session, so run 2 of TPC-H reads 128064 GETs and run 5 reads 320160 -- a
    median across runs would compare one predicted pass against three measured
    ones. This is also where the frozen 64032 / 149.32 GiB oracle came from;
    it is read here instead of transcribed so that a re-run of E2 moves it.
    """
    if not path or not os.path.exists(path):
        return None, None
    runs = _load_json(path).get("runs") or []
    if not runs:
        return None, None
    io = runs[0].get("io") or {}
    nbytes = io.get("remote_bytes")
    return io.get("ranged_gets"), (nbytes / 2 ** 30 if nbytes else None)


def run_validate(args, regime, vectored, col_stats, medians):
    baseline = {"candidate_id": "baseline", "actions": [], "partition": "none",
                "file_bytes": None, "rg_bytes": None, "sort_columns": []}
    queries = sorted(catalog.QUERIES)
    ev = evaluate_workload(baseline, queries, medians, col_stats, regime,
                           args.empirical_corr, vectored)
    e2_gets, e2_gib = e2_observed_io(getattr(args, "e2_report", None))
    if e2_gets and e2_gib:
        gets_err = abs(ev["ranged_gets"] - e2_gets) / e2_gets
        gib_err = abs(ev["bytes_gib"] - e2_gib) / e2_gib
        ok = gets_err <= VALIDATE_TOL and gib_err <= VALIDATE_TOL
    else:
        gets_err = gib_err = None
        ok = True
    return {
        "gate": "L1 self-consistency vs the E2 baseline it was calibrated on",
        "e2_report": getattr(args, "e2_report", None),
        "predicted_gets": ev["ranged_gets"],
        "actual_gets": e2_gets,
        "gets_rel_error": None if gets_err is None else round(gets_err, 4),
        "predicted_gib": ev["bytes_gib"],
        "actual_gib": None if e2_gib is None else round(e2_gib, 2),
        "gib_rel_error": None if gib_err is None else round(gib_err, 4),
        "tolerance": VALIDATE_TOL,
        "pass": ok,
        "t_io_s": ev["t_io_s"],
        "measured_median_s": sum(medians.values()) if medians else None,
        "note": None if e2_gets else "E2 IO not available; search still runs",
        "per_query": {q: {"gets": rec["ranged_gets"], "t_io_s": rec["t_io_s"],
                          "measured_s": rec["measured_median_s"]}
                      for q, rec in ev["per_query"].items()},
    }, ev, ok


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sysconst", required=True)
    ap.add_argument("--analyze-dir", required=True)
    advisor_catalog.add_arguments(ap)
    ap.add_argument("--e2-report", default=None,
                    help="E2 report.json; supplies the measured GETs/bytes the "
                         "L1 self-consistency gate replays")
    ap.add_argument("--column-stats", default=None,
                    help="override the snapshot's column facts")
    ap.add_argument("--per-query", default=None)
    ap.add_argument("--regime", default="measured_cross_cloud")
    ap.add_argument("--empirical-corr", action="store_true", default=True)
    ap.add_argument("--no-empirical-corr", dest="empirical_corr", action="store_false")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--search", action="store_true")
    ap.add_argument("--rtt-sweep", action="store_true")
    ap.add_argument("--grid", choices=("per-table", "global"), default="per-table",
                    help="per-table file size/sort (default) or the old global PTO grid")
    ap.add_argument("--cluster-headroom-min", type=float,
                    default=CLUSTER_HEADROOM_MIN,
                    help="Gate A: reject sort if the prefix's baseline rg_span "
                         "is less than this multiple of the span a sort could "
                         "reach (0 disables). Ablation knob.")
    ap.add_argument("--prune-parallelism-floor", type=int,
                    default=PRUNE_PARALLELISM_FLOOR,
                    help="Gate C: reject sort if a pruned scan keeps fewer RGs "
                         "AND fewer files than this (0 disables). Ablation knob.")
    ap.add_argument("--max-partitions", type=int, default=MAX_PARTITIONS,
                    help="Gate D: reject an identity partition with more "
                         "directories than this (0 disables). Ablation knob.")
    ap.add_argument("--min-partition-bytes", type=int, default=MIN_PARTITION_BYTES,
                    help="Gate D: reject an identity partition leaving less than "
                         "this per directory (0 disables). Ablation knob.")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    bind_catalog(advisor_catalog.from_args(args))

    sysc = _load_json(args.sysconst)
    regime = sysc["regimes"][args.regime]
    vectored = sysc.get("vectored") or {}
    col_stats = load_column_stats(args.column_stats)
    # The workload snapshot carries the medians it was built with; --per-query
    # re-reads them if a longer E2 has landed since.
    medians = dict(catalog.measured_median_s)
    if args.per_query and os.path.exists(args.per_query):
        medians = advisor_policy.load_per_query_medians(args.per_query)
    if not medians:
        raise SystemExit(
            "no measured medians: build the workload snapshot with "
            "--per-query, or pass --per-query here. The execution residual is "
            "calibrated against them and is zero without them.")

    out_dir = args.out or args.analyze_dir
    os.makedirs(out_dir, exist_ok=True)

    if args.validate or args.search or args.rtt_sweep:
        vreport, baseline_ev, vok = run_validate(
            args, regime, vectored, col_stats, medians)
        with open(os.path.join(out_dir, "validate.json"), "w") as fh:
            json.dump(vreport, fh, indent=2)
        print("# validate")
        ge = vreport.get("gets_rel_error")
        be = vreport.get("gib_rel_error")
        print(f"  gets  pred={vreport['predicted_gets']} actual={vreport['actual_gets']} "
              f"err={ge if ge is None else round(ge * 100, 1)}")
        print(f"  GiB   pred={vreport['predicted_gib']} actual={vreport['actual_gib']} "
              f"err={be if be is None else round(be * 100, 1)}")
        print(f"  t_io  {vreport['t_io_s']}s  measured {vreport['measured_median_s']}")
        print(f"  gate  {'PASS' if vok else 'FAIL'}  (≤10%)")
        if args.validate and not (args.search or args.rtt_sweep):
            return 0 if vok else 1

    residual_base, rows_base = calibrate_execution_residual(baseline_ev, medians)
    queries = sorted(catalog.QUERIES)
    candidates = load_candidates(args.analyze_dir, args.grid)
    baseline_cand = next(c for c in candidates if c["candidate_id"] == "baseline")
    baseline_pred = evaluate_workload(
        baseline_cand, queries, medians, col_stats, regime,
        args.empirical_corr, vectored, residual_base, rows_base)

    def slim_row(r, keys):
        return {k: r[k] for k in keys if k in r}

    l0_kwargs = dict(
        col_stats=col_stats,
        empirical_corr=args.empirical_corr,
        cluster_headroom_min=args.cluster_headroom_min,
        prune_parallelism_floor=args.prune_parallelism_floor,
        max_partitions=args.max_partitions,
        min_partition_bytes=args.min_partition_bytes,
    )

    if args.search:
        ranked = []
        n_l0_fail = 0
        n_guardrail_fail = 0
        n_cluster_fail = 0
        n_prune_par_fail = 0
        n_partition_fail = 0
        for cand in candidates:
            ok, viol, _geom = l0_check(cand, **l0_kwargs)
            if not ok:
                n_l0_fail += 1
                if any("already clustered" in v for v in viol):
                    n_cluster_fail += 1
                if any("prune leaves" in v for v in viol):
                    n_prune_par_fail += 1
                if any("partition" in v for v in viol):
                    n_partition_fail += 1
                ranked.append({
                    "candidate_id": cand["candidate_id"],
                    "l0_ok": False,
                    "l0_violations": viol,
                    "t_e2e_s": None,
                    "candidate": cand,
                })
                continue
            ev = evaluate_workload(cand, queries, medians, col_stats, regime,
                                   args.empirical_corr, vectored,
                                   residual_base, rows_base)
            regs_all = query_regressions(ev, baseline_pred["per_query"])
            regressions = [r for r in regs_all if r["regression"] > GUARDRAIL_REGRESSION]
            max_reg = max((r["regression"] for r in regs_all), default=0.0)
            if regressions:
                n_guardrail_fail += 1
            ranked.append({
                "candidate_id": cand["candidate_id"],
                "l0_ok": True,
                "guardrail_ok": not regressions,
                "guardrail_regressions": regressions,
                "n_guardrail_queries": len(regressions),
                "max_regression": round(max_reg, 4),
                "t_io_s": ev["t_io_s"],
                "t_exec_residual_s": ev["t_exec_residual_s"],
                "t_e2e_s": ev["t_e2e_s"],
                "ranged_gets": ev["ranged_gets"],
                "bytes_gib": ev["bytes_gib"],
                "candidate": cand,
            })
        l0_legal = [r for r in ranked if r["l0_ok"]]
        l0_legal.sort(key=lambda r: r["t_e2e_s"])
        legal = [r for r in l0_legal if r.get("guardrail_ok")]
        legal.sort(key=lambda r: r["t_e2e_s"])
        baseline_t = baseline_pred["t_e2e_s"]
        for pool in (l0_legal, legal):
            for r in pool:
                r["delta_vs_baseline"] = round(r["t_e2e_s"] - baseline_t, 3)
                r["improve_frac"] = (
                    round(1.0 - r["t_e2e_s"] / baseline_t, 4) if baseline_t else None)
        # Among L0-legal points that miss §4.1, the nearest is min max-regression
        # then t_e2e. Documents that a global TFS grid may have no improving
        # feasible point (orders/partsupp already coalesce at 128 MB).
        nearest = min(l0_legal, key=lambda r: (
            max(0.0, r["max_regression"] - GUARDRAIL_REGRESSION),
            r["t_e2e_s"]))
        exhaustive_best = legal[0]
        iter_choice, iter_cand = iterative_search(legal)
        regret = ((iter_cand["t_e2e_s"] - exhaustive_best["t_e2e_s"])
                  / exhaustive_best["t_e2e_s"])
        top_keys = ("candidate_id", "t_e2e_s", "t_io_s", "t_exec_residual_s",
                    "ranged_gets", "bytes_gib", "delta_vs_baseline",
                    "improve_frac", "max_regression", "n_guardrail_queries",
                    "guardrail_ok")
        rank_prefix = "ranked_per_table" if args.grid == "per-table" else "ranked"
        search_out = {
            "searched_at": datetime.now(timezone.utc).isoformat(),
            "regime": args.regime,
            "grid": args.grid,
            "empirical_corr": args.empirical_corr,
            "n_candidates": len(candidates),
            "n_l0_fail": n_l0_fail,
            "n_cluster_fail": n_cluster_fail,
            "n_prune_parallelism_fail": n_prune_par_fail,
            "n_partition_fail": n_partition_fail,
            "n_l0_legal": len(l0_legal),
            "n_guardrail_fail": n_guardrail_fail,
            "n_legal": len(legal),
            "baseline_t_e2e_s": baseline_t,
            "cluster_headroom_min": args.cluster_headroom_min,
            "prune_parallelism_floor": args.prune_parallelism_floor,
            "provenance": catalog.provenance(),
            "note": (
                "legal = L0 ∧ §4.1 vs baseline L1. L0 includes Gate A "
                "(rg_span headroom vs what a sort could reach), Gate C "
                "(post-prune RG∧file floor) and Gate D (partition shape). "
                "Per-table grid: every table above the size threshold chooses "
                "file size, sort and partition independently; smaller tables "
                "stay at the engine default."
            ),
            "top10": [slim_row(r, top_keys) for r in legal[:10]],
            "top10_l0": [slim_row(r, top_keys) for r in l0_legal[:10]],
            "exhaustive_best": exhaustive_best["candidate_id"],
            "l0_best": l0_legal[0]["candidate_id"],
            "nearest_guardrail": slim_row(nearest, top_keys),
            "iterative": {
                "choice": iter_choice,
                "candidate_id": iter_cand["candidate_id"],
                "t_e2e_s": iter_cand["t_e2e_s"],
                "regret_vs_exhaustive": round(regret, 4),
            },
        }
        with open(os.path.join(out_dir, f"{rank_prefix}_{args.regime}.json"), "w") as fh:
            json.dump(search_out, fh, indent=2)
        with open(os.path.join(out_dir, f"{rank_prefix}_{args.regime}_full.json"), "w") as fh:
            slim = []
            for r in l0_legal:
                row = {k: r[k] for k in r if k != "candidate"}
                cand = r["candidate"]
                if cand.get("scope") == "per_table":
                    row.update({
                        "li_file": cand.get("li_file"),
                        "li_sort": cand.get("li_sort"),
                        "o_file": cand.get("o_file"),
                        "o_sort": cand.get("o_sort"),
                        "ps_file": cand.get("ps_file"),
                    })
                else:
                    row.update({
                        "partition": cand.get("partition"),
                        "file_label": cand.get("file_label"),
                        "rg_label": cand.get("rg_label"),
                        "sort_label": cand.get("sort_label"),
                    })
                slim.append(row)
            json.dump(slim, fh, indent=2)
        print("# search")
        print(f"  legal {len(legal)} / {len(candidates)}  "
              f"(L0 fail {n_l0_fail} [cluster {n_cluster_fail}, "
              f"prune-par {n_prune_par_fail}, partition {n_partition_fail}], "
              f"§4.1 fail {n_guardrail_fail})")
        print(f"  best  {exhaustive_best['candidate_id']}  "
              f"{exhaustive_best['t_e2e_s']:.1f}s  "
              f"Δ{exhaustive_best['delta_vs_baseline']:.1f}s")
        print(f"  l0-best {l0_legal[0]['candidate_id']}  "
              f"{l0_legal[0]['t_e2e_s']:.1f}s  "
              f"max_reg={l0_legal[0]['max_regression']*100:.0f}%")
        print(f"  nearest §4.1 {nearest['candidate_id']}  "
              f"max_reg={nearest['max_regression']*100:.0f}%")
        print(f"  iter  {iter_cand['candidate_id']}  regret={regret*100:.2f}%")
        for r in legal[:5]:
            print(f"    {r['t_e2e_s']:8.1f}s  {r['candidate_id']}")
        rec_path = os.path.join(out_dir, f"recommended_{args.grid.replace('-', '_')}.json")
        with open(rec_path, "w") as fh:
            json.dump({
                "candidate_id": exhaustive_best["candidate_id"],
                "scope": exhaustive_best["candidate"].get("scope"),
                "actions": exhaustive_best["candidate"].get("actions") or [],
                "tables": exhaustive_best["candidate"].get("tables"),
            }, fh, indent=2)
        print(f"  wrote {rec_path}")

    if args.rtt_sweep:
        recs = {}
        ranks = {}

        def uniq_id(cid):
            return cid.replace("_s-l_shipdate_l_suppkey", "_s-l_shipdate")

        for rname, rgm in sysc["regimes"].items():
            base_ev = evaluate_workload(
                baseline_cand, queries, medians, col_stats, rgm,
                args.empirical_corr, vectored, residual_base, rows_base)
            legal = []
            for cand in load_candidates(args.analyze_dir, args.grid):
                ok, _v, _g = l0_check(cand, **l0_kwargs)
                if not ok:
                    continue
                ev = evaluate_workload(cand, queries, medians, col_stats, rgm,
                                       args.empirical_corr, vectored,
                                       residual_base, rows_base)
                if guardrail_regressions(ev, base_ev["per_query"]):
                    continue
                legal.append((ev["t_e2e_s"], cand["candidate_id"], ev))
            legal.sort()
            seen, order = set(), []
            for t, cid, ev in legal:
                uid = uniq_id(cid)
                if uid in seen:
                    continue
                seen.add(uid)
                order.append(uid)
            ranks[rname] = {cid: i for i, cid in enumerate(order)}
            recs[rname] = {
                "rtt_s": rgm["rtt_s"],
                "best": legal[0][1] if legal else None,
                "best_t_e2e_s": legal[0][0] if legal else None,
                "top3": [c[1] for c in legal[:3]],
                "n_unique": len(order),
                "n_legal": len(legal),
            }
        changed = recs["measured_cross_cloud"]["best"] != recs["same_region_m5d"]["best"]
        ra, rb = ranks["measured_cross_cloud"], ranks["same_region_m5d"]
        common = [k for k in ra if k in rb]
        n = len(common)
        d2 = sum((ra[k] - rb[k]) ** 2 for k in common)
        spearman = 1 - 6 * d2 / (n * (n * n - 1)) if n > 2 else None
        max_shift = max(abs(ra[k] - rb[k]) for k in common) if common else 0
        sweep = {
            "regimes": recs,
            "recommendation_changed": changed,
            "spearman_unique": round(spearman, 4) if spearman is not None else None,
            "max_rank_shift": max_shift,
            "note": (
                "§4.1 is applied per regime against that regime's baseline L1. "
                "If both winners are baseline, the global grid has no improving "
                "feasible point; RTT cannot create one."
            ),
        }
        sweep_name = ("rtt_sweep_per_table.json" if args.grid == "per-table"
                      else "rtt_sweep.json")
        with open(os.path.join(out_dir, sweep_name), "w") as fh:
            json.dump(sweep, fh, indent=2)
        print("# rtt-sweep")
        for name, rec in recs.items():
            print(f"  {name:24s} RTT={rec['rtt_s']*1e3:.1f}ms  best={rec['best']}")
        print(f"  changed {changed}  spearman={sweep['spearman_unique']}  "
              f"max_shift={max_shift}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
