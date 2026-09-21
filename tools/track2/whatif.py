#!/usr/bin/env python3
"""EVALUATE side: L0 static checks + L1 analytical cost over a virtual footer.

DB2 Design Advisor compiles each candidate in EVALUATE mode against the
optimizer. We don't have that optimizer, so a candidate is a virtual footer and
the cost is the frozen Reader's request plan:

    t_io = Σ_pattern (RTT × requests + bytes / BW) / K_eff
           data GETs scale with RG accesses × merge_gets
           scan meta scales with scan units, file meta with file count
           K_eff = min(K_busy, n_scan_units, local[N])

r5 simplified this in one structural way worth stating plainly. v1's actions
(sort, partition) changed *which rows* a query read, so the model needed a
selectivity estimate, and it needed an execution residual rescaled by rows to
convert I/O savings into end-to-end time. None of the six v2 actions change the
row set: every candidate reads exactly the same data, just laid out
differently. So the residual is a constant across candidates, ranking by t_io
is ranking by t_e2e, and the whole CDF/prune apparatus is gone.

That simplification has one hole, recorded rather than papered over: a codec
change moves decode CPU, which lands in the residual and is *not* constant.
`advisor_policy.DECODE_MODELLED = False` marks it, and a compression candidate
that wins on predicted bytes alone should be treated as a hypothesis until E-0
measures it.

L0 keeps the checks that still have referents -- writer/reader capability,
readable row-group size, structural fidelity, parallelism floor -- and adds the
three r5 needs: a column order must be a permutation, a codec must be readable,
an encoding must match the physical type.

Usage:
  python3 tools/track2/whatif.py --search --validate \
      --sysconst .../sysconst.json --plans .../plans \
      --dataset-snapshot .../dataset_snapshot.json \
      --access-profile .../access_profile.json \
      --regime measured_cross_cloud
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import advisor_catalog  # noqa: E402
import layout_actions as la  # noqa: E402
import virtual_footer as vf  # noqa: E402
from advisor_policy import (  # noqa: E402
    DECODE_RATE_SCALE, FOOTER_BYTES, GEOMETRY_TOL, GUARDRAIL_REGRESSION,
    HEAD_PER_OPEN, LARGE_TABLE_BYTES, MAX_READABLE_RG_BYTES,
    META_GETS_PER_OPEN, PARALLELISM, VALIDATE_TOL)
from layout_actions import check_l0 as check_actions_l0  # noqa: E402
from layout_actions import render, strip_for_spark  # noqa: E402

# The measured catalog. bind_catalog installs it here and in virtual_footer.
catalog = None


def bind_catalog(cat):
    global catalog
    catalog = cat
    vf.catalog = cat
    return cat


def _primary_table():
    return max(catalog.BASELINE_GEOMETRY,
               key=lambda t: catalog.BASELINE_GEOMETRY[t]["compressed_bytes"])


def physical_types(table):
    """column -> Parquet physical type, for the encoding compatibility check."""
    cols = ((catalog.COLUMN_STATS.get(table) or {}).get("columns") or {})
    return {name: rec.get("physical_type") for name, rec in cols.items()
            if rec.get("physical_type")}


# ------------------------------------------------------------------------- L0

def l0_check(candidate, source_bytes=None, writer="pyarrow"):
    """Contract 5.3 as revised by r5, plus the vectored-timeout readability bound."""
    violations = []
    actions = candidate.get("actions") or []

    for table in catalog.BASELINE_GEOMETRY:
        try:
            rendered = render(actions, table=table)
        except ValueError as exc:
            return False, [str(exc)], None
        table_b = (source_bytes if table == _primary_table() and source_bytes
                   else vf.table_bytes(table))
        # Tiny tables cannot satisfy "several row groups"; keep monotonicity only.
        structural = table_b if table_b >= LARGE_TABLE_BYTES else None
        # Capability is judged on what the plan actually says, before
        # strip_for_spark drops what Spark cannot express. Checking the
        # stripped rendering would pass every candidate for parquet-mr and
        # then hand UC2 a layout L1 priced but the renderer will not write.
        violations.extend(check_actions_l0(
            rendered, structural, schema=catalog.ALL_COLUMNS.get(table),
            physical_types=physical_types(table), writer=writer))
        if writer == "parquet-mr":
            rendered = strip_for_spark(rendered)

        rg = vf.layout_for(table, candidate)["rg_bytes"]
        if rg and rg > MAX_READABLE_RG_BYTES:
            violations.append(
                f"{table} requested row-group {rg} B exceeds readable bound "
                f"{MAX_READABLE_RG_BYTES} B (parquet vectored wait is hardcoded "
                f"300s; the M2 canary's 35-182 MiB ranges timed out)")

    primary = _primary_table()
    geom = vf.predict_geometry(primary, candidate)
    if geom["n_files"] < 2:
        violations.append(f"{primary} files {geom['n_files']} < 2")
    if geom["n_rg"] < 4:
        violations.append(f"{primary} row groups {geom['n_rg']} < 4")
    if geom["n_files"] > 20000:
        violations.append(f"{primary} files {geom['n_files']} exceed rewrite budget")

    # Parallelism floor: each large table on its own predicted file count.
    for table, base in catalog.BASELINE_GEOMETRY.items():
        if base["compressed_bytes"] < LARGE_TABLE_BYTES:
            continue
        g = vf.predict_geometry(table, candidate)
        floor = min(PARALLELISM, base["files"])
        if g["n_files"] < floor:
            violations.append(
                f"{table} files {g['n_files']} < parallelism floor {floor} "
                f"(baseline {base['files']})")

    return (not violations), violations, geom


# ------------------------------------------------------------------------- L1

def _file_scale(table, geom):
    """File-discovery metadata scales with the number of objects listed."""
    base_files = max(1, catalog.BASELINE_GEOMETRY[table]["files"])
    return geom["n_files"] / base_files


def _scan_scale(table, geom):
    """Per-task footer/page-index work scales with Spark input splits."""
    base = catalog.BASELINE_GEOMETRY[table]
    base_units = base.get("n_scan_units") or vf.scan_unit_count(
        base.get("file_sizes") or [base.get("compressed_bytes") or 1],
        base.get("split_size_bytes") or vf.SPLIT_SIZE_BYTES)
    cand_units = geom.get("n_scan_units") or vf.scan_unit_count(
        geom.get("file_sizes") or [geom.get("compressed_bytes") or 1],
        geom.get("split_size_bytes") or vf.SPLIT_SIZE_BYTES)
    return cand_units / max(base_units, 1)


def _rg_scale(table, geom):
    base = catalog.BASELINE_GEOMETRY[table]
    base_rg = max(1, base.get("n_rg") or base.get("files") or 1)
    return geom["n_rg"] / base_rg


def _avg_meta_bytes():
    """Bytes per metadata request, from the profile when available."""
    shape = catalog.REQUEST_SHAPE or {}
    n = shape.get("meta_requests") or 0
    b = shape.get("meta_bytes") or 0
    if n and b:
        return b / n
    return float(FOOTER_BYTES)


def _table_open_weights():
    """Baseline open weight per table, used to apportion metadata-only GETs."""
    weights = {}
    for pattern in catalog.PATTERNS:
        table = pattern.get("table")
        if table in catalog.BASELINE_GEOMETRY:
            weights[table] = weights.get(table, 0.0) + pattern.get("n_episodes", 0)
    return weights


def evaluate_pattern(pattern, candidate, regime, vectored):
    """Price one access pattern from measured totals, not per-open cancellation.

    Data GETs = measured_data × (merge_gets_cand / merge_gets_base) × (n_rg_cand / n_rg_base).
    Scan-span metadata scales with Spark input splits. File-discovery metadata
    is priced globally, not here.
    """
    table = pattern["table"]
    geom = vf.predict_geometry(table, candidate)
    order = vf.column_order_for(table, candidate)
    lay = vf.layout_for(table, candidate)
    min_seek = vectored.get("min_seek_bytes", 131072)
    max_merged = vectored.get("max_merged_bytes", 2097152)

    gets_per_rg, bytes_per_rg = vf.merge_gets(
        table, pattern["columns"], geom, min_seek, max_merged,
        order=order, column_ratios=lay.get("column_ratios"))

    measured_data = pattern.get("data_requests")
    if measured_data is None:
        measured_data = (pattern.get("requests_per_episode") or 0) * pattern.get(
            "n_episodes", 0)
    measured_bytes = pattern.get("data_bytes")
    if measured_bytes is None:
        measured_bytes = (pattern.get("bytes_per_episode") or 0) * pattern.get(
            "n_episodes", 0)

    base_cand = {"candidate_id": "baseline", "actions": []}
    base_geom_v = vf.predict_geometry(table, base_cand)
    base_order = vf.column_order_for(table, base_cand)
    base_lay = vf.layout_for(table, base_cand)
    bg, bb = vf.merge_gets(
        table, pattern["columns"], base_geom_v, min_seek, max_merged,
        order=base_order, column_ratios=base_lay.get("column_ratios"))
    rg_ratio = _rg_scale(table, geom)
    merge_gets_ratio = (gets_per_rg / bg) if bg else 1.0
    merge_bytes_ratio = (bytes_per_rg / bb) if bb else 1.0
    data_gets = measured_data * merge_gets_ratio * rg_ratio
    data_bytes = measured_bytes * merge_bytes_ratio * rg_ratio

    scan_meta = pattern.get("scan_meta_requests")
    if scan_meta is None:
        scan_meta = (pattern.get("meta_requests_per_episode") or 0) * pattern.get(
            "n_episodes", 0)
    scan_meta *= _scan_scale(table, geom)
    scan_meta_bytes = scan_meta * _avg_meta_bytes()
    heads = float(HEAD_PER_OPEN) * pattern.get("n_episodes", 0) * _scan_scale(
        table, geom)

    n_ep = pattern["n_episodes"]
    rtt = regime["rtt_s"]
    bw = regime["bw_bps"]
    k_busy = regime.get("K_busy") or 1.0
    k = max(1.0, min(k_busy, geom.get("n_scan_units") or geom["n_files"],
                     PARALLELISM))
    requests = data_gets + scan_meta + heads
    nbytes = data_bytes + scan_meta_bytes
    t_io = (requests * rtt + (nbytes / bw if bw else 0.0)) / k

    decode_core_s, decode_missing = _decode_core_s(pattern, table, candidate)
    # Decode gets its own divisor. K_busy is average *IO* concurrency while a
    # request is in flight, and it sits near 7 rather than 16 precisely
    # because tasks spend part of their life decoding instead of waiting on
    # S3. Dividing decode by that same number would charge the CPU term for
    # the reader's IO idleness twice over.
    k_decode = max(1.0, min(geom.get("n_scan_units") or geom["n_files"],
                            PARALLELISM))
    t_decode = (decode_core_s / k_decode) if decode_core_s is not None else None

    return {
        "pattern_id": pattern["pattern_id"],
        "table": table,
        "n_episodes": n_ep,
        "n_columns": len(pattern["columns"]),
        "gets_per_rg": round(gets_per_rg, 3),
        "rg_ratio": round(rg_ratio, 4),
        "data_gets": data_gets,
        "data_bytes": data_bytes,
        "scan_meta_gets": scan_meta,
        "file_meta_gets": 0.0,
        "meta_gets": scan_meta,
        "meta_bytes": scan_meta_bytes,
        "heads": heads,
        "k_eff": round(k, 2),
        "t_io_s": t_io,
        # Core-seconds is the calibratable quantity: it is what the event
        # log's Executor CPU Time measures. t_decode_s is that divided by a
        # parallelism assumption, so the two are reported separately and only
        # the first is ever checked against a measurement.
        "decode_core_s": decode_core_s,
        "k_decode": round(k_decode, 2),
        "t_decode_s": t_decode,
        "decode_missing_columns": decode_missing,
    }


def _decode_core_s(pattern, table, candidate):
    """Core-seconds this pattern spends decoding, or None when unpriced.

    The scan count is `rg_touched_total / n_rg_baseline`: how many times over
    the pattern reads its column set. Taken against the *baseline* row-group
    count on purpose. The candidate's own count cancels -- touches scale up
    with n_rg while bytes per row group scale down -- which is the arithmetic
    form of decode work depending on rows read rather than on how they are
    grouped.
    """
    plan, _unpriced = vf.decode_plan(table, candidate)
    if not plan:
        return None, []
    base = catalog.BASELINE_GEOMETRY[table]
    base_rg = max(1, base.get("n_rg") or base.get("files") or 1)
    touches = pattern.get("rg_touched_total")
    if not touches:
        touches = pattern.get("n_episodes", 0) * base_rg
    per_scan, missing = vf.decode_core_s_per_scan(
        table, pattern["columns"], plan)
    if per_scan is None:
        return None, missing
    return per_scan * (touches / base_rg), missing


def _selected_tables(patterns):
    selected = {
        pattern["table"] for pattern in patterns
        if pattern.get("table") in catalog.BASELINE_GEOMETRY
    } or {_primary_table()}
    weights = _table_open_weights()
    denom = sum(weights.get(t, 0.0) for t in selected)
    if not denom:
        denom = float(len(selected))
        weights = {t: 1.0 for t in selected}
    return selected, weights, denom


def _scale_meta_pool(patterns, candidate, regime, leftover, scale_fn):
    if leftover <= 0:
        return 0.0, 0.0, 0.0, 0.0
    selected, weights, denom = _selected_tables(patterns)
    rtt = regime["rtt_s"]
    bw = regime["bw_bps"]
    k_busy = regime.get("K_busy") or 1.0
    total_gets = total_bytes = total_heads = total_io = 0.0
    for table in selected:
        geom = vf.predict_geometry(table, candidate)
        share = leftover * (weights.get(table, 0.0) / denom) * scale_fn(table, geom)
        scaled_bytes = share * _avg_meta_bytes()
        scaled_heads = (share / max(META_GETS_PER_OPEN, 1)) * HEAD_PER_OPEN
        k = max(1.0, min(k_busy, geom.get("n_scan_units") or geom["n_files"],
                         PARALLELISM))
        total_gets += share
        total_bytes += scaled_bytes
        total_heads += scaled_heads
        total_io += ((share + scaled_heads) * rtt +
                     (scaled_bytes / bw if bw else 0.0)) / k
    return total_gets, total_bytes, total_heads, total_io


def _patterned_scan_meta():
    patterned = 0.0
    for pattern in catalog.PATTERNS:
        if pattern.get("table") not in catalog.BASELINE_GEOMETRY:
            continue
        if pattern.get("scan_meta_requests") is not None:
            patterned += pattern["scan_meta_requests"]
        else:
            patterned += ((pattern.get("meta_requests_per_episode") or 0) *
                          pattern.get("n_episodes", 0))
    return patterned


def _file_meta(patterns, candidate, regime):
    """File-listing footer discovery, scaled with object count."""
    shape = catalog.REQUEST_SHAPE or {}
    leftover = shape.get("file_meta_requests")
    if leftover is None and shape.get("scan_meta_requests") is None:
        # Pre-span profile: leftover unpatterned meta used to scale with files.
        leftover = max(0.0, (shape.get("meta_requests") or 0) - _patterned_scan_meta())
        return _scale_meta_pool(patterns, candidate, regime, leftover, _file_scale)
    if leftover is None:
        leftover = 0.0
    return _scale_meta_pool(patterns, candidate, regime, leftover, _file_scale)


def _unpatterned_scan_meta(patterns, candidate, regime):
    """Scan-span metadata that never joined a column set, scaled with splits."""
    shape = catalog.REQUEST_SHAPE or {}
    if shape.get("file_meta_requests") is None and shape.get("scan_meta_requests") is None:
        return 0.0, 0.0, 0.0, 0.0
    observed = shape.get("scan_meta_requests")
    if observed is None:
        observed = max(0.0, (shape.get("meta_requests") or 0) -
                       (shape.get("file_meta_requests") or 0))
    leftover = max(0.0, observed - _patterned_scan_meta())
    return _scale_meta_pool(patterns, candidate, regime, leftover, _scan_scale)


def _page_index_bytes(patterns, candidate, regime):
    """OffsetIndex/ColumnIndex bytes the page axis moves on the scan path.

    Bytes only: the page index rides inside the metadata range the reader
    already issues, so changing the page size makes that range fatter or
    thinner without adding or removing a round trip.

    Charged per pattern and per row-group open, over that pattern's own
    columns. A pattern reading two columns pays for two columns' worth of
    index, however many the table has.
    """
    bw = regime["bw_bps"]
    k_busy = regime.get("K_busy") or 1.0
    total_bytes = total_io = 0.0
    for pattern in patterns:
        table = pattern["table"]
        if table not in catalog.BASELINE_GEOMETRY:
            continue
        geom = vf.predict_geometry(table, candidate)
        lay = vf.layout_for(table, candidate)
        delta = vf.page_index_delta_bytes(
            table, geom, pattern["columns"], lay.get("column_ratios"))
        if not delta:
            continue
        rg_touches = ((pattern.get("rg_touched_total") or 0)
                      * _rg_scale(table, geom))
        scaled = delta * rg_touches
        k = max(1.0, min(k_busy, geom.get("n_scan_units") or geom["n_files"],
                         PARALLELISM))
        total_bytes += scaled
        total_io += (scaled / bw if bw else 0.0) / k
    return total_bytes, total_io


def evaluate_workload(candidate, patterns, regime, vectored, residual_s=0.0):
    per_pattern = []
    sum_io = 0.0
    sum_data = sum_scan = sum_file = sum_bytes = sum_heads = 0.0
    sum_decode_core = sum_decode = 0.0
    decode_priced = True
    decode_missing = set()
    for pattern in patterns:
        if pattern["table"] not in catalog.BASELINE_GEOMETRY:
            continue
        rec = evaluate_pattern(pattern, candidate, regime, vectored)
        per_pattern.append(rec)
        sum_io += rec["t_io_s"]
        if rec.get("decode_core_s") is None:
            decode_priced = False
        else:
            sum_decode_core += rec["decode_core_s"]
            sum_decode += rec["t_decode_s"]
        decode_missing.update(rec.get("decode_missing_columns") or ())
        sum_data += rec["data_gets"]
        sum_scan += rec.get("scan_meta_gets") or rec["meta_gets"]
        sum_bytes += rec["data_bytes"] + rec["meta_bytes"]
        sum_heads += rec["heads"]
    file_gets, file_bytes, file_heads, file_io = _file_meta(
        patterns, candidate, regime)
    extra_scan, extra_bytes, extra_heads, extra_io = _unpatterned_scan_meta(
        patterns, candidate, regime)
    page_bytes, page_io = _page_index_bytes(patterns, candidate, regime)
    sum_file += file_gets
    sum_scan += extra_scan
    sum_bytes += file_bytes + extra_bytes + page_bytes
    sum_heads += file_heads + extra_heads
    sum_io += file_io + extra_io + page_io
    sum_gets = sum_data + sum_scan + sum_file
    # A pattern whose decode could not be priced makes the *total* unpriced.
    # Summing the patterns that happened to be measurable would report a
    # smaller CPU bill for the candidate with more unmeasured tuples, which
    # inverts the term's purpose.
    if decode_missing:
        decode_priced = False
    return {
        "candidate_id": candidate.get("candidate_id") or candidate.get("plan_id"),
        "t_io_s": round(sum_io, 3),
        # t_io_s stays pure transfer so the baseline replay and the written-
        # candidate check keep comparing predictions against measured GETs and
        # bytes. Decode is carried alongside, and t_cost_s is the sum the
        # ranking uses once DECODE_MODELLED says the rates have been checked.
        "decode_core_s": round(sum_decode_core, 3) if decode_priced else None,
        "t_decode_s": round(sum_decode, 3) if decode_priced else None,
        "t_cost_s": (round(sum_io + sum_decode, 3) if decode_priced
                     else round(sum_io, 3)),
        "decode_priced": decode_priced,
        "decode_unpriced_columns": sorted(decode_missing),
        # No v2 action changes the row set, so the residual is the same constant
        # for every candidate and t_e2e ranks identically to t_io. It is carried
        # anyway so the number printed is comparable with a measured wall clock.
        "t_residual_s": round(residual_s, 3),
        "t_e2e_s": round(sum_io + residual_s, 3),
        "ranged_gets": int(round(sum_gets)),
        "data_gets": int(round(sum_data)),
        "file_meta_gets": int(round(sum_file)),
        "scan_meta_gets": int(round(sum_scan)),
        "heads": int(round(sum_heads)),
        "bytes": int(round(sum_bytes)),
        "bytes_gib": round(sum_bytes / 2 ** 30, 3),
        "page_index_bytes": int(round(page_bytes)),
        "per_pattern": per_pattern,
    }


def pattern_regressions(ev, baseline_per_pattern):
    """Per-pattern predicted t_io vs the same L1 on the baseline."""
    base = {r["pattern_id"]: r for r in baseline_per_pattern}
    out = []
    for rec in ev["per_pattern"]:
        b = base.get(rec["pattern_id"])
        if not b or not b["t_io_s"]:
            continue
        out.append({
            "pattern_id": rec["pattern_id"],
            "t_io_s": round(rec["t_io_s"], 3),
            "baseline_t_io_s": round(b["t_io_s"], 3),
            "regression": round(rec["t_io_s"] / b["t_io_s"] - 1.0, 4),
        })
    return out


def guardrail_regressions(ev, baseline_per_pattern, limit=GUARDRAIL_REGRESSION):
    return [r for r in pattern_regressions(ev, baseline_per_pattern)
            if r["regression"] > limit]


# -------------------------------------------------------------- self-consistency

def observed_io(profile_doc):
    """(ranged GETs, GiB) actually recorded, for the L1 replay gate."""
    shape = profile_doc.get("request_shape") or {}
    reqs = (shape.get("data_requests") or 0) + (shape.get("meta_requests") or 0)
    nbytes = (shape.get("data_bytes") or 0) + (shape.get("meta_bytes") or 0)
    return reqs or None, (nbytes / 2 ** 30 if nbytes else None)


def run_validate(regime, vectored, profile_doc):
    """Replaying the baseline must reproduce the GETs and bytes we recorded."""
    baseline = {"candidate_id": "baseline", "actions": []}
    ev = evaluate_workload(baseline, catalog.PATTERNS, regime, vectored)
    obs_gets, obs_gib = observed_io(profile_doc)
    shape = profile_doc.get("request_shape") or {}
    obs_data = shape.get("data_requests")
    obs_file = shape.get("file_meta_requests")
    obs_scan = shape.get("scan_meta_requests")
    if obs_gets and obs_gib:
        gets_err = abs(ev["ranged_gets"] - obs_gets) / obs_gets
        gib_err = abs(ev["bytes_gib"] - obs_gib) / obs_gib
        ok = gets_err <= VALIDATE_TOL and gib_err <= VALIDATE_TOL
    else:
        gets_err = gib_err = None
        ok = True
    return {
        "gate": "L1 self-consistency vs the access profile it was built from",
        "predicted_gets": ev["ranged_gets"],
        "observed_gets": obs_gets,
        "gets_rel_error": None if gets_err is None else round(gets_err, 4),
        "predicted_data_gets": ev.get("data_gets"),
        "observed_data_gets": obs_data,
        "predicted_file_meta_gets": ev.get("file_meta_gets"),
        "observed_file_meta_gets": obs_file,
        "predicted_scan_meta_gets": ev.get("scan_meta_gets"),
        "observed_scan_meta_gets": obs_scan,
        "predicted_gib": ev["bytes_gib"],
        "observed_gib": None if obs_gib is None else round(obs_gib, 3),
        "gib_rel_error": None if gib_err is None else round(gib_err, 4),
        "tolerance": VALIDATE_TOL,
        "pass": ok,
        "t_io_s": ev["t_io_s"],
        "note": None if obs_gets else "no observed IO in the profile; search still runs",
    }, ev, ok


def _rel_err(pred, obs):
    if pred is None or obs in (None, 0):
        return None
    return abs(pred - obs) / obs


def _direction_ok(pred_base, pred_cand, obs_base, obs_cand):
    if None in (pred_base, pred_cand, obs_base, obs_cand):
        return True
    pred_delta = pred_cand - pred_base
    obs_delta = obs_cand - obs_base
    if abs(obs_delta) < max(1.0, 0.02 * max(obs_base, 1)):
        return abs(pred_delta) <= max(1.0, 0.10 * max(obs_base, 1))
    return (pred_delta >= 0) == (obs_delta >= 0)


def run_validate_candidate(regime, vectored, baseline_profile, candidate_profile,
                           candidate):
    """Component-level candidate check, separate from the wall-clock E-0 verdict."""
    base_ev = evaluate_workload(
        {"candidate_id": "baseline", "actions": []},
        catalog.PATTERNS, regime, vectored)
    cand_ev = evaluate_workload(candidate, catalog.PATTERNS, regime, vectored)
    base_shape = (baseline_profile or {}).get("request_shape") or {}
    cand_shape = (candidate_profile or {}).get("request_shape") or {}
    obs_base_data = base_shape.get("data_requests")
    obs_cand_data = cand_shape.get("data_requests")
    obs_base_meta = (base_shape.get("meta_requests") or 0)
    obs_cand_meta = (cand_shape.get("meta_requests") or 0)
    pred_base_meta = (base_ev.get("file_meta_gets") or 0) + (base_ev.get("scan_meta_gets") or 0)
    pred_cand_meta = (cand_ev.get("file_meta_gets") or 0) + (cand_ev.get("scan_meta_gets") or 0)
    data_err = _rel_err(cand_ev.get("data_gets"), obs_cand_data)
    meta_err = _rel_err(pred_cand_meta, obs_cand_meta or None)
    data_dir = _direction_ok(base_ev.get("data_gets"), cand_ev.get("data_gets"),
                             obs_base_data, obs_cand_data)
    meta_dir = _direction_ok(pred_base_meta, pred_cand_meta,
                             obs_base_meta, obs_cand_meta)
    ok = True
    if data_err is not None:
        ok = ok and data_err <= VALIDATE_TOL and data_dir
    if meta_err is not None:
        ok = ok and meta_err <= VALIDATE_TOL and meta_dir
    encoding_priced = any(
        "encoding" in (a.get("canonical") or "")
        for a in (candidate.get("actions") or []))
    notes = []
    if not encoding_priced and meta_err is not None and meta_err > VALIDATE_TOL:
        notes.append(
            "encoding is unpriced; a written layout that changed encodings "
            "can inflate scan-meta GETs (page index / footer) beyond L1")
    if data_err is not None and data_err > VALIDATE_TOL:
        notes.append("data GET relative error exceeds tolerance")
    return {
        "gate": "L1 candidate components vs measured candidate IO",
        "predicted": {
            "data_gets": cand_ev.get("data_gets"),
            "file_meta_gets": cand_ev.get("file_meta_gets"),
            "scan_meta_gets": cand_ev.get("scan_meta_gets"),
            "ranged_gets": cand_ev.get("ranged_gets"),
        },
        "observed": {
            "data_gets": obs_cand_data,
            "meta_gets": obs_cand_meta or None,
            "file_meta_gets": cand_shape.get("file_meta_requests"),
            "scan_meta_gets": cand_shape.get("scan_meta_requests"),
            "ranged_gets": (cand_shape.get("data_requests") or 0) + (
                cand_shape.get("meta_requests") or 0) or None,
        },
        "baseline_predicted": {
            "data_gets": base_ev.get("data_gets"),
            "file_meta_gets": base_ev.get("file_meta_gets"),
            "scan_meta_gets": base_ev.get("scan_meta_gets"),
            "ranged_gets": base_ev.get("ranged_gets"),
        },
        "baseline_observed": {
            "data_gets": obs_base_data,
            "meta_gets": obs_base_meta or None,
        },
        "data_rel_error": None if data_err is None else round(data_err, 4),
        "meta_rel_error": None if meta_err is None else round(meta_err, 4),
        "data_direction_ok": data_dir,
        "meta_direction_ok": meta_dir,
        "tolerance": VALIDATE_TOL,
        "pass": ok,
        "t_io_s": cand_ev["t_io_s"],
        "note": "; ".join(notes) or None,
    }, cand_ev, ok


def ablation_candidates(plan):
    """identity / file-only / order-only / file+order slices of a plan."""
    actions = plan.get("actions") or []
    file_acts = [a for a in actions if a.get("canonical") == la.TARGET_FILE_SIZE]
    order_acts = [a for a in actions if a.get("canonical") == la.COLUMN_ORDER]
    return [
        {"candidate_id": "identity", "actions": []},
        {"candidate_id": "file-only", "actions": list(file_acts)},
        {"candidate_id": "order-only", "actions": list(order_acts)},
        {"candidate_id": "file+order", "actions": list(file_acts) + list(order_acts)},
    ]


def run_l1_ablations(regime, vectored, plan):
    """Model-level single-round ablations. Separate from the wall-clock E-0 gate."""
    table = _primary_table()
    variants = []
    for cand in ablation_candidates(plan):
        ev = evaluate_workload(cand, catalog.PATTERNS, regime, vectored)
        geom = vf.predict_geometry(table, cand)
        variants.append({
            "id": cand["candidate_id"],
            "n_files": geom.get("n_files"),
            "n_rg": geom.get("n_rg"),
            "n_scan_units": geom.get("n_scan_units"),
            "data_gets": ev["data_gets"],
            "file_meta_gets": ev["file_meta_gets"],
            "scan_meta_gets": ev["scan_meta_gets"],
            "ranged_gets": ev["ranged_gets"],
            "bytes_gib": ev["bytes_gib"],
            "t_io_s": ev["t_io_s"],
        })
    by_id = {row["id"]: row for row in variants}
    identity = by_id.get("identity") or {}
    file_only = by_id.get("file-only") or {}
    return {
        "variants": variants,
        "winner": (plan.get("search") or {}).get(table) or {},
        "note": (
            "file-only must not invent a GET drop on the data axis; data GETs "
            f"stay at {identity.get('data_gets')} while scan meta follows "
            f"splits {identity.get('n_scan_units')} -> "
            f"{file_only.get('n_scan_units')}."
        ),
    }


def geometry_matches(predicted, measured, tol=GEOMETRY_TOL):
    """Hard-check a written snapshot against the virtual candidate.

    `n_rg` is exact to one row group: that is the dimension `rg=baseline`
    promises. File count may be off by one because the last file is a
    remainder after RG-granular rotation; beyond that files and compressed
    bytes share `GEOMETRY_TOL`, which is wider than the GET replay
    tolerance because a bound probe is a sample.
    """
    failures = []
    pred_f = predicted.get("n_files")
    meas_f = measured.get("files")
    if meas_f is None:
        meas_f = measured.get("n_files")
    if pred_f and meas_f:
        gap = abs(pred_f - meas_f)
        if gap > 1 and gap / max(pred_f, 1) > tol:
            failures.append(f"files predicted {pred_f} measured {meas_f}")
    pred_rg = predicted.get("n_rg")
    meas_rg = measured.get("n_rg")
    if pred_rg and meas_rg and abs(pred_rg - meas_rg) > 1:
        failures.append(f"n_rg predicted {pred_rg} measured {meas_rg}")
    pred_b = predicted.get("compressed_bytes")
    meas_b = measured.get("compressed_bytes")
    if pred_b and meas_b and abs(pred_b - meas_b) / max(pred_b, 1) > tol:
        failures.append(
            f"compressed_bytes predicted {pred_b} measured {meas_b}")
    return (not failures), failures


def load_plans(path):
    """One plan file, or every *.json in a directory."""
    if os.path.isdir(path):
        out = []
        for name in sorted(glob.glob(os.path.join(path, "*.json"))):
            with open(name) as fh:
                doc = json.load(fh)
            out.extend(doc if isinstance(doc, list) else [doc])
        return out
    with open(path) as fh:
        doc = json.load(fh)
    return doc if isinstance(doc, list) else [doc]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sysconst", required=True)
    ap.add_argument("--plans", default=None,
                    help="plan JSON file or directory of them")
    advisor_catalog.add_arguments(ap)
    ap.add_argument("--regime", default="measured_cross_cloud")
    ap.add_argument("--writer", choices=("pyarrow", "parquet-mr"), default="pyarrow",
                    help="which writer's capability matrix L0 checks against")
    ap.add_argument("--compression-probe", default=None,
                    help="compression_probe.py output; without it a codec change "
                         "is recorded but not priced, because the byte effect "
                         "has not been measured")
    ap.add_argument("--decode-probe", default=None,
                    help="decode_probe.py output; without it the decode term "
                         "is reported as unpriced rather than as zero")
    ap.add_argument("--decode-reader", choices=vf.DECODE_READERS,
                    default="pyarrow",
                    help="which probe's rate table to price decode with. Not "
                         "the same choice as --writer: UC1 means written by "
                         "PyArrow, but both use cases are read by Spark")
    ap.add_argument("--decode-rate-scale", type=float,
                    default=DECODE_RATE_SCALE,
                    help="calibration scalar on the probe's decode rates")
    ap.add_argument("--residual-s", type=float, default=0.0,
                    help="measured wall clock minus predicted baseline IO; a "
                         "constant across v2 candidates, carried only so the "
                         "printed t_e2e is comparable with a benchmark run")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--validate-candidate", action="store_true",
                    help="component-level check against a measured candidate profile")
    ap.add_argument("--candidate-profile", default=None,
                    help="access_profile.py output from the candidate IO trace")
    ap.add_argument("--search", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    bind_catalog(advisor_catalog.from_args(args))

    with open(args.sysconst) as fh:
        sysc = json.load(fh)
    regime = sysc["regimes"][args.regime]
    vectored = sysc.get("vectored") or {}
    profile_doc = catalog.profile.doc
    if args.compression_probe and os.path.exists(args.compression_probe):
        with open(args.compression_probe) as fh:
            vf.bind_probe(json.load(fh))
    if args.decode_probe and os.path.exists(args.decode_probe):
        with open(args.decode_probe) as fh:
            vf.bind_decode_profile(json.load(fh), reader=args.decode_reader,
                                   rate_scale=args.decode_rate_scale)

    out_dir = args.out or os.path.dirname(os.path.abspath(args.access_profile))
    os.makedirs(out_dir, exist_ok=True)

    baseline_ev = None
    if args.validate or args.search:
        vreport, baseline_ev, vok = run_validate(regime, vectored, profile_doc)
        with open(os.path.join(out_dir, "validate.json"), "w") as fh:
            json.dump(vreport, fh, indent=2)
        ge, be = vreport.get("gets_rel_error"), vreport.get("gib_rel_error")
        print("# validate")
        print(f"  gets  pred={vreport['predicted_gets']} "
              f"observed={vreport['observed_gets']} "
              f"err={ge if ge is None else round(ge * 100, 1)}")
        print(f"  GiB   pred={vreport['predicted_gib']} "
              f"observed={vreport['observed_gib']} "
              f"err={be if be is None else round(be * 100, 1)}")
        print(f"  gate  {'PASS' if vok else 'FAIL'}  (<= {VALIDATE_TOL * 100:.0f}%)")
        if not vok:
            print("  STOP: fix collection or L1 before searching candidates")
            return 1
        if args.validate and not args.search and not args.validate_candidate:
            return 0

    if args.validate_candidate:
        if not args.candidate_profile:
            raise SystemExit("--validate-candidate needs --candidate-profile")
        if not args.plans:
            raise SystemExit("--validate-candidate needs --plans")
        with open(args.candidate_profile) as fh:
            cand_profile = json.load(fh)
        plans = load_plans(args.plans)
        if not plans:
            raise SystemExit("no plan in --plans")
        creport, _ev, cok = run_validate_candidate(
            regime, vectored, profile_doc, cand_profile, plans[0])
        with open(os.path.join(out_dir, "candidate_validate.json"), "w") as fh:
            json.dump(creport, fh, indent=2)
        print("# candidate validate")
        print(f"  data  pred={creport['predicted']['data_gets']} "
              f"observed={creport['observed']['data_gets']} "
              f"err={creport.get('data_rel_error')} "
              f"dir={'ok' if creport['data_direction_ok'] else 'FAIL'}")
        print(f"  meta  pred={creport['predicted']['file_meta_gets'] + creport['predicted']['scan_meta_gets']} "
              f"observed={creport['observed']['meta_gets']} "
              f"err={creport.get('meta_rel_error')} "
              f"dir={'ok' if creport['meta_direction_ok'] else 'FAIL'}")
        print(f"  gate  {'PASS' if cok else 'FAIL'}  (<= {VALIDATE_TOL * 100:.0f}%)")
        if creport.get("note"):
            print(f"  note  {creport['note']}")
        if not args.search:
            return 0 if cok else 1

    if not args.search:
        return 0
    if not args.plans:
        raise SystemExit("--search needs --plans")

    plans = load_plans(args.plans)
    baseline_cand = {"candidate_id": "baseline", "actions": []}
    baseline_pred = evaluate_workload(baseline_cand, catalog.PATTERNS, regime,
                                      vectored, args.residual_s)

    ranked = []
    for plan in plans:
        pid = plan.get("plan_id") or plan.get("candidate_id") or "unnamed"
        ok, viol, _geom = l0_check(plan, writer=args.writer)
        if not ok:
            ranked.append({"candidate_id": pid, "l0_ok": False,
                           "l0_violations": viol, "t_io_s": None})
            continue
        ev = evaluate_workload(plan, catalog.PATTERNS, regime, vectored,
                               args.residual_s)
        regs = pattern_regressions(ev, baseline_pred["per_pattern"])
        bad = [r for r in regs if r["regression"] > GUARDRAIL_REGRESSION]
        ranked.append({
            "candidate_id": pid,
            "l0_ok": True,
            "guardrail_ok": not bad,
            "guardrail_regressions": bad,
            "max_regression": round(max((r["regression"] for r in regs), default=0.0), 4),
            "t_io_s": ev["t_io_s"],
            "t_e2e_s": ev["t_e2e_s"],
            "ranged_gets": ev["ranged_gets"],
            "bytes_gib": ev["bytes_gib"],
            "improve_frac": (round(1.0 - ev["t_io_s"] / baseline_pred["t_io_s"], 4)
                             if baseline_pred["t_io_s"] else None),
            "plan": plan,
        })

    legal = [r for r in ranked if r["l0_ok"] and r.get("guardrail_ok")]
    legal.sort(key=lambda r: r["t_io_s"])
    l0_legal = [r for r in ranked if r["l0_ok"]]
    l0_legal.sort(key=lambda r: r["t_io_s"])

    report = {
        "searched_at": datetime.now(timezone.utc).isoformat(),
        "contract": "TRACK2_M0_CONTRACT.md r5",
        "regime": args.regime,
        "writer": args.writer,
        "n_plans": len(plans),
        "n_l0_fail": len(ranked) - len(l0_legal),
        "n_legal": len(legal),
        "baseline_t_io_s": baseline_pred["t_io_s"],
        "baseline_gets": baseline_pred["ranged_gets"],
        "baseline_gib": baseline_pred["bytes_gib"],
        "provenance": catalog.provenance(),
        "decode_modelled": False,
        "note": ("no v2 action changes the row set, so the execution residual is "
                 "constant across candidates and t_io ranks identically to "
                 "t_e2e. The exception is decode CPU under a codec change, "
                 "which L1 does not model."),
        "ranked": [{k: v for k, v in r.items() if k != "plan"} for r in l0_legal],
        "rejected": [r for r in ranked if not r["l0_ok"]],
    }
    path = os.path.join(out_dir, f"whatif_{args.regime}.json")
    with open(path, "w") as fh:
        json.dump(report, fh, indent=2)

    print("# search")
    print(f"  baseline  t_io={baseline_pred['t_io_s']:.1f}s  "
          f"gets={baseline_pred['ranged_gets']}  {baseline_pred['bytes_gib']:.2f}GiB")
    print(f"  legal     {len(legal)} / {len(plans)} "
          f"(L0 fail {report['n_l0_fail']})")
    for r in legal[:5]:
        print(f"    {r['t_io_s']:8.1f}s  {(r['improve_frac'] or 0) * 100:+5.1f}%  "
              f"{r['candidate_id']}")
    for r in ranked:
        if not r["l0_ok"]:
            print(f"    REJECTED {r['candidate_id']}: {r['l0_violations'][0]}")
    print(f"  wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
