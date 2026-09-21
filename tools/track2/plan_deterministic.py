#!/usr/bin/env python3
"""First layer: generate a file-level layout plan from SDK bytes + footers.

*** This is the main plan generator (TRACK2_V2_PLAN.md §8.1). ***

Input is an access profile (what got read, and what got read together) and a
dataset snapshot (where the bytes are). Output is an engine-neutral plan in the
six-dimension action space. No query plans, no predicates, no engine: that is
the claim the paper makes, and this file is where it has to hold.

Four dimensions are priced together by L1; two stay rules. A rule states
what evidence it consumed, so a plan can be argued with, and the four-axis
search is small enough that the winner is still one of those rule-generated
alternatives rather than an invented point. Page geometry and encoding stay
out of the search until they have a cost model worth comparing. `explain`
records which field moved each decision.

Dimension by dimension:

  column order  Seriation over the co-access matrix, then L1 vs schema order.
                Columns read together get adjacent so the reader's vectored
                merge turns two ranges into one; cold columns sink to the
                tail. This is the only dimension whose evidence *needs*
                episodes, and the only one v1 could not express at all.
  row group     Ladder from measured geometry, priced by L1.
  file size     Same, bounded below by the parallelism floor.
  compression   Baseline vs global zstd vs per-column (from a probe), priced
                by L1. Without a probe the axis stays at baseline.
  page geometry Two rules keyed on request shape: RTT-bound workloads want
                bigger pages, chunk-splitting workloads want smaller ones.
  encoding      Per-column, from physical type and NDV. UC1-only.

Usage:
  python3 tools/track2/plan_deterministic.py \
      --dataset-snapshot .../dataset_snapshot.json \
      --access-profile .../access_profile.json \
      --sysconst .../sysconst.json \
      --out plans/hits-v2-001.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import advisor_catalog  # noqa: E402
import advisor_policy as policy  # noqa: E402
import layout_actions as la  # noqa: E402
import virtual_footer as vf  # noqa: E402
import whatif  # noqa: E402


# --------------------------------------------------------------- column order

def seriate(columns, coaccess, weights, noise_floor=policy.COACCESS_NOISE_FLOOR):
    """Order columns so co-accessed ones are adjacent.

    Optimal seriation is a Hamiltonian-path problem, so this is the standard
    greedy substitute: start from the heaviest column, repeatedly append the
    unplaced column with the strongest link to the one just placed, and when
    nothing is linked (a new cluster) restart from the heaviest column left.
    That "restart from the heaviest" step is what produces clusters without a
    separate clustering pass -- a column with no co-access link begins a new
    contiguous block rather than being wedged between two unrelated ones.

    Weak links are dropped first. With hundreds of columns a single stray
    episode otherwise chains two unrelated clusters together, and the resulting
    order is worse than the baseline because it interleaves them.
    """
    if not columns:
        return []
    strongest = max(coaccess.values()) if coaccess else 0
    cutoff = strongest * noise_floor
    remaining = set(columns)
    order = []

    def heaviest(pool):
        return max(pool, key=lambda c: (weights.get(c, 0.0), c))

    current = heaviest(remaining)
    while remaining:
        order.append(current)
        remaining.discard(current)
        if not remaining:
            break
        best, best_w = None, 0.0
        for candidate in remaining:
            w = coaccess.get((current, candidate), 0)
            if w > cutoff and w > best_w:
                best, best_w = candidate, w
        current = best if best is not None else heaviest(remaining)
    return order


def plan_column_order(catalog, table, notes):
    """Hot seriated prefix, then cold columns, then anything unobserved."""
    schema = list(catalog.ALL_COLUMNS.get(table) or [])
    if not schema:
        return None
    read = [c for c in catalog.profile.columns_read(table) if c in set(schema)]
    if len(read) < 2:
        notes.append(f"{table}: fewer than two columns observed; column order left alone")
        return None

    coaccess = catalog.coaccess_matrix(table)
    weights = {c: r.get("episodes", 0)
               for c, r in (catalog.COLUMN_WEIGHT.get(table) or {}).items()}
    hot = seriate(read, coaccess, weights)
    cold = [c for c in schema if c not in set(hot)]
    notes.append(f"{table}: seriated {len(hot)} read columns over "
                 f"{len(coaccess) // 2} co-access pairs; {len(cold)} cold "
                 f"columns placed at the tail")
    return hot + cold


# ----------------------------------------------------------- page geometry

def page_options(catalog, table, enabled, notes):
    """Baseline page geometry versus the ladder points the writer honours.

    The two rules this used to be -- "RTT-bound workloads want bigger pages,
    chunk-splitting workloads want smaller ones" -- are gone, and their
    replacement is the layout probe plus L1. The chunk-splitting rule was
    keyed on a symptom this workload does not have, and both were priced by a
    square-root proxy that had no measurement behind it in either direction.

    What is priced now is the OffsetIndex, in bytes the probe measured per
    page. A coarser page buys fewer index bytes, a finer page costs more, and
    a size the writer ignores is dropped. Nothing here claims a *skipping*
    benefit for finer pages: that would need to know which pages a predicate
    eliminates, and predicates are the input this advisor does not read.
    """
    options = [("baseline", None)]
    if not enabled:
        return options
    rec = vf.page_probe(table)
    if not rec:
        notes.append(f"{table}: page axis stays at baseline; the layout probe "
                     f"has no resolved page pass (run compression_probe "
                     f"without --skip-page on a large enough sample)")
        return options
    noop = set(rec.get("noop_page_bytes") or ())
    dropped = []
    for page_bytes in sorted(policy.PAGE_BYTES_LADDER, reverse=True):
        if page_bytes in noop:
            dropped.append(page_bytes)
            continue
        options.append((f"{page_bytes // 1024}KiBpage", page_bytes))
    if dropped:
        notes.append(f"{table}: page point(s) "
                     f"{[b // 1024 for b in dropped]} KiB dropped; the writer "
                     f"returned a byte-identical file for them")
    notes.append(f"{table}: page index measured at "
                 f"{rec.get('index_bytes_per_page')} B/page over "
                 f"{rec.get('pages_per_column')} pages/column; the axis is "
                 f"priced in scan-metadata bytes only, never as a GET change")
    return options


# ------------------------------------------------------ compression/encoding

def plan_compression(catalog, table, probe, notes):
    """Per-column codec from measured compressibility, global codec as fallback.

    Without a probe this proposes the global codec only. Proposing per-column
    zstd on a guess would be the kind of unfalsifiable recommendation the whole
    pipeline exists to avoid.
    """
    actions = []
    ratios = ((probe or {}).get(table) or {}).get("columns") or {}
    if not ratios:
        notes.append(f"{table}: no compression probe; proposing global "
                     f"{policy.CODEC_LADDER[-1]} only")
        return [{"canonical": la.COMPRESSION, "value": policy.CODEC_LADDER[-1],
                 "table": table}]

    incompressible = []
    for column, rec in ratios.items():
        best = rec.get("best_codec")
        ratio = rec.get("best_ratio")
        if ratio is not None and ratio > policy.INCOMPRESSIBLE_RATIO:
            incompressible.append(column)
            actions.append({"canonical": la.COMPRESSION_COLUMN_PREFIX + column,
                            "value": "uncompressed", "table": table})
        elif best and best != policy.BASELINE_CODEC:
            actions.append({"canonical": la.COMPRESSION_COLUMN_PREFIX + column,
                            "value": best, "table": table})
    if incompressible:
        notes.append(f"{table}: {len(incompressible)} column(s) compress worse "
                     f"than {policy.INCOMPRESSIBLE_RATIO}; codec turned off for "
                     f"them so the CPU is not spent for nothing")
    return actions


def _cheapest_tuple(rec, margin=0.02):
    """Cheapest measured (codec, encoding) for one column, or None.

    Recomputed here rather than trusted from the probe document so a plan can
    be built from a probe written by an older run of the measurement.
    """
    points = [p for p in (rec.get("joint") or [])
              if p.get("ratio") is not None and p.get("codec") in policy.CODEC_LADDER]
    if not points:
        return None
    best = min(points, key=lambda p: p["ratio"])
    if best["ratio"] > 1.0 - margin:
        return None
    return {"codec": best["codec"], "encoding": best["encoding"],
            "ratio": best["ratio"]}


def plan_joint_codec_encoding(probe, table, notes):
    """Per-column (codec, encoding) from the probe's measured joint winners.

    The pair is chosen together because it was measured together. A column
    whose dictionary already collapses it to a few values leaves the codec
    almost nothing to do, so the cheapest codec at the default encoding and
    the cheapest encoding at the default codec are frequently not the cheapest
    pair -- and their ratios cannot be multiplied to find out.
    """
    columns = ((probe or {}).get(table) or {}).get("columns") or {}
    actions = []
    families, codecs = set(), set()
    for column, rec in sorted(columns.items()):
        best = rec.get("best_joint") or _cheapest_tuple(rec)
        if not best:
            continue
        codec, encoding = best["codec"], best["encoding"]
        if codec != policy.BASELINE_CODEC:
            actions.append({"canonical": la.COMPRESSION_COLUMN_PREFIX + column,
                            "value": codec, "table": table})
            codecs.add(codec)
        if encoding != "baseline":
            allowed = la.ENCODING_PHYSICAL_TYPES.get(encoding)
            if allowed and rec.get("physical_type") not in allowed:
                continue
            actions.append({"canonical": la.ENCODING_COLUMN_PREFIX + column,
                            "value": encoding, "table": table})
            families.add(encoding)
    if actions:
        notes.append(f"{table}: joint codec/encoding on {len(actions)} "
                     f"action(s) over {len(columns)} probed column(s); "
                     f"codecs {sorted(codecs) or ['baseline']}, encodings "
                     f"{sorted(families) or ['baseline']}; UC1-only")
    return actions


# ------------------------------------------------------- L1 candidate axes

def _seriation_addressable(catalog, table, order, notes):
    """True only when multi-column traffic exists and merge_gets improves."""
    shape = catalog.REQUEST_SHAPE or {}
    multi = shape.get("multicol_data_span_share")
    if multi is not None and multi < policy.SERIATION_MULTICOL_MIN:
        notes.append(
            f"{table}: seriation skipped; multi-column data spans "
            f"{multi:.0%} < {policy.SERIATION_MULTICOL_MIN:.0%}")
        return False
    patterns = [p for p in catalog.patterns_for(table) if p.get("n_columns", 0) > 1]
    if not patterns:
        notes.append(f"{table}: seriation skipped; no multi-column patterns")
        return False
    min_seek = ((catalog.profile.doc.get("vectored") or {}).get("min_seek_bytes")
                or 131072)
    max_merged = ((catalog.profile.doc.get("vectored") or {}).get("max_merged_bytes")
                  or 2097152)
    base_cand = {"candidate_id": "baseline", "actions": []}
    ser_cand = {"candidate_id": "seriation", "actions": [
        {"canonical": la.COLUMN_ORDER, "value": list(order), "table": table}]}
    saved = 0.0
    for pattern in patterns:
        bg, _ = vf.merge_gets(table, pattern["columns"],
                              vf.predict_geometry(table, base_cand),
                              min_seek, max_merged,
                              order=vf.column_order_for(table, base_cand))
        sg, _ = vf.merge_gets(table, pattern["columns"],
                              vf.predict_geometry(table, ser_cand),
                              min_seek, max_merged,
                              order=vf.column_order_for(table, ser_cand))
        rg = pattern.get("rg_touched_total") or (
            (pattern.get("rg_per_episode") or 1) * pattern.get("n_episodes", 0))
        saved += max(0.0, bg - sg) * rg
    if saved <= 0:
        notes.append(f"{table}: seriation skipped; merge_gets found no "
                     f"addressable GET reduction on multi-column patterns")
        return False
    notes.append(f"{table}: seriation addressable ~{saved:.0f} RG-GET reduction "
                 f"on {len(patterns)} multi-column pattern(s)")
    return True


def order_options(catalog, table, enabled, notes):
    """Baseline schema order versus one seriation. L1 picks between them."""
    options = [("baseline", None)]
    if not enabled:
        return options
    order = plan_column_order(catalog, table, notes)
    baseline = list(catalog.ALL_COLUMNS.get(table) or [])
    if order and order != baseline and _seriation_addressable(
            catalog, table, order, notes):
        options.append(("seriation", order))
    elif order and order == baseline:
        notes.append(f"{table}: seriation equals the baseline order; nothing to search")
    return options


def compression_options(catalog, table, probe, dims, notes):
    """Baseline versus the measured codec and codec+encoding alternatives.

    Compression and encoding share one axis because the probe measures them as
    one point. Splitting them into two independent axes would let the search
    combine a codec from one measurement with an encoding from another and
    price the pair as if the two effects added up.

    Without a probe there is no ratio at all, so the axis stays at baseline
    rather than inventing a gain.
    """
    options = [("baseline", [])]
    want_codec = "compression" in dims
    want_encoding = "encoding" in dims
    if not (want_codec or want_encoding):
        return options
    columns = ((probe or {}).get(table) or {}).get("columns") or {}
    if not columns:
        notes.append(f"{table}: no layout probe; compression/encoding axis "
                     f"stays at baseline (L1 will not invent a ratio)")
        return options
    if want_encoding and vf.probe_schema_version < 2:
        notes.append(f"{table}: probe is schema v{vf.probe_schema_version}; "
                     f"encoding needs joint (codec, encoding) measurements, "
                     f"so the encoding half stays at baseline")
        want_encoding = False
    if want_codec:
        options.append(("global-zstd", [
            {"canonical": la.COMPRESSION, "value": "zstd", "table": table}]))
        per_column = plan_compression(catalog, table, probe, notes)
        if per_column:
            options.append(("per-column-codec", per_column))
    if want_encoding:
        joint = plan_joint_codec_encoding(probe, table, notes)
        if joint:
            options.append(("joint-codec-encoding", joint))
    return options


def _unpriced_for(table, actions, writer):
    """Actions the winner carries that no measurement backs."""
    try:
        rendered = la.render(actions, table=table)
    except ValueError:
        return []
    if writer == "parquet-mr":
        rendered = la.strip_for_spark(rendered)
    return vf.unpriced_encodings(table, rendered)


def choose_axes(catalog, table, regime, vectored, probe, dims, notes,
                writer="pyarrow"):
    """Price every searched axis with L1 and keep the cheapest legal point.

    All six dimensions are candidate axes now, and each one carries its
    baseline value as an option. Page geometry and encoding used to be picked
    by a rule *before* the search and pinned onto every point, which meant
    nothing ever compared them against leaving them alone.

    Compression and encoding travel as one axis because the probe measures
    them as one point; see `compression_options`.
    """
    patterns = catalog.patterns_for(table)
    if not patterns:
        return [], None

    orders = order_options(catalog, table, "column_order" in dims, notes)
    codecs = compression_options(catalog, table, probe, dims, notes)
    files = catalog.file_options(table) if "file_size" in dims else [("baseline", None)]
    rgs = catalog.rg_options(table) if "row_group" in dims else [("baseline", None)]
    pages = page_options(catalog, table, "page" in dims, notes)

    best, best_t, tried, rejected = None, None, 0, 0
    for order_label, order in orders:
        for codec_label, codec_actions in codecs:
            for page_label, page_bytes in pages:
                for file_label, file_bytes in files:
                    for rg_label, rg_bytes in rgs:
                        actions = list(codec_actions)
                        if order:
                            actions.append({"canonical": la.COLUMN_ORDER,
                                            "value": list(order), "table": table})
                        if page_bytes:
                            actions.append({"canonical": la.PAGE_SIZE,
                                            "value": int(page_bytes), "table": table})
                        if file_bytes:
                            actions.append({"canonical": la.TARGET_FILE_SIZE,
                                            "value": int(file_bytes), "table": table})
                        if rg_bytes:
                            actions.append({"canonical": la.ROW_GROUP_SIZE,
                                            "value": int(rg_bytes), "table": table})
                        cand = {
                            "candidate_id": (
                                f"{table}-{order_label}-{codec_label}-"
                                f"{page_label}-{file_label}-{rg_label}"),
                            "actions": actions,
                        }
                        # Judge legality against the renderer that will
                        # actually write this, or the search hands UC2 a plan
                        # that strip_for_spark quietly turns into a different
                        # layout.
                        ok, _viol, _geom = whatif.l0_check(cand, writer=writer)
                        if not ok:
                            rejected += 1
                            continue
                        tried += 1
                        ev = whatif.evaluate_workload(cand, patterns, regime,
                                                      vectored)
                        if best_t is None or ev["t_io_s"] < best_t:
                            best = (order_label, codec_label, page_label,
                                    file_label, rg_label, actions, ev)
                            best_t = ev["t_io_s"]

    if not best:
        notes.append(f"{table}: no legal L1 point "
                     f"(rejected {rejected}); leaving searched axes at baseline")
        return [], None

    order_label, codec_label, page_label, file_label, rg_label, actions, ev = best

    if page_label != "baseline":
        # The page axis only wins on metadata bytes, and the other half of the
        # trade -- how precisely a predicate can skip pages, how coarse the
        # decode unit gets -- needs a query plan L1 does not have. Moving the
        # axis for a saving smaller than that blind spot would be pricing
        # noise, so it has to clear a margin against leaving pages alone.
        without = [a for a in actions if a["canonical"] != la.PAGE_SIZE]
        alt = whatif.evaluate_workload(
            {"candidate_id": "no-page", "actions": without},
            patterns, regime, vectored)
        gain = (alt["t_io_s"] - ev["t_io_s"]) / max(alt["t_io_s"], 1e-9)
        if gain < policy.PAGE_SWITCH_MARGIN:
            notes.append(
                f"{table}: page={page_label} dropped; it saves only "
                f"{gain * 100:.2f}% of t_io, under the "
                f"{policy.PAGE_SWITCH_MARGIN:.0%} margin, and L1 cannot price "
                f"the page-skipping side of the trade")
            actions, ev, page_label = without, alt, "baseline"

    unpriced = _unpriced_for(table, actions, writer)
    if unpriced:
        # Degradation, not refusal: an unmeasured column drops back to the
        # baseline tuple and is listed, so the plan is still executable and
        # the gap is visible. Refusing the whole candidate would empty the
        # search the moment the probe had one hole.
        drop = {u["column"] for u in unpriced}
        actions = [a for a in actions
                   if not (a["canonical"].startswith(la.ENCODING_COLUMN_PREFIX)
                           and a["canonical"][len(la.ENCODING_COLUMN_PREFIX):] in drop)
                   and not (a["canonical"].startswith(la.COMPRESSION_COLUMN_PREFIX)
                            and a["canonical"][len(la.COMPRESSION_COLUMN_PREFIX):] in drop)]
        notes.append(f"{table}: {len(drop)} column(s) dropped back to the "
                     f"baseline codec/encoding tuple; no joint measurement "
                     f"covers what the search asked for")
        ev = whatif.evaluate_workload(
            {"candidate_id": "repriced", "actions": actions},
            patterns, regime, vectored)

    notes.append(
        f"{table}: L1 chose order={order_label} compression={codec_label} "
        f"page={page_label} file={file_label} rg={rg_label} over {tried} legal "
        f"point(s) ({rejected} L0-rejected), t_io={ev['t_io_s']:.1f}s")
    rec = {
        "winner": {
            "column_order": order_label,
            "compression": codec_label,
            "page": page_label,
            "file": file_label,
            "row_group": rg_label,
        },
        "writer": writer,
        "t_io_s": ev["t_io_s"],
        "ranged_gets": ev["ranged_gets"],
        "bytes_gib": ev["bytes_gib"],
        "page_index_bytes": ev.get("page_index_bytes"),
        "n_legal": tried,
        "n_l0_rejected": rejected,
        "n_order": len(orders),
        "n_compression": len(codecs),
        "n_page": len(pages),
        "n_file": len(files),
        "n_rg": len(rgs),
        "unpriced": unpriced,
    }
    return actions, rec


SEARCHED_AXES = ("column_order", "compression", "encoding", "page",
                 "file_size", "row_group")


# -------------------------------------------------------------------- plan

def build_plan(catalog, regime, vectored, probe=None, dimensions=None,
               plan_id=None, writer="pyarrow"):
    """Assemble one plan across every table with observed traffic."""
    dims = set(dimensions or {"column_order", "row_group", "file_size",
                              "compression", "page", "encoding"})
    notes = []
    evidence_tables = catalog.tables_observed() or [catalog.largest_table()]

    actions = []
    search = {}
    if set(SEARCHED_AXES) & dims:
        for table in evidence_tables:
            picked, rec = choose_axes(
                catalog, table, regime, vectored, probe, dims, notes,
                writer=writer)
            actions.extend(picked)
            if rec:
                search[table] = rec

    shape = catalog.REQUEST_SHAPE or {}
    return {
        "plan_id": plan_id or f"deterministic-{'-'.join(sorted(dims))}",
        "generator": "deterministic",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md r5",
        "target": {"tables": evidence_tables},
        "dimensions": sorted(dims),
        "evidence": {
            "source": ["sdk_io", "parquet_footer"],
            "episodes": catalog.profile.n_episodes,
            "patterns": len(catalog.PATTERNS),
            "tiny_get_fraction": shape.get("tiny_get_fraction"),
            "requests_per_chunk_touched": shape.get("requests_per_chunk_touched"),
        },
        "search": search,
        "writer": writer,
        "unpriced": {t: rec.get("unpriced") for t, rec in search.items()
                     if rec.get("unpriced")},
        "actions": actions,
        "constraints": {
            "format": "parquet",
            "page_index": "required_on",
            "readable_by": ["parquet-mr", "pyarrow"],
        },
        "explain": notes,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    advisor_catalog.add_arguments(ap)
    ap.add_argument("--sysconst", required=True)
    ap.add_argument("--regime", default="measured_cross_cloud")
    ap.add_argument("--compression-probe", default=None,
                    help="compression_probe.py output; without it the "
                         "compression axis stays at baseline")
    ap.add_argument("--dimensions", nargs="*", default=None,
                    choices=("column_order", "row_group", "file_size",
                             "compression", "page", "encoding"),
                    help="restrict the plan to these dimensions (E-B ablation)")
    ap.add_argument("--writer", choices=("pyarrow", "parquet-mr"),
                    default="pyarrow",
                    help="renderer the plan must be executable by; L0 filters "
                         "candidates against its capability matrix during the "
                         "search, not after it")
    ap.add_argument("--ablation", action="store_true",
                    help="also emit one single-dimension plan per dimension")
    ap.add_argument("--plan-id", default=None)
    ap.add_argument("--out", required=True,
                    help="plan JSON path, or a directory when --ablation is set")
    args = ap.parse_args()

    whatif.bind_catalog(advisor_catalog.from_args(args))
    catalog = whatif.catalog
    with open(args.sysconst) as fh:
        sysc = json.load(fh)
    regime = sysc["regimes"][args.regime]
    vectored = sysc.get("vectored") or {}

    # Hard gate: a planner that cannot replay the baseline it was built from
    # is not allowed to rank candidates. E-0 used to skip this and emit a
    # plan whose L1 numbers were already 2× the measured GET count.
    out_dir = args.out if args.ablation or os.path.isdir(args.out) else (
        os.path.dirname(os.path.abspath(args.out)) or ".")
    os.makedirs(out_dir, exist_ok=True)
    vreport, _ev, vok = whatif.run_validate(regime, vectored, catalog.profile.doc)
    validate_path = os.path.join(out_dir, "validate.json")
    with open(validate_path, "w") as fh:
        json.dump(vreport, fh, indent=2)
    ge, be = vreport.get("gets_rel_error"), vreport.get("gib_rel_error")
    print("# L1 self-consistency")
    print(f"  gets  pred={vreport['predicted_gets']} "
          f"observed={vreport['observed_gets']} "
          f"err={ge if ge is None else round(ge * 100, 1)}%")
    print(f"  GiB   pred={vreport['predicted_gib']} "
          f"observed={vreport['observed_gib']} "
          f"err={be if be is None else round(be * 100, 1)}%")
    print(f"  gate  {'PASS' if vok else 'FAIL'}  (<= {whatif.VALIDATE_TOL * 100:.0f}%)")
    print(f"  wrote {validate_path}")
    if not vok:
        print("  STOP: fix collection or L1 before emitting a plan")
        return 1

    probe = None
    if args.compression_probe and os.path.exists(args.compression_probe):
        with open(args.compression_probe) as fh:
            doc = json.load(fh)
        # L1 prices a codec, an encoding or a page size only against a
        # measurement. bind_probe also records the probe's schema version, so
        # a v1 document cannot be read as if it carried joint tuples.
        vf.bind_probe(doc)
        probe = vf.probe
        if vf.probe_schema_version < 2:
            print(f"  note  probe schema v{vf.probe_schema_version}: "
                  f"encoding and page axes stay at baseline")

    all_dims = ["column_order", "row_group", "file_size", "compression",
                "page", "encoding"]
    plans = [build_plan(catalog, regime, vectored, probe, args.dimensions,
                        args.plan_id, writer=args.writer)]
    if args.ablation:
        for dim in all_dims:
            plans.append(build_plan(catalog, regime, vectored, probe, {dim},
                                    plan_id=f"only-{dim}", writer=args.writer))

    written = []
    if args.ablation or os.path.isdir(args.out):
        os.makedirs(args.out, exist_ok=True)
        for plan in plans:
            path = os.path.join(args.out, f"{plan['plan_id']}.json")
            with open(path, "w") as fh:
                json.dump(plan, fh, indent=2)
            written.append(path)
    else:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(plans[0], fh, indent=2)
        written.append(args.out)

    head = plans[0]
    problems = la.validate_plan(head, table=catalog.largest_table())
    print(f"# deterministic plan: {head['plan_id']}")
    print(f"  actions   {len(head['actions'])} over {head['dimensions']}")
    for note in head["explain"]:
        print(f"    - {note}")
    if problems:
        print("  SELF-CHECK FAILED:")
        for problem in problems:
            print(f"    ! {problem}")
    for path in written:
        print(f"  wrote     {path}")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
