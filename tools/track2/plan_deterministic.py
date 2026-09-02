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

def plan_page_geometry(catalog, notes):
    """Two rules over request shape; neither fires by default.

    Returns (page_bytes, page_row_limit), either possibly None meaning "leave
    the writer default alone" -- which is the right answer when the workload
    shows neither symptom.
    """
    shape = catalog.REQUEST_SHAPE or {}
    tiny = shape.get("tiny_get_fraction") or 0.0
    per_chunk = shape.get("requests_per_chunk_touched") or 1.0

    if per_chunk > policy.REQUESTS_PER_CHUNK_HIGH:
        # The reader is issuing several ranges per column chunk, so it is
        # skipping inside chunks. Finer pages give the OffsetIndex more places
        # to cut, which is only useful because the page index is pinned on.
        page = min(policy.PAGE_BYTES_LADDER)
        notes.append(f"page size -> {page // 1024} KiB: {per_chunk} requests per "
                     f"chunk touched exceeds {policy.REQUESTS_PER_CHUNK_HIGH}, so "
                     f"the reader is already sub-dividing chunks")
        return page, None
    if tiny > policy.TINY_GET_FRACTION_HIGH:
        # RTT-bound: most requests are too small to amortise a round trip.
        # Bigger pages mean fewer, fatter ranges.
        page = max(policy.PAGE_BYTES_LADDER)
        notes.append(f"page size -> {page // 1024} KiB: {tiny * 100:.0f}% of GETs "
                     f"are sub-64KiB, so the regime is RTT-bound")
        return page, None
    notes.append("page geometry left at the writer default: neither the "
                 "tiny-GET nor the chunk-splitting rule fired")
    return None, None


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


def plan_encoding(catalog, table, notes):
    """Per-column encoding from physical type and NDV.

    Only proposes an encoding the type actually supports -- L0 would reject the
    rest, but more importantly the writer would silently fall back to PLAIN and
    the experiment would read as "encoding does not help".
    """
    actions = []
    stats = ((catalog.COLUMN_STATS.get(table) or {}).get("columns") or {})
    n_rows = (catalog.COLUMN_STATS.get(table) or {}).get("n_rows")
    chosen = {}
    for column, rec in stats.items():
        ptype = rec.get("physical_type")
        ndv = rec.get("ndv")
        if not ptype:
            continue
        encoding = None
        if ndv and n_rows and ndv / max(n_rows, 1) < policy.DICTIONARY_NDV_FRACTION:
            encoding = "RLE_DICTIONARY"
        elif ptype in {"INT32", "INT64"}:
            encoding = "DELTA_BINARY_PACKED"
        elif ptype in {"FLOAT", "DOUBLE"}:
            encoding = "BYTE_STREAM_SPLIT"
        elif ptype == "BYTE_ARRAY":
            encoding = "DELTA_BYTE_ARRAY"
        if not encoding:
            continue
        allowed = la.ENCODING_PHYSICAL_TYPES.get(encoding)
        if allowed and ptype not in allowed:
            continue
        chosen[column] = encoding
        actions.append({"canonical": la.ENCODING_COLUMN_PREFIX + column,
                        "value": encoding, "table": table})
    if chosen:
        families = sorted(set(chosen.values()))
        notes.append(f"{table}: encoding set on {len(chosen)} column(s) "
                     f"({', '.join(families)}); UC1-only, Spark drops all but "
                     f"the dictionary switch")
    else:
        notes.append(f"{table}: no physical types in the snapshot; encoding "
                     f"left to the writer")
    return actions


# ------------------------------------------------------- L1 candidate axes

def order_options(catalog, table, enabled, notes):
    """Baseline schema order versus one seriation. L1 picks between them."""
    options = [("baseline", None)]
    if not enabled:
        return options
    order = plan_column_order(catalog, table, notes)
    baseline = list(catalog.ALL_COLUMNS.get(table) or [])
    if order and order != baseline:
        options.append(("seriation", order))
    elif order:
        notes.append(f"{table}: seriation equals the baseline order; nothing to search")
    return options


def compression_options(catalog, table, probe, enabled, notes):
    """Baseline codec versus measured alternatives.

    Without a probe there is no byte ratio, so a zstd action would be priced
    as a no-op. The axis then stays at baseline rather than inventing a gain.
    """
    options = [("baseline", [])]
    if not enabled:
        return options
    columns = ((probe or {}).get(table) or {}).get("columns") or {}
    if not columns:
        notes.append(f"{table}: no compression probe; compression axis stays "
                     f"at baseline (L1 will not invent a ratio)")
        return options
    options.append(("global-zstd", [
        {"canonical": la.COMPRESSION, "value": "zstd", "table": table}]))
    per_column = plan_compression(catalog, table, probe, notes)
    if per_column:
        options.append(("per-column", per_column))
    return options


def choose_axes(catalog, table, fixed_actions, regime, vectored, probe, dims,
                notes):
    """Price the searched axes with L1 and keep the cheapest legal point.

    Searched: column order, compression, file size, row-group size.
    `fixed_actions` carry page/encoding, which are still rule picks.
    """
    patterns = catalog.patterns_for(table)
    if not patterns:
        return [], None

    orders = order_options(catalog, table, "column_order" in dims, notes)
    codecs = compression_options(catalog, table, probe, "compression" in dims, notes)
    files = catalog.file_options(table) if "file_size" in dims else [("baseline", None)]
    rgs = catalog.rg_options(table) if "row_group" in dims else [("baseline", None)]

    best, best_t, tried, rejected = None, None, 0, 0
    for order_label, order in orders:
        for codec_label, codec_actions in codecs:
            for file_label, file_bytes in files:
                for rg_label, rg_bytes in rgs:
                    actions = list(fixed_actions) + list(codec_actions)
                    if order:
                        actions.append({"canonical": la.COLUMN_ORDER,
                                        "value": list(order), "table": table})
                    if file_bytes:
                        actions.append({"canonical": la.TARGET_FILE_SIZE,
                                        "value": int(file_bytes), "table": table})
                    if rg_bytes:
                        actions.append({"canonical": la.ROW_GROUP_SIZE,
                                        "value": int(rg_bytes), "table": table})
                    cand = {
                        "candidate_id": (
                            f"{table}-{order_label}-{codec_label}-"
                            f"{file_label}-{rg_label}"),
                        "actions": actions,
                    }
                    ok, _viol, _geom = whatif.l0_check(cand)
                    if not ok:
                        rejected += 1
                        continue
                    tried += 1
                    ev = whatif.evaluate_workload(cand, patterns, regime, vectored)
                    if best_t is None or ev["t_io_s"] < best_t:
                        best, best_t = (order_label, codec_label, file_label,
                                        rg_label, actions, ev), ev["t_io_s"]

    if not best:
        notes.append(f"{table}: no legal L1 point "
                     f"(rejected {rejected}); leaving searched axes at baseline")
        return [], None

    order_label, codec_label, file_label, rg_label, actions, ev = best
    notes.append(
        f"{table}: L1 chose order={order_label} compression={codec_label} "
        f"file={file_label} rg={rg_label} over {tried} legal point(s) "
        f"({rejected} L0-rejected), t_io={best_t:.1f}s")
    rec = {
        "winner": {
            "column_order": order_label,
            "compression": codec_label,
            "file": file_label,
            "row_group": rg_label,
        },
        "t_io_s": ev["t_io_s"],
        "ranged_gets": ev["ranged_gets"],
        "bytes_gib": ev["bytes_gib"],
        "n_legal": tried,
        "n_l0_rejected": rejected,
        "n_order": len(orders),
        "n_compression": len(codecs),
        "n_file": len(files),
        "n_rg": len(rgs),
    }
    # Extras (page/encoding) stay on the candidate for pricing; the returned
    # actions are only the four searched axes so they can be concatenated
    # across tables without duplicating a global page action.
    extras = set(id(a) for a in fixed_actions)
    searched = [a for a in actions if id(a) not in extras]
    return searched, rec


SEARCHED_AXES = ("column_order", "compression", "file_size", "row_group")


# -------------------------------------------------------------------- plan

def build_plan(catalog, regime, vectored, probe=None, dimensions=None,
               plan_id=None):
    """Assemble one plan across every table with observed traffic."""
    dims = set(dimensions or {"column_order", "row_group", "file_size",
                              "compression", "page", "encoding"})
    notes = []
    extras = []
    evidence_tables = catalog.tables_observed() or [catalog.largest_table()]

    # Page and encoding are still rule picks. They ride along as extras so L0
    # and the page-split term see them, but they are not search axes.
    if "page" in dims:
        page_bytes, page_rows = plan_page_geometry(catalog, notes)
        if page_bytes:
            extras.append({"canonical": la.PAGE_SIZE, "value": int(page_bytes)})
        if page_rows:
            extras.append({"canonical": la.PAGE_ROW_LIMIT, "value": int(page_rows)})
    for table in evidence_tables:
        if "encoding" in dims:
            extras.extend(plan_encoding(catalog, table, notes))

    actions = list(extras)
    search = {}
    if set(SEARCHED_AXES) & dims:
        for table in evidence_tables:
            picked, rec = choose_axes(
                catalog, table, extras, regime, vectored, probe, dims, notes)
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

    probe = None
    if args.compression_probe and os.path.exists(args.compression_probe):
        with open(args.compression_probe) as fh:
            probe = json.load(fh).get("tables")
        # L1 prices a codec change only against a measurement; handing the same
        # probe to virtual_footer is what lets choose_axes see that compression
        # moved the bytes it is sizing files against.
        vf.probe = probe

    all_dims = ["column_order", "row_group", "file_size", "compression",
                "page", "encoding"]
    plans = [build_plan(catalog, regime, vectored, probe, args.dimensions,
                        args.plan_id)]
    if args.ablation:
        for dim in all_dims:
            plans.append(build_plan(catalog, regime, vectored, probe, {dim},
                                    plan_id=f"only-{dim}"))

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
