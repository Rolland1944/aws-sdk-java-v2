#!/usr/bin/env python3
"""Virtual footer: predict Parquet geometry without rewriting the data.

This is the lakehouse stand-in for DB2's virtual indexes. A candidate exists
only as predicted metadata -- file count, row-group count, per-column chunk
sizes and the physical order they sit in -- and L1 prices the reader's request
plan against that.

r5 removed the pruning half of this module. `prune_fraction`, the CDF
selectivity estimate and the partition geometry all existed to answer "how many
row groups can a predicate skip", and predicates left with the Semantic layer.
What is left is the part that never needed a query plan: where the bytes are,
and how the reader's range merging walks over them.

That makes `merge_gets` the centre of the model rather than a detail of it. It
is now order-aware: it takes the candidate's column order instead of reading
the baseline's, which is what lets a reordering be priced at all.
"""

from __future__ import annotations

import math

# Set by whatif.bind_catalog before any pricing happens. This is an
# AdvisorCatalog: measured geometry plus the observed access profile.
catalog = None


def table_bytes(table):
    return catalog.BASELINE_GEOMETRY[table]["compressed_bytes"]


def layout_for(table, candidate):
    """Resolve the six-dimension layout for one table.

    A candidate arrives in one of two shapes and both have to work. A *plan*
    carries canonical `actions`, which is what a planner emits and what a
    renderer writes; a *search point* carries a pre-resolved `tables` dict,
    which is what the ladder search builds thousands of and does not want to
    re-render each time. Pricing a plan without rendering its actions is how a
    column-order plan silently costs exactly what the baseline costs, so the
    action path is the default and the dict is the override.
    """
    # Actions are the source of truth for a plan. `tables` overlays a few
    # already-resolved fields (file/rg bytes) so a search loop does not have
    # to re-render every point; it must not wipe compression or column order
    # that only exist as actions.
    spec = {}
    if candidate.get("actions"):
        spec.update(_from_actions(candidate["actions"], table))
    # Overlay only keys that are actually set. A search point used to carry
    # file/rg bytes in `tables` and nothing else; a bare `update` would wipe
    # action-derived compression and column order with missing keys.
    for key, value in ((candidate.get("tables") or {}).get(table) or {}).items():
        if value is not None:
            spec[key] = value

    rg_bytes = spec.get("rg_bytes")
    file_bytes = spec.get("file_bytes")
    if rg_bytes is None and file_bytes:
        rg_bytes = catalog.BASELINE_RG_BYTES
    order = spec.get("column_order")
    if order:
        known = set(catalog.ALL_COLUMNS.get(table) or [])
        order = [c for c in order if c in known] or None
    return {
        "file_bytes": file_bytes,
        "rg_bytes": rg_bytes,
        "column_order": order,
        "page_bytes": spec.get("page_bytes"),
        "compression": spec.get("compression"),
        "compression_ratio": spec.get("compression_ratio"),
        "column_ratios": spec.get("column_ratios"),
    }


def _from_actions(actions, table):
    """Canonical actions -> the resolved-layout dict this module prices."""
    import layout_actions

    try:
        rendered = layout_actions.render(actions, table=table)
    except ValueError:
        return {}
    return {
        "file_bytes": rendered.target_file_size,
        "rg_bytes": rendered.row_group_size,
        "column_order": rendered.column_order,
        "page_bytes": rendered.page_size,
        "compression": rendered.compression,
        # Per-column codec changes are priced only when a measurement says by
        # how much (compression_probe). Without one the codec is recorded and
        # the bytes are left alone, which understates the action rather than
        # inventing a ratio for it.
        "column_ratios": compression_ratios(table, rendered),
    }


# Set by whatif/plan_deterministic when a compression probe is available:
# {table: {column: {"ratios": {codec: ratio}, ...}}}
probe = None


def compression_ratios(table, rendered):
    """Per-column byte scaling implied by a codec change, if it was measured."""
    if not probe:
        return None
    columns = ((probe.get(table) or {}).get("columns") or {})
    if not columns:
        return None
    out = {}
    for column, rec in columns.items():
        codec = rendered.column_compression.get(column) or rendered.compression
        if not codec:
            continue
        ratios = rec.get("ratios") or {}
        baseline = ratios.get("snappy")
        target = ratios.get(codec)
        if baseline and target:
            out[column] = target / baseline
    return out or None


def column_order_for(table, candidate):
    """The physical order a candidate would write, defaulting to the baseline."""
    lay = layout_for(table, candidate)
    if lay["column_order"]:
        order = list(lay["column_order"])
        tail = [c for c in (catalog.COLUMN_ORDER.get(table) or [])
                if c not in set(order)]
        return order + tail
    return list(catalog.COLUMN_ORDER.get(table) or [])


def predict_geometry(table, candidate):
    """Scale file/RG counts from the measured baseline."""
    base = catalog.BASELINE_GEOMETRY[table]
    base_files = base["files"]
    # Row groups per file is not an integer in practice: Spark's output averages
    # 1.5. The snapshot carries the measured total, so use it rather than
    # rounding a ratio and multiplying the error by the file count.
    base_rg = base.get("n_rg") or int(round(base["files"] * base["rg_per_file"]))
    base_rg_b = base["rg_bytes"]  # mean uncompressed; base_rg * this = total
    lay = layout_for(table, candidate)

    # Compression changes the byte total, hence the file count for a given
    # target size. The ratio is measured by a sampling pass (compression_probe),
    # not assumed; absent a measurement this stays 1.0 and the candidate is
    # priced at baseline bytes.
    ratio = lay.get("compression_ratio")
    if ratio is None:
        ratio = table_compression_ratio(table, lay.get("column_ratios"))
    compressed = table_bytes(table) * ratio

    file_bytes = lay["file_bytes"]
    n_files = max(1, int(math.ceil(compressed / file_bytes))) if file_bytes else base_files

    rg_bytes = lay["rg_bytes"]
    if rg_bytes:
        n_rg = max(1, int(round(base_rg * (base_rg_b / rg_bytes))))
    else:
        n_rg = base_rg
    n_rg = max(n_rg, n_files)  # at least one RG per file
    rg_per_file = max(1, int(math.ceil(n_rg / n_files)))
    n_rg = n_files * rg_per_file
    return {
        "table": table,
        "n_files": n_files,
        "n_rg": n_rg,
        "rg_per_file": rg_per_file,
        "compressed_bytes": compressed,
        "compression_ratio": ratio,
        "rg_compressed": compressed / n_rg,
        "page_bytes": lay.get("page_bytes"),
    }


def table_compression_ratio(table, column_ratios):
    """Per-column ratios folded into one table ratio, weighted by byte share."""
    if not column_ratios:
        return 1.0
    shares = catalog.COLUMN_SHARE.get(table) or {}
    total = sum(shares.values())
    if not total:
        return 1.0
    scaled = sum(shares.get(c, 0) * column_ratios.get(c, 1.0) for c in shares)
    return scaled / total


def chunk_bytes(table, column, geom, ratio_override=None):
    """Bytes one column occupies inside one row group.

    Byte share comes from the measured footers. A per-column codec change
    scales that column's share only, which is the whole reason per-column
    compression is worth expressing: it moves bytes without moving anything
    else.
    """
    shares = catalog.COLUMN_SHARE.get(table)
    order = catalog.COLUMN_ORDER.get(table) or [column]
    if shares and column in shares:
        total = sum(shares.get(c, 0) for c in order)
        frac = shares[column] / total if total else 1.0 / len(order)
    else:
        frac = 1.0 / max(len(order), 1)
    size = geom["rg_compressed"] * frac
    if ratio_override:
        size *= ratio_override
    return size


def merge_gets(table, columns, geom, min_seek=131072, max_merged=2097152,
               order=None, column_ratios=None):
    """Apply the frozen vectored-IO merge rules to one row group's projection.

    The reader issues one range per run of wanted columns, splitting a run that
    exceeds max_merged and bridging a skipped column only when that column is
    smaller than min_seek (seeking costs more than reading through it).

    `order` is the candidate's physical column order. Passing it is what makes
    a reordering visible to the cost model: the same projection over the same
    bytes costs fewer requests when the wanted columns are adjacent, and the
    wasted bytes of a bridged gap shrink when the cold columns are elsewhere.

    Returns (n_gets, total_bytes) for one row group, where total_bytes includes
    bytes read through a bridged gap -- those are transferred and paid for.
    """
    order = order or catalog.COLUMN_ORDER.get(table) or list(columns)
    wanted = set(columns)
    ratios = column_ratios or {}
    n_gets = 0
    total = 0.0
    run = []
    run_bytes = 0.0

    def flush():
        nonlocal n_gets, total, run, run_bytes
        if not run:
            return
        # split the run into GETs of at most max_merged bytes
        acc = 0.0
        pieces = 1
        for b in run:
            if acc and acc + b > max_merged:
                pieces += 1
                acc = b
            else:
                acc += b
        n_gets += pieces
        total += run_bytes
        run, run_bytes = [], 0.0

    for col in order:
        b = chunk_bytes(table, col, geom, ratios.get(col))
        if col in wanted:
            if run and run_bytes + b > max_merged and b > max_merged:
                flush()
            run.append(b)
            run_bytes += b
        else:
            # a skipped column. Bridging it costs its bytes; seeking past it
            # costs a request. The frozen reader bridges only below min_seek.
            if run:
                if b > min_seek:
                    flush()
                else:
                    run.append(b)
                    run_bytes += b
    flush()
    return n_gets, total


def page_split_factor(geom, baseline_page_bytes=1048576):
    """How the page size scales the request count for one merged range.

    Smaller pages mean finer OffsetIndex granularity, so the reader can skip
    more precisely -- but a range that used to be one request becomes several
    when the pages inside it are no longer contiguous in the wanted set. This
    is a first-order proxy, not a page-level model: it moves the request count
    in the observed direction with page size and leaves bytes alone. The real
    effect needs page-range intersection, which correlate.py does not do yet.
    """
    page = geom.get("page_bytes")
    if not page or page == baseline_page_bytes:
        return 1.0
    return max(0.25, min(4.0, (baseline_page_bytes / page) ** 0.5))
