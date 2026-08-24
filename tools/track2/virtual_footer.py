#!/usr/bin/env python3
"""Virtual footer: predict Parquet geometry and min/max without rewriting.

This is the lakehouse stand-in for DB2's virtual indexes. A candidate exists
only as predicted metadata: file count, row-group count, and per-RG min/max
derived from the column CDF (sorted prefix) or from the unsorted overlap
assumption (everything else).
"""

from __future__ import annotations

import math

from column_stats import cdf_selectivity

# Set by whatif.bind_catalog / analyze_layout before any pricing happens. This
# is an AdvisorCatalog: measured geometry plus the observed scan catalogue, not
# a hand-written module (see advisor_catalog.py).
catalog = None


def _coerce(cdf_val, pred_val):
    """Make a predicate literal comparable to a CDF bucket value."""
    if pred_val is None or cdf_val is None:
        return pred_val
    if isinstance(cdf_val, str) and not isinstance(pred_val, str):
        return str(pred_val)
    if isinstance(cdf_val, (int, float)) and isinstance(pred_val, str):
        try:
            return type(cdf_val)(pred_val)
        except (TypeError, ValueError):
            return pred_val
    return pred_val


def table_bytes(table):
    return catalog.BASELINE_GEOMETRY[table]["compressed_bytes"]


def layout_for(table, candidate):
    """Resolve file size, RG, sort and partition for one table.

    A candidate with a `tables` dict is per-table (E8 fix): each table keeps
    baseline unless that entry overrides it. Global-grid candidates have no
    `tables` key; file_bytes / sort_columns apply to every table, but a sort
    column that is not on this table is ignored (write_layout already skips it).
    """
    cols = set(catalog.ALL_COLUMNS.get(table, []))
    specs = candidate.get("tables")
    if specs is not None:
        spec = specs.get(table) or {}
        sort = [c for c in (spec.get("sort_columns") or []) if c in cols]
        file_bytes = spec.get("file_bytes")
        rg_bytes = spec.get("rg_bytes")
        if rg_bytes is None and file_bytes:
            rg_bytes = catalog.BASELINE_RG_BYTES
        return {
            "file_bytes": file_bytes,
            "rg_bytes": rg_bytes,
            "sort_columns": sort,
            "sort_ndv": spec.get("sort_ndv"),
            "partition": spec.get("partition") or "none",
            "partition_n": spec.get("partition_n"),
        }
    sort = [c for c in (candidate.get("sort_columns") or []) if c in cols]
    return {
        "file_bytes": candidate.get("file_bytes"),
        "rg_bytes": candidate.get("rg_bytes"),
        "sort_columns": sort,
        "sort_ndv": candidate.get("sort_ndv"),
        "partition": candidate.get("partition") or "none",
        "partition_n": candidate.get("partition_n"),
    }


def partition_spec(lay):
    """(column, transform, n_partitions) for a resolved layout, or None.

    Identity partitions carry `partition_n` from the runtime NDV, because the
    directory count *is* the cardinality. Derived transforms (`col:year`) keep
    the old fixed counts and are never treated as pruning -- Spark cannot infer
    `year(l_shipdate) = 1995` from a predicate on `l_shipdate`.
    """
    partition = lay.get("partition") or "none"
    if not partition or partition == "none":
        return None
    column, _, transform = str(partition).partition(":")
    transform = transform or "identity"
    if transform == "identity":
        n_parts = lay.get("partition_n") or 1
    elif transform == "year":
        n_parts = 7  # TPC-H spans 1992-1998
    elif transform == "month":
        n_parts = 7 * 12
    else:
        n_parts = 7
    return column, transform, max(1, int(n_parts))


def predict_geometry(table, candidate):
    """Scale file/RG counts from the measured baseline by F and R."""
    base = catalog.BASELINE_GEOMETRY[table]
    base_files = base["files"]
    # Row groups per file is not an integer in practice: Spark's lineitem
    # output averages 1.5. The snapshot carries the measured total, so use it
    # rather than rounding a ratio and multiplying the error by the file count.
    base_rg = base.get("n_rg") or int(round(base["files"] * base["rg_per_file"]))
    base_rg_b = base["rg_bytes"]  # mean uncompressed; base_rg * this = total
    lay = layout_for(table, candidate)

    file_bytes = lay["file_bytes"]
    rg_bytes = lay["rg_bytes"]

    if file_bytes:
        n_files = max(1, int(math.ceil(table_bytes(table) / file_bytes)))
    else:
        n_files = base_files

    # A global sort is written with repartitionByRange, which cannot produce
    # more non-empty ranges than the prefix has distinct values. Measured:
    # ClickBench EventDate has 17 distinct dates, so asking for 110 files
    # yielded 17 of 882 MiB. Ignoring this made the model price a layout that
    # cannot be written.
    sort_ndv = lay.get("sort_ndv")
    if lay["sort_columns"] and sort_ndv:
        n_files = max(1, min(n_files, int(sort_ndv)))

    spec = partition_spec(lay)
    n_parts = 1
    part_col = None
    part_transform = None
    if spec:
        part_col, part_transform, n_parts = spec
        # partitionBy is applied by each writing task, so a task emits one file
        # per partition value it happens to hold. write_layout shuffles by the
        # *sort* key, which is uncorrelated with the partition key, so every
        # task sees every value: F files become F x P. This is the cost that
        # made the first modelled partition candidate look cheap when it was
        # not -- 200 lineitem files x 3 returnflag values is 600 files of
        # 36 MiB, and at 228 ms RTT the extra opens outweigh the pruning.
        # Whether pruning buys it back is decided in prune_fraction.
        n_files = max(n_files, 1) * n_parts

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
        "n_partitions": n_parts,
        "partition_column": part_col,
        "partition_transform": part_transform,
        "compressed_bytes": table_bytes(table),
        "rg_compressed": table_bytes(table) / n_rg,
    }


def _pred_selectivity(pred, col_stats, empirical_corr, sort_prefix):
    """Row-selectivity of one predicate. 1.0 = no filter."""
    col = pred["column"]
    op = pred["op"]
    stats = (col_stats or {}).get(col) or {}
    cdf = stats.get("cdf")
    ndv = stats.get("ndv") or 0

    if op in {"like", "in", "notin", "eq", "ne"}:
        if op == "eq" and ndv:
            return 1.0 / ndv
        if op in ("in", "notin") and ndv:
            n = len(pred["value"]) if isinstance(pred["value"], list) else 1
            frac = min(1.0, n / ndv)
            return frac if op == "in" else 1.0 - frac
        if op == "ne" and ndv:
            return 1.0 - 1.0 / ndv
        if op == "like":
            return 0.05  # TPC-H p_name / p_type likes; not a prune key
        return 1.0

    if not cdf:
        # Unsorted / no CDF: a range predicate on a high-NDV column still
        # typically hits every large RG. Selectivity is used only when sorted.
        return 1.0

    lo = hi = None
    inclusive_lo, inclusive_hi = True, False
    v = _coerce(cdf[len(cdf)//2], pred["value"])
    v2 = _coerce(cdf[len(cdf)//2], pred.get("value2"))
    if op == "ge":
        lo = v
    elif op == "gt":
        lo = v
        inclusive_lo = False
    elif op == "lt":
        hi = v
    elif op == "le":
        hi = v
        inclusive_hi = True
    elif op == "between":
        lo, hi = v, v2
        inclusive_hi = True
    else:
        return 1.0
    return cdf_selectivity(cdf, lo=lo, hi=hi,
                           inclusive_lo=inclusive_lo, inclusive_hi=inclusive_hi)


def partition_fraction(scan, lay, col_stats):
    """Fraction of partition directories a scan opens. 1.0 = no pruning.

    Directory pruning needs no ordering: Spark evaluates the filter against the
    partition values before opening anything, so a range predicate prunes an
    identity partition just as an equality does. That is exactly what a derived
    transform cannot do, so those return 1.0 and keep only their file-count
    cost -- the modelled reason `l_shipdate:year` never wins.
    """
    spec = partition_spec(lay)
    if not spec:
        return 1.0, None
    column, transform, n_parts = spec
    if transform != "identity" or n_parts <= 1:
        return 1.0, column
    stats_tbl = (col_stats or {}).get(scan["table"], {}).get("columns") or {}
    sel = 1.0
    for pred in scan["predicates"]:
        if pred["column"] != column:
            continue
        sel = min(sel, _pred_selectivity(pred, stats_tbl, False, column))
    # cannot open less than one directory
    return max(sel, 1.0 / n_parts) if sel < 1.0 else 1.0, column


def prune_fraction(scan, candidate, col_stats, empirical_corr=True):
    """Fraction of row groups that survive. Three branches (plan §3).

    1. Predicate column is a prefix of the sort key → CDF range / n_rg + 1 RG.
    2. Otherwise → unsorted overlap ≈ 1 (large RGs contain the full domain).
    3. Correlated column (l_receiptdate ~ l_shipdate) → same as the sort key
       iff empirical_corr is on. This branch is falsifiable.

    An identity partition multiplies in on top, except when it partitions the
    sort prefix itself: there both mechanisms keep the *same* rows, so the two
    fractions are combined with min() rather than squared.
    """
    lay = layout_for(scan["table"], candidate)
    part_sel, part_col = partition_fraction(scan, lay, col_stats)
    sort_cols = lay["sort_columns"]
    sort_prefix = sort_cols[0] if sort_cols else None
    table = scan["table"]
    stats_tbl = (col_stats or {}).get(table, {}).get("columns") or {}

    # Collapse range predicates per column so ge+lt become one interval.
    ranges = {}  # col -> {lo, hi, inclusive_lo, inclusive_hi}
    eq_sel = {}
    used_corr = False
    for pred in scan["predicates"]:
        col = pred["column"]
        effective = col
        if (empirical_corr and sort_prefix
                and catalog.CORRELATED_WITH.get(col) == sort_prefix):
            effective = sort_prefix
            used_corr = True
        if not sort_prefix or effective != sort_prefix:
            continue
        op = pred["op"]
        stats = stats_tbl.get(effective) or stats_tbl.get(col) or {}
        cdf = stats.get("cdf")
        sample = cdf[len(cdf)//2] if cdf else pred["value"]
        v = _coerce(sample, pred["value"])
        v2 = _coerce(sample, pred.get("value2"))
        slot = ranges.setdefault(effective, {
            "lo": None, "hi": None, "inclusive_lo": True, "inclusive_hi": False,
            "cdf": cdf, "ndv": stats.get("ndv") or 0,
        })
        if op == "ge":
            slot["lo"] = v if slot["lo"] is None else max(slot["lo"], v)
        elif op == "gt":
            slot["lo"] = v if slot["lo"] is None else max(slot["lo"], v)
            slot["inclusive_lo"] = False
        elif op == "lt":
            slot["hi"] = v if slot["hi"] is None else min(slot["hi"], v)
        elif op == "le":
            slot["hi"] = v if slot["hi"] is None else min(slot["hi"], v)
            slot["inclusive_hi"] = True
        elif op == "between":
            lo, hi = v, v2
            slot["lo"] = lo if slot["lo"] is None else max(slot["lo"], lo)
            slot["hi"] = hi if slot["hi"] is None else min(slot["hi"], hi)
            slot["inclusive_hi"] = True
        elif op in {"eq", "in", "ne", "like"}:
            eq_sel[effective] = min(eq_sel.get(effective, 1.0),
                                    _pred_selectivity(pred, stats_tbl, empirical_corr, sort_prefix))

    if not ranges and not eq_sel:
        return _combine(1.0, "unsorted_overlap", False, part_sel, part_col,
                        sort_prefix)

    sel = 1.0
    for col, slot in ranges.items():
        if slot["cdf"]:
            sel = min(sel, cdf_selectivity(
                slot["cdf"], lo=slot["lo"], hi=slot["hi"],
                inclusive_lo=slot["inclusive_lo"],
                inclusive_hi=slot["inclusive_hi"]))
        else:
            sel = min(sel, 1.0)
    for s in eq_sel.values():
        sel = min(sel, s)
    branch = "sorted_cdf" if not used_corr else "empirical_corr"
    return _combine(sel, branch, used_corr, part_sel, part_col, sort_prefix)


def _combine(sort_sel, branch, used_corr, part_sel, part_col, sort_prefix):
    """Fold directory pruning into row-group pruning."""
    if part_sel >= 1.0:
        return sort_sel, {"branch": branch, "selectivity": sort_sel,
                          "empirical_corr": used_corr, "partition_sel": 1.0}
    if part_col and part_col == sort_prefix:
        # same column: the surviving rows are the same set, so the two
        # mechanisms do not compound
        sel = min(sort_sel, part_sel)
    else:
        sel = sort_sel * part_sel
    return sel, {"branch": f"{branch}+partition" if sort_sel < 1.0 else "partition",
                 "selectivity": sel, "empirical_corr": used_corr,
                 "partition_sel": round(part_sel, 6),
                 "partition_column": part_col}


def surviving_rg_and_files(geom, prune_sel):
    n_rg = geom["n_rg"]
    n_files = geom["n_files"]
    rg_per_file = geom["rg_per_file"]
    if prune_sel >= 0.999:
        return n_rg, n_files
    # Consecutive RGs under a global sort; +1 for the boundary RG.
    kept = min(n_rg, max(1, int(math.ceil(prune_sel * n_rg)) + 1))
    files = min(n_files, max(1, int(math.ceil(kept / rg_per_file))))
    return kept, files


def chunk_bytes(table, column, geom):
    shares = catalog.COLUMN_SHARE.get(table)
    order = catalog.COLUMN_ORDER.get(table) or [column]
    if shares and column in shares:
        total = sum(shares.get(c, 0) for c in order)
        frac = shares[column] / total if total else 1.0 / len(order)
    else:
        frac = 1.0 / max(len(order), 1)
    return geom["rg_compressed"] * frac


def merge_gets(table, columns, geom, min_seek=131072, max_merged=2097152):
    """Apply frozen vectored-IO merge rules to projected column chunks.

    Columns are laid out in schema order. Adjacent chunks have zero gap; a
    chunk larger than max_merged can never merge with a neighbour.
    Returns (n_gets, total_bytes).
    """
    order = catalog.COLUMN_ORDER.get(table) or list(columns)
    wanted = set(columns)
    # walk schema order, emit runs of wanted columns
    n_gets = 0
    total = 0
    run = []
    run_bytes = 0

    def flush():
        nonlocal n_gets, total, run, run_bytes
        if not run:
            return
        # split run into GETs of at most max_merged bytes
        acc = 0
        pieces = 1
        for b in run:
            if acc and acc + b > max_merged:
                pieces += 1
                acc = b
            else:
                acc += b
        n_gets += pieces
        total += run_bytes
        run, run_bytes = [], 0

    for col in order:
        b = chunk_bytes(table, col, geom)
        if col in wanted:
            # starting a new run, or continuing
            if run and run_bytes + b > max_merged and b > max_merged:
                flush()
            run.append(b)
            run_bytes += b
        else:
            # gap: skipped column. If the gap is larger than min_seek, break.
            if run:
                if b > min_seek:
                    flush()
                else:
                    # would include wasted bytes to merge across the gap;
                    # frozen reader only merges if seek < min_seek. Include
                    # the gap in the current run as waste.
                    run.append(b)
                    run_bytes += b
    flush()
    return n_gets, total
