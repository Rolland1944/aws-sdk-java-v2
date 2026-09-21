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

try:
    from advisor_policy import SPLIT_SIZE_BYTES
except ImportError:
    SPLIT_SIZE_BYTES = 128 * 1024 * 1024

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

    # File size and row-group size are independent axes. A file-only action
    # must not invent a row-group size (the old path stuffed in the 128 MiB
    # Spark default and silently rewrote `rg=baseline`).
    rg_bytes = spec.get("rg_bytes")
    file_bytes = spec.get("file_bytes")
    order = spec.get("column_order")
    if order:
        known = set(catalog.ALL_COLUMNS.get(table) or [])
        order = [c for c in order if c in known] or None
    return {
        "file_bytes": file_bytes,
        "rg_bytes": rg_bytes,
        "column_order": order,
        "page_bytes": effective_page_bytes(table, spec.get("page_bytes")),
        "page_row_limit": spec.get("page_row_limit"),
        "compression": spec.get("compression"),
        "compression_ratio": spec.get("compression_ratio"),
        "column_ratios": spec.get("column_ratios"),
    }


def page_probe(table):
    """The page pass of the layout probe, when it had enough resolution."""
    rec = ((probe or {}).get(table) or {}).get("page")
    if not rec or not rec.get("resolved"):
        return None
    return rec


def effective_page_bytes(table, page_bytes):
    """The page size the writer will actually apply, or None for no change.

    A requested page size the writer ignores produces a byte-identical file,
    and carrying the request through the model would price a rewrite that
    never happens. Which sizes are ignored is measured per point, not assumed
    from a threshold: it depends on how many bytes a column holds in one row
    group, so the same request can be a no-op on a narrow table and a real
    change on a wide one.
    """
    if not page_bytes:
        return None
    rec = page_probe(table)
    if not rec:
        return page_bytes
    if page_bytes in set(rec.get("noop_page_bytes") or ()):
        return None
    return page_bytes


def _rendered_for(candidate, table):
    """The rendered layout spec of a candidate, or None for the baseline.

    A candidate with no actions *is* the baseline, and `render([])` is not the
    way to say so: an empty action list is falsy and several callers rely on
    that to mean "nothing was asked for". Returning None makes the two cases
    distinguishable to the decode model, which unlike the byte model has to
    price the unchanged columns too.
    """
    if not candidate.get("actions"):
        return None
    import layout_actions

    try:
        return layout_actions.render(candidate["actions"], table=table)
    except ValueError:
        return None


def _from_actions(actions, table):
    """Canonical actions -> the resolved-layout dict this module prices."""
    rendered = _rendered_for({"actions": actions}, table)
    if rendered is None:
        return {}
    return {
        "file_bytes": rendered.target_file_size,
        "rg_bytes": rendered.row_group_size,
        "column_order": rendered.column_order,
        "page_bytes": rendered.page_size,
        "page_row_limit": rendered.page_row_limit,
        "compression": rendered.compression,
        # Per-column codec and encoding changes are priced only when a
        # measurement says by how much, and only as one joint point. Without
        # one the change is recorded and the bytes are left alone, which
        # understates the action rather than inventing a ratio for it.
        "column_ratios": layout_ratios(table, rendered)[0],
    }


# Set by whatif/plan_deterministic when a layout probe is available:
# {table: {"columns": {...}, "page": {...}}}. Set alongside it so a v1
# document (independent codec/encoding ratio maps) cannot be read as if it
# carried joint measurements.
probe = None
probe_schema_version = 1

BASELINE_ENCODING = "baseline"


def bind_probe(doc):
    """Install a layout-probe document and remember which schema it is."""
    global probe, probe_schema_version
    if doc is None:
        probe, probe_schema_version = None, 1
        return
    if "tables" in doc:
        probe = doc.get("tables")
        probe_schema_version = int(doc.get("schema_version") or 1)
    else:
        # Bare {table: {...}} mapping, as the older callers passed.
        probe = doc
        probe_schema_version = 1


def layout_ratios(table, rendered):
    """Per-column byte scaling for the (codec, encoding) tuple a plan asks for.

    Codec and encoding are looked up as one point. They are not independent:
    a dictionary that collapses a column to a few values leaves the codec
    nothing to compress, so multiplying a codec ratio by an encoding ratio
    invents a saving neither measurement supports. A tuple the probe never
    measured is returned as unpriced rather than as 1.0, because 1.0 is itself
    a claim -- that the change is free.

    Returns (ratios, unpriced) where `unpriced` lists the columns whose
    requested tuple has no measurement behind it.
    """
    from advisor_policy import BASELINE_CODEC

    if not probe:
        return None, []
    columns = ((probe.get(table) or {}).get("columns") or {})
    if not columns:
        return None, []

    wants_encoding = bool(rendered.column_encoding)
    if wants_encoding and probe_schema_version < 2:
        # v1 documents carry codec ratios and encoding ratios separately, and
        # the only way to combine them is the product this function exists to
        # refuse. Price the codec, report the encoding as unmeasured.
        wants_encoding = False

    out = {}
    unpriced = []
    for column, rec in columns.items():
        codec = (rendered.column_compression.get(column)
                 or rendered.compression or BASELINE_CODEC)
        encoding = (rendered.column_encoding.get(column)
                    if wants_encoding else None) or BASELINE_ENCODING
        if codec == BASELINE_CODEC and encoding == BASELINE_ENCODING:
            continue
        ratio = None
        for point in rec.get("joint") or []:
            if point.get("codec") == codec and point.get("encoding") == encoding:
                ratio = point.get("ratio")
                break
        if ratio is None and probe_schema_version < 2:
            ratios = rec.get("ratios") or {}
            base, target = ratios.get(BASELINE_CODEC), ratios.get(codec)
            if base and target:
                ratio = target / base
        if ratio is None:
            unpriced.append({"column": column, "codec": codec,
                             "encoding": encoding})
            continue
        out[column] = ratio
    return (out or None), unpriced


# ------------------------------------------------------------------- decode

# Set by bind_decode_profile. Until it is, every candidate's decode cost is
# `None` rather than zero: an unmodelled term reported as 0.0 is the specific
# failure the byte side already refuses (see layout_ratios).
decode_profile = None

# Which probe's rate table prices decode. This is *not* the writer choice:
# UC1 means written by PyArrow, and both use cases are read by Spark.
#
# The default is `pyarrow` even though the benchmark reads through parquet-mr,
# because parquet-mr's absolute rates are not per-byte. Its measurement pays
# the noop sink's per-row cost, which dominates on narrow columns: INT32 comes
# back at 0.01 GB/s against BYTE_ARRAY's 0.29, and a term that sums across
# columns of different widths would read that as ints costing thirty times
# more per byte to decode than strings. PyArrow's read has no sink and is
# comparable across types, so it supplies the shape and DECODE_RATE_SCALE
# absorbs the implementation difference. Separating the two would mean fitting
# parquet-mr's per-row constant out; the cases carry `rows` for that.
DECODE_READERS = ("pyarrow", "parquet-mr")


def bind_decode_profile(doc, reader="pyarrow", rate_scale=1.0):
    """Install the measured decode rates L1 charges CPU against.

    `doc` is a decode_probe.py document. Rates are encoded bytes per second
    per core, keyed by physical type and by the joint (codec, encoding) tuple
    -- the same tuple discipline as the byte side, for the same reason: a
    dictionary that collapses a column changes what the codec is handed, so a
    per-codec rate multiplied by a per-encoding rate is not a measurement.

    `rate_scale` relates the probe's single-threaded tmpfs read to the reader
    the benchmark actually runs. It is calibrated against an independent
    measurement (per-task Executor CPU Time in the event logs), and it is one
    scalar on purpose: a per-tuple fudge factor would absorb the model error
    it exists to expose.
    """
    global decode_profile
    if doc is None:
        decode_profile = None
        return
    if reader not in DECODE_READERS:
        raise ValueError(f"unknown decode reader {reader!r}")
    rates = (doc.get("rates") or {}).get(reader)
    if not rates:
        decode_profile = None
        return
    decode_profile = {
        "reader": reader,
        "rate_scale": float(rate_scale),
        "rates": rates,
        "measured_at": doc.get("measured_at"),
    }


def decode_rate(physical_type, codec, encoding):
    """Encoded bytes per second per core, or None when the tuple is unmeasured."""
    if not decode_profile:
        return None
    rec = (decode_profile["rates"].get(physical_type) or {}).get(
        f"{codec}|{encoding}")
    bps = (rec or {}).get("bytes_per_s")
    if not bps:
        return None
    return bps * decode_profile["rate_scale"]


def baseline_encoded_bytes(table):
    """Encoded bytes each column holds across the whole table today.

    Encoded bytes are Parquet's `total_uncompressed_size`: what the codec
    hands the decoder. This is a *different* split from COLUMN_SHARE, which is
    compressed, and the two cannot be substituted -- a column that compresses
    ten times better than its neighbour holds ten times its compressed share
    of the decoder's work. A snapshot without the encoded split returns None
    rather than borrowing the compressed one.
    """
    shares = (getattr(catalog, "COLUMN_UNCOMPRESSED_SHARE", None) or {}).get(table)
    if not shares:
        return None
    denom = sum(shares.values())
    if not denom:
        return None
    base = catalog.BASELINE_GEOMETRY[table]
    n_rg = base.get("n_rg") or base["files"]
    total = (base.get("rg_bytes") or 0) * n_rg
    if not total:
        return None
    return {c: total * (b / denom) for c, b in shares.items()}


def decode_plan(table, candidate):
    """Per-column encoded-byte ratio and decode rate for one candidate.

    Unlike `layout_ratios` this does not skip the columns a candidate leaves
    alone. Decode is an absolute cost, not a delta: pricing only the rewritten
    columns would report a candidate that re-encodes one column as decoding
    one column. Every column the table has is either priced or listed as
    unpriced.

    Returns (plan, unpriced). `plan` maps column -> {"codec", "encoding",
    "physical_type", "encoded_ratio", "bytes_per_s"}.
    """
    from advisor_policy import BASELINE_CODEC

    if not probe or not decode_profile:
        return None, []
    columns = ((probe.get(table) or {}).get("columns") or {})
    if not columns:
        return None, []
    rendered = _rendered_for(candidate, table)

    plan = {}
    unpriced = []
    for column, rec in columns.items():
        codec = BASELINE_CODEC
        encoding = BASELINE_ENCODING
        if rendered is not None:
            codec = (rendered.column_compression.get(column)
                     or rendered.compression or BASELINE_CODEC)
            encoding = (rendered.column_encoding.get(column)
                        or BASELINE_ENCODING)
        ptype = rec.get("physical_type")
        point = next((p for p in (rec.get("joint") or [])
                      if p.get("codec") == codec
                      and p.get("encoding") == encoding), None)
        # `encoded_ratio` only exists from probe schema v3. A v2 document has
        # the wire ratio and nothing else, and reading that as the encoded one
        # would price DELTA_BYTE_ARRAY's 4% byte saving as a 4% decode saving
        # when it actually halves the bytes the decoder sees.
        ratio = (point or {}).get("encoded_ratio")
        bps = decode_rate(ptype, codec, encoding)
        if ratio is None or not bps:
            unpriced.append({"column": column, "codec": codec,
                             "encoding": encoding, "physical_type": ptype,
                             "reason": ("no encoded_bytes" if ratio is None
                                        else "no measured rate")})
            continue
        plan[column] = {"codec": codec, "encoding": encoding,
                        "physical_type": ptype, "encoded_ratio": ratio,
                        "bytes_per_s": bps}
    return (plan or None), unpriced


def decode_core_s_per_scan(table, columns, plan):
    """Core-seconds to decode `columns` once over the whole table.

    Row-group and file geometry are deliberately absent. Decode work is a
    function of the rows and columns read, not of how they are grouped, so
    the rg/file axes must not move this term -- if they did, L1 would report a
    row-group change as a CPU change and the residual would stop being
    constant across candidates.

    Returns (core_s, missing) where `missing` lists requested columns with no
    entry in the plan.
    """
    encoded = baseline_encoded_bytes(table)
    if not encoded or not plan:
        return None, list(columns)
    total = 0.0
    missing = []
    for column in columns:
        rec = plan.get(column)
        base = encoded.get(column)
        if not rec or not base:
            missing.append(column)
            continue
        total += base * rec["encoded_ratio"] / rec["bytes_per_s"]
    return total, missing


def unpriced_encodings(table, rendered):
    """Columns whose requested encoding has no joint measurement."""
    if rendered.column_encoding and probe_schema_version < 2:
        return [{"column": c, "encoding": e, "codec": None}
                for c, e in sorted(rendered.column_encoding.items())]
    return layout_ratios(table, rendered)[1]


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
    """Scale file/RG counts from the measured baseline.

    Baseline (no file-size or row-group action) is replayed as measured:
    fractional `rg_per_file` is kept, and `n_rg` is *not* rounded up to a
    multiple of `n_files`. That rounding used to turn ClickBench's 165 row
    groups into 220 before L1 ever priced a point.
    """
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
        # rg=baseline keeps rows/RG. File merge must not invent extra RGs;
        # the writer is required to buffer across source files so this holds.
        n_rg = base_rg
    # A file must contain at least one row group, but do not invent extra row
    # groups just to make n_rg divisible by n_files.
    if n_rg < n_files:
        n_rg = n_files
    rg_per_file = n_rg / n_files
    file_sizes = predict_file_sizes(compressed, n_files, file_bytes, base)
    split = (base.get("split_size_bytes") or SPLIT_SIZE_BYTES)
    n_scan_units = scan_unit_count(file_sizes, split)
    return {
        "table": table,
        "n_files": n_files,
        "n_rg": n_rg,
        "rg_per_file": rg_per_file,
        "compressed_bytes": compressed,
        "compression_ratio": ratio,
        "rg_compressed": compressed / n_rg,
        "page_bytes": lay.get("page_bytes"),
        "file_sizes": file_sizes,
        "n_scan_units": n_scan_units,
        "split_size_bytes": split,
    }


def scan_unit_count(file_sizes, split_size=SPLIT_SIZE_BYTES):
    """Spark input splits: each file contributes ceil(size / split) tasks."""
    split = max(int(split_size or SPLIT_SIZE_BYTES), 1)
    return sum(max(1, math.ceil(size / split)) for size in file_sizes) or 1


def predict_file_sizes(compressed, n_files, file_bytes, base):
    """Candidate file-size list used to predict scan-unit count."""
    if file_bytes and n_files:
        sizes = [int(file_bytes)] * n_files
        leftover = int(compressed) - int(file_bytes) * (n_files - 1)
        sizes[-1] = max(1, leftover)
        return sizes
    measured = base.get("file_sizes")
    if measured:
        return list(measured)
    if n_files:
        each = max(1, int(round(compressed / n_files)))
        return [each] * n_files
    return [int(compressed)] if compressed else [1]


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


def _merge_one(order, wanted, sizes, min_seek, max_merged):
    """Vectored merge of one row group's column sizes. Returns (gets, bytes)."""
    n_gets = 0
    total = 0.0
    run = []
    run_bytes = 0.0

    def flush():
        nonlocal n_gets, total, run, run_bytes
        if not run:
            return
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
        b = float(sizes.get(col, 0.0))
        if col in wanted:
            if run and run_bytes + b > max_merged and b > max_merged:
                flush()
            run.append(b)
            run_bytes += b
        elif run:
            if b > min_seek:
                flush()
            else:
                run.append(b)
                run_bytes += b
    flush()
    return n_gets, total


def _rg_size_maps(table, geom, column_ratios=None):
    """Per-RG {column: bytes} maps. Footer samples when present, else shares."""
    ratios = column_ratios or {}
    samples = []
    if catalog is not None:
        samples = list(getattr(catalog, "RG_CHUNK_SAMPLES", {}).get(table) or [])
    if samples:
        out = []
        scale = 1.0
        if geom.get("rg_compressed"):
            measured = sum(samples[0].values()) or 1.0
            if measured:
                scale = geom["rg_compressed"] / measured
        for sample in samples:
            sized = {}
            for col, nbytes in sample.items():
                sized[col] = nbytes * scale * ratios.get(col, 1.0)
            out.append(sized)
        return out
    order = catalog.COLUMN_ORDER.get(table) if catalog else None
    order = order or []
    return [{col: chunk_bytes(table, col, geom, ratios.get(col)) for col in order}]


def merge_gets(table, columns, geom, min_seek=131072, max_merged=2097152,
               order=None, column_ratios=None):
    """Apply the frozen vectored-IO merge rules to one row group's projection.

    Uses sampled per-RG chunk sizes from the dataset snapshot when they exist,
    then reorders those sizes into the candidate column order. Average share
    reconstruction is the fallback when no footer samples were collected.

    Returns (n_gets, total_bytes) averaged over the sampled row groups.
    """
    order = order or catalog.COLUMN_ORDER.get(table) or list(columns)
    wanted = set(columns)
    maps = _rg_size_maps(table, geom, column_ratios)
    if not maps:
        return 0.0, 0.0
    gets = bytes_ = 0.0
    for sizes in maps:
        g, b = _merge_one(order, wanted, sizes, min_seek, max_merged)
        gets += g
        bytes_ += b
    return gets / len(maps), bytes_ / len(maps)


def _pages_in_rg(table, geom, columns, page_bytes, column_ratios=None):
    """Pages the listed columns occupy inside one row group.

    The page limit bounds the *uncompressed* page buffer, so the count is
    taken against uncompressed row-group bytes, split by the measured column
    byte shares and scaled by whatever the codec/encoding tuple does to each
    column.
    """
    base = catalog.BASELINE_GEOMETRY[table]
    total_unc = (base.get("rg_bytes") or 0) * (base.get("n_rg") or base["files"])
    n_rg = max(1, geom.get("n_rg") or 1)
    shares = catalog.COLUMN_SHARE.get(table) or {}
    denom = sum(shares.values())
    if not total_unc or not denom:
        return None
    rg_unc = total_unc / n_rg
    ratios = column_ratios or {}
    pages = 0.0
    for column in columns:
        share = shares.get(column)
        if not share:
            continue
        col_unc = rg_unc * (share / denom) * ratios.get(column, 1.0)
        pages += max(1, math.ceil(col_unc / page_bytes))
    return pages


def page_index_delta_bytes(table, geom, columns, column_ratios=None,
                           baseline_page_bytes=None):
    """Page-index bytes one row-group open gains or loses, for one projection.

    Restricted to the columns the projection actually reads: the reader takes
    the OffsetIndex/ColumnIndex ranges for the chunks it is about to fetch,
    not for all 105 of them, so charging the whole table's index to every scan
    would overstate the axis by the projection ratio.

    The sign follows the page size. A coarser page merges pages and shrinks
    the index; a finer one grows it. What the model does *not* do is credit a
    finer page with skipping: `requests_per_chunk_touched` below 1 says the
    reader is merging whole chunks into single ranges rather than sub-dividing
    them, so there is nothing inside a chunk for a finer OffsetIndex to skip.
    Predicting otherwise would need to know which pages a predicate
    eliminates, and predicates are the input this advisor refuses to read.
    """
    rec = page_probe(table)
    page = geom.get("page_bytes")
    if not rec or not page:
        return 0.0
    per_page = rec.get("index_bytes_per_page")
    if not per_page:
        return 0.0
    default = baseline_page_bytes or rec.get("default_page_bytes")
    cand = _pages_in_rg(table, geom, columns, page, column_ratios)
    base = _pages_in_rg(table, geom, columns, default, column_ratios)
    if cand is None or base is None:
        return 0.0
    return (cand - base) * per_page
