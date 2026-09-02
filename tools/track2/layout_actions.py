#!/usr/bin/env python3
"""The canonical action vocabulary, and the L0 checks that guard it.

*** This file defines what a layout candidate is allowed to say. ***

Candidates are written in engine-neutral canonical names (contract §11.2,
aligned with Iceberg table properties) so that analysis, planning and costing
never learn which writer is downstream. Two renderers consume this module:

    write_layout.py         -> Spark / parquet-mr   (UC2: SQL + parameters)
    write_layout_pyarrow.py -> PyArrow ParquetWriter (UC1: hand-written code)

Contract r5 replaced the action space. `sort.columns` and `partition.spec` are
gone -- streaming arrivals cannot be globally sorted, and directory layout is
not a file-format property -- and six dimensions took their place, every one of
which is a constructor argument on a writer that already exists. Nothing here
requires patching parquet-mr, Arrow or Spark.

Two of the six do not survive the trip to Spark intact. Per-column compression
and specific encoding families have no `parquet.*` property (contract §6.2
M-5), so the Spark renderer records a warning and drops them. That asymmetry is
not a defect to route around; it is the measurable difference between the two
use cases, and `rendered.warnings` is where the experiment reads it off.

Page index is deliberately *not* a seventh dimension. parquet-mr always writes
ColumnIndex/OffsetIndex and PyArrow defaults to not writing it, so leaving it
free would make the two writers incomparable for reasons having nothing to do
with the layout under test. UC1 pins it on and `--verify` fails the write if the
footer comes back without it.
"""

from __future__ import annotations

# ---------------------------------------------------------------- vocabulary

COLUMN_ORDER = "write.parquet.column-order"
ROW_GROUP_SIZE = "write.parquet.row-group-size-bytes"
TARGET_FILE_SIZE = "write.target-file-size-bytes"
COMPRESSION = "write.parquet.compression-codec"
PAGE_SIZE = "write.parquet.page-size-bytes"
PAGE_ROW_LIMIT = "write.parquet.page-row-limit"

COMPRESSION_COLUMN_PREFIX = "write.parquet.compression-codec.column."
ENCODING_COLUMN_PREFIX = "write.parquet.encoding.column."
DICT_COLUMN_PREFIX = "write.parquet.dict-encoding-enabled.column."

# contract 11.2: canonical (Iceberg vocabulary) -> parquet-mr property
CANONICAL_TO_PARQUET_MR = {
    ROW_GROUP_SIZE: "parquet.block.size",
    PAGE_SIZE: "parquet.page.size",
    PAGE_ROW_LIMIT: "parquet.page.row.count.limit",
    COMPRESSION: "parquet.compression",
    "write.parquet.writer-version": "parquet.writer.version",
}

# per-column properties parquet-mr *does* expose; the column name is appended
CANONICAL_COLUMN_PREFIXES = {
    DICT_COLUMN_PREFIX: "parquet.enable.dictionary#",
    "write.parquet.stats-enabled.column.": "parquet.column.statistics.enabled#",
    "write.parquet.bloom-filter-enabled.column.": "parquet.bloom.filter.enabled#",
    "write.parquet.bloom-filter-ndv.column.": "parquet.bloom.filter.expected.ndv#",
    "write.parquet.bloom-filter-fpp.column.": "parquet.bloom.filter.fpp#",
}

# canonical names that are transforms or writer-constructor arguments rather
# than parquet-mr configuration keys
TRANSFORM_CANONICALS = {COLUMN_ORDER, TARGET_FILE_SIZE}

# Actions the six-dimension space is allowed to contain. Anything outside this
# set still renders (bloom, stats) but the planners do not generate it.
V2_ACTION_SPACE = {
    COLUMN_ORDER, ROW_GROUP_SIZE, TARGET_FILE_SIZE, COMPRESSION,
    PAGE_SIZE, PAGE_ROW_LIMIT,
}
V2_ACTION_PREFIXES = (COMPRESSION_COLUMN_PREFIX, ENCODING_COLUMN_PREFIX,
                      DICT_COLUMN_PREFIX)

# Retired in r5. Named explicitly so an old candidate file fails loudly rather
# than being silently written as a baseline.
RETIRED_CANONICALS = {
    "sort.columns": "sort left the action space in r5 (streaming arrivals "
                    "cannot be globally sorted; row order is not a file-format "
                    "property)",
    "partition.spec": "partition left the action space in r5 (directory layout "
                      "is not a file-format property)",
}

BASELINE_CANDIDATE = {"candidate_id": "baseline", "actions": []}

# ------------------------------------------------------------- capabilities

# Codecs both parquet-mr and PyArrow read. `uncompressed` is spelled `none` by
# parquet-mr and `NONE` by PyArrow; normalise on the Parquet spec name.
CODECS = ("uncompressed", "snappy", "gzip", "zstd", "lz4")
CODEC_ALIASES = {"none": "uncompressed", "": "uncompressed"}

# Encoding -> the physical types it may be applied to. Applying an encoding to
# an incompatible type is not an error at the API level: the writer silently
# falls back to PLAIN, so the experiment would record a null result that looks
# like "encoding does not help" when the encoding was never used. L0 rejects
# the mismatch instead.
ENCODING_PHYSICAL_TYPES = {
    "PLAIN": None,  # any
    "RLE_DICTIONARY": None,
    "DELTA_BINARY_PACKED": {"INT32", "INT64"},
    "DELTA_BYTE_ARRAY": {"BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"},
    "DELTA_LENGTH_BYTE_ARRAY": {"BYTE_ARRAY"},
    "BYTE_STREAM_SPLIT": {"FLOAT", "DOUBLE"},
}
ENCODINGS = tuple(ENCODING_PHYSICAL_TYPES)

# parquet-mr can only turn the dictionary on and off; it has no property that
# selects an encoding family (contract §6.2 M-5).
SPARK_EXPRESSIBLE_ENCODINGS = {"RLE_DICTIONARY", "PLAIN"}


def normalise_codec(value):
    v = str(value or "").strip().lower()
    return CODEC_ALIASES.get(v, v)


# ---------------------------------------------------------------- rendering

class Rendered(object):
    """One candidate resolved for one table, in writer-neutral terms."""

    def __init__(self):
        self.writer_options = {}       # parquet-mr property -> str
        self.column_order = None       # list[str] or None
        self.target_file_size = None   # bytes or None
        self.row_group_size = None     # bytes or None
        self.page_size = None          # bytes or None
        self.page_row_limit = None     # rows or None
        self.compression = None        # global codec or None
        self.column_compression = {}   # column -> codec   (UC1 only)
        self.column_encoding = {}      # column -> encoding (UC1 only)
        self.column_dictionary = {}    # column -> bool
        self.warnings = []

    def as_dict(self):
        return {
            "writer_options": self.writer_options,
            "column_order": self.column_order,
            "target_file_size": self.target_file_size,
            "row_group_size": self.row_group_size,
            "page_size": self.page_size,
            "page_row_limit": self.page_row_limit,
            "compression": self.compression,
            "column_compression": self.column_compression,
            "column_encoding": self.column_encoding,
            "column_dictionary": self.column_dictionary,
        }


def _as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def render(actions, table=None):
    """Render canonical actions for one table into writer-neutral settings.

    `table` selects per-table actions (contract 3.3 `scope.table`). An action
    with `"table": "hits"` applies only when writing hits; an action without
    `table` is global. Table-scoped values override global ones.
    """
    out = Rendered()
    scoped = {}   # canonical -> value, table-scoped
    globals_ = {}  # canonical -> value, unscoped

    for action in actions:
        canonical = action.get("canonical")
        value = action.get("value")
        action_table = action.get("table")
        if not canonical:
            raise ValueError(f"action without a canonical name: {action}")
        if canonical in RETIRED_CANONICALS:
            raise ValueError(
                f"'{canonical}' is retired: {RETIRED_CANONICALS[canonical]}. "
                f"Regenerate the candidate with a v2 planner.")
        if table and action_table and action_table != table:
            continue
        if action_table and action_table == table:
            scoped[canonical] = value
        elif action_table is None:
            globals_[canonical] = value
        # A table-scoped action seen during a global render (table=None) is
        # skipped: a hits-only 1 GiB must not read back as a global 1 GiB.

    resolved = dict(globals_)
    resolved.update(scoped)

    for canonical, value in resolved.items():
        if canonical in CANONICAL_TO_PARQUET_MR:
            if canonical == COMPRESSION:
                value = normalise_codec(value)
                out.compression = value
            elif canonical == ROW_GROUP_SIZE:
                out.row_group_size = int(value)
            elif canonical == PAGE_SIZE:
                out.page_size = int(value)
            elif canonical == PAGE_ROW_LIMIT:
                out.page_row_limit = int(value)
            out.writer_options[CANONICAL_TO_PARQUET_MR[canonical]] = str(value)
            continue

        if canonical == COLUMN_ORDER:
            out.column_order = list(value or [])
            continue
        if canonical == TARGET_FILE_SIZE:
            out.target_file_size = int(value)
            continue

        if canonical.startswith(COMPRESSION_COLUMN_PREFIX):
            column = canonical[len(COMPRESSION_COLUMN_PREFIX):]
            out.column_compression[column] = normalise_codec(value)
            continue
        if canonical.startswith(ENCODING_COLUMN_PREFIX):
            column = canonical[len(ENCODING_COLUMN_PREFIX):]
            out.column_encoding[column] = str(value).upper()
            continue

        prefix_hit = next(
            ((p, r) for p, r in CANONICAL_COLUMN_PREFIXES.items()
             if canonical.startswith(p)), None)
        if prefix_hit:
            prefix, rendered_prefix = prefix_hit
            column = canonical[len(prefix):]
            if prefix == DICT_COLUMN_PREFIX:
                out.column_dictionary[column] = _as_bool(value)
            out.writer_options[rendered_prefix + column] = (
                str(value).lower() if isinstance(value, bool) else str(value))
            continue

        raise ValueError(
            f"unrenderable canonical name '{canonical}'. Every candidate action "
            f"must be expressible on a real writer (contract 5.3 L0 check 1); "
            f"add a mapping here or reject the action.")

    return out


def strip_for_spark(rendered):
    """Drop what parquet-mr cannot express, and say so.

    Returns a copy. The warnings it records are the UC1/UC2 delta the
    experiment reports, so they must survive into the manifest rather than
    being logged and forgotten.
    """
    import copy
    out = copy.deepcopy(rendered)
    if out.column_compression:
        out.warnings.append(
            f"UC1-only: per-column compression on "
            f"{sorted(out.column_compression)} dropped; parquet.compression is "
            f"global (contract §6.2 M-5)")
        out.column_compression = {}
    unexpressible = {c: e for c, e in out.column_encoding.items()
                     if e not in SPARK_EXPRESSIBLE_ENCODINGS}
    if unexpressible:
        out.warnings.append(
            f"UC1-only: encoding family {sorted(set(unexpressible.values()))} on "
            f"{sorted(unexpressible)} dropped; parquet-mr exposes only "
            f"parquet.enable.dictionary#col (contract §6.2 M-5)")
    # RLE_DICTIONARY / PLAIN are reachable through the dictionary switch.
    for column, encoding in out.column_encoding.items():
        if encoding in SPARK_EXPRESSIBLE_ENCODINGS:
            enabled = encoding == "RLE_DICTIONARY"
            out.column_dictionary.setdefault(column, enabled)
            out.writer_options.setdefault(
                "parquet.enable.dictionary#" + column, str(enabled).lower())
    out.column_encoding = {}
    return out


# ------------------------------------------------------------------- L0

def check_l0(rendered, source_bytes=None, schema=None, physical_types=None,
             writer="parquet-mr"):
    """Static candidate legality, contract 5.3 as revised by r5.

    Returns a list of violations. A non-empty list must stop the run: the point
    of L0 is to reject a candidate before spending machine time on an
    experiment whose negative result would be an artefact of the candidate
    being unrealisable.

    `schema` is the source column list, needed to check that a column order is
    a permutation. `physical_types` maps column -> Parquet physical type, which
    is what makes an encoding legal or a silent no-op.
    """
    violations = []
    options = rendered.writer_options
    row_group = rendered.row_group_size
    page = rendered.page_size
    target_file = rendered.target_file_size

    # check 5: monotonicity
    if row_group and target_file and row_group > target_file:
        violations.append(
            f"row group size ({row_group}) > target file size ({target_file})")
    if page and row_group and page > row_group:
        violations.append(f"page size ({page}) > row group size ({row_group})")
    if rendered.page_row_limit is not None and rendered.page_row_limit <= 0:
        violations.append(f"page row limit {rendered.page_row_limit} must be positive")

    # check 3: structural fidelity
    if source_bytes:
        if target_file and source_bytes / target_file < 2:
            violations.append(
                f"target file size {target_file} yields < 2 files over "
                f"{source_bytes} bytes")
        effective_rg = row_group or 128 * 1024 * 1024
        if source_bytes / effective_rg < 4:
            violations.append(
                f"row group size {effective_rg} yields < 4 row groups over "
                f"{source_bytes} bytes")

    # r5 check: a column order must be a permutation of the schema. Dropping a
    # column is not a layout action, and adding one is not expressible.
    if rendered.column_order is not None:
        order = rendered.column_order
        if len(set(order)) != len(order):
            dupes = sorted({c for c in order if order.count(c) > 1})
            violations.append(f"column order repeats {dupes}")
        if schema:
            missing = [c for c in schema if c not in set(order)]
            extra = [c for c in order if c not in set(schema)]
            if missing:
                violations.append(
                    f"column order omits {len(missing)} column(s) "
                    f"{missing[:5]}{'...' if len(missing) > 5 else ''}; a "
                    f"reordering must be a permutation, not a projection")
            if extra:
                violations.append(f"column order names unknown column(s) {extra[:5]}")

    # r5 check: codecs and encodings must exist and match the physical type.
    codecs = dict(rendered.column_compression)
    if rendered.compression:
        codecs["*"] = rendered.compression
    for column, codec in codecs.items():
        if normalise_codec(codec) not in CODECS:
            violations.append(
                f"compression codec '{codec}' on {column} is not in the "
                f"reader capability matrix {CODECS}")
    for column, encoding in rendered.column_encoding.items():
        if encoding not in ENCODING_PHYSICAL_TYPES:
            violations.append(
                f"encoding '{encoding}' on {column} is not a Parquet encoding "
                f"{ENCODINGS}")
            continue
        allowed = ENCODING_PHYSICAL_TYPES[encoding]
        ptype = (physical_types or {}).get(column)
        if allowed and ptype and ptype not in allowed:
            violations.append(
                f"encoding {encoding} on {column} needs physical type in "
                f"{sorted(allowed)}, but the column is {ptype}; the writer "
                f"would silently fall back to PLAIN and the result would read "
                f"as 'encoding did not help'")
    if writer == "parquet-mr":
        for column, encoding in rendered.column_encoding.items():
            if encoding not in SPARK_EXPRESSIBLE_ENCODINGS:
                violations.append(
                    f"encoding {encoding} on {column} is not expressible on "
                    f"parquet-mr (contract §6.2 M-5); render it for PyArrow or "
                    f"strip it with strip_for_spark()")
        if rendered.column_compression:
            violations.append(
                f"per-column compression {sorted(rendered.column_compression)} "
                f"is not expressible on parquet-mr; parquet.compression is global")

    # check 2: reader capability -- page index is not an action at all
    for name in options:
        if "columnindex" in name.lower() or "page.write-checksum" in name:
            violations.append(
                f"{name} is not a candidate action (contract 6.2 M-1); page "
                f"index is pinned on and verified, not searched")

    return violations


def validate_plan(plan, schema=None, physical_types=None, writer="pyarrow",
                  source_bytes=None, table=None):
    """Schema + L0 validation of a whole plan document.

    Used by the deterministic planner to self-check and by the LLM planner to
    decide whether to feed an error back and retry, so it reports *why* rather
    than raising on the first problem.
    """
    problems = []
    actions = plan.get("actions")
    if actions is None:
        return ["plan has no 'actions' list"]
    if not isinstance(actions, list):
        return ["plan 'actions' must be a list"]

    for action in actions:
        if not isinstance(action, dict):
            problems.append(f"action is not an object: {action!r}")
            continue
        canonical = action.get("canonical")
        if not canonical:
            problems.append(f"action without a canonical name: {action}")
        elif canonical in RETIRED_CANONICALS:
            problems.append(f"'{canonical}' is retired: {RETIRED_CANONICALS[canonical]}")
        elif (canonical not in V2_ACTION_SPACE
              and not canonical.startswith(V2_ACTION_PREFIXES)):
            problems.append(
                f"'{canonical}' is outside the v2 six-dimension action space; "
                f"allowed: {sorted(V2_ACTION_SPACE)} plus per-column prefixes "
                f"{list(V2_ACTION_PREFIXES)}")
    if problems:
        return problems

    try:
        rendered = render(actions, table=table)
    except ValueError as exc:
        return [str(exc)]
    return check_l0(rendered, source_bytes, schema, physical_types, writer)
