#!/usr/bin/env python3
"""Measure what a codec, an encoding and a page size actually cost.

The compression, encoding and page dimensions need numbers the footers cannot
supply: what would this column cost under a codec it is *not* currently written
with, under an encoding it does not currently use, at a page size nobody has
tried. L1 has no way to derive any of that -- they are properties of the data
and of the writer build -- so the honest options are to measure them or to
leave the dimension alone. This measures them.

Three properties of the measurement matter more than the numbers.

*Codec and encoding are measured jointly, never multiplied.* A dictionary that
collapses a column to a handful of distinct values leaves the codec almost
nothing to do, so `ratio(zstd) x ratio(DELTA)` is not `ratio(zstd, DELTA)`. The
old shape of this file reported two independent ratio maps and invited exactly
that multiplication. Now every measured point is one (codec, encoding) tuple
and a candidate that names a tuple nobody measured is `unpriced`, not 1.0.

*A requested encoding that silently downgrades is detected, not recorded as a
win.* Parquet writers fall back to PLAIN when a type does not support the
family asked for, which reads afterwards as "encoding did not help". Each write
is read back and the requested family is checked against the footer.

*Page size is measured on the whole sample, not per column.* Page geometry pays
off through cross-column range merging and OffsetIndex granularity, neither of
which exists in a single-column buffer. The page pass also detects the writer's
no-op ceiling: on PyArrow 25 a `data_page_size` at or above the 1 MiB default
produces a byte-identical file, so pricing a gain for it would be fiction.

Still deliberately not done here: timing the codecs. A single-threaded
in-process *write* is not a proxy for decode cost, so the rates live in
`decode_probe.py`, which times reads instead. What this probe owes that model
is the denominator: every joint point records `encoded_bytes` (the footer's
`total_uncompressed_size`), because decode is paid per byte handed to the
decoder and an encoding moves that number independently of the wire bytes.

Usage:
  python3 tools/track2/compression_probe.py \
      --layout s3a://bucket/track2/clickbench_sf1 \
      --dataset-snapshot docs/.../dataset_snapshot.json \
      --out docs/.../layout_probe.json

Pass the dataset snapshot. The page pass sizes its sample to the measured rows
per row group, and without that it writes a sample too small for the page
limit to bind: every ladder point looks like a no-op and the page axis comes
back unpriced.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import advisor_policy as policy  # noqa: E402
import layout_actions as la  # noqa: E402
from dataset_snapshot import _normalise, list_files, list_tables  # noqa: E402
from parse_footer import _open_filesystem  # noqa: E402

DEFAULT_SAMPLE_ROWS = 200_000
# Below this relative byte saving the switch is inside sampling error.
MIN_SWITCH_MARGIN = 0.02
# Schema version. v1 reported independent `ratios` / `encoding_ratios` maps;
# v2 reports joint (codec, encoding) tuples plus a page pass; v3 adds
# `encoded_bytes` per tuple so the decode term has a denominator. Consumers
# refuse a v1 document for encoding pricing rather than multiplying the old
# maps, and treat a v2 document as unpriced for decode rather than reading the
# compressed ratio as if it were the encoded one.
SCHEMA_VERSION = 3
# The page pass writes its sample several times over; cap it so a table with
# very large row groups cannot turn the probe into a rewrite.
MAX_PAGE_SAMPLE_ROWS = 800_000
# "Leave the writer's default alone" as an encoding choice, so the baseline is
# a measured point in the joint grid rather than an implied one.
BASELINE_ENCODING = "baseline"


def sample_table(fs, files, sample_rows):
    """Read up to sample_rows from the first files of a table."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    batches = []
    rows = 0
    for path, _size in files:
        with fs.open_input_file(path) as handle:
            pf = pq.ParquetFile(handle)
            for batch in pf.iter_batches(batch_size=65536):
                batches.append(batch)
                rows += batch.num_rows
                if rows >= sample_rows:
                    break
        if rows >= sample_rows:
            break
    if not batches:
        return None
    return pa.Table.from_batches(batches).slice(0, sample_rows)


def _encoding_landed(requested, seen):
    """Did the writer actually use the family we asked for?

    A fallback to PLAIN is the failure mode this whole check exists for: the
    file is written, the bytes look unremarkable, and the experiment records
    "encoding did not help" for an encoding that was never applied.
    """
    if requested == BASELINE_ENCODING:
        return True
    seen = {str(e).upper() for e in seen}
    if requested == "RLE_DICTIONARY":
        return bool(seen & {"RLE_DICTIONARY", "PLAIN_DICTIONARY"})
    return requested.upper() in seen


def probe_column(table, column, codecs, encodings):
    """Bytes for one column under each legal (codec, encoding) tuple.

    Returns joint points only. Nothing here may be recombined by multiplying a
    codec ratio with an encoding ratio: the two interact through the dictionary
    and that product is not a measurement of anything.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    single = pa.table({column: table.column(column)})
    field = single.schema.field(column)
    ptype = _physical_type(field)

    def write(codec, encoding):
        """(bytes, encoded_bytes, fallback), or None when the writer refused.

        `encoded_bytes` is the footer's `total_uncompressed_size`: what the
        codec hands the decoder. It is the denominator decode cost is paid
        against, and it is not derivable from the compressed number -- an
        encoding moves it while a codec does not.
        """
        kwargs = {"compression": ("NONE" if codec == "uncompressed" else codec)}
        if encoding == BASELINE_ENCODING:
            pass
        elif encoding == "RLE_DICTIONARY":
            kwargs["use_dictionary"] = True
        else:
            # column_encoding is ignored while the dictionary is on, so an
            # explicit family has to turn it off or the request disappears.
            kwargs["use_dictionary"] = False
            kwargs["column_encoding"] = {column: encoding}
        buf = io.BytesIO()
        try:
            pq.write_table(single, buf, write_page_index=True, **kwargs)
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid, OSError, ValueError):
            return None
        nbytes = buf.tell()
        buf.seek(0)
        encoded = None
        try:
            col = pq.ParquetFile(buf).metadata.row_group(0).column(0)
            landed = _encoding_landed(encoding, col.encodings)
            encoded = col.total_uncompressed_size
        except (pa.ArrowInvalid, OSError, IndexError):
            landed = None
        return nbytes, encoded, (landed is False)

    joint = []
    pruned = []
    for encoding in encodings:
        allowed = la.ENCODING_PHYSICAL_TYPES.get(encoding)
        if allowed and ptype not in allowed:
            pruned.append({"encoding": encoding, "reason":
                           f"physical type {ptype} not in {sorted(allowed)}"})
            continue
        for codec in codecs:
            got = write(codec, encoding)
            if got is None:
                pruned.append({"encoding": encoding, "codec": codec,
                               "reason": "writer refused the combination"})
                continue
            nbytes, encoded, fallback = got
            if fallback:
                pruned.append({"encoding": encoding, "codec": codec,
                               "reason": "writer fell back to another family"})
                continue
            joint.append({"codec": codec, "encoding": encoding, "bytes": nbytes,
                          "encoded_bytes": encoded})

    results = {"physical_type": ptype, "joint": joint, "pruned": pruned}
    base = next((p["bytes"] for p in joint
                 if p["codec"] == policy.BASELINE_CODEC
                 and p["encoding"] == BASELINE_ENCODING), None)
    results["baseline_bytes"] = base
    if base:
        for point in joint:
            point["ratio"] = round(point["bytes"] / base, 4)
    # Two ratios against the same baseline tuple, because the two axes of the
    # cost model consume different bytes: transfer pays for `ratio` (wire
    # bytes) and decode pays for `encoded_ratio` (bytes handed to the
    # decoder). An encoding can shrink one and grow the other, which is
    # exactly the trade L1 has to see.
    base_encoded = next((p.get("encoded_bytes") for p in joint
                         if p["codec"] == policy.BASELINE_CODEC
                         and p["encoding"] == BASELINE_ENCODING), None)
    results["baseline_encoded_bytes"] = base_encoded
    if base_encoded:
        for point in joint:
            if point.get("encoded_bytes"):
                point["encoded_ratio"] = round(
                    point["encoded_bytes"] / base_encoded, 4)
    raw = next((p["bytes"] for p in joint
                if p["codec"] == "uncompressed"
                and p["encoding"] == BASELINE_ENCODING), None)
    results["uncompressed_bytes"] = raw
    return results


def joint_lookup(rec):
    """(codec, encoding) -> ratio against the baseline tuple."""
    return {(p["codec"], p["encoding"]): p["ratio"]
            for p in (rec.get("joint") or []) if p.get("ratio") is not None}


def probe_page_geometry(sample, ladder, default_page_bytes, codec):
    """Page-size pass over the whole sample: index bytes, and the no-op ceiling.

    Writes the full sampled table as one row group at each ladder point and
    splits the result into column-chunk bytes and everything else (footer,
    OffsetIndex, ColumnIndex). The second number is what a finer page costs:
    it lands in the per-scan metadata traffic L1 already prices separately.

    A ladder point whose file is byte-identical to the default is recorded as
    `noop: true`. On PyArrow 25 every `data_page_size` at or above the 1 MiB
    default is such a point, and offering one as a candidate would be pricing
    a write that never happens.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    n_rows = sample.num_rows

    def write(page_bytes=None):
        kwargs = {"compression": codec, "write_page_index": True}
        if page_bytes:
            kwargs["data_page_size"] = int(page_bytes)
        buf = io.BytesIO()
        try:
            writer = pq.ParquetWriter(buf, sample.schema, **kwargs)
            writer.write_table(sample, row_group_size=n_rows)
            writer.close()
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid, OSError, ValueError):
            return None
        total = buf.tell()
        buf.seek(0)
        md = pq.ParquetFile(buf).metadata
        rg = md.row_group(0)
        chunk = sum(rg.column(c).total_compressed_size for c in range(md.num_columns))
        uncompressed = [rg.column(c).total_uncompressed_size
                        for c in range(md.num_columns)]
        return {"file_bytes": total, "chunk_bytes": chunk,
                "footer_index_bytes": total - chunk,
                "column_uncompressed": uncompressed}

    base = write(None)
    if base is None:
        return None

    def est_pages(page_bytes):
        if not page_bytes:
            page_bytes = default_page_bytes
        return sum(max(1, -(-b // int(page_bytes)))
                   for b in base["column_uncompressed"])

    base_pages = est_pages(default_page_bytes)
    points = []
    per_page = []
    for page_bytes in sorted(set(ladder)):
        rec = write(page_bytes)
        if rec is None:
            continue
        noop = rec["file_bytes"] == base["file_bytes"]
        pages = est_pages(page_bytes)
        delta_pages = pages - base_pages
        delta_index = rec["footer_index_bytes"] - base["footer_index_bytes"]
        # Both directions carry information: fewer pages shrink the index by
        # the same per-page cost that more pages grow it. Only a point that
        # moved no pages at all is useless for the fit.
        if delta_pages and not noop:
            per_page.append(delta_index / delta_pages)
        points.append({
            "page_bytes": page_bytes,
            "file_bytes": rec["file_bytes"],
            "chunk_bytes": rec["chunk_bytes"],
            "footer_index_bytes": rec["footer_index_bytes"],
            "estimated_pages": pages,
            "noop": noop,
        })

    # The fit needs at least one ladder point that actually split a page. If
    # every point came back byte-identical, the sample was too small for the
    # page limit to bind and "the writer ignored this" cannot be told apart
    # from "one page was always going to be enough".
    resolved = bool(per_page)
    # Per point, never "everything at or above X". Whether a page size is a
    # no-op depends on how much data a column holds in one row group: on a
    # two-column toy sample every size above the default is ignored, but on a
    # 105-column row group of 585k rows a 4 MiB page really does merge pages
    # and shrink the index. Only the sizes measured as no-ops are dropped.
    noop_pages = sorted(p["page_bytes"] for p in points if p["noop"])
    return {
        "resolved": resolved,
        "insufficient_sample": not resolved,
        "noop_page_bytes": noop_pages,
        "n_columns": sample.num_columns,
        "sample_rows": n_rows,
        "codec": codec,
        "default_page_bytes": default_page_bytes,
        "baseline": {k: v for k, v in base.items() if k != "column_uncompressed"},
        "baseline_estimated_pages": base_pages,
        "points": points,
        "index_bytes_per_page": (round(sum(per_page) / len(per_page), 2)
                                 if per_page and resolved else None),
        "pages_per_column": round(base_pages / max(sample.num_columns, 1), 2),
        "note": ("footer_index_bytes is footer + OffsetIndex + ColumnIndex for "
                 "one row group of the sample; a byte-identical file means the "
                 "writer ignored the requested page size"),
    }


def rows_per_row_group(snapshot_path, table):
    """Measured rows per row group, so the page pass writes a realistic one.

    Page limits bind on per-column bytes inside one row group. A sample much
    smaller than a real row group puts every column in a single page, the
    ladder collapses to "no-op everywhere", and the measurement describes a
    page regime the table never enters.
    """
    try:
        with open(snapshot_path) as fh:
            geom = (json.load(fh).get("geometry") or {}).get(table) or {}
    except (OSError, ValueError):
        return None
    n_rows = geom.get("n_rows")
    n_rg = geom.get("n_rg")
    if not n_rows or not n_rg:
        return None
    return max(1, int(n_rows // n_rg))


def _physical_type(field):
    """Arrow type -> the Parquet physical type it maps onto."""
    import pyarrow as pa
    t = field.type
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_floating(t):
        return "FLOAT" if pa.types.is_float32(t) else "DOUBLE"
    if pa.types.is_integer(t):
        return "INT32" if t.bit_width <= 32 else "INT64"
    if pa.types.is_date(t) or pa.types.is_time32(t):
        return "INT32"
    if pa.types.is_timestamp(t) or pa.types.is_time64(t):
        return "INT64"
    if pa.types.is_fixed_size_binary(t) or pa.types.is_decimal(t):
        return "FIXED_LEN_BYTE_ARRAY"
    return "BYTE_ARRAY"


def choose(results, baseline=policy.BASELINE_CODEC, margin=MIN_SWITCH_MARGIN):
    """Best codec at the writer's default encoding, if it clears the margin.

    Codec-only, because this is what the compression axis alone may claim. The
    joint winner (codec *and* encoding together) is chosen by `choose_joint`,
    and the two are reported separately so a plan cannot take the joint saving
    while emitting only a codec action.
    """
    lookup = joint_lookup(results)
    ratios = {codec: ratio for (codec, encoding), ratio in lookup.items()
              if encoding == BASELINE_ENCODING}
    if not ratios:
        return None, None
    base_ratio = ratios.get(baseline)
    candidates = {c: r for c, r in ratios.items() if c in policy.CODEC_LADDER}
    if not candidates:
        return None, base_ratio
    best = min(candidates, key=lambda c: candidates[c])
    if base_ratio is not None and best != baseline:
        if (base_ratio - candidates[best]) / max(base_ratio, 1e-9) < margin:
            return baseline, base_ratio
    return best, candidates[best]


def choose_joint(results, margin=MIN_SWITCH_MARGIN):
    """Cheapest measured (codec, encoding) tuple, if it clears the margin."""
    lookup = joint_lookup(results)
    if not lookup:
        return None
    usable = {k: v for k, v in lookup.items()
              if k[0] in policy.CODEC_LADDER}
    if not usable:
        return None
    codec, encoding = min(usable, key=lambda k: usable[k])
    ratio = usable[(codec, encoding)]
    if ratio > 1.0 - margin:
        return None
    return {"codec": codec, "encoding": encoding, "ratio": ratio}


def build(layout, tables=None, sample_rows=DEFAULT_SAMPLE_ROWS,
          codecs=None, encodings=None, page_ladder=None, skip_page=False,
          page_rows=None):
    fs, base = _open_filesystem(_normalise(layout))
    found = list_tables(fs, base) or {os.path.basename(base.rstrip("/")): base}
    if tables:
        found = {t: p for t, p in found.items() if t in set(tables)}
    codecs = list(codecs or (("uncompressed",) + policy.CODEC_LADDER))
    encodings = [BASELINE_ENCODING] + [e for e in (encodings or la.ENCODINGS)
                                       if e != BASELINE_ENCODING]
    page_ladder = list(page_ladder or policy.PAGE_BYTES_LADDER)

    out = {}
    for table, path in sorted(found.items()):
        files = list_files(fs, path)
        if not files:
            continue
        sample = sample_table(fs, files, sample_rows)
        if sample is None:
            continue
        cols = {}
        for column in sample.column_names:
            results = probe_column(sample, column, codecs, encodings)
            best_codec, best_ratio = choose(results)
            cols[column] = {
                "physical_type": results["physical_type"],
                "joint": results["joint"],
                "pruned": results["pruned"],
                "baseline_bytes": results["baseline_bytes"],
                "uncompressed_bytes": results["uncompressed_bytes"],
                "best_codec": best_codec,
                "best_ratio": best_ratio,
                "best_joint": choose_joint(results),
                "incompressible": (best_ratio is not None
                                   and best_ratio > policy.INCOMPRESSIBLE_RATIO),
            }
        rec = {"sample_rows": sample.num_rows, "columns": cols}
        if not skip_page:
            page_sample = sample
            if page_rows and page_rows > sample.num_rows:
                page_sample = sample_table(fs, files, page_rows) or sample
            rec["page"] = probe_page_geometry(
                page_sample, page_ladder, policy.DEFAULT_PAGE_BYTES,
                policy.BASELINE_CODEC)
            if rec["page"]:
                rec["page"]["target_row_group_rows"] = page_rows
        out[table] = rec

    return {
        "schema_version": SCHEMA_VERSION,
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md r5",
        "layout": layout,
        "method": ("per-column single-column parquet writes over a row sample "
                   "for each (codec, encoding) tuple, plus a whole-sample pass "
                   "over the page ladder. Column ratios are against the "
                   "(baseline codec, writer-default encoding) tuple. `ratio` "
                   "is wire bytes; `encoded_ratio` is the footer's "
                   "total_uncompressed_size, which is what the decoder reads."),
        "sample_rows_requested": sample_rows,
        "codecs": codecs,
        "encodings": encodings,
        "page_ladder": page_ladder,
        "min_switch_margin": MIN_SWITCH_MARGIN,
        "caveat": ("bytes only: no time is measured here. The decode rates "
                   "that turn `encoded_ratio` into seconds come from "
                   "decode_probe.py, and until both are bound a codec that "
                   "wins on bytes can still lose end to end on a CPU-bound "
                   "reader. Joint tuples must not be factorised back into "
                   "independent codec/encoding ratios."),
        "tables": out,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--layout", required=True, help="layout root (local, s3:// or s3a://)")
    ap.add_argument("--tables", nargs="*", default=None)
    ap.add_argument("--sample-rows", type=int, default=DEFAULT_SAMPLE_ROWS)
    ap.add_argument("--skip-page", action="store_true",
                    help="codec/encoding tuples only; leave the page axis unpriced")
    ap.add_argument("--dataset-snapshot", default=None,
                    help="dataset_snapshot.py output; the page pass writes a "
                         "sample the size of a real row group so the page "
                         "limit binds the way it will in the candidate")
    ap.add_argument("--page-rows", type=int, default=None,
                    help="override the page pass row count")
    ap.add_argument("--max-page-rows", type=int, default=MAX_PAGE_SAMPLE_ROWS)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    page_rows = args.page_rows
    if page_rows is None and args.dataset_snapshot:
        table = (args.tables or [None])[0]
        page_rows = rows_per_row_group(args.dataset_snapshot, table)
        if page_rows:
            print(f"  page pass sized at {page_rows} rows "
                  f"(measured rows per row group)")
    if page_rows:
        page_rows = min(page_rows, args.max_page_rows)

    doc = build(args.layout, args.tables, args.sample_rows,
                skip_page=args.skip_page, page_rows=page_rows)
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(doc, fh, indent=2)

    print(f"# layout probe: {args.layout}")
    for table, rec in doc["tables"].items():
        cols = rec["columns"]
        switched = [c for c, r in cols.items()
                    if r.get("best_codec") not in (None, policy.BASELINE_CODEC)]
        joint = [c for c, r in cols.items() if r.get("best_joint")]
        incompressible = [c for c, r in cols.items() if r.get("incompressible")]
        fallback = sum(1 for r in cols.values()
                       for p in (r.get("pruned") or [])
                       if "fell back" in p.get("reason", ""))
        tuples = sum(len(r.get("joint") or []) for r in cols.values())
        print(f"  {table:12s} {rec['sample_rows']} rows sampled, {len(cols)} columns")
        print(f"    joint tuples measured:   {tuples}")
        print(f"    switch off {policy.BASELINE_CODEC}:     {len(switched)}")
        print(f"    joint winner != baseline: {len(joint)}")
        print(f"    encoding fallbacks:      {fallback}")
        print(f"    incompressible:          {len(incompressible)}")
        page = rec.get("page")
        if page:
            live = [p for p in page["points"] if not p["noop"]]
            noop = [p for p in page["points"] if p["noop"]]
            dead = [b // 1024 for b in (page.get("noop_page_bytes") or [])]
            print(f"    page: {page['sample_rows']} rows, "
                  f"{page.get('pages_per_column')} pages/column at the default")
            print(f"          {len(live)} live point(s), {len(noop)} no-op; "
                  f"index {page.get('index_bytes_per_page')} B/page; "
                  f"no-op at {dead or 'none'} KiB")
            if not page.get("resolved"):
                print("          NOT RESOLVED: every ladder point came back "
                      "byte-identical; page axis stays unpriced")
    if args.out:
        print(f"  out          {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
