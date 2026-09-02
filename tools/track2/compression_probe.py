#!/usr/bin/env python3
"""Measure per-column compressibility by writing sample files, not by guessing.

The compression and encoding dimensions need one number the footers cannot
supply: what would this column cost under a codec it is *not* currently written
with. L1 has no way to derive that -- compressibility is a property of the data,
not of the layout -- so the honest options are to measure it or to leave the
dimension alone. This measures it.

The method is a small L2 experiment rather than a model. Read a sample of row
groups, and for each column write it out under each codec/encoding combination
to an in-memory buffer, recording the resulting bytes. The ratio against the
uncompressed size extrapolates to the full table, which is exactly the
assumption a sampled measurement makes and the reason `sample_rows` is reported
next to every ratio.

Two things it deliberately does not do. It does not time the codecs: decode CPU
matters (advisor_policy.DECODE_MODELLED = False admits L1 ignores it) but a
single-threaded in-process write is not a credible proxy for decode cost under
a 16-way reader. And it does not pick a winner on bytes alone below a margin --
a 2% byte saving is inside the sampling error and switching for it would be
noise dressed as a recommendation.

Usage:
  python3 tools/track2/compression_probe.py \
      --layout s3a://bucket/track2/clickbench_sf1 \
      --out docs/.../compression_probe.json
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


def probe_column(table, column, codecs, encodings):
    """Bytes for one column under each codec, and under each legal encoding."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    single = pa.table({column: table.column(column)})
    field = single.schema.field(column)
    results = {"codecs": {}, "encodings": {}}

    def write(**kwargs):
        buf = io.BytesIO()
        try:
            pq.write_table(single, buf, write_page_index=True, **kwargs)
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid, OSError, ValueError):
            return None
        return buf.tell()

    for codec in codecs:
        nbytes = write(compression=("NONE" if codec == "uncompressed" else codec))
        if nbytes is not None:
            results["codecs"][codec] = nbytes

    raw = results["codecs"].get("uncompressed")
    for encoding in encodings:
        allowed = la.ENCODING_PHYSICAL_TYPES.get(encoding)
        if allowed and _physical_type(field) not in allowed:
            continue
        if encoding == "RLE_DICTIONARY":
            nbytes = write(compression="NONE", use_dictionary=True)
        else:
            nbytes = write(compression="NONE", use_dictionary=False,
                           column_encoding={column: encoding})
        if nbytes is not None:
            results["encodings"][encoding] = nbytes

    if raw:
        results["ratios"] = {c: round(b / raw, 4)
                             for c, b in results["codecs"].items()}
        results["encoding_ratios"] = {e: round(b / raw, 4)
                                      for e, b in results["encodings"].items()}
    return results


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


def choose(results, baseline=policy.BASELINE_CODEC,
           margin=MIN_SWITCH_MARGIN):
    """Best codec, but only if it beats the baseline by more than the margin."""
    ratios = results.get("ratios") or {}
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


def build(layout, tables=None, sample_rows=DEFAULT_SAMPLE_ROWS,
          codecs=None, encodings=None):
    fs, base = _open_filesystem(_normalise(layout))
    found = list_tables(fs, base) or {os.path.basename(base.rstrip("/")): base}
    if tables:
        found = {t: p for t, p in found.items() if t in set(tables)}
    codecs = list(codecs or (("uncompressed",) + policy.CODEC_LADDER))
    encodings = list(encodings or la.ENCODINGS)

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
            enc = results.get("encoding_ratios") or {}
            best_encoding = min(enc, key=lambda e: enc[e]) if enc else None
            cols[column] = {
                "physical_type": _physical_type(sample.schema.field(column)),
                "ratios": results.get("ratios"),
                "encoding_ratios": enc,
                "best_codec": best_codec,
                "best_ratio": best_ratio,
                "best_encoding": best_encoding,
                "incompressible": (best_ratio is not None
                                   and best_ratio > policy.INCOMPRESSIBLE_RATIO),
            }
        out[table] = {"sample_rows": sample.num_rows, "columns": cols}

    return {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md r5",
        "layout": layout,
        "method": ("per-column single-column parquet writes over a row sample; "
                   "ratios are against the uncompressed write of the same sample"),
        "sample_rows_requested": sample_rows,
        "codecs": codecs,
        "encodings": encodings,
        "min_switch_margin": MIN_SWITCH_MARGIN,
        "caveat": ("bytes only. Decode CPU is not measured and L1 does not "
                   "model it, so a codec that wins here can still lose end to "
                   "end on a CPU-bound reader."),
        "tables": out,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--layout", required=True, help="layout root (local, s3:// or s3a://)")
    ap.add_argument("--tables", nargs="*", default=None)
    ap.add_argument("--sample-rows", type=int, default=DEFAULT_SAMPLE_ROWS)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    doc = build(args.layout, args.tables, args.sample_rows)
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(doc, fh, indent=2)

    print(f"# compression probe: {args.layout}")
    for table, rec in doc["tables"].items():
        cols = rec["columns"]
        switched = [c for c, r in cols.items()
                    if r.get("best_codec") not in (None, policy.BASELINE_CODEC)]
        incompressible = [c for c, r in cols.items() if r.get("incompressible")]
        print(f"  {table:12s} {rec['sample_rows']} rows sampled, {len(cols)} columns")
        print(f"    switch off {policy.BASELINE_CODEC}: {len(switched)}")
        print(f"    incompressible:  {len(incompressible)}")
    if args.out:
        print(f"  out          {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
