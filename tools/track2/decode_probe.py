#!/usr/bin/env python3
"""Measure decode cost per (codec, encoding, physical type). Measurement only.

`compression_probe.py` measures what a layout costs in *bytes* and explicitly
refuses to time anything. That leaves the hole `advisor_policy.DECODE_MODELLED
= False` admits: L1 prices a codec change through the bandwidth term alone, so
zstd looks free and a CPU-bound reader is invisible to the search.

This file measures the missing constant and nothing else. It does not import
whatif, it does not read a plan, and it does not flip DECODE_MODELLED. A rate
that has not been checked against an independent measurement should not be
ranking candidates.

Four decisions make the number mean something.

*The reader that will read the candidate is the reader that gets timed.* UC1
reads with PyArrow and UC2 with Spark/parquet-mr, and their zstd bindings and
DELTA implementations are different code with different throughput. One rate
for "decode" would be a proxy for whichever engine happened to be measured, so
both are timed separately and reported separately.

*The rate denominates in uncompressed bytes.* Decode work scales with the bytes
handed to the reader, not with the bytes fetched. Compressed bytes are recorded
alongside so the two can be related, never conflated.

*Decode is timed on tmpfs, single-threaded.* The file is in RAM, so no disk or
network time is inside the number, and one thread makes the result a per-core
rate -- which is the form L1 needs, since its `t_io` is already core-seconds
divided by K_eff.

*Size is reached by repeating the sample across independent row groups.* A
200k-row sample of a narrow column decodes faster than a Spark job starts, and
the rate would be measuring the scheduler. Each row group is encoded and
compressed independently, so M identical row groups scale both the uncompressed
and the compressed total by exactly M and leave the ratio -- and therefore the
rate -- intact. The per-job cost is measured on a tiny file and subtracted, and
a case whose signal does not clear it by MIN_SIGNAL_RATIO is marked unreliable
rather than reported as a rate.

A requested encoding that silently fell back to PLAIN is dropped, not timed.
Timing it would record PLAIN's throughput under another family's name, which is
the same failure compression_probe guards against on the byte side.

Usage:
  python3 tools/track2/decode_probe.py \
      --layout s3a://bucket/track2/clickbench_sf1 --tables hits \
      --layout-probe docs/.../layout_probe.json \
      --out docs/.../decode_probe.json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import advisor_policy as policy  # noqa: E402
import layout_actions as la  # noqa: E402
from compression_probe import (  # noqa: E402
    BASELINE_ENCODING, _encoding_landed, sample_table)
from dataset_snapshot import _normalise, list_files, list_tables  # noqa: E402
from parse_footer import _open_filesystem  # noqa: E402

SCHEMA_VERSION = 1
DEFAULT_SAMPLE_ROWS = 200_000
# Uncompressed bytes per timed case. Large enough that a fast column still
# takes far longer to decode than a Spark job takes to start.
DEFAULT_TARGET_MIB = 192
# Only binds for narrow columns, where writing many row groups is cheap.
DEFAULT_MAX_ROW_GROUPS = 512
# Reaching a byte target on a 4-byte column needs tens of millions of rows, and
# parquet-mr's sink cost is per row: 48M rows of INT32 spend most of the
# measurement in row materialisation rather than decode. Capping rows keeps the
# constant term from swallowing the signal; the case records both `rows` and
# `uncompressed_bytes` so a two-term fit can separate them later.
DEFAULT_MAX_ROWS = 8_000_000
DEFAULT_REPEATS = 3
DEFAULT_COLUMNS_PER_TYPE = 2
DEFAULT_WORK_DIR = "/tmp/track2-decode"
# A timed read has to be this many times the fixed per-job cost, or the rate is
# mostly the scheduler and is reported as unreliable.
MIN_SIGNAL_RATIO = 3.0
# Rows in the file used to measure the fixed per-job cost.
OVERHEAD_ROWS = 512


def encoding_kwargs(column, codec, encoding):
    """Writer arguments for one (codec, encoding) tuple."""
    kwargs = {"compression": ("NONE" if codec == "uncompressed" else codec),
              "write_page_index": True}
    if encoding == BASELINE_ENCODING:
        return kwargs
    if encoding == "RLE_DICTIONARY":
        kwargs["use_dictionary"] = True
        return kwargs
    # column_encoding is ignored while the dictionary is on, so an explicit
    # family has to turn it off or the request disappears.
    kwargs["use_dictionary"] = False
    kwargs["column_encoding"] = {column: encoding}
    return kwargs


def footer_stats(path):
    """Physical type, byte totals and encodings actually written."""
    import pyarrow.parquet as pq
    md = pq.ParquetFile(path).metadata
    ptype = md.schema.column(0).physical_type
    uncompressed = compressed = 0
    encodings = set()
    for i in range(md.num_row_groups):
        col = md.row_group(i).column(0)
        uncompressed += col.total_uncompressed_size
        compressed += col.total_compressed_size
        encodings.update(col.encodings or ())
    return {"physical_type": ptype, "uncompressed_bytes": uncompressed,
            "compressed_bytes": compressed, "encodings": sorted(encodings),
            "n_row_groups": md.num_row_groups, "rows": md.num_rows}


def write_case(path, single, column, codec, encoding, target_bytes, max_rgs,
               max_rows=DEFAULT_MAX_ROWS):
    """Write M independent row groups of one column, or None if refused."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    per_rg = max(int(single.nbytes), 1)
    by_rows = max(1, max_rows // max(single.num_rows, 1))
    n_rgs = max(1, min(max_rgs, by_rows, -(-target_bytes // per_rg)))
    kwargs = encoding_kwargs(column, codec, encoding)
    try:
        writer = pq.ParquetWriter(path, single.schema, **kwargs)
        for _ in range(n_rgs):
            writer.write_table(single, row_group_size=single.num_rows)
        writer.close()
    except (pa.ArrowNotImplementedError, pa.ArrowInvalid, OSError, ValueError) as exc:
        if os.path.exists(path):
            os.unlink(path)
        return None, f"writer refused the combination: {type(exc).__name__}"
    stats = footer_stats(path)
    if _encoding_landed(encoding, stats["encodings"]) is False:
        os.unlink(path)
        return None, "writer fell back to another family"
    return stats, None


def time_pyarrow(path, repeats):
    """Single-threaded PyArrow read: wall and process CPU, median of repeats."""
    import pyarrow.parquet as pq

    def once():
        t0, c0 = time.perf_counter(), time.process_time()
        pq.read_table(path, use_threads=False)
        return time.perf_counter() - t0, time.process_time() - c0

    once()  # warm the allocator and the page cache entry
    walls, cpus = [], []
    for _ in range(repeats):
        wall, cpu = once()
        walls.append(wall)
        cpus.append(cpu)
    return {"wall_s": statistics.median(walls), "cpu_s": statistics.median(cpus),
            "wall_samples": [round(w, 4) for w in walls]}


def spark_session(driver_memory):
    """local[1] so the measurement is one core, with the frozen reader's
    vectorized decode path pinned rather than left to a default."""
    from pyspark.sql import SparkSession
    import s3a_session
    spark = (SparkSession.builder
             .master("local[1]")
             .appName("track2-decode-probe")
             .config("spark.driver.memory", driver_memory)
             .config("spark.local.dir", s3a_session.spark_scratch())
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.parquet.enableVectorizedReader", "true")
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def time_parquet_mr(spark, path, repeats):
    """Time a full materialisation through parquet-mr.

    The `noop` sink exists for exactly this: it forces every value to be
    decoded without paying for a write or a collect back to the driver.
    """
    def once():
        t0 = time.perf_counter()
        (spark.read.parquet(f"file://{path}")
         .write.format("noop").mode("overwrite").save())
        return time.perf_counter() - t0

    once()  # let the JIT compile the decode path before it is timed
    walls = [once() for _ in range(repeats)]
    return {"wall_s": statistics.median(walls),
            "wall_samples": [round(w, 4) for w in walls]}


def pick_columns(sample, probe_doc, table, per_type, columns):
    """Representative columns per physical type, heaviest first.

    Byte weight decides, because a rate fitted on a column nobody reads would
    be averaged into the same table as the rate for the column that dominates
    the scan.
    """
    if columns:
        wanted = [c for c in columns if c in sample.column_names]
        return [(c, None) for c in wanted]
    probed = ((probe_doc or {}).get("tables") or {}).get(table, {}).get("columns") or {}
    by_type = {}
    for name in sample.column_names:
        rec = probed.get(name) or {}
        ptype = rec.get("physical_type")
        weight = rec.get("uncompressed_bytes") or rec.get("baseline_bytes") or 0
        by_type.setdefault(ptype, []).append((name, weight))
    out = []
    for ptype in sorted(by_type, key=lambda t: str(t)):
        ranked = sorted(by_type[ptype], key=lambda p: -p[1])
        out.extend((name, ptype) for name, _w in ranked[:per_type])
    return out


def rate_bps(uncompressed_bytes, seconds):
    if not seconds or seconds <= 0:
        return None
    return uncompressed_bytes / seconds


def aggregate(cases, readers):
    """Median rate per (physical type, codec, encoding), and the cost ratio
    against the baseline tuple *of the same column*.

    The ratio is paired per column and only then medianed. Dividing one median
    rate by another mixes column sets whenever a case was dropped as
    unreliable, and then reports the difference between two columns as if it
    were the effect of an encoding.
    """
    rates = {}
    for reader in readers:
        rate_of = {}
        for case in cases:
            rec = case.get(reader) or {}
            if not rec.get("reliable") or not rec.get("bytes_per_s"):
                continue
            rate_of[(case["column"], case["codec"], case["encoding"])] = (
                case["physical_type"], rec["bytes_per_s"])
        buckets = {}
        for (column, codec, encoding), (ptype, bps) in rate_of.items():
            entry = buckets.setdefault((ptype, codec, encoding),
                                       {"bps": [], "ratio": []})
            entry["bps"].append(bps)
            base = rate_of.get((column, policy.BASELINE_CODEC,
                                BASELINE_ENCODING))
            if base and bps:
                entry["ratio"].append(base[1] / bps)
        per_type = {}
        for (ptype, codec, encoding), vals in buckets.items():
            # Ranking only ever needs the cost ratio against the baseline
            # tuple; the absolute rate is carried so it can be re-checked.
            per_type.setdefault(ptype, {})[f"{codec}|{encoding}"] = {
                "bytes_per_s": int(statistics.median(vals["bps"])),
                "n_columns": len(vals["bps"]),
                "relative_cost": (round(statistics.median(vals["ratio"]), 4)
                                  if vals["ratio"] else None),
                "n_paired": len(vals["ratio"]),
            }
        rates[reader] = per_type
    return rates


def build(args):
    fs, base = _open_filesystem(_normalise(args.layout))
    found = list_tables(fs, base) or {os.path.basename(base.rstrip("/")): base}
    if args.tables:
        found = {t: p for t, p in found.items() if t in set(args.tables)}
    if not found:
        raise SystemExit(f"no tables under {args.layout}")

    probe_doc = None
    if args.layout_probe and os.path.exists(args.layout_probe):
        with open(args.layout_probe) as fh:
            probe_doc = json.load(fh)

    codecs = list(args.codecs or (("uncompressed",) + policy.CODEC_LADDER))
    encodings = [BASELINE_ENCODING] + [e for e in (args.encodings or la.ENCODINGS)
                                       if e != BASELINE_ENCODING]
    target_bytes = args.target_mib * 1024 * 1024
    readers = list(args.readers)

    work = os.path.abspath(args.work_dir)
    os.makedirs(work, exist_ok=True)
    spark = None
    overhead = None
    cases, pruned = [], []

    try:
        if "parquet-mr" in readers:
            spark = spark_session(args.driver_memory)

        for table, path in sorted(found.items()):
            files = list_files(fs, path)
            if not files:
                continue
            print(f"# sampling {args.sample_rows} rows from {table}", flush=True)
            sample = sample_table(fs, files, args.sample_rows)
            if sample is None:
                continue
            picked = pick_columns(sample, probe_doc, table,
                                  args.columns_per_type, args.columns)
            print(f"  {len(picked)} representative column(s): "
                  f"{', '.join(c for c, _t in picked)}", flush=True)

            if spark is not None and overhead is None:
                # Not a leading underscore: Hadoop's default path filter hides
                # `_*` and `.*` as metadata, and Spark then finds no files.
                tiny = os.path.join(work, "overhead.parquet")
                column = picked[0][0]
                single = sample.select([column]).slice(0, OVERHEAD_ROWS)
                stats, _err = write_case(tiny, single, column,
                                         policy.BASELINE_CODEC,
                                         BASELINE_ENCODING, 1, 1)
                if stats:
                    overhead = time_parquet_mr(spark, tiny, args.repeats)["wall_s"]
                    print(f"  parquet-mr fixed per-job cost "
                          f"{overhead * 1000:.0f} ms", flush=True)
                    os.unlink(tiny)

            for column, hint in picked:
                single = sample.select([column])
                for encoding in encodings:
                    allowed = la.ENCODING_PHYSICAL_TYPES.get(encoding)
                    if allowed and hint and hint not in allowed:
                        # Known illegal from the probe's physical type; skip
                        # the write instead of discovering it from the footer.
                        for codec in codecs:
                            pruned.append({
                                "table": table, "column": column,
                                "codec": codec, "encoding": encoding,
                                "reason": f"physical type {hint} not in "
                                          f"{sorted(allowed)}"})
                        continue
                    for codec in codecs:
                        case_path = os.path.join(work, "case.parquet")
                        if os.path.exists(case_path):
                            os.unlink(case_path)
                        stats, err = write_case(case_path, single, column, codec,
                                                encoding, target_bytes,
                                                args.max_row_groups,
                                                args.max_rows)
                        if stats is None:
                            pruned.append({"table": table, "column": column,
                                           "codec": codec, "encoding": encoding,
                                           "reason": err})
                            continue
                        ptype = stats["physical_type"]
                        if allowed and ptype not in allowed:
                            pruned.append({
                                "table": table, "column": column,
                                "codec": codec, "encoding": encoding,
                                "reason": f"physical type {ptype} not in "
                                          f"{sorted(allowed)}"})
                            os.unlink(case_path)
                            continue

                        case = {
                            "table": table,
                            "column": column,
                            "physical_type": ptype,
                            "codec": codec,
                            "encoding": encoding,
                            "n_row_groups": stats["n_row_groups"],
                            "rows": stats["rows"],
                            "uncompressed_bytes": stats["uncompressed_bytes"],
                            "compressed_bytes": stats["compressed_bytes"],
                            "encodings_written": stats["encodings"],
                        }
                        unc = stats["uncompressed_bytes"]
                        if "pyarrow" in readers:
                            rec = time_pyarrow(case_path, args.repeats)
                            secs = rec["cpu_s"] or rec["wall_s"]
                            rec["bytes_per_s"] = rate_bps(unc, secs)
                            rec["reliable"] = bool(rec["bytes_per_s"])
                            case["pyarrow"] = rec
                        if spark is not None:
                            rec = time_parquet_mr(spark, case_path, args.repeats)
                            net = rec["wall_s"] - (overhead or 0.0)
                            rec["job_overhead_s"] = overhead
                            rec["decode_s"] = net
                            rec["reliable"] = bool(
                                overhead and net > MIN_SIGNAL_RATIO * overhead)
                            rec["bytes_per_s"] = (rate_bps(unc, net)
                                                  if rec["reliable"] else None)
                            case["parquet-mr"] = rec
                        cases.append(case)
                        os.unlink(case_path)
                        pa_r = (case.get("pyarrow") or {}).get("bytes_per_s")
                        mr_r = (case.get("parquet-mr") or {}).get("bytes_per_s")
                        print(f"  {column:22.22} {codec:12} {encoding:24} "
                              f"{unc / 2 ** 20:7.1f}MiB  "
                              f"pyarrow={_gbps(pa_r)}  mr={_gbps(mr_r)}",
                              flush=True)
    finally:
        if spark is not None:
            spark.stop()
        if not args.keep_work:
            shutil.rmtree(work, ignore_errors=True)

    return {
        "schema_version": SCHEMA_VERSION,
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md r5",
        "layout": args.layout,
        "method": ("single-column parquet on tmpfs, the row sample repeated "
                   "across independent row groups to reach a target "
                   "uncompressed size; decoded single-threaded once per "
                   "reader. Rates are uncompressed bytes per second per core. "
                   "parquet-mr subtracts a measured fixed per-job cost."),
        "readers": readers,
        "sample_rows_requested": args.sample_rows,
        "target_uncompressed_bytes": target_bytes,
        "max_row_groups": args.max_row_groups,
        "max_rows": args.max_rows,
        "repeats": args.repeats,
        "parquet_mr_job_overhead_s": overhead,
        "min_signal_ratio": MIN_SIGNAL_RATIO,
        "reader_conf": {
            "pyarrow": {"use_threads": False},
            "parquet-mr": {"master": "local[1]",
                           "spark.sql.parquet.enableVectorizedReader": "true",
                           "sink": "noop"},
        },
        "caveat": ("measurement only: nothing here is wired into L1 and "
                   "advisor_policy.DECODE_MODELLED stays False until a rate "
                   "has been checked against an independent measurement (the "
                   "per-task Executor CPU Time in the benchmark event logs). "
                   "The parquet-mr number is scan plus materialisation into "
                   "the noop sink, not decode alone. That cost is per row and "
                   "is present in both sides of a ratio, so parquet-mr "
                   "relative_cost is a *lower bound* on the codec/encoding "
                   "effect -- most so on narrow columns, where the constant "
                   "term dominates. PyArrow's read_table has no sink and is "
                   "the cleaner per-byte signal. Repeated row groups keep "
                   "the compression ratio exact but leave the CPU caches "
                   "warmer than a real scan would. Absolute rates are machine "
                   "constants; only relative_cost belongs in a ranking."),
        "rates": aggregate(cases, readers),
        "cases": cases,
        "pruned": pruned,
    }


def _gbps(value):
    if not value:
        return "     n/a"
    return f"{value / 1e9:6.2f}GB/s"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--layout", required=True,
                    help="layout root to sample real rows from")
    ap.add_argument("--tables", nargs="*", default=None)
    ap.add_argument("--layout-probe", default=None,
                    help="compression_probe.py output; supplies per-column "
                         "physical type and byte weight so representative "
                         "columns are picked without re-probing them")
    ap.add_argument("--columns", nargs="*", default=None,
                    help="measure exactly these columns instead of picking")
    ap.add_argument("--columns-per-type", type=int,
                    default=DEFAULT_COLUMNS_PER_TYPE)
    ap.add_argument("--codecs", nargs="*", default=None)
    ap.add_argument("--encodings", nargs="*", default=None)
    ap.add_argument("--readers", nargs="+", default=["pyarrow", "parquet-mr"],
                    choices=("pyarrow", "parquet-mr"))
    ap.add_argument("--sample-rows", type=int, default=DEFAULT_SAMPLE_ROWS)
    ap.add_argument("--target-mib", type=int, default=DEFAULT_TARGET_MIB,
                    help="uncompressed bytes per timed case")
    ap.add_argument("--max-row-groups", type=int, default=DEFAULT_MAX_ROW_GROUPS)
    ap.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS,
                    help="row ceiling per case; parquet-mr's sink cost is per "
                         "row, so a narrow column must not be grown to tens "
                         "of millions of rows to hit a byte target")
    ap.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    ap.add_argument("--work-dir", default=DEFAULT_WORK_DIR,
                    help="must be tmpfs, or disk time lands in the rate")
    ap.add_argument("--keep-work", action="store_true")
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    doc = build(args)
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".",
                    exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(doc, fh, indent=2)

    print(f"\n# decode rates (uncompressed bytes/s per core)")
    for reader, per_type in (doc.get("rates") or {}).items():
        print(f"\n  {reader}")
        for ptype in sorted(per_type):
            print(f"    {ptype}")
            for key in sorted(per_type[ptype]):
                rec = per_type[ptype][key]
                rel = rec.get("relative_cost")
                mark = "baseline" if rel == 1.0 else (
                    f"{rel:.2f}x baseline cost" if rel else "unpaired")
                print(f"      {key:32} {rec['bytes_per_s'] / 1e9:6.2f} GB/s  "
                      f"n={rec['n_columns']}/{rec.get('n_paired')}  {mark}")
    unreliable = [c for c in doc["cases"]
                  if not (c.get("parquet-mr") or {}).get("reliable", True)]
    print(f"\n  cases          {len(doc['cases'])}")
    print(f"  pruned         {len(doc['pruned'])}")
    print(f"  mr unreliable  {len(unreliable)} (signal under "
          f"{MIN_SIGNAL_RATIO}x the per-job cost)")
    if unreliable:
        print(f"                 raise --target-mib above {args.target_mib} "
              f"to lift these above the scheduler")
    print(f"  DECODE_MODELLED still {policy.DECODE_MODELLED}; this file only "
          f"measures")
    if args.out:
        print(f"  out            {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
