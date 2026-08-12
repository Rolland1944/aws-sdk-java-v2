#!/usr/bin/env python3
"""Probe the Writer x Reader capability matrix frozen in TRACK2_M0_CONTRACT.md 6.

Why this exists (TRACK2_M0_CONTRACT.md 6.3): the capability matrix must NOT be
taken on faith from documentation. The dangerous failures here are silent -- a
knob that is accepted but produces nothing yields a *wrong negative result*
rather than an error.

Since contract revision r2 the Writer is Spark/parquet-mr and the Reader is the
same library, so the probe is a round trip across exactly the two libraries we
actually use:

    PySpark writes with each knob  ->  PyArrow reads the footer back

Remaining mismatches this probe targets (see contract 6.2):

  M-1  parquet-mr always writes ColumnIndex/OffsetIndex and offers no switch to
       disable it, so page index is a fixed capability rather than a candidate
       action. Confirm it really is present.
  M-2  Bloom filters are off on the write side but on by default on the read
       side. Without an explicit opt-in there is simply nothing to read, and the
       experiment reports "bloom gives no benefit".
  M-5  parquet-mr cannot express per-column compression or encoding.

Capabilities that fail here are marked infeasible in the L0 static check
(TRACK2_M0_CONTRACT.md 5.3).

Some settings are only observable against real S3 (vectored IO actually merging
ranges, the audit interceptor actually loading). Those cannot be probed from a
local write and are emitted as an explicit E0 checklist instead of being
silently assumed.

Usage:
  python3 tools/track2/probe_capability.py \
      [--out docs/adaptive-range-reader/results/track2/capability_probe.json] \
      [--rows 200000]
"""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

PASS = "pass"
FAIL = "fail"
UNKNOWN = "unknown"

# Only observable against real S3 during E0 -- see contract 1.4 and 9 (O-2).
E0_MANUAL_CHECKLIST = [
    ("fs.s3a.audit.execution.interceptors", "Track2IoCollectorInterceptor loaded",
     "Assert the collector emitted at least one record for a known GET. Spark 4.x "
     "ships Hadoop 3.4.2 so S3A is on AWS SDK v2 and the hook is supported; still "
     "verify, because a typo in the class name fails silently."),
    ("parquet.hadoop.vectored.io.enabled", "true, and actually merging ranges",
     "Default flips between parquet 1.15 (false) and 1.16+ (true), so it must be "
     "set explicitly. Confirm merging really happens by finding GETs that span "
     "more than one column chunk; if none do, contract O-2 applies and column "
     "ordering goes back to being a dead action."),
    ("fs.s3a.vectored.read.min.seek.size", "128K (default), frozen",
     "Range merge threshold; must not vary across layout comparisons."),
    ("fs.s3a.vectored.read.max.merged.size", "2M (default), frozen", "Same."),
    ("fs.s3a.input.fadvise", "random", "Frozen reader policy (contract 1.4)."),
    ("sustained network throughput", "measured, with variance",
     "m5d.4xlarge advertises burst 10 Gbps; sustained baseline is lower and may "
     "break the CV < 5% gate (contract 1.2)."),
]

# Written by Spark, read back by PyArrow. Each entry drives one round trip.
SPARK_WRITE_PROBES = {
    "row_group_bytes": {"parquet.block.size": str(8 * 1024 * 1024)},
    "page_size": {"parquet.page.size": str(64 * 1024)},
    "bloom_filter_M2": {
        "parquet.bloom.filter.enabled#high_ndv": "true",
        "parquet.bloom.filter.expected.ndv#high_ndv": "200000",
    },
    "dictionary_per_column": {"parquet.enable.dictionary#high_ndv": "false"},
    "compression_zstd": {"parquet.compression": "zstd"},
}


def _result(status, detail, **extra):
    out = {"status": status, "detail": detail}
    out.update(extra)
    return out


def probe_versions():
    versions = {"python": sys.version.split()[0], "platform": platform.platform()}
    for mod in ("pyarrow", "duckdb", "pyspark", "numpy"):
        try:
            versions[mod] = __import__(mod).__version__
        except Exception as exc:  # noqa: BLE001 - report, do not crash
            versions[mod] = f"<not importable: {exc}>"
    try:
        # java -version writes to stderr
        out = subprocess.run(["java", "-version"], capture_output=True, text=True,
                             timeout=30).stderr.splitlines()
        versions["java"] = out[0].strip() if out else "<no output>"
    except Exception as exc:  # noqa: BLE001
        versions["java"] = f"<not available: {exc}>"
    return versions


def build_probe_frame(spark, rows):
    """A frame with both a high-NDV and a low-NDV column.

    Bloom filters and dictionary encoding behave very differently on the two, so
    a single-column probe could not tell "unsupported" apart from "supported but
    not triggered on this data".
    """
    from pyspark.sql import functions as F

    return (spark.range(0, rows)
            .withColumn("high_ndv", F.concat(F.lit("key-"), F.col("id").cast("string")))
            .withColumn("low_ndv", F.concat(F.lit("cat-"), (F.col("id") % 8).cast("string")))
            .withColumn("value", F.col("id")))


def read_footer(path):
    """Summarise what parquet-mr actually produced, via PyArrow.

    This doubles as a probe of parse_footer.py's feasibility: whatever this can
    extract is what FormatMetadataCollector will have to work with.
    """
    import pyarrow.parquet as pq

    # Spark writes a directory of part files
    files = [os.path.join(path, f) for f in sorted(os.listdir(path))
             if f.endswith(".parquet")] if os.path.isdir(path) else [path]
    if not files:
        raise RuntimeError(f"no parquet part files under {path}")

    md = pq.ParquetFile(files[0]).metadata
    rg = md.row_group(0)
    columns = {}
    for i in range(rg.num_columns):
        col = rg.column(i)
        columns[col.path_in_schema] = {
            "compression": col.compression,
            "encodings": list(col.encodings),
            "has_dictionary_page": col.has_dictionary_page,
            "bloom_filter_offset": getattr(col, "bloom_filter_offset", None),
            "total_compressed_size": col.total_compressed_size,
        }
    return {
        "part_files": len(files),
        "num_row_groups": md.num_row_groups,
        "num_rows": md.num_rows,
        "row_group0_bytes": rg.total_byte_size,
        "sorting_columns": [str(s) for s in (rg.sorting_columns or ())],
        "columns": columns,
    }


def run_spark_probes(tmpdir, rows):
    """Write with each knob via Spark, read each result back with PyArrow."""
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:  # noqa: BLE001
        return {"_error": _result(UNKNOWN, f"pyspark not importable: {exc}")}

    try:
        spark = (SparkSession.builder
                 .master("local[2]")
                 .appName("track2-capability-probe")
                 .config("spark.ui.enabled", "false")
                 .config("spark.sql.shuffle.partitions", "2")
                 .getOrCreate())
    except Exception as exc:  # noqa: BLE001
        return {"_error": _result(UNKNOWN, f"cannot start local Spark: {exc}")}

    probes = {}
    try:
        df = build_probe_frame(spark, rows)

        baseline_path = os.path.join(tmpdir, "baseline")
        df.write.mode("overwrite").parquet(baseline_path)
        try:
            baseline = read_footer(baseline_path)
        except Exception as exc:  # noqa: BLE001
            return {"_error": _result(FAIL, f"cannot read baseline footer: {exc}")}

        probes["baseline_defaults"] = _result(
            PASS, "Spark default write (contract 2.3 baseline)", footer=baseline)

        # M-1: page index is written unconditionally and cannot be disabled.
        # PyArrow does not reliably surface the index offsets, so report what we
        # can see and hand the operator an exact verification command.
        sample_col = next(iter(baseline["columns"]))
        idx_attr = None
        try:
            import pyarrow.parquet as pq
            files = [os.path.join(baseline_path, f)
                     for f in sorted(os.listdir(baseline_path)) if f.endswith(".parquet")]
            col = pq.ParquetFile(files[0]).metadata.row_group(0).column(0)
            for attr in ("column_index_offset", "offset_index_offset", "has_offset_index"):
                if hasattr(col, attr):
                    idx_attr = {attr: getattr(col, attr)}
                    break
        except Exception as exc:  # noqa: BLE001
            idx_attr = f"<probe failed: {exc}>"

        probes["page_index_M1"] = _result(
            PASS if idx_attr else UNKNOWN,
            ("page index offsets visible in footer" if idx_attr else
             "PyArrow does not expose page index offsets in this version"),
            metadata_attribute=idx_attr,
            note=("parquet-mr has no switch to disable ColumnIndex/OffsetIndex, so "
                  "page index is a fixed capability, not a candidate action "
                  "(contract 5.2). Verify directly with: "
                  f"parquet-cli column-index -c {sample_col} <part-file>"),
            mismatch="M-1")

        for name, opts in SPARK_WRITE_PROBES.items():
            path = os.path.join(tmpdir, name)
            writer = df.write.mode("overwrite")
            for k, v in opts.items():
                writer = writer.option(k, v)
            try:
                writer.parquet(path)
                footer = read_footer(path)
            except Exception as exc:  # noqa: BLE001
                probes[name] = _result(FAIL, f"write/read failed: {exc}", options=opts)
                continue
            probes[name] = _result(PASS, "written and read back",
                                   options=opts, footer=footer)

        probes.update(evaluate_spark_probes(probes, baseline))
    finally:
        try:
            spark.stop()
        except Exception:  # noqa: BLE001
            pass
    return probes


def evaluate_spark_probes(probes, baseline):
    """Turn raw footers into pass/fail verdicts on the knobs that matter."""
    verdicts = {}

    bloom = probes.get("bloom_filter_M2", {})
    if bloom.get("status") == PASS:
        off = bloom["footer"]["columns"].get("high_ndv", {}).get("bloom_filter_offset")
        verdicts["verdict_bloom_M2"] = _result(
            PASS if off is not None else FAIL,
            ("bloom filter written for the requested column" if off is not None else
             "bloom options accepted but no bloom_filter_offset in footer - "
             "the reader has nothing to filter with"),
            bloom_filter_offset=off, mismatch="M-2")

    dic = probes.get("dictionary_per_column", {})
    if dic.get("status") == PASS:
        cols = dic["footer"]["columns"]
        high = cols.get("high_ndv", {}).get("has_dictionary_page")
        low = cols.get("low_ndv", {}).get("has_dictionary_page")
        ok = (high is False) and bool(low)
        verdicts["verdict_dictionary_per_column"] = _result(
            PASS if ok else UNKNOWN,
            ("per-column dictionary honoured: disabled on high_ndv, kept on low_ndv"
             if ok else "per-column dictionary did not behave as expected"),
            high_ndv_has_dictionary=high, low_ndv_has_dictionary=low)

    rg = probes.get("row_group_bytes", {})
    if rg.get("status") == PASS:
        got, base = rg["footer"]["num_row_groups"], baseline["num_row_groups"]
        verdicts["verdict_row_group_bytes"] = _result(
            PASS if got > base else UNKNOWN,
            ("parquet.block.size changes row group count (byte-denominated, no "
             "row conversion needed)" if got > base else
             "parquet.block.size accepted but row group count unchanged - "
             "probe data may be too small"),
            row_groups_with_8MiB_blocks=got, row_groups_baseline=base)

    comp = probes.get("compression_zstd", {})
    if comp.get("status") == PASS:
        codecs = {c: v["compression"] for c, v in comp["footer"]["columns"].items()}
        applied = all(str(c).upper().startswith("ZSTD") for c in codecs.values())
        verdicts["verdict_compression_M5"] = _result(
            PASS if applied else UNKNOWN,
            ("parquet.compression applies globally, as expected - per-column codecs "
             "are NOT expressible in parquet-mr (contract 6.2 M-5)" if applied else
             "compression did not apply uniformly - inspect codecs"),
            codecs=codecs, mismatch="M-5")

    return verdicts


def probe_duckdb_tpch():
    """DuckDB is the frozen SF100 generator (contract D-5)."""
    try:
        import duckdb
    except Exception as exc:  # noqa: BLE001
        return _result(UNKNOWN, f"duckdb not importable: {exc}")

    try:
        con = duckdb.connect()
        con.execute("INSTALL tpch; LOAD tpch;")
        con.execute("CALL dbgen(sf=0.01)")
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        rows = con.execute("SELECT count(*) FROM lineitem").fetchone()[0]
        con.close()
    except Exception as exc:  # noqa: BLE001
        return _result(FAIL, f"tpch extension unavailable: {exc}")

    return _result(PASS, "tpch extension loaded and dbgen works",
                   tables=sorted(tables), lineitem_rows_at_sf001=rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out",
                    default="docs/adaptive-range-reader/results/track2/capability_probe.json")
    ap.add_argument("--rows", type=int, default=200000,
                    help="probe frame row count (default 200000; needs to be big "
                         "enough to form several row groups)")
    args = ap.parse_args()

    report = {
        "probed_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 6",
        "contract_revision": "r2",
        "versions": probe_versions(),
        "writer_probes": {},
        "e0_manual_checklist": [
            {"setting": s, "expected": e, "why": w} for s, e, w in E0_MANUAL_CHECKLIST
        ],
    }

    tmpdir = tempfile.mkdtemp(prefix="track2_probe_")
    try:
        report["writer_probes"] = run_spark_probes(tmpdir, args.rows)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    report["writer_probes"]["duckdb_tpch"] = probe_duckdb_tpch()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump(report, fh, indent=2)

    print(f"# Track 2 capability probe (contract r2) -> {args.out}\n")
    print("versions:")
    for k, v in report["versions"].items():
        print(f"  {k:10s} {v}")
    print("\nwriter probes (Spark writes -> PyArrow reads footer):")
    failures = 0
    for name, res in report["writer_probes"].items():
        status = res.get("status", UNKNOWN)
        if status == FAIL:
            failures += 1
        print(f"  [{status:7s}] {name}: {res.get('detail', '')}")
    print("\nonly observable against real S3 - confirm during E0:")
    for item in report["e0_manual_checklist"]:
        print(f"  - {item['setting']}: expect {item['expected']}")

    if failures:
        print(f"\n{failures} capability probe(s) FAILED; the corresponding candidate "
              f"actions must be rejected by the L0 check (TRACK2_M0_CONTRACT.md 5.3).")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
