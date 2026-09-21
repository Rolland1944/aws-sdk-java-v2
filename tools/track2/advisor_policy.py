#!/usr/bin/env python3
"""Advisor policy: thresholds, model assumptions, and workload-agnostic helpers.

Nothing here describes a dataset or a query set; everything here is a *decision*
about what the advisor is willing to recommend, or an admission about what the
cost model cannot see.

Three kinds live here, and the distinction matters when reading a result:

  * Reader/engine facts measured once (MAX_READABLE_RG_BYTES, PARALLELISM).
    Changing these means the environment changed.
  * Policy thresholds (LARGE_TABLE_BYTES, PAGE_*, COLD_COLUMN_*,
    GUARDRAIL_REGRESSION). These are starting cuts, not fitted values, and the
    ones the planner reads are exposed as CLI knobs so an ablation can move them.
  * Model assumptions (CODEC_*, DECODE_*). These are the parts of L1 that are
    guesses. They are falsifiable and should be measured when the data allows.

r5 deleted the gate thresholds that made v1's sort and partition actions safe:
CLUSTER_HEADROOM_MIN (Gate A), PRUNE_PARALLELISM_FLOOR (Gate C), MAX_PARTITIONS
and MIN_PARTITION_BYTES (Gate D), and CORRELATED_WITH. All five were about row
ordering or directory layout, and all five became unreachable when those
actions left the action space. They are not commented out here: an unreachable
threshold that still reads like policy is worse than no threshold, because the
next person tunes it and nothing happens.
"""

from __future__ import annotations

import csv

# ---------------------------------------------------------------- engine facts

# Frozen E2 client is local[16].
PARALLELISM = 16

# Spark's default `spark.sql.files.maxPartitionBytes`. Large Parquet files
# are split into this many scan units; L1 must scale opens by scan-unit
# count, not raw file count. Frozen in s3a_session.apply_frozen_reader.
SPLIT_SIZE_BYTES = 128 * 1024 * 1024

# Page-range intersection is not implemented in correlate.py, and it is not
# on the way: knowing which pages a scan can skip means knowing which pages a
# predicate eliminates, and predicates are the input this advisor refuses to
# read. So no page-skipping benefit may be priced.
#
# That does not leave the page axis unpriced. What a finer page reliably does
# is enlarge the OffsetIndex/ColumnIndex, the layout probe measures the bytes
# per page, and L1 charges them to the scan-metadata traffic. The axis is
# therefore one-sided by construction: it can lose, and it can only win if a
# measurement someday shows intra-chunk skipping.
PAGE_RANGE_INTERSECTION = False

# Seriation is only offered when this share of data spans read 2+ columns.
# Below that, adjacency cannot reduce GETs and the option is noise.
SERIATION_MULTICOL_MIN = 0.25

# parquet-hadoop 1.16 hardcodes HADOOP_VECTORED_READ_TIMEOUT_SECONDS = 300 in
# ParquetFileReader$ConsecutivePartList. There is no config key, so this is not
# a Reader freeze knob but a hard property of the read path. The M2 canary of
# requested RG = 256 MiB wrote ~473 MiB uncompressed row groups, issued
# 35-182 MiB vectored ranges, and timed out. E2's spark default (requested
# ~128 MiB) did not. Reject requested row groups above the size E2 proved
# readable.
MAX_READABLE_RG_BYTES = 128 * 1024 * 1024

# Per-open request shape of the frozen Reader, used by L1 to price a file open.
META_GETS_PER_OPEN = 4  # footer-length + footer + page-index + slop
HEAD_PER_OPEN = 1
FOOTER_BYTES = 8192

# Only tables this large get a physical-layout search. Smaller ones stay at the
# engine default: rewriting them cannot pay for the rewrite.
LARGE_TABLE_BYTES = 2 * 1024 ** 3

# ----------------------------------------------------------- page geometry

# parquet-mr and PyArrow both default to 1 MiB pages / 20k rows.
DEFAULT_PAGE_BYTES = 1024 * 1024
DEFAULT_PAGE_ROW_LIMIT = 20000
# Candidate page sizes. Which of them the writer honours is measured per run
# rather than assumed: a request only binds if some column holds more than
# that many bytes in one row group, so 4 MiB is a no-op on a narrow sample and
# a real change on ClickBench's 105-column, 585k-row groups. The layout probe
# writes each point and drops the ones that came back byte-identical.
PAGE_BYTES_LADDER = (256 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024)

# What L1 can price on the page axis is the OffsetIndex, which is metadata and
# therefore small: on ClickBench the whole ladder spans about 1.5% of observed
# scan-metadata bytes. What it cannot price is the other side of a coarser
# page -- less precise predicate-driven page skipping, and a coarser decode
# unit -- because both need a query plan. A page action that wins by less than
# this share of t_io is inside the part of the trade the model does not see,
# so the axis stays at baseline rather than moving on a tie.
PAGE_SWITCH_MARGIN = 0.02

# ------------------------------------------------------- compression policy

# Codecs the planner will propose. zstd trades CPU for bytes and wins whenever
# the regime is bandwidth-bound; snappy is the parquet-mr/Spark default and the
# baseline; uncompressed is only proposed for columns that already fail to
# compress, where the codec is paying CPU for nothing.
CODEC_LADDER = ("snappy", "zstd")
BASELINE_CODEC = "snappy"

# A column whose measured compression ratio is above this is not compressing:
# the codec is spending CPU to save nothing. Measured by compression_probe.
INCOMPRESSIBLE_RATIO = 0.92

# Below this NDV/rows ratio a dictionary encoding is expected to pay off.
DICTIONARY_NDV_FRACTION = 0.1

# Whether the ranking is allowed to spend the decode term. L1 can *price*
# decode as soon as a layout probe (encoded bytes) and a decode probe (rates)
# are both bound -- `t_decode_s` appears in every evaluation either way. This
# flag only says whether that number has been checked against an independent
# measurement yet, and therefore whether a candidate may be chosen on it.
# False means: report decode, rank on IO alone.
#
# It has now been checked, and it failed. A paired A/B that changed only the
# encoding of three columns -- same queries, same files, same row-group
# geometry, same task count -- removed 36.7% of the decode-cost weight over
# actually-read bytes and moved measured task CPU by 0.88%. That puts the
# whole decode budget at ~70 core-s of ~2940, i.e. 2.4% of task CPU and ~1.5%
# of wall, against the 1595 core-s this model prices for the same layout.
# So the flag stays False for a stronger reason than "uncalibrated": the term
# is real but two orders of magnitude too small to rank on, and the residual
# CPU is filters, aggregation and shuffle rather than decode. See
# docs/adaptive-range-reader/DECODE_AXIS.md.
DECODE_MODELLED = False

# Relates the decode probe's single-threaded read of a tmpfs file to the
# reader the benchmark actually runs: 16 concurrent tasks over S3, competing
# for memory bandwidth, with colder caches than a file read five times in a
# row. Calibrated against per-task `Executor CPU Time` in the benchmark event
# logs, which is the same quantity the model predicts (core-seconds), so the
# fit does not depend on any parallelism assumption.
#
# Deliberately one scalar for every column, codec and encoding. A per-tuple
# correction would fit away exactly the error this term exists to expose, and
# the probe's relative rates are the part worth trusting -- they come from the
# same column measured under different tuples, so anything column-specific
# cancels.
#
# This multiplies the *rate*, so it is below 1.0 when the real reader is
# slower than the probe -- which it is, by 6.03x. Fitted on the ClickBench
# 5-pair run (decode_calibrate.py): measured task CPU 2880.9 -> 2676.3 core-s
# against predicted decode 226.4 -> 264.6. Two things about that number have
# to travel with it.
#
# It is not identified by the measurement alone. The event log reports total
# task CPU, so `CPU = cost x decode + other` has three unknowns and two
# equations. The 6.03 assumes non-decode CPU falls with task count
# (5847 -> 4170); a 10% error in that ratio moves the fit to 4.8-6.9, i.e.
# this constant to 0.14-0.21. The alternative assumption -- non-decode CPU
# unchanged -- has no positive solution at all, so the direction is safe even
# though the magnitude is not.
#
# About a factor of two of it is not concurrency but implementation: the
# probe's PyArrow read decodes BYTE_ARRAY 1.99x faster than parquet-mr does
# (same file, same tuple), and PyArrow's table is used because parquet-mr's
# absolute rates are contaminated by its sink's per-row cost. The remaining
# ~3x is 16-way contention, colder caches and the rest of the scan path.
#
# The identification gap above has since been closed from the other side. The
# decode_veto A/B holds task count fixed at 4170 on both arms, so `N` is
# unchanged by construction and `dCPU = s x d_decode` has one unknown: -25.7
# core-s against a 36.7% cut in decode weight implies ~70 core-s of decode in
# total. This constant would have to be ~20x smaller again to reproduce that.
# The residual is most likely that the probe's *relative* rates do not survive
# parquet-mr's vectorised path either, which is the one part of the probe this
# comment claimed was worth trusting.
#
# Either way the scale is left as fitted rather than re-fitted to one A/B: at
# this magnitude the term cannot change a ranking, and a second fit would only
# lend it false precision. Pinning it for real still needs the scan-only Spark
# job with spark.sql.parquet.filterPushdown=false, one column at a time.
DECODE_RATE_SCALE = 1.0 / 6.03

# ------------------------------------------------------------ column order

# Co-access weights below this fraction of the table's maximum are treated as
# noise by the clustering pass: with hundreds of columns, a single stray
# episode otherwise links two unrelated clusters.
COACCESS_NOISE_FLOOR = 0.02

# Columns never read in the observed window. They are placed at the tail of the
# order (so the hot prefix is contiguous) and reported, never dropped.
COLD_COLUMN_PLACEMENT = "tail"

# ------------------------------------------------------------- guardrails

# Contract 4.1: a candidate may not make any single access pattern more than
# this much slower than the baseline *under the same L1*. A feasibility cut,
# not a speed-up claim.
GUARDRAIL_REGRESSION = 0.10

# L1 self-consistency tolerance for the baseline replay (predicted vs measured
# GETs and bytes).
VALIDATE_TOL = 0.10

# Written-layout geometry vs the virtual candidate. `n_rg` is exact (off by
# at most one group). File count and compressed bytes are allowed a wider
# band because a bound compression probe is a sample, not a full rewrite:
# ClickBench's 200k-row prefix understated the table by ~16%. Checking
# unpriced baseline bytes against a codec rewrite is a different failure
# (that one is "did not bind the probe") and is not relaxed here.
GEOMETRY_TOL = 0.20


# ------------------------------------------------------- workload-agnostic ops

def load_per_query_medians(path):
    """query number -> median seconds, from a run_benchmark per_query.csv.

    v2 does not use per-query medians for planning -- there are no queries in
    the advisor's input. This survives because the E-0 smoke test and the
    benchmark reports still speak in queries, and comparing two runs needs it.
    """
    out = {}
    with open(path) as fh:
        for row in csv.DictReader(fh):
            out[int(row["query"])] = float(row["median_s"])
    return out
