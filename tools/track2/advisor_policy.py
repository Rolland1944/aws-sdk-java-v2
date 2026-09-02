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
PAGE_BYTES_LADDER = (256 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024)

# The page rule keys on two observations from access_profile.request_shape.
# Above this share of sub-64KiB GETs the workload is RTT-bound and larger pages
# (fewer, fatter ranges) are the right direction.
TINY_GET_FRACTION_HIGH = 0.6
# More requests than chunks touched means the reader could not merge a chunk
# into one range; finer pages let the OffsetIndex skip inside it instead.
REQUESTS_PER_CHUNK_HIGH = 1.5

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

# L1 does not model decode CPU. A codec change therefore moves predicted bytes
# but not predicted time beyond the bandwidth term, which understates zstd's
# cost and overstates its benefit on a CPU-bound reader. Recorded here so the
# gap is visible in the report rather than discovered in a regression.
DECODE_MODELLED = False

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
