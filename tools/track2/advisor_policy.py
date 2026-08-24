#!/usr/bin/env python3
"""Advisor policy: thresholds, model assumptions, and workload-agnostic helpers.

These used to be scattered between `whatif.py` module constants and
`workload.py`, where they sat next to TPC-H table names and looked like part of
the dataset definition. They are not. Nothing here describes a dataset or a
query set; everything here is a *decision* about what the advisor is willing to
recommend, or an admission about what the cost model cannot see.

Three kinds live here, and the distinction matters when reading a result:

  * Reader/engine facts measured once (MAX_READABLE_RG_BYTES, PARALLELISM).
    Changing these means the environment changed.
  * Gate thresholds (CLUSTER_SPAN_MIN, PRUNE_PARALLELISM_FLOOR,
    MAX_PARTITIONS, MIN_PARTITION_BYTES, GUARDRAIL_REGRESSION). These are
    starting cuts, not fitted values, and every one is exposed as a CLI knob
    so an ablation can move it.
  * Model assumptions (CORRELATED_WITH, JOIN_AGG_*). These are the parts of
    L1 that are guesses. They are falsifiable and should be measured when
    the data allows it.
"""

from __future__ import annotations

import csv

# ---------------------------------------------------------------- engine facts

# Frozen E2 client is local[16]. A file size that suits a 22 GB fact table can
# starve a 4 GB one, so L0 checks each large table separately.
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

# ------------------------------------------------------------ gate thresholds

# Gate A: refuse to re-sort a column that is already as clustered as sorting
# could make it. rg_span = avg(rg.max - rg.min) / (global.max - global.min).
#
# This used to be an absolute cut, `rg_span < 0.5 -> reject`, and that was
# wrong in form and falsified in fact.
#
# Wrong in form because rg_span has no absolute meaning. A perfect sort leaves
# each row group covering roughly 1/n_rg of the domain, or 1/ndv when ties
# force wider groups, so 0.358 is "barely clustered" on a table with 165 row
# groups and "perfectly clustered" on one with 3. An absolute threshold reads
# those as the same table.
#
# Falsified in fact because the 0.5 cut was calibrated against a *transcribed*
# rg_span for ClickBench EventDate of 0.5232, which sat just above it. The
# measured value is 0.358, so the gate in its old form vetoes the EventDate
# layout -- the one E8 measured at -51.3% end to end. Measuring the statistic
# instead of typing it is what exposed that.
#
# So the test is headroom against what a sort can actually achieve:
#     achievable = max(1/n_rg, 1/ndv)
#     reject if  rg_span < achievable * CLUSTER_HEADROOM_MIN
# TPC-H l_shipdate scores 300x (unsorted), l_orderkey 1.3x (dbgen already
# emits in that order), ClickBench EventDate 6.1x. 2.0 is a starting cut, not
# a fitted one, and it is an ablation knob like the rest.
#
# Caveat kept in view: rg_span is measured on the value domain but sorting
# equalises row *counts*, so `achievable` understates the floor for a skewed
# column and the gate is correspondingly lenient there.
CLUSTER_HEADROOM_MIN = 2.0

# Gate C. Deliberately not PARALLELISM: a three-month TPC-H window on orders
# opens about 5 files and is a measured win. 4 catches the 1-2 task collapse
# (ClickBench CounterID=62 pruned to 1 file / 2 RGs) without killing date-range
# sorts.
PRUNE_PARALLELISM_FLOOR = 4

# Gate D. Identity partitioning writes one directory per distinct value and at
# least one file per directory, so NDV multiplies the file count. 64 keeps
# o_orderstatus (2) and ClickBench EventDate (17); the byte floor is what
# actually stops l_shipdate (2505 directories, 9 MiB each) turning a 21 GiB
# table into RTT-bound rubble.
MAX_PARTITIONS = 64
MIN_PARTITION_BYTES = 128 * 1024 * 1024

# Contract 4.1: a candidate may not make any single query more than this much
# slower than the baseline *under the same L1*. A feasibility cut, not a
# speed-up claim.
GUARDRAIL_REGRESSION = 0.10

# L1 self-consistency tolerance for the baseline replay (predicted vs measured
# GETs and bytes).
VALIDATE_TOL = 0.10

# ------------------------------------------------------------- model guesses

# Penalty for an unpruned multi-scan query when the fact table coalesces into
# fewer files. Not a join cardinality model: (n_files_base / n_files) ** alpha,
# which is why TPC-H Q18's two full lineitem scans plus GROUP BY get more
# expensive as files drop. A scan that prunes keeps scale 1.
JOIN_AGG_NO_PRUNE = 0.95
JOIN_AGG_FILE_ALPHA = 0.5

# Columns assumed to cluster with another column, so a sort on the second also
# prunes predicates on the first. TPC-H commit/receipt dates are ship date plus
# a few days. This is an assumption about the data generator, switchable with
# --no-empirical-corr, and it is the single least defensible branch in L1.
CORRELATED_WITH = {
    "tpch": {
        "l_commitdate": "l_shipdate",
        "l_receiptdate": "l_shipdate",
    },
    # EventTime clusters with EventDate by construction, but ClickBench never
    # pushes an EventTime range down, so the branch is never taken.
    "clickbench": {"EventTime": "EventDate"},
}


def correlations(dataset):
    return dict(CORRELATED_WITH.get(dataset) or {})


# ------------------------------------------------------- workload-agnostic ops

def load_per_query_medians(path):
    """query number -> median seconds, from a run_benchmark per_query.csv."""
    out = {}
    with open(path) as fh:
        for row in csv.DictReader(fh):
            out[int(row["query"])] = float(row["median_s"])
    return out


def compress_workload(medians, x_frac):
    """DB2 6.2 workload compression: keep the costliest queries covering x_frac.

    Search runs on the compressed set; the acceptance gate still runs on all
    queries, so compression can only cost search quality, never hide a
    regression.
    """
    total = sum(medians.values())
    if not total:
        return sorted(medians), 0.0
    kept, acc = [], 0.0
    for q in sorted(medians, key=lambda q: medians[q], reverse=True):
        kept.append(q)
        acc += medians[q]
        if acc / total >= x_frac:
            break
    return kept, acc / total
