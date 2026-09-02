#!/usr/bin/env python3
"""What the advisor knows, assembled from measurements instead of literals.

The L1 model needs four things, and they used to arrive as one hand-written
Python module per dataset:

    layout geometry + column facts  <- dataset_snapshot   (footers, listing, stats)
    what the workload reads         <- access_profile     (SDK bytes + footers)
    physical action space           <- adaptive_physical_options (from geometry)
    thresholds and assumptions      <- advisor_policy

The second line is the r5 change. It used to read `workload_snapshot` (Spark
event logs -> `QUERIES`, one entry per query with its predicates and
projection). Two problems retired it. The advisor could only advise where an
event log existed, which excluded every non-SQL reader; and the predicates it
supplied fed only the sort and partition actions, both of which left the action
space. What survives is the projection -- which columns are read together --
and that is recoverable from bytes alone.

So the unit changed from a query to an **access pattern**: a table plus the set
of columns one episode read, with a count of how often that happened. The
attribute is `PATTERNS`, not `QUERIES`, deliberately: code still asking for
`QUERIES` is asking for predicates that no longer exist anywhere.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import access_profile  # noqa: E402
import adaptive_physical_options as physical  # noqa: E402
import advisor_policy as policy  # noqa: E402
import dataset_snapshot  # noqa: E402


class AdvisorCatalog:
    def __init__(self, dataset, profile, dataset_name="clickbench",
                 parallelism=policy.PARALLELISM,
                 large_table_bytes=policy.LARGE_TABLE_BYTES):
        self.dataset = dataset
        self.profile = profile
        self.dataset_name = dataset_name
        self.parallelism = parallelism

        self.BASELINE_GEOMETRY = dataset.BASELINE_GEOMETRY
        self.COLUMN_ORDER = dataset.COLUMN_ORDER
        self.COLUMN_SHARE = dataset.COLUMN_SHARE
        self.ALL_COLUMNS = dataset.ALL_COLUMNS
        self.COLUMN_STATS = dataset.COLUMN_STATS
        self.BASELINE_RG_BYTES = dataset.BASELINE_RG_BYTES
        self.LARGE_TABLE_BYTES = large_table_bytes

        self.PATTERNS = profile.PATTERNS
        self.COLUMN_WEIGHT = profile.COLUMN_WEIGHT
        self.REQUEST_SHAPE = profile.REQUEST_SHAPE

    # -- what the workload reads -------------------------------------------

    def patterns_for(self, table):
        return self.profile.patterns_for(table)

    def coaccess_matrix(self, table):
        return self.profile.coaccess_matrix(table)

    def column_weight(self, table, column):
        return ((self.COLUMN_WEIGHT.get(table) or {}).get(column) or {})

    def tables_observed(self):
        """Tables with observed traffic, intersected with tables that exist."""
        return [t for t in self.profile.tables() if t in self.BASELINE_GEOMETRY]

    # -- action space ------------------------------------------------------

    def file_options(self, table):
        return physical.file_options(table, self.dataset, self.parallelism,
                                     self.LARGE_TABLE_BYTES)

    def rg_options(self, table):
        return physical.rg_options(table, self.dataset,
                                   large_table_bytes=self.LARGE_TABLE_BYTES)

    def large_tables(self):
        """Tables the search may touch, largest first."""
        return self.dataset.large_tables(self.LARGE_TABLE_BYTES)

    def largest_table(self):
        return self.dataset.largest_table()

    # -- provenance --------------------------------------------------------

    def provenance(self):
        return {
            "dataset": self.dataset_name,
            "layout": self.dataset.layout,
            "contract": "TRACK2_M0_CONTRACT.md r5 (SDK + footer only)",
            "geometry_source": (self.dataset.doc.get("source") or {}).get("geometry"),
            "column_stats_source": (self.dataset.doc.get("source") or {}).get("column_stats"),
            "geometry_collected_at": self.dataset.doc.get("collected_at"),
            "access_profile": self.profile.provenance(),
            "n_patterns": len(self.PATTERNS),
            "n_tables": len(self.BASELINE_GEOMETRY),
            "parallelism": self.parallelism,
            "large_table_bytes": self.LARGE_TABLE_BYTES,
        }


def load(dataset_snapshot_path, access_profile_path, dataset_name="clickbench",
         **kwargs):
    return AdvisorCatalog(
        dataset_snapshot.load(dataset_snapshot_path),
        access_profile.load(access_profile_path),
        dataset_name=dataset_name, **kwargs)


def add_arguments(ap):
    """The three flags every advisor entry point needs."""
    ap.add_argument("--dataset-snapshot", required=True,
                    help="dataset_snapshot.py output: measured layout geometry")
    ap.add_argument("--access-profile", required=True,
                    help="access_profile.py output: column weight, co-access, patterns")
    ap.add_argument("--dataset", choices=("tpch", "clickbench"), default="clickbench",
                    help="selects dataset-specific policy in advisor_policy")


def from_args(args, **kwargs):
    return load(args.dataset_snapshot, args.access_profile, args.dataset,
                **kwargs)
