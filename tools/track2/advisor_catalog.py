#!/usr/bin/env python3
"""What the advisor knows, assembled from measurements instead of literals.

The L1 model needs four things, and they used to arrive as one hand-written
Python module per dataset:

    layout geometry + column facts  <- dataset_snapshot   (footers, listing, stats)
    scans per query                 <- workload_snapshot  (event logs)
    physical action space           <- adaptive_physical_options (from geometry)
    thresholds and assumptions      <- advisor_policy

This class bundles them behind the attribute names `virtual_footer` and
`whatif` already read, so those modules did not need rewriting around a new
interface -- they just get handed a catalog instead of importing a module.
The point of the shape being unchanged is that the *values* changed source:
every number below is now traceable to a file that was measured, and
`provenance()` says which one.

The split matters for a reason beyond tidiness. A dataset snapshot describes
one directory at one moment. When the writer materialises a candidate layout,
the right thing to do is snapshot *that* and re-price against it, which is
impossible while the geometry is a literal in a module shared by every run.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import adaptive_physical_options as physical  # noqa: E402
import advisor_policy as policy  # noqa: E402
import dataset_snapshot  # noqa: E402
import workload_snapshot  # noqa: E402


class AdvisorCatalog:
    def __init__(self, dataset, workload, dataset_name="tpch",
                 parallelism=policy.PARALLELISM,
                 large_table_bytes=policy.LARGE_TABLE_BYTES):
        self.dataset = dataset
        self.workload = workload
        self.dataset_name = dataset_name
        self.parallelism = parallelism

        self.BASELINE_GEOMETRY = dataset.BASELINE_GEOMETRY
        self.COLUMN_ORDER = dataset.COLUMN_ORDER
        self.COLUMN_SHARE = dataset.COLUMN_SHARE
        self.ALL_COLUMNS = dataset.ALL_COLUMNS
        self.COLUMN_STATS = dataset.COLUMN_STATS
        self.BASELINE_RG_BYTES = dataset.BASELINE_RG_BYTES
        self.LARGE_TABLE_BYTES = large_table_bytes
        self.CORRELATED_WITH = policy.correlations(dataset_name)
        self.QUERIES = workload.QUERIES
        self.measured_median_s = workload.measured_median_s

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
            "geometry_source": (self.dataset.doc.get("source") or {}).get("geometry"),
            "column_stats_source": (self.dataset.doc.get("source") or {}).get("column_stats"),
            "geometry_collected_at": self.dataset.doc.get("collected_at"),
            "eventlog": self.workload.doc.get("eventlog"),
            "query_id_source": self.workload.query_id_source,
            "workload_collected_at": self.workload.doc.get("collected_at"),
            "n_queries": len(self.QUERIES),
            "n_tables": len(self.BASELINE_GEOMETRY),
            "parallelism": self.parallelism,
            "large_table_bytes": self.LARGE_TABLE_BYTES,
        }


def load(dataset_snapshot_path, workload_snapshot_path, dataset_name="tpch",
         **kwargs):
    return AdvisorCatalog(
        dataset_snapshot.load(dataset_snapshot_path),
        workload_snapshot.load(workload_snapshot_path),
        dataset_name=dataset_name, **kwargs)


def add_arguments(ap):
    """The three flags every advisor entry point needs."""
    ap.add_argument("--dataset-snapshot", required=True,
                    help="dataset_snapshot.py output: measured layout geometry")
    ap.add_argument("--workload-snapshot", required=True,
                    help="workload_snapshot.py output: scans per query")
    ap.add_argument("--dataset", choices=("tpch", "clickbench"), default="tpch",
                    help="selects the correlation assumptions in advisor_policy")


def from_args(args, **kwargs):
    return load(args.dataset_snapshot, args.workload_snapshot, args.dataset,
                **kwargs)
