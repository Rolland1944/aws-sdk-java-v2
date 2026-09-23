#!/usr/bin/env python3
"""Summarize diagnostic D1 hot-path counters without ranking benchmark cells."""

import argparse
import json
from pathlib import Path
from statistics import median

FIELDS = (
    "d1_cache_lookup_ns", "d1_cache_hit_copy_ns", "d1_cache_hit_copy_bytes",
    "d1_cache_stitch_ns", "d1_cache_put_ns", "d1_tee_copy_ns",
    "d1_tee_copy_bytes", "d1_rejected_payload_copy_bytes",
)


def probes(report):
    for run in report.get("runs", []):
        for query in run.get("queries", []):
            probe = query.get("track1_s3a") or query.get("track1_s3a_stats")
            if probe:
                yield probe


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("reports", nargs="+", type=Path)
    args = parser.parse_args()
    samples = {field: [] for field in FIELDS}
    for path in args.reports:
        with path.open() as source:
            for probe in probes(json.load(source)):
                for field in FIELDS:
                    if field in probe:
                        samples[field].append(probe[field])
    print(json.dumps({
        "used_for_ranking": False,
        "reports": [str(path) for path in args.reports],
        "median": {field: median(values) if values else None for field, values in samples.items()},
        "samples": {field: len(values) for field, values in samples.items()},
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
