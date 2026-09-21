#!/usr/bin/env python3
"""Companion cost table for the paper, not a ranking input.

Wall-clock median is the only score. This ledger sits next to that number
so a write-up can say what a speedup costs: S3 GET $, transfer $, peak
heap, cache occupancy, tee-copy, eviction, and GC. Same-region S3→EC2
transfer is $0; GET uses the published S3 standard request price.
Instance memory is a step on the already-provisioned driver heap.
"""

from __future__ import annotations

import argparse
import json
import sys

GIB = 1024 ** 3
# S3 standard GET, us-east-2 list price. Request $ only; not a rank metric.
GET_USD_PER_1K = 0.0004
# Same AZ / same region to EC2. Set --xfer-usd-per-gib for cross-region.
DEFAULT_XFER_USD_PER_GIB = 0.0
# Spark Xmx headroom before we would have to buy a bigger box.
HEAP_STEP_FRAC = 0.85


def parse_driver_bytes(text, default=32 * GIB):
    if not text:
        return default
    raw = str(text).strip().lower()
    try:
        if raw.endswith("g"):
            return int(float(raw[:-1]) * GIB)
        if raw.endswith("m"):
            return int(float(raw[:-1]) * 1024 * 1024)
        return int(raw)
    except ValueError:
        return default


def _num(cell, key):
    value = cell.get(key)
    return value if isinstance(value, (int, float)) else None


def line(cell, baseline, driver_bytes, xfer_usd_per_gib):
    wall = _num(cell, "median_s")
    base_wall = _num(baseline, "median_s")
    gets = _num(cell, "gets")
    base_gets = _num(baseline, "gets")
    remote = _num(cell, "remote_bytes")
    base_remote = _num(baseline, "remote_bytes")
    heap = _num(cell, "peak_heap_bytes")
    base_heap = _num(baseline, "peak_heap_bytes")
    useful = _num(cell, "cache_useful_bytes") or 0
    teed = _num(cell, "teed_bytes")
    evicted = _num(cell, "evicted_bytes")
    cached = _num(cell, "cached_bytes")
    peak_cached = _num(cell, "peak_cached_bytes")
    gc_ms = _num(cell, "gc_ms")
    base_gc = _num(baseline, "gc_ms")

    get_usd = None if gets is None else gets / 1000.0 * GET_USD_PER_1K
    base_get_usd = None if base_gets is None else base_gets / 1000.0 * GET_USD_PER_1K
    xfer_usd = None if remote is None else remote / GIB * xfer_usd_per_gib
    base_xfer_usd = None if base_remote is None else base_remote / GIB * xfer_usd_per_gib

    heap_step = None
    if heap is not None and driver_bytes:
        heap_step = heap > HEAP_STEP_FRAC * driver_bytes

    tee_efficiency = None
    if teed and teed > 0:
        tee_efficiency = useful / teed

    return {
        "id": cell.get("id"),
        "wall_s": wall,
        "wall_vs_000": None if not (wall and base_wall) else wall / base_wall - 1.0,
        "gets": gets,
        "gets_vs_000": None if not (gets is not None and base_gets) else gets - base_gets,
        "get_usd": get_usd,
        "get_usd_vs_000": None if not (get_usd is not None and base_get_usd is not None)
        else get_usd - base_get_usd,
        "remote_bytes": remote,
        "remote_gib_vs_000": None if not (remote is not None and base_remote is not None)
        else (remote - base_remote) / GIB,
        "xfer_usd": xfer_usd,
        "xfer_usd_vs_000": None if not (xfer_usd is not None and base_xfer_usd is not None)
        else xfer_usd - base_xfer_usd,
        "peak_heap_bytes": heap,
        "peak_heap_gib_vs_000": None if not (heap is not None and base_heap is not None)
        else (heap - base_heap) / GIB,
        "heap_step_risk": heap_step,
        "cached_bytes": cached,
        "peak_cached_bytes": peak_cached,
        "cache_useful_bytes": useful,
        "teed_bytes": teed,
        "tee_efficiency": tee_efficiency,
        "evicted_bytes": evicted,
        "gc_ms": gc_ms,
        "gc_ms_vs_000": None if not (gc_ms is not None and base_gc is not None)
        else gc_ms - base_gc,
        "instance_hours_vs_000": None if not (wall and base_wall) else wall / base_wall - 1.0,
    }


def from_cell_summaries(cells, driver_memory="32g", xfer_usd_per_gib=DEFAULT_XFER_USD_PER_GIB,
                        baseline_id="000"):
    by_id = {c.get("id"): c for c in cells}
    baseline = by_id.get(baseline_id) or (cells[0] if cells else {})
    driver_bytes = parse_driver_bytes(driver_memory)
    lines = [line(c, baseline, driver_bytes, xfer_usd_per_gib) for c in cells]
    memory_up = [r["id"] for r in lines
                 if r.get("peak_heap_gib_vs_000") and r["peak_heap_gib_vs_000"] > 0.25]
    heap_steps = [r["id"] for r in lines if r.get("heap_step_risk")]
    gc_up = [r["id"] for r in lines
             if r.get("gc_ms_vs_000") and r["gc_ms_vs_000"] > 1000]
    return {
        "purpose": "paper companion: costs that travel with a wall-clock speedup",
        "used_for_ranking": False,
        "rank_metric": "wall_clock_median_s",
        "baseline": baseline_id,
        "driver_memory": driver_memory,
        "get_usd_per_1k": GET_USD_PER_1K,
        "xfer_usd_per_gib": xfer_usd_per_gib,
        "heap_step_frac": HEAP_STEP_FRAC,
        "cells": lines,
        "other_costs_up": {
            "peak_heap": memory_up,
            "heap_step_risk": heap_steps,
            "gc": gc_up,
        },
        "notes": [
            "Do not feed this table into the oracle. Ranking stays wall median.",
            "Same-region S3→EC2 transfer is $0; GET $ is list price.",
            "Peak heap is a reservation on the already-paid instance. It "
            "becomes incremental $ only if heap_step_risk trips.",
            "Instance-hours move with wall: a faster run on the same box "
            "lowers compute $ even when peak heap rises.",
            "tee_efficiency = useful_bytes / teed_bytes. Low values mean "
            "we copied ranges that never paid back.",
        ],
    }


def from_report(report, xfer_usd_per_gib=DEFAULT_XFER_USD_PER_GIB):
    return from_cell_summaries(
        report.get("results") or [],
        driver_memory=report.get("driver_memory", "32g"),
        xfer_usd_per_gib=xfer_usd_per_gib,
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("report", help="p3_report.json or p2_report.json")
    ap.add_argument("--xfer-usd-per-gib", type=float, default=DEFAULT_XFER_USD_PER_GIB)
    args = ap.parse_args()
    with open(args.report) as fh:
        report = json.load(fh)
    json.dump(from_report(report, args.xfer_usd_per_gib), sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
