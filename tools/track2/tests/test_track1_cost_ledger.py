#!/usr/bin/env python3
"""Cost ledger: GET/bytes down vs heap/GC/tee tax."""

from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import track1_cost_ledger as cost  # noqa: E402


class Ledger(unittest.TestCase):
    def test_same_region_transfer_is_zero_and_heap_step_is_flagged(self):
        cells = [
            {"id": "000", "median_s": 100, "gets": 10000,
             "remote_bytes": 10 * cost.GIB, "peak_heap_bytes": 8 * cost.GIB,
             "cache_useful_bytes": 0, "teed_bytes": 0, "gc_ms": 1000},
            {"id": "100", "median_s": 85, "gets": 3000,
             "remote_bytes": 9 * cost.GIB, "peak_heap_bytes": 18 * cost.GIB,
             "cache_useful_bytes": 2 * cost.GIB, "teed_bytes": 4 * cost.GIB,
             "gc_ms": 2500},
            {"id": "big", "median_s": 80, "gets": 2000,
             "remote_bytes": 8 * cost.GIB, "peak_heap_bytes": 30 * cost.GIB,
             "cache_useful_bytes": 3 * cost.GIB, "teed_bytes": 6 * cost.GIB,
             "gc_ms": 4000},
        ]
        ledger = cost.from_cell_summaries(cells, driver_memory="32g")
        self.assertFalse(ledger["used_for_ranking"])
        self.assertEqual(ledger["rank_metric"], "wall_clock_median_s")
        by_id = {r["id"]: r for r in ledger["cells"]}
        self.assertAlmostEqual(by_id["100"]["get_usd_vs_000"],
                               (3000 - 10000) / 1000.0 * cost.GET_USD_PER_1K)
        self.assertEqual(by_id["100"]["xfer_usd_vs_000"], 0.0)
        self.assertAlmostEqual(by_id["100"]["tee_efficiency"], 0.5)
        self.assertFalse(by_id["100"]["heap_step_risk"])
        self.assertTrue(by_id["big"]["heap_step_risk"])
        self.assertIn("100", ledger["other_costs_up"]["peak_heap"])
        self.assertIn("big", ledger["other_costs_up"]["heap_step_risk"])
        self.assertAlmostEqual(by_id["100"]["instance_hours_vs_000"], -0.15)

    def test_cross_region_transfer_is_priced(self):
        cells = [
            {"id": "000", "median_s": 100, "gets": 1000,
             "remote_bytes": 10 * cost.GIB, "peak_heap_bytes": 4 * cost.GIB},
            {"id": "100", "median_s": 90, "gets": 400,
             "remote_bytes": 8 * cost.GIB, "peak_heap_bytes": 5 * cost.GIB},
        ]
        ledger = cost.from_cell_summaries(cells, xfer_usd_per_gib=0.02)
        by_id = {r["id"]: r for r in ledger["cells"]}
        self.assertAlmostEqual(by_id["100"]["xfer_usd_vs_000"], -0.04)


if __name__ == "__main__":
    unittest.main()
