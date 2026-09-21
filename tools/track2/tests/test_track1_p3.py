#!/usr/bin/env python3
"""P3 runner cells, adaptive flags, and optimistic bound."""

from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import run_track1_p3 as p3  # noqa: E402
import track1_dynamic_oracle as oracle  # noqa: E402


class _Args:
    data = "s3a://b/x"
    queries_file = "q.json"
    tables = ["hits"]
    master = "local[16]"
    driver_memory = "32g"
    queries = None


class Phase(unittest.TestCase):
    def test_characterize_excludes_online(self):
        ids = [c.id for c in p3.cells_for("characterize")]
        self.assertEqual(ids, ["000", "100", "100-4m1g", "100-8m2g"])

    def test_online_phase_includes_soft_controller(self):
        cells = p3.cells_for("online")
        self.assertEqual([c.id for c in cells], ["000", "100", "online"])
        online = [c for c in cells if c.id == "online"][0]
        self.assertTrue(online.d1)
        self.assertTrue(online.d1_adaptive)
        self.assertEqual(online.d1_hard_mib, 4096)
        self.assertEqual(online.d1_coverage, 1.0)

    def test_sf8_smoke_keeps_budget_only_oracle(self):
        self.assertEqual([c.id for c in p3.cells_for("p3-2-sf8")],
                         ["000", "100", "100-1g", "online"])


class Argv(unittest.TestCase):
    def test_online_cell_sets_adaptive_flags(self):
        argv = p3.m.bench_argv(_Args, p3.ONLINE, "/tmp/out")
        self.assertIn("--track1-d1", argv)
        self.assertIn("--track1-d1-adaptive", argv)
        self.assertEqual(argv[argv.index("--track1-d1-hard-mib") + 1], "4096")
        self.assertEqual(argv[argv.index("--track1-d1-coverage") + 1], "1.0")
        self.assertNotIn("--track1-d2", argv)

    def test_fixed_cell_does_not_set_adaptive(self):
        argv = p3.m.bench_argv(_Args, p3.FIXED[1], "/tmp/out")
        self.assertNotIn("--track1-d1-adaptive", argv)

    def test_admit_only_holds_1g_and_sweeps_cap(self):
        ids = [c.id for c in p3.cells_for("admit-only")]
        self.assertEqual(
            ids, ["000", "100", "64k-1g", "128k-1g", "256k-1g",
                  "512k-1g", "1m-1g", "2m-1g"])
        argv = p3.m.bench_argv(_Args, p3.HOLD_BUDGET[3], "/tmp/out")
        self.assertEqual(argv[argv.index("--track1-d1-admit-bytes") + 1],
                         str(512 * 1024))
        self.assertEqual(argv[argv.index("--track1-d1-cache-mib") + 1], "1024")

    def test_budget_only_keeps_256kib_admit(self):
        ids = [c.id for c in p3.cells_for("budget-only")]
        self.assertEqual(ids, ["000", "100", "100-1g", "100-2g"])
        argv = p3.m.bench_argv(_Args, p3.HOLD_ADMIT[1], "/tmp/out")
        self.assertEqual(argv[argv.index("--track1-d1-admit-bytes") + 1],
                         str(256 * 1024))
        self.assertEqual(argv[argv.index("--track1-d1-cache-mib") + 1], "2048")
        self.assertEqual(argv[argv.index("--track1-d1-block-bytes") + 1],
                         str(p3.MIB))


class Optimistic(unittest.TestCase):
    def _cell(self, cid, q1, q2, adaptive=False):
        return {
            "id": cid,
            "d1_adaptive": adaptive,
            "median_s": q1 + q2,
            "per_query": [
                {"query": 1, "median_s": q1},
                {"query": 2, "median_s": q2},
            ],
            "d1_u_h": 100,
            "d1_r_h": 50,
            "d1_target_budget": 25,
        }

    def test_optimistic_picks_per_query_min(self):
        result = oracle.from_cell_summaries([
            self._cell("000", 10, 12),
            self._cell("100", 8, 11),
            self._cell("online", 7, 7, adaptive=True),
        ])
        self.assertEqual(result["winner"], "100")
        self.assertEqual(result["optimistic_sum_s"], 19)
        self.assertEqual(result["per_query"][0]["cell"], "100")
        self.assertEqual(result["per_query"][1]["cell"], "100")
        self.assertNotIn("online", result["chosen_cells"])
        # query-aware bound still exists even when one cell wins both
        mixed = oracle.from_cell_summaries([
            self._cell("000", 10, 8),
            self._cell("100", 6, 12),
        ])
        self.assertEqual(mixed["optimistic_sum_s"], 14)
        self.assertEqual(mixed["per_query"][0]["cell"], "100")
        self.assertEqual(mixed["per_query"][1]["cell"], "000")


if __name__ == "__main__":
    unittest.main()
