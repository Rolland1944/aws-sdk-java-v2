#!/usr/bin/env python3
"""P2 matrix schedule, resume, and joint-oracle ranking."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import run_track1_matrix as m  # noqa: E402


class Schedule(unittest.TestCase):
    def test_round_robin_and_rotation(self):
        cells = m.parse_cells("000,100,010,011,110,111")
        slots = m.schedule(cells, 2)
        self.assertEqual(len(slots), 12)
        first = [s["cell"] for s in slots if s["run"] == 1]
        second = [s["cell"] for s in slots if s["run"] == 2]
        self.assertEqual(first, ["000", "100", "010", "011", "110", "111"])
        self.assertEqual(second[0], "100")
        self.assertEqual(second[-1], "000")
        # no config occupies two consecutive slots across a round boundary
        # except by rotation; the first slot of round 2 is not 000.
        self.assertNotEqual(first[0], second[0])


class Oracle(unittest.TestCase):
    def _cell(self, cid, walls, gets=100, bytes_=1000, heap=1, queries=None):
        spec = next(c for c in m.CELLS if c[0] == cid)
        reports = []
        for i, wall in enumerate(walls, 1):
            qs = queries or [{"query": 1, "wall_s": wall / 2, "error": None},
                             {"query": 2, "wall_s": wall / 2, "error": None}]
            reports.append({
                "_path": f"{cid}/{i}",
                "runs": [{
                    "query_sum_s": wall,
                    "queries": qs,
                    "io": {"gets": gets, "ranged_gets": gets, "remote_bytes": bytes_},
                    "track1": {"cache_hits": 0, "cache_useful_bytes": 0,
                               "merged_gets": 0, "wasted_bytes": 0,
                               "queue_wait_ns": 0, "peak_heap_bytes": heap,
                               "g_star_bytes": 0, "fallbacks": 0},
                }],
            })
        return m.summarize_cell(cid, spec, reports)

    def test_winner_is_lowest_median(self):
        cells = [
            self._cell("000", [100, 102, 101, 99, 103]),
            self._cell("100", [80, 81, 79, 82, 80]),
            self._cell("010", [110, 111, 109, 108, 112]),
        ]
        oracle = m.build_oracle(cells, 5)
        self.assertEqual(oracle["winner"], "100")
        self.assertTrue(oracle["winner_stable"])
        self.assertEqual(oracle["rank_metric"], "wall_clock_median_s")

    def test_query_regression_is_flagged_not_ranked(self):
        # 000 Q2 median 2s; 100 Q2 median 10s is a severe per-query regression
        base_reports = []
        for wall in (10, 10, 10, 10, 10):
            base_reports.append({
                "runs": [{"query_sum_s": wall, "queries": [
                    {"query": 1, "wall_s": 8, "error": None},
                    {"query": 2, "wall_s": 2, "error": None},
                ], "io": {}, "track1": {}}]
            })
        cand_reports = []
        for wall in (48, 48, 48, 48, 48):
            cand_reports.append({
                "runs": [{"query_sum_s": wall, "queries": [
                    {"query": 1, "wall_s": 38, "error": None},
                    {"query": 2, "wall_s": 10, "error": None},
                ], "io": {}, "track1": {}}]
            })
        cells = [
            m.summarize_cell("000", m.CELLS[0], base_reports),
            m.summarize_cell("100", m.CELLS[1], cand_reports),
        ]
        flags = m.query_regressions(cells)
        self.assertTrue(any(f["query"] == 2 and f["cell"] == "100" for f in flags))

    def test_resume_detects_complete_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "report.json")
            with open(path, "w") as fh:
                json.dump({"runs": [{"queries": [{"error": None}]}]}, fh)
            self.assertTrue(m.run_ok(path))
            with open(path, "w") as fh:
                json.dump({"runs": [{"queries": [{"error": "boom"}]}]}, fh)
            self.assertFalse(m.run_ok(path))
            self.assertFalse(m.run_ok(os.path.join(tmp, "missing.json")))


class Argv(unittest.TestCase):
    def test_bench_flags_follow_cell(self):
        class A:
            data = "s3a://b/x"
            queries_file = "q.json"
            tables = ["hits"]
            master = "local[16]"
            driver_memory = "32g"
            queries = None
        argv = m.bench_argv(
            A, m.Cell("111", d1=True, d2=True, d4=True, wait_us=50), "/tmp/out")
        self.assertIn("--track1-s3a", argv)
        self.assertIn("--track1-d1", argv)
        self.assertIn("--track1-d2", argv)
        self.assertIn("--track1-d4", argv)
        self.assertIn("50", argv)
        argv0 = m.bench_argv(A, m.Cell("000"), "/tmp/out")
        self.assertIn("--track1-s3a", argv0)
        self.assertNotIn("--track1-d1", argv0)

    def test_admission_sweep_cell_carries_cap_budget_and_block(self):
        class A:
            data = "s3a://b/x"
            queries_file = "q.json"
            tables = ["hits"]
            master = "local[16]"
            driver_memory = "32g"
            queries = None
        argv = m.bench_argv(A, m.parse_cells("100-8m2g")[0], "/tmp/out")
        self.assertEqual(argv[argv.index("--track1-d1-admit-bytes") + 1],
                         str(8 * m.MIB))
        self.assertEqual(argv[argv.index("--track1-d1-cache-mib") + 1], "2048")
        # an 8 MiB admission needs an 8 MiB block, or every repeat read stitches
        self.assertEqual(argv[argv.index("--track1-d1-block-bytes") + 1],
                         str(8 * m.MIB))
        # the default cell keeps the reader's 1 MiB block
        argv100 = m.bench_argv(A, m.parse_cells("100")[0], "/tmp/out")
        self.assertEqual(argv100[argv100.index("--track1-d1-block-bytes") + 1],
                         str(m.MIB))


if __name__ == "__main__":
    unittest.main()
