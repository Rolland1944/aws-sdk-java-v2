#!/usr/bin/env python3
"""Capacity report: G1 rule and an end-to-end pass over synthetic cell reports."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import phase3_capacity_report as cap  # noqa: E402


def _write_run(root, cell, rnd, wall, rows=1, error=None, gets=100, peak_heap=10 * cap.GIB):
    d = os.path.join(root, "cells", cell, f"run-{rnd}")
    os.makedirs(d)
    queries = [{"run": 1, "query": 1, "wall_s": wall / 2, "rows": rows, "error": error},
               {"run": 1, "query": 2, "wall_s": wall / 2, "rows": 5, "error": None}]
    report = {"runs": [{"query_sum_s": wall, "queries": queries,
                        "io": {"gets": gets, "remote_bytes": 5 * cap.GIB},
                        "track1": {"cache_hits": 10, "peak_heap_bytes": peak_heap,
                                   "gc_ms": 3000, "peak_cached_bytes": cap.GIB,
                                   "evicted_bytes": 0}}]}
    with open(os.path.join(d, "report.json"), "w") as fh:
        json.dump(report, fh)


class G1Rule(unittest.TestCase):
    def test_pass_needs_every_round_and_five_percent(self):
        ref = {1: 100, 2: 100, 3: 100}
        v = cap.decide_g1(ref, {1: 90, 2: 92, 3: 91})
        self.assertEqual(v["verdict"], "PASS")
        self.assertEqual(v["level"], "screening")

    def test_fail_when_gap_small(self):
        v = cap.decide_g1({1: 100, 2: 100, 3: 100}, {1: 99.5, 2: 99, 3: 99.2})
        self.assertEqual(v["verdict"], "FAIL")

    def test_fail_when_sign_flips_even_with_large_median_gap(self):
        v = cap.decide_g1({1: 100, 2: 100, 3: 100, 4: 100, 5: 100}, {1: 80, 2: 82, 3: 85, 4: 101, 5: 83})
        self.assertEqual(v["verdict"], "FAIL")
        self.assertEqual(v["level"], "formal")

    def test_middle_band_is_inconclusive(self):
        v = cap.decide_g1({1: 100, 2: 100, 3: 100}, {1: 96, 2: 96.5, 3: 97})
        self.assertEqual(v["verdict"], "INCONCLUSIVE")

    def test_too_few_rounds(self):
        self.assertEqual(cap.decide_g1({1: 100, 2: 100}, {1: 90, 2: 90})["verdict"], "INSUFFICIENT")


class EndToEnd(unittest.TestCase):
    def test_report_on_synthetic_grid(self):
        with tempfile.TemporaryDirectory() as root:
            for rnd in (1, 2, 3):
                _write_run(root, "000", rnd, 2000 + rnd)
                _write_run(root, "pol-wtinylfu-av-4g", rnd, 1700 + rnd)
                _write_run(root, "pol-wtinylfu-av-8g", rnd, 1600 + rnd)
            # an arm that did not fit the heap: archived as a failure, not dropped
            _write_run(root, "pol-wtinylfu-av-16g", 1, 0, error="OutOfMemoryError: Java heap space")
            rep = cap.build(root, "pol-wtinylfu-av-4g", 112.0)
            self.assertEqual(rep["g1"]["best_larger"], "pol-wtinylfu-av-8g")
            self.assertEqual(rep["g1"]["verdict"], "PASS")
            self.assertEqual([x["cell"] for x in rep["failures"]], ["pol-wtinylfu-av-16g"])
            self.assertEqual(rep["p8_row_mismatch"], [])
            rho = {r["cell"]: r["rho"] for r in rep["table1"]}
            self.assertAlmostEqual(rho["pol-wtinylfu-av-4g"], 4 / 112.0)
            md = cap.render(rep)
            self.assertIn("OutOfMemoryError", md)
            self.assertIn("**PASS**", md)

    def test_row_count_mismatch_is_reported(self):
        with tempfile.TemporaryDirectory() as root:
            for rnd in (1, 2, 3):
                _write_run(root, "000", rnd, 100)
                _write_run(root, "pol-wtinylfu-av-4g", rnd, 90, rows=2 if rnd == 2 else 1)
            rep = cap.build(root, "pol-wtinylfu-av-4g", None)
            self.assertEqual([x["query"] for x in rep["p8_row_mismatch"]], [1])

    def test_regression_check_flags_slow_queries(self):
        with tempfile.TemporaryDirectory() as root:
            for rnd in (1, 2, 3):
                _write_run(root, "000", rnd, 100)
                _write_run(root, "pol-wtinylfu-av-4g", rnd, 140)  # +20 s per query > 15% and >= 1 s
            rep = cap.build(root, "pol-wtinylfu-av-4g", None)
            self.assertEqual({r["query"] for r in rep["table4_regressions"]}, {1, 2})


if __name__ == "__main__":
    unittest.main()
