#!/usr/bin/env python3
"""Capacity-curve driver: stage logic with a fake grid runner (no Spark, no S3)."""
from __future__ import annotations

import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import capacity_curve_core as core  # noqa: E402
import capacity_curve_stages as stages  # noqa: E402
import run_capacity_curve as drv  # noqa: E402
from test_phase3_capacity_report import _write_run  # noqa: E402

GIB = 1 << 30


class Args:
    tag, min_free_gib = "t1", None


def make_runner(plan, calls):
    """plan(out, argv) writes reports for a launch and returns (rc, log text)."""
    def runner(argv, out):
        calls.append(argv)
        return plan(out, argv)
    return runner


class Setup(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.result_md = os.path.join(self.tmp.name, "RESULT.md")
        with open(self.result_md, "w") as fh:
            fh.write("# r\n\n## 1. a\n\n## 3. c\n")
        self._old = core.RESULT_MD
        core.RESULT_MD = self.result_md
        self.P = {k: os.path.join(self.tmp.name, k) for k in ("root", "sf1", "smoke", "sf8")}
        os.makedirs(self.P["root"])
        self.st = core.Status(os.path.join(self.P["root"], "status.json"))

    def tearDown(self):
        core.RESULT_MD = self._old
        self.tmp.cleanup()

    def ctx(self, runner):
        return stages.Ctx(Args, self.P, self.st, runner)

    def md(self):
        with open(self.result_md) as fh:
            return fh.read()


class Smoke(Setup):
    def test_feasible_16g_is_recorded_and_flags_a_tight_heap(self):
        def plan(out, argv):
            _write_run(out, core.cid("16g"), 1, 2000, peak_heap=46 * GIB)
            return 0, ""
        ok, note = stages.stage_smoke(self.ctx(make_runner(plan, [])))
        self.assertTrue(ok)
        self.assertFalse(self.st.fact("heap_infeasible_16g"))
        self.assertTrue(self.st.fact("smoke")["heap_tight"])
        self.assertIn("HEAP TIGHT", note)
        self.assertIn("## 4. SF8", self.md())  # next free section number

    def test_oom_is_archived_not_retried(self):
        calls = []

        def plan(out, argv):
            _write_run(out, core.cid("16g"), 1, 0, error="OutOfMemoryError: Java heap space")
            return 2, "java.lang.OutOfMemoryError: Java heap space"
        ok, note = stages.stage_smoke(self.ctx(make_runner(plan, calls)))
        self.assertTrue(ok)
        self.assertEqual(len(calls), 1)  # never retried with another heap
        self.assertTrue(self.st.fact("heap_infeasible_16g"))
        self.assertTrue(os.path.isfile(os.path.join(self.P["smoke"], "HEAP_INFEASIBLE.md")))
        self.assertIn("堆不可行", self.md())

    def test_other_failures_stop_the_stage(self):
        ok, note = stages.stage_smoke(self.ctx(make_runner(lambda o, a: (4, "DISK GUARD: only 10 GiB"), [])))
        self.assertFalse(ok)
        self.assertIn("disk_guard", note)


class Screen(Setup):
    def full(self, out, cells, base=2000):
        for rnd in (1, 2, 3):
            for c in cells:
                if not os.path.isdir(os.path.join(out, "cells", c, f"run-{rnd}")):
                    _write_run(out, c, rnd, base - (200 if c.endswith("8g") else 0) + rnd)

    def test_screen_runs_the_six_arms_and_records_g1(self):
        calls = []

        def plan(out, argv):
            self.full(out, core.sf8_cells())
            return 0, ""
        ok, note = stages.stage_screen(self.ctx(make_runner(plan, calls)))
        self.assertTrue(ok, note)
        cells = calls[0][calls[0].index("--cells") + 1].split(",")
        self.assertEqual(cells, core.sf8_cells())
        self.assertEqual(self.st.fact("g1_screen")["best_larger"], core.cid("8g"))
        self.assertEqual(self.st.fact("g1_screen")["verdict"], "PASS")

    def test_16g_oom_in_the_screen_is_archived_and_dropped(self):
        calls = []

        def plan(out, argv):
            if len(calls) == 1:
                for c in core.sf8_cells(drop_16g=True):
                    _write_run(out, c, 1, 2000)
                _write_run(out, core.cid("16g"), 1, 0, error="OutOfMemoryError: Java heap space")
                return 2, "=== 6/18 pol-wtinylfu-16g run 1 ===\nOutOfMemoryError: Java heap space"
            self.full(out, core.sf8_cells(drop_16g=True))
            return 0, ""
        ok, note = stages.stage_screen(self.ctx(make_runner(plan, calls)))
        self.assertEqual(len(calls), 2)
        second = calls[1][calls[1].index("--cells") + 1].split(",")
        self.assertNotIn(core.cid("16g"), second)
        self.assertTrue(self.st.fact("heap_infeasible_16g"))
        self.assertTrue(os.path.isfile(os.path.join(self.P["sf8"], "HEAP_INFEASIBLE_screen.md")))

    def test_oom_in_another_arm_is_not_blamed_on_16g(self):
        def plan(out, argv):
            _write_run(out, core.cid("4g"), 1, 0, error="OutOfMemoryError: Java heap space")
            return 2, "=== 4/18 pol-wtinylfu-4g run 1 ===\nOutOfMemoryError"
        ok, _ = stages.stage_screen(self.ctx(make_runner(plan, [])))
        self.assertFalse(ok)
        self.assertFalse(self.st.fact("heap_infeasible_16g"))


class TopUpAndState(Setup):
    def test_topup_adds_rounds_for_the_key_arm_only(self):
        out = self.P["sf8"]
        for rnd in (1, 2, 3):
            for c in core.sf8_cells():
                _write_run(out, c, rnd, 2000 - (300 if c.endswith("16g") else 0) + rnd)
        calls = []

        def plan(o, argv):
            for rnd in (4, 5):
                for c in ("000", core.cid("4g"), core.cid("16g")):
                    _write_run(o, c, rnd, 2000 - (300 if c.endswith("16g") else 0) + rnd)
            return 0, ""
        ok, note = stages.stage_topup(self.ctx(make_runner(plan, calls)))
        self.assertTrue(ok, note)
        self.assertEqual(self.st.fact("topup_cells"), ["000", core.cid("4g"), core.cid("16g")])
        self.assertEqual(calls[0][calls[0].index("--runs") + 1], "5")
        self.assertEqual(self.st.fact("g1_final")["level"], "formal")

    def test_a_section_is_appended_only_once(self):
        ctx = self.ctx(None)
        ctx.text("k", "t", "body\n")
        ctx.text("k", "t", "body\n")
        self.assertEqual(self.md().count("body"), 1)

    def test_status_survives_a_restart(self):
        self.st.set_stage("sf1", state="done", note="x")
        again = core.Status(os.path.join(self.P["root"], "status.json"))
        self.assertEqual(again.stage("sf1")["state"], "done")


class Plan(unittest.TestCase):
    def test_grids_and_policy(self):
        self.assertEqual(core.p3.CAP_POLICY, "wtinylfu")
        self.assertEqual(len(core.sf1_cells()), 8)
        self.assertEqual(len(core.sf8_cells()), 6)
        self.assertNotIn(core.cid("16g"), core.sf8_cells(drop_16g=True))

    def test_stage_order_and_unknown_stage(self):
        self.assertEqual(drv.parse_args(["--stages", "screen,sf1"]).stages, ["sf1", "screen"])
        with self.assertRaises(SystemExit):
            drv.parse_args(["--stages", "nope"])

    def test_paths_are_keyed_by_tag(self):
        a, b = core.paths("a"), core.paths("b")
        self.assertTrue(all(a[k] != b[k] for k in a))

    def test_estimated_total_is_about_eighteen_hours(self):
        total = sum(float(l.split("=")[1].split("h")[0]) for l in drv.plan_lines(core.paths("x"))[:-1])
        self.assertTrue(17.0 < total < 19.0)

    def test_failure_text_is_classified(self):
        self.assertEqual(core.classify("No space left on device"), "enospc")
        self.assertEqual(core.classify("code state changed since x"), "code_state")
        self.assertEqual(core.classify("whatever"), "other")


if __name__ == "__main__":
    unittest.main()
