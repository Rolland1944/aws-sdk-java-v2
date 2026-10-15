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


class PolicyCells(unittest.TestCase):
    def test_policy_phase_has_baseline_and_all_policies(self):
        ids = [c.id for c in p3.cells_for("policy-512m")]
        self.assertEqual(ids, ["000", "pol-wtinylfu-av-512m", "pol-wtinylfu-512m", "pol-sieve-512m",
                               "pol-slru-512m", "pol-lru-512m"])

    def test_policy_cells_differ_only_in_policy(self):
        cells = [p3.policy_cell(p, "4g") for p in p3.POLICIES]
        stripped = {c._replace(id="", d1_policy="") for c in cells}
        self.assertEqual(len(stripped), 1)
        only = stripped.pop()
        self.assertFalse(only.d1_doorkeeper)
        self.assertTrue(only.d1_fixed_capacity)
        self.assertEqual(only.d1_admit_bytes, 0)
        self.assertEqual(only.d1_hard_mib, 4096)

    def test_policy_flag_reaches_argv(self):
        argv = p3.m.bench_argv(_Args, p3.policy_cell("slru", "1g"), "/tmp/out")
        self.assertEqual(argv[argv.index("--track1-d1-policy") + 1], "slru")
        self.assertIn("--no-track1-d1-doorkeeper", argv)
        self.assertEqual(argv[argv.index("--track1-d1-hard-mib") + 1], "1024")

    def test_default_cells_do_not_pass_policy(self):
        argv = p3.m.bench_argv(_Args, p3.ONLINE, "/tmp/out")
        self.assertNotIn("--track1-d1-policy", argv)

    def test_catalog_contains_policy_cells(self):
        self.assertIn("pol-sieve-2g", p3.CATALOG)


class CapacityPhases(unittest.TestCase):
    def test_cap_sf8_arms(self):
        ids = [c.id for c in p3.cells_for("cap-sf8")]
        self.assertEqual(ids, ["000"] + [f"pol-wtinylfu-av-{s}" for s in ("1g", "2g", "4g", "8g", "16g")])

    def test_cap_sf1_arms_mirror_sf8_ratios(self):
        ids = [c.id for c in p3.cells_for("cap-sf1")]
        self.assertEqual(ids[0], "000")
        self.assertEqual([i.rsplit("-", 1)[1] for i in ids[1:]],
                         ["128m", "256m", "512m", "1g", "2g", "4g", "16g"])
        # SF1 / SF8 data ratio is 14.1 / 112.0 = 0.126; 4g -> 512m is the 3.6% arm.
        self.assertAlmostEqual(512 / 1024 / 14.1, 4 / 112.0, delta=0.002)

    def test_capacity_arms_differ_only_in_capacity(self):
        arms = [c for c in p3.cells_for("cap-sf8") if c.id != "000"]
        stripped = {c._replace(id="", d1_hard_mib=0, d1_cache_mib=0) for c in arms}
        self.assertEqual(len(stripped), 1)
        only = stripped.pop()
        self.assertEqual(only.d1_policy, "wtinylfu-av")
        self.assertTrue(only.d1_fixed_capacity)
        self.assertFalse(only.d1_doorkeeper)
        self.assertEqual(only.d1_admit_bytes, 0)
        for c in arms:
            self.assertEqual(c.d1_hard_mib, c.d1_cache_mib)

    def test_phase_pins_data_and_heap(self):
        self.assertTrue(p3.PHASE_DATA["cap-sf8"].endswith("clickbench_sf8"))
        self.assertTrue(p3.PHASE_DATA["cap-sf1"].endswith("clickbench_sf1"))
        for phase in ("cap-sf1", "cap-sf8", "cap-sf8-smoke"):
            self.assertEqual(p3.PHASE_DRIVER_MEMORY[phase], "48g")
        self.assertNotIn("policy-512m", p3.PHASE_DRIVER_MEMORY)

    def test_av_vs_wtlfu_phase(self):
        cells = p3.cells_for("av-vs-wtlfu-sf8-joint2")
        self.assertEqual([c.id for c in cells], ["pol-wtinylfu-av-4g", "pol-wtinylfu-4g"])
        self.assertEqual({c._replace(id="", d1_policy="") for c in cells}.__len__(), 1)
        self.assertTrue(p3.PHASE_DATA["av-vs-wtlfu-sf8-joint2"].endswith("clickbench_sf8_e0_uc1_joint2"))
        self.assertEqual(p3.PHASE_DRIVER_MEMORY["av-vs-wtlfu-sf8-joint2"], "32g")
        wt = cells[1]
        # same cache settings as the finalized 2x2 `joint2-track1-final` cell
        self.assertTrue(wt.d1_adaptive and wt.d1_fixed_capacity)
        self.assertEqual((wt.d1_cache_mib, wt.d1_hard_mib, wt.d1_admit_bytes), (4096, 4096, 0))
        self.assertFalse(wt.d1_doorkeeper)
        self.assertTrue(wt.d1_zero_copy and wt.d1_shared_backing)

    def test_smoke_phase_is_the_largest_sf8_arm_only(self):
        self.assertEqual([c.id for c in p3.cells_for("cap-sf8-smoke")], ["pol-wtinylfu-av-16g"])

    def test_meta_derives_the_dash_d_properties(self):
        import phase3_meta as meta
        spec = p3.CATALOG["pol-wtinylfu-av-8g"]
        _conf, props = meta.spark_conf_for(spec)
        self.assertIn("-Dtrack1.d1.policy=wtinylfu-av", props)
        self.assertIn("-Dtrack1.d1.hard.mib=8192", props)
        self.assertIn("-Dtrack1.d1.fixed.capacity=true", props)
        self.assertIn("-Dtrack1.d1.doorkeeper=false", props)
        self.assertFalse([p for p in props if p.startswith(meta.PER_RUN_PROPS)])
        conf, _ = meta.spark_conf_for(spec)
        self.assertNotIn("spark.hadoop.fs.s3a.secret.key", conf)
        self.assertNotIn("spark.hadoop.fs.s3a.access.key", conf)

    def test_meta_write_refuses_a_changed_build(self):
        import json
        import tempfile
        import phase3_meta as meta
        with tempfile.TemporaryDirectory() as d:
            rec = {"jar_sha256": "a", "git_head": "h", "git_diff_sha256": "d",
                   "recorded_at": "t", "argv0": []}
            meta.write(d, rec)
            meta.write(d, dict(rec))  # same build: resume is fine
            with self.assertRaises(SystemExit):
                meta.write(d, dict(rec, jar_sha256="b"))
            with open(f"{d}/meta.json") as fh:
                self.assertEqual(json.load(fh)["jar_sha256"], "a")


class DiskGuard(unittest.TestCase):
    def test_clean_removes_only_spark_leftovers(self):
        import os
        import tempfile
        import phase3_meta as meta
        with tempfile.TemporaryDirectory() as d:
            for name in ("blockmgr-abc", "spark-123", "keep-me", "notes"):
                os.makedirs(os.path.join(d, name, "sub"))
                with open(os.path.join(d, name, "sub", "f"), "w") as fh:
                    fh.write("x")
            real = meta.scratch_in_use
            meta.scratch_in_use = lambda: False
            try:
                self.assertEqual(meta.scratch_clean(d), 2)
            finally:
                meta.scratch_in_use = real
            self.assertEqual(sorted(os.listdir(d)), ["keep-me", "notes"])

    def test_clean_refuses_while_a_run_is_alive(self):
        import tempfile
        import phase3_meta as meta
        real = meta.scratch_in_use
        meta.scratch_in_use = lambda: True
        try:
            with tempfile.TemporaryDirectory() as d:
                with self.assertRaises(SystemExit):
                    meta.scratch_clean(d)
        finally:
            meta.scratch_in_use = real

    def test_guard_threshold(self):
        import tempfile
        import phase3_meta as meta
        with tempfile.TemporaryDirectory() as d:
            ok, free = meta.disk_guard(d, 0.0)
            self.assertTrue(ok)
            self.assertGreater(free, 0)
            self.assertFalse(meta.disk_guard(d, free + 1024.0)[0])

    def test_sampler_records_minimum(self):
        import tempfile
        import time
        import phase3_meta as meta
        with tempfile.TemporaryDirectory() as d:
            with meta.DiskSampler(d, period_s=0.05) as sampler:
                time.sleep(0.2)
            self.assertGreaterEqual(sampler.samples, 2)
            self.assertIsNotNone(sampler.min_free)

    def test_default_threshold_covers_the_observed_scratch_peak(self):
        self.assertGreaterEqual(p3.DEFAULT_MIN_FREE_GIB, 46.0)


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
