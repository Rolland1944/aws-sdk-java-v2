#!/usr/bin/env python3
"""P0: Track1 S3A factory stays off unless the benchmark flag is set."""

from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import s3a_session  # noqa: E402

FACTORY_KEY = "spark.hadoop.fs.s3a.s3.client.factory.impl"


class _Builder:
    def __init__(self):
        self.conf = {}

    def config(self, key, value):
        self.conf[key] = value
        return self


class Track1FactorySwitch(unittest.TestCase):
    def test_factory_class_name_is_stable(self):
        self.assertEqual(
            s3a_session.TRACK1_S3_CLIENT_FACTORY,
            "software.amazon.awssdk.s3.adaptive.s3a.Track1S3ClientFactory")

    def test_default_does_not_install_factory(self):
        built = s3a_session.apply_frozen_reader(_Builder(), "ak", "sk")
        self.assertNotIn(FACTORY_KEY, built.conf)
        self.assertNotIn("track1.s3a.probe.dir",
                         built.conf.get("spark.driver.extraJavaOptions", ""))

    def test_opt_in_installs_factory_and_probe_dir(self):
        collector = os.path.join(os.path.dirname(HERE), "_tmp_track1_probe_test")
        built = s3a_session.apply_frozen_reader(
            _Builder(), "ak", "sk", collector_dir=collector, track1_s3a=True)
        self.assertEqual(built.conf[FACTORY_KEY],
                         s3a_session.TRACK1_S3_CLIENT_FACTORY)
        extra = built.conf["spark.driver.extraJavaOptions"]
        self.assertIn("track1.s3a.probe.dir", extra)
        self.assertIn("-Dtrack1.d1=false", extra)
        self.assertIn("-Dtrack1.d2=false", extra)
        self.assertIn("-Dtrack1.d4=false", extra)
        self.assertEqual(built.conf["spark.executor.extraJavaOptions"], extra)

    def test_dimensions_are_explicit_java_opts(self):
        built = s3a_session.apply_frozen_reader(
            _Builder(), "ak", "sk", track1_s3a=True,
            track1_d1=True, track1_d2=True, track1_d4=True,
            track1_d2_wait_us=50)
        extra = built.conf["spark.driver.extraJavaOptions"]
        self.assertIn("-Dtrack1.d1=true", extra)
        self.assertIn("-Dtrack1.d2=true", extra)
        self.assertIn("-Dtrack1.d4=true", extra)
        self.assertIn("-Dtrack1.d2.wait.us=50", extra)
        self.assertIn("-Dtrack1.d1.admit.bytes=262144", extra)
        self.assertIn("-Dtrack1.d1.cache.mib=256", extra)
        # block size stays on the reader's own default unless asked for
        self.assertNotIn("-Dtrack1.d1.block.bytes", extra)

    def test_admission_cap_and_budget_are_passed_together(self):
        built = s3a_session.apply_frozen_reader(
            _Builder(), "ak", "sk", track1_s3a=True, track1_d1=True,
            track1_d1_admit_bytes=8 * 1024 * 1024,
            track1_d1_cache_mib=2048,
            track1_d1_block_bytes=8 * 1024 * 1024)
        extra = built.conf["spark.driver.extraJavaOptions"]
        self.assertIn("-Dtrack1.d1.admit.bytes=8388608", extra)
        self.assertIn("-Dtrack1.d1.cache.mib=2048", extra)
        self.assertIn("-Dtrack1.d1.block.bytes=8388608", extra)
        self.assertIn("-Dtrack1.d1.adaptive=false", extra)

    def test_adaptive_knobs_are_explicit_java_opts(self):
        built = s3a_session.apply_frozen_reader(
            _Builder(), "ak", "sk", track1_s3a=True, track1_d1=True,
            track1_d1_adaptive=True, track1_d1_hard_mib=4096,
            track1_d1_coverage=0.5, track1_d1_observe_gets=256,
            track1_d1_min_mib=16)
        extra = built.conf["spark.driver.extraJavaOptions"]
        self.assertIn("-Dtrack1.d1.adaptive=true", extra)
        self.assertIn("-Dtrack1.d1.hard.mib=4096", extra)
        self.assertIn("-Dtrack1.d1.coverage=0.5", extra)
        self.assertIn("-Dtrack1.d1.observe.gets=256", extra)
        self.assertIn("-Dtrack1.d1.min.mib=16", extra)


if __name__ == "__main__":
    unittest.main()
