#!/usr/bin/env python3
"""Regression fixtures for the repaired L1 GET model."""

from __future__ import annotations

import math
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import compression_probe as probe  # noqa: E402
import correlate  # noqa: E402
import layout_actions as la  # noqa: E402
import plan_deterministic  # noqa: E402
import virtual_footer as vf  # noqa: E402
import whatif  # noqa: E402


def _rec(**kwargs):
    base = {
        "range_offset": 0,
        "range_length": 1024,
        "ts_wall_ms": 0,
        "ts_start_ns": 0,
        "thread": "s3a-transfer-t1",
        "audit_path": "bucket/hits/part-00000.parquet",
        "path": "bucket/hits/part-00000.parquet",
    }
    base.update(kwargs)
    return base


class AuditSpanGrouping(unittest.TestCase):
    def test_audit_span_collapses_transfer_threads(self):
        records = [
            _rec(thread="s3a-transfer-t1", audit_span_id="span-a",
                 audit_process_id="p1", ts_wall_ms=1),
            _rec(thread="s3a-transfer-t2", audit_span_id="span-a",
                 audit_process_id="p1", ts_wall_ms=2),
            _rec(thread="s3a-transfer-t3", audit_span_id="span-b",
                 audit_process_id="p1", ts_wall_ms=3,
                 audit_path="bucket/hits/part-00001.parquet",
                 path="bucket/hits/part-00001.parquet"),
        ]
        ids, n, grouping = correlate.assign_episodes(records)
        self.assertEqual(grouping, "audit_span")
        self.assertEqual(n, 2)
        self.assertEqual(ids[0], ids[1])
        self.assertNotEqual(ids[0], ids[2])

    def test_gap_fallback_without_audit_ids(self):
        records = [
            _rec(thread="t1", ts_wall_ms=1),
            _rec(thread="t1", ts_wall_ms=100),
            _rec(thread="t1", ts_wall_ms=5000),
        ]
        for rec in records:
            rec.pop("audit_span_id", None)
            rec.pop("audit_process_id", None)
        ids, n, grouping = correlate.assign_episodes(records, gap_ms=2000)
        self.assertEqual(grouping, "gap_fallback")
        self.assertEqual(n, 2)
        self.assertEqual(ids[0], ids[1])
        self.assertNotEqual(ids[0], ids[2])


class ScanUnitCount(unittest.TestCase):
    def test_split_count_scales_with_file_size(self):
        split = 128 * 1024 * 1024
        small = vf.scan_unit_count([100 * 1024 * 1024] * 110, split)
        large = vf.scan_unit_count([900 * 1024 * 1024] * 17, split)
        self.assertEqual(small, 110)
        self.assertEqual(large, 17 * 8)
        self.assertGreater(large, 100)


class DummyCatalog:
    def __init__(self):
        self.BASELINE_GEOMETRY = {
            "hits": {
                "files": 110,
                "n_rg": 165,
                "rg_per_file": 1.5,
                "rg_bytes": 200 * 1024 * 1024,
                "compressed_bytes": 15 * 1024 ** 3,
                "file_sizes": [136 * 1024 * 1024] * 110,
                "n_scan_units": 110,
                "split_size_bytes": 128 * 1024 * 1024,
            }
        }
        self.COLUMN_ORDER = {"hits": ["a", "b", "c"]}
        self.COLUMN_SHARE = {"hits": {"a": 10, "b": 10, "c": 10}}
        # Deliberately a different split from COLUMN_SHARE: `a` compresses
        # worse than its neighbours, so it carries less of the decoder's work
        # than its compressed share suggests. A decode term that reached for
        # COLUMN_SHARE would pass an equal-share fixture and fail here.
        self.COLUMN_UNCOMPRESSED_SHARE = {"hits": {"a": 20, "b": 40, "c": 40}}
        self.ALL_COLUMNS = {"hits": ["a", "b", "c"]}
        self.COLUMN_STATS = {}
        self.RG_CHUNK_SAMPLES = {
            "hits": [{"a": 100000, "b": 100000, "c": 100000}]
        }
        self.PATTERNS = [{
            "pattern_id": "hits/0000",
            "table": "hits",
            "columns": ["a"],
            "n_columns": 1,
            "n_episodes": 100,
            "data_requests": 1000,
            "data_bytes": 1000 * 1024 * 1024,
            "requests_per_episode": 10,
            "bytes_per_episode": 10 * 1024 * 1024,
            "meta_requests_per_episode": 0,
            "scan_meta_requests": 200,
            "rg_touched_total": 1650,
            "rg_per_episode": 16.5,
        }]
        self.REQUEST_SHAPE = {
            "data_requests": 1000,
            "meta_requests": 400,
            "data_bytes": 1000 * 1024 * 1024,
            "meta_bytes": 400 * 8192,
            "file_meta_requests": 200,
            "scan_meta_requests": 200,
            "multicol_data_span_share": 0.1,
        }
        self.LARGE_TABLE_BYTES = 1
        self.BASELINE_RG_BYTES = 128 * 1024 * 1024


class RgAccessScaling(unittest.TestCase):
    def setUp(self):
        self.cat = DummyCatalog()
        whatif.bind_catalog(self.cat)
        self.regime = {"rtt_s": 0.025, "bw_bps": 200 * 1024 * 1024, "K_busy": 6.0}
        self.vectored = {"min_seek_bytes": 131072, "max_merged_bytes": 2097152}

    def test_data_gets_scale_with_n_rg(self):
        base = whatif.evaluate_pattern(
            self.cat.PATTERNS[0], {"candidate_id": "baseline", "actions": []},
            self.regime, self.vectored)
        grown = dict(self.cat.BASELINE_GEOMETRY["hits"])
        grown["n_rg"] = 330
        self.cat.BASELINE_GEOMETRY["hits"] = self.cat.BASELINE_GEOMETRY["hits"]
        cand = {
            "candidate_id": "more-rg",
            "actions": [],
            "tables": {"hits": {"rg_bytes": grown["rg_bytes"] / 2}},
        }
        ev = whatif.evaluate_pattern(
            self.cat.PATTERNS[0], cand, self.regime, self.vectored)
        self.assertGreater(ev["data_gets"], base["data_gets"] * 1.5)

    def test_file_merge_does_not_hold_data_flat_when_n_rg_rises(self):
        base = whatif.evaluate_workload(
            {"candidate_id": "baseline", "actions": []},
            self.cat.PATTERNS, self.regime, self.vectored)
        cand = {
            "candidate_id": "16f",
            "actions": [{"canonical": "write.target-file-size-bytes",
                         "value": 939470939, "table": "hits"}],
            "tables": {"hits": {"file_bytes": 939470939, "rg_bytes": None}},
        }
        # Force predict_geometry to keep n_rg unless we overlay; freeze n_rg
        # by leaving rg unset. Data GETs should stay near baseline, not drop
        # with file count.
        ev = whatif.evaluate_workload(cand, self.cat.PATTERNS, self.regime,
                                      self.vectored)
        self.assertAlmostEqual(ev["data_gets"], base["data_gets"], delta=50)

    def test_single_column_seriation_invariant(self):
        base_geom = vf.predict_geometry("hits", {"actions": []})
        g1, _ = vf.merge_gets("hits", ["a"], base_geom, order=["a", "b", "c"])
        g2, _ = vf.merge_gets("hits", ["a"], base_geom, order=["c", "b", "a"])
        self.assertEqual(g1, g2)


class MetadataDecomposition(unittest.TestCase):
    def setUp(self):
        self.cat = DummyCatalog()
        whatif.bind_catalog(self.cat)
        self.regime = {"rtt_s": 0.025, "bw_bps": 200 * 1024 * 1024, "K_busy": 6.0}
        self.vectored = {"min_seek_bytes": 131072, "max_merged_bytes": 2097152}

    def test_file_and_scan_meta_reported_separately(self):
        ev = whatif.evaluate_workload(
            {"candidate_id": "baseline", "actions": []},
            self.cat.PATTERNS, self.regime, self.vectored)
        self.assertIn("data_gets", ev)
        self.assertIn("file_meta_gets", ev)
        self.assertIn("scan_meta_gets", ev)
        self.assertEqual(ev["ranged_gets"],
                         ev["data_gets"] + ev["file_meta_gets"] + ev["scan_meta_gets"])

    def test_file_meta_scales_with_files_scan_meta_with_splits(self):
        base = whatif.evaluate_workload(
            {"candidate_id": "baseline", "actions": []},
            self.cat.PATTERNS, self.regime, self.vectored)
        cand = {
            "candidate_id": "16f",
            "actions": [],
            "tables": {"hits": {"file_bytes": 939470939}},
        }
        ev = whatif.evaluate_workload(cand, self.cat.PATTERNS, self.regime,
                                      self.vectored)
        self.assertLess(ev["file_meta_gets"], base["file_meta_gets"])
        # 17 ~896MiB files -> ~8 splits each, close to baseline 110 units
        self.assertGreater(ev["scan_meta_gets"], ev["file_meta_gets"])


class GeometryGate(unittest.TestCase):
    def test_geometry_matches_and_rejects(self):
        ok, fails = whatif.geometry_matches(
            {"n_files": 16, "n_rg": 165, "compressed_bytes": 1000},
            {"files": 16, "n_rg": 165, "compressed_bytes": 1000})
        self.assertTrue(ok)
        self.assertEqual(fails, [])
        # Last-file remainder is allowed; n_rg is not a remainder.
        ok, fails = whatif.geometry_matches(
            {"n_files": 16, "n_rg": 165, "compressed_bytes": 1000},
            {"files": 17, "n_rg": 165, "compressed_bytes": 1000})
        self.assertTrue(ok)
        ok, fails = whatif.geometry_matches(
            {"n_files": 16, "n_rg": 165, "compressed_bytes": 1000},
            {"files": 17, "n_rg": 225, "compressed_bytes": 1000})
        self.assertFalse(ok)
        self.assertTrue(any("n_rg" in f for f in fails))

    def test_probe_sample_bytes_use_wider_tolerance(self):
        # 20 vs 23 files / 8.61 vs 10.24 GiB is the ClickBench probe miss.
        ok, fails = whatif.geometry_matches(
            {"n_files": 20, "n_rg": 169, "compressed_bytes": 8610000000},
            {"files": 23, "n_rg": 169, "compressed_bytes": 10240000000})
        self.assertTrue(ok, fails)
        ok, fails = whatif.geometry_matches(
            {"n_files": 20, "n_rg": 169, "compressed_bytes": 8000000000},
            {"files": 23, "n_rg": 169, "compressed_bytes": 12000000000})
        self.assertFalse(ok)
        self.assertTrue(any("compressed_bytes" in f for f in fails))


class RowGroupMatch(unittest.TestCase):
    def test_ceil_reproduces_source_rg_count(self):
        import write_layout_pyarrow as w
        n_rows, n_rg = 99997497, 169
        size = w.rows_per_rg_to_match(n_rows, n_rg)
        self.assertEqual(size, 591702)
        self.assertEqual(math.ceil(n_rows / size), n_rg)
        self.assertGreater(math.ceil(n_rows / (n_rows // n_rg)), n_rg)


class FileDiscoveryThread(unittest.TestCase):
    def test_footer_pool_is_file_meta(self):
        self.assertTrue(correlate.is_file_discovery_thread(
            "readingParquetFooters-ForkJoinPool-3-worker-1"))
        self.assertFalse(correlate.is_file_discovery_thread(
            "s3a-transfer-home-haoyue-bounded-pool1-t28"))


class CandidateValidateAndAblation(unittest.TestCase):
    def setUp(self):
        self.cat = DummyCatalog()
        whatif.bind_catalog(self.cat)
        self.regime = {"rtt_s": 0.025, "bw_bps": 200 * 1024 * 1024, "K_busy": 6.0}
        self.vectored = {"min_seek_bytes": 131072, "max_merged_bytes": 2097152}
        self.base_prof = {"request_shape": {
            "data_requests": 1000, "meta_requests": 400,
            "file_meta_requests": 200, "scan_meta_requests": 200,
            "data_bytes": 1000 * 1024 * 1024, "meta_bytes": 400 * 8192,
        }}

    def test_validate_candidate_data_direction(self):
        more_rg = {
            "candidate_id": "more-rg",
            "actions": [],
            "tables": {"hits": {"rg_bytes": 100 * 1024 * 1024}},
        }
        ev = whatif.evaluate_workload(
            more_rg, self.cat.PATTERNS, self.regime, self.vectored)
        cand_prof = {"request_shape": {
            "data_requests": ev["data_gets"],
            "meta_requests": ev["file_meta_gets"] + ev["scan_meta_gets"],
            "file_meta_requests": ev["file_meta_gets"],
            "scan_meta_requests": ev["scan_meta_gets"],
        }}
        report, _ev, ok = whatif.run_validate_candidate(
            self.regime, self.vectored, self.base_prof, cand_prof, more_rg)
        self.assertTrue(report["data_direction_ok"])
        self.assertTrue(ok)

    def test_validate_candidate_rejects_wrong_data_direction(self):
        more_rg = {
            "candidate_id": "more-rg",
            "actions": [],
            "tables": {"hits": {"rg_bytes": 100 * 1024 * 1024}},
        }
        cand_prof = {"request_shape": {
            "data_requests": 500,
            "meta_requests": 400,
            "file_meta_requests": 200,
            "scan_meta_requests": 200,
        }}
        report, _ev, ok = whatif.run_validate_candidate(
            self.regime, self.vectored, self.base_prof, cand_prof, more_rg)
        self.assertFalse(report["data_direction_ok"])
        self.assertFalse(ok)

    def test_ablation_candidates_split_file_and_order(self):
        plan = {"actions": [
            {"canonical": "write.parquet.column-order", "value": ["b", "a"],
             "table": "hits"},
            {"canonical": "write.target-file-size-bytes", "value": 939470939,
             "table": "hits"},
        ]}
        ids = [c["candidate_id"] for c in whatif.ablation_candidates(plan)]
        self.assertEqual(ids, ["identity", "file-only", "order-only", "file+order"])
        report = whatif.run_l1_ablations(self.regime, self.vectored, plan)
        by_id = {row["id"]: row for row in report["variants"]}
        self.assertEqual(by_id["identity"]["data_gets"],
                         by_id["file-only"]["data_gets"])
        self.assertLess(by_id["file-only"]["file_meta_gets"],
                        by_id["identity"]["file_meta_gets"])
        self.assertEqual(by_id["file+order"]["n_files"],
                         by_id["file-only"]["n_files"])


def _probe_doc(version=3, page=None, joint=None):
    """A layout-probe document over the DummyCatalog's three columns.

    The encoded ratios are set up so the two axes disagree: the codec moves
    wire bytes and leaves encoded bytes alone, while the encoding moves both
    and in different proportions. Any model that reads one ratio for the other
    gets a different answer here.
    """
    columns = {}
    for column in ("a", "b", "c"):
        columns[column] = {
            "physical_type": "INT64",
            "joint": joint if joint is not None else [
                {"codec": "snappy", "encoding": "baseline", "bytes": 1000,
                 "ratio": 1.0, "encoded_bytes": 2000, "encoded_ratio": 1.0},
                {"codec": "zstd", "encoding": "baseline", "bytes": 800,
                 "ratio": 0.8, "encoded_bytes": 2000, "encoded_ratio": 1.0},
                {"codec": "zstd", "encoding": "DELTA_BINARY_PACKED",
                 "bytes": 700, "ratio": 0.7, "encoded_bytes": 1000,
                 "encoded_ratio": 0.5},
            ],
            "pruned": [],
            "baseline_bytes": 1000,
            "baseline_encoded_bytes": 2000,
        }
    doc = {"schema_version": version,
           "tables": {"hits": {"sample_rows": 1000, "columns": columns}}}
    if page:
        doc["tables"]["hits"]["page"] = page
    return doc


def _decode_doc(rates=None):
    """A decode-probe document covering the fixture's one physical type."""
    return {
        "schema_version": 1,
        "readers": ["pyarrow"],
        "rates": {"pyarrow": {"INT64": rates if rates is not None else {
            # DELTA halves the encoded bytes and halves the rate, so total
            # decode time is unchanged. A model using only `relative_cost`
            # would report it as twice as expensive.
            "snappy|baseline": {"bytes_per_s": 500_000_000},
            "zstd|baseline": {"bytes_per_s": 500_000_000},
            "zstd|DELTA_BINARY_PACKED": {"bytes_per_s": 250_000_000},
        }}},
    }


class DecodePricing(unittest.TestCase):
    """The decode term: encoded bytes over a measured rate, and nothing else."""

    def setUp(self):
        self.cat = DummyCatalog()
        whatif.bind_catalog(self.cat)
        self.regime = {"rtt_s": 0.025, "bw_bps": 200 * 1024 * 1024, "K_busy": 6.0}
        self.vectored = {"min_seek_bytes": 131072, "max_merged_bytes": 2097152}
        vf.bind_probe(_probe_doc())
        vf.bind_decode_profile(_decode_doc())

    def tearDown(self):
        vf.bind_probe(None)
        vf.bind_decode_profile(None)

    def _ev(self, actions):
        return whatif.evaluate_workload(
            {"candidate_id": "x", "actions": actions}, self.cat.PATTERNS,
            self.regime, self.vectored)

    def test_baseline_with_no_actions_is_still_fully_priced(self):
        # An empty action list is falsy, and the byte model uses that to mean
        # "nothing requested". Decode is absolute, so the baseline has to
        # resolve to the baseline tuple rather than to no tuple at all.
        ev = self._ev([])
        self.assertTrue(ev["decode_priced"], ev["decode_unpriced_columns"])
        self.assertGreater(ev["decode_core_s"], 0.0)

    def test_encoded_bytes_not_wire_bytes_drive_decode(self):
        # zstd cuts wire bytes 20% and leaves encoded bytes alone, so it must
        # move t_io and not decode.
        base, zstd = self._ev([]), self._ev([
            {"canonical": la.COMPRESSION, "value": "zstd", "table": "hits"}])
        self.assertLess(zstd["t_io_s"], base["t_io_s"])
        self.assertAlmostEqual(zstd["decode_core_s"], base["decode_core_s"],
                               places=6)

    def test_halved_bytes_at_halved_rate_costs_the_same(self):
        # The error this exists to catch: pricing DELTA off `relative_cost`
        # alone charges 2x for an encoding that also halves what is decoded.
        base = self._ev([])
        delta = self._ev([
            {"canonical": la.COMPRESSION, "value": "zstd", "table": "hits"},
            {"canonical": la.ENCODING_COLUMN_PREFIX + "a",
             "value": "DELTA_BINARY_PACKED", "table": "hits"}])
        self.assertAlmostEqual(delta["decode_core_s"], base["decode_core_s"],
                               places=6)

    def test_row_group_size_does_not_move_decode(self):
        # Decode work follows rows and columns read, never their grouping. If
        # this fails, an rg action shows up as a CPU change and the execution
        # residual stops being constant across candidates.
        base = self._ev([])
        rg = self._ev([{"canonical": la.ROW_GROUP_SIZE,
                        "value": 64 * 1024 * 1024, "table": "hits"}])
        self.assertNotEqual(rg["t_io_s"], base["t_io_s"])
        self.assertAlmostEqual(rg["decode_core_s"], base["decode_core_s"],
                               places=6)

    def test_decode_uses_the_encoded_share_not_the_compressed_one(self):
        # Pattern reads only column `a`, which holds 20/100 of encoded bytes
        # but 10/30 of compressed bytes.
        plan, unpriced = vf.decode_plan("hits", {"actions": []})
        self.assertEqual(unpriced, [])
        per_scan, missing = vf.decode_core_s_per_scan("hits", ["a"], plan)
        self.assertEqual(missing, [])
        total_encoded = (200 * 1024 * 1024) * 165
        expected = total_encoded * 0.2 / 500_000_000
        self.assertAlmostEqual(per_scan, expected, places=6)

    def test_unmeasured_rate_leaves_the_total_unpriced_not_smaller(self):
        vf.bind_decode_profile(_decode_doc(rates={
            "zstd|baseline": {"bytes_per_s": 500_000_000}}))
        ev = self._ev([])
        self.assertFalse(ev["decode_priced"])
        self.assertIsNone(ev["decode_core_s"])
        # Ranking must fall back to IO rather than to a partial CPU bill.
        self.assertEqual(ev["t_cost_s"], ev["t_io_s"])

    def test_v2_probe_has_no_encoded_bytes_and_stays_unpriced(self):
        vf.bind_probe(_probe_doc(version=2, joint=[
            {"codec": "snappy", "encoding": "baseline", "bytes": 1000,
             "ratio": 1.0}]))
        ev = self._ev([])
        self.assertFalse(ev["decode_priced"])
        self.assertIsNone(ev["decode_core_s"])

    def test_unbound_decode_profile_prices_io_only(self):
        vf.bind_decode_profile(None)
        ev = self._ev([])
        self.assertFalse(ev["decode_priced"])
        self.assertEqual(ev["t_cost_s"], ev["t_io_s"])

    def test_binding_decode_does_not_move_the_io_term(self):
        # The two validate gates compare t_io_s against measured GETs and
        # bytes. Decode must not leak into it.
        with_decode = self._ev([])["t_io_s"]
        vf.bind_decode_profile(None)
        self.assertEqual(self._ev([])["t_io_s"], with_decode)

    def test_rate_scale_is_linear_in_the_total(self):
        one = self._ev([])["decode_core_s"]
        vf.bind_decode_profile(_decode_doc(), rate_scale=4.0)
        self.assertAlmostEqual(self._ev([])["decode_core_s"], one / 4.0,
                               places=6)


class JointCodecEncodingPricing(unittest.TestCase):
    """Codec and encoding are one measurement, never a product of two."""

    def tearDown(self):
        vf.bind_probe(None)

    def setUp(self):
        self.cat = DummyCatalog()
        whatif.bind_catalog(self.cat)

    def test_joint_tuple_is_not_the_product_of_its_parts(self):
        vf.bind_probe(_probe_doc())
        rendered = la.render([
            {"canonical": la.COMPRESSION_COLUMN_PREFIX + "a", "value": "zstd",
             "table": "hits"},
            {"canonical": la.ENCODING_COLUMN_PREFIX + "a",
             "value": "DELTA_BINARY_PACKED", "table": "hits"},
        ], table="hits")
        ratios, unpriced = vf.layout_ratios("hits", rendered)
        self.assertEqual(unpriced, [])
        # The measured pair is 0.7, not codec 0.8 times any encoding ratio.
        self.assertAlmostEqual(ratios["a"], 0.7)

    def test_unmeasured_tuple_is_unpriced_not_one(self):
        vf.bind_probe(_probe_doc())
        rendered = la.render([
            {"canonical": la.ENCODING_COLUMN_PREFIX + "a",
             "value": "DELTA_BINARY_PACKED", "table": "hits"},
        ], table="hits")
        ratios, unpriced = vf.layout_ratios("hits", rendered)
        # (snappy, DELTA_BINARY_PACKED) was never measured.
        self.assertNotIn("a", ratios or {})
        self.assertEqual([u["column"] for u in unpriced], ["a"])

    def test_v1_probe_refuses_to_price_encoding(self):
        doc = _probe_doc(version=1)
        doc["tables"]["hits"]["columns"]["a"]["ratios"] = {"snappy": 0.5,
                                                          "zstd": 0.4}
        vf.bind_probe(doc)
        rendered = la.render([
            {"canonical": la.ENCODING_COLUMN_PREFIX + "a",
             "value": "DELTA_BINARY_PACKED", "table": "hits"},
        ], table="hits")
        self.assertTrue(vf.unpriced_encodings("hits", rendered))

    def test_cheaper_tuple_shrinks_predicted_bytes(self):
        vf.bind_probe(_probe_doc())
        base = whatif.evaluate_workload(
            {"candidate_id": "baseline", "actions": []},
            self.cat.PATTERNS, {"rtt_s": 0.025, "bw_bps": 2 ** 28, "K_busy": 6.0},
            {"min_seek_bytes": 131072, "max_merged_bytes": 2097152})
        cand = {"candidate_id": "joint", "actions": [
            {"canonical": la.COMPRESSION_COLUMN_PREFIX + c, "value": "zstd",
             "table": "hits"} for c in ("a", "b", "c")
        ] + [
            {"canonical": la.ENCODING_COLUMN_PREFIX + c,
             "value": "DELTA_BINARY_PACKED", "table": "hits"}
            for c in ("a", "b", "c")
        ]}
        ev = whatif.evaluate_workload(
            cand, self.cat.PATTERNS,
            {"rtt_s": 0.025, "bw_bps": 2 ** 28, "K_busy": 6.0},
            {"min_seek_bytes": 131072, "max_merged_bytes": 2097152})
        self.assertLess(ev["bytes"], base["bytes"])


class PageGeometryPricing(unittest.TestCase):
    """The page axis is one-sided and measured, never a square-root proxy."""

    PAGE = {
        "resolved": True,
        "default_page_bytes": 1024 * 1024,
        "index_bytes_per_page": 70.0,
        "noop_page_bytes": [1024 * 1024],
        "points": [],
    }

    def setUp(self):
        self.cat = DummyCatalog()
        whatif.bind_catalog(self.cat)
        self.regime = {"rtt_s": 0.025, "bw_bps": 2 ** 28, "K_busy": 6.0}
        self.vectored = {"min_seek_bytes": 131072, "max_merged_bytes": 2097152}
        vf.bind_probe(_probe_doc(page=self.PAGE))

    def tearDown(self):
        vf.bind_probe(None)

    def test_measured_noop_page_collapses_but_others_survive(self):
        noop = vf.layout_for("hits", {"actions": [
            {"canonical": la.PAGE_SIZE, "value": 1024 * 1024,
             "table": "hits"}]})
        self.assertIsNone(noop["page_bytes"])
        # A larger page is not automatically a no-op: on a wide table it
        # really does merge pages and shrink the index.
        live = vf.layout_for("hits", {"actions": [
            {"canonical": la.PAGE_SIZE, "value": 4 * 1024 * 1024,
             "table": "hits"}]})
        self.assertEqual(live["page_bytes"], 4 * 1024 * 1024)

    def test_coarser_page_saves_index_bytes(self):
        base = whatif.evaluate_workload(
            {"candidate_id": "baseline", "actions": []},
            self.cat.PATTERNS, self.regime, self.vectored)
        ev = whatif.evaluate_workload(
            {"candidate_id": "coarse", "actions": [
                {"canonical": la.PAGE_SIZE, "value": 4 * 1024 * 1024,
                 "table": "hits"}]},
            self.cat.PATTERNS, self.regime, self.vectored)
        self.assertLess(ev["page_index_bytes"], 0)
        self.assertLess(ev["bytes"], base["bytes"])

    def test_finer_page_survives_and_costs_index_bytes(self):
        cand = {"candidate_id": "fine", "actions": [
            {"canonical": la.PAGE_SIZE, "value": 256 * 1024, "table": "hits"}]}
        self.assertEqual(vf.layout_for("hits", cand)["page_bytes"], 256 * 1024)
        base = whatif.evaluate_workload(
            {"candidate_id": "baseline", "actions": []},
            self.cat.PATTERNS, self.regime, self.vectored)
        ev = whatif.evaluate_workload(cand, self.cat.PATTERNS, self.regime,
                                      self.vectored)
        self.assertGreater(ev["page_index_bytes"], 0)
        self.assertEqual(base["page_index_bytes"], 0)
        self.assertGreater(ev["bytes"], base["bytes"])

    def test_finer_page_does_not_change_data_gets(self):
        base = whatif.evaluate_workload(
            {"candidate_id": "baseline", "actions": []},
            self.cat.PATTERNS, self.regime, self.vectored)
        ev = whatif.evaluate_workload(
            {"candidate_id": "fine", "actions": [
                {"canonical": la.PAGE_SIZE, "value": 256 * 1024,
                 "table": "hits"}]},
            self.cat.PATTERNS, self.regime, self.vectored)
        self.assertEqual(ev["data_gets"], base["data_gets"])

    def test_unresolved_page_probe_leaves_the_axis_alone(self):
        vf.bind_probe(_probe_doc(page=dict(self.PAGE, resolved=False)))
        cand = {"candidate_id": "fine", "actions": [
            {"canonical": la.PAGE_SIZE, "value": 256 * 1024, "table": "hits"}]}
        ev = whatif.evaluate_workload(cand, self.cat.PATTERNS, self.regime,
                                      self.vectored)
        self.assertEqual(ev["page_index_bytes"], 0)


class ProbeMeasurement(unittest.TestCase):
    """The probe itself: type pruning, fallback detection, page resolution."""

    def test_physical_type_pruning_and_joint_grid(self):
        pa = __import__("pyarrow")
        table = pa.table({"n": pa.array([1, 2, 3, 4], pa.int64())})
        rec = probe.probe_column(table, "n", ["snappy"],
                                 [probe.BASELINE_ENCODING,
                                  "DELTA_BINARY_PACKED", "DELTA_BYTE_ARRAY"])
        self.assertEqual(rec["physical_type"], "INT64")
        measured = {(p["codec"], p["encoding"]) for p in rec["joint"]}
        self.assertIn(("snappy", "DELTA_BINARY_PACKED"), measured)
        # BYTE_ARRAY-only family must be pruned, not written and mismeasured.
        self.assertNotIn(("snappy", "DELTA_BYTE_ARRAY"), measured)
        self.assertTrue(any(p["encoding"] == "DELTA_BYTE_ARRAY"
                            for p in rec["pruned"]))

    def test_ratios_are_against_the_baseline_tuple(self):
        pa = __import__("pyarrow")
        table = pa.table({"n": pa.array(list(range(5000)), pa.int64())})
        rec = probe.probe_column(table, "n", ["snappy", "zstd"],
                                 [probe.BASELINE_ENCODING])
        base = next(p for p in rec["joint"]
                    if p["codec"] == "snappy"
                    and p["encoding"] == probe.BASELINE_ENCODING)
        self.assertEqual(base["ratio"], 1.0)
        self.assertEqual(rec["baseline_bytes"], base["bytes"])

    def test_encoding_fallback_is_detected(self):
        self.assertFalse(probe._encoding_landed("DELTA_BYTE_ARRAY", ["PLAIN"]))
        self.assertTrue(probe._encoding_landed("RLE_DICTIONARY",
                                               ["PLAIN_DICTIONARY", "RLE"]))
        self.assertTrue(probe._encoding_landed(probe.BASELINE_ENCODING, ["PLAIN"]))

    def test_tiny_sample_reports_no_page_resolution(self):
        pa = __import__("pyarrow")
        table = pa.table({"n": pa.array(list(range(100)), pa.int64())})
        rec = probe.probe_page_geometry(table, (256 * 1024,), 1024 * 1024,
                                        "snappy")
        self.assertFalse(rec["resolved"])
        self.assertIsNone(rec["index_bytes_per_page"])


class SixAxisSearch(unittest.TestCase):
    """Every axis carries its baseline, and an unpriced pick degrades."""

    def setUp(self):
        self.cat = DummyCatalog()
        self.cat.profile = type("P", (), {"doc": {}})()
        self.cat.patterns_for = lambda t: self.cat.PATTERNS
        self.cat.file_options = lambda t: [("baseline", None),
                                           ("16f", 939470939)]
        self.cat.rg_options = lambda t: [("baseline", None)]
        self.cat.coaccess_matrix = lambda t: {}
        self.cat.COLUMN_WEIGHT = {"hits": {}}
        whatif.bind_catalog(self.cat)
        self.regime = {"rtt_s": 0.025, "bw_bps": 2 ** 28, "K_busy": 6.0}
        self.vectored = {"min_seek_bytes": 131072, "max_merged_bytes": 2097152}

    def tearDown(self):
        vf.bind_probe(None)

    def test_page_and_encoding_both_offer_baseline(self):
        vf.bind_probe(_probe_doc(page={
            "resolved": True, "default_page_bytes": 1024 * 1024,
            "index_bytes_per_page": 70.0,
            "noop_page_bytes": [1024 * 1024], "points": []}))
        notes = []
        pages = plan_deterministic.page_options(self.cat, "hits", True, notes)
        codecs = plan_deterministic.compression_options(
            self.cat, "hits", vf.probe, {"compression", "encoding"}, notes)
        self.assertEqual(pages[0][0], "baseline")
        self.assertEqual(codecs[0][0], "baseline")
        # Only the measured no-op is removed; 4 MiB stays a live option.
        self.assertEqual([p[1] for p in pages[1:]],
                         [4 * 1024 * 1024, 512 * 1024, 256 * 1024])
        self.assertIn("joint-codec-encoding", [c[0] for c in codecs])

    def test_measured_noop_points_never_become_options(self):
        vf.bind_probe(_probe_doc(page={
            "resolved": True, "default_page_bytes": 1024 * 1024,
            "index_bytes_per_page": 70.0,
            "noop_page_bytes": [256 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024], "points": []}))
        pages = plan_deterministic.page_options(self.cat, "hits", True, [])
        self.assertEqual(pages, [("baseline", None)])

    def test_unresolved_page_probe_keeps_axis_at_baseline(self):
        vf.bind_probe(_probe_doc(page={"resolved": False, "points": []}))
        notes = []
        pages = plan_deterministic.page_options(self.cat, "hits", True, notes)
        self.assertEqual(pages, [("baseline", None)])
        self.assertTrue(any("no resolved page pass" in n for n in notes))

    def test_unpriced_column_degrades_instead_of_emptying_the_search(self):
        # A probe that measured only column "a" leaves "b" and "c" unpriced.
        doc = _probe_doc()
        for column in ("b", "c"):
            doc["tables"]["hits"]["columns"][column]["joint"] = [
                {"codec": "snappy", "encoding": "baseline", "bytes": 1000,
                 "ratio": 1.0}]
        vf.bind_probe(doc)
        actions = [
            {"canonical": la.COMPRESSION_COLUMN_PREFIX + c, "value": "zstd",
             "table": "hits"} for c in ("a", "b", "c")]
        unpriced = plan_deterministic._unpriced_for("hits", actions, "pyarrow")
        self.assertEqual(sorted(u["column"] for u in unpriced), ["b", "c"])

    def test_l0_is_checked_against_the_target_writer(self):
        cand = {"candidate_id": "delta", "actions": [
            {"canonical": la.ENCODING_COLUMN_PREFIX + "a",
             "value": "DELTA_BINARY_PACKED", "table": "hits"}]}
        self.cat.COLUMN_STATS = {"hits": {"columns": {
            "a": {"physical_type": "INT64"}}}}
        ok_pyarrow, _v, _g = whatif.l0_check(cand, writer="pyarrow")
        ok_spark, viol, _g = whatif.l0_check(cand, writer="parquet-mr")
        self.assertTrue(ok_pyarrow)
        self.assertFalse(ok_spark)
        self.assertTrue(any("parquet-mr" in v for v in viol))


if __name__ == "__main__":
    unittest.main()
