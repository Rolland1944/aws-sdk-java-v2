#!/usr/bin/env python3
"""The decode veto drops CPU-for-bytes trades and nothing else."""

from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import decode_veto_plan as v  # noqa: E402

ENC = "write.parquet.encoding.column."
CODEC = "write.parquet.compression-codec.column."


def probe(**by_column):
    return {"tables": {"hits": {"columns": by_column}}}


def rates(reader, **by_type):
    return {"rates": {reader: by_type}}


class Veto(unittest.TestCase):
    def _plan(self, actions):
        return {"plan_id": "p", "actions": actions}

    def test_expensive_encoding_is_dropped_cheap_one_survives(self):
        plan = self._plan([
            {"canonical": CODEC + "Title", "value": "zstd", "table": "hits"},
            {"canonical": ENC + "Title", "value": "DELTA_BYTE_ARRAY", "table": "hits"},
            {"canonical": CODEC + "ClientIP", "value": "zstd", "table": "hits"},
            {"canonical": ENC + "ClientIP", "value": "PLAIN", "table": "hits"},
        ])
        decode = rates("parquet-mr",
                       BYTE_ARRAY={"zstd|baseline": {"relative_cost": 0.97},
                                   "zstd|DELTA_BYTE_ARRAY": {"relative_cost": 2.39}},
                       INT32={"zstd|baseline": {"relative_cost": 0.95},
                              "zstd|PLAIN": {"relative_cost": 0.62}})
        layout = probe(Title={"physical_type": "BYTE_ARRAY"},
                       ClientIP={"physical_type": "INT32"})
        kept, dropped, unmeasured = v.veto(plan, decode, layout, "parquet-mr")
        self.assertEqual([d["column"] for d in dropped], ["Title"])
        self.assertEqual(unmeasured, [])
        # both codec actions and the cheap encoding survive
        self.assertEqual(len(kept), 3)
        self.assertIn({"canonical": ENC + "ClientIP", "value": "PLAIN",
                       "table": "hits"}, kept)

    def test_reader_choice_changes_the_verdict(self):
        """DELTA_BINARY_PACKED is neutral under PyArrow and costly under mr."""
        plan = self._plan([
            {"canonical": ENC + "WatchID", "value": "DELTA_BINARY_PACKED",
             "table": "hits"},
        ])
        layout = probe(WatchID={"physical_type": "INT64"})
        decode = {"rates": {
            "pyarrow": {"INT64": {"snappy|baseline": {"relative_cost": 1.0},
                                  "snappy|DELTA_BINARY_PACKED": {"relative_cost": 1.01}}},
            "parquet-mr": {"INT64": {"snappy|baseline": {"relative_cost": 1.0},
                                     "snappy|DELTA_BINARY_PACKED": {"relative_cost": 1.53}}},
        }}
        _, dropped_mr, _ = v.veto(plan, decode, layout, "parquet-mr")
        self.assertEqual([d["column"] for d in dropped_mr], ["WatchID"])
        _, dropped_pa, _ = v.veto(plan, decode, layout, "pyarrow")
        self.assertEqual([d["column"] for d in dropped_pa], ["WatchID"])
        # 1.01 > 1.0, so even the PyArrow margin is refused; a strictly
        # cheaper encoding is what survives
        decode["rates"]["pyarrow"]["INT64"][
            "snappy|DELTA_BINARY_PACKED"]["relative_cost"] = 0.99
        _, dropped_pa2, _ = v.veto(plan, decode, layout, "pyarrow")
        self.assertEqual(dropped_pa2, [])

    def test_unmeasured_encoding_is_kept_not_vetoed(self):
        plan = self._plan([
            {"canonical": ENC + "Odd", "value": "BYTE_STREAM_SPLIT",
             "table": "hits"},
        ])
        decode = rates("parquet-mr",
                       FLOAT={"snappy|baseline": {"relative_cost": 1.0}})
        layout = probe(Odd={"physical_type": "FLOAT"})
        kept, dropped, unmeasured = v.veto(plan, decode, layout, "parquet-mr")
        self.assertEqual(dropped, [])
        self.assertEqual(len(unmeasured), 1)
        self.assertEqual(len(kept), 1)

    def test_column_codec_sets_the_reference(self):
        """A per-column codec, not the global one, picks the probe row."""
        actions = [
            {"canonical": "write.parquet.compression-codec", "value": "snappy",
             "table": "hits"},
            {"canonical": CODEC + "Title", "value": "zstd", "table": "hits"},
        ]
        self.assertEqual(v.column_codec(actions, "hits", "Title"), "zstd")
        self.assertEqual(v.column_codec(actions, "hits", "Other"), "snappy")

    def test_missing_reader_is_an_error_not_a_silent_pass(self):
        plan = self._plan([])
        with self.assertRaises(SystemExit):
            v.veto(plan, rates("pyarrow", INT32={}), probe(), "parquet-mr")


if __name__ == "__main__":
    unittest.main()
