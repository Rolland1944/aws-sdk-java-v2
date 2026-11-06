#!/usr/bin/env python3
"""Regression tests for UC1 writer action isolation."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import layout_actions as la  # noqa: E402
import write_layout_pyarrow as writer  # noqa: E402


class PerColumnEncodingIsolation(unittest.TestCase):
    def test_explicit_encoding_does_not_disable_other_dictionaries(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({
            "plain": ["value-%04d" % (i % 97) for i in range(20_000)],
            "untouched": ["hot-value" for _ in range(20_000)],
            "codec_only": ["codec-%03d" % (i % 11) for i in range(20_000)],
        })
        rendered = la.Rendered()
        rendered.column_encoding = {"plain": "PLAIN"}
        rendered.column_compression = {"codec_only": "zstd"}
        notes = []
        kwargs = writer.writer_kwargs(
            rendered, list(table.schema.names), notes, "test")

        self.assertEqual(kwargs["column_encoding"], {"plain": "PLAIN"})
        self.assertIn("untouched", kwargs["use_dictionary"])
        self.assertIn("codec_only", kwargs["use_dictionary"])
        self.assertNotIn("plain", kwargs["use_dictionary"])
        self.assertEqual(kwargs["compression"]["codec_only"], "zstd")

        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "source")
            good = os.path.join(tmp, "good")
            bad = os.path.join(tmp, "bad")
            os.mkdir(source)
            os.mkdir(good)
            os.mkdir(bad)
            pq.write_table(table, os.path.join(source, "source.parquet"),
                           compression="snappy", write_page_index=True)
            path = os.path.join(good, "layout.parquet")
            pq.write_table(table, path, **kwargs)
            md = pq.ParquetFile(path).metadata
            columns = {
                md.schema.column(index).name: md.row_group(0).column(index)
                for index in range(md.num_columns)
            }
            fs = pa.fs.LocalFileSystem()
            expect = {
                "encodings": rendered.column_encoding,
                "column_codecs": rendered.column_compression,
            }
            verified = writer.verify_footers(fs, good, 20, expect, fs, source)
            self.assertEqual(verified["status"], "pass", verified.get("failures"))

            # This is the pre-fix behaviour: the presence of an explicit
            # column_encoding disables dictionary for every other column.
            old_kwargs = dict(kwargs)
            old_kwargs["use_dictionary"] = []
            pq.write_table(table, os.path.join(bad, "layout.parquet"),
                           **old_kwargs)
            old = writer.verify_footers(fs, bad, 20, expect, fs, source)
            self.assertEqual(old["status"], "fail")
            self.assertTrue(any("untouched: dictionary" in failure
                                for failure in old["failures"]))

        self.assertIn("PLAIN", {str(e) for e in columns["plain"].encodings})
        self.assertNotIn(
            "RLE_DICTIONARY", {str(e) for e in columns["plain"].encodings})
        self.assertTrue(
            {"RLE_DICTIONARY", "PLAIN_DICTIONARY"} &
            {str(e) for e in columns["untouched"].encodings})
        self.assertEqual(str(columns["codec_only"].compression).upper(), "ZSTD")

    def test_conflicting_dictionary_and_encoding_is_rejected(self):
        rendered = la.Rendered()
        rendered.column_encoding = {"a": "PLAIN"}
        rendered.column_dictionary = {"a": True}
        with self.assertRaisesRegex(ValueError, "conflicts"):
            writer.writer_kwargs(rendered, ["a", "b"], [], "test")

    def test_untouched_source_non_dictionary_column_stays_non_dictionary(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({
            "explicit_plain": ["value-%04d" % (i % 97) for i in range(20_000)],
            "source_dictionary": ["hot-value" for _ in range(20_000)],
            "source_plain": ["other-%04d" % (i % 97) for i in range(20_000)],
        })
        rendered = la.Rendered()
        rendered.column_encoding = {"explicit_plain": "PLAIN"}

        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "source")
            target = os.path.join(tmp, "target")
            os.mkdir(source)
            os.mkdir(target)
            source_file = os.path.join(source, "source.parquet")
            # Deliberately mix source policies: this matches NYC columns that
            # carry no dictionary in their source footers.
            pq.write_table(
                table, source_file, compression="snappy",
                use_dictionary=["explicit_plain", "source_dictionary"],
                write_page_index=True)
            fs = pa.fs.LocalFileSystem()
            source_policy = writer.source_dictionary_policy(fs, [source_file])
            self.assertTrue(source_policy["source_dictionary"])
            self.assertFalse(source_policy["source_plain"])

            notes = []
            kwargs = writer.writer_kwargs(
                rendered, list(table.schema.names), notes, "test",
                baseline_dictionary=source_policy)
            self.assertIn("source_dictionary", kwargs["use_dictionary"])
            self.assertNotIn("source_plain", kwargs["use_dictionary"])
            self.assertNotIn("explicit_plain", kwargs["use_dictionary"])

            pq.write_table(table, os.path.join(target, "layout.parquet"), **kwargs)
            verified = writer.verify_footers(
                fs, target, 20,
                {"encodings": rendered.column_encoding},
                fs, source)
            self.assertEqual(verified["status"], "pass", verified.get("failures"))
            check = verified["column_checks"]["source_plain"]
            self.assertFalse(check["expected_dictionary"])
            self.assertEqual(check["observed"]["non_dictionary_chunks"],
                             check["observed"]["chunks"])

    def test_baseline_file_quota_is_reallocated_when_row_groups_change(self):
        # file=baseline remains a topology constraint even when the new RG
        # cut differs; it must not fall back to buffered byte rotation.
        with mock.patch.object(
                writer, "source_row_group_geometry",
                return_value=(100, 4, 450)):
            self.assertEqual(
                writer.output_rg_quota(None, ["a", "b", "c", "d"], 200),
                [1, 1, 1])
            self.assertEqual(
                writer.output_rg_quota(None, ["a", "b", "c", "d"], 50),
                [3, 2, 2, 2])


if __name__ == "__main__":
    unittest.main()
