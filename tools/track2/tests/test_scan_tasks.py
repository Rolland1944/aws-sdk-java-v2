import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import virtual_footer as vf  # noqa: E402

MIB = 1024 * 1024


class ScanTaskCountTest(unittest.TestCase):
    def test_whole_splits_are_one_task_each(self):
        self.assertEqual(vf.scan_task_count([128 * MIB] * 4), 4)

    def test_small_tails_are_packed_together(self):
        # Four 136 MiB files: four full splits plus four 8 MiB tails that
        # share one task (4 x (8 + 4 open cost) MiB fits in 128 MiB).
        self.assertEqual(vf.scan_unit_count([136 * MIB] * 4), 8)
        self.assertEqual(vf.scan_task_count([136 * MIB] * 4), 5)

    def test_open_cost_limits_packing(self):
        # 40 x 1 MiB files cost 40 x 5 MiB = 200 MiB with open cost: two tasks.
        self.assertEqual(vf.scan_task_count([1 * MIB] * 40), 2)


if __name__ == "__main__":
    unittest.main()
