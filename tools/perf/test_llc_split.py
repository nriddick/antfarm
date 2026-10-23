#!/usr/bin/env python3
"""Unit tests for llc_split.py: group shares, commands and combined rates.

Run: python3 -B -m unittest tools/perf/test_llc_split.py
Topology and output are synthetic; nothing is built or timed.
"""
import importlib.util
import pathlib
import unittest

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("llc_split", HERE / "llc_split.py")
llc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(llc)

# The 5900XT in placement order: each CCD's cores, then their SMT siblings.
GROUPS = [list(range(0, 8)) + list(range(16, 24)), list(range(8, 16)) + list(range(24, 32))]


class SplitTest(unittest.TestCase):
    def test_one_group_holds_up_to_its_size(self):
        self.assertEqual(llc.split(16, GROUPS), [(16, GROUPS[0])])

    def test_larger_counts_split_evenly_over_groups(self):
        self.assertEqual([s for s, _ in llc.split(20, GROUPS)], [10, 10])
        self.assertEqual([s for s, _ in llc.split(21, GROUPS)], [11, 10])
        self.assertEqual([s for s, _ in llc.split(40, GROUPS)], [20, 20])


class PartsTest(unittest.TestCase):
    def test_headline_layout_is_the_unconfined_headline_command(self):
        (payload,) = llc.parts_for("payload", "headline", 24, GROUPS, 100)
        self.assertNotEqual(payload[0], "taskset")
        (private,) = llc.parts_for("private", "headline", 20, GROUPS, 100)
        self.assertEqual(private[4], "19")
        self.assertEqual(private[8], llc.cpu_list(GROUPS[0] + GROUPS[1][:4]))

    def test_per_llc_parts_stay_in_their_group(self):
        parts = llc.parts_for("private", "per-llc", 20, GROUPS, 100)
        self.assertEqual(len(parts), 2)
        for part, group in zip(parts, GROUPS):
            self.assertEqual(part[:3], ["taskset", "-c", llc.cpu_list(group[:10])])
            self.assertEqual(part[3 + 4], "9")
            self.assertEqual(part[3 + 8], llc.cpu_list(group[:10]))

    def test_oversubscribed_share_is_confined_and_unpinned(self):
        for part, group in zip(llc.parts_for("private", "per-llc", 40, GROUPS, 100), GROUPS):
            self.assertEqual(part[2], llc.cpu_list(group))
            self.assertEqual(part[3 + 8], "-")

    def test_payload_shares_use_the_headline_split(self):
        (part,) = llc.parts_for("payload", "per-llc", 16, GROUPS, 100)
        self.assertEqual(part[part.index("--ns") + 1], "5")
        self.assertEqual(part[part.index("--nc") + 1], "11")


class CombineTest(unittest.TestCase):
    def test_payload_total_over_longest_time(self):
        parsed = [{"n": 2_000_000_000, "sec": 1.0}, {"n": 2_000_000_000, "sec": 2.0}]
        self.assertEqual(llc.combine("payload", parsed), 2000.0)

    def test_private_rates_add(self):
        self.assertEqual(llc.combine("private", [{"metric": 1.5}, {"metric": 2.5}]), 4.0)


if __name__ == "__main__":
    unittest.main()
