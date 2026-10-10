#!/usr/bin/env python3
"""Unit tests for private_lifecycle_matrix.py grouping.

Run: python3 -m unittest perftest/test_private_lifecycle_matrix.py
The script is import-safe, so no benchmark or subprocess work happens.
"""
import importlib.util
import pathlib
import unittest

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('private_lifecycle_matrix',
                                              HERE / 'private_lifecycle_matrix.py')
plm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plm)

NARROW = '0,1,2,3,4,5'
WIDE = ','.join(map(str, range(12)))


class CaseKeyTests(unittest.TestCase):
    def test_cpu_list_is_part_of_the_key(self):
        base = plm.case_key('mimalloc', 11, NARROW, 'private', 256, 'small', 16384)
        other = plm.case_key('mimalloc', 11, WIDE, 'private', 256, 'small', 16384)
        self.assertNotEqual(base, other)


class SummarizeTests(unittest.TestCase):
    def _row(self, **overrides):
        row = dict(allocator='mimalloc', consumers=11, cpus=NARROW, mode='private',
                   batch=256, tier='small', quota=16384, rate=1.0)
        row.update(overrides)
        return row

    def test_rows_pinned_to_different_cpu_sets_do_not_merge(self):
        rows = [self._row(cpus=NARROW, rate=10.0),
                self._row(cpus=NARROW, rate=20.0),
                self._row(cpus=WIDE, rate=100.0),
                self._row(cpus=WIDE, rate=200.0)]
        cases = [('mimalloc', 11, NARROW, 'private', 256, 'small', 16384),
                 ('mimalloc', 11, WIDE, 'private', 256, 'small', 16384)]
        summary = plm.summarize(rows, cases)
        self.assertEqual(len(summary), 2)
        self.assertEqual([s['cpus'] for s in summary], [NARROW, WIDE])
        self.assertEqual([s['median'] for s in summary], [15.0, 150.0])


if __name__ == '__main__':
    unittest.main()
