#!/usr/bin/env python3
"""Unit tests for run_performance_md.py.

Run from the repository root:

    python3 -B -m unittest tools/perf/test_run_performance_md.py -v

Parser inputs are synthetic in-memory strings and no benchmark binary is run.
Driver tests inject a fake runner or patch the module; temp files stay under a
TemporaryDirectory.
"""
import argparse
import contextlib
import json
import importlib.util
import io
import os
import pathlib
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


def _pid_alive(pid):
    """True when the pid still runs; a zombie is treated as gone."""
    try:
        stat = pathlib.Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return False
    return stat.rsplit(")", 1)[1].split()[0] not in ("Z", "X")

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("run_performance_md",
                                              HERE / "run_performance_md.py")
rpm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rpm)


# Real rows use a banner, a header and a fixed-width numeric row. The banner
# and header must be ignored; the note column is optional.
THROUGHPUT_OUT = """\
throughput --once --nc 8 --nb 0
 K  nc  nb  ns  body   bat          N      sec     Mpps    MiB/s   stalls  note
 8   11   1   1     2  1700  500000000    1.123  445.066   6791.2    36464
"""

# `dual` differs only in the third column, which is the tier name.
DUAL_BULK_OUT = """\
 K  nd tier  body   bat  cons          N      sec     Mpps    MiB/s   stalls  note
 6   2 bulk    16   256     1   10000000    0.456   21.900    320.0      100
"""

DUAL_SMALL_OUT = """\
 K  nd tier  body   bat  cons          N      sec     Mpps    MiB/s   stalls  note
 4   1 small   16   256     1    1000000    0.100   10.000    160.0       12
"""

CHURN_CYCLES_OUT = """\
mode=actor actors=16384 consumers=6 batch=256
peak_rss_kib=17924
root_hint=auto root_cost_hint=6 root_hint_table=16 root_hint_claims=8
rounds=12345 elapsed_s=3.000000 Mactor_cycles/s=16.553397 ns/actor_cycle=60.41
alloc_calls=100 free_calls=100 cross_thread_frees=27852800 cross_thread_free_bytes=445644800
"""

CHURN_SUBSET_OUT = """\
mode=subset actors=16384 consumers=6
rounds=1 elapsed_s=3.000000 waves/s=1.0 Mselected_callbacks/s=345.000000 ns/wave=123.4
dispatched_fraction=0.9994
"""

TAIL_OUT = """\
scene      nc  tlen   spin      n      p50      p99    p99.9      max   miss   zero  note
mid-drain   6   256    0ns   2000    912ns    2.7us  142.0us   3.00ms      1      0
mid-drain   6   256    1us   2000   14.9us   34.4us   48.9us   50.2us      0     39
mailbox     6  8192    1us   2000     91ns    150ns    180ns    2.8us      0      0
idle        6  2048      -   2000    340ns    5.2us    7.4us   11.2us      0      0
"""

FIBER_MT_OUT = """\
workers: 12 across 1 LLC(s) hugePages=false coverage=false
create: 100000 fibers in 0.012345 s: 8100300.49 fibers/s
run:    100000 fibers in 0.008540 s: 11710150.36 fibers/s
create (warm pool): 100000 fibers in 0.001000 s: 100000000.00 fibers/s
run    (warm pool): 100000 fibers in 0.000900 s: 111111111.11 fibers/s
"""

# The single-thread benchmark prints `run   :` (spaces before the colon)
# instead of the multi-thread `run:`.
FIBER_ST_OUT = """\
create: 200000 fibers in 0.045000 s: 4444444.44 fibers/s
run   : 200000 fibers in 0.050000 s: 4000000.00 fibers/s
run    (warm pool): 200000 fibers in 0.038000 s: 5263157.89 fibers/s
"""


class ThroughputTests(unittest.TestCase):
    def test_parse_throughput_row(self):
        d = rpm.parse_throughput(THROUGHPUT_OUT)
        self.assertEqual(
            d,
            {"k": 8, "nc": 11, "nb_or_nd": 1, "ns": 1, "body": 2, "batch": 1700,
             "n": 500000000, "sec": 1.123, "Mpps": 445.066, "MiB_s": 6791.2,
             "stalls": 36464, "metric": 445.066},
        )

    def test_parse_throughput_header_only_raises(self):
        # The banner and header match nothing and must not be mistaken for a row.
        header = THROUGHPUT_OUT.splitlines()[0] + "\n" + THROUGHPUT_OUT.splitlines()[1] + "\n"
        with self.assertRaises(ValueError):
            rpm.parse_throughput(header)

    def test_parse_throughput_without_a_row_raises(self):
        with self.assertRaises(ValueError):
            rpm.parse_throughput("nothing to see here\n")


class DualTests(unittest.TestCase):
    def test_parse_dual_bulk_row(self):
        d = rpm.parse_dual(DUAL_BULK_OUT)
        self.assertEqual(
            d,
            {"k": 6, "nd": 2, "tier": "bulk", "body": 16, "batch": 256,
             "consume": 1, "n": 10000000, "sec": 0.456, "Mpps": 21.9,
             "MiB_s": 320.0, "stalls": 100, "metric": 21.9},
        )

    def test_parse_dual_small_tier(self):
        d = rpm.parse_dual(DUAL_SMALL_OUT)
        self.assertEqual(d["tier"], "small")
        self.assertEqual(d["k"], 4)
        self.assertEqual(d["nd"], 1)
        self.assertEqual(d["metric"], 10.0)

    def test_parse_dual_without_a_row_raises(self):
        with self.assertRaises(ValueError):
            rpm.parse_dual("no dual row\n")


class ChurnTests(unittest.TestCase):
    def test_cycles_mode_metric_and_details(self):
        d = rpm.parse_churn(CHURN_CYCLES_OUT)
        self.assertEqual(d["metric"], 16.553397)
        self.assertEqual(d["Mactor_cycles_s"], 16.553397)
        self.assertEqual(d["ns_per_cycle"], 60.41)
        self.assertEqual(d["cross_thread_frees"], 27852800)
        self.assertEqual(d["cross_thread_free_bytes"], 445644800)
        self.assertEqual(d["root_cost_hint"], 6)
        self.assertEqual(d["root_hint_table"], 16)
        self.assertEqual(d["root_hint_claims"], 8)
        self.assertEqual(d["peak_rss_kib"], 17924)

    def test_subset_mode_metric_and_details(self):
        d = rpm.parse_churn(CHURN_SUBSET_OUT)
        self.assertEqual(d["metric"], 345.0)
        self.assertEqual(d["Mselected_callbacks_s"], 345.0)
        self.assertEqual(d["ns_per_wave"], 123.4)
        self.assertEqual(d["dispatched_fraction"], 0.9994)
        self.assertNotIn("Mactor_cycles_s", d)

    def test_cycles_metric_wins_over_subset_metric(self):
        out = CHURN_SUBSET_OUT + "rounds=1 elapsed_s=1.0 Mactor_cycles/s=2.5 ns/actor_cycle=1.0\n"
        self.assertEqual(rpm.parse_churn(out)["metric"], 2.5)

    def test_missing_metric_raises(self):
        with self.assertRaises(ValueError):
            rpm.parse_churn("peak_rss_kib=1000\n")


class ReadyScalingTests(unittest.TestCase):
    def test_parse_ready(self):
        out = "actors=16384 rounds=200 batch=32 ns/actor=27.14 Mactor/s=60.91 flushes=12345\n"
        d = rpm.parse_ready(out)
        self.assertEqual(d["metric"], 60.91)
        self.assertEqual(d["Mactor_s"], 60.91)

    def test_parse_ready_without_metric_raises(self):
        with self.assertRaises(ValueError):
            rpm.parse_ready("actors=16384 rounds=200\n")

    def test_parse_scaling(self):
        d = rpm.parse_scaling("actor create n=16384 ms=1.301\n")
        self.assertEqual(d["metric"], 1.301)
        self.assertEqual(d["ms"], 1.301)

    def test_parse_scaling_without_metric_raises(self):
        with self.assertRaises(ValueError):
            rpm.parse_scaling("actor create n=16384\n")


class TailTests(unittest.TestCase):
    def test_numeric_latency_conversions(self):
        row = rpm.parse_tail(TAIL_OUT)["rows"][0]
        self.assertEqual(row["scene"], "mid-drain")
        self.assertEqual(row["tlen"], 256)
        self.assertEqual(row["spin"], "0ns")
        self.assertEqual(row["p50_ns"], 912.0)
        self.assertEqual(row["p99_ns"], 2700.0)
        self.assertEqual(row["p999_ns"], 142000.0)
        self.assertEqual(row["max_ns"], 3000000.0)
        self.assertEqual(row["miss"], 1)
        self.assertEqual(row["zero"], 0)

    def test_all_scene_rows_are_returned(self):
        rows = rpm.parse_tail(TAIL_OUT)["rows"]
        # The mailbox row is an out-of-band signalling path the benchmark no
        # longer has; an old log's row is ignored rather than parsed.
        self.assertEqual([r["scene"] for r in rows],
                         ["mid-drain", "mid-drain", "idle"])
        self.assertEqual(rows[2]["spin"], "-")
        self.assertEqual(rows[2]["p99_ns"], 5200.0)

    def test_metric_prefers_mid_drain_256_1us(self):
        d = rpm.parse_tail(TAIL_OUT)
        self.assertEqual(d["metric"], 34400.0)

    def test_metric_falls_back_to_the_first_row(self):
        only_idle = "\n".join(
            line for line in TAIL_OUT.splitlines() if line.startswith("idle") or line.startswith("scene")
        ) + "\n"
        d = rpm.parse_tail(only_idle)
        self.assertEqual(len(d["rows"]), 1)
        self.assertEqual(d["metric"], d["rows"][0]["p99_ns"])
        self.assertEqual(d["metric"], 5200.0)

    def test_no_rows_raises(self):
        with self.assertRaises(ValueError):
            rpm.parse_tail("scene nc tlen spin n p50 p99 p99.9 max miss zero\n")


class HeadlineTests(unittest.TestCase):
    ORDER = [0, 1, 2, 3, 16, 17, 18, 19]

    def args(self, *extra):
        args = rpm.build_parser().parse_args(list(extra))
        args.cpu_order = self.ORDER
        return args

    def test_payload_split_is_about_one_producer_per_two_consumers(self):
        self.assertEqual(rpm.payload_split(2), (1, 1))
        self.assertEqual(rpm.payload_split(4), (1, 3))
        self.assertEqual(rpm.payload_split(16), (5, 11))
        self.assertEqual(rpm.payload_split(40), (13, 27))

    def test_pins_follow_placement_order_and_unpin_when_oversubscribed(self):
        self.assertEqual(rpm.pins_for(2, self.ORDER), "0,1")
        self.assertEqual(rpm.pins_for(8, self.ORDER), "0,1,2,3,16,17,18,19")
        self.assertEqual(rpm.pins_for(9, self.ORDER), "-")

    def test_cpu_order_falls_back_to_numeric_order_without_sysfs(self):
        with tempfile.TemporaryDirectory() as tmp:
            order = rpm.cpu_order(sys_root=Path(tmp))
        self.assertEqual(order, list(range(os.cpu_count() or 1)))

    def test_cpu_order_groups_by_cache_then_cores_then_siblings(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "devices/system/cpu"
            # Two LLCs of two cores each, SMT siblings offset by 4, as a
            # 4-core/8-thread two-CCD part exposes them.
            layout = {0: ("0", "0,2,4,6"), 1: ("1", "1,3,5,7"), 2: ("0", "0,2,4,6"),
                      3: ("1", "1,3,5,7"), 4: ("0", "0,2,4,6"), 5: ("1", "1,3,5,7"),
                      6: ("0", "0,2,4,6"), 7: ("1", "1,3,5,7")}
            core_of = {0: "0", 4: "0", 2: "1", 6: "1", 1: "2", 5: "2", 3: "3", 7: "3"}
            for cpu, (llc, shared) in layout.items():
                (base / f"cpu{cpu}/topology").mkdir(parents=True)
                (base / f"cpu{cpu}/topology/core_id").write_text(core_of[cpu] + "\n")
                (base / f"cpu{cpu}/cache/index3").mkdir(parents=True)
                (base / f"cpu{cpu}/cache/index3/shared_cpu_list").write_text(shared + "\n")
            with mock.patch.object(rpm.os, "cpu_count", return_value=8):
                order = rpm.cpu_order(sys_root=Path(tmp))
        self.assertEqual(order, [0, 2, 4, 6, 1, 3, 5, 7])

    def test_headline_cells_cover_every_workload_and_skip_oversubscribed_fibers(self):
        args = self.args("--threads", "2,8,9")
        cells = rpm.cases_for("headline", args)
        names = [c["name"] for c in cells]
        self.assertEqual(names, ["payload_t2", "payload_t8", "payload_t9",
                                 "private_t2", "private_t8", "private_t9",
                                 "tree_t2", "tree_t8", "tree_t9",
                                 "public_t2", "public_t8", "public_t9",
                                 "wave_t2", "wave_t8", "wave_t9",
                                 "fiber_t2", "fiber_t8"])
        private9 = next(c for c in cells if c["name"] == "private_t9")
        self.assertEqual(private9["cmd"][4], "8")   # consumers = threads - 1
        self.assertEqual(private9["cmd"][8], "-")   # unpinned beyond the CPU count
        fiber8 = next(c for c in cells if c["name"] == "fiber_t8")
        self.assertEqual(fiber8["cmd"][-3:], [str(args.fiber_tasks), "private", "8"])

    def test_tail_cells_use_one_consumer_fewer_than_threads(self):
        cells = rpm.cases_for("tail", self.args("--tail-threads", "2,6"))
        self.assertEqual([c["cmd"][c["cmd"].index("--nc") + 1] for c in cells], ["1", "5"])

    def test_thread_counts_below_two_are_rejected(self):
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            rpm.main(["--threads", "1", "--sections", "host", "--out", tempfile.mkdtemp()])

    def test_markdown_tables_use_median_and_stddev(self):
        args = self.args("--threads", "2", "--tail-threads", "2")
        results = [
            {"section": "headline", "name": "payload_t2", "median": 700.0, "stddev": 5.0},
            {"section": "headline", "name": "private_t2", "median": 60.0, "stddev": 1.0},
            {"section": "headline", "name": "public_t2", "median": 10.0, "stddev": 0.5},
            {"section": "headline", "name": "fiber_t2", "median": 2.5e6, "stddev": 1e5,
             "field_stats": {"warm_total_fibers_s": {"median": 2.5e6, "stddev": 1e5},
                             "warm_run_fibers_s": {"median": 5.0e6, "stddev": 2e5}}},
            {"section": "tail", "name": "tail_t2", "tail_rows": [
                {"scene": "idle", "tlen": 0, "spin": "-", "runs": 5, "valid_runs": 5,
                 "p50_ns": {"median": 200.0, "stddev": 10.0}, "p99_ns": {"median": 2000.0, "stddev": 100.0},
                 "p999_ns": {"median": 4000.0, "stddev": 300.0}, "max_ns": {"median": 9000.0, "stddev": 500.0}}]},
            {"section": "dual", "name": "nd2", "median": 70.123, "stddev": 0.5,
             "valid_runs": 5, "requested_runs": 5},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "tables.md"
            rpm.write_markdown(results, args, out)
            text = out.read_text()
        self.assertIn("| 2 | 700.0 ± 5.0 | 60.00 ± 1.00 | — | 10.00 ± 0.50 | — | 2.50 ± 0.10 |", text)
        # Workers can finish during creation: dividing the entire burst by
        # the residual drain interval is not a throughput measurement.
        self.assertNotIn("5.00 ± 0.20", text)
        self.assertNotIn("Warm Fiber drain", text)
        self.assertIn("pinned workers plus a separate controller", text)
        self.assertIn("| 2 | idle | 0 | - | 0.20 ± 0.01 | 2.00 ± 0.10 | 4.00 ± 0.30 | 9.00 ± 0.50 | 5 / 5 |", text)
        self.assertIn("| `nd2` | 70.123 ± 0.500 | 5 / 5 |", text)


class TailAggregationTests(unittest.TestCase):
    def test_timeout_scene_does_not_count_as_valid_quantiles(self):
        good = rpm.parse_tail(TAIL_OUT)
        bad = rpm.parse_tail(TAIL_OUT)
        bad["rows"][0].update(p50_ns=None, p99_ns=None, p999_ns=None, max_ns=None)
        rows = rpm.aggregate_tail([good, bad])
        scene = next(r for r in rows if r["scene"] == "mid-drain" and r["spin"] == "0ns")
        self.assertEqual(scene["runs"], 2)
        self.assertEqual(scene["valid_runs"], 1)
        self.assertEqual(scene["p99_ns"]["median"], 2700.0)


class FiberTests(unittest.TestCase):
    def test_single_thread_rate_only_totals_use_counted_phases(self):
        out = FIBER_ST_OUT + (
            "\ncreate (warm pool): 200000 fibers in 0.010000 s: 20000000.00 fibers/s\n"
            "total : 1.00 fibers/s\ntotal  (warm pool): 2.00 fibers/s\n")
        parsed = rpm.parse_fiber(out)
        self.assertEqual(parsed["metric_kind"], "drain")
        self.assertEqual(parsed["run_fibers_s"], 4000000.0)
        self.assertAlmostEqual(parsed["total_fibers_s"], 200000 / 0.095)
        self.assertAlmostEqual(parsed["warm_total_fibers_s"], 200000 / 0.048)

    def test_multithread_rate_only_total_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "timing/count"):
            rpm.parse_fiber(FIBER_MT_OUT + "\ntotal: 1000.00 fibers/s\n")

    def test_mt_explicit_totals_accept_unmeasurable_drain(self):
        for cold, warm in ((True, False), (False, True), (True, True)):
            with self.subTest(cold=cold, warm=warm):
                lines = FIBER_MT_OUT.splitlines()
                for i, line in enumerate(lines):
                    if (cold and line.startswith("run:")) or (warm and line.startswith("run ")):
                        lines[i] = line.split(":", 1)[0] + ": 100000 fibers: not measurable (finished before the timer started)"
                lines += ["total: 100000 fibers in 0.020885000 s: 4788125.00 fibers/s",
                          "total  (warm pool): 100000 fibers in 0.001900000 s: 52631578.95 fibers/s"]
                parsed = rpm.parse_fiber("\n".join(lines))
                self.assertAlmostEqual(parsed["metric"], 100000 / 0.020885)
                self.assertAlmostEqual(parsed["warm_total_fibers_s"], 100000 / 0.0019)
                self.assertEqual(parsed["metric_kind"], "spawn_and_drain")
                if cold:
                    self.assertNotIn("run_fibers_s", parsed)
                if warm:
                    self.assertNotIn("warm_run_fibers_s", parsed)

    def test_mt_explicit_total_takes_precedence_over_phase_rounding(self):
        out = FIBER_MT_OUT + "\ntotal: 100000 fibers in 0.020885123 s: 1 fibers/s\n"
        self.assertAlmostEqual(rpm.parse_fiber(out)["metric"], 100000 / 0.020885123)

    def test_mt_invalid_explicit_total_is_not_replaced_by_legacy_phases(self):
        for total in ("99999 fibers in 0.02 s: 1 fibers/s",
                      "100000 fibers in 0.000000000 s: 1 fibers/s",
                      "100000 fibers: not measurable (finished before the timer started)"):
            with self.subTest(total=total), self.assertRaisesRegex(ValueError, "timing/count"):
                rpm.parse_fiber(FIBER_MT_OUT + "\ntotal: " + total + "\n")

    def test_mt_unmeasurable_drain_without_total_is_rejected(self):
        out = FIBER_MT_OUT.replace("run:    100000 fibers in 0.008540 s: 11710150.36 fibers/s",
                                  "run: 100000 fibers: not measurable (finished before the timer started)")
        with self.assertRaisesRegex(ValueError, "complete multithread"):
            rpm.parse_fiber(out)

    def test_multithread_run_line(self):
        d = rpm.parse_fiber(FIBER_MT_OUT)
        self.assertAlmostEqual(d["metric"], 100000 / (0.012345 + 0.008540))
        self.assertEqual(d["metric_kind"], "spawn_and_drain")
        self.assertAlmostEqual(d["warm_total_fibers_s"], 100000 / (0.001000 + 0.000900))
        self.assertEqual(d["run_fibers_s"], 11710150.36)
        self.assertEqual(d["create_fibers_s"], 8100300.49)
        self.assertEqual(d["warm_run_fibers_s"], 111111111.11)

    def test_single_thread_run_line_spacing(self):
        d = rpm.parse_fiber(FIBER_ST_OUT)
        self.assertEqual(d["metric"], 4000000.0)
        self.assertEqual(d["run_fibers_s"], 4000000.0)
        self.assertEqual(d["create_fibers_s"], 4444444.44)
        self.assertEqual(d["warm_run_fibers_s"], 5263157.89)

    def test_mt_completed_during_spawn_does_not_report_infinite_throughput(self):
        out = FIBER_MT_OUT.replace("0.008540", "0.000000")
        self.assertAlmostEqual(rpm.parse_fiber(out)["metric"], 100000 / 0.012345)

    def test_mt_incomplete_timing_is_rejected(self):
        out = "\n".join(FIBER_MT_OUT.splitlines()[0:1] + FIBER_MT_OUT.splitlines()[2:])
        with self.assertRaisesRegex(ValueError, "complete multithread"):
            rpm.parse_fiber(out)

    def test_mt_mismatched_counts_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "timing/count"):
            rpm.parse_fiber(FIBER_MT_OUT.replace("run:    100000", "run:    99999"))

    def test_truncated_line_raises(self):
        # The `run` line is present but lost its rate column.
        with self.assertRaises(ValueError):
            rpm.parse_fiber("run:    100000 fibers in 0.008540 s\n")

    def test_no_run_line_raises(self):
        with self.assertRaises(ValueError):
            rpm.parse_fiber("create: 100000 fibers in 0.012345 s: 8100300.49 fibers/s\n")


class MatrixTests(unittest.TestCase):
    def test_default_matrix_is_unchanged(self):
        args = rpm.build_parser().parse_args([])
        self.assertEqual(len(rpm.cases_for("dual", args)), 9)
        self.assertEqual(len(rpm.cases_for("private", args)), 42)

    def test_custom_dual_threads(self):
        args = rpm.build_parser().parse_args(["--dual-threads", "12,18,20,20"])
        cells = rpm.cases_for("dual", args)
        self.assertEqual([c["name"] for c in cells], ["nd12", "nd18", "nd20"])
        for cell, count in zip(cells, (12, 18, 20)):
            self.assertEqual(cell["cmd"][cell["cmd"].index("--nd") + 1], str(count))

    def test_private_counts_exclude_controller(self):
        args = rpm.build_parser().parse_args(["--private-consumers", "11,17,19"])
        cells = rpm.cases_for("private", args)
        self.assertEqual(len(cells), 63)
        for cell in cells:
            consumers = int(cell["cmd"][4])
            self.assertIn(consumers, (11, 17, 19))
            self.assertEqual(cell["cmd"][8], ",".join(map(str, range(consumers + 1))))
        self.assertEqual(cells[-1]["name"], "subset-whole-unreserved_c19")

    def test_invalid_thread_counts(self):
        for value in ("", "-1", "1,x", "1,,2"):
            with self.subTest(value=value), self.assertRaises(SystemExit):
                rpm.build_parser().parse_args(["--private-consumers", value])
        with self.assertRaises(SystemExit):
            rpm.main(["--dual-threads", "0"])


class ParserMapTests(unittest.TestCase):
    def test_windows_executable_uses_the_same_parser(self):
        self.assertIs(rpm.parser_for(["actor_churn_mimalloc.exe"]), rpm.parse_churn)
        self.assertIs(rpm.parser_for(["tail.exe"]), rpm.parse_tail)

    def test_benchmark_paths_have_the_native_executable_suffix(self):
        expected = "throughput.exe" if os.name == "nt" else "throughput"
        self.assertEqual(pathlib.Path(rpm.benchmark_path("throughput")).name, expected)

    def test_each_binary_maps_to_its_parser(self):
        self.assertIs(rpm.PARSERS["throughput"], rpm.parse_throughput)
        self.assertIs(rpm.PARSERS["dual"], rpm.parse_dual)
        self.assertIs(rpm.PARSERS["actor_churn"], rpm.parse_churn)
        self.assertIs(rpm.PARSERS["actor_churn_mimalloc"], rpm.parse_churn)
        self.assertIs(rpm.PARSERS["actor_ready"], rpm.parse_ready)
        self.assertIs(rpm.PARSERS["scaling"], rpm.parse_scaling)
        self.assertIs(rpm.PARSERS["tail"], rpm.parse_tail)
        self.assertIs(rpm.PARSERS["dub_fiber"], rpm.parse_fiber)

    def test_no_unexpected_binaries(self):
        self.assertEqual(
            set(rpm.PARSERS),
            {"throughput", "dual", "actor_churn", "actor_churn_mimalloc",
             "actor_ready", "scaling", "tail", "dub_fiber"},
        )


SCALING_OUT = "actor create n=16384 ms=1.301\n"


class DriverTests(unittest.TestCase):
    def setUp(self):
        for name, replacement in (("prepare_manifest", lambda *a: {}),
                                  ("frozen_cell", lambda c, *a: c)):
            patch = mock.patch.object(rpm, name, replacement)
            patch.start()
            self.addCleanup(patch.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = pathlib.Path(self.tmp.name)
        self.raw = self.out / "raw"
        self.raw.mkdir()
        # Exercise real fixture creation/cleanup without reading the host's /proc.
        plan = rpm.fake_topology_plan
        patch = mock.patch.object(rpm, "fake_topology_plan", side_effect=lambda root:
                                  plan(root, cpuinfo=_cpuinfo([(0, 0, 0), (1, 0, 1)])))
        patch.start()
        self.addCleanup(patch.stop)

    def cell(self):
        return {"section": "scaling", "name": "cell",
                "cmd": [str(rpm.PERFTEST / "scaling"), "create", "16384"],
                "cwd": None, "env": {}, "note": ""}

    def args(self, runs=1):
        return argparse.Namespace(runs=runs, timeout=1.0)

    def test_nonzero_rc_with_valid_output_is_rejected(self):
        row = rpm.measure_cell(self.args(), self.cell(), self.raw, "scaling",
                               runner=lambda *a, **k: (SCALING_OUT, "", 1))
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["valid_runs"], 0)
        self.assertNotIn("median", row)

    def test_unparseable_run_is_failed(self):
        row = rpm.measure_cell(self.args(), self.cell(), self.raw, "scaling",
                               runner=lambda *a, **k: ("no metric here", "", 0))
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["valid_runs"], 0)

    def test_main_exits_nonzero_when_a_cell_fails(self):
        original_run, original_meta = rpm.run_cmd, rpm.host_metadata
        rpm.run_cmd = lambda *a, **k: ("no metric here", "", 0)
        rpm.host_metadata = lambda: {}
        self.addCleanup(lambda: (setattr(rpm, "run_cmd", original_run),
                                 setattr(rpm, "host_metadata", original_meta)))
        rc = rpm.main(["--out", str(self.out), "--sections", "scaling", "--runs", "1"])
        self.assertEqual(rc, 1)

    def test_cache_requires_matching_argv_and_env(self):
        cell = self.cell()
        cached = {"status": "ok", "values": [1.0], "cmd": cell["cmd"],
                  "cwd": None, "env": {}}
        self.assertTrue(rpm.cell_is_cached(cached, cell, 1))
        changed_cmd = {**cached, "cmd": cell["cmd"] + ["--x"]}
        changed_env = {**cached, "env": {"ANTFARM_HUGE_PAGES": "1"}}
        self.assertFalse(rpm.cell_is_cached(changed_cmd, cell, 1))
        self.assertFalse(rpm.cell_is_cached(changed_env, cell, 1))
        self.assertFalse(rpm.cell_is_cached({**cached, "status": "failed"}, cell, 1))
        self.assertFalse(rpm.cell_is_cached({**cached, "values": []}, cell, 1))

    def test_six_consumer_minimum_and_cache(self):
        args = rpm.build_parser().parse_args(["--runs", "5", "--six-consumer-runs", "30",
                                              "--tail-threads", "2,7"])
        for section in ("public", "private", "farm_consumers", "tail"):
            cells = rpm.cases_for(section, args)
            six = next(c for c in cells if rpm.requested_runs(args, c) == 30)
            cached = {**six, "status": "ok", "values": [1.0] * 5}
            self.assertFalse(rpm.cell_is_cached(cached, six, rpm.requested_runs(args, six)))
        self.assertEqual(rpm.requested_runs(args, rpm.churn("public", "serial", "actor", 0)), 5)
        self.assertEqual(rpm.requested_runs(args, rpm.dcase("dual", "six_threads", nd=6)), 5)
        args.runs = 40
        self.assertEqual(rpm.requested_runs(args, six), 40)

    def test_six_consumer_measurement_requires_all_samples(self):
        args = self.args()
        args.six_consumer_runs = 3
        cell = rpm.churn("public", "actor6", "actor", 6)
        runner = mock.Mock(side_effect=[(CHURN_CYCLES_OUT, "", 0),
                                       (CHURN_CYCLES_OUT, "", 1),
                                       (CHURN_CYCLES_OUT, "", 0)])
        row = rpm.measure_cell(args, cell, self.raw, "public", runner=runner)
        self.assertEqual(runner.call_count, 3)
        self.assertEqual(row["requested_runs"], 3)
        self.assertEqual(row["valid_runs"], 2)
        self.assertEqual(row["status"], "failed")

    def test_mt_cache_requires_full_burst_metric(self):
        cell = {**self.cell(), "section": "fibers", "name": "benchmark_mt"}
        cached = {"status": "ok", "values": [1.0], "cmd": cell["cmd"], "cwd": None, "env": {}}
        self.assertFalse(rpm.cell_is_cached(cached, cell, 1))
        cached["config"] = {"metric_kind": "spawn_and_drain"}
        self.assertTrue(rpm.cell_is_cached(cached, cell, 1))

    def test_bytes_output_is_decoded(self):
        class FakeProc:
            pid = 424242
            returncode = 0

            def communicate(self, timeout=None):
                return b"\xffok", b"err"

            def poll(self):
                return 0

        original = rpm.subprocess.Popen
        rpm.subprocess.Popen = lambda *a, **k: FakeProc()
        self.addCleanup(setattr, rpm.subprocess, "Popen", original)
        out, err, rc = rpm.run_cmd(["x"], ".", {}, 1)
        self.assertEqual(rc, 0)
        self.assertIn("\ufffd", out)
        self.assertEqual(err, "err")

    def test_windows_timeout_cleanup_targets_only_the_child_tree(self):
        proc = mock.Mock(pid=424242)
        proc.poll.return_value = None
        with mock.patch.object(rpm.os, "name", "nt"), \
                mock.patch.object(rpm.subprocess, "run") as run:
            run.return_value.returncode = 0
            rpm.kill_process_group(proc)
        run.assert_called_once_with(["taskkill", "/PID", "424242", "/T", "/F"],
                                    capture_output=True, text=True, timeout=15)
        proc.wait.assert_called_once_with(timeout=5)

    def test_failed_tree_termination_reaps_leader_and_reports_failure(self):
        proc = mock.Mock(pid=424242)
        proc.poll.return_value = None
        with mock.patch.object(rpm.os, "name", "nt"), \
                mock.patch.object(rpm.subprocess, "run", return_value=mock.Mock(
                    returncode=1, stderr="access denied")):
            with self.assertRaisesRegex(RuntimeError, "access denied"):
                rpm.kill_process_group(proc)
        proc.kill.assert_called_once_with()
        proc.wait.assert_called_once_with(timeout=5)

    @unittest.skipUnless(os.name == "nt", "requires Windows taskkill")
    def test_windows_timeout_reaps_real_child(self):
        out, err, rc = rpm.run_cmd(
            [sys.executable, "-c", "import time; print('started', flush=True); time.sleep(30)"],
            str(self.out), dict(os.environ), 2)
        self.assertEqual(rc, "timeout", err)
        self.assertIn("started", out)
        self.assertIsNone(rpm._CURRENT_PROC["proc"])

    def test_cleanup_does_not_target_an_exited_process(self):
        proc = mock.Mock()
        proc.poll.return_value = 0
        with mock.patch.object(rpm.os, "name", "nt"), \
                mock.patch.object(rpm.subprocess, "run") as run:
            rpm.kill_process_group(proc)
        run.assert_not_called()
        proc.wait.assert_not_called()

    @unittest.skipUnless(sys.platform == "linux", "requires Linux process groups and /proc")
    def test_timeout_kills_process_group(self):
        bash = shutil.which("bash")
        if bash is None:
            self.skipTest("bash not available")
        marker = self.out / "grandchild.pid"
        script = self.out / "spawn.sh"
        script.write_text(f"#!{bash}\nsleep 300 &\necho $! > \"$1\"\nwait\n")
        out, err, rc = rpm.run_cmd([bash, str(script), str(marker)], str(self.out),
                                   dict(os.environ), 1)
        self.assertEqual(rc, "timeout")
        pid = int(marker.read_text().strip())
        for _ in range(50):
            if not _pid_alive(pid):
                break
            time.sleep(0.1)
        self.assertFalse(_pid_alive(pid), f"grandchild {pid} survived the timeout")

    def test_main_refuses_fake_topology_before_writing(self):
        original = rpm.SYS_ROOT
        rpm.SYS_ROOT = self.out
        self.addCleanup(setattr, rpm, "SYS_ROOT", original)
        (self.out / "devices").mkdir()
        out = self.out / "report"
        with contextlib.redirect_stderr(io.StringIO()):
            rc = rpm.main(["--out", str(out), "--sections", "scaling", "--runs", "1",
                           "--fake-topology"])
        self.assertEqual(rc, 2)
        self.assertFalse(out.exists())

    def test_runs_zero_is_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as caught:
                rpm.main(["--out", str(self.out), "--runs", "0"])
        self.assertEqual(caught.exception.code, 2)

    def test_fewer_valid_runs_than_requested_is_failed(self):
        calls = {"n": 0}

        def runner(*a, **k):
            calls["n"] += 1
            return (SCALING_OUT if calls["n"] == 1 else "no metric", "", 0)

        row = rpm.measure_cell(self.args(runs=3), self.cell(), self.raw, "scaling",
                               runner=runner)
        self.assertEqual(row["valid_runs"], 1)
        self.assertEqual(row["status"], "failed")

    def test_fake_topology_created_and_removed(self):
        original = rpm.SYS_ROOT
        rpm.SYS_ROOT = self.out
        self.addCleanup(setattr, rpm, "SYS_ROOT", original)
        created = rpm.install_fake_topology()
        self.assertEqual(created, self.out / "devices")
        self.assertTrue((self.out / "devices/system/cpu/present").exists())
        rpm.remove_fake_topology(created)
        self.assertFalse((self.out / "devices").exists())

    def test_fake_topology_removed_when_body_raises(self):
        original_root, original_run, original_meta = rpm.SYS_ROOT, rpm.run_cmd, rpm.host_metadata
        original_term = signal.getsignal(signal.SIGTERM)
        rpm.SYS_ROOT = self.out

        def boom(*a, **k):
            raise RuntimeError("boom")

        rpm.run_cmd = boom
        rpm.host_metadata = lambda: {}
        self.addCleanup(lambda: (setattr(rpm, "SYS_ROOT", original_root),
                                 setattr(rpm, "run_cmd", original_run),
                                 setattr(rpm, "host_metadata", original_meta)))
        with self.assertRaises(RuntimeError):
            rpm.main(["--out", str(self.out / "report"), "--sections", "scaling",
                      "--runs", "1", "--fake-topology"])
        self.assertFalse((self.out / "devices").exists())
        self.assertEqual(signal.getsignal(signal.SIGTERM), original_term)

    def test_fake_topology_refuses_existing_devices(self):
        original = rpm.SYS_ROOT
        rpm.SYS_ROOT = self.out
        self.addCleanup(setattr, rpm, "SYS_ROOT", original)
        (self.out / "devices").mkdir()
        sentinel = self.out / "devices" / "sentinel"
        sentinel.write_text("keep")
        with self.assertRaises(rpm.TopologyError):
            rpm.install_fake_topology()
        self.assertTrue(sentinel.exists())

    def test_fake_topology_refuses_sysfs_mount(self):
        original_root, original_mount = rpm.SYS_ROOT, rpm._is_sysfs_mount
        rpm.SYS_ROOT = self.out
        rpm._is_sysfs_mount = lambda *a, **k: True
        self.addCleanup(lambda: (setattr(rpm, "SYS_ROOT", original_root),
                                 setattr(rpm, "_is_sysfs_mount", original_mount)))
        with self.assertRaises(rpm.TopologyError):
            rpm.install_fake_topology()
        self.assertFalse((self.out / "devices").exists())

    def test_partial_fake_topology_setup_is_cleaned(self):
        original_root, original_write = rpm.SYS_ROOT, rpm._write_plan
        rpm.SYS_ROOT = self.out

        def partial(plan):
            (self.out / "devices").mkdir(parents=True)
            raise OSError("disk full")

        rpm._write_plan = partial
        self.addCleanup(lambda: (setattr(rpm, "SYS_ROOT", original_root),
                                 setattr(rpm, "_write_plan", original_write)))
        with self.assertRaises(OSError):
            rpm.install_fake_topology()
        self.assertFalse((self.out / "devices").exists())

    def test_sysfs_mount_detection(self):
        mounts = self.out / "mounts"
        mounts.write_text("sysfs /sys sysfs rw 0 0\n")
        self.assertTrue(rpm._is_sysfs_mount("/sys", mounts))
        self.assertFalse(rpm._is_sysfs_mount(self.out, mounts))


class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.binary = self.root / "scaling"
        self.binary.write_bytes(b"binary A")
        self.out = self.root / "report"
        self.out.mkdir()
        self.args = rpm.build_parser().parse_args(["--out", str(self.out), "--sections", "scaling"])
        self.cell = {"cmd": [str(self.binary)], "cwd": None, "env": {}, "name": "test"}
        for name, value in (("source_identity", {"test.d": "source A"}),
                            ("cases_for", [self.cell])):
            patch = mock.patch.object(rpm, name, return_value=value)
            patch.start()
            self.addCleanup(patch.stop)
        self.meta = {"git_rev": "A", "timestamp": "original"}

    def prepare(self):
        return rpm.prepare_manifest(self.args, ["scaling"], self.meta)

    def test_resume_keeps_original_metadata_and_allows_sample_topup(self):
        original = self.prepare()
        self.meta["timestamp"] = "later"
        self.args.runs += 1
        self.assertEqual(self.prepare(), original)
        self.assertEqual(json.loads((self.out / "metadata.json").read_text())["timestamp"], "original")

    def test_launcher_bookkeeping_does_not_invalidate_resume(self):
        original = self.prepare()
        with mock.patch.dict(os.environ, {"_": "different-launcher", "SHLVL": "99"}):
            self.assertEqual(self.prepare(), original)

    def test_changed_binary_refused_even_with_no_resume(self):
        self.prepare()
        metadata = (self.out / "metadata.json").read_bytes()
        self.binary.write_bytes(b"binary B")
        self.args.no_resume = True
        with self.assertRaisesRegex(ValueError, "executable changed"):
            self.prepare()
        self.assertEqual((self.out / "metadata.json").read_bytes(), metadata)

    def test_changed_revision_source_environment_and_config_refused(self):
        self.prepare()
        for change in (mock.patch.dict(self.meta, git_rev="B"),
                       mock.patch.object(rpm, "source_identity", return_value={"test.d": "B"}),
                       mock.patch.dict(os.environ, ANTFARM_TEST_MANIFEST="changed"),
                       mock.patch.object(self.args, "timeout", 1)):
            with change, self.assertRaisesRegex(ValueError, "identity changed"):
                self.prepare()

    def test_legacy_output_is_untouched(self):
        path = self.out / "metadata.json"
        path.write_text('"old metadata"')
        with self.assertRaisesRegex(ValueError, "legacy"):
            self.prepare()
        self.assertEqual(path.read_text(), '"old metadata"')
        self.assertFalse((self.out / "manifest.json").exists())

    def test_main_rejects_changed_binary_before_measurement_or_metadata_write(self):
        argv = ["--out", str(self.out), "--sections", "scaling", "--runs", "1"]
        with mock.patch.object(rpm, "host_metadata", return_value=self.meta), \
                mock.patch.object(rpm, "measure_cell", return_value={
                    **self.cell, "section": "scaling", "status": "ok", "values": [1.]}) as measure:
            self.assertEqual(rpm.main(argv), 0)
            before = (self.out / "metadata.json").read_bytes()
            self.binary.write_bytes(b"binary B")
            self.assertEqual(rpm.main(argv), 2)
            self.assertEqual(measure.call_count, 1)
            self.assertEqual((self.out / "metadata.json").read_bytes(), before)

    def test_binary_change_before_cell_is_refused(self):
        manifest = self.prepare()
        self.binary.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "during the run"):
            rpm.frozen_cell(self.cell, self.args, manifest)

    def test_fibers_build_once_and_resume_uses_hashed_snapshot(self):
        self.cell.update(cmd=["dub", "run", "-c", "benchmark_mt", "--build=release",
                              "--compiler=ldc2", "--", "200000", "private", "2"], cwd=str(self.root))
        (self.root / ("ant_fibers.exe" if os.name == "nt" else "ant_fibers")).write_bytes(b"fiber A")
        with mock.patch.object(rpm.subprocess, "run") as build:
            manifest = self.prepare()
            build.assert_called_once()
            self.prepare()
            build.assert_called_once()
        frozen = rpm.frozen_cell(self.cell, self.args, manifest)
        self.assertEqual(pathlib.Path(frozen["cmd"][0]).read_bytes(), b"fiber A")
        self.assertEqual(frozen["cmd"][1:], ["200000", "private", "2"])
        self.assertIs(rpm.parser_for(frozen["cmd"]), rpm.parse_fiber)


@unittest.skipUnless(sys.platform == "linux", "requires Linux process groups and /proc")
class ResistantDescendantTests(unittest.TestCase):
    def test_both_drivers_kill_descendant_after_leader_exits(self):
        spec = importlib.util.spec_from_file_location("offccd_cleanup", HERE / "offccd_sweep.py")
        offccd = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(offccd)
        import subprocess
        for driver in (rpm, offccd):
            for exit_first in (False, True):
                with self.subTest(driver=driver.__name__, exit_first=exit_first), tempfile.TemporaryDirectory() as tmp:
                    marker = pathlib.Path(tmp) / "ready"
                    child = ("import os,signal,time,pathlib; "
                             "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                             f"pathlib.Path({str(marker)!r}).write_text(str(os.getpid())); time.sleep(60)")
                    parent = ("import subprocess,sys,time; "
                              f"subprocess.Popen([sys.executable,'-c',{child!r}]); "
                              + ("" if exit_first else "time.sleep(60)"))
                    proc = subprocess.Popen([sys.executable, "-c", parent], start_new_session=True)
                    try:
                        deadline = time.monotonic() + 5
                        while not marker.exists() and time.monotonic() < deadline:
                            time.sleep(.01)
                        self.assertTrue(marker.exists())
                        pid = int(marker.read_text())
                        if exit_first:
                            proc.wait(timeout=5)
                        driver.kill_process_group(proc)
                        deadline = time.monotonic() + 5
                        while _pid_alive(pid) and time.monotonic() < deadline:
                            time.sleep(.01)
                        self.assertFalse(_pid_alive(pid), f"descendant {pid} survived")
                    finally:
                        try:
                            os.killpg(proc.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        proc.wait(timeout=5)


def _cpuinfo(rows):
    """Synthetic /proc/cpuinfo from (processor, package, core) rows."""
    blocks = []
    for cpu, package, core in rows:
        blocks.append("\n".join([
            f"processor\t: {cpu}",
            f"physical id\t: {package}",
            f"core id\t\t: {core}",
        ]))
    return "\n\n".join(blocks) + "\n"


def _l3_shares(plan):
    indexes = {}
    for path, text in plan:
        if path.parent.name.startswith("index") and path.name in ("level", "shared_cpu_list"):
            indexes.setdefault(path.parent, {})[path.name] = text.strip()
    return sorted({v["shared_cpu_list"] for v in indexes.values()
                   if v.get("level") == "3"})


def _sibling_lists(plan):
    return {path.parent.parent.name: text.strip()
            for path, text in plan if path.name == "thread_siblings_list"}


class FakeTopologyPlanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)

    def ryzen5500(self):
        # Linux numbers SMT as (0,6)..(5,11), one package.
        return [(core + (6 if half else 0), 0, core)
                for half in (False, True) for core in range(6)]

    def zen3_5900xt(self):
        # One package, 16 cores, sibling N+16 (the 5900XT's CCD numbering).
        return [(cpu, 0, cpu % 16) for cpu in range(32)]

    def l3_5900xt(self):
        # Two CCDs; /proc/cpuinfo carries no CCD field, so it is explicit.
        return [list(range(0, 8)) + list(range(16, 24)),
                list(range(8, 16)) + list(range(24, 32))]

    def two_packages_same_core(self):
        # Core 0 exists on both packages; siblings must not merge across them.
        return [(0, 0, 0), (1, 0, 0), (2, 1, 0), (3, 1, 0)]

    def test_single_package_has_one_l3_set(self):
        plan = rpm.fake_topology_plan(self.root, cpuinfo=_cpuinfo(self.ryzen5500()))
        self.assertEqual(_l3_shares(plan), ["0-11"])

    def test_two_l3_layout_is_non_contiguous(self):
        plan = rpm.fake_topology_plan(self.root, cpuinfo=_cpuinfo(self.zen3_5900xt()),
                                      l3_groups=self.l3_5900xt())
        self.assertEqual(_l3_shares(plan), ["0-7,16-23", "8-15,24-31"])

    def test_5900xt_siblings_are_n_plus_16(self):
        plan = rpm.fake_topology_plan(self.root, cpuinfo=_cpuinfo(self.zen3_5900xt()),
                                      l3_groups=self.l3_5900xt())
        sibs = _sibling_lists(plan)
        self.assertEqual(sibs["cpu0"], "0,16")
        self.assertEqual(sibs["cpu8"], "8,24")

    def test_siblings_group_by_package_and_core(self):
        plan = rpm.fake_topology_plan(self.root, cpuinfo=_cpuinfo(self.two_packages_same_core()))
        sibs = _sibling_lists(plan)
        self.assertEqual(sibs["cpu0"], "0-1")
        self.assertEqual(sibs["cpu2"], "2-3")

    def test_cpu_list_format_is_compact_and_non_contiguous(self):
        self.assertEqual(rpm._format_cpu_list([0, 1, 2, 8]), "0-2,8")
        self.assertEqual(rpm._format_cpu_list([3]), "3")


if __name__ == "__main__":
    unittest.main()
