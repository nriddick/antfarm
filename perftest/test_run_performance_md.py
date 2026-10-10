#!/usr/bin/env python3
"""Unit tests for run_performance_md.py.

Run from the repository root:

    python3 -B -m unittest perftest/test_run_performance_md.py -v

Parser inputs are synthetic in-memory strings and no benchmark binary is run.
Driver tests inject a fake runner or patch the module; temp files stay under a
TemporaryDirectory.
"""
import argparse
import contextlib
import importlib.util
import io
import os
import pathlib
import shutil
import signal
import subprocess
import tempfile
import time
import unittest


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
        self.assertEqual([r["scene"] for r in rows],
                         ["mid-drain", "mid-drain", "mailbox", "idle"])
        self.assertEqual(rows[3]["spin"], "-")
        self.assertEqual(rows[3]["p99_ns"], 5200.0)

    def test_metric_prefers_mid_drain_256_1us(self):
        d = rpm.parse_tail(TAIL_OUT)
        self.assertEqual(d["metric"], 34400.0)

    def test_metric_falls_back_to_the_first_row(self):
        only_mailbox = "\n".join(
            line for line in TAIL_OUT.splitlines() if "mailbox" in line or line.startswith("scene")
        ) + "\n"
        d = rpm.parse_tail(only_mailbox)
        self.assertEqual(len(d["rows"]), 1)
        self.assertEqual(d["metric"], d["rows"][0]["p99_ns"])
        self.assertEqual(d["metric"], 150.0)

    def test_no_rows_raises(self):
        with self.assertRaises(ValueError):
            rpm.parse_tail("scene nc tlen spin n p50 p99 p99.9 max miss zero\n")


class FiberTests(unittest.TestCase):
    def test_multithread_run_line(self):
        d = rpm.parse_fiber(FIBER_MT_OUT)
        self.assertEqual(d["metric"], 11710150.36)
        self.assertEqual(d["run_fibers_s"], 11710150.36)
        self.assertEqual(d["create_fibers_s"], 8100300.49)
        self.assertEqual(d["warm_run_fibers_s"], 111111111.11)

    def test_single_thread_run_line_spacing(self):
        d = rpm.parse_fiber(FIBER_ST_OUT)
        self.assertEqual(d["metric"], 4000000.0)
        self.assertEqual(d["run_fibers_s"], 4000000.0)
        self.assertEqual(d["create_fibers_s"], 4444444.44)
        self.assertEqual(d["warm_run_fibers_s"], 5263157.89)

    def test_truncated_line_raises(self):
        # The `run` line is present but lost its rate column.
        with self.assertRaises(ValueError):
            rpm.parse_fiber("run:    100000 fibers in 0.008540 s\n")

    def test_no_run_line_raises(self):
        with self.assertRaises(ValueError):
            rpm.parse_fiber("create: 100000 fibers in 0.012345 s: 8100300.49 fibers/s\n")


class ParserMapTests(unittest.TestCase):
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
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = pathlib.Path(self.tmp.name)
        self.raw = self.out / "raw"
        self.raw.mkdir()

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
