"""Check Windows lane selection, failure handling and process-tree cleanup."""
import contextlib
import ctypes
import importlib.util
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("check_windows", Path(__file__).with_name("check-windows.py"))
windows = importlib.util.module_from_spec(spec)
spec.loader.exec_module(windows)


class WindowsChecksTest(unittest.TestCase):
    def test_quick_uses_only_ldc_and_portable_lanes(self):
        calls = []
        test_build = Path(tempfile.gettempdir())
        with mock.patch.object(windows.os, "name", "nt"), \
                mock.patch.object(windows.shutil, "which", return_value="tool"), \
                mock.patch.object(windows, "BUILD", test_build), \
                mock.patch.object(windows.Checks, "packages", lambda self: calls.append((self.compiler, "packages"))), \
                mock.patch.object(windows.Checks, "ring", lambda self: calls.append((self.compiler, "ring"))), \
                mock.patch.object(windows.Checks, "review", lambda self: calls.append((self.compiler, "review"))), \
                mock.patch.object(windows.Checks, "examples", lambda self: calls.append((self.compiler, "examples"))), \
                contextlib.redirect_stdout(io.StringIO()):
            windows.main(["--quick"])
        self.assertEqual(calls, [("ldc2", lane) for lane in windows.QUICK_LANES])

    def test_missing_libuv_skips_default_matrix_but_fails_when_requested(self):
        test_build = Path(tempfile.gettempdir())
        absent = test_build / "antfarm-no-libuv"
        lanes = [lane for lane in windows.LANES if lane != "ant_uv"]
        patches = [mock.patch.object(windows.Checks, lane) for lane in lanes]
        with mock.patch.object(windows.os, "name", "nt"), \
                mock.patch.object(windows.shutil, "which", return_value="tool"), \
                mock.patch.object(windows, "BUILD", test_build), \
                mock.patch.object(windows, "DEFAULT_LIBUV", absent), \
                mock.patch.object(windows.Checks, "ant_uv") as uv_lane, \
                contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                windows.main(["--compiler", "ldc2"])
            uv_lane.assert_not_called()
            self.assertIn("SKIP ant_uv", output.getvalue())
            self.assertIn("WINDOWS CHECKS PASSED (ant_uv skipped)", output.getvalue())
            for argv in (["--lanes", "ant_uv"], ["--libuv", str(absent)]):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    with self.assertRaisesRegex(RuntimeError, "ant_uv needs libuv"):
                        windows.main(["--compiler", "ldc2", *argv])
                self.assertNotIn("WINDOWS CHECKS PASSED", output.getvalue())
            uv_lane.assert_not_called()

    def test_failure_stops_matrix_without_pass_message(self):
        output = io.StringIO()
        test_build = Path(tempfile.gettempdir())
        with mock.patch.object(windows.os, "name", "nt"), \
                mock.patch.object(windows.shutil, "which", return_value="tool"), \
                mock.patch.object(windows, "BUILD", test_build), \
                mock.patch.object(windows.Checks, "packages", side_effect=RuntimeError("failed")), \
                mock.patch.object(windows.Checks, "threadpool") as next_lane, \
                contextlib.redirect_stdout(output):
            with self.assertRaisesRegex(RuntimeError, "failed"):
                windows.main(["--lanes", "packages", "threadpool"])
        next_lane.assert_not_called()
        self.assertNotIn("WINDOWS CHECKS PASSED", output.getvalue())

    def test_native_exit_and_hardware_skip_are_failures(self):
        for output, code, error in (("bad", 7, subprocess.CalledProcessError),
                                    ("SKIP worker cases: too few CPUs", 0, RuntimeError)):
            proc = mock.Mock()
            proc.returncode = code

            def start(*args, **kwargs):
                kwargs["stdout"].write(output.encode())
                return proc

            with mock.patch.object(windows.subprocess, "Popen", side_effect=start), \
                    contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(error):
                    windows.run(["check.exe"], {}, 10)
                if code == 0:
                    windows.run(["check.exe"], {}, 10, allow_hardware_skips=True)

    def test_timeout_targets_only_spawned_process_tree(self):
        proc = mock.Mock(pid=424242)
        with mock.patch.object(windows.subprocess, "run", return_value=mock.Mock(returncode=0)) as kill:
            windows.stop_tree(proc)
        kill.assert_called_once_with(["taskkill", "/PID", "424242", "/T", "/F"],
                                     capture_output=True, text=True, timeout=15)
        proc.wait.assert_called_once_with(timeout=5)

    @unittest.skipUnless(os.name == "nt", "requires Windows taskkill")
    def test_timeout_kills_real_grandchild(self):
        # The child publishes its grandchild PID before it sleeps. Keep a process
        # handle open so Windows cannot recycle that PID before our assertion.
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.restype = ctypes.c_void_p
        kernel.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
        kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        # The checkout stays accessible to children in restricted Windows hosts;
        # a host-specific redirected temp directory may not.
        (windows.ROOT / ".build").mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=windows.ROOT / ".build") as directory:
            pidfile = Path(directory) / "pid"
            child = ("import pathlib, subprocess, sys, time; "
                     "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
                     "pathlib.Path(sys.argv[1]).write_text(str(p.pid)); time.sleep(60)")
            cmd = [sys.executable, "-c", child, str(pidfile)]
            original_stop = windows.stop_tree
            handles = []

            def capture_then_stop(proc):
                try:
                    handle = kernel.OpenProcess(0x00100000, False, int(pidfile.read_text()))
                    self.assertTrue(handle, ctypes.get_last_error())
                    handles.append(handle)
                finally:
                    original_stop(proc)

            try:
                with mock.patch.object(windows, "stop_tree", side_effect=capture_then_stop), \
                        contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaises(subprocess.TimeoutExpired):
                        windows.run(cmd, os.environ.copy(), 3)
                self.assertEqual(kernel.WaitForSingleObject(handles[0], 5000), 0,
                                 "grandchild survived the check timeout")
            finally:
                for handle in handles:
                    kernel.CloseHandle(handle)


if __name__ == "__main__":
    unittest.main()
