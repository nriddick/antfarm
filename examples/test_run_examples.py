"""Protect the platform/compiler matrix used by both check runners."""
import contextlib
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("run_examples", Path(__file__).with_name("run_examples.py"))
examples = importlib.util.module_from_spec(spec)
spec.loader.exec_module(examples)


class ExamplesMatrixTest(unittest.TestCase):
    def matrix(self, platform, **kwargs):
        test_build = Path(tempfile.gettempdir())
        with mock.patch.object(examples.os, "name", platform), \
                mock.patch.object(examples, "build", test_build), \
                mock.patch.object(examples, "check_readme_programs"), \
                mock.patch.object(examples, "compile_example") as compile_example, \
                mock.patch.object(examples, "run_example") as run_example, \
                mock.patch.object(examples, "compile_termbox"), \
                mock.patch.object(examples, "compile_life_tui") as tui, \
                contextlib.redirect_stdout(io.StringIO()):
            examples.main(**kwargs)
        return compile_example.call_args_list, run_example.call_args_list, tui.call_count

    def test_windows_runs_both_compilers_without_tsan_or_terminal(self):
        builds, runs, terminal_builds = self.matrix("nt")
        self.assertEqual(len(builds), 4 * len(examples.examples))
        self.assertEqual(len(runs), 8 * len(examples.examples))
        self.assertEqual({call.args[0] for call in builds}, {"dmd", "ldc2"})
        self.assertFalse(any(call.kwargs.get("sanitize") for call in runs))
        self.assertEqual(terminal_builds, 0)
        for call in builds:
            self.assertTrue(str(call.args[3]).endswith(".exe"))

    def test_linux_default_retains_tsan_and_terminal(self):
        builds, runs, terminal_builds = self.matrix("posix")
        self.assertEqual(len(builds), 5 * len(examples.examples))
        sanitized = [call for call in runs if call.kwargs.get("sanitize")]
        self.assertEqual(len(sanitized), 2 * len(examples.examples) - 1)
        self.assertEqual(terminal_builds, 4)

    def test_focused_dmd_and_quick_ldc_selection(self):
        for kwargs, compiler in (({"compiler": "dmd"}, "dmd"), ({"ldc_only": True}, "ldc2")):
            builds, runs, terminal_builds = self.matrix("nt", **kwargs)
            self.assertEqual(len(builds), 2 * len(examples.examples))
            self.assertEqual({call.args[0] for call in builds}, {compiler})
            self.assertEqual(len(runs), 4 * len(examples.examples))
            self.assertEqual(terminal_builds, 0)


if __name__ == "__main__":
    unittest.main()
