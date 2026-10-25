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
                mock.patch.object(examples, "check_quickstart_programs"), \
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


class QuickstartExtractionTest(unittest.TestCase):
    def test_extracts_and_runs_dispatcher_and_payload_from_quickstart(self):
        with tempfile.TemporaryDirectory() as directory:
            build = Path(directory)
            with mock.patch.object(examples, "build", build), \
                    mock.patch.object(examples.subprocess, "run") as run, \
                    contextlib.redirect_stdout(io.StringIO()):
                examples.check_quickstart_programs(examples.configs)
            sources = sorted(build.glob("quickstart_*.d"))
            self.assertEqual(len(sources), 2)
            programs = [source.read_text() for source in sources]
            for program in programs:
                self.assertEqual(program.count("void main"), 1)
            self.assertIn("forkJoinReduce", programs[0])
            self.assertIn("payloadRange!addSquare", programs[1])
            self.assertIn("first payload: 256 squares summed to", programs[1])
            self.assertEqual(run.call_count, 2 * len(examples.configs) * len(sources))
            compiled_sources = []
            for compile_call, execute_call in zip(run.call_args_list[::2],
                                                  run.call_args_list[1::2]):
                command = compile_call.args[0]
                compiled_sources.append(next(arg for arg in command if arg.endswith(".d")))
                self.assertIn("-Iforkjoin/source", command)
                self.assertEqual(command[-1], "-of=" + execute_call.args[0][0])
                self.assertEqual(execute_call.kwargs["env"]["ANTFARM_HUGE_PAGES"], "0")
                self.assertEqual(execute_call.kwargs["timeout"], examples.run_timeout)
            self.assertEqual(compiled_sources,
                             [str(source) for source in sources for _ in examples.configs])

    def test_rejects_unmarked_fragment_in_quickstart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            page = root / "docs/guide/quickstart.md"
            page.parent.mkdir(parents=True)
            page.write_text("```d\nimport antfarm;\n```\n")
            with mock.patch.object(examples, "root", root), \
                    self.assertRaisesRegex(SystemExit, "neither a complete program"):
                examples.check_quickstart_programs(())


if __name__ == "__main__":
    unittest.main()
