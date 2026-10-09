#!/usr/bin/env python3
"""Adversarial probes for scripts/check-doc-links.py (stdlib only).

Creates a temporary git repository, copies the checker into it, stages the
fixtures, and asserts the exit status for each case. Run directly, or from
`make check-docs` alongside the checker.
"""
import pathlib
import shutil
import subprocess
import tempfile
import unittest


HERE = pathlib.Path(__file__).resolve().parent
CHECKER = HERE / "check-doc-links.py"


def run(cwd, *args):
    return subprocess.run(args, cwd=str(cwd), capture_output=True, text=True)


class LinkProbes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="check-doc-links-")
        cls.root = pathlib.Path(cls.tmp)
        run(cls.root, "git", "init", "-q")
        run(cls.root, "git", "config", "user.email", "probe@example.invalid")
        run(cls.root, "git", "config", "user.name", "probe")
        (cls.root / "scripts").mkdir()
        shutil.copy2(CHECKER, cls.root / "scripts" / "check-doc-links.py")
        (cls.root / "README.md").write_text("# Root\n")
        (cls.root / "probe.md").write_text("# Probe\n")
        (cls.root / "target.md").write_text(
            "Setext heading\n"
            "==============\n"
            "\n"
            "Ordinary paragraph\n"
            "\n"
            "---\n"
            "\n"
            "## [Linked heading](probe.md)\n")
        (cls.root / "untracked.txt").write_text("untracked\n")
        (cls.root / "untracked-dir").mkdir()
        (cls.root / "docs" / "guide").mkdir(parents=True)
        (cls.root / "docs" / "guide" / "x.md").write_text("# x\n")
        (cls.root / "outside-link").symlink_to("/etc/passwd")
        run(cls.root, "git", "add", "README.md", "probe.md", "target.md",
            "docs/guide/x.md")
        run(cls.root, "git", "commit", "-q", "-m", "fixtures")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def check(self, text, expected):
        (self.root / "probe.md").write_text("# Probe\n\n" + text + "\n")
        result = run(self.root, "python3", "-B", "scripts/check-doc-links.py")
        if result.returncode != expected:
            self.fail(f"expected exit {expected}, got {result.returncode}\n"
                      f"{result.stdout}{result.stderr}")

    def test_01_missing_path(self):
        self.check("[x](missing.md)", 1)

    def test_02_missing_anchor(self):
        self.check("[x](README.md#missing-anchor)", 1)

    def test_03_untracked_file(self):
        self.check("[x](untracked.txt)", 1)

    def test_04_relative_escape_file(self):
        self.check("[x](../../etc/passwd)", 1)

    def test_05_absolute_file(self):
        self.check("[x](/etc/passwd)", 1)

    def test_06_untracked_symlink_outside(self):
        self.check("[x](outside-link)", 1)

    def test_07_untracked_directory(self):
        self.check("[x](untracked-dir/)", 1)

    def test_08_tracked_directory(self):
        self.check("[x](docs/guide/)", 0)

    def test_09_setext_heading(self):
        self.check("[x](target.md#setext-heading)", 0)

    def test_10_inline_code_single_backtick(self):
        self.check("`[x](missing.md)`", 0)

    def test_11_bracket_in_link_text(self):
        self.check("[x with ] inside](target.md#setext-heading)", 0)

    def test_12_bracket_in_broken_link_text(self):
        self.check("[x with ] inside](missing.md)", 1)

    def test_13_escape_parent_directory(self):
        self.check("[x](../)", 1)

    def test_14_escape_root_directory(self):
        self.check("[x](../../)", 1)

    def test_15_tracked_symlink_outside(self):
        run(self.root, "git", "add", "outside-link")
        try:
            self.check("[x](outside-link)", 1)
        finally:
            run(self.root, "git", "reset", "-q", "HEAD", "--", "outside-link")

    def test_16_inline_code_with_inner_backtick_and_broken_link(self):
        # One double-backtick span containing a single backtick and a link.
        self.check("`` a ` [x](missing.md) ``", 0)

    def test_17_horizontal_rule_is_not_a_heading(self):
        self.check("[x](target.md#ordinary-paragraph)", 1)

    def test_18_reference_definition(self):
        self.check("[x]: missing.md", 1)

    def test_19_fenced_code(self):
        self.check("```text\n[x](missing.md)\n```", 0)

    def test_20_mismatched_backtick_runs_do_not_mask(self):
        # The single-backtick opener closes on the later single backtick, so the
        # length-2 run in between must not hide the broken link.
        self.check("`a `` b` [x](missing.md)", 1)

    def test_21_fence_requires_the_same_character(self):
        self.check("```\n[x](missing.md)\n~~~\n[x](missing.md)\n```", 0)

    def test_22_fence_closes_only_on_a_long_enough_run(self):
        self.check("````\n[x](missing.md)\n```\n[x](missing.md)\n````", 0)

    def test_23_fence_close_with_info_string_is_content(self):
        self.check("```\n[x](missing.md)\n``` still-open\n[x](missing.md)\n```", 0)

    def test_24_escaped_backtick_is_literal(self):
        self.check("\\` [x](missing.md) \\`", 1)

    def test_25_heading_slug_strips_link_syntax(self):
        self.check("[x](target.md#linked-heading)", 0)

    def test_26_query_string_is_stripped_from_the_path(self):
        self.check("[x](target.md?query=1#setext-heading)", 0)

    def test_27_tracked_markdown_symlink_outside(self):
        outside = self.root / "outside.md"
        if outside.is_symlink() or outside.exists():
            outside.unlink()
        outside.symlink_to("/etc/passwd")
        run(self.root, "git", "add", "outside.md")
        try:
            self.check("[x](outside.md)", 1)
        finally:
            run(self.root, "git", "reset", "-q", "HEAD", "--", "outside.md")


if __name__ == "__main__":
    unittest.main(verbosity=2)
