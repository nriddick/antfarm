#!/usr/bin/env python3
"""Compile and run the actor examples with LDC, DMD, and threaded TSan.

Windows builds use the compiler command in examples/README.md. This script is
the Linux `make check` lane. It does not bump versions or require a shell.

Every ```d block in README.md must be a complete program (`void main`) or
contain the word fragment. Complete programs are compiled and run here.
"""
import os
import re
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
build = Path(__file__).resolve().parent / ".build"
examples = ("public_service", "public_wave", "private_tree",
            "request_deadline", "fail_fast_fold", "fiber_cancel_at")
extra_includes = {
    # The fiber adapter example bridges to the separate antfarm-fibers package.
    "fiber_cancel_at": ("-Ifibers/source", "-Ithreadpool/source"),
}
configs = (
    ("ldc2", ("-g",), ""),
    ("ldc2", ("-O2", "-release"), "_release"),
    ("dmd", ("-g",), "_dmd"),
    ("dmd", ("-O", "-release"), "_dmd_release"),
)
tsan = ("ldc2", ("-g", "-O1", "-fsanitize=thread"), "_tsan")


def compile_example(compiler, flags, source, output):
    cmd = [compiler, *flags, "-i", "-I.", "-Iexamples",
           *extra_includes.get(source, ()),
           f"examples/{source}.d", f"-of={output}"]
    print("==>", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=root, check=True)


def run_example(binary, workers, sanitize=False):
    env = os.environ.copy()
    env["ANTFARM_HUGE_PAGES"] = "0"
    if sanitize:
        env["TSAN_OPTIONS"] = "history_size=7:halt_on_error=1"
    cmd = [str(binary), workers]
    print("==>", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=root, env=env, check=True)


def check_readme_programs():
    text = (root / "README.md").read_text()
    blocks = re.findall(r"```d\n(.*?)\n```", text, re.S)
    if not blocks:
        raise SystemExit("README.md has no ```d blocks")
    programs = []
    for index, block in enumerate(blocks):
        if "void main" in block:
            programs.append(block)
        elif "fragment" not in block:
            raise SystemExit(
                f"README.md ```d block {index} is neither a complete program "
                "nor marked as a fragment")
    if len(programs) != 1:
        raise SystemExit(
            f"expected one complete README program, found {len(programs)}")
    source = build / "readme_payload.d"
    source.write_text(programs[0] + "\n")
    for compiler, flags, suffix in configs:
        output = build / f"readme_payload{suffix}"
        cmd = [compiler, *flags, "-i", "-I.", str(source), f"-of={output}"]
        print("==>", " ".join(cmd), flush=True)
        subprocess.run(cmd, cwd=root, check=True)
        env = os.environ.copy()
        env["ANTFARM_HUGE_PAGES"] = "0"
        print("==>", output, flush=True)
        subprocess.run([str(output)], cwd=root, env=env, check=True)


def main():
    build.mkdir(exist_ok=True)
    check_readme_programs()
    for name in examples:
        for compiler, flags, suffix in configs:
            output = build / f"{name}{suffix}"
            compile_example(compiler, flags, name, output)
            for workers in ("0", "2"):
                run_example(output, workers)
        compiler, flags, suffix = tsan
        output = build / f"{name}{suffix}"
        compile_example(compiler, flags, name, output)
        # TSan fails at the sanitizer/runtime stack-switch boundary for
        # migrating DRuntime Fibers; see fibers/DESIGN.md "Validation boundary".
        tsan_workers = ("0",) if name == "fiber_cancel_at" else ("0", "2")
        for workers in tsan_workers:
            run_example(output, workers, sanitize=True)
    print("PASS actor examples")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode or 1)
