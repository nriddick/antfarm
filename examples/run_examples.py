#!/usr/bin/env python3
"""Compile and run the actor examples with LDC, DMD, and threaded TSan.

Windows builds use the compiler command in examples/README.md. This script is
the Linux `make check` lane. It does not bump versions or require a shell.
"""
import os
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
build = Path(__file__).resolve().parent / ".build"
examples = ("public_service", "public_wave", "private_tree")
configs = (
    ("ldc2", ("-g",), ""),
    ("ldc2", ("-O2", "-release"), "_release"),
    ("dmd", ("-g",), "_dmd"),
    ("dmd", ("-O", "-release"), "_dmd_release"),
)
tsan = ("ldc2", ("-g", "-O1", "-fsanitize=thread"), "_tsan")


def compile_example(compiler, flags, source, output):
    cmd = [compiler, *flags, "-i", "-I.", "-Iexamples",
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


def main():
    build.mkdir(exist_ok=True)
    for name in examples:
        for compiler, flags, suffix in configs:
            output = build / f"{name}{suffix}"
            compile_example(compiler, flags, name, output)
            for workers in ("0", "2"):
                run_example(output, workers)
        compiler, flags, suffix = tsan
        output = build / f"{name}{suffix}"
        compile_example(compiler, flags, name, output)
        for workers in ("0", "2"):
            run_example(output, workers, sanitize=True)
    print("PASS actor examples")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode or 1)
