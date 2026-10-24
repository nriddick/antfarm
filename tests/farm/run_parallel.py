#!/usr/bin/env python3
"""Build/run focused parallel-loop checks, with bounded subprocess lifetimes."""
import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
BUILD = ROOT / ".build/parallel-check"
SOURCES = ["tests/farm/parallel_tests.d", "antfarm/parallel.d",
           "antfarm/package.d", "antfarm/allocation.d", "antfarm/templates.d"]


def run(args, env, death=None):
    print("==>", subprocess.list2cmdline([str(a) for a in args]), flush=True)
    result = subprocess.run(args, cwd=ROOT, env=env, capture_output=True,
                            text=True, timeout=300)
    output = result.stdout + result.stderr
    print(output, end="", flush=True)
    if death:
        if result.returncode == 0 or death not in output:
            raise RuntimeError("death probe did not report its expected fatal condition")
    elif result.returncode:
        raise subprocess.CalledProcessError(result.returncode, args)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compiler", choices=("ldc2", "dmd"), default="ldc2")
    parser.add_argument("--tsan", action="store_true")
    args = parser.parse_args()
    if args.tsan and args.compiler != "ldc2":
        parser.error("--tsan requires ldc2")
    BUILD.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["ANTFARM_HUGE_PAGES"] = "0"
    for mode in (("tsan",) if args.tsan else ("debug", "release")):
        binary = BUILD / (args.compiler + "_" + mode + (".exe" if os.name == "nt" else ""))
        flags = ["-g", "-I.", "-O1" if args.compiler == "ldc2" else "-O"]
        if mode == "release":
            flags.append("-release")
        elif mode == "tsan":
            flags.extend(["-fsanitize=thread", "-d-version=TSan"])
        run([args.compiler, *flags, *SOURCES, "-of=" + str(binary)], env)
        run([binary], env)
        if mode != "tsan":
            run([binary, "invalid-hook"], env, death="table completion hook requires a callback")
            run([binary, "grain-zero"], env, death="payload grain must be positive")
            run([binary, "cost-class"], env, death="parallel costClass out of range")
            run([binary, "drop"], env, death="parallel handle dropped before submission completed")
    print("PARALLEL CHECKS PASSED", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        print("FAIL:", error, file=sys.stderr)
        sys.exit(1)
