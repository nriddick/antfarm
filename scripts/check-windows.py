#!/usr/bin/env python3
"""Native Windows x64 integration checks; no Bash, make or TSan required."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / ".build/windows-check"
LANES = ("packages", "threadpool", "ordering", "review", "actors", "private", "examples")
QUICK_LANES = ("packages", "review", "examples")
HARDWARE_SKIPS = ("SKIP worker cases", "SKIP affinity regression", "SKIP confine-test")


def stop_tree(proc):
    """Kill only this check's process tree, including Dub's test child."""
    result = subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                            capture_output=True, text=True, timeout=15)
    if result.returncode and proc.poll() is None:
        proc.kill()
        proc.wait(timeout=5)
        raise RuntimeError("could not terminate check's process tree: " + result.stderr.strip())
    proc.wait(timeout=5)


def run(cmd, env, timeout, allow_hardware_skips=False):
    cmd = [str(arg) for arg in cmd]
    print("==>", subprocess.list2cmdline(cmd), flush=True)
    # A file avoids a pipe-reader thread waiting indefinitely when a descendant
    # inherits stdout and tree termination itself fails (e.g. a restricted host).
    with tempfile.TemporaryFile() as log:
        proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=log,
                                stderr=subprocess.STDOUT)
        try:
            proc.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            stop_tree(proc)
            raise
        finally:
            log.seek(0)
            output = log.read().decode("utf-8", errors="replace").replace("\r\n", "\n")
            print(output, end="", flush=True)
        if proc.returncode:
            raise subprocess.CalledProcessError(proc.returncode, cmd)
        if not allow_hardware_skips and any(skip in output for skip in HARDWARE_SKIPS):
            raise RuntimeError("hardware coverage skipped; use --allow-hardware-skips to accept it")


class Checks:
    def __init__(self, compiler, args, env):
        self.compiler = compiler
        self.args = args
        self.env = env
        self.version_flag = "-version=" if compiler == "dmd" else "-d-version="
        self.flags = ["-g"] if compiler == "dmd" else ["-g", "-O1"]

    def run(self, cmd):
        run(cmd, self.env, self.args.timeout, self.args.allow_hardware_skips)

    def dub(self, command, package=None, *options):
        cmd = ["dub", command]
        if package:
            cmd.append(package)
        self.run([*cmd, "--compiler=" + self.compiler, "--arch=x86_64",
                  "--combined", "--nodeps", *options])

    def build_run(self, name, sources, versions=(), arguments=()):
        binary = BUILD / (name + "_" + self.compiler + ".exe")
        self.run([self.compiler, *self.flags, "-I.",
                  *(self.version_flag + version for version in versions),
                  *sources, "-of=" + str(binary)])
        self.run([binary, *arguments])

    def packages(self):
        self.run([sys.executable, "-B", "scripts/check-betterc.py", "--compiler", self.compiler])
        for package in (None, ":threadpool", ":ant_fibers"):
            self.dub("test", package)

    def threadpool(self):
        for config in ("affinity-test", "confine-test", "wait-races", "host-test"):
            self.dub("run", ":threadpool", "-c", config)
        self.dub("run", ":threadpool", "-c", "wait-races", "--build=release")
        if self.compiler == "ldc2":
            self.dub("run", ":threadpool", "-c", "host-test", "--build=release")

    def ordering(self):
        self.run([sys.executable, "-B", "scripts/check-ring-stores.py"])
        for name, versions in (("farm_ordering", ("AntfarmWriteAuditHooks",)), ("leaf_reuse", ())):
            self.build_run(name, ["tests/farm/" + name + ".d",
                                 "antfarm/package.d", "antfarm/allocation.d"], versions)

    def review(self):
        self.run([sys.executable, "-B", "tests/farm/run_parallel.py",
                  "--compiler", self.compiler])
        common = ["tests/farm/torture_common.d", "antfarm/package.d",
                  "antfarm/allocation.d", "antfarm/templates.d"]
        self.build_run("torture", ["tests/farm/torture_tests.d", *common])
        self.build_run("sweep_cursor", ["tests/farm/sweep_cursor.d", *common],
                       ("AntfarmSweepStats",))
        if not self.args.quick:
            self.build_run("t19", ["tests/farm/t19_flood_lap.d", *common])
            self.build_run("concurrent_create", ["tests/farm/concurrent_create.d",
                                                 "antfarm/package.d", "antfarm/allocation.d"])

    def actors(self):
        self.build_run("actors", ["tests/actors/actors_torture.d", "antfarm/package.d",
                                  "antfarm/allocation.d", "antfarm/templates.d",
                                  "antfarm/actors/package.d", "antfarm/actors/actor.d", "antfarm/actors/wave.d",
                                  "antfarm/actors/private_actor.d", "antfarm/actors/mimalloc.d"],
                       ("AntfarmActorTestHooks", "AntfarmMimallocV3", "AntfarmMimallocStub"))

    def private(self):
        sources = ["antfarm/package.d", "antfarm/allocation.d", "antfarm/actors/actor.d",
                   "antfarm/actors/wave.d", "antfarm/actors/private_actor.d"]
        self.build_run("private", ["tests/private/main.d", *sources],
                       ("AntfarmWriteAuditHooks", "AntfarmActorTestHooks"))
        self.run([sys.executable, "-B", "tests/private/extract_doc_examples.py"])
        for source in sorted((ROOT / "tests/private/.doc-examples").glob("private_doc_*.d")):
            self.build_run(source.stem, [source, *sources])

    def examples(self):
        self.run([sys.executable, "-B", "examples/run_examples.py", "--compiler", self.compiler])


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compiler", choices=("all", "ldc2", "dmd"), default="all")
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--quick", action="store_true", help="LDC packages, review and examples only")
    selection.add_argument("--lanes", nargs="+", choices=LANES, help="run only selected lanes")
    parser.add_argument("--timeout", type=int, default=300, help="seconds per build or run (default: 300)")
    parser.add_argument("--allow-hardware-skips", action="store_true",
                        help="accept missing CPUs/groups for live affinity and host probes")
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.quick and args.compiler == "dmd":
        parser.error("--quick requires LDC; use --lanes to select DMD checks")
    return args


def main(argv=None):
    args = parse_args(argv)
    if os.name != "nt":
        raise RuntimeError("check-windows requires Windows; use make check on Linux")
    compilers = ("ldc2",) if args.quick else (("ldc2", "dmd") if args.compiler == "all" else (args.compiler,))
    for tool in ("dub", "taskkill", *compilers):
        if shutil.which(tool) is None:
            raise RuntimeError("required tool is not on PATH: " + tool)
    BUILD.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["ANTFARM_HUGE_PAGES"] = "0"
    # Confine Dub's generated files to the checkout.
    env["DUB_HOME"] = str(BUILD / "dub-home")
    lanes = QUICK_LANES if args.quick else (args.lanes or LANES)
    for compiler in compilers:
        checks = Checks(compiler, args, env)
        for lane in lanes:
            getattr(checks, lane)()
    print("Excluded: TSan, POSIX fork/death probes, Life terminal UI, real mimalloc and timing gates.", flush=True)
    print("WINDOWS CHECKS PASSED" + (" (hardware skips allowed)" if args.allow_hardware_skips else ""), flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        print("FAIL:", error, file=sys.stderr, flush=True)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)
