#!/usr/bin/env python3
"""Native Windows x64 integration checks; no Bash, make or TSan required."""
import argparse
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / ".build/windows-check"
LANES = ("packages", "threadpool", "forkjoin", "ordering", "ring", "ant_uv", "review", "actors",
         "private", "examples")
QUICK_LANES = ("packages", "ring", "review", "examples")
HARDWARE_SKIPS = ("SKIP worker cases", "SKIP affinity regression", "SKIP confine-test",
                  "SKIP worker Fiber reads")


def load_script(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


build_api = load_script("cabi_build", "scripts/build-c-abi.py")
# The pinned build's installation.
DEFAULT_LIBUV = load_script("libuv_build", "scripts/build-libuv.py").PREFIX


def libuv_missing(prefix):
    """The first file of a libuv installation that is absent, else None."""
    for name in ("include/uv.h", "lib/uv.lib", "bin/uv.dll"):
        if not (prefix / name).is_file():
            return prefix / name
    return None


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

    def run(self, cmd, env=None):
        run(cmd, env or self.env, self.args.timeout, self.args.allow_hardware_skips)

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
        self.run([sys.executable, "-B", "scripts/check-c-abi.py", "--compiler", self.compiler, "--core-only"])
        for package in (None, ":threadpool", ":ant_fibers"):
            self.dub("test", package)

    def threadpool(self):
        self.run([sys.executable, "-B", "scripts/check-c-abi.py", "--compiler", self.compiler, "--runtime-only"])
        for config in ("affinity-test", "confine-test", "wait-races", "host-test"):
            self.dub("run", ":threadpool", "-c", config)
        self.dub("run", ":threadpool", "-c", "wait-races", "--build=release")
        if self.compiler == "ldc2":
            self.dub("run", ":threadpool", "-c", "host-test", "--build=release")

    def forkjoin(self):
        # The runtime flag runs the suite driver after the module unittests.
        self.dub("test", ":forkjoin", "--", "--DRT-testmode=run-main")

    def ordering(self):
        self.run([sys.executable, "-B", "scripts/check-ring-stores.py"])
        for name, versions in (("farm_ordering", ("AntfarmWriteAuditHooks",)),
                               ("subscription_live", ("AntfarmWriteAuditHooks",)),
                               ("leaf_reuse", ())):
            self.build_run(name, ["tests/farm/" + name + ".d",
                                 "antfarm/package.d", "antfarm/allocation.d"], versions)

    def ring(self):
        self.build_run("ring_test", ["tests/ring/ring_test.d", "antfarm/ring.d",
                                     "antfarm/allocation.d"], ("AntfarmRingTestHooks",))

    def ant_uv(self):
        # The layout oracle is C against the real uv.h; libuv's own headers
        # are external so /W4 /WX applies to the oracle alone.
        libuv = self.args.libuv
        layout = BUILD / "uv_layout.obj"
        build_api.msvc(["cl", "/nologo", "/O1", "/W4", "/WX", "/MT", "/c", "tests/ant_uv/layout.c",
                        "/external:I", libuv / "include", "/external:W0", "/Fo" + str(layout)], BUILD)
        env = dict(self.env)
        env["PATH"] = str(libuv / "bin") + os.pathsep + env.get("PATH", "")
        # The suite, the suite under DIP1000 (which holds the @safe surface's
        # scope rules), then both examples as a user builds them.
        for name, sources, arguments, extra in (
                ("uv_test", ["tests/ant_uv/uv_test.d", layout], (), ()),
                ("uv_test_dip1000", ["tests/ant_uv/uv_test.d", layout], (), ("-preview=dip1000",)),
                ("read_files", ["ant_uv/examples/read_files.d"], ("README.md", "ant_uv/README.md"), ()),
                ("load_assets", ["ant_uv/examples/load_assets.d"], (), ())):
            binary = BUILD / (name + "_" + self.compiler + ".exe")
            self.run([self.compiler, *self.flags, *extra, "-i", "-I.", "-Ithreadpool/source",
                      "-Iant_fibers/source", "-Iant_uv/source", "-Iforkjoin/source", *sources,
                      libuv / "lib/uv.lib",
                      "-of=" + str(binary), "-od=" + str(BUILD / (name + "_obj_" + self.compiler))])
            self.run([binary, *arguments], env)

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
    selection.add_argument("--quick", action="store_true", help="LDC packages, ring, review and examples only")
    selection.add_argument("--lanes", nargs="+", choices=LANES, help="run only selected lanes")
    parser.add_argument("--timeout", type=int, default=300, help="seconds per build or run (default: 300)")
    parser.add_argument("--allow-hardware-skips", action="store_true",
                        help="accept missing CPUs/groups for live affinity and host probes")
    parser.add_argument("--libuv", type=Path,
                        help="libuv installation (include/uv.h, lib/uv.lib, bin/uv.dll) for the ant_uv "
                             "lane; default: the pinned build of scripts/build-libuv.py")
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
    uv_skipped = False
    if "ant_uv" in lanes:
        explicit = args.libuv is not None or (args.lanes is not None and "ant_uv" in args.lanes)
        args.libuv = (args.libuv or DEFAULT_LIBUV).resolve()
        missing = libuv_missing(args.libuv)
        if missing is not None:
            hint = "; python -B scripts/build-libuv.py builds the pinned one"
            if explicit:
                raise RuntimeError("ant_uv needs libuv: " + str(missing) + " not found" + hint)
            print("SKIP ant_uv: " + str(missing) + " not found" + hint, flush=True)
            lanes = tuple(lane for lane in lanes if lane != "ant_uv")
            uv_skipped = True
    for compiler in compilers:
        checks = Checks(compiler, args, env)
        for lane in lanes:
            getattr(checks, lane)()
    print("Excluded: TSan, POSIX fork/death probes, Life terminal UI, real mimalloc and timing gates"
          + ("; ant_uv (no libuv)." if uv_skipped else "."), flush=True)
    notes = [note for note, on in (("hardware skips allowed", args.allow_hardware_skips),
                                   ("ant_uv skipped", uv_skipped)) if on]
    print("WINDOWS CHECKS PASSED" + (" (" + ", ".join(notes) + ")" if notes else ""), flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        print("FAIL:", error, file=sys.stderr, flush=True)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)
