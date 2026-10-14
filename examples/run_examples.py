#!/usr/bin/env python3
"""Compile and run the actor examples with LDC, DMD, and threaded TSan.

Runs on Windows and Linux without a shell. Windows omits TSan and the POSIX
terminal front end. This is also the Linux `make check` examples lane.

Every ```d block in docs/guide/quickstart.md must be a complete program (`void main`) or
contain the word fragment. Complete programs are compiled and run here.
"""
import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
build = Path(__file__).resolve().parent / ".build"
examples = ("public_service", "public_wave", "private_tree",
            "request_deadline", "fail_fast_fold", "fiber_cancel_at", "life/headless")
# Every program runs its actors on farmhost.FarmHost.
includes = ("-I.", "-Iexamples", "-Ithreadpool/source", "-Ifarmhost/source")
extra_includes = {
    # The fiber adapter example bridges to the separate ant_fibers package.
    "fiber_cancel_at": ("-Iant_fibers/source",),
}
# A stuck example fails the run instead of hanging it.
run_timeout = 300
configs = (
    ("ldc2", ("-g",), ""),
    ("ldc2", ("-O2", "-release"), "_release"),
    ("dmd", ("-g",), "_dmd"),
    ("dmd", ("-O", "-release"), "_dmd_release"),
)
tsan = ("ldc2", ("-g", "-O1", "-fsanitize=thread", "-d-version=TSan"), "_tsan")


def compile_example(compiler, flags, source, output):
    cmd = [compiler, *flags, "-i", *includes,
           *extra_includes.get(source, ()),
           f"examples/{source}.d", f"-of={output}"]
    print("==>", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=root, check=True)


def compile_termbox():
    # termbox2 is a POSIX-only dependency; never execute this without a tty.
    if os.name != "posix":
        return
    shim = build / "termbox_shim.o"
    build.mkdir(exist_ok=True)
    cmd = [os.environ.get("CC", "cc"), "-std=c99", "-O2", "-c",
           "examples/life/termbox_shim.c", "-o", str(shim)]
    print("==>", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=root, check=True)
    return shim


def compile_life_tui(compiler, flags, suffix, shim):
    cmd = [compiler, *flags, "-i", *includes,
           "examples/life/tui.d", str(shim), f"-of={build / ('life' + suffix)}"]
    print("==>", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=root, check=True)


def run_example(binary, workers, sanitize=False):
    env = os.environ.copy()
    env["ANTFARM_HUGE_PAGES"] = "0"
    if sanitize:
        env["TSAN_OPTIONS"] = "history_size=7:halt_on_error=1"
    cmd = [str(binary), workers]
    print("==>", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=root, env=env, check=True, timeout=run_timeout)


def check_quickstart_programs(selected_configs):
    text = (root / "docs/guide/quickstart.md").read_text()
    blocks = re.findall(r"```d\n(.*?)\n```", text, re.S)
    if not blocks:
        raise SystemExit("docs/guide/quickstart.md has no ```d blocks")
    programs = []
    for index, block in enumerate(blocks):
        if "void main" in block:
            programs.append(block)
        elif "fragment" not in block:
            raise SystemExit(
                f"docs/guide/quickstart.md ```d block {index} is neither a complete program "
                "nor marked as a fragment")
    if len(programs) != 1:
        raise SystemExit(
            f"expected one complete quickstart program, found {len(programs)}")
    source = build / "quickstart_payload.d"
    source.write_text(programs[0] + "\n")
    for compiler, flags, suffix in selected_configs:
        output = executable_path(f"quickstart_payload{suffix}")
        cmd = [compiler, *flags, "-i", "-I.", str(source), f"-of={output}"]
        print("==>", " ".join(cmd), flush=True)
        subprocess.run(cmd, cwd=root, check=True)
        env = os.environ.copy()
        env["ANTFARM_HUGE_PAGES"] = "0"
        print("==>", output, flush=True)
        subprocess.run([str(output)], cwd=root, env=env, check=True,
                       timeout=run_timeout)


def executable_path(name):
    return build / (name + (".exe" if os.name == "nt" else ""))


def main(ldc_only=False, compiler=None):
    enable_tsan = os.name == "posix" and not ldc_only and compiler != "dmd"
    selected_configs = tuple(config for config in configs
                             if compiler is None or config[0] == compiler)
    if ldc_only:
        selected_configs = configs[:2]
    build.mkdir(exist_ok=True)
    check_quickstart_programs(selected_configs)
    for name in examples:
        for compiler, flags, suffix in selected_configs:
            output = executable_path(f"{name.replace('/', '_')}{suffix}")
            compile_example(compiler, flags, name, output)
            for workers in ("0", "2"):
                run_example(output, workers)
        if enable_tsan:
            compiler, flags, suffix = tsan
            output = build / f"{name.replace('/', '_')}{suffix}"
            compile_example(compiler, flags, name, output)
            # Migration under TSan is covered by the Fiber TSan smoke suite;
            # this example's worker runs are not yet validated under TSan.
            tsan_workers = ("0",) if name == "fiber_cancel_at" else ("0", "2")
            for workers in tsan_workers:
                run_example(output, workers, sanitize=True)
    if os.name == "posix" and not ldc_only:
        shim = compile_termbox()
        for compiler, flags, suffix in selected_configs:
            compile_life_tui(compiler, flags, suffix, shim)
    print("PASS actor examples")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--ldc-only", action="store_true",
                           help="LDC debug/release only, without TSan or terminal builds")
    selection.add_argument("--compiler", choices=("ldc2", "dmd"))
    selection.add_argument("--life", action="store_true", help="build the POSIX terminal app")
    args = parser.parse_args()
    try:
        if args.life:
            if os.name != "posix":
                raise SystemExit("The Life terminal front end requires POSIX")
            compiler = os.environ.get("DC", "ldc2")
            flags = ("-O2", "-release") if compiler == "ldc2" else ("-O", "-release")
            compile_life_tui(compiler, flags, "", compile_termbox())
        else:
            main(ldc_only=args.ldc_only, compiler=args.compiler)
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode or 1)
    except subprocess.TimeoutExpired as error:
        print(f"FAIL timed out after {error.timeout} s: {' '.join(error.cmd)}",
              file=sys.stderr, flush=True)
        sys.exit(1)
