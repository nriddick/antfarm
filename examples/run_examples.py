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
            "request_deadline", "fail_fast_fold", "fiber_cancel_at", "life/headless")
# Every program runs its actors on threadpool.host.FarmHost.
includes = ("-I.", "-Iexamples", "-Ithreadpool/source")
extra_includes = {
    # The fiber adapter example bridges to the separate antfarm-fibers package.
    "fiber_cancel_at": ("-Ifibers/source",),
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


def check_readme_programs(selected_configs):
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
    for compiler, flags, suffix in selected_configs:
        output = build / f"readme_payload{suffix}"
        cmd = [compiler, *flags, "-i", "-I.", str(source), f"-of={output}"]
        print("==>", " ".join(cmd), flush=True)
        subprocess.run(cmd, cwd=root, check=True)
        env = os.environ.copy()
        env["ANTFARM_HUGE_PAGES"] = "0"
        print("==>", output, flush=True)
        subprocess.run([str(output)], cwd=root, env=env, check=True,
                       timeout=run_timeout)


def main(ldc_only=False):
    selected_configs = configs[:2] if ldc_only else configs
    build.mkdir(exist_ok=True)
    check_readme_programs(selected_configs)
    for name in examples:
        for compiler, flags, suffix in selected_configs:
            output = build / f"{name.replace('/', '_')}{suffix}"
            compile_example(compiler, flags, name, output)
            for workers in ("0", "2"):
                run_example(output, workers)
        if not ldc_only:
            compiler, flags, suffix = tsan
            output = build / f"{name.replace('/', '_')}{suffix}"
            compile_example(compiler, flags, name, output)
            # TSan fails at the sanitizer/runtime stack-switch boundary for
            # migrating DRuntime Fibers; see fibers/DESIGN.md "Validation boundary".
            tsan_workers = ("0",) if name == "fiber_cancel_at" else ("0", "2")
            for workers in tsan_workers:
                run_example(output, workers, sanitize=True)
    if os.name == "posix" and not ldc_only:
        shim = compile_termbox()
        for compiler, flags, suffix in configs:
            compile_life_tui(compiler, flags, suffix, shim)
    print("PASS actor examples")


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "--life":
            if os.name != "posix":
                raise SystemExit("The Life terminal front end requires POSIX")
            compiler = os.environ.get("DC", "ldc2")
            flags = ("-O2", "-release") if compiler == "ldc2" else ("-O", "-release")
            compile_life_tui(compiler, flags, "", compile_termbox())
        else:
            main(ldc_only=len(sys.argv) > 1 and sys.argv[1] == "--ldc-only")
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode or 1)
    except subprocess.TimeoutExpired as error:
        print(f"FAIL timed out after {error.timeout} s: {' '.join(error.cmd)}",
              file=sys.stderr, flush=True)
        sys.exit(1)
