#!/usr/bin/env python3
"""Build the static C ABI distributions; no runtime dependency in antfarm_core."""
import argparse
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
CORE = ["antfarm/package.d", "antfarm/allocation.d", "cabi/source/antfarm/c/core.d",
        "antfarm/actors/actor.d", "antfarm/actors/wave.d", "cabi/source/antfarm/actors/cabi.d",
        "antfarm/actors/private_actor.d", "cabi/source/antfarm/actors/private_cabi.d"]


def run(command, **kwargs):
    command = [str(x) for x in command]
    print("==>", subprocess.list2cmdline(command), flush=True)
    return subprocess.run(command, cwd=ROOT, check=True, timeout=180, **kwargs)


def msvc(command, build, **kwargs):
    vswhere = Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "Microsoft Visual Studio/Installer/vswhere.exe"
    install = subprocess.check_output([str(vswhere), "-latest", "-products", "*",
                                      "-property", "installationPath"], text=True).strip()
    vcvars = Path(install) / "VC/Auxiliary/Build/vcvars64.bat"
    batch = build / "msvc.cmd"
    batch.write_text('@echo off\ncall "' + str(vcvars) + '" >nul\nif errorlevel 1 exit /b 1\n'
                     + subprocess.list2cmdline([str(x) for x in command]) + '\n', encoding="utf-8")
    return run(["cmd.exe", "/d", "/c", batch], **kwargs)


def library(build, name):
    return build / (name + ".lib" if os.name == "nt" else "lib" + name + ".a")


def build_core(compiler, build, audit=False, sanitize=False, debug=False):
    build.mkdir(parents=True, exist_ok=True)
    sources = list(CORE)
    flags = ["-g", "-fsanitize=thread"] if sanitize else []
    if audit:
        flags.append(("-version=" if compiler == "dmd" else "-d-version=") + "AntfarmWriteAuditHooks")
        flags.append(("-version=" if compiler == "dmd" else "-d-version=") + "AntfarmCActorTestHooks")
        sources.append("tests/cabi/audit.d")
    result = library(build, "antfarm_core")
    # LDC updates an existing archive in place, retaining removed source members.
    result.unlink(missing_ok=True)
    run([compiler, "-betterC", *(["-g"] if debug else ["-O", "-release"]), "-I.", "-Icabi/source", "-lib",
         *flags, *sources, "-of=" + str(result)])
    return result


def build_runtime(compiler, build):
    sources = ["cabi/source/antfarm/c/runtime.d", "cabi/source/antfarm/c/forkjoin.d", "cabi/source/antfarm/c/groups.d", "antfarm/templates.d", "antfarm/parallel.d",
               "antfarm/actors/package.d"]
    sources += [str(p.relative_to(ROOT)) for p in sorted((ROOT / "threadpool/source").rglob("*.d"))]
    sources += [str(p.relative_to(ROOT)) for p in sorted((ROOT / "forkjoin/source").rglob("*.d"))]
    result = library(build, "antfarm_runtime")
    result.unlink(missing_ok=True)
    run([compiler, "-O", "-release", "-op", "-I.", "-Icabi/source", "-Ithreadpool/source",
         "-Iforkjoin/source", "-lib", *sources, "-of=" + str(result)])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compiler", choices=("ldc2", "dmd"), default="ldc2")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--runtime", action="store_true", help="also build the optional non-Fiber runtime archive")
    args = parser.parse_args()
    build = (args.output or ROOT / ".build" / ("c-abi-" + args.compiler + "-" + os.name)).resolve()
    print(build_core(args.compiler, build), flush=True)
    if args.runtime:
        print(build_runtime(args.compiler, build), flush=True)


if __name__ == "__main__":
    main()
