#!/usr/bin/env python3
"""Compile runtime-free modules and link/run the Farm from a native C main."""
import argparse
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
MODULES = ["antfarm/package.d", "antfarm/allocation.d", "antfarm/templates.d",
           "antfarm/parallel.d", "antfarm/actors/actor.d",
           "antfarm/actors/private_actor.d", "antfarm/actors/wave.d"]

def run(command, **kwargs):
    print("==>", subprocess.list2cmdline([str(x) for x in command]), flush=True)
    subprocess.run([str(x) for x in command], cwd=ROOT, check=True, timeout=120, **kwargs)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compiler", choices=("ldc2", "dmd"), default="ldc2")
    args = parser.parse_args()
    build = ROOT / ".build" / ("betterc-" + args.compiler + "-" + os.name)
    build.mkdir(parents=True, exist_ok=True)
    suffix = ".obj" if os.name == "nt" else ".o"
    objects = []
    for source in MODULES + ["tests/farm/betterc_probe.d"]:
        obj = build / (source.replace("/", "_").removesuffix(".d") + suffix)
        run([args.compiler, "-betterC", "-O", "-I.", "-c", source, "-of=" + str(obj)])
        if source in (MODULES[0], MODULES[1], "tests/farm/betterc_probe.d"):
            objects.append(obj)
    binary = build / ("native-c.exe" if os.name == "nt" else "native-c")
    source = ROOT / "tests/farm/betterc_main.c"
    if os.name == "nt":
        vswhere = Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "Microsoft Visual Studio/Installer/vswhere.exe"
        install = subprocess.check_output([str(vswhere), "-latest", "-products", "*",
                                          "-property", "installationPath"], text=True).strip()
        vcvars = Path(install) / "VC/Auxiliary/Build/vcvars64.bat"
        batch = build / "link-c.cmd"
        command = ["cl", "/nologo", "/std:c11", "/W4", "/WX", "/MT", str(source),
                   *map(str, objects), "/Fo" + str(build / "main.obj"), "/Fe" + str(binary),
                   "/link", "kernel32.lib", "advapi32.lib"]
        batch.write_text('@echo off\ncall "' + str(vcvars) + '" >nul\nif errorlevel 1 exit /b 1\n'
                         + subprocess.list2cmdline(command) + '\n', encoding="utf-8")
        run(["cmd.exe", "/d", "/c", str(batch)])
    else:
        run([os.environ.get("CC", "cc"), "-std=c11", "-Wall", "-Wextra", "-Werror",
             source, *objects, "-pthread", "-o", binary])
    run([binary], env={**os.environ, "ANTFARM_HUGE_PAGES": "0"})
    print("BETTERC CHECKS PASSED", flush=True)

if __name__ == "__main__":
    main()
