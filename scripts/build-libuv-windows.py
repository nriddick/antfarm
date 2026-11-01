#!/usr/bin/env python3
"""Build the pinned libuv for ant_uv on Windows x64: a checksummed release
archive, configured by CMake, installed as uv.dll with its import library
uv.lib and the headers under .build/libuv/<version>/."""
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
VERSION = "1.53.0"
ARCHIVE = "libuv-v" + VERSION + ".tar.gz"
URL = "https://dist.libuv.org/dist/v" + VERSION + "/" + ARCHIVE
# Signed by Santiago Gimeno (612F 0EAD 9401 6223 79DF 4402 F28C 3C8D A33C 03BE,
# listed in libuv's MAINTAINERS.md); the signature was checked when pinning.
SHA256 = "cb0d6dd2128d5a95bd242c6cc982a24fe608fa93da57b6b4ec763b0018c53e64"
WORK = ROOT / ".build/libuv"
PREFIX = WORK / VERSION


def run(cmd):
    cmd = [str(arg) for arg in cmd]
    print("==>", subprocess.list2cmdline(cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT, check=True, timeout=900)


def find_cmake(explicit):
    if explicit:
        return explicit
    found = shutil.which("cmake")
    if found:
        return found
    # Visual Studio ships CMake; prefer it to a missing PATH entry.
    vswhere = Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / \
        "Microsoft Visual Studio/Installer/vswhere.exe"
    if vswhere.exists():
        install = subprocess.check_output([str(vswhere), "-latest", "-products", "*",
                                           "-property", "installationPath"], text=True).strip()
        bundled = Path(install) / "Common7/IDE/CommonExtensions/Microsoft/CMake/CMake/bin/cmake.exe"
        if bundled.exists():
            return str(bundled)
    raise RuntimeError("CMake not found; pass --cmake or install Visual Studio's CMake component")


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def fetch(archive):
    if archive is not None:
        source = Path(archive)
    else:
        source = WORK / ARCHIVE
        if not source.exists():
            print("==> download", URL, flush=True)
            partial = source.with_suffix(".part")
            with urllib.request.urlopen(URL, timeout=120) as response, open(partial, "wb") as out:
                shutil.copyfileobj(response, out)
            partial.replace(source)
    actual = sha256(source)
    if actual != SHA256:
        raise RuntimeError(f"{source}: SHA-256 {actual}, expected {SHA256}")
    return source


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", help="use this copy of " + ARCHIVE + " instead of downloading")
    parser.add_argument("--cmake", help="full path to CMake when it is not on PATH")
    parser.add_argument("--force", action="store_true", help="rebuild an existing installation")
    args = parser.parse_args(argv)
    if os.name != "nt":
        raise RuntimeError("build-libuv-windows targets Windows; use the system libuv elsewhere")
    if (PREFIX / "lib/uv.lib").exists() and (PREFIX / "bin/uv.dll").exists() and not args.force:
        print("libuv", VERSION, "already installed in", PREFIX)
        return
    WORK.mkdir(parents=True, exist_ok=True)
    archive = fetch(args.archive)
    source = WORK / "src"
    build = WORK / "build"
    for stale in (source, build, PREFIX):
        shutil.rmtree(stale, ignore_errors=True)
    with tarfile.open(archive) as tar:
        tar.extractall(source, filter="data")
    tree = source / ("libuv-v" + VERSION)
    cmake = find_cmake(args.cmake)
    # The shared library carries its own CRT, so the D programs keep their
    # static one (-mscrtlib=libcmt) without a CRT mismatch at link time.
    run([cmake, "-S", tree, "-B", build, "-A", "x64",
         "-DLIBUV_BUILD_SHARED=ON", "-DLIBUV_BUILD_TESTS=OFF", "-DLIBUV_BUILD_BENCH=OFF",
         "-DBUILD_TESTING=OFF"])
    run([cmake, "--build", build, "--config", "Release", "--target", "uv", "--parallel"])
    for sub in ("include", "lib", "bin"):
        (PREFIX / sub).mkdir(parents=True, exist_ok=True)
    shutil.copytree(tree / "include", PREFIX / "include", dirs_exist_ok=True)
    shutil.copy2(build / "Release/uv.lib", PREFIX / "lib/uv.lib")
    shutil.copy2(build / "Release/uv.dll", PREFIX / "bin/uv.dll")
    print("libuv", VERSION, "installed in", PREFIX, flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError, tarfile.TarError) as error:
        print("FAIL:", error, file=sys.stderr, flush=True)
        sys.exit(1)
