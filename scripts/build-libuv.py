#!/usr/bin/env python3
"""Build the pinned libuv for ant_uv, the same on Linux and Windows: the
checksummed release archive, the local patches listed below applied, then
CMake. Linux installs a static libuv.a with the headers under
.build/libuv/<version>/; Windows installs uv.dll with its import library
uv.lib and the headers there. Both lanes, Dub and the perf tools link that
installation, never the system libuv."""
import argparse
import hashlib
import os
from pathlib import Path
import re
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
# Unified diffs against the archive's tree, applied in this order. Each is
# described in the docs/development record beside it; drop one once the
# pinned release carries it.
PATCHES = [
    ROOT / "docs/development/libuv-sqpoll-wakeup-2026-11-04/libuv-1.53.0-sqpoll-wakeup-fence.patch",
]
WORK = ROOT / ".build/libuv"
PREFIX = WORK / VERSION
WINDOWS = os.name == "nt"


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
    if WINDOWS:
        # Visual Studio ships CMake; prefer it to a missing PATH entry.
        vswhere = Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / \
            "Microsoft Visual Studio/Installer/vswhere.exe"
        if vswhere.exists():
            install = subprocess.check_output([str(vswhere), "-latest", "-products", "*",
                                               "-property", "installationPath"], text=True).strip()
            bundled = Path(install) / \
                "Common7/IDE/CommonExtensions/Microsoft/CMake/CMake/bin/cmake.exe"
            if bundled.exists():
                return str(bundled)
        raise RuntimeError("CMake not found; pass --cmake or install Visual Studio's CMake component")
    raise RuntimeError("CMake not found; pass --cmake or install cmake")


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


def apply_patch(tree, patch):
    """Apply one unified diff to the files under `tree`. Each hunk must match
    exactly once, so a patch that no longer fits the archive fails here
    rather than building something else."""
    lines = patch.read_text().splitlines()
    i = 0
    files = 0
    while i < len(lines):
        if not lines[i].startswith("--- "):
            i += 1
            continue
        if not lines[i + 1].startswith("+++ "):
            raise RuntimeError(f"{patch.name}: malformed file header at line {i + 1}")
        target = tree / re.sub(r"^[ab]/", "", lines[i + 1][4:].split("\t")[0])
        if not target.is_file():
            raise RuntimeError(f"{patch.name}: {target} is not in the archive")
        text = target.read_text().splitlines(keepends=True)
        i += 2
        hunks = 0
        while i < len(lines) and lines[i].startswith("@@"):
            old, new = [], []
            i += 1
            while i < len(lines) and not lines[i].startswith(("@@", "--- ")):
                line = lines[i]
                if line.startswith("+"):
                    new.append(line[1:] + "\n")
                elif line.startswith("-"):
                    old.append(line[1:] + "\n")
                elif line.startswith("\\"):
                    pass  # "No newline at end of file": not expected here
                else:
                    old.append(line[1:] + "\n")
                    new.append(line[1:] + "\n")
                i += 1
            matches = [k for k in range(len(text) - len(old) + 1) if text[k:k + len(old)] == old]
            if len(matches) != 1:
                raise RuntimeError(f"{patch.name}: hunk {hunks + 1} for {target.name} matches "
                                   f"{len(matches)} places, not one")
            text[matches[0]:matches[0] + len(old)] = new
            hunks += 1
        target.write_text("".join(text))
        print("==> patched", target.relative_to(tree), f"({hunks} hunk{'s' if hunks != 1 else ''}"
              f" of {patch.name})", flush=True)
        files += 1
    if files == 0:
        raise RuntimeError(f"{patch.name}: no file headers")


def patch_stamp():
    return "".join(f"{sha256(p)}  {p.name}\n" for p in PATCHES)


def installed():
    if WINDOWS:
        present = (PREFIX / "lib/uv.lib").exists() and (PREFIX / "bin/uv.dll").exists()
    else:
        present = (PREFIX / "lib/libuv.a").exists()
    present = present and (PREFIX / "include/uv.h").exists()
    stamp = PREFIX / "patches.sha256"
    return present and stamp.exists() and stamp.read_text() == patch_stamp()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", help="use this copy of " + ARCHIVE + " instead of downloading")
    parser.add_argument("--cmake", help="full path to CMake when it is not on PATH")
    parser.add_argument("--force", action="store_true", help="rebuild an existing installation")
    parser.add_argument("--print-prefix", action="store_true",
                        help="print the installation directory and exit")
    args = parser.parse_args(argv)
    if args.print_prefix:
        print(PREFIX)
        return
    for patch in PATCHES:
        if not patch.is_file():
            raise RuntimeError(f"patch not found: {patch}")
    if installed() and not args.force:
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
    for patch in PATCHES:
        apply_patch(tree, patch)
    cmake = find_cmake(args.cmake)
    common = ["-DLIBUV_BUILD_TESTS=OFF", "-DLIBUV_BUILD_BENCH=OFF", "-DBUILD_TESTING=OFF"]
    if WINDOWS:
        # The shared library carries its own CRT, so the D programs keep their
        # static one (-mscrtlib=libcmt) without a CRT mismatch at link time.
        run([cmake, "-S", tree, "-B", build, "-A", "x64", "-DLIBUV_BUILD_SHARED=ON", *common])
        run([cmake, "--build", build, "--config", "Release", "--target", "uv", "--parallel"])
    else:
        # A static archive, position-independent for the PIE executables the
        # D compilers produce, so nothing depends on a library path at run time.
        run([cmake, "-S", tree, "-B", build, "-DCMAKE_BUILD_TYPE=Release",
             "-DLIBUV_BUILD_SHARED=OFF", "-DCMAKE_POSITION_INDEPENDENT_CODE=ON", *common])
        run([cmake, "--build", build, "--target", "uv_a", "--parallel"])
    for sub in ("include", "lib") + (("bin",) if WINDOWS else ()):
        (PREFIX / sub).mkdir(parents=True, exist_ok=True)
    shutil.copytree(tree / "include", PREFIX / "include", dirs_exist_ok=True)
    if WINDOWS:
        shutil.copy2(build / "Release/uv.lib", PREFIX / "lib/uv.lib")
        shutil.copy2(build / "Release/uv.dll", PREFIX / "bin/uv.dll")
    else:
        shutil.copy2(build / "libuv.a", PREFIX / "lib/libuv.a")
    (PREFIX / "patches.sha256").write_text(patch_stamp())
    print("libuv", VERSION, "installed in", PREFIX, "with", len(PATCHES), "patch(es)", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError, tarfile.TarError) as error:
        print("FAIL:", error, file=sys.stderr, flush=True)
        sys.exit(1)
