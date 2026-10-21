#!/usr/bin/env python3
"""Native C/C++ ABI, release contract deaths, and deterministic publication join."""
import argparse
import collections
import importlib.util
import os
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("cabi_build", ROOT / "scripts/build-c-abi.py")
build_api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build_api)


def native(source, build, libraries, defines=(), no_exceptions=False):
    cpp = str(source).endswith(".cpp")
    name = Path(source).stem + ("-noexcept" if no_exceptions else "")
    binary = build / (name + (".exe" if os.name == "nt" else ""))
    if str(source).startswith("tests/cabi/"):
        defines = (*defines, "NDEBUG")
    if os.name == "nt":
        build_api.msvc(["cl", "/nologo", "/std:c++14" if cpp else "/std:c11", "/W4", "/WX", "/MT",
                        *(["/EHs-c-", "/D_HAS_EXCEPTIONS=0", "/U_CPPUNWIND"] if no_exceptions else
                          ["/EHsc"] if cpp else []), "/Iinclude", "/Itests/cabi",
                        *("/D" + x for x in defines), source,
                        "/Fo" + str(build / (name + ".obj")), "/Fe" + str(binary),
                        "/link", *libraries, "kernel32.lib", "advapi32.lib"], build)
    else:
        build_api.run([os.environ.get("CXX" if cpp else "CC", "c++" if cpp else "cc"),
                       "-std=c++11" if cpp else "-std=c11", "-Wall", "-Wextra", "-Werror",
                       *(["-fno-exceptions"] if no_exceptions else []),
                       "-Iinclude", "-Itests/cabi", *("-D" + x for x in defines),
                       source, *libraries, "-pthread", "-o", binary])
    return binary


def audit_symbols(library, headers, build, core=False):
    """Inspect every archive member, including those no example happens to link."""
    defined, undefined = [], set()
    if os.name == "nt":
        output = build_api.msvc(["dumpbin", "/symbols", library], build,
                                capture_output=True, text=True).stdout
        for line in output.splitlines():
            if "External" not in line or "|" not in line:
                continue
            symbol = line.split("|", 1)[1].strip().split()[0]
            if "UNDEF" in line:
                undefined.add(symbol)
            else:
                defined.append(symbol)
    else:
        for flag, target in (("--defined-only", defined), ("--undefined-only", undefined)):
            output = subprocess.check_output(["nm", "-g", flag, str(library)], text=True)
            for line in output.splitlines():
                parts = line.split()
                if len(parts) >= 2 and len(parts[-2]) == 1:
                    if isinstance(target, set):
                        target.add(parts[-1])
                    else:
                        target.append(parts[-1])
    text = "\n".join((ROOT / "include" / h).read_text() for h in headers)
    expected = set(re.findall(r"^[A-Za-z_][\w\s*]*\b(af_\w+)\(", text, re.M))
    exports = collections.Counter(s for s in defined if re.fullmatch(r"af_\w+", s))
    if set(exports) != expected or any(n != 1 for n in exports.values()):
        raise RuntimeError(f"archive/header mismatch: missing={expected - exports.keys()}, "
                           f"extra={exports.keys() - expected}, duplicates="
                           f"{[s for s, n in exports.items() if n != 1]}")
    if core:
        # Resolve internal references across archive members first. No unresolved
        # D ABI symbol, runtime entry point, or GC function is allowed in core.
        external = undefined - set(defined)
        forbidden = [s for s in external if re.match(
            r"(?:__imp_)?(?:_D|_d_|_aa|_ad|gc_|rt_|thread_)", s)]
        if forbidden:
            raise RuntimeError("core archive requires D runtime: " + ", ".join(sorted(forbidden)))
    print(f"Archive audit passed: {library.name}, {len(exports)} header exports", flush=True)


def native_runtime(source, build, libraries, compiler, probe=False):
    obj = build / (Path(source).stem + (".obj" if os.name == "nt" else ".o"))
    binary = build / (Path(source).stem + (".exe" if os.name == "nt" else ""))
    if os.name == "nt":
        build_api.msvc(["cl", "/nologo", "/std:c11", "/W4", "/WX", "/MT", "/c",
                        "/Iinclude", "/Itests/cabi", "/DNDEBUG", source, "/Fo" + str(obj)], build)
        extra = ["kernel32.lib", "advapi32.lib"]
    else:
        build_api.run([os.environ.get("CC", "cc"), "-std=c11", "-Wall", "-Wextra", "-Werror",
                       "-Iinclude", "-Itests/cabi", "-DNDEBUG", "-c", source, "-o", obj])
        extra = ["-L-lpthread", "-L-lrt", "-L-latomic"]
    probe_objects = []
    if probe:
        probe_obj = build / ("runtime_probe.obj" if os.name == "nt" else "runtime_probe.o")
        build_api.run([compiler, "-O", "-release", "-I.", "-Icabi/source", "-Ithreadpool/source",
                       "-c", "tests/cabi/runtime_probe.d", "-of=" + str(probe_obj)])
        probe_objects.append(probe_obj)
    build_api.run([compiler, obj, *probe_objects,
                   *libraries, *extra, "-of=" + str(binary)])
    return binary


def runtime_checks(compiler, build, core):
    runtime = build_api.build_runtime(compiler, build)
    audit_symbols(runtime, ["antfarm_runtime.h"], build)
    libraries = [runtime, core]
    binary = native_runtime("tests/cabi/runtime.c", build, libraries, compiler, probe=True)
    execute(binary)
    execute(binary, "borrowed", death="cannot destroy a borrowed host Farm")
    execute(binary, "nested", death="runtime control operation inside Farm callback")
    actors = native_runtime("tests/cabi/actors_host.c", build, libraries, compiler, probe=True)
    execute(actors)
    execute(actors, "borrowed", death="cannot destroy a host-owned actor runtime")
    execute(native_runtime("tests/cabi/private_host.c", build, libraries, compiler, probe=True))
    embedded = build / ("embedded.exe" if os.name == "nt" else "embedded")
    build_api.run([compiler, "-O", "-I.", "-Icabi/source", "-Ithreadpool/source",
                   "tests/cabi/embedded.d", *libraries,
                   *(["kernel32.lib", "advapi32.lib"] if os.name == "nt" else ["-L-lpthread", "-L-lrt", "-L-latomic"]),
                   "-of=" + str(embedded)])
    execute(embedded)
    execute(native_runtime("examples/c_host.c", build, libraries, compiler))


def execute(binary, *args, death=None):
    env = {**os.environ, "ANTFARM_HUGE_PAGES": "0"}
    result = subprocess.run([str(binary), *args], cwd=ROOT, env=env, capture_output=True,
                            text=True, timeout=45)
    output = result.stdout + result.stderr
    print(output, end="", flush=True)
    if death:
        if result.returncode == 0 or "C_FATAL_OBSERVER:" not in output or death not in output:
            raise RuntimeError("missing fatal diagnostic for " + str(args))
    elif result.returncode:
        raise RuntimeError("C ABI probe failed: " + str(binary) + " " + str(result.returncode))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--compiler", choices=("ldc2", "dmd"), default="ldc2")
    selection = p.add_mutually_exclusive_group()
    selection.add_argument("--core-only", action="store_true")
    selection.add_argument("--runtime-only", action="store_true")
    p.add_argument("--tsan", action="store_true", help="Linux LDC native C/core ThreadSanitizer lane")
    args = p.parse_args()
    if args.tsan and (os.name != "posix" or args.compiler != "ldc2" or args.runtime_only):
        p.error("--tsan requires Linux LDC and checks the core")
    build = ROOT / ".build" / ("c-abi-" + args.compiler + "-" + os.name)
    if args.tsan:
        build = build / "tsan"
    lib = build_api.build_core(args.compiler, build, sanitize=args.tsan)
    audit_symbols(lib, ["antfarm.h", "antfarm_actors.h", "antfarm_private.h"], build, core=True)
    if args.tsan:
        for source in ("core", "actors", "waves", "private"):
            obj = build / (source + "-main.o")
            binary = build / (source + "-tsan")
            build_api.run([os.environ.get("CC", "cc"), "-std=c11", "-g", "-O1", "-fsanitize=thread",
                           "-Wall", "-Wextra", "-Werror", "-Iinclude", "-Itests/cabi", "-DNDEBUG",
                           "-c", "tests/cabi/" + source + ".c", "-o", obj])
            build_api.run([args.compiler, "-betterC", "-fsanitize=thread", obj, lib, "-L-lpthread", "-of=" + str(binary)])
            execute(binary)
        for source in ("tests/cabi/cpp.cpp", "examples/cpp_threads.cpp"):
            name = Path(source).stem
            obj, binary = build / (name + "-main.o"), build / (name + "-tsan")
            build_api.run([os.environ.get("CXX", "c++"), "-std=c++11", "-g", "-O1",
                           "-fno-exceptions", "-fsanitize=thread", "-Wall", "-Wextra", "-Werror",
                           "-Iinclude", "-Itests/cabi", "-DNDEBUG", "-DAF_TEST_TSAN",
                           "-include", "tests/cabi/tsan_preinit.h",
                           "-c", source, "-o", obj])
            build_api.run([args.compiler, "-betterC", "-fsanitize=thread", obj, lib,
                           "-L-lstdc++", "-L-lpthread", "-of=" + str(binary)])
            execute(binary)
        print("C ABI TSAN CHECKS PASSED", flush=True)
        return
    if not args.runtime_only:
        # A whole-archive native link also rejects runtime dependencies not
        # recognizable by name, including unused objects and unusual C exports.
        whole = ["/WHOLEARCHIVE:" + str(lib)] if os.name == "nt" else [
            "-Wl,--whole-archive", lib, "-Wl,--no-whole-archive"]
        execute(native("tests/cabi/link_all.c", build, whole))
        binary = native("tests/cabi/core.c", build, [lib])
        execute(binary)
        for name, diagnostic in {
            "callback": "null payload callback", "cost": "avgCost out of range",
            "span": "null nonempty span", "overflow": "fixed body size overflow",
            "entry-overflow": "span size overflow", "hook": "hook requires a callback",
            "foreign": "foreign or null producer token", "farm-live": "live consumers",
            "token-live": "live small ticket",
            "view-live": "destroy subscribed view", "nested": "inside Farm callback",
            "nested-hook": "inside Farm callback",
        }.items():
            execute(binary, name, death=diagnostic)
        actors = native("tests/cabi/actors.c", build, [lib])
        execute(actors)
        for name, diagnostic in {
            "farm": "destroy Farm with actor runtimes", "live": "destroy actor runtime with live owners",
            "foreign": "foreign or null actor producer token", "cost": "avgCost out of range",
            "context": "actor context outside its callback",
            "complete": "complete actor inbox node not owned", "alignment": "allocator returned misaligned",
            "set": "modify busy actor message",
            "failure": "failure code out of range",
        }.items():
            execute(actors, name, death=diagnostic)
        waves = native("tests/cabi/waves.c", build, [lib])
        execute(waves)
        for name, diagnostic in {
            "uninitialized": "publish uninitialized actor wave", "operation": "null actor wave operation",
            "hook": "hook requires a callback", "begin": "reuse unfinished actor wave",
            "destroy": "destroy unfinished actor wave", "span": "invalid wave span",
            "overflow": "invalid wave span", "foreign": "foreign or null wave producer token",
            "cost": "avgCost out of range", "seal": "seal actor wave twice",
            "sealed": "publish sealed actor wave", "farm": "destroy Farm with actor waves",
            "tail-begin": "reuse unfinished actor wave or completion callback",
            "tail-destroy": "destroy unfinished actor wave or completion callback",
        }.items():
            execute(waves, name, death=diagnostic)
        private = native("tests/cabi/private.c", build, [lib])
        execute(private)
        for name, diagnostic in {
            "farm": "destroy Farm with private parks", "live": "destroy nonempty private root park",
            "cost": "avgCost out of range", "outcome": "untracked park",
            "disposition": "invalid private root disposition", "fail": "private root fail: untracked park",
            "context": "private context outside its callback", "init-context": "private context outside its callback",
            "authority": "unauthorized context", "subset": "duplicate member",
            "early": "wave active", "leaf": "private leaf cannot suspend",
        }.items():
            execute(private, name, death=diagnostic)
        execute(native("tests/cabi/private_cpp.cpp", build, [lib]))
        execute(native("tests/cabi/private_cpp.cpp", build, [lib], no_exceptions=True))
        debug = build / "private-debug"
        debug_lib = build_api.build_core(args.compiler, debug, debug=True)
        execute(native("tests/cabi/private.c", debug, [debug_lib]))
        execute(native("tests/cabi/cpp.cpp", build, [lib]))
        execute(native("tests/cabi/cpp.cpp", build, [lib], no_exceptions=True))
        execute(native("tests/cabi/cpp_failures.cpp", build, []))
        execute(native("tests/cabi/cpp_failures.cpp", build, [], no_exceptions=True))
        execute(native("examples/c_core.c", build, [lib]))
        execute(native("examples/cpp_threads.cpp", build, [lib]))
        execute(native("examples/c_actors.c", build, [lib]))
        execute(native("examples/c_wave.c", build, [lib]))
        execute(native("examples/c_private.c", build, [lib]))
        audit = build / "audit"
        audit_lib = build_api.build_core(args.compiler, audit, audit=True)
        execute(native("tests/cabi/core.c", audit, [audit_lib], defines=("AF_TEST_AUDIT",)))
        execute(native("tests/cabi/waves.c", audit, [audit_lib], defines=("AF_TEST_AUDIT",)))
        execute(native("tests/cabi/actors.c", audit, [audit_lib], defines=("AF_TEST_ACTOR_AUDIT",)))
    if not args.core_only:
        runtime_checks(args.compiler, build, lib)
    print("C ABI CHECKS PASSED", flush=True)


if __name__ == "__main__":
    main()
