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


# Header <-> D declaration audit. audit_symbols proves the names match; this
# proves the types do. The D side comes from the compiler itself: -HC writes C++
# prototypes for every extern(C) function, and a pragma(msg) module dumps each
# struct's size, alignment and field offsets. One C++ program then checks every
# prototype with static_assert and prints the same layout from the headers.
DECLARATION_MODULES = ["cabi/source/antfarm/c/core.d", "cabi/source/antfarm/c/runtime.d",
                       "cabi/source/antfarm/c/forkjoin.d", "cabi/source/antfarm/c/groups.d",
                       "cabi/source/antfarm/actors/cabi.d",
                       "cabi/source/antfarm/actors/private_cabi.d"]
DECLARATION_HEADERS = ["antfarm.h", "antfarm_runtime.h", "antfarm_actors.h", "antfarm_private.h", "antfarm_forkjoin.h", "antfarm_groups.h"]
# Header struct -> D type, where the D spelling differs.
D_STRUCT_NAMES = {"af_completion_hook": "TableCompletionHook"}
# Header structs that are deliberately opaque storage (C sees padding words, D
# a typed value): size and alignment are compared, fields are not.
OPAQUE_STRUCTS = {"af_actor_message", "af_private_root_handle"}
# The ABI 1 freeze as a test: header prototypes, numeric constants and the
# layout rows above. The audit proves header == D; the lock proves neither moved.
ABI_LOCK = ROOT / "cabi" / "abi1.lock"
ABI_LOCK_HEADER = """# C ABI 1 lock: header prototypes, numeric constants and struct layouts.
# The declaration audit in scripts/check-c-abi.py fails on any difference.
# Within ABI 1 only additions are compatible; regenerate deliberately with
# scripts/check-c-abi.py --write-abi-lock and review the diff.
"""
DECLARATION_TRAITS = r"""
#include <cstddef>
#include <cstdio>
#include <type_traits>
template <class... T> struct af_list {};
// Same C ABI type. Beyond identity: integers of equal size and signedness
// (D spells size_t callback parameters as uint64_t, which is a distinct C++
// type on some LP64 targets) and D's erased handles, where void* stands for a
// typed opaque pointer with the same qualification. Function pointers compare
// return and parameter types recursively.
template <class C, class D, bool = std::is_integral<C>::value && std::is_integral<D>::value>
struct af_integer_same : std::false_type {};
template <class C, class D> struct af_integer_same<C, D, true> : std::integral_constant<bool,
    !std::is_same<C, bool>::value && !std::is_same<D, bool>::value
    && sizeof(C) == sizeof(D) && std::is_signed<C>::value == std::is_signed<D>::value> {};
template <class C, class D> struct af_abi_same : std::integral_constant<bool,
    std::is_same<C, D>::value || af_integer_same<C, D>::value> {};
template <class C, class D> struct af_pointee_same : af_abi_same<C, D> {};
template <class T> struct af_pointee_same<T, void>
    : std::integral_constant<bool, !std::is_function<T>::value> {};
template <class C, class D> struct af_abi_same<C *, D *> : std::integral_constant<bool,
    std::is_const<C>::value == std::is_const<D>::value
    && af_pointee_same<typename std::remove_const<C>::type,
                       typename std::remove_const<D>::type>::value> {};
template <class C, class D> struct af_params_same : std::false_type {};
template <> struct af_params_same<af_list<>, af_list<>> : std::true_type {};
template <class C, class... Cs, class D, class... Ds>
struct af_params_same<af_list<C, Cs...>, af_list<D, Ds...>> : std::integral_constant<bool,
    af_abi_same<C, D>::value && af_params_same<af_list<Cs...>, af_list<Ds...>>::value> {};
template <class CR, class... CA, class DR, class... DA>
struct af_abi_same<CR (*)(CA...), DR (*)(DA...)> : std::integral_constant<bool,
    af_abi_same<CR, DR>::value && af_params_same<af_list<CA...>, af_list<DA...>>::value> {};
"""


def d_option(compiler, name):
    return ("-" if compiler == "dmd" else "--") + name


def header_abi(headers):
    """Normalized prototypes and numeric constant names, as a C consumer sees them."""
    prototypes = {}
    for match in re.finditer(r"^([A-Za-z_][\w \t*]*\b(af_\w+)\(.*?\));", headers, re.M | re.S):
        text = " ".join(match.group(1).split())
        for pattern, replacement in ((r"\s*(\*+)\s*", r" \1"), (r"\(\s+", "("),
                                     (r"\s+\)", ")"), (r"\s*,\s*", ", ")):
            text = re.sub(pattern, replacement, text)
        prototypes[match.group(2)] = text
    constants = set(re.findall(r"^#define (AF_\w+)[ \t]", headers, re.M))
    for body in re.findall(r"\benum\s*\{(.*?)\}", headers, re.S):
        constants.update(re.findall(r"\b(AF_\w+)\s*=", body))
    return prototypes, sorted(constants)


def lock_key(line):
    """The identity a lock row describes, so a change reads as one row, not two."""
    kind, rest = line.split(" ", 1)
    if kind == "PROTO":
        return kind + " " + re.search(r"\b(af_\w+)\(", rest).group(1)
    words = {"CONST": 2, "LAYOUT": 4 if rest.startswith("F ") else 3}[kind]
    return " ".join(line.split()[:words])


def check_abi_lock(rows, write):
    if write:
        ABI_LOCK.write_text(ABI_LOCK_HEADER + "\n".join(rows) + "\n", newline="\n")
        print(f"ABI lock written: {ABI_LOCK.relative_to(ROOT)}, {len(rows)} rows", flush=True)
        return
    if not ABI_LOCK.exists():
        raise RuntimeError(f"{ABI_LOCK.relative_to(ROOT)} is missing; see --write-abi-lock")
    locked = [line for line in ABI_LOCK.read_text().splitlines() if line and not line.startswith("#")]
    if locked == rows:
        print(f"ABI lock matched: {len(rows)} rows", flush=True)
        return
    old = {lock_key(line): line for line in locked}
    new = {lock_key(line): line for line in rows}
    report = [f"changed: {old[k]!r} -> {new[k]!r}" for k in old if k in new and old[k] != new[k]]
    report += [f"removed: {old[k]!r}" for k in old if k not in new]
    report += [f"added:   {new[k]!r}" for k in new if k not in old]
    if not report:
        report = ["row order changed"]
    raise RuntimeError("C ABI 1 differs from " + str(ABI_LOCK.relative_to(ROOT))
                       + " (only additions are ABI-1-compatible; regenerate with --write-abi-lock"
                       " only for those):\n" + "\n".join(report))


def audit_declarations(compiler, build, write_lock=False):
    """Compare every header prototype and struct layout with the D definitions,
    then the header-side result with the committed ABI 1 lock."""
    work = build / "declarations"
    work.mkdir(parents=True, exist_ok=True)
    includes = ["-I.", "-Icabi/source", "-Ithreadpool/source", "-Iforkjoin/source"]
    generated = work / "d_prototypes.h"
    build_api.run([compiler, "-o-", *includes, d_option(compiler, "HC"),
                   d_option(compiler, "HCf=") + str(generated), *DECLARATION_MODULES])
    headers = "\n".join((ROOT / "include" / h).read_text() for h in DECLARATION_HEADERS)
    expected = set(re.findall(r"^[A-Za-z_][\w\s*]*\b(af_\w+)\(", headers, re.M))
    prototypes, erased = {}, 0
    for line in generated.read_text().splitlines():
        match = re.match(r'extern "C" (.*?)\b(af_\w+)\((.*)\);$', line.strip())
        if match and match.group(2) in expected:
            # -HC cannot name a template instance and prints a bare `*`. Such a
            # parameter is checked only as a pointer of the same constness.
            params = re.sub(r"(^|,\s*)\*", r"\1void*", match.group(3))
            erased += params != match.group(3)
            prototypes[match.group(2)] = (match.group(1).strip(), params or "void")
    missing = sorted(expected - prototypes.keys())
    if missing:
        raise RuntimeError("header functions without a D extern(C) definition: " + ", ".join(missing))

    structs = sorted(set(re.findall(r"^typedef struct (af_\w+) \{", headers, re.M)))
    layout = ["module af_layout;", "import antfarm;",
              *("import " + m[len("cabi/source/"):-2].replace("/", ".") + ";" for m in DECLARATION_MODULES),
              "template Dump(T, string name, bool fields) {",
              '    pragma(msg, "AFLAYOUT S ", name, " ", cast(int) T.sizeof, " ", cast(int) T.alignof);',
              "    static if (fields) static foreach (i; 0 .. T.tupleof.length)",
              '        pragma(msg, "AFLAYOUT F ", name, " ", __traits(identifier, T.tupleof[i]), " ",',
              "            cast(int) T.tupleof[i].offsetof, \" \", cast(int) typeof(T.tupleof[i]).sizeof);",
              "    enum Dump = true;", "}"]
    for name in structs:
        layout.append(f'enum dump_{name} = Dump!({D_STRUCT_NAMES.get(name, name)}, "{name}", '
                      f'{"false" if name in OPAQUE_STRUCTS else "true"});')
    (work / "af_layout.d").write_text("\n".join(layout) + "\n")
    result = subprocess.run([compiler, "-o-", *includes, str(work / "af_layout.d")], cwd=ROOT,
                            capture_output=True, text=True, timeout=180)
    if result.returncode:
        raise RuntimeError("D layout dump failed:\n" + result.stdout + result.stderr)
    d_layout = [line for line in (result.stdout + result.stderr).splitlines()
                if line.startswith("AFLAYOUT ")]

    program = [*("#include \"" + h + "\"" for h in DECLARATION_HEADERS), DECLARATION_TRAITS,
               "namespace d_side {", "typedef af_completion_hook TableCompletionHook;"]
    for name, (ret, params) in sorted(prototypes.items()):
        program.append(f"typedef {ret} (*{name}_t)({params});")
        program.append(f'static_assert(af_abi_same<decltype(&::{name}), {name}_t>::value, '
                       f'"{name}: header prototype differs from the D definition");')
    header_prototypes, constants = header_abi(headers)
    if header_prototypes.keys() != expected:
        raise RuntimeError("lock prototype scan disagrees with the header function scan: "
                           + ", ".join(sorted(header_prototypes.keys() ^ expected)))
    program +=["}", "int main() {"]
    for name in constants:
        program.append(f'std::printf("CONST {name} %llu\\n", (unsigned long long) {name});')
    for line in d_layout:
        kind, name = line.split()[1:3]
        if kind == "S":
            program.append(f'std::printf("AFLAYOUT S {name} %d %d\\n", (int) sizeof({name}), '
                           f"(int) alignof({name}));")
        else:
            field = line.split()[3]
            program.append(f'std::printf("AFLAYOUT F {name} {field} %d %d\\n", '
                           f"(int) offsetof({name}, {field}), (int) sizeof((({name} *) 0)->{field}));")
    program += ["return 0;", "}"]
    source = work / "declarations.cpp"
    source.write_text("\n".join(program) + "\n")
    binary = native(str(source.relative_to(ROOT)), work, [])
    output = subprocess.run([str(binary)], cwd=ROOT, capture_output=True, text=True,
                            timeout=45, check=True).stdout.splitlines()
    constant_rows = [line for line in output if line.startswith("CONST ")]
    output = [line for line in output if not line.startswith("CONST ")]
    if output != d_layout:
        diff = [f"D {d!r} / C {c!r}" for d, c in zip(d_layout, output) if d != c]
        raise RuntimeError("header/D struct layout mismatch:\n" + "\n".join(diff or [
            f"{len(d_layout)} D rows vs {len(output)} C rows"]))
    print(f"Declaration audit passed: {len(prototypes)} prototypes ({erased} with erased handle "
          f"parameters), {len(structs)} structs, {len(d_layout) - len(structs)} fields", flush=True)
    check_abi_lock([*("PROTO " + header_prototypes[n] for n in sorted(header_prototypes)),
                    *constant_rows, *("LAYOUT " + line[len("AFLAYOUT "):] for line in output)],
                   write_lock)


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
    audit_symbols(runtime, ["antfarm_runtime.h", "antfarm_forkjoin.h", "antfarm_groups.h"], build)
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
    execute(native_runtime("tests/cabi/groups.c", build, libraries, compiler))
    forkjoin = native_runtime("tests/cabi/forkjoin.c", build, libraries, compiler)
    execute(forkjoin)
    for case, message in (("zero", "invalid children span"), ("self", "self child"),
                          ("double", "current uncompleted run callback"),
                          ("outside", "current uncompleted run callback"),
                          ("join", "current uncompleted run callback"),
                          ("noreturn", "run returned without af_complete or af_fork"),
                          ("stale", "run must return the value af_fork/af_complete returned"),
                          ("joinvalue", "join without af_fork must return NULL"),
                          ("leafcomplete", "current uncompleted run callback"),
                          ("stop", "control operation inside runtime callback"),
                          ("destroy", "stopped dispatcher"),
                          ("uninitialized", "node must be initialized")):
        execute(forkjoin, case, death=message)
    execute(native_runtime("cabi/examples/forkjoin.c", build, libraries, compiler))


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
    p.add_argument("--write-abi-lock", action="store_true",
                   help="regenerate cabi/abi1.lock after the declaration audit, then stop")
    args = p.parse_args()
    if args.tsan and (os.name != "posix" or args.compiler != "ldc2" or args.runtime_only):
        p.error("--tsan requires Linux LDC and checks the core")
    if args.tsan and args.write_abi_lock:
        p.error("--write-abi-lock runs the declaration audit, which the TSan lane skips")
    build = ROOT / ".build" / ("c-abi-" + args.compiler + "-" + os.name)
    if args.tsan:
        build = build / "tsan"
    lib = build_api.build_core(args.compiler, build, sanitize=args.tsan)
    audit_symbols(lib, ["antfarm.h", "antfarm_actors.h", "antfarm_private.h"], build, core=True)
    if not args.tsan:
        audit_declarations(args.compiler, build, args.write_abi_lock)
        if args.write_abi_lock:
            return
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
