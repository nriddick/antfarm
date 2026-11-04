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
                       "cabi/source/antfarm/actors/private_cabi.d",
                       "cabi/source/antfarm/c/api.d"]
DECLARATION_HEADERS = ["antfarm.h", "antfarm_runtime.h", "antfarm_actors.h", "antfarm_private.h", "antfarm_forkjoin.h", "antfarm_groups.h",
                       "antfarm_api.h"]
# The ABI 2 function table (antfarm_api.h, and the D sections behind it) is
# generated from the other headers: one section per header, one member per
# function in header order, named without the af_ prefix. The core fills its
# three sections; the runtime archive installs the other three at
# af_druntime_init. --write-api-table regenerates the three files; the audit
# fails when they differ from what the headers say.
API_SECTIONS = [("core", "antfarm.h"), ("actors", "antfarm_actors.h"), ("private_roots", "antfarm_private.h"),
                ("runtime", "antfarm_runtime.h"), ("forkjoin", "antfarm_forkjoin.h"), ("groups", "antfarm_groups.h")]
API_CORE_SECTIONS = {"core", "actors", "private_roots"}
API_FILES = {"header": ROOT / "include/antfarm_api.h", "core": ROOT / "cabi/source/antfarm/c/api.d",
             "runtime": ROOT / "cabi/source/antfarm/c/api_runtime.d"}
API_STAMP = "GENERATED by scripts/check-c-abi.py --write-api-table from the headers; do not edit."
# Header struct -> D type, where the D spelling differs.
D_STRUCT_NAMES = {"af_completion_hook": "TableCompletionHook"}
# Header structs that are deliberately opaque storage (C sees padding words, D
# a typed value): size and alignment are compared, fields are not.
OPAQUE_STRUCTS = {"af_actor_message", "af_private_root_handle"}
# The ABI freezes as tests: header prototypes, numeric constants and the
# layout rows above. The audit proves header == D; the locks prove neither
# moved. abi2.lock is the current surface, exact. abi1.lock is ABI 1's, and
# every row of it but the version constant must still be present verbatim:
# ABI 2 is a superset.
ABI_LOCK = ROOT / "cabi" / "abi2.lock"
ABI_LOCK_PREVIOUS = ROOT / "cabi" / "abi1.lock"
ABI_LOCK_EXEMPT = {"CONST AF_ABI_VERSION"}
# Versioned option structures that grew in ABI 2. Their ABI 1 images are
# accepted by their own size and version, and frozen in
# tests/cabi/abi1_images.h, which the runtime and group tests fill; the lock
# cannot see that acceptance, so their layout rows are exempt here. Every
# other layout row of ABI 1 must survive verbatim.
ABI_LOCK_GROWN = {"af_host_options", "af_group_options"}
ABI_LOCK_HEADER = """# C ABI 2 lock: header prototypes, numeric constants and struct layouts.
# The declaration audit in scripts/check-c-abi.py fails on any difference.
# Within ABI 2 only additions are compatible; regenerate deliberately with
# scripts/check-c-abi.py --write-abi-lock and review the diff. Every row of
# abi1.lock but the version constant must remain here verbatim.
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


def section_prototypes(header):
    """(name, return type, parameter list) per function of one header, in header order."""
    prototypes, _ = header_abi((ROOT / "include" / header).read_text())
    rows = []
    for name, text in prototypes.items():
        match = re.match(r"^(.*?)\b(af_\w+)(\(.*\))$", text)
        rows.append((name, match.group(1).strip(), match.group(3)))
    return rows


def generate_api():
    """The table header and its two D sides, as text keyed like API_FILES."""
    header = ["/* Ant Farm C ABI 2 function table. " + API_STAMP,
              "   One section per header, one member per function in header order, named",
              "   without the af_ prefix, after a size word: within ABI 2 sections are",
              "   append-only, so check a section's size before a member appended after",
              "   your build. af_api_get returns the library's table, static and never",
              "   freed; a resident may hand a module a copy with sections set to NULL to",
              "   withhold them. The runtime, forkjoin and groups sections are NULL until",
              "   af_druntime_init has succeeded; fibers and uv are reserved. A module that",
              "   calls only through the table imports nothing from the executable, which",
              "   is what a Windows DLL needs; the table is the shared-library story for",
              "   modules, not for the library. */",
              "#ifndef ANTFARM_API_H", "#define ANTFARM_API_H", '#include "antfarm_groups.h"',
              "#ifdef __cplusplus", 'extern "C" {', "#endif"]
    core = ["// " + API_STAMP,
            "// The ABI 2 function table: one section per header, one member per function",
            "// in header order. The core sections are filled here; the runtime archive",
            "// installs its sections at af_druntime_init (api_runtime.d).",
            "module antfarm.c.api;", "import antfarm.c.core;", "import antfarm.actors.cabi;",
            "import antfarm.actors.private_cabi;", ""]
    runtime = ["// " + API_STAMP,
               "// The runtime archive's sections of the ABI 2 function table, installed",
               "// into the core's table at af_druntime_init.",
               "module antfarm.c.api_runtime;",
               "import antfarm.c.api : af_api_runtime, af_api_forkjoin, af_api_groups, installRuntimeApi;",
               "import antfarm.c.runtime;", "import antfarm.c.forkjoin;", "import antfarm.c.groups;", ""]
    for section, name in API_SECTIONS:
        rows = section_prototypes(name)
        header.append(f"typedef struct af_api_{section} {{")
        header.append("    size_t size;")
        for function, ret, params in rows:
            header.append(f"    {ret} (*{function[3:]}){params};")
        header.append(f"}} af_api_{section};")
        in_core = section in API_CORE_SECTIONS
        core.append(f"struct af_api_{section}")
        core.append("{")
        core.append("    size_t size;")
        for function, ret, params in rows:
            core.append(f"    {'typeof(&' + function + ')' if in_core else 'void*'} {function[3:]};")
        core.append("}")
        addresses = ", ".join("&" + function for function, _, _ in rows)
        if in_core:
            core.append(f"private static immutable af_api_{section} {section}Section = af_api_{section}(")
            core.append(f"    af_api_{section}.sizeof, {addresses});")
        else:
            typed = section.capitalize() + "Section"
            runtime.append(f"private struct {typed}")
            runtime.append("{")
            runtime.append("    size_t size;")
            for function, ret, params in rows:
                runtime.append(f"    typeof(&{function}) {function[3:]};")
            runtime.append("}")
            runtime.append(f"static assert({typed}.sizeof == af_api_{section}.sizeof);")
            runtime.append(f"private static immutable {typed} {section}Section = {typed}(")
            runtime.append(f"    {typed}.sizeof, {addresses});")
        core.append("")
    header += ["typedef struct af_api {", "    size_t size;", "    uint32_t abi_version, reserved;",
               "    const af_api_core *core;", "    const af_api_actors *actors;",
               "    const af_api_private_roots *private_roots;",
               "    const af_api_runtime *runtime;   /* NULL until af_druntime_init succeeded */",
               "    const af_api_forkjoin *forkjoin;", "    const af_api_groups *groups;",
               "    const void *fibers;              /* reserved */", "    const void *uv;                  /* reserved */",
               "    const void *reserved_sections[6];", "} af_api;",
               "const af_api *af_api_get(void);", "#ifdef __cplusplus", "}", "#endif", "#endif", ""]
    core += ["struct af_api", "{", "    size_t size;", "    uint abi_version, reserved;",
             "    const(af_api_core)* core;", "    const(af_api_actors)* actors;",
             "    const(af_api_private_roots)* private_roots;", "    const(af_api_runtime)* runtime;",
             "    const(af_api_forkjoin)* forkjoin;", "    const(af_api_groups)* groups;",
             "    const(void)* fibers;", "    const(void)* uv;", "    const(void)*[6] reserved_sections;", "}",
             "static assert(af_api.sizeof == 128 && af_api.core.offsetof == 16);",
             "private __gshared af_api table = af_api(af_api.sizeof, AF_ABI_VERSION, 0,",
             "    &coreSection, &actorsSection, &private_rootsSection, null, null, null, null, null,",
             "    [null, null, null, null, null, null]);",
             "// The runtime archive, at af_druntime_init: a setup operation on the control thread.",
             "void installRuntimeApi(const(af_api_runtime)* runtime, const(af_api_forkjoin)* forkjoin,",
             "    const(af_api_groups)* groups) nothrow @nogc @system",
             "{", "    table.runtime = runtime;", "    table.forkjoin = forkjoin;", "    table.groups = groups;", "}",
             "extern(C) const(af_api)* af_api_get() nothrow @nogc @system { return &table; }", ""]
    runtime += ["package void installRuntimeSections() nothrow @nogc @system", "{",
                "    installRuntimeApi(cast(const(af_api_runtime)*) &runtimeSection,",
                "        cast(const(af_api_forkjoin)*) &forkjoinSection, cast(const(af_api_groups)*) &groupsSection);",
                "}", ""]
    return {"header": "\n".join(header), "core": "\n".join(core), "runtime": "\n".join(runtime)}


def check_api_table():
    expected = generate_api()
    stale = [str(API_FILES[k].relative_to(ROOT)) for k, text in expected.items()
             if not API_FILES[k].exists() or API_FILES[k].read_text().replace("\r\n", "\n") != text]
    if stale:
        raise RuntimeError("the ABI 2 function table differs from the headers: " + ", ".join(stale)
                           + "; regenerate with scripts/check-c-abi.py --write-api-table")
    print("API table matched the headers: " + ", ".join(k for k in expected), flush=True)


def write_api_table():
    for key, text in generate_api().items():
        API_FILES[key].write_text(text, newline="\n")
        print(f"API table written: {API_FILES[key].relative_to(ROOT)}", flush=True)


def lock_key(line):
    """The identity a lock row describes, so a change reads as one row, not two."""
    kind, rest = line.split(" ", 1)
    if kind == "PROTO":
        return kind + " " + re.search(r"\b(af_\w+)\(", rest).group(1)
    words = {"CONST": 2, "LAYOUT": 4 if rest.startswith("F ") else 3}[kind]
    return " ".join(line.split()[:words])


def lock_rows(path):
    if not path.exists():
        raise RuntimeError(f"{path.relative_to(ROOT)} is missing; see --write-abi-lock")
    return [line for line in path.read_text().splitlines() if line and not line.startswith("#")]


def check_abi_lock(rows, write):
    # ABI 1 first: its rows must survive verbatim whatever the current lock says.
    current = {lock_key(line): line for line in rows}
    previous = lock_rows(ABI_LOCK_PREVIOUS)
    report = []
    for line in previous:
        key = lock_key(line)
        if key in ABI_LOCK_EXEMPT or (key.startswith("LAYOUT ") and key.split()[2] in ABI_LOCK_GROWN):
            continue
        if key not in current:
            report.append(f"removed: {line!r}")
        elif current[key] != line:
            report.append(f"changed: {line!r} -> {current[key]!r}")
    if report:
        raise RuntimeError("C ABI 2 breaks ABI 1 rows of " + str(ABI_LOCK_PREVIOUS.relative_to(ROOT))
                           + " (ABI 2 must keep every ABI 1 prototype, constant and layout):\n"
                           + "\n".join(report))
    print(f"ABI 1 lock honoured: {len(previous)} rows", flush=True)
    if write:
        ABI_LOCK.write_text(ABI_LOCK_HEADER + "\n".join(rows) + "\n", newline="\n")
        print(f"ABI lock written: {ABI_LOCK.relative_to(ROOT)}, {len(rows)} rows", flush=True)
        return
    locked = lock_rows(ABI_LOCK)
    if locked == rows:
        print(f"ABI 2 lock matched: {len(rows)} rows", flush=True)
        return
    old = {lock_key(line): line for line in locked}
    new = {lock_key(line): line for line in rows}
    report = [f"changed: {old[k]!r} -> {new[k]!r}" for k in old if k in new and old[k] != new[k]]
    report += [f"removed: {old[k]!r}" for k in old if k not in new]
    report += [f"added:   {new[k]!r}" for k in new if k not in old]
    if not report:
        report = ["row order changed"]
    raise RuntimeError("C ABI 2 differs from " + str(ABI_LOCK.relative_to(ROOT))
                       + " (only additions are ABI-2-compatible; regenerate with --write-abi-lock"
                       " only for those):\n" + "\n".join(report))


def audit_declarations(compiler, build, write_lock=False):
    """Compare every header prototype and struct layout with the D definitions,
    then the header-side result with the committed ABI 1 lock."""
    work = build / "declarations"
    work.mkdir(parents=True, exist_ok=True)
    check_api_table()
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


def native_shared(source, build):
    """A shared library that must resolve every symbol from itself and libc: a
    module that calls Ant Farm only through the table."""
    name = Path(source).stem
    if os.name == "nt":
        binary = build / (name + ".dll")
        build_api.msvc(["cl", "/nologo", "/std:c11", "/W4", "/WX", "/MT", "/LD", "/Iinclude", "/Itests/cabi",
                        "/DNDEBUG", source, "/Fo" + str(build / (name + ".obj")), "/Fe" + str(binary),
                        "/link", "kernel32.lib"], build)
    else:
        binary = build / ("lib" + name + ".so")
        build_api.run([os.environ.get("CC", "cc"), "-std=c11", "-Wall", "-Wextra", "-Werror", "-shared", "-fPIC",
                       "-Wl,--no-undefined", "-Iinclude", "-Itests/cabi", "-DNDEBUG", source, "-o", binary])
    return binary


def native_runtime(source, build, libraries, compiler, probe=False, system=()):
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
                       "-Iforkjoin/source", "-c", "tests/cabi/runtime_probe.d", "-of=" + str(probe_obj)])
        probe_objects.append(probe_obj)
    build_api.run([compiler, obj, *probe_objects,
                   *libraries, *extra, *system, "-of=" + str(binary)])
    return binary


def runtime_checks(compiler, build, core):
    runtime = build_api.build_runtime(compiler, build)
    audit_symbols(runtime, ["antfarm_runtime.h", "antfarm_forkjoin.h", "antfarm_groups.h"], build)
    libraries = [runtime, core]
    binary = native_runtime("tests/cabi/runtime.c", build, libraries, compiler, probe=True)
    execute(binary)
    execute(binary, "borrowed", death="cannot destroy a borrowed host Farm")
    execute(binary, "nested", death="runtime control operation inside Farm callback")
    execute(binary, "error", death="D Error inside an export: injected D Error")
    execute(binary, "gc-callback", death="GC control inside Farm callback")
    execute(binary, "emit-control", death="control operation inside runtime callback")
    actors = native_runtime("tests/cabi/actors_host.c", build, libraries, compiler, probe=True)
    execute(actors)
    execute(actors, "borrowed", death="cannot destroy a host-owned actor runtime")
    private_host = native_runtime("tests/cabi/private_host.c", build, libraries, compiler, probe=True)
    execute(private_host)
    for case in ("pump-transferred", "destroy-transferred"):
        execute(private_host, case, death="private root park transferred to a host")
    embedded = build / ("embedded.exe" if os.name == "nt" else "embedded")
    build_api.run([compiler, "-O", "-I.", "-Icabi/source", "-Ithreadpool/source", "-Iforkjoin/source",
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
    module = native_shared("tests/cabi/api_module.c", build)
    table = native_runtime("tests/cabi/api_host.c", build, libraries, compiler,
                           system=() if os.name == "nt" else ("-L-ldl",))
    execute(table, str(module))


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
                   help="regenerate cabi/abi2.lock after the declaration audit, then stop")
    p.add_argument("--write-api-table", action="store_true",
                   help="regenerate include/antfarm_api.h and the D sections from the headers, then stop")
    args = p.parse_args()
    if args.write_api_table:
        write_api_table()
        return
    if args.tsan and (os.name != "posix" or args.compiler != "ldc2" or args.runtime_only):
        p.error("--tsan requires Linux LDC and checks the core")
    if args.tsan and args.write_abi_lock:
        p.error("--write-abi-lock runs the declaration audit, which the TSan lane skips")
    build = ROOT / ".build" / ("c-abi-" + args.compiler + "-" + os.name)
    if args.tsan:
        build = build / "tsan"
    lib = build_api.build_core(args.compiler, build, sanitize=args.tsan)
    audit_symbols(lib, ["antfarm.h", "antfarm_actors.h", "antfarm_private.h", "antfarm_api.h"], build, core=True)
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
        execute(native("tests/cabi/api.c", build, [lib]))
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
