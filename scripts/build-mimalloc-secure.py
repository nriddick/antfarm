#!/usr/bin/env python3
"""Build the pinned mimalloc a second time as a secure instance with its own names.

The secure instance is mimalloc built with MI_SECURE=CHECKED (pointers passed
to free are validated, free lists are encoded and randomized, and metadata is
fenced by guard pages) and MI_SECURE_NO_STATIC_GUARD (no guard pages inside the
static process data, which the D garbage collector scans). Every external
symbol is renamed with a prefix (afsec_ by default), so the archive links
beside the ordinary (fast) mimalloc archive in one image, each instance with
its own heaps, arenas, thread state and options.

Steps, all checked; any surprise stops the build:

1. Build a probe archive with the secure settings and read its defined
   external symbols from the object files themselves (ELF or COFF, no nm or
   dumpbin needed). Every name must be a mimalloc name (`mi_*`, `_mi_*`;
   for MSVC C++ objects, the identifier inside the mangled name).
2. Write prefix.h, `#define name prefixNAME` for each, so a forced include
   renames every definition and every reference at preprocessing time.
3. Copy the source tree and rewrite linker directives that name a symbol in a
   string (`#pragma comment(linker, "/INCLUDE:_mi_tls_callback_pre")` on
   Windows), which the preprocessor cannot reach. Without this the secure
   instance's thread-attach and thread-detach callbacks would not be kept.
4. Build the copy with the forced include and check the result: every
   defined external carries the prefix, no object refers to an unprefixed
   mimalloc name, and nothing collides with the fast archive (--fast-archive).

The output is <build-dir>/libmimalloc-secure.a (mimalloc-secure.lib from
MSVC), with prefix.h, rename-table.txt and report.json beside it. The last
line printed summarizes the counts.

usage: build-mimalloc-secure.py --build-dir DIR [--source third_party/mimalloc]
           [--config Release] [--secure CHECKED] [--prefix afsec_]
           [--fast-archive PATH] [--cmake cmake] [--cmake-arg ARG ...]
           [--forced-include-style auto|gnu|msvc] [--allow REGEX ...]
"""
import argparse, json, os, pathlib, re, shutil, struct, subprocess, sys

P = pathlib.Path
ROOT = P(__file__).resolve().parent.parent

# Defined externals shared harmlessly by both instances: the weak C++ new
# handler mimalloc defines for C builds, and MSVC's COMDAT constants (string
# literals, floating-point and vector constants), which the linker folds by
# content. Anything else that is not a mimalloc name stops the build.
SHARED_OK = [r'_ZSt15get_new_handlerv', r'\?\?_C@.*', r'__real@[0-9a-f]+', r'__xmm@[0-9a-f]+',
             r'__ymm@[0-9a-f]+', r'__zmm@[0-9a-f]+']
# Inline definitions from the Windows SDK and UCRT headers (winnt.h's
# NtCurrentTeb, time.h's time, the vcruntime ISA default), which MSVC emits
# as select-any COMDATs in every object that uses them. Accepted only from a
# COFF object and only in a COMDAT section with IMAGE_COMDAT_SELECT_ANY, where
# the linker keeps one copy; the same name defined any other way stops the
# build like any unknown definition.
COFF_HEADER_COMDATS = {'NtCurrentTeb', 'time', '__isa_available_default'}
MI_NAME = re.compile(r'_*mi_\w+')
MSVC_MANGLED = re.compile(r'\?(_*mi_\w+)@@')
COMMON = ['-DMI_OVERRIDE=OFF', '-DMI_BUILD_SHARED=OFF', '-DMI_BUILD_STATIC=ON', '-DMI_BUILD_OBJECT=OFF',
          '-DMI_BUILD_TESTS=OFF', '-DMI_OPT_ARCH=OFF', '-DMI_NO_LIB_SUFFIX=ON',
          '-DMI_SECURE_NO_STATIC_GUARD=ON',
          # Plain C everywhere. mimalloc compiles as C++ under MSVC and clang-cl,
          # which defines a std::atomic template instance for every mimalloc
          # type (COMDATs the check would have to accept wholesale); its C
          # path uses the MSVC interlocked intrinsics. GCC and Clang build C
          # by default, so this changes nothing there.
          '-DMI_USE_CXX=OFF']


def fail(msg):
    sys.exit(f'build-mimalloc-secure: {msg}')


# --- archives and object files ------------------------------------------------

def archive_members(path):
    """(name, bytes) of each object in a GNU or MSVC (COFF) ar archive."""
    data = P(path).read_bytes()
    if not data.startswith(b'!<arch>\n'):
        fail(f'{path}: not an ar archive')
    pos, longnames, out = 8, b'', []
    while pos + 60 <= len(data):
        hdr = data[pos:pos + 60]
        name = hdr[:16].decode('latin-1').rstrip()
        size = int(hdr[48:58].decode('latin-1').strip())
        body = data[pos + 60:pos + 60 + size]
        pos += 60 + size + (size & 1)
        if name == '//':
            longnames = body
            continue
        if name in ('/', '/SYM64/', '/<ECSYMBOLS>/', '/<XFGHASHMAP>/') or name.startswith('__.SYMDEF'):
            continue
        if name.startswith('/') and name[1:].isdigit():  # long name, GNU or MSVC
            off = int(name[1:])
            end = min(i for i in (longnames.find(b'\n', off), longnames.find(b'\0', off), len(longnames)) if i >= 0)
            name = longnames[off:end].decode('latin-1').rstrip('/')
        out.append((name.rstrip('/'), body))
    return out


def elf_symbols(obj):
    """(name, defined, select_any) for each global or weak symbol of a 64-bit little-endian ELF object.

    select_any is always False: the COMDAT allowance is for COFF only."""
    if obj[4] != 2 or obj[5] != 1:
        raise ValueError('only 64-bit little-endian ELF is supported')
    shoff, = struct.unpack_from('<Q', obj, 0x28)
    shentsize, shnum = struct.unpack_from('<HH', obj, 0x3A)
    sections = [struct.unpack_from('<IIQQQQIIQQ', obj, shoff + i * shentsize) for i in range(shnum)]
    out = []
    for s in sections:
        if s[1] != 2:  # SHT_SYMTAB
            continue
        strtab = sections[s[6]]
        for i in range(1, s[5] // 24):
            st_name, st_info, _, st_shndx = struct.unpack_from('<IBBH', obj, s[4] + i * 24)
            if st_info >> 4 not in (1, 2):  # STB_GLOBAL, STB_WEAK
                continue
            start = strtab[4] + st_name
            name = obj[start:obj.index(b'\0', start)].decode('latin-1')
            out.append((name, st_shndx != 0, False))
    return out


def coff_symbols(obj):
    """(name, defined, select_any) for each external symbol of a COFF object (MSVC, clang-cl, MinGW).

    select_any: defined in a COMDAT section whose selection is IMAGE_COMDAT_SELECT_ANY."""
    machine, nsections, _, symptr, nsyms = struct.unpack_from('<HHIII', obj, 0)
    if machine == 0 and nsections == 0xFFFF:
        raise ValueError('anonymous COFF object (/GL, /bigobj or an import stub) is not supported')
    optional, = struct.unpack_from('<H', obj, 16)
    comdat = [struct.unpack_from('<I', obj, 20 + optional + k * 40 + 36)[0] & 0x1000 != 0  # IMAGE_SCN_LNK_COMDAT
              for k in range(nsections)]
    strtab = symptr + nsyms * 18
    select_any, i = set(), 0
    while i < nsyms:  # section definitions: IMAGE_SYM_CLASS_STATIC, value 0, one aux record
        value, section, _, storage, naux = struct.unpack_from('<IhHBB', obj, symptr + i * 18 + 8)
        if storage == 3 and value == 0 and naux >= 1 and 0 < section <= nsections and comdat[section - 1]:
            if obj[symptr + (i + 1) * 18 + 14] == 2:  # IMAGE_COMDAT_SELECT_ANY
                select_any.add(section)
        i += 1 + naux
    out, i = [], 0
    while i < nsyms:
        raw = obj[symptr + i * 18: symptr + i * 18 + 18]
        value, section, _, storage, naux = struct.unpack_from('<IhHBB', raw, 8)
        if raw[:4] == b'\0\0\0\0':
            off, = struct.unpack_from('<I', raw, 4)
            name = obj[strtab + off:obj.index(b'\0', strtab + off)].decode('latin-1')
        else:
            name = raw[:8].rstrip(b'\0').decode('latin-1')
        if storage in (2, 105):  # IMAGE_SYM_CLASS_EXTERNAL, WEAK_EXTERNAL
            out.append((name, section > 0 or (section == 0 and value > 0) or storage == 105,
                        storage == 2 and section in select_any))
        i += 1 + naux
    return out


def archive_symbols(path):
    """Defined and undefined external names over every object in the archive, and the
    defined names whose every definition is a COFF select-any COMDAT."""
    defined, undefined, any_defs, other_defs = set(), set(), set(), set()
    for member, body in archive_members(path):
        try:
            if body[:4] == b'\x7fELF':
                syms = elf_symbols(body)
            elif struct.unpack_from('<H', body, 0)[0] in (0x8664, 0xAA64, 0x14C, 0xA641, 0x0000):
                syms = coff_symbols(body)
            else:
                raise ValueError('unknown object format')
        except (ValueError, struct.error) as e:
            fail(f'{path}: member {member}: {e}')
        for name, is_def, is_any in syms:
            (defined if is_def else undefined).add(name)
            if is_def:
                (any_defs if is_any else other_defs).add(name)
    return defined, undefined - defined, any_defs - other_defs


def identifier(symbol):
    """The C identifier a defined symbol comes from, if it is a mimalloc name."""
    if MI_NAME.fullmatch(symbol):
        return symbol
    m = MSVC_MANGLED.match(symbol)
    return m.group(1) if m else None


# --- build ---------------------------------------------------------------------

def cmake_build(a, src, build, extra, env=None):
    if build.exists():
        shutil.rmtree(build)
    lib = build / 'lib'
    cfg = [f'-DCMAKE_BUILD_TYPE={a.config}', f'-DMI_SECURE={a.secure}',
           f'-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY={lib}',
           f'-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_{a.config.upper()}={lib}']
    run_env = dict(os.environ, **(env or {}))
    for cmd in ([a.cmake, '-S', str(src), '-B', str(build), *cfg, *COMMON, *a.cmake_arg, *extra],
                [a.cmake, '--build', str(build), '--config', a.config, '--target', 'mimalloc-static', '--parallel']):
        r = subprocess.run(cmd, env=run_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        (build.parent / (build.name + '.log')).open('a').write(' '.join(cmd) + '\n' + r.stdout)
        if r.returncode != 0:
            fail(f'{" ".join(cmd[:2])} failed in {build}; log: {build.parent / (build.name + ".log")}\n'
                 + '\n'.join(r.stdout.splitlines()[-20:]))
    found = sorted(lib.glob('*mimalloc*.a')) + sorted(lib.glob('*mimalloc*.lib'))
    if len(found) != 1:
        fail(f'{lib}: expected one mimalloc archive, found {[f.name for f in found]}')
    return found[0]


def rewrite_directives(tree, table, prefix):
    """Rename symbols named inside /INCLUDE: linker directives. Returns the count."""
    pattern = re.compile(r'(/INCLUDE:)(\w+)')
    count, stale = 0, []

    def rename(m):
        nonlocal count
        name = m.group(2)
        if name in table:
            count += 1
            return m.group(1) + prefix + name
        if name.startswith('_') and name[1:] in table:  # x86 adds a leading underscore
            count += 1
            return m.group(1) + '_' + prefix + name[1:]
        return m.group(0)

    for f in list(tree.rglob('*.c')) + list(tree.rglob('*.h')) + list(tree.rglob('*.cpp')):
        text = f.read_text(encoding='latin-1')
        if '/INCLUDE:' not in text:
            continue
        new = pattern.sub(rename, text)
        for m in pattern.finditer(new):
            n = m.group(2)
            if n in table or (n.startswith('_') and n[1:] in table):
                stale.append(f'{f.relative_to(tree)}: {n}')
        if new != text:
            f.write_text(new, encoding='latin-1')
    if stale:
        fail('linker directives still name unrenamed symbols:\n  ' + '\n  '.join(stale))
    return count


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--source', type=P, default=ROOT / 'third_party' / 'mimalloc')
    ap.add_argument('--build-dir', type=P, required=True)
    ap.add_argument('--config', default='Release', choices=['Release', 'Debug', 'RelWithDebInfo'])
    ap.add_argument('--secure', default='CHECKED', choices=['MINIMAL', 'CHECKED', 'ON', 'FULL'])
    ap.add_argument('--prefix', default='afsec_')
    ap.add_argument('--fast-archive', type=P, help='the ordinary mimalloc archive this one must not collide with')
    ap.add_argument('--cmake', default='cmake')
    ap.add_argument('--cmake-arg', action='append', default=[], help='passed to both configure steps')
    ap.add_argument('--forced-include-style', default='auto', choices=['auto', 'gnu', 'msvc'])
    ap.add_argument('--allow', action='append', default=[], help='regex of a further shared definition to accept')
    a = ap.parse_args()

    src = a.source.resolve()
    if not (src / 'CMakeLists.txt').is_file():
        fail(f'{src}: no mimalloc source (initialize the submodule: git submodule update --init)')
    header = (src / 'include' / 'mimalloc.h').read_text(encoding='latin-1')
    version = int(re.search(r'#define\s+MI_MALLOC_VERSION\s+(\d+)', header).group(1))
    out = a.build_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    for log in out.glob('*.log'):
        log.unlink()
    shared_ok = [re.compile(p) for p in SHARED_OK + a.allow]
    is_shared = lambda s, select_any: (any(p.fullmatch(s) for p in shared_ok)
                                       or (s in COFF_HEADER_COMDATS and s in select_any))

    # 1. probe
    probe = cmake_build(a, src, out / 'probe', [])
    defined, _, select_any = archive_symbols(probe)
    table, unknown = set(), []
    for s in sorted(defined):
        ident = identifier(s)
        if ident:
            table.add(ident)
        elif not is_shared(s, select_any):
            unknown.append(s)
    if unknown:
        fail(f'{probe}: defined externals that are neither mimalloc names nor known shared '
             f'definitions (rename them or pass --allow):\n  ' + '\n  '.join(unknown))
    if not table:
        fail(f'{probe}: no mimalloc symbols found')

    # 2. prefix header
    clash = sorted(n for n in table if a.prefix + n in table)
    if clash:
        fail(f'prefix {a.prefix} produces names that already exist: {clash}')
    prefix_h = out / 'prefix.h'
    prefix_h.write_text('/* Generated by scripts/build-mimalloc-secure.py: every external mimalloc\n'
                        f'   symbol of mimalloc {version} renamed with the prefix {a.prefix}. */\n'
                        + ''.join(f'#define {n} {a.prefix}{n}\n' for n in sorted(table)))
    (out / 'rename-table.txt').write_text(''.join(n + '\n' for n in sorted(table)))

    # 3. source copy with linker directives renamed
    tree = out / 'src'
    if tree.exists():
        shutil.rmtree(tree)
    shutil.copytree(src, tree, ignore=shutil.ignore_patterns('.git', 'out', 'build', 'bin', 'docs'))
    directives = rewrite_directives(tree, table, a.prefix)

    # 4. renamed build, then checks
    style = a.forced_include_style
    if style == 'auto':
        style = 'msvc' if os.name == 'nt' else 'gnu'
    flag = f'/FI"{prefix_h}"' if style == 'msvc' else f'-include "{prefix_h}"'
    env = {k: (os.environ.get(k, '') + ' ' + flag).strip() for k in ('CFLAGS', 'CXXFLAGS')}
    built = cmake_build(a, tree, out / 'build', [], env)
    defined, undefined, select_any = archive_symbols(built)
    unprefixed_defs = sorted(s for s in defined if not s.startswith(a.prefix)
                             and not (s.startswith('?' + a.prefix)) and not is_shared(s, select_any))
    unprefixed_refs = sorted(s for s in undefined if identifier(s))
    problems = []
    if unprefixed_defs:
        problems.append('definitions without the prefix:\n  ' + '\n  '.join(unprefixed_defs))
    if unprefixed_refs:
        problems.append('references to unprefixed mimalloc names (they would bind to the fast '
                        'instance):\n  ' + '\n  '.join(unprefixed_refs))
    collisions = []
    if a.fast_archive:
        fast_defined, _, fast_select_any = archive_symbols(a.fast_archive)
        both = sorted(fast_defined & defined)
        collisions = [s for s in both if not is_shared(s, select_any & fast_select_any)]
        if collisions:
            problems.append(f'definitions also in {a.fast_archive}:\n  ' + '\n  '.join(collisions))
        shared = [s for s in both if is_shared(s, select_any & fast_select_any)]
    else:
        shared = sorted(s for s in defined if is_shared(s, select_any))
    if problems:
        fail(f'{built}:\n' + '\n'.join(problems))

    final = out / ('mimalloc-secure.lib' if built.suffix == '.lib' else 'libmimalloc-secure.a')
    shutil.copyfile(built, final)
    report = dict(archive=str(final), mimalloc_version=version, secure=a.secure, no_static_guard=True,
                  config=a.config, prefix=a.prefix, renamed=len(table), directives_rewritten=directives,
                  defined=len(defined), shared_definitions=shared, fast_archive=str(a.fast_archive or ''),
                  forced_include=style)
    (out / 'report.json').write_text(json.dumps(report, indent=1) + '\n')
    # MSVC folds every string literal into a COMDAT constant: list them in report.json only.
    listed = ', '.join(shared) if len(shared) <= 8 else f'{len(shared)} (listed in report.json)'
    print(f'mimalloc-secure {version} {a.secure} ({a.config}): {len(table)} symbols renamed to {a.prefix}*, '
          f'{directives} linker directives rewritten, 0 unprefixed definitions or references, '
          f'shared: {listed or "none"} -> {final}')


if __name__ == '__main__':
    main()
