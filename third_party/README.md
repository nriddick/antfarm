# Third-party dependencies

## mimalloc

The `mimalloc` submodule is pinned to upstream release **v3.7.0**, commit
`1ed67b5156a763441d2ec3c20221a7b3cd866129`.

Initialize it after cloning Ant Farm:

```text
git submodule update --init --recursive
```

The actor correctness lane builds the pinned source as a static library with
`MI_OVERRIDE=OFF`, so it does not replace the C or D runtime allocator. Release,
`MI_DEBUG=FULL`, and mimalloc-instrumented ThreadSanitizer builds are exercised by
`tests/actors/Makefile`. The ordinary Ant Farm library and unit-test builds do
not build or link mimalloc.

Dub's `mimalloc-v3` configuration builds the pinned release archive and passes
its exact path to the linker. It needs CMake and a C compiler, plus Make on
Linux. It does not resolve `-lmimalloc` from the host. A checkout without the
initialized submodule fails the build instead of selecting a system library.
Select it directly with `dub build --config=mimalloc-v3`, or from a dependent
package with `subConfiguration "antfarm" "mimalloc-v3"` in its `dub.sdl`.

For Windows full-debug allocator verification, use
`tests/actors/run-mimalloc-windows.ps1 -Configuration Debug -Compiler ldc2`
(or `dmd`). It keeps `MI_DEBUG=FULL` and debug code generation while selecting
the static release CRT paired with the D executable. See the
[actor test instructions](../tests/actors/README.md) for both configurations.

### The secure instance

`scripts/build-mimalloc-secure.py` builds the same pinned source a second
time for `antfarm.secure_allocation`: `MI_SECURE=CHECKED` (pointers passed to
free are validated, free lists are encoded and randomized, freed blocks are
reused late, metadata sits behind guard pages) with
`MI_SECURE_NO_STATIC_GUARD`, because the D garbage collector scans static
data and a guard page there faults it (mimalloc#1474, fixed in v3.7.0).
Every external symbol is renamed with the prefix `afsec_` through a
generated forced-include header, so the archive links beside the ordinary
one, each instance with its own heaps, arenas, thread state and options.

The script reads the symbol table from the probe archive's ELF or COFF
objects itself (no `nm` or `dumpbin`), rewrites `/INCLUDE:` linker
directives in a copy of the source (on Windows they keep the renamed
instance's TLS and CRT callbacks), and refuses an archive with an
unprefixed definition, a reference to an unprefixed mimalloc name, or a
collision with the fast archive. `make -C tests/actors
.build/mimalloc-v3.7.0-secure/libmimalloc-secure.a` builds it on Linux;
Dub's `mimalloc-v3-secure` configuration builds and links both archives
and sets `AntfarmMimallocV3` and `AntfarmMimallocSecure`.
`tests/mimalloc_secure` checks it (`make -C tests/mimalloc_secure all`,
part of `make check-mimalloc`); `tests/mimalloc_secure/run-windows.ps1` is
the Windows runner, not yet verified there.

To update the pin deliberately:

1. Check out a reviewed v3 release in `third_party/mimalloc`.
2. Update the version, commit, and runtime `mi_version()` assertion in the
   actor torture suite.
3. Run the stub, release, DMD, and full-debug mimalloc actor lanes, and the
   secure instance's lanes (`make -C tests/mimalloc_secure all`); update the
   build directory names, which carry the version.
4. Commit the submodule gitlink and documentation changes together.
