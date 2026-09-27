# Third-party dependencies

## mimalloc

The `mimalloc` submodule is pinned to upstream release **v3.5.3**, commit
`d4881d338125e1cb7c47ba4cfb398d6f7c0c8d45`.

Initialize it after cloning Ant Farm:

```text
git submodule update --init --recursive
```

The actor correctness lane builds the pinned source as a static library with
`MI_OVERRIDE=OFF`, so it does not replace the C or D runtime allocator. Release,
`MI_DEBUG=FULL`, and mimalloc-instrumented ThreadSanitizer builds are exercised by
`actor_torture/Makefile`. The ordinary Ant Farm library and unit-test builds do
not build or link mimalloc.

Dub's `mimalloc-v3` configuration builds the pinned release archive and passes
its exact path to the linker. It needs CMake and a C compiler, plus Make on
Linux. It does not resolve `-lmimalloc` from the host. A checkout without the
initialized submodule fails the build instead of selecting a system library.
Select it directly with `dub build --config=mimalloc-v3`, or from a dependent
package with `subConfiguration "antfarm" "mimalloc-v3"` in its `dub.sdl`.

To update the pin deliberately:

1. Check out a reviewed v3 release in `third_party/mimalloc`.
2. Update the version, commit, and runtime `mi_version()` assertion in the
   actor torture suite.
3. Run the stub, release, DMD, and full-debug mimalloc actor lanes.
4. Commit the submodule gitlink and documentation changes together.
