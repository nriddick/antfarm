# Third-party dependencies

## mimalloc

The `mimalloc` submodule is pinned to upstream release **v3.5.0**, commit
`18b08671c9302247bfb682286e6bf3cc1773f801`.

Initialize it after cloning Ant Farm:

```text
git submodule update --init --recursive
```

The actor correctness lane builds the pinned source as a static library with
`MI_OVERRIDE=OFF`, so it does not replace the C or D runtime allocator. Release,
`MI_DEBUG=FULL`, and mimalloc-instrumented ThreadSanitizer builds are exercised by
`actor_torture/Makefile`. The ordinary Ant Farm library and unit-test builds do
not build or link mimalloc.

To update the pin deliberately:

1. Check out a reviewed v3 release in `third_party/mimalloc`.
2. Update the version, commit, and runtime `mi_version()` assertion in the
   actor torture suite.
3. Run the stub, release, DMD, and full-debug mimalloc actor lanes.
4. Commit the submodule gitlink and documentation changes together.
