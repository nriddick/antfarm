# Actor torture suite

This suite promotes the A2 actor inbox beyond ordinary contention coverage.
It compiles deterministic test hooks into `antfarm_actors.actor` and forces senders
to stop at each ownership/publication boundary while activation and retirement
proceed on other threads.

Coverage includes:

- node claim before inbox publication;
- close after reservation, claim, publication, and activation signal;
- eight simultaneous pre-close reservations racing to claim one node;
- accepted-before-close drain and reservation quiescence;
- an engine-side module-generation fence model which closes admission and
  aggregates resident type-erased actor owners with old-generation code and
  ownership claims;
- independent unload stalls for a running actor callback, a module-owned
  destructor, a sender paused after its actor submission reservation has been
  released, and a popped-but-uncompleted inbox node;
- proof in the sender and popped-node cases that the actor can be reclaimed
  first while the remaining engine claim still forbids the simulated unload;
- a wave-reservation/retirement race proving that aggregate membership pins a
  retired actor and its runtime until the open wave is sealed;
- multi-actor ready-queue contention with six producers and six consumers;
- FIFO order per producer, exact-once delivery, payload checksums, bounded
  carry republishing, 32 rounds of generation reuse, and a disabled GC;
- balanced cold-path allocation and a flat warm-path allocation count;
- the optional mimalloc v3 adapter's exact pointer/size/alignment forwarding
  and 128-byte-aligned actor state, using a local ABI stub rather than
  requiring mimalloc for the core suite;
- a real static link against the pinned mimalloc v3.5.0 submodule, checked at
  runtime with `mi_version() == 30500`; and
- 8,192 actor-state frees from eight threads other than the allocation thread,
  plus foreign-thread frees of each runtime and stable-slot array. The same
  test runs against release, `MI_DEBUG=FULL`, and mimalloc-instrumented
  ThreadSanitizer builds.

Run with LDC, DMD, or LDC ThreadSanitizer:

```text
make -C actor_torture run
make -C actor_torture run-dmd
make -C actor_torture run-tsan
```

After initializing submodules, run the real mimalloc lanes with:

```text
make -C actor_torture run-mimalloc
make -C actor_torture run-mimalloc-dmd
make -C actor_torture run-mimalloc-debug
make -C actor_torture run-mimalloc-tsan
```

The real TSan lane requires Clang for mimalloc and LDC for the D test binary.

The ordinary library build contains neither the deterministic hook branches
nor a mimalloc link dependency. The Makefile builds the pinned static source
with allocator override disabled. The root Dub configuration `mimalloc-v3`
only exposes the optional adapter and names `mimalloc` for downstream linking;
the pinned correctness lane links its archive directly so it cannot silently
select another installed version.

The module-generation fence in this suite is deliberately a test-side engine
model. Its close/count gate is the unload authority, while its per-kind counts
are diagnostics and its actor registry is driven by one coordinator. It does
not add module hierarchy to `antfarm_actors`, implement dynamic-library loading,
or define non-POD actor-state destruction. The "destructor" claimant models
module-owned resource or message destructor code admitted before close.
