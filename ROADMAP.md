# Ant Farm roadmap

## 1.7.1-rc.1 private actors

Adds experimental private actor hierarchies: self-owned roots, parent-owned
child cohorts, and joined descendant lifetimes. Includes Farm shard-completion
ordering and public actor-wave completion credits, with deterministic reuse
tests. All new `AntFarm.create` calls use the rc.4 `ringMiB` parameter, preserving
their original buffer sizes. A compile-time assertion guards the child park
layout required by `PrivateChildPark.fromWave`.

See [the ownership contract](actors/PRIVATE_ACTORS.md) and
[the lifecycle measurements](perftest/PRIVATE_LIFECYCLE.md). Follow-up work
remains on root polling while children run, batch-pool sizing for small
`batchLimit` values, and handling a root that finishes with an active child
wave. These behaviors are unchanged in this release candidate.

## 1.7.0-rc.4 usability and shutdown fixes

Behavior changes since 1.7.0-rc.3:

- `AntFarm.create`'s first parameter is now `ringMiB`, the ring size in MiB
  (a power of two in `[2, MAX_RING_MIB]`), instead of `ln` in ulongs. Divide a
  former length by 131072, or use `ringMiBFromUlongs`. A leftover ulong
  length such as `1 << 18` exceeds `MAX_RING_MIB` and is fatal rather than
  mapping 256 GiB.
- `ConsumerView.subscribe` failures are distinguishable: `SUBSCRIBE_FULL`
  (-1, 128 views already subscribed), `SUBSCRIBE_RETRY` (-2, transient
  frontier), and `SUBSCRIBE_INVALID` (-3, null Farm or already subscribed).
- `PoolOptions.workerStop` runs once on each `workerBody` worker during
  shutdown so worker-owned consumers can unsubscribe deterministically. The
  examples previously relied on a final pump visit and could abort with
  "destroy with live consumers" under load.
- `CacheAwarePool.shutdown(bool drain)` is deprecated; it never drained. Use
  `shutdown()`.
- Assigning over a still-valid producer `Token` is fatal instead of silently
  leaking its slot.
- Generated payload shims require implicit argument conversion. Narrowing or
  mutable-to-`immutable` pointer arguments no longer compile.
- An `ANTFARM_HUGE_PAGES` value other than `0` or `1` is fatal.
- The Fiber `unittest` and `stress` configurations keep `assert` checks in
  `--build=release`.
- Fixed a Fiber ready-lane accounting race that the release-stripped asserts
  had hidden: `flush` counted published activations only after `write()`
  returned, so a consumer could enter one first and drive `published`
  negative. With asserts enabled, release stress hung in about a third of
  runs; it now counts before publishing and retracts the unwritten tail.

## 1.7.0-rc.3 lifecycle performance

Consolidates the host-audit fixes (swept quota grants, serialized producer
registration, armed waits, fresh-segment protection checks in every build)
with actor lifecycle optimizations: bounded ready-queue snapshots, linear actor
creation and backlog scans, reused table ownership for single-shot payload
admission, and optional pinned mimalloc v3.5.3 linking through Dub. Measured
results are in [writeup.md](writeup.md) and [perftest/](perftest/README.md).

## 1.7.0-rc.2 specification

[SPEC.md](SPEC.md) was rewritten as one synthesized living specification of
the current algorithm, replacing the separate edit notes.

## 1.7.0-rc.1 actor waves

Make `actors` a directly onboarded work representation alongside bare
payloads and Fibers. The release candidate includes explicit non-GC actor
ownership, generation-tagged handles, exclusive callback-local borrowing,
coalesced autonomous activation, intrusive inboxes, sealed multi-table waves,
and Fiber generation triggers for dependent phases.

Verified on 2026-09-02 on the current Linux/Ryzen 5 5500 host:

- [x] Root unit tests pass under DMD and LDC.
- [x] The forced actor deterministic-interleaving, aggregate unload-fence,
  sustained-contention, and allocator-adapter suite passes under LDC.
- [x] Fiber unit tests and the LDC release stress suite, including actor-wave
  generation handoff, pass.
- [x] Root onboarding and architecture distinguish autonomous actors, actor
  waves, bare payloads, and suspendable Fiber control flow.

## 1.6.1 Windows integration

Patch the Fiber cache-line allocator to use the same Microsoft CRT-safe
64-byte allocation path as the core Farm. The shared allocator avoids a C11
`aligned_alloc` import that is unavailable in DMD's Microsoft runtime bindings
and keeps allocation and deallocation paired on every supported platform.

Verified on 2026-08-30 on Windows x64:

- [x] Root, threadpool, and Fiber unit tests pass under DMD 2.112.1 and LDC
  1.42.0.
- [x] The Fiber LDC release stress configuration passes.
- [x] The review torture and T19 flood/lap suites pass under DMD and LDC.
- [x] T01 and T17 remain POSIX-only fork/abort probes; ThreadSanitizer remains
  unavailable in the Windows LDC runtime (`Scoped EH not supported`).

## 1.6.0 objective

Ship the unified Ant Farm, cache-aware threadpool, and freely migrating Fiber
scheduler as one understandable repository with a passing cross-package build
matrix. Version 1.6 prioritizes a small coherent public surface over retaining
experiments.

## 1.6.0 scope

- Treat `antfarm`, `threadpool`, and `antfarm-fibers` as ordinary packages in
  one repository, not Git submodules.
- Require generic `write()` sources to be forward ranges so sizing and
  emission always use independent checkpoints.
- Provide common-header and fixed-body-length write forms for homogeneous
  payload batches.
- Route Fiber ready activations through the fixed one-word write path.
- Default Farms to ordinary 4 KiB backing while retaining huge pages as an
  explicit, benchmark-driven option for high-volume payloads.
- Remove the experimental Fiber parallel-loop API, descriptor, completion
  bridge, and tests. There is no replacement compatibility promise in 1.6.
- Make each README an onboarding document and keep the full layer relationship
  in `ARCHITECTURE.md`.
- Keep examples as real compile targets; identify characterization programs as
  benchmarks rather than onboarding.

## Release gates

Verified on 2026-08-29 on the current Linux/Ryzen 5 5500 host:

- [x] Root unit tests pass under DMD and LDC.
- [x] The 4K default and forced huge-page override pass the root suite; an
  unset override reports `usedLargePages=false` through the Fiber benchmark.
- [x] Threadpool captured-topology tests pass under DMD and LDC.
- [x] Fiber smoke passes under DMD and LDC debug, plus LDC release and
  release-debug.
- [x] Fiber stress passes under DMD and LDC release.
- [x] Root torture and supported ThreadSanitizer tests pass.
- [x] Every example and benchmark configuration builds under DMD and LDC.
- [x] `examples/iota_sum.d`, threadpool hello programs, and Fiber hello execute on
  supported local hardware.
- [x] Documentation contains one roadmap, no stale command paths, and no claim
  that a currently failing matrix passes.
- [x] Release notes call out the forward-range requirement and removal of the
  experimental parallel-loop API.

## After 1.6.0

- Cross the multi-LLC hardware validation bridge before making remote-lane
  performance or failover guarantees.
- Profile multi-producer cold Fiber admission before deciding whether stack
  preparation or moving mmap outside the admission lock is justified.
- Add higher-level synchronization or task-group facilities only in response
  to concrete application ownership and shutdown requirements.
- Evaluate an explicit actor/exclusive-borrow payload handle without making
  ring bodies mutable or weakening the default shim policy. Its design must
  define actor lifetime and rooting, one-activation serial ownership,
  republish ordering, stale-handle behavior, and interaction with thread-safe
  mailboxes. A non-GC A1/A2 evaluation spike now covers the exclusive borrow,
  intrusive inbox admission, close-before-drain retirement, deterministic
  interleaving torture, a pinned mimalloc v3.5.3 adapter with cross-thread
  reclamation tests, and sealed multi-table actor waves with dependent-phase
  visibility; its remaining
  promotion gates are in [ACTOR_ROADMAP.md](actors/ACTOR_ROADMAP.md).
- Reconsider bounded parallel work from first principles, with explicit
  lifetime, completion, cancellation, error, and allocation contracts.

The sanitizer/runtime boundary for migrating DRuntime Fibers remains a known
external limitation. Until stack-switch-aware sanitizer evidence is available,
the Fiber concurrency gate is the DMD/LDC smoke, release, deterministic
migration, and repeated stress matrix.
