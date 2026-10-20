# Ant Farm roadmap

The current release is **1.8.2**; **1.9.0** is in progress on
`1.9.0-progress`. This page lists remaining and next work only. Delivered
work is in [CHANGELOG.md](CHANGELOG.md); there is no history here. Windows
validation is separate and is not part of the local gate.

## 1.9.0

The release content and the migration from 1.8.2 are in
[CHANGELOG.md](CHANGELOG.md#190--unreleased): single-shot payloads, body-only
C-linkage callbacks, `write` hooks, `antfarm.parallel`, FarmHost emission
bursts and C ABI 1. Before release:

- Run the Linux pre-push gates and the native Windows integration matrix on
  the candidate.
- Settle the open performance limits recorded in
  [PERFORMANCE.md](PERFORMANCE.md#open-190-performance-limits): table spacing
  and serial private-root throughput against 1.8.2, and the C wave bridge's
  inconclusive six-consumer gate. Small perf-gate shifts on this host are
  dominated by code layout; performance is reassessed holistically in a
  dedicated session.
- Gather production usage of the [C ABI](docs/design/c-abi.md) core, public
  actors, waves and optional non-Fiber host. Shared-library packaging and
  language ownership adapters can follow the static C/C++ distributions.

## Next

- Private root/child C exports, preserving their cohort/slab advantage. The
  D GC-rooted adapter and Fibers stay outside the runtime-free ABI.
- Continue public-actor tuning from the fused retirement and occupancy path.
  The [lifecycle optimization record](PERFORMANCE.md#public-actor-lifecycle-optimization-190-progress)
  compares both the value-owner baseline and the pre-refresh implementation,
  including C/D contention and native Windows evidence. Evaluate a combined
  idle release/dispose operation and D batch creation as separate API changes,
  with explicit busy/partial-result contracts and matched measurements. Preserve
  copied-owner reclamation arbitration and account for unresolved gate limits.
- Revise PERFORMANCE.md in a dedicated performance session, including
  tail-latency rows at chunk 2 and 4.

## Deferred after 1.8.0

Committed compatibility limits and features that are deliberately not in the
release. The actor API is supported functionality; these surfaces
are not promised yet.

- Typed root-to-application outputs (X1b). Put them in application storage
  reached from `T` and read after `takeOutcome` until then.
- Timed or external wake of a waiting private root as a join credit, and an
  emitter-owned timer source. Private deadlines stay lazy until then.
- Per-wave deadlines, public `sendAfter`, and actor timers.
- Supervision/retry policies and a guaranteed public final activation. Public
  retirement stays cooperative with no guaranteed last callback.
- `PrivateRootHandle` hashing (handles compare by `==`, they are not map keys).
- Per-member retirement, concurrent subsets, and non-POD child results.
- Adopted/arena-owned actor storage, retirement groups/fences, and reserve/
  prewarm APIs. Arena safety must not rely on transitive compiler lifetime
  inference; reserve/prewarm must report exhaustion explicitly rather than fall
  back to hidden allocation; and arena reset must be guarded by an aggregate
  retirement fence.
- Engine wake integration between the ready queue and Farm publication. Keep the
  ready-queue and Farm-publication notification boundaries separate; notify only
  after the release-publication, and treat the notification as a nudge that
  workers recheck; target an execution lane, not a per-actor owner; use a
  setup-time wake-set handle rather than the single-owner `Director` selection
  API. Until then, `flush()` plus the application's own notification is
  sufficient.
- Engine-owned module-generation reload. The engine owns the close/migrate/
  retire/unload sequence; old-generation handles never redirect silently to the
  replacement generation.
- Custom mailbox adapters. A future adapter surface must not weaken generation
  admission or retirement.
- Long-running resident-memory characterization and Windows static/shared link
  coverage for the mimalloc adapter.
- Early validation of the child-wave `avgCost` at `start`/`startFinal`/
  `startSubset`. It is diagnostics only: `chunkOf` clamps the class and
  `writeImpl` already fails fatally on an out-of-range value, except for a
  wave truncated before its first table, which reports nothing. It was
  deferred past rc.1 to keep the runtime identical to the hardened candidate.
  The patch and its death tests are on the local branch
  `followup/early-avgcost-check`.

## Platform and validation work

- Windows x64 validation uses `scripts/check-windows.py` for root, threadpool,
  Fiber, ordering, actor, review/T19 and example coverage (see
  [Testing](docs/development/testing.md#windows-x64-integration)); real allocator
  pairing remains a separate lane. A Windows pass is not claimed until it is
  run. Known limits: T01 and T17 are POSIX-only
  fork/abort probes, and ThreadSanitizer is unavailable in the Windows LDC
  runtime (`Scoped EH not supported`).
- Multi-LLC Fiber service: native-only service by default; rescue/assist
  validated on the 2-LLC host; selective wake (`servingGroups`) is opt-in
  until it is performance-characterized. Topology-specific
  performance and lost-worker-group claims still need their own evidence.
- Fiber ThreadSanitizer: migration is covered. The scheduler keeps a TSan
  context per task behind an uninstrumented switch bridge, which assumes the
  standard, uninstrumented DRuntime
  ([testing](docs/development/testing.md)). What remains limited: the TSan
  smoke disables DRuntime's parallel GC marking, whose mark stack TSan
  reports, and runs outside `make check`; the Fiber gate stays the DMD/LDC
  smoke, release, deterministic-migration and repeated-stress matrix.
- Profile multi-producer cold Fiber admission before deciding whether stack
  preparation or moving `mmap` outside the admission lock is justified.
- Add higher-level synchronization or task-group facilities only in response to
  concrete application ownership and shutdown requirements.

## Actor API promotion gates

The actor source and ABI compatibility limits are lifted only when all of these
hold (the current implementation satisfies most of them; the list is the bar for
a stability promise):

- The default shim policy and ring ABI are unchanged.
- Handlers use the exact `scope ref ActorBorrow!T` and `scope ref ActorContext`
  callback contract, and documentation does not claim this makes an `@system`
  escape impossible.
- A long run with the GC disabled completes without actor-path allocation.
- Allocator counters show zero warm-path allocations and balanced cold-path
  reclamation.
- Concurrent entry instrumentation never observes two borrows for one actor.
- A stale handle remains safe after state reclamation and slot reuse.
- Mailbox retirement races have deterministic, documented outcomes.
- Arena reset is guarded by an aggregate retirement fence.
- Shutdown detects live owners, queued activations, producer submissions, or
  unreclaimed state rather than silently freeing underneath them.
