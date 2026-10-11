# Ant Farm roadmap

The current release candidate is **1.8.0-rc.2**. This page lists remaining
and next work only. Delivered work is in [CHANGELOG.md](CHANGELOG.md); there is
no history here. Windows validation follows the release candidate and is not
part of its local gate.

## 1.8.0-rc.2

The candidate content is frozen in [CHANGELOG.md](CHANGELOG.md). rc.2 adds only
an example; its runtime is identical to rc.1, whose local Linux gate passed.
The numbers in [PERFORMANCE.md](PERFORMANCE.md) are tied to the revision they
were taken at and apply unchanged.

## Deferred after the RC

Committed compatibility limits and features that are deliberately not in the
RC. The actor API is supported release-candidate functionality; these surfaces
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

## Post-RC platform and validation work

- Windows x64 validation by hand: root, threadpool and Fiber unit tests, the
  review torture and T19 flood/lap suites, and allocator pairing. A Windows pass
  is not claimed until it is run. Known limits: T01 and T17 are POSIX-only
  fork/abort probes, and ThreadSanitizer is unavailable in the Windows LDC
  runtime (`Scoped EH not supported`).
- Multi-LLC Fiber service: native-only service by default; rescue/assist
  validated on the 2-LLC host; selective wake pending. Topology-specific
  performance and lost-worker-group claims still need their own evidence.
- Fiber ThreadSanitizer migration remains a known external limitation at the
  DRuntime stack-switch boundary; the Fiber gate stays the DMD/LDC smoke,
  release, deterministic-migration and repeated-stress matrix.
- Profile multi-producer cold Fiber admission before deciding whether stack
  preparation or moving `mmap` outside the admission lock is justified.
- Add higher-level synchronization or task-group facilities only in response to
  concrete application ownership and shutdown requirements.

## Actor API promotion gates

The release-candidate compatibility limits are lifted only when all of these
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
