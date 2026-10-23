# Ant Farm roadmap

The current release is **1.9.0**. This page lists remaining
and next work only. Delivered work is in [CHANGELOG.md](CHANGELOG.md); there is
no history here. Windows validation is separate and is not
part of its local gate.

## 1.8.2

The release content is in [CHANGELOG.md](CHANGELOG.md): the repository
reorganization, verified usage guidance, a payload-first onboarding path and
documentation deduplication. It passed the Linux pre-push gates and the native
Windows integration matrix. The numbers in [PERFORMANCE.md](PERFORMANCE.md)
remain tied to the revisions at which they were measured.

Small perf-gate shifts on this host are dominated by code layout; performance
is reassessed holistically in dedicated sessions.

## 1.9.0 progress

- Keep the [matched MT-excision checks](docs/performance/mt-excision-check-2026-10-20.md)
  with the October 20 baseline. The apparent fourfold headline loss did not
  reproduce with the saved executable, and matched 1.8.3/current core probes
  retained fast explicit-length uniform and mixed-callback paths on both
  Windows and WSL2. The later
  [inferred-writer check](docs/performance/writer-scan-2026-10-20.md) found a
  separate sizing-scan regression and measures its restoration in `651030b`.
  The environmental cause of the earlier low rates is unresolved. These short
  core checks do not clear the separate private-actor regression below.
- Revisit table spacing and actor-churn performance in a dedicated optimization
  session. The 1.9 review retains the compact five-word layout. Compare
  `private`, `private-remote` and `dtor` at zero and six consumers against 1.8.2,
  alongside the grain grid, throughput and tail probes. Local same-binary
  controls implicated fixed-table spacing; disabling the callback guard gave
  no consistent benefit. A diagnostic with three padding words on index-free
  tables of at least four payloads recovered serial private-root throughput,
  but needs a complete performance/correctness evaluation before adoption.
  The October 19 quieter-host retest still measured a -14.1% paired-median
  change for `private` at zero consumers against 1.8.2 (`2182eb8`), using
  LDC 1.42 `-O2 -release` on an i7-12700H under WSL2. The other two serial
  private cases were inconclusive. Six-consumer samples also encountered
  persistent CPU-accounting exclusions whose cause remains unresolved. Keep
  these limits with the measurements; the 392-case grain grid verified output
  throughout, but does not establish a general performance win.
- Gather production usage of the [C ABI](docs/design/c-abi.md) core, public
  actors, waves and optional non-Fiber host. Shared-library packaging and language
  ownership adapters can follow the static C/C++ distributions.
- The unreleased public actor C API now has runtime-owned kinds, direct state
  initialization, value owners with retirement/reclaim arbitration, strided
  batches and caller-owned inbox nodes. The
  [implemented contract](docs/design/c-abi.md#actor-construction-and-ownership)
  defines lifetimes and partial results. Gather production usage of private
  root/child exports, including direct initialization and cohort slabs. The
  D GC-rooted adapter and Fibers stay
  outside the runtime-free ABI.
- The C wave bridge's existing typed-D `wave` path was compared at
  `d94acc1` against `be025b1` with LDC 1.42 `-O2 -release`, mimalloc 3.5.3,
  16,384 actors and distinct-core pins under Linux/WSL. Caller-only execution
  passed the -3% boundary with 20 clean pairs: paired-median change **-0.63%**,
  descriptive interval **[-2.70%, +0.80%]**. A byte-identical candidate/candidate
  control also passed with 20 clean pairs. Six-consumer execution remained
  **inconclusive**: 12 clean pairs from 40 attempts, short of the required 20,
  after background-CPU exclusions. The combined performance gate therefore
  did not pass. Preserve this limit for the holistic optimization session;
  this comparison does not measure the new C dispatch bridge's throughput.
  Local raw evidence is archived in `.build/cabi-waves-performance-evidence.zip`.

## Next

- A per-group-Farm host: one `FarmHost` owning one Farm per accepted LLC
  group, each group's workers pinned to its own Farm, a token per Farm, and
  `publish`/`pump` routed by group so the control thread emits locally into
  its home group and one-way into the others. The measurements and the
  interim hand-written shape are in
  [several physical groups](docs/guide/hosting.md#several-physical-groups).

- Continue public-actor tuning from the fused retirement and occupancy path.
  The [lifecycle optimization record](docs/performance/archive-2026-10-20.md#public-actor-lifecycle-optimization-190-progress)
  compares both the value-owner baseline and the pre-refresh implementation,
  including C/D contention and native Windows evidence. Evaluate a combined
  idle release/dispose operation and D batch creation as separate API changes,
  with explicit busy/partial-result contracts and matched measurements. Preserve
  copied-owner reclamation arbitration and account for unresolved gate limits.
  Batch reservation now limits each gate hold to 64 slots. Evaluate a contiguous
  free-index stack only with matched large-batch contention measurements; it
  would change slot metadata and runtime allocation, beyond this bounded-walk fix.
  The [Part 5 follow-up](docs/performance/archive-2026-10-20.md#public-actor-part-5-follow-up-190-progress)
  records release-store retirement, the bounded reservation walk and the
  remaining serial-private/historical-inbox measurement uncertainty.

## 2.0 goals — draft

### Work-stealing deques for worker-pool dispatch

Add a dispatch layer with worker-local work-stealing deques to the
topology-aware worker pool. The goal is to keep locally spawned work cheap
while allowing idle workers to help with uneven, recursive or bursty workloads.
Success means better utilization and completion latency on those workloads
without losing the Farm's efficient bulk-publication path. This is a proposed
direction, not an implemented API or a performance claim.

Today the pool runs an application-provided pump and owns no task queue; see
the [current threadpool design](docs/design/threadpool.md). The proposed
starting point is an optional dispatcher that composes with that machinery.
Keep the raw worker hooks and direct Farm consumption available. Work stealing
does not reintroduce MT payloads: each accepted work item still executes once.

Proposed requirements:

- **Local dispatch and external admission.** Give each worker exclusive access
  to its deque's owner end; eligible thieves claim from the other end. Define
  a separate submission path for application threads, rather than letting
  arbitrary producers mutate the owner end. Decide whether a Farm, an injection
  queue, or both supply that path, including partial acceptance and backpressure.
- **Topology-aware stealing.** Prefer victims in the worker's own LLC group.
  Make cross-group stealing an explicit policy, with bounded attempts and
  clear eligibility rules for affinity-bound work. Balance idle capacity against
  the cost of moving work and data; connect this design to the per-group-Farm
  host proposed above rather than making every worker consume every Farm.
- **Bounded storage and ownership.** Start with preallocated deque capacity and
  explicit overflow behavior. Keep the raw dispatch path `nothrow @nogc`, with
  no hidden allocation when a queue fills. Choose the descriptor representation
  and transfer rules before exposing an API. A ring-borrowed payload pointer,
  actor borrow, or thread-affine state cannot simply be retained in a deque.
- **Completion and lifecycle.** Specify publication, claim, execution and
  reclamation separately. Define how accepted work is counted through steals,
  nested submission, cancellation, worker failure and shutdown. If a Farm
  callback forwards work to a deque, its table hook joins that callback, not
  the later execution; any bridge needs its own completion/lifetime contract.
  Preserve actor serialization and Fiber ownership rules in future adapters.
- **Fairness and parking.** Define how workers alternate local work, external
  submissions, Farm polling and steal attempts so recursive local work cannot
  indefinitely starve another source. Integrate release-publication with the
  existing wake/recheck protocol. An empty local deque or failed steal does not
  prove global completion, and an idle worker must not miss newly admitted work.

Implementation and acceptance sequence:

1. Write the dispatch and memory-order contracts, then choose the deque
   algorithm. Resolve bounded versus growable storage, overflow policy,
   single-item versus batch stealing, victim selection, and local execution
   order. Do not promise FIFO scheduling or lock freedom before those choices
   and their proofs are established.
2. Prototype the standalone deque and external admission path. Add deterministic
   tests for the last-item owner/thief race, competing thieves, wraparound,
   full queues, publication visibility, and safe destruction. Establish exactly
   one execution or an explicitly reported cancellation for every accepted item.
3. Integrate the dispatcher with pool wake, idle and shutdown behavior; test
   nested submissions, parked workers, admission racing shutdown, and affinity
   restrictions. Introduce Farm, actor or Fiber adapters only after their
   ownership and completion mappings are documented.
4. Compare against direct Farm dispatch on the 5900XT: balanced bulk work,
   skewed task costs, recursive fan-out, external producer bursts, and idle-to-busy
   transitions. Report throughput, tail latency, steal success/attempts, wakeups,
   queue occupancy and memory cost, with same-LLC and opt-in cross-LLC placement.
   Run the Linux correctness/TSan lanes and native Windows checks. Use these
   results to decide defaults; do not assume deques improve every workload.

The public D submission surface and any C exports are design decisions for
this work. Keep the runtime-free core and optional druntime worker host distinct;
adding deques does not by itself make the worker pool runtime-free. Do not
silently repurpose C ABI 1 entry points or change their completion semantics.

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
