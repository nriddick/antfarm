# Ant Farm roadmap

This page lists remaining and next work only; the version is in
[VERSION](VERSION) and delivered work is in [CHANGELOG.md](CHANGELOG.md).
Windows validation is separate and is not part of the local gate.

## Next

- Revisit table spacing and actor-churn performance in a dedicated
  optimization session: the 1.9 review kept the compact five-word layout, a
  diagnostic with three padding words on index-free tables recovered serial
  private-root throughput, and the serial `private` case still measured
  -14.1% against 1.8.2 on the laptop. The records are in the 1.9.1 CHANGELOG
  entry.
- Gather production usage of the [C ABI](docs/design/c-abi.md) core, public
  actors, waves, private root/child exports and the optional non-Fiber host
  before promoting them; shared-library packaging and language adapters can
  follow the static distributions.
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

## 2.0.0 status

The 2.0.0 release content is: the `forkjoin`
subpackage (work-stealing deques, continuation-passing join, parking and
foreign admission, `forkJoinFor`/`forkJoinEach`/`forkJoinReduce`,
`submitDetached`); `GroupHost` as the owner of pool, Farms and tokens for
raw worker bodies; the pool's `wakeOne`/`anyWorkerArmed`; blocking parallel
loops removed in favour of fork-join; `threadpool.exchange` removed;
counters and cadence verbs behind versions; MIT licensing; `VERSION`; and
the onboarding pass (three rules first, a ten-line first program on every
core, the hosting guide split). The ordered plan with each step's checks is
[docs/development/cleanup-plan-2.0.md](docs/development/cleanup-plan-2.0.md).
The fork-join suite runs under ThreadSanitizer and AddressSanitizer in
`make check`. Left after 2.0.0: private trees retreating to typed results
and tracked outcomes (2.1.1), and Windows validation of the wake path (with
the next Windows matrix). `FarmHost` runs on `GroupHost` and routes its
runtimes and parks by group (2.1.0, below).

## 2.1.0 — released 2026-10-25

Everything since 2.0.0 is collected here; there are no separate 2.0.1 or 2.0.2
releases. Shipped:

- **Fork-join frame pool and wrappers** (planned as 2.0.1).
  `forkjoin.frames` takes the process allocator out of the fork-join hot path
  (fib(39) at 32 threads: 102 ms under any allocator, from 106 to 135 with
  `malloc`). `forkJoinScan`, `forkJoinInvoke`, `forkJoinMap`, `TaskGroup`,
  and `DispatcherOptions.oneFarm()`/`.perGroup()` with `printCounters`.
- **C ABI for the dispatcher and `GroupHost`** (planned as 2.0.2). This adds
  dispatcher start/stop/counters; caller-owned C nodes with run/join
  callbacks; fork/complete, synchronous and foreign roots, reusable
  completions and detached submission; and deterministic callback for/reduce.
  On the `GroupHost` side it adds resolve/start, per-thread tokens, group
  Farms and two-phase close. The contract and validation are in
  [the C dispatcher design](docs/design/c-dispatcher.md), with a
  [native C example](cabi/examples/forkjoin.c). Existing ABI 1 rows are
  unchanged, and Fibers have no C exports. Promoting the ABI still needs
  native Windows execution, production C-host usage, and a decision on
  optional map/scan/invoke/task-group/frame-allocation helpers.
- **Fiber rework, which changes the `ant_fibers` source API.** It has three
  parts:
  - wait arbitration by a per-task claim word, with one wake per event or
    semaphore unit and timed waits;
  - `FiberDomainOptions` with `retire()` in place of
    `takeCompletions`/`release`;
  - lifecycle records in a domain-owned ring, emitted when `onRecord` is
    set, and one service pass (`serve`) shared by every host.

  See the
  [completions, records and hosting design](docs/design/fiber-completion-and-records.md).
- Fixes across actors, threadpool (pool ownership, Windows NUMA identity) and
  fork-join (lost spills, wake fences, frame adoption, start/stop); see
  CHANGELOG.

Remaining for 2.1.0:

- Route `FarmHost`'s runtimes and parks by group on top of `GroupHost`
  (`threadpool.groups`), the one owner of the pool, caller placement, one
  Farm per accepted group and per-thread tokens, whose worker loop is
  composed from attachments; the dispatcher, `FarmHost` and `FiberGroups`
  run on it. A `FarmHost` still puts its runtimes and parks on one Farm. The
  measurements are in
  [several physical groups](docs/guide/scaling-groups.md#several-physical-groups).
  Done so far: `GroupHost` composes its worker loop from
  `GroupHostOptions.attachments` and picks the lane at start (raw
  `WorkerBody` with only `@nogc` attachments, the dispatcher's body installed
  as is: fib(39) at 32 threads 101.5 vs 101.9 ms interleaved; the pool's
  managed lane when a GC-enabled hook attaches), and
  `ant_fibers.groups.FiberGroups` hosts a domain with one lane per group
  Farm, served by every worker with `until(deadline)` parks
  ([design](docs/design/threadpool.md#group-host)), and a `FarmHost` with
  workers runs on a `GroupHost` with one shared Farm, its body the single raw
  attachment, keeping its API, state machine and owned runtimes and parks
  (`FarmHost.groupHost`), and the dispatcher has an attachment form
  (`Dispatcher.attachment` + `start(host)`) so dispatcher, Fibers and a
  `FarmHost` share one host. `FarmHostOptions.farmPerGroup` routes the
  host's actor runtimes and private parks by group: each is created on one
  group's Farm and emitted through that Farm's control token, so the
  application decides which group runs which actors. The dispatcher's
  participant tables (`g_host`, `g_participants`) stay process globals: one
  dispatcher per process, like one host.
- Released after the full `make check` on `a8c84a3`, the native Windows
  matrix on `a8c84a3` plus its follow-up (a one-LLC host, so live multi-LLC
  cases are validated on Linux only), and the PERFORMANCE.md fork-join table
  re-taken under the harness's rules with the frame pool at `e44e0f9`. Still
  open: point the staged upstream runtime-benchmarks port at the tag (convert
  its ports to `allocFrames`).
- Optional Fiber follow-up, only if the bound matters: reserve 2 lifecycle
  records per task instead of 4. Fold the failure record into the terminal
  record, and reserve a cancellation record when a request is accepted. This
  doubles the default concurrent-fiber bound (4,096 at retention 16,384).
  Timed parks keep millisecond resolution.

## 2.1.1 — in progress on `2.1.1-progress`

- Done: `spillInline` is the dispatcher's default (D and C); the Farm spill
  is opt-in, after the fwbench `widetree` experiment measured it slower than
  the inline fallback wherever it engages.
- Private trees retreat to typed results and tracked outcomes.
- Private-root deadlines are lazy. Decide whether to document or close two
  gaps: a root parked in `waitChildren` is not woken when its deadline passes,
  and a root that keeps returning `again` never observes its deadline. Closing
  them is the deferred "timed or external wake of a waiting private root"
  below.

## Later

- The deque-based Fiber executor in the
  [coroutine replacement proposal](docs/design/coroutine-replacement.md).
  The FiberDomain baseline was re-measured after the 2.1.0 rework
  ([re-run](docs/performance/coroutines-2026-10-25-rework/README.md)): 6.42 M
  lifetimes/s at one thread and 1.65 M at 32, zero GC bytes, Director-bound
  at high counts, which is what per-worker admission and completion would
  remove. Its wake
  step reuses the claim word, and timers can become per-worker.
- Async I/O stays out of the runtime. An I/O library can run in a worker loop
  beside `serve` (the `GroupHost` attachment hook above gives it a place).

## 2.0 goals — draft

### Work-stealing deques for worker-pool dispatch

**Status (October 24, 2026): implemented as the `forkjoin` subpackage**,
released in 2.0.0, with the deque exactly-once test, the admission test
and the design page [docs/design/forkjoin.md](docs/design/forkjoin.md). The
measurements against citor, libfork, TooManyCooks and TBB are in
antfarm-fwbench. Of the requirements below: local dispatch and external
admission (deque owner end, Farm injection), topology-aware stealing
(same-LLC bias, measured as neutral on the 5900XT), bounded storage (fixed
deques, Farm spill, inline fallback), completion (continuation-passing join,
exactly-once by deque and Farm claims) and fairness and parking (periodic
Farm poll, Director parking with a parked bitmap) are done, and so are the
TSan/ASan lanes and the Fiber adapter (`FiberGroups` beside the dispatcher
on one `GroupHost`, 2.1.0). Still open: Windows validation of the wake path
and a submit token per foreign thread. The cleanup that follows the addition (one owner of the
pool, idle machinery, loops over fork-join, private trees, stubs,
onboarding, duplicated status) is ordered in
[docs/development/cleanup-plan-2.0.md](docs/development/cleanup-plan-2.0.md).
The text below is the original proposal.

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
- Fibers on several groups: `FiberGroups` serves each group's Farm with that
  group's workers; the former cross-group service policy (rescue/assist
  edges, selective wake) was removed with the service plans in 2.1.0 and
  returns only for a workload that needs it. Topology-specific performance
  claims still need their own evidence.
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
