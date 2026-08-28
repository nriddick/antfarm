# Ant Farm fibers roadmap

The target is a standalone, freely migrating druntime-`Fiber` scheduler whose
ready activations are carried by Ant Farm. `threadpool_llc` is the current
topology, cadence, affinity, and wake-policy embedding; Phobos integration is
an optional adapter rather than the scheduler's architecture.

## Immediate review priorities (updated 2026-08-28)

Characterization, Fiber-parity, and the control-word split are in
`PROGRESS.md` and `POSTMORTEM.md`. Remaining review work that is still open:

- TSan Fiber probes (item 0 below) stay separate from the passing release suite.
- Mixed-payload 100,000-sleeper timer overhead (item 1 acceptance).
- Hardware multi-LLC sweeper tests (item 6; this host has one LLC).
- Ant Farm-native bounded `parallelFor`/`parallelForeach` payloads (item 6b).
- Task groups, condition variables, Phobos adapter (items 7–8).
- mmap off admission / `prepare()` if multi-producer spawn shows up in profiles.

### 0. Keep sanitizer failure separate from the corrected release test

The apparent LDC 1.42.0 release and release-debug `SIGILL` was a test-harness
false positive. `migrationSmoke` performed `flush`, `subscribe`, `signal`,
`pollTimers`, and `cancel` calls inside `assert` expressions. Release builds
removed those calls, so the fresh consumer never subscribed and eventually
executed the test's deliberate `assert(false)` trap after 100,000 empty polls.
Moving side effects outside assertions makes the complete smoke suite and the
stress suite pass under LDC release optimization, including deterministic
cross-thread migration, suspended GC roots, cancellation unwinding, and
retained exceptions.

The fsanitize-enabled druntime failure remains independent: LDC 1.42.0 and the
local master experiment both fail inside the sanitizer/runtime boundary even
for isolated Fiber probes. Continue that investigation with explicit TSan
Fiber handles and a minimal two-thread handoff, but do not use the corrected
Ant Farm release suite as evidence of an optimizer or druntime migration bug.

## Architectural contract

- A suspended Farm-published fiber may resume on any eligible OS thread.
- Fiber and logical Payload code must not depend on TLS continuity across a
  suspension or dispatch boundary; doing so is explicitly undefined behavior.
- LLC placement is a preference. It must never become fiber ownership.
- Ant Farm carries ready activations only. Waits, timers, cancellation,
  lifecycle events, roots, and completion state remain outside the ring.
- The warm ready path stays fixed-size and allocation-free.
- Raw recyclable task storage and externally stable logical identity are
  separate concepts.
- Director policy has one owner. Workers and producers report readiness; they
  do not acquire Director authority.

## Completed foundation

- Lazy one-word fiber payloads sharing one resume header; separate range-based
  Ant Farm writing. Production pumps use `consumeNext()` so a visit finishes a
  table or first-claimant-yields; `consumeQuantum` is a one-resume test probe.
- One `ulong` control word: PHASE plus CANCEL/ENTER/WAKE/PARK `fetch_add`
  addends. Park/signal and pre-entry cancel compose without CAS twins.
- Cancellation exposure is bounded to the current Fiber plus one Director
  application. Arbitrary threads queue generation-paired handles; the Director
  coalesces remote requests, owns release/recycle, and applies at most one
  remote CANCEL per generation. The running→terminating add's returned word is
  the terminal cutoff, so a later counted Director add is harmless and needs
  no rollback or task-word CAS. Only the first accepted request may publish
  pending disposition/lifecycle metadata.
- Fiber-parity smoke: `scope(exit)` on return/yield/cancel/fail, nested
  scheduler suspends, recycle+`yieldReady`, raw `Fiber.yield` fails the
  generation instead of stranding.
- Throughput vs ameliorated DRuntime Fibers, MT topology sweep, and
  Farm-spin vs threadpool vs Fiber-pump embedding (`POSTMORTEM.md`).
- Intrusive MPSC ready/completion staging, lossless partial-write requeue, and
  detached stack snapshots safe against immediate prefix consumption.
- Spawn, cooperative yield, signals, timers, cancellation unwind, shutdown,
  outcomes, exception retention, active GC roots, and Fiber/stack recycling.
- Managed LLC lanes with persistent consumer/tokens, Director-bounded timer
  parks, broadcast or Director-owned wakes, and responder selection.
- Deterministic free migration through yield, signal, timer, cancellation,
  live GC stack roots, and escaping exception under DMD and debug LDC; the
  optimized-LDC/druntime boundary is tracked above.
- The ownership audit in `MEMORY_ORDER_AUDIT.md` identifies a release/acquire
  chain for each current migration path. Fiber TSan remains abandoned until
  its standalone druntime/libtsan migration probe works.



## 6b. Bounded parallel payloads — move the primitive into Ant Farm

The existing `FiberDomain.parallelFor(length, grain, fn, context)` proves the
transport: a managed Fiber publishes one Ant Farm MT payload on its selected
lane, parks on a private WaitSet signal, and the pump drains last-step
completion so the `@nogc` Farm callback never takes the wait mutex. Its
`ParallelRange` descriptor and fixed `maxCs = 2`, however, mix general bounded
job mechanics with Fiber waiting policy.

Build the lower-level facility on Ant Farm's payload type. A caller-owned job
descriptor should map a well-defined half-open bound onto 2 .. 512 balanced
steps and expose one `PayloadEntry`. Keep these controls distinct:

- `done`/steps is partitioning granularity, not the range length;
- `maxCs` is the independent maximum number of concurrent consumers; and
- one step may cover an arbitrarily large subrange, so ranges larger than 512
  elements do not require coarsening or multiple payloads for correctness.

Use quotient/remainder partitioning for exact, overflow-safe coverage. The
descriptor, context, inputs, outputs, and scratch storage remain caller-owned
and live through completion. Each step retires exactly once, including a
cooperatively skipped or failed step. The final completion decrement publishes
worker writes with release semantics and polling/waiting observes completion
with acquire. Submission, polling, and completion belong to the Farm primitive;
parking and waking remain embedding policy.

The fast callback contract remains `@nogc`. The non-throwing form stays
allocation-free. An exception-aware template may catch a throwing-but-`@nogc`
body into caller-provided scratch storage, preferably one independently-owned
slot per step so simultaneous failures require no contended first-winner CAS.
Keep `std.outbuffer` in mind for the eventual public API: an OutBuffer-like
caller-supplied scratch sink could support compact failure aggregation and
later growth policy without making Phobos or GC allocation part of the Ant
Farm core. Arbitrary allocating D delegates do not fit behind the existing
`@nogc` callback ABI; they need an explicitly slower userland/Fiber adapter.

The initial `parallelForeach` sugar should accept arrays, slices, integer
bounds, and explicitly sized random-access ranges. Do not claim support for a
general input range that would first need serial materialization. The Fiber
adapter should retain the Farm descriptor, submit it on the selected lane,
park until completion, and translate Fiber cancellation into a cooperative
stop flag; workers still retire skipped steps. Multi-payload splitting remains
a later fairness/latency option, not a requirement for large bounds.

Acceptance: Farm-level tests prove exact once-only coverage for uneven and
very large bounds at 2, 3, and 512 steps; independently vary `maxCs`; exercise
backpressure and caller-owned lifetime; capture simultaneous failures in
scratch storage without loss; and prove completion publication under DMD and
LDC. Fiber tests then prove park/resume, cooperative cancellation, and retained
descriptor lifetime using the same primitive rather than a second range engine.

## 7. Complete standalone facilities — in progress

- Poll and external blocking/timed join are implemented with generation-safe
  handles. A reusable manual-reset DRuntime event avoids polling; a waiter gate
  prevents `release`/reuse from resetting it while an old-generation waiter is
  waking. The event and gate occupy their own manually allocated 64-byte-aligned
  reference-free control region.
- Fiber-aware and timed-Fiber joins are implemented for tasks in one backend.
  An intrusive target list performs registration and the terminal recheck under
  one aligned spin gate, closing the poll/register lost-wakeup window. Inbound
  list ownership and outbound membership occupy distinct cache lines. Target
  completion, cancellation, and indexed timer expiry detach exactly once; a
  2,000-round registration race and 100 rounds of 32-way completion/cancellation
  fan-out cover the arbitration and the early-wake PARK/WAKE window. Cross-backend
  joins await the domain/lane split.
- One-shot events and a counting semaphore are implemented on private WaitSet
  keys with register-then-recheck. A scheduler-aware condition variable (mutex
  + wait queue that can drop the lock) remains open.
- Task groups with child ownership, Director-routed generation-paired sibling
  cancellation, deterministic outcome aggregation, and result abrogation.
- Explicit drain, cancel-and-drain, and fatal-stop reporting; no claim of forced
  cancellation for fibers which never cooperate.
- Bounded completion/event retention for detached tasks.

## 8. Optional Phobos adapter — planned

After the standalone contract stabilizes, add a separate
`std.concurrency.Scheduler` adapter without requiring Phobos's thread pool or
making `std.concurrency` a core dependency.

## 9. UX and performance follow-ups — in progress

From using the three layers together (`POSTMORTEM.md`):

- **Done:** `spawn(void function())` reaches druntime's function-pointer
  constructor/reset directly, so a module-level body needs no delegate adapter.
- **Done:** `drainUntilEmpty` takes `ref ConsumerView` (never a copy), polls
  timers, and drives a one-lane domain to terminal completion. Examples and
  tests use `subscribeOrThrow`, so optimized builds cannot remove subscription.
- Move cold `new Fiber`/stack mmap off the admission mutex, or add `prepare()`
  so multi-threaded spawners do not convoy on mmap.
- **Done:** `applyFiberTopologyPreset` and the README expose two topology
  presets: fiber-throughput (all LPs, `flushBatch=256`, `avgCost=0` on this
  host) and payload-throughput (`skipSmtSiblings`, 32/2 tables). Default 32/2
  left Fiber drain on the table. These knobs were
  tuned on this Ryzen; it is expected to be more pessimistic about
  concurrency scaling than the Intel host already used to profile Ant
  Farm. Re-check the presets there before treating SMT-12 payload drops
  as a Farm limit.
- Keep huge pages on for non-test `AntFarm.create`; tests keep
  `ANTFARM_HUGE_PAGES=0`.
- Do not compete with single-thread DRuntime `Fiber.call`. Measure mixed
  waiting Fibers plus payloads on the same workers.

Acceptance met: the README recipe keeps `ConsumerView` by reference, uses a
module-level function body, and names both presets without using
`Fiber.yield`/`Thread.sleep`.

## Execution order

1. Ant Farm bounded `parallelFor`/`parallelForeach` primitive, then reduce the
   Fiber implementation to its awaiting adapter.
2. Task groups with outcome aggregation.
3. Scheduler-aware condition variable.
4. Optional Phobos adapter.
5. mmap off admission / `prepare()` if multi-producer spawn shows up in
   profiles.

Run DMD/LDC smoke, stress, forced migration, and throughput benchmarks after
every task-layout, state, publication, or wake change. A target which cannot
safely migrate a suspended druntime fiber is unsupported rather than silently
changed to pinned semantics. Re-run `benchmark_vs_druntime` and
`benchmark_farm_embed` when changing flush, pump, or worker selection.
