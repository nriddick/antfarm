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
- Task groups, condition variables, Phobos adapter (items 7–8).
- UX/performance follow-ups listed under Execution order: `function` spawn,
  non-copying drain helper, mmap off admission, topology presets.

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



## 6b. MT parallel-range payloads — working set

`FiberDomain.parallelFor(length, grain, fn, context)` is the low-level bridge:
a managed fiber publishes one Ant Farm MT payload on its selected lane, parks
on a private WaitSet signal, and the pump drains last-grain completion so the
`@nogc` Farm callback never takes the wait mutex. `maxCs = 2` so a sweeper can
always enter; `done` is the grain count. Ranges that would exceed 512 grains
are coarsened. Nested `parallelFor`, multi-payload splits, grain cancellation,
and a throwing `foreach` wrapper are later refinements.

Acceptance remaining: multi-payload split for huge ranges without coarsening,
cooperative grain cancellation, and a Fiber-per-grain foreach that may allocate
or throw.

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
- Task groups with child ownership, sibling cancellation, deterministic outcome
  aggregation, and result abrogation.
- Explicit drain, cancel-and-drain, and fatal-stop reporting; no claim of forced
  cancellation for fibers which never cooperate.
- Bounded completion/event retention for detached tasks.

## 8. Optional Phobos adapter — planned

After the standalone contract stabilizes, add a separate
`std.concurrency.Scheduler` adapter without requiring Phobos's thread pool or
making `std.concurrency` a core dependency.

## 9. UX and performance follow-ups — open

From using the three layers together (`POSTMORTEM.md`):

- `spawn(void function())` so a module-level body does not allocate a nested
  closure per spawn.
- A `drainUntilEmpty` helper that takes `ref ConsumerView` (never a copy).
  Examples and tests must check `subscribe >= 0` with a throw, not `assert`.
- Move cold `new Fiber`/stack mmap off the admission mutex, or add `prepare()`
  so multi-threaded spawners do not convoy on mmap.
- Document two topology presets: fiber-throughput (all LPs, `flushBatch=256`,
  `avgCost=0` on this host) and payload-throughput (`skipSmtSiblings`, smaller
  chunks). Default 32/2 left Fiber drain on the table. These knobs were
  tuned on this Ryzen; it is expected to be more pessimistic about
  concurrency scaling than the Intel host already used to profile Ant
  Farm. Re-check the presets there before treating SMT-12 payload drops
  as a Farm limit.
- Keep huge pages on for non-test `AntFarm.create`; tests keep
  `ANTFARM_HUGE_PAGES=0`.
- Do not compete with single-thread DRuntime `Fiber.call`. Measure mixed
  waiting Fibers plus payloads on the same workers.

Acceptance: a new user can copy a recipe that neither copies `ConsumerView`
nor uses `Fiber.yield`/`Thread.sleep`, and the README names the two presets.

## Execution order

1. UX/performance follow-ups in item 9 (spawn `function`, drain helper,
   subscribe checks, topology presets).
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
