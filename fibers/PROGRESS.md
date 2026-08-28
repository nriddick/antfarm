# antfarm_fibers project state

Last updated 2026-08-28. This is a working experimental scheduler, not yet a
production library. `POSTMORTEM.md` collates how the three layers feel to use,
where they fight Phobos Fiber habits, and what the numbers actually mean.

## Objective

Build a standalone, freely migrating druntime-`Fiber` scheduler whose runnable
activations use Ant Farm as a high-throughput transport. `threadpool_llc` is the
current topology/cadence/wake embedding, but fiber lifecycle remains outside
the pool and a future Phobos adapter remains optional.

Ant Farm, threadpool, and Fiber sources now share one authoritative repository.
The Fiber DUB package resolves the root Farm and `../threadpool` by local path,
so there is no second vendored source snapshot to reconcile.

## Current execution model

```text
spawn / yield / signal / timer / cancellation
                    │
                    ▼
          PublishAccumulator (MPSC)
                    │ batched flush (default 32; 256 helped Fibers here)
                    ▼
              Ant Farm tables
                    │ consumeNext (production); consumeQuantum is a test probe
                    ▼
              resumeCallback
                    │
                    ▼
                 Fiber.call
             ┌──────┼──────────┐
             ▼      ▼          ▼
          terminal ready     wait/timer
             │      │          │
             ▼      └──────────┘
      completions MPSC   (republishable)
             │
      takeCompletions → release → recycle
```

Ant Farm carries only runnable activations. Indefinite waits, timers,
cancellation state, exceptions, completions, GC roots, and recycled stacks stay
outside the ring.

Every fiber activation is a single-threaded, one-shot Farm payload:

```text
maxCs = 1
done  = 1
body  = one ulong containing the FiberTask pointer
```

A fiber is never itself an Ant Farm MT payload. Parallel `0 .. N` work uses a
distinct MT range payload; last-grain completion wakes the coordinating fiber.

## Implemented components

### Task and fiber lifecycle

- `FiberTask` owns one druntime `Fiber`, scheduler state, failure, intrusive
  queue links, stack-size class, and migration diagnostics.
- Cold spawn creates a task/Fiber and maps a stack. Explicitly released terminal
  tasks are pooled by stack size and reused with `Fiber.reset`.
- Active tasks are held in an explicit roots array because Farm bodies contain
  raw integer pointer words which are not trusted as GC roots. The backend is
  itself an explicit GC root from first admission until the last completion is
  taken, so the roots array remains reachable through the entire exchange.
- Terminal outcomes are currently `completed`, `failed`, or `cancelled`.
  Escaping `Throwable`s are returned by `Fiber.call!(Rethrow.no)`, retained on
  the task, and never unwind through Ant Farm's callback.

### Logical task identity

- Each recyclable slot has a local 64-bit generation incremented on admitted
  spawn. There is no global task counter.
- `TaskHandle` captures exact slot/generation identity plus a diagnostic-only
  hash.
- Generation-safe cancellation and double-validated terminal snapshots reject
  stale handles, including reuse racing an outcome/exception read.
- `TaskHandle.poll` returns the coherent terminal snapshot with explicit
  pending/stale states. External blocking and timed joins use a reusable
  DRuntime event rather than sleeping or polling. Release closes waiter
  admission and waits for registered wakees before pooling, so reuse cannot
  reset an event beneath an old generation.
- Managed Fibers use `joinFiber` or its timed overload. A generation-checked
  intrusive target list makes registration atomic with the terminal recheck;
  target completion, cancellation, and timer expiry arbitrate list removal.
  Inbound-list and outbound-membership mutations occupy separate aligned lines,
  and the backend's aggregate managed-waiter count is isolated on its own line.
- Lifecycle-enabled admission reserves four possible records against a bounded
  backend budget (65,536 by default). Terminal classification returns unused
  kinds, and copy or successful-handler acknowledgement returns emitted slots.
  Backpressure is reported by `FiberLifecycleBackpressure` before pool dequeue.
- Bounded lifecycle drains retain an older suffix ahead of concurrently queued
  records. Director handler failure preserves the failing suffix and its
  already-assigned sequence for retry; arbitrary callbacks remain outside
  workers and the Farm callback.
- Arbitrary-thread cancellation submits a GC-visible `(slot,generation)`
  request to the domain control queue. The construction/Director thread alone
  applies remote requests and owns release/recycle, so an old request racing
  reuse is rejected before it can touch the new generation.
- Raw `FiberTask` APIs remain temporarily for compatibility and retain the
  strict no-use-after-`release` contract.

### Publication and consumption

- `PublishAccumulator` is an intrusive MPSC. A push uses a count update, one
  atomic exchange, and a release link; it allocates nothing and is unbounded.
- `flush` steals a FIFO chain, copies references to a fixed stack snapshot,
  detaches every queue link, and exposes a lazy `PayloadEntry` range to
  `AntFarm.write`.
- All fiber entries share one prebuilt resume header. Only the task pointer word
  varies.
- A partial Farm write reconstructs and republishes the entire unwritten suffix;
  backpressure never drops work.
- Snapshot detachment prevents an immediately consumed prefix from reusing
  `queueNext` while flush still traverses the suffix. This fixed the former MT
  SEGV/lost-chain failure.

### Suspension and readiness

- Production pumps and worker-like drains use `consumeNext()`, so a visit
  finishes a table or first-claimant-yields (Ant Farm 5e-m). `consumeQuantum`
  remains a one-resume test artifact (`migrationSmoke` / `consumeOnFreshThread`).
- `yieldReady` republishes a cooperative continuation through the accumulator.
  Raw `Fiber.yield()` is not a scheduler suspend: HOLD with `phase` still
  running fails that generation with a retained exception rather than dropping
  the only activation. `fiberParitySmoke` covers cleanup, nested suspend,
  recycle+`yieldReady`, and that raw-yield failure.
- `WaitSet` uses 64 mutex-striped signal maps with intrusive waiter chains.
- Waiters have predecessor links and membership generations, so individual
  removal is O(1) under the stripe lock and whole-signal detach remains O(1).
- One `ulong` control word on the task's 64-byte line holds PHASE in the high
  half and CANCEL/ENTER/WAKE/PARK 8-bit addends below. Outsiders `fetch_add`
  CANCEL or WAKE; the exclusive Farm consumer `fetch_add`s ENTER, PARK, and
  phase deltas. A whole-word store is legal only while the slot is unpublished.
  Park vs signal is composition: exactly one of PARK-first or WAKE-first
  publishes ready. There are no cancelled or signalled phase twins, and no
  `fetch_or` (that is a first-wins CAS).
- `TimerSet` generates private high-bit signals and uses an indexed binary
  min-heap ordered by `(deadline, sequence)`. Lookup is O(1), insert/remove are
  O(log N), and expiration pops without allocating an intermediate array.
- Cancellation wakes parked waiters and throws `FiberCancelled` at cooperative
  checkpoints so D cleanup scopes unwind normally.
- `cancelCurrent` is the only direct worker-side cancellation source.
  `directorCancelDetailed` applies generation-paired remote requests and
  distinguishes stale/already-requested/already-terminal operations, a
  fetch_add-defined pre-entry win (CANCEL before ENTER), and requests delivered
  to running, ready, or waiting tasks. A pre-entry win suppresses the delegate.
- An unrelated exception raised during cancellation cleanup is retained as a
  failed outcome instead of being masked by the cancellation request.
- Immutable `CancellationDisposition` distinguishes acknowledged cancellation,
  a normally returned result abrogated by a winning request, and cleanup
  failure; lifecycle terminal records carry the same snapshot.
- The old word returned by running→terminating is the cancellation cutoff.
  Terminal finalization waits for a winning cancellation publisher to finish;
  only the first publisher may write `pending`. A later counted Director add is
  ineffective, is not rolled back, and cannot overwrite final disposition.
- A 2,000-generation timer-expiry/cancellation race covers cancellation-first,
  expiry-first, and unconstrained orderings. It requires one ready activation,
  no residual timer, an acknowledged cancellation, and balanced recycling for
  every generation.
- Early wakeup is the same PARK/WAKE composition as a parked waiter: the
  unique publisher `fetch_add`s phase to ready and clears the wait fields
  before MPSC publication. The next Farm callback therefore never observes a
  signalled-running state.

### Threadpool lanes and wake policy

- One `FiberLane` is installed per LLC. Each managed worker owns a persistent
  `ConsumerView` and small-tier producer token from start through stop.
- The default wake policy broadcasts for compatibility.
- `directorOwned` instead coalesces spawn, signal, cancellation, shutdown, and
  worker-publication bits for the exclusive Director thread to drain.
- An optional `nothrow @nogc` notifier fires only on the atomic mailbox
  empty-to-nonempty edge, allowing a dormant Director event loop to be signaled
  without executing arbitrary handlers in workers/producers.
- `responderSelector` chooses workers which poll timers, accept timer-bounded
  parks, and actively sweep Farm holes/leftovers. Other workers may remain
  primarily cadence-driven while still consuming/publishing when scheduled.
- Worker publication now obeys the lane wake policy; it no longer bypasses
  Director ownership with an unconditional `wakeAll`.
- Managed workers register lane and responder counts before pumping. The final
  start publishes coverage: active lanes with no responder fail explicitly,
  while lanes with no selected workers reject subsequent spawns.

## Free migration

Free migration is the shared-Farm contract, not an optional stealing mode.
After suspension, any eligible consumer may resume the fiber. Stack locals move
with the fiber; TLS, errno, affinity, floating-point environment, and other OS
thread state do not. Any Fiber or logical Payload which carries TLS-derived
state across a suspension or dispatch boundary is outside the supported model
and has undefined behavior. This includes indirect TLS use in dependencies;
successful DMD or unoptimized runs do not make it portable to LLVM optimization.

The deterministic migration smoke test resumes one task on five fresh OS
threads through:

1. initial entry;
2. `yieldReady`;
3. application-signal wake;
4. timer wake;
5. cancellation wake and unwind.

It verifies distinct adjacent worker identities, four migrations, a GC object
rooted only on the suspended stack across forced collections, cleanup execution,
and exactly one completion. Another task throws after migration and verifies
failure retention.

Per-task `resumeCount`, `migrationCount`, and `lastResumeIdentity` are
observational diagnostics and never create affinity.

## Memory-order audit

`MEMORY_ORDER_AUDIT.md` traces the ready, MPSC, Farm sentinel, wait/signal,
timer/cancel, terminal/completion, root-removal, and recycle edges. Every current
cross-thread stack handoff has an identified release/acquire chain; no missing
stack-publication edge was found.

The audit's immediate responder-validation and dormant-Director notification
issues are implemented. The opt-in ordered lifecycle/failure lane and the
shared-domain remote-sweeper ring are also implemented. Application
handler/retention policy remains architectural work.

Generation-stable inspection, fetch_add-arbitrated cancellation, and a
mutex-linearized spawn/shutdown admission transaction resolved the immediate
stale-read, pre-entry, cleanup-failure, and pooled-slot leak hazards. Broader
handle-based APIs remain unfinished.

## GC and allocation profile

Allocation-free after warm-up on the common ready path:

- ready/completion MPSC pushes;
- stolen-chain snapshot and lazy Farm range;
- fiber callback and ready republish;
- wait-chain insertion/removal for an existing signal bucket;
- timer expiry after the heap has reached its high-water capacity;
- lifecycle event publication after four nodes have been created for the slot;
- `Fiber.reset` reuse of task object and mapped stack.

Allocation remains in cold or control paths:

- cold task/Fiber creation and stack mmap;
- first lifecycle enable and four event-node objects per participating slot;
- roots-array growth;
- first associative-array insertion for a signal key;
- timer-heap growth;
- `takeCompletions` result allocation;
- `takeLifecycleEvents` result allocation;
- cancellation-shutdown snapshots;
- user fiber code and exception construction.

The recycle pool grows toward the concurrent live-fiber high-water mark rather
than total historical spawn count.

The independently contended task state, global wait counts, accumulator drain
gate, backend admission/active/timer counters, lane wake mailbox, external join
event, and managed-join list/membership reside in reference-free 64-byte-aligned
control records. GC-visible references remain in ordinary scanned owner
objects; no manually allocated region is conservatively scanned.

## Performance snapshot

Host: Ryzen 5 5500, 12 logical processors, one LLC, LDC release. Benchmarks
now leave Ant Farm huge pages on (the Farm default; `ANTFARM_HUGE_PAGES=0`
forces 4K). These are characterization numbers, not stable API guarantees.
This box is expected to be more pessimistic about concurrency scaling
than the Intel host already used to profile Ant Farm (12700H,
~200–390M items/s). SMT-12 payload drops and extra-worker plateaus here
should not be read as the Farm's ceiling.

| Phase | Representative result |
|---|---:|
| Cold create, ST, 200k | ~147–153k fibers/s |
| Warm create, ST, 200k | ~3.2–3.6M fibers/s |
| Run, ST, 200k | ~3.4–4.2M resumptions/s |
| Run, MT, 200k (warm) | ~6.9–8.1M resumptions/s |
| Warm create, MT, 200k | ~2.0–2.3M fibers/s |

Cold creation is dominated by druntime GC/Fiber/stack mmap; Farm huge pages,
`flush` 32–256, `avgCost` 0–2, and 4 KiB stacks do not move it. Empty Fiber
bodies are also not Farm-bound: the same flush/avgCost matrix stays inside
run noise. Huge pages still matter for Ant Farm's own sequential table walk;
on this host (`THP=always`) they do not accelerate the empty-fiber
microbench, and the first MT drain after a cold map can be slower while
pages collapse. Steady/warm MT drain is similar or slightly better with huge
pages on. `FiberDomain.reserve` avoids roots realloc during a known burst.
Warm create is still control-thread-only.

ST comparison against an ameliorated DRuntime `Fiber` loop (`dub run -c
benchmark_vs_druntime`, 100k, LDC release, huge pages on). DRuntime side:
reserved `Fiber[]`, `Fiber.reset` reuse, one recycled Fiber when N stacks
are unnecessary, two-pass `call` for a known yield, `GC.disable` on
non-allocating run loops.

| Path | Rate |
|---|---:|
| DRuntime reserved run | ~7.0M /s |
| DRuntime reset+run (warm) | ~7.3M /s |
| DRuntime 1× Fiber reset+run | ~27M /s |
| DRuntime 1-yield two-pass | ~3.0M fibers/s |
| antfarm fiber run (warm) | ~4.1M /s |
| antfarm fiber 1-yield run | ~1.8M fibers/s |
| antfarm payload run | ~55M /s |
| Cold create (both Fiber paths) | ~150–175k /s |

Folding a Fiber into the Farm is about half a well-tuned same-thread
`Fiber.call` loop. The low-level payload path is an order of magnitude
above either Fiber. That is the intended split: serial jobs stay payloads;
Fibers share the same worker/Farm when the body actually needs to suspend.

The same binary then sweeps Ant Farm + threadpool topologies on this 1-LLC
Ryzen (12 LPs / 6 cores). Warm empty-fiber drain and control-thread payload
produce, 100k jobs, `flush=256 avgCost=0` unless noted:

| Topology | Fiber /s | Payload /s |
|---|---:|---:|
| 1 worker | 3.5M | 42M |
| 2 workers | 3.6M | 31M |
| 4 workers | 5.4M | 32M |
| 6 physical cores | 6.7M | 29M |
| 6 phys, flush 32 / avgCost 2 | 5.4M | **53M** |
| 12 SMT | **8.3M** | 32M |
| 12 SMT, flush 32 / avgCost 2 | 5.1M | 51M |

On this host the fiber optimum is **all 12 SMT LPs and large Farm tables**
(`flushBatch=256`, `avgCost=0`): 8.3M warm resumptions/s, above the 7.5M
DRuntime ST `Fiber.call` loop. Default lane flush 32/2 leaves fiber drain
on the table. Payload throughput is highest with **6 physical cores and
smaller chunks** (53M), still below the 63M same-thread payload loop
because the control thread is the sole producer. Extra SMT consumers help
Fibers (more `Fiber.call` hands) and hurt or do nothing for tiny payloads
(ring contention). Treat that last split as Ryzen-pessimistic: the
Intel Ant Farm host is expected to keep scaling concurrency further.

Same-payload embedding check (`dub run -c benchmark_farm_embed`, 2M
one-shot jobs, avgCost 0, LDC release). Farm ST produce+consume is the
Ant Farm ceiling for this job; farm spin is pinned threads in a tight
`consumeNext` loop; pool pump is CacheAwarePool with no FiberLane;
fiber pump is `fiberWorkerHooks`.

| Embedding | 6 physical cores | 12 SMT |
|---|---:|---:|
| farm ST (1 thread) | 53.8M | — |
| farm spin consumeNext | 54.5M | 32.9M |
| pool pump spin | 54.9M | 30.2M |
| pool pump idle+wake | 47.2M | 38.9M |
| fiber managed pump | 51.0M | 41.3M |

On the 6-core set the pool’s consume-only spin matches Ant Farm’s own
spinning consumers, and the Fiber managed pump is within ~6%. Threadpool
machinery is not the limiter. SMT-12 spinning consumers fight over a
tiny serial payload and drop to ~30M; idle/fiber do better there only
because extra workers park. Use 6 physical cores for this job shape.

## Verification state

Passing from the self-contained repository:

- `dub test --compiler=dmd`
- `dub test --compiler=ldc2`
- `dub run -c unittest --compiler=ldc2 --build=release`
- `dub run -c unittest --compiler=ldc2 --build=release-debug`
- `dub run -c stress --compiler=dmd`
- `dub run -c stress --compiler=ldc2 --build=release`
- local LDC master plus matching master DRuntime release smoke/stress;
- 2,000 completion-versus-managed-join registrations and 100 rounds of 32-way
  completion/cancellation fan-out;
- 2,000 indexed timer-expiry-versus-cancellation generations across both forced
  orders and a freely racing third;
- 30 consecutive full LDC debug stress runs after repairing the early-wake
  park handshake loss found by that repetition;
- `consumeNext` production pumps plus `fiberParitySmoke` (cleanup, nested
  suspend, recycle, raw `Fiber.yield` failure);
- ST DRuntime-vs-Fiber-vs-payload and MT topology sweep
  (`benchmark_vs_druntime`); Farm-spin vs threadpool vs Fiber-pump embedding
  (`benchmark_farm_embed`);
- limit-8 lifecycle recovery/handler retry smoke and four-producer bounded
  lifecycle admission/drain stress;
- deterministic two-Farm cross-lane join/wake routing and 1,000-task shared-
  domain lane-isolation stress;
- remote-sweeper ring assignment, wake relay, and a native-disabled covering
  lane that completes every activation exactly once;
- deterministic cross-thread migration within the smoke suite;
- repeated large MT benchmarks which previously reproduced the hole/chain bugs.

The concurrent stress test races signal and cancellation. More forced repeated
migration and consumer-contention coverage remains planned.

Fiber TSan is unavailable as evidence: current matching instrumented-runtime
experiments crash inside the sanitizer/runtime boundary even for isolated
Fiber probes. DMD/LDC deterministic and stress tests remain the concurrency
path until that upstream boundary changes.
The earlier optimized-LDC `SIGILL` was the release build compiling
side-effecting test calls out of `assert` expressions; it was not a Fiber
context-switch failure.

## Current domain/lane baseline

Lifecycle ownership is now separated from locality:

```text
FiberDomain
    generations, roots, waits, timers, cancellation,
    lifecycle/failure events, admission/shutdown

FiberLane
    preferred LLC, Ant Farm, publish accumulator,
    local consumers and responder/sweeper coverage
```

A task belongs to one domain, but each runnable activation is assigned a
preferred lane. `FiberBackend` remains a compatibility alias for `FiberDomain`;
one domain can attach multiple threadpool lanes/Farms before admission. Yield,
external wake, and cross-lane join completion republish to the selected lane,
whose own notification mailbox is signalled. Any worker may legally resume it.
Multi-LLC liveness uses an Ant-Farm-like sweeper coverage graph: local workers
are native consumers and selected responders keep persistent remote consumers
for the next lane in a shared-domain ring (`FiberLane.cover` for an explicit
graph). A per-lane published-but-not-entered count treats Farm holes as
occupancy rather than emptiness. Loss of a native group is lossless when a
covering responder exists, and fails explicitly when no sweeper remains.

Mixed work uses two distinct Farm payload classes:

- ST one-shot fiber activations;
- MT countable parallel ranges with bounded grains and a completion-to-fiber
  bridge. `FiberDomain.parallelFor` publishes one MT payload on the current
  fiber's lane (`done` grains, `maxCs = 2`), parks on a private wait signal,
  and resumes when the last grain enqueues completion for the pump to drain.
  Grain bodies are `nothrow @nogc`; a throwing/allocating `foreach` remains a
  later wrapper. Ant Farm caps `done` at 512, so oversized ranges coarsen grain
  width rather than emitting multiple payloads.

`FiberEvent` (auto-reset or manual-reset) and `FiberSemaphore` park managed
fibers on private WaitSet keys with register-then-recheck so a `set`/`post`
cannot lose a wakeup between observing empty and inserting. Extra auto-reset
or semaphore waiters woken by a broadcast re-park if they lose the consume
race. Grain callbacks skip remaining work once the coordinating fiber is
cancelled; outstanding grains still complete so the range can retire.

## Next work

See `ROADMAP.md` execution order and `POSTMORTEM.md` suggestions. Immediate
standalone gaps are still task groups, a scheduler-aware condition variable,
and a Phobos adapter. UX/performance follow-ups that the characterization
actually supports: `spawn(void function())`, drain helper that does not copy
`ConsumerView`, mmap off the admission mutex, documented fiber vs payload
topology presets, and `subscribe` checks that survive `-release`.
