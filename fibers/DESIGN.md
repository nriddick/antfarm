# Fiber scheduler design

`antfarm-fibers` is a freely migrating DRuntime Fiber scheduler built on Ant
Farm. It owns suspendable task lifecycle; Ant Farm transports only runnable
activations, and `threadpool` supplies pinned workers and wake policy when the
scheduler is used concurrently.

This document records the current 1.6 design and its synchronization
invariants. Start with [README.md](README.md) for usage and
[../ARCHITECTURE.md](../ARCHITECTURE.md) for the relationship between packages.

## Execution model

```text
spawn / yield / signal / timer / cancellation
                    |
                    v
          PublishAccumulator (MPSC)
                    | batched flush
                    v
              Ant Farm tables
                    | consumeNext
                    v
              resumeCallback
                    |
                    v
                 Fiber.call
             +------+------+
             |             |
          terminal      ready / wait
             |             |
             v             +----> republish when runnable
      completions MPSC
             |
      takeCompletions -> release -> recycle
```

Every runnable activation is a one-shot serial Farm payload:

```text
maxCs = 1
done  = 1
body  = one ulong containing the FiberTask address
```

All activations share one prebuilt callback/header. A flush presents a forward
range of fixed one-word bodies to Ant Farm's common-header, fixed-length
`write` overload. Waiters, timers, cancellation state, exceptions,
completions, GC roots, and recycled stacks never enter the Farm ring.

The scheduler deliberately has no parallel-loop abstraction. Countable Farm
payloads and suspendable Fibers remain separate work representations.

## Domain, lanes, and tasks

`FiberDomain` owns application-wide lifecycle state:

- task generations and active GC roots;
- wait sets, timers, cancellation requests, and managed joins;
- completions and optional lifecycle records;
- admission, shutdown, and recycled task/stack pools.

`FiberLane` owns locality and transport state:

- its preferred LLC and Ant Farm;
- its runnable publication accumulator;
- local consumers and responder/sweeper coverage;
- batching parameters and its wake mailbox.

A task belongs to one domain. Each runnable generation is assigned a preferred
lane, but any eligible consumer may resume it. One domain can attach multiple
lanes before admission. `FiberBackend` remains a compatibility alias for
`FiberDomain`.

`FiberTask` owns one DRuntime `Fiber`, scheduler state, a retained failure,
intrusive queue links, stack-size class, and migration diagnostics. Cold spawn
creates and maps a stack. Explicit release pools terminal tasks by stack size;
later spawn reuses the task and stack with `Fiber.reset`.

Each admitted generation increments the slot's 64-bit generation. A
`TaskHandle` is the stable `(slot, generation)` identity. A raw `FiberTask`
reference is valid only until `release`; handle operations validate the
generation before observing or changing task state.

## Task states and scheduler boundaries

A live generation occupies exactly one scheduler disposition: running,
publishable, published, waiting, terminal/completing, or explicitly recycled.
At most one thread may execute it.

The supported suspension boundaries are:

- `FiberDomain.yieldReady()` for voluntary republishing;
- `await`, `FiberEvent.wait`, and `FiberSemaphore.wait` for application waits;
- `sleepFor` and `sleepUntil` for timer waits;
- `TaskHandle.joinFiber` for managed joins;
- cooperative cancellation checks at scheduler boundaries.

Calling `core.thread.Fiber.yield()` directly does not publish an activation. If
a body returns to the scheduler in HOLD without recording a scheduler wait, the
generation fails instead of being silently stranded. `Thread.sleep` blocks the
entire worker and is not a scheduler operation.

Normal return yields `completed`. An uncaught `Throwable` is retained and
yields `failed`. A winning cancellation throws `FiberCancelled` inside the
Fiber so cleanup scopes unwind, then yields `cancelled`. A different exception
during cancellation cleanup remains a failure rather than being masked.

## Publication and backpressure

`PublishAccumulator` is an intrusive MPSC queue. A producer links a runnable
task without allocation. Flush steals a FIFO chain, copies task references to
a fixed stack snapshot, and detaches every queue link before calling
`AntFarm.write`.

Detachment is required: an immediately consumed prefix may become runnable
again and reuse its intrusive queue word while the flushing thread still owns
the unpublished suffix. A partial Farm write reconstructs and republishes the
entire unwritten suffix, so backpressure never drops an activation.

Production pumps use `consumeNext()`. It finishes a table unless the Farm's
first-claimant rule yields it. `consumeQuantum()` is retained only as a
one-resume test probe; it is not a production latency control.

## Waits, timers, joins, and cancellation

`WaitSet` uses mutex-striped signal maps with intrusive doubly linked waiter
chains. Individual removal and whole-signal detach are O(1). `FiberEvent` and
`FiberSemaphore` register and then recheck their state so a signal cannot be
lost between observing it and parking. A reusable `FiberGenerationTrigger`
retains one empty map bucket after first use, avoiding delete/reinsert
allocation on every generation; ordinary events, semaphores, arbitrary
application signals, and one-shot timer keys are removed normally. Trigger
instances are intended to be stable streams because the retained bucket lives
until its domain does.

`TimerSet` assigns private signals and stores them in an indexed binary
min-heap ordered by `(deadline, sequence)`. Nearest-deadline lookup is O(1),
insert and removal are O(log N), and expiry does not allocate after the heap
has reached its high-water capacity.

Managed joins use a generation-checked intrusive list on the target. Target
completion, joiner cancellation, and timer expiry arbitrate removal so exactly
one path wakes the joiner. External thread joins use a reusable manual-reset
event and a registration gate that prevents release/reuse from resetting the
event beneath an older generation.

Arbitrary threads request cancellation by appending a GC-visible
`(slot, generation)` handle to the domain control queue. Only the Director
applies those requests. The currently executing Fiber may cancel itself
directly. Cancellation is cooperative: a body that never reaches a scheduler
boundary cannot be forcibly unwound.

## Control-word arbitration

A 64-bit task control word holds the phase in its high half and counted
`CANCEL`, `ENTER`, `WAKE`, and `PARK` fields below it. Outsiders add `CANCEL` or
`WAKE`; the exclusive Farm consumer adds `ENTER`, `PARK`, and phase deltas. A
whole-word store is legal only while the slot is unpublished.

PARK and WAKE compose in either order. PARK-first and WAKE-first each elect
exactly one publisher, avoiding a separate signalled phase and a first-wins
CAS. ENTER and CANCEL compose similarly: a cancellation which precedes first
entry suppresses the body.

For a running task, the old word returned by the running-to-terminating
transition is the cancellation cutoff. A request visible at that cutoff
abrogates a normal result. A later counted Director request is ineffective and
cannot rewrite the terminal disposition.

## Memory-order invariants

The primary invariant is that writes made before a Fiber suspends happen-before
the next thread calls it. The handoff chain is explicit at every boundary.

| Handoff | Publication | Acquisition |
| --- | --- | --- |
| spawn/reset to runnable | initialized task, then MPSC release link | accumulator acquire while stealing |
| yielded/woken task to runnable | control-word transition, then MPSC release link | accumulator acquire |
| accumulator to Farm | snapshot copy, then Farm sentinel release | consumer sentinel acquire |
| Farm to executing Fiber | sentinel acquire, then ENTER/phase transition | precedes `Fiber.call` |
| event post to waiter | event state under stripe lock, then WAKE | PARK/WAKE arbitration and ready publication |
| timer/cancel to waiter | one remover wins timer/stripe ownership | ready MPSC/Farm chain |
| remote cancel to Director | append under request mutex | swap under the same mutex |
| terminal data to completion | outcome/failure stores, then completion MPSC release | completion acquire or stable handle snapshot |
| terminal data to thread join | terminal transition, then event set | event wake, then generation-stable acquire read |
| lifecycle event to Director | initialized record and release publication | acquire drain into ordered backlog |
| completion to drained | completion push before active-count decrement | acquire observation of zero active tasks |
| release to reuse | Director ownership and recycle-pool mutex | initialization before the next ready publication |

The Farm portion depends on the root implementation writing table contents
before its release sentinel and consumers acquiring that sentinel before
reading the body. Ant Farm 1.6 provides those edges.

## Container ownership

- `queueNext` belongs to a `PublishAccumulator` only while a task is queued.
  Flush detaches it before Farm publication.
- `waitNext` belongs either to `WaitSet` or the recycle freelist, never both
  for a live generation. Wait stripes and the recycle pool use separate locks.
- Consumers and producer tokens are persistent and worker-owned. Remote
  responders register and destroy their covering views on that same worker.
- Cancellation requests live in a GC-visible handle array protected by the
  request mutex. Only the domain's Director swaps and applies it.
- Managed-join inbound-list and waiter-membership state occupy separate aligned
  control records and have separate ownership.
- `currentTask` is D TLS rewritten for every resume. It is a resume-local
  association, not Fiber-affine TLS.

## GC visibility and allocation

Farm bodies and intrusive integer links are not treated as GC roots. Before
first publication, the domain registers itself as an explicit root; its roots
array retains every active task until completion and any enabled lifecycle
records have been taken. The task retains its DRuntime Fiber, and DRuntime
registers suspended stacks for scanning.

Reference-free control records are manually aligned to isolate contended task,
wait, admission, timer, wake, join, and queue state. GC-visible delegates,
Fibers, exceptions, requests, and task references remain in ordinary scanned
owner objects.

After warm-up, the ready path does not allocate: MPSC publication, snapshot
flush, fixed-width Farm write, callback, ready republish, established waits,
timer expiry, lifecycle publication, and recycled `Fiber.reset` reuse are all
allocation-free. Cold Fiber/stack creation, roots or heap growth, new signal
keys, returned drain arrays, shutdown snapshots, exceptions, and user code may
allocate.

## Migration and TLS

Free migration is the shared-Farm contract. Stack locals move with the Fiber;
OS-thread state does not. A body must not carry TLS-derived addresses or
references, `errno`, locale, affinity, floating-point environment, or a
thread-relative library resource across suspension. Reading and consuming TLS
within one non-suspending slice is ordinary thread-local use.

`resumeCount`, `migrationCount`, and `lastResumeIdentity` are diagnostics. They
do not create affinity. Pinning, if needed by an application, is a separate
embedding policy.

## Pool integration and wake ownership

The intended concurrent layout installs one `FiberLane` per LLC. Each managed
worker keeps persistent Farm consumer and producer registrations. Selected
responders poll timers, accept timer-bounded parks, and sweep Farm holes and
leftovers; startup validates that every active lane has responder coverage.

The default `broadcast` wake policy signals the pool for external admission,
signal, cancellation, shutdown, and worker publication. Under
`directorOwned`, lanes coalesce those reasons into a mailbox. The thread that
owns `pool.director()` drains it and chooses which workers to signal. An
optional `nothrow @nogc` notifier fires on an empty-to-nonempty mailbox edge so
an external event loop can wake without running arbitrary handlers on workers.

The multi-lane model and covering responder graph are implemented, but
multi-LLC hardware validation remains a post-1.6 bridge. Current release
evidence does not claim multi-LLC performance or failover guarantees.

## Completion, lifecycle records, and shutdown

Terminal tasks enter the completion MPSC and remain rooted. The Director calls
`takeCompletions`, inspects immutable outcomes, and then calls `release` or
`releaseAll`. Release refuses unsafe reuse while cancellation or join state for
the generation remains outstanding.

Optional lifecycle reporting reserves bounded capacity before admission and
emits ordered admitted, cancellation, terminal, and failure records. A
throwing Director handler preserves its untouched suffix for retry. Lifecycle
records and completions must both be retired before the final task root is
removed.

`beginShutdown(false)` closes admission and drains cooperative work.
`beginShutdown(true)` also requests cancellation and wakes indefinite waiters.
The application must still bound or otherwise control user code that never
reaches a scheduler boundary.

## Validation boundary

Smoke and stress coverage includes forced migration, GC retention across
migration, park/wake and timer/cancel races, generation reuse, joins,
cancellation cutoff behavior, lifecycle backpressure and retry, cross-lane
wakes, responder coverage, and repeated concurrent drain.

ThreadSanitizer is not currently evidence for migrating DRuntime Fibers: a
matching instrumented runtime fails at the sanitizer/runtime stack-switch
boundary even for isolated probes. Until that boundary changes, the supported
concurrency evidence is the DMD/LDC debug and release matrix plus deterministic
race tests and repeated stress. See [../ROADMAP.md](../ROADMAP.md) for the
current release gates.
