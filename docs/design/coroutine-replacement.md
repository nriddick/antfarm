# Coroutine replacement: keep druntime first, move ownership to workers

**Proposal, with an isolated mechanism experiment.** The current
`ant_fibers` scheduler is unchanged, and Fibers remain outside the C ABI.
Use druntime Fibers for the first replacement prototype. Reuse the bounded
work-stealing deque implementation, but build a coroutine executor whose
local ready work, foreign admission and overflow do not pass through the
Farm ring. Replacing the queue alone leaves the centralized lifecycle costs
in place.

The [warm reuse measurements](../performance/coroutines-2026-10-25/README.md)
support this direction on the 5900XT: at 32 dispatch threads a minimal deque
executor completes 141.77 M warm Fiber lifetimes/s versus 35.80 M through a
shared Farm; with three yields those rates are 23.18 M and 8.25 M, or 92.74 M
and 32.98 M dispatches/s. Existing FiberDomain's complete lifecycle measures
1.85 M and 1.17 M lifetimes/s. Those last comparisons have different feature
sets and are not a prediction of replacement speed. At one thread the Farm
beats the deque, so keep a simple owner-local path when there are no thieves.

## First replacement: druntime contexts, worker-owned scheduling

Use a distinct experimental D package until its lifecycle matches
[the existing scheduler contract](ant_fibers.md). Do not route a Fiber through
`forkjoin.Dispatcher` unchanged: that dispatcher's foreign roots and overflow
still use the Farm, and its run-to-completion node accounting does not own a
suspended Fiber's waits or lifetime. Reuse `forkjoin.deque`, topology/parking
machinery and the proven frame-pool ownership pattern, not an actor or
fork-join node's completion contract.

Each worker owns:

- A bounded ready deque containing stable activation tickets, plus a small
  owner-only overflow list. Only the owner pushes/pops the bottom. Thieves
  claim the top. Prefer same-LLC victims, with an explicit cross-LLC policy.
- An intrusive MPSC inbox for external admissions and remote readiness. An
  external producer never mutates the deque's owner end. The owner detaches a
  batch and publishes it to its deque. Use a proven reclamation-safe MPSC
  algorithm; do not improvise a Treiber pop stack of recyclable nodes.
- A stack-size-binned warm Fiber pool, prewarmed before measurement. The worker
  that finishes a task can retain the stack locally, or batch returns to its
  allocator owner. Give both policies a measurement; a return to one global
  mutex-protected pool defeats locality.
- Local timer ownership and batched completion records. Do not allocate a new
  completion array per drain. The caller supplies output storage and receives
  an accepted prefix/count. Keep lifecycle logging separately bounded.

Admission allocates/reserves a descriptor and generation before publication.
For the warm path, use preallocated descriptor/stack reserves and return
explicit exhaustion/backpressure; report cold stack construction separately.
Publish after all callbacks, contexts and ownership credits are initialized.
An intrusive ticket needs a queue reference and a generation, not a borrowed
pointer into a Farm table. The descriptor stays allocated through all stale
wake/cancellation references; generation comparison alone cannot make a freed
pointer safe. Roots must retain the Fiber, its stack-visible objects and its
D delegate context for the GC.

For local spawn and cooperative yield, publish to the local runnable structure
only after the old `Fiber.call` has returned. A deque's exclusivity does not
make it safe to expose a still-running stack. A yielded Fiber may migrate only
under druntime's actual platform/compiler constraints, and user code must not
retain thread-local addresses or thread-affine locks across a yield. The
installed LDC runtime exposes `migrationUnsafe`/`allowMigration`; an explicit
opt-in is valid only when the body obeys that restriction. The experiment's
body deliberately contains no TLS-dependent operations. See the official
[Fiber contract](https://dlang.org/phobos/core_thread_fiber_base.html) and the
installed runtime source hashes in the measurement metadata.

## State and ordering that must survive the queue change

Keep an explicit generation-tagged lifecycle, for example:

```text
free -> admitted/ready -> running -> parking -> parked -> ready
                              |          |                  |
                              +----------+------> terminal -> reclaimable -> free
```

These are conceptual phases; the implementation should pack the state, queue
credit and wake-pending bit so each ownership transition has one arbiter.

1. **Claim:** a deque/inbox claim gives one worker the activation ticket.
   Acquire its published generation/state; stale or already-terminal tickets
   only release their queue reference. Exactly one worker changes ready to
   running. No concurrent `Fiber.call` is permitted.
2. **Park:** register the wait while the task is running, publish a parking
   intent, then switch out. A signal/cancel/timeout before the switch finishes
   records a wake-pending bit; it cannot publish a second runnable activation.
   After `Fiber.call` returns, the owning worker atomically settles parked
   versus ready and transfers the single execution credit.
3. **Wake:** one winner claims the wait and only it publishes. `ant_fibers`
   now does this with a per-task claim word (see
   [the scheduler design](ant_fibers.md#waits-timers-joins-and-cancellation)):
   each wait arms a fresh count, every waker CASes it, timer entries carry
   the exact word they may claim, and waiters unlink themselves after they
   resume. Reuse that protocol, and its arm/cancel fence pairing, rather than
   reintroducing removal-wins arbitration; per-worker timer heaps then need
   no cross-container locking. Keep the deterministic schedules for the two
   races fixed earlier on the parent branch.
4. **Publish/wake worker:** release-publish a ticket, then use the pool's
   arm/recheck protocol with the required publication-to-armed-read fence.
   Check local deque, owned overflow, remote inbox, and due timer/reactor work
   before sleeping. Failed stealing is not global quiescence.
5. **Complete/recycle:** store result/exception/cancellation disposition, join
   waiters and publish completion before making the stack reusable. Release
   queue/wait/timer/I/O references and diagnostic retention credits first.
   Use generation-safe handles to make an old completion harmless after reuse.
6. **Stop:** close admission, wait for in-flight producers to exit, request
   cooperative cancellation where required, settle waits and external I/O,
   drain terminal records, join workers, then free queues/roots/stacks.
   Count every admitted task independently of its current queue membership.

A pending cancellation cannot discard a suspended stack with live cleanup
scopes. Resume it through cooperative cancellation/cleanup; report cleanup
failure separately. An external OS thread must never unwind another Fiber.
An opaque terminal result can be retained after the stack is recycled only
when its retained metadata has an independent lifetime.

## Fairness, capacity and topology

Immediate LIFO requeue can keep running one warm Fiber and starve an old
ready Fiber. The experiment's oldest-item probe every 32 dispatches has
similar throughput, but it is only evidence that a fairness mechanism need
not erase the gain. Specify a scheduling quantum: bound local continuations,
poll foreign inboxes and timers on a fixed cadence, and periodically choose
oldest ready work. Suspended I/O work should resume on its reactor owner by
default; freely stealable compute Fibers can use same-LLC-biased stealing.

If a local deque fills, retain the ticket in owner-controlled storage and
service that storage before sleeping. Do not recursively run a yielding
Fiber as an overflow fallback: it can strand the continuation or consume
unbounded native stack. External admission consumes a bounded descriptor
credit before enqueuing; no credit means an explicit refusal, not hidden
allocation or silent loss. Batch import and overflow redistribution should
preserve one queue reference per ticket.

Start with `CacheAwarePool` raw bodies or a GroupHost-backed experimental
host. GroupHost currently creates Farms even if a coroutine executor never
uses them; a later pool-only host can remove that cold allocation. Do not
accidentally claim "ring-free" while using the fork-join dispatcher's spill
path. An io_uring submission/completion ring is a separate kernel I/O ABI,
not the Farm dispatch ring being removed.

## Measurement and promotion plan

The primary metrics are **completed warm Fiber lifetimes/s** and
**resume dispatches/s**, reported separately. A yielded task can contribute
many dispatches but only one completion. Keep cold construction, reserved
virtual stack bytes, resident bytes and stack reuse rate as separate metrics.
The baseline uses 1/2/4/8/16/32 total dispatch threads, 64 warm Fibers per
thread, and zero/three yields. Repeat with a fixed total pool so changes in
working set do not masquerade as scaling.

Build the full druntime-backed replacement in four reviewable increments:

1. Ready tickets, owner queues/inboxes, bounded credits, warm pool and
   completion drain. First prove exactly-once admission and zero *warm*
   allocations; compare the complete lifecycle against existing FiberDomain.
2. Cooperative yield, migration and parking. Force last-item steal races,
   publication during worker arming, full queues, skewed bursts, nested spawn
   and cross-LLC steals. Measure p50/p99/max ready latency as well as throughput.
3. Generation-safe waits/joins/timers/cancellation and shutdown. Port the
   deterministic detached-waiter and timer-expiry regressions; add wake-before-
   park, stale-ticket-after-reuse, simultaneous cancellation/completion and
   admission-vs-stop cases. Carry the existing release/death and TSan switch
   bridge obligations into this package.
4. Match exception cleanup, GC-on behavior, signal/debugger interaction and
   native Windows behavior. Then measure mixed compute/I/O and choose defaults.

For every step, compare identical lifecycle features and callback bodies in
paired randomized runs, with allocation counts and distribution ranges. A
production switch needs a repeatable multithread improvement, no material
single-thread regression (use the serial fast path if needed), bounded memory,
no lost wake/work, and acceptable p99 latency under skew and external producers.
The mechanism rates are headroom estimates, not acceptance promises. Current
measurements contain no full-feature deque replacement or I/O throughput.

## A later custom coroutine system: two explicit tiers

Share executor tickets, admission/backpressure, timers and completion-lifetime
rules. Keep execution context implementations and their promises distinct.
Do not silently route a capability-heavy task through the restricted tier.

### Tier 1: druntime Fiber compatibility

The first implementation of this tier should simply keep druntime's context
engine. A custom stackful engine becomes justified only after profiling the
new full scheduler shows that context switching or druntime integration is the
remaining bottleneck. The direct warm-reuse ceiling is much higher than
current FiberDomain throughput; replacing assembly first does not address the
measured gap.

"Full parity" is a testable contract, not just preservation of callee-saved
registers. For every supported OS/ABI it must cover:

- Function/delegate entry, nested call/yield, current-Fiber identity, terminal
  state and reset/rebind semantics; propagate or capture unhandled exceptions
  as the caller requested.
- D cleanup/destructors and exception unwinding on the Fiber stack; valid
  unwind metadata, debugger/profiler stack walks, register/stack alignment and
  platform context state required by the calling convention.
- Correct GC reachability for suspended and running stacks and saved registers,
  stack registration/unregistration, bounds and thread stop/resume coordination.
  Native malloc buffers or a saved stack pointer alone do not provide this.
- Guard pages, stack bounds, failure behavior and allocation/reuse policy;
  current migration/TLS restrictions, nested/thread-affine operations, runtime
  thread attachment and signal integration.
- TSan/ASan fiber-switch annotations and the existing migration bridge model,
  plus cancellation/joins/timers/lifecycle promises supplied by the scheduler
  above the context engine.

Implement this behind a context-backend interface and run the same semantic
suite against druntime and the candidate. Differential tests must include GC
while tasks are parked, exceptions through nested cleanup, migration, terminal
reset and repeated pool reuse. Prefer an upstream-supported druntime hook or
a deliberate runtime integration patch over reaching into private GC/TLS
layouts. A Linux-only context-switch microbenchmark cannot establish full
Windows parity.

### Tier 2: native async I/O continuations

Make this a restricted, normally reactor-affine **stackless** continuation
backend first. A fixed-size or size-classed frame holds explicit state, buffer
ownership and its resume function. It has no per-operation stack allocation or
suspended D stack for the GC to scan. D currently needs explicit state machines,
generated adapters or a suitable compiler transformation for this interface;
ordinary blocking-looking D code does not automatically become stackless.
If ergonomic stackful I/O is later required, measure a restricted stackful
backend separately rather than promising the stackless cost model.

The fast tier's contract should require nonthrowing, non-GC callbacks,
explicit completion/error values, stable owned buffers, explicit cancellation,
and no arbitrary deep-stack suspension. Reactor affinity is a default
performance/lifetime rule, with deliberate transfer for compute offload. Do
not promise druntime Fiber parity here; a caller chooses Tier 1 when it needs
ordinary D stack semantics.

On Linux, batch io_uring SQ submissions and CQ completions with one owner per
reactor. Consider epoll where readiness or platform support is the right fit;
on Windows use IOCP with an equivalent ownership model. A completion enqueues
a local continuation or a message to its owner, not a Farm payload. Each
in-flight operation holds the frame/buffer alive through its terminal CQE.
Cancellation and the original operation complete separately and their CQE
order is not guaranteed; the cancel acknowledgement is not a buffer-reuse
fence. Multishot/zero-copy operations need their own final-notification rules.
See the primary [liburing cancellation manual](https://man7.org/linux/man-pages/man7/io_uring_cancelation.7.html)
and [io_uring entry documentation](https://kernel.googlesource.com/pub/scm/linux/kernel/git/axboe/liburing/+/refs/heads/master/man/io_uring_enter.2).

Performance acceptance for this tier needs actual loopback/network and storage
runs: operations/s, p50/p99/p99.9 latency, CPU per operation, syscalls/batching,
outstanding depth, memory per connection, fairness, and cancellation/error
rates. Vary message sizes, ready/already-complete vs genuinely pending work,
producer counts and 1–32 reactor/worker threads. Compare both a plain reactor
callback baseline and Tier 1 on the same I/O workload. Warm Fiber dispatch rates
alone say nothing reliable about those I/O outcomes.

## Recommendation

Proceed first with the druntime-backed, worker-owned executor and retain the
current scheduler until parity tests and matched measurements justify changing
it. Share its scheduling and ownership layer with a separate stackless I/O
backend. Defer a custom full-parity context engine until profiling provides a
specific reason to replace druntime. This orders the work around the observed
costs while keeping both future feature tiers achievable and honest.
