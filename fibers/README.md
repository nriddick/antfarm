# Ant Farm fibers

This is the Fiber subpackage of the authoritative Ant Farm repository. Its DUB
manifest resolves the root `antfarm` package and `../threadpool` by local path;
there are no duplicated vendored sources to reconcile.

This is a standalone scheduling layer over druntime's `core.thread.Fiber` and
Ant Farm. It deliberately does not own a thread pool. An embedding pool remains
free to choose worker count, topology, affinity, wake policy, and group cadence.

## TLS boundary

Farm dispatch does not preserve OS-thread identity. A suspended Fiber may
resume on another worker, and successive invocations or chunks of a bare
Payload may run on different workers. User code must therefore not carry a
TLS address, reference, pointer, cached access, or thread-relative resource
across a Fiber suspension or Payload dispatch boundary. Depending on TLS
continuity in such work is undefined behavior for this project. This includes
D/C thread-local variables, `errno`, thread-local allocator or runtime state,
locale, affinity, and similar resources.

Reading TLS and consuming the result entirely within one non-suspending
Payload invocation is ordinary thread-local use; treating that state as part
of a logical Payload spanning multiple invocations is not. The same restriction
applies to libraries called by dispatched code, not just direct TLS syntax.
Pinning is the caller's separate scheduling policy; this generalized backend
does not infer or enforce affinity for TLS-using work.

LLVM deliberately treats TLS addresses as stable across calls and can cache or
hoist them. It has no generally available equivalent of MSVC's `/GT` option,
so compiler optimization can expose violations that happen to work in other
builds. See [LLVM issue 19551](https://github.com/llvm/llvm-project/issues/19551).

The split is:

- `FiberDomain`: generations, roots, waits, timers, cancellation, completion,
  and lifecycle state shared by its tasks;
- `FiberLane`: one Farm and its LLC-local ready accumulator, worker tokens,
  consumers, and responder policy;
- `WaitSet`: fibers suspended for an indefinitely distant application signal;
- `PublishAccumulator`: newly spawned, yielded, or signalled fibers waiting in
  their selected lane to become a batch;
- Ant Farm: published ready work, consumed with `consumeNext()` so a worker
  visit finishes a table (or first-claimant-yields when the next table is
  live). `flushBatch` sizes that table; `consumeQuantum` is a test-only
  one-chunk probe.

For the bound threadpool path, create one `FiberLane` per LLC, call
`installFiberLanes(lanes, pool)`, and assign `fiberWorkerHooks()` to
`PoolOptions.managedWorker`. Each pinned worker then owns a persistent farm
consumer and small-producer token until its managed stop callback.
Constructing `FiberLane(farm)` creates a private one-lane domain for source
compatibility. To share lifecycle ownership across LLC lanes, construct one
`FiberDomain` and pass it to each `FiberLane(domain, farm)`. A task's external
wakes, yields, and cross-lane join completion return to its selected lane;
completions, lifecycle records, shutdown, and generation checks remain
domain-wide. `FiberBackend` remains an alias for `FiberDomain`, and
`lane.backend` remains the compatibility spelling of `lane.domain`.
`installFiberLanes` gives shared-domain lanes a ring of remote sweepers:
responders of lane *i* also consume and flush lane *i+1*. Call
`FiberLane.cover` before install for an explicit graph. Covering mailboxes
receive `FiberWakeEvent.remoteReady` when the destination lane becomes ready,
and a per-lane published-but-not-entered count keeps responders sweeping Farm
holes instead of treating them as emptiness. Remote sweepers need extra small
producer slots on the covered Farm.

Two setup presets encode the measured workload extremes. They are starting
points, not portable guarantees: the values below were characterized on a
Ryzen 5 5500 and should be rechecked on the deployment topology.

```d
import threadpool : CacheAwarePool, PoolOptions;

PoolOptions options;
applyFiberTopologyPreset(
    FiberTopologyPreset.fiberThroughput, lanes, options);
// all logical processors; lane.flushBatch=256, lane.avgCost=0

// Or, when short Farm payload throughput dominates:
applyFiberTopologyPreset(
    FiberTopologyPreset.payloadThroughput, lanes, options);
// skip SMT siblings; lane.flushBatch=32, lane.avgCost=2

auto pool = new CacheAwarePool(options); // managed Fiber hooks are installed
installFiberLanes(lanes, pool);
pool.start();
```

The fiber preset favors many concurrent `Fiber.call` sites. The payload preset
reduces ring contention between SMT siblings and uses smaller Farm tables.
Mixed workloads should benchmark both rather than infer a universal default.

Wake behavior is configurable before the pool starts. The default
`FiberWakePolicy.broadcast` preserves eager `pool.wakeAll()` on external spawn,
signal, cancellation, and cancelling shutdown. With `directorOwned`, those
operations instead coalesce `FiberWakeEvent` bits on the lane. The thread which
owns threadpool's `Director` takes the bits and chooses an LLC/class/label/number
selection to signal. This lets a responsive subset handle event latency while
the rest remain primarily cadence-driven consumers of whatever reaches the
Farm. An optional empty-to-nonempty notifier can wake the Director's surrounding
event loop without transferring Director ownership to the producer.

```d
import core.time : msecs;

void notifyControlLoop(void* context) nothrow @nogc;

foreach (lane; lanes)
{
    lane.wakePolicy = FiberWakePolicy.directorOwned;
    lane.responderSelector = (WorkerSelf* worker) {
        return worker.classIndex == 0; // example: only one chosen class
    };
    lane.setWakeNotifier(&notifyControlLoop, controlContext);
}

pool.start();
auto director = pool.director(); // retained by this control thread
director.labeled("fiber-responsive").wait();
director.labeled("farm-cadence").cadence(msecs(1));

// In the Director owner's normal event loop, after lane.signal/spawn/etc.:
foreach (llcIndex, lane; lanes)
{
    if (lane.domain.pendingCancellationRequests != 0)
        cast(void) lane.drainCancellationRequests();
    auto events = lane.takeWakeEvents();
    if (events != FiberWakeEvent.none)
        director.llc(cast(ushort) llcIndex)
                .labeled("fiber-responsive").signal();
}
```

`responderSelector` is evaluated once in managed worker start. Only selected
workers poll `TimerSet`, shorten their Director-controlled park to the next
fiber deadline, and actively sweep Farm holes/leftovers while runnable fibers
remain. Nonresponders still consume and publish Farm work whenever their own
Director cadence or policy runs them. Wake policy and responder predicates are
setup-time configuration and must not be mutated concurrently with workers.
Managed start acts as a coverage barrier: pumps do no scheduler work until all
selected workers have registered. Each active lane must then have at least one
responder or its workers fail explicitly. Installed lanes with no selected
workers become `FiberCoverage.noWorkers` and reject later `FiberLane.spawn`
calls. Rebinding a new pool resets and repeats coverage registration.

The notifier is a `nothrow @nogc` function pointer plus opaque context, suitable
for signalling an eventfd, self-pipe, event object, or caller-owned condition.
It fires only when the coalesced mailbox was empty and must not run arbitrary
user handlers.

Use `FiberLane.spawn` and `FiberLane.signal` from external threads so a parked
pool is woken. Terminal tasks are returned by
`lane.domain.takeCompletions()`; unhandled fiber exceptions are retained on
the task as `FiberOutcome.failed` and do not escape through Ant Farm.

Terminal tasks drained through `takeCompletions()` can be returned with
`release`/`releaseAll`. `spawn` then recycles the task object, its druntime
`Fiber`, and the fiber's mmap'd stack in place via `Fiber.reset`, so
steady-state spawning performs no GC allocation and no stack allocation;
the pool grows to the high-water mark of concurrently live fibers and is
bucketed by requested stack size. Recycling is explicit because a released
handle must not be read again: a later `spawn` reuses the same object.

Calling `enableLifecycleEvents()` before spawning enables an ordered control
lane with admitted, cancellation, terminal, and failure records. Producers use
four reusable per-task nodes; `takeLifecycleEvents()` performs the allocating,
GC-enabled copy on the Director side. With events enabled, both completion and
all emitted events must be drained before `release`. This keeps raw queue words
out of GC scans while ensuring records cannot be overwritten by recycling.
Lifecycle delivery reserves at most four records per admitted task against a
hard bound (65,536 records by default, or the argument to
`enableLifecycleEvents(limit)`). Admission throws `FiberLifecycleBackpressure`
before touching a pool slot when the reservation would exceed that limit.
Terminal classification returns unused capacity; bounded
`takeLifecycleEvents(maximum)` acknowledges copied records individually.
`handleLifecycleEvents(handler, maximum)` runs only on the Director side and
acknowledges after each normal return. A throwing handler leaves its record and
all later records queued with stable sequence numbers for retry.
Escaping `Error` becomes sticky fatal backend state, rejects further admission,
and is rethrown from a managed pump only after the Farm callback has returned.

Code which retains logical identity across recycling can capture
`task.handle` or use `spawnHandle`. `TaskHandle` contains the recyclable slot,
its per-slot generation, and a cached diagnostic hash. Exact identity is always
slot plus generation; the hash is for tracing only. `requestCancel(handle)`
queues that generation pair for the Director and refuses an already-stale
handle; `tryOutcome`/`tryException` likewise refuse stale generations.
`handle.poll()` returns one coherent status/outcome/failure/cancellation
snapshot. `handle.join()` and `handle.join(timeout)` use a manual-reset DRuntime
event to block an external OS thread without polling; they reject stale reused
generations, and timed waits report `FiberJoinStatus.timedOut`. A managed Fiber
gets `FiberJoinStatus.wouldBlock` instead of parking its worker; a
scheduler-aware Fiber calls `handle.joinFiber()` or `joinFiber(timeout)`.
Managed joins currently require both tasks to belong to the same backend and
return `wrongBackend` otherwise. They use an intrusive register-then-recheck
handoff, so completion cannot be lost between observing pending state and
suspending. Cancellation and timer expiry arbitrate removal exactly once.
Raw `FiberTask` identity retains the strict no-use-after-`release` contract;
the task overload of `requestCancel` snapshots its handle immediately.
Generation and cancellation tags are per slot; there is no global task counter
or generation operation on ordinary resume.

Cancellation has two bounded control-word sources. The currently executing
managed Fiber may call `FiberDomain.cancelCurrent()`. Every handle-based remote
request—including parent/sibling control—uses `FiberLane.requestCancel` or
`FiberDomain.requestCancel`; arbitrary submitters never mutate task control.
The one Director thread calls `drainCancellationRequests()`, which validates
the captured generation, coalesces repeats, removes a parked waiter if needed,
and wakes its lane. `directorCancelDetailed` is the synchronous Director-only
diagnostic/application form. The thread which constructs `FiberDomain` owns its
control lane; applying requests, shutdown cancellation, or releasing tasks from
another thread fails explicitly. Drain queued cancellation before
`release`/`releaseAll`.

`beginShutdown(false)` rejects new fibers and drains cooperative work.
`beginShutdown(true)` additionally requests cancellation, wakes indefinite
waiters, and lets their cleanup scopes unwind through `FiberCancelled`.
Cooperative cancel is a CANCEL addend on the task's control word, not a twin
of the running/ready/waiting phase.

`FiberEvent` and `FiberSemaphore` are scheduler-aware: `wait` parks a managed
Fiber on a private WaitSet key, while `set`/`post` may run from any thread.
Auto-reset events and the semaphore wake every waiter on the key; extras that
lose the consume/decrement race re-park. Manual-reset events stay posted until
`reset`.

`FiberDomain.parallelFor(length, grain, fn, context)` from a managed Fiber
publishes one multithreaded Farm payload on that fiber's selected lane and
parks until every grain has run. `fn` is `nothrow @nogc` and receives
`(index, count, context)`; it must not yield. A range that would need more
than 512 grains is coarsened to fit Ant Farm's iteration cap. This is the
low-level working set a later parallel-foreach wrapper can sit on.

`Fiber.yield()` is not a scheduler suspend. A spawned body that yields without
`yieldReady`/`await`/`sleep*` fails that generation with a retained exception.
`Thread.sleep` blocks the worker and is not timer registration. Bound how long
a worker stays in one Farm table with `FiberLane.flushBatch`, not by skipping
the rest of the table.

`FiberBackend.sleepUntil` and `sleepFor` register managed timers. The lane
returns its nearest absolute deadline through `ManagedPumpResult`; threadpool
then caps its Director-controlled park at that deadline. Timers therefore need
neither a dedicated timer thread nor cadence polling. `TimerSet` is an indexed
binary min-heap ordered by deadline and insertion sequence: nearest-deadline
queries are O(1), insertion/cancellation are O(log N), and the pump pops
expired tasks one at a time without constructing a result array.

Workers subscribe a `ConsumerView`, call `consumeNext()`, and periodically
flush an accumulator with their own producer `Token`. `PublishAccumulator` is
an intrusive lock-free MPSC queue (one atomic exchange per push), and `flush`
publishes a stolen chain as a lazy `PayloadEntry` range: every entry shares
one prebuilt resume header and the task's own embedded payload word, so
flushing allocates nothing and requeues only the unpublished suffix.
`WaitSet` buckets are intrusive chains under 64 striped locks, so unrelated
signals never contend and registration allocates nothing. Waiters carry both
bucket links and a generation tag, making individual cancellation O(1) while
whole-signal fan-out remains O(1) to detach. The default flush batch is 32;
callers can explicitly request up to the fixed 256-entry stack snapshot cap.

For a one-thread embedding, keep the subscribed cursor in the same stack
frame and pass it by reference to the provided drain helper. Subscription is
checked with an exception so release builds cannot remove the operation:

```d
auto token = farm.registerProducer(Tier.small);
ConsumerView consumer;
subscribeOrThrow(consumer, farm);
scope (exit) consumer.unsubscribe();

domain.spawn(&moduleLevelBody); // no delegate/closure allocation
// Capturing bodies remain supported: domain.spawn({ use(localState); });
drainUntilEmpty(domain, token, consumer, 256, 0);
domain.releaseAll(domain.takeCompletions());
```

`drainUntilEmpty` also polls managed timers. It intentionally waits forever
when a task is parked on an application signal that nobody delivers; cancel or
signal such tasks before using it as a shutdown drain. Do not wrap
`ConsumerView` in a by-value helper or put `subscribe` inside `assert`.

Shared scheduler state uses separately allocated, reference-free control
records with 64-byte alignment and cache-line-sized strides. This includes the
DRuntime event used for joins, whose OS synchronization storage is isolated
from the GC task object. Managed-join inbound lists and outbound membership use
separate aligned lines, and the backend-wide managed-waiter count has its own
line. The lifecycle reservation count is likewise isolated. GC owner objects
continue to contain every `Fiber`, delegate, exception, and task reference; the
manual control records are intentionally not GC scan ranges. Before the
first raw task address is published, `FiberBackend` explicitly roots itself
with the GC. That root remains through the exchange and completion queue and is
removed only when both completion and any enabled lifecycle records have
retired the final task from the backend's roots array.

`directorCancelDetailed(handle)` reports whether a generation was stale,
already requested, or already terminal; whether cancellation won before first
entry; or whether a request reached running, ready, or waiting work. A pre-entry
win never calls the user delegate. Requests against executing work remain
cooperative. If cancellation unwinding itself throws an unrelated exception,
the task is `failed` and retains that exception; the cancellation request
remains independently observable. Terminal
`CancellationDisposition` distinguishes acknowledged unwind/pre-entry,
abrogation of a normal return after the request won, and cleanup failure.
`TaskHandle.tryCancellationDisposition` provides the generation-stable read.

The important boundary for a possible Phobos integration is an adapter around
worker lifecycle and wake-up. Fiber lifecycle, signalling, batching, and the
farm transport have no dependency on `std.concurrency.FiberScheduler`,
`std.parallelism`, or the local `threadpool` package.

Run the smoke test from this directory with:

```text
ANTFARM_HUGE_PAGES=0 dub test
```

Open facilities and acceptance work are tracked in `ROADMAP_pruned.md`.
Cancellation remains cooperative for a Fiber that never reaches a scheduler
boundary.

`dub run -c stress` exercises concurrent signalling and cancellation. TSan
also requires an LDC druntime built with `SupportSanitizers`; the ordinary
distribution runtime does not bracket fiber stack switches and TSan crashes
there before producing useful race reports.
