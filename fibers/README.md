# Ant Farm fibers

`antfarm-fibers` runs freely migrating DRuntime Fibers on Ant Farm work lanes.
It supplies Fiber lifecycle, waits, timers, cancellation, joins, completion,
and stack recycling; it deliberately leaves worker topology and wake policy to
the embedding application.

Use a Fiber when work needs ordinary D control flow, allocation, cleanup
scopes, or suspension. Use a bare Ant Farm payload for short
`nothrow @nogc` work that never waits.

| Work shape | Use |
| --- | --- |
| Short non-suspending compute | Ant Farm payload |
| Wait, yield, sleep, join, allocate, or throw | Managed Fiber |
| OS-thread-affine or TLS-dependent operation | Finish it without suspending |

A single-threaded DRuntime Fiber scheduler generally outperforms Ant Farm
Fibers when the goal is to create and run many transient Fibers, or to execute
asynchronous control flow which does not need concurrency. Use DRuntime
directly for that shape.

An Ant Farm Fiber lane is a compatibility layer for work which needs the raw
cycles of the shared worker pool but is cumbersome to express as an
`@nogc nothrow` payload, or which naturally composes as “wait for this
prerequisite, then continue.” It lets that work share workers with fast bare
payloads while retaining ordinary D control flow, scheduler-aware waits, joins,
timers, and cooperative cancellation.

The full three-layer arrangement is described in
[../ARCHITECTURE.md](../ARCHITECTURE.md).

## Build and test

From this directory:

```text
ANTFARM_HUGE_PAGES=0 dub test --compiler=dmd
ANTFARM_HUGE_PAGES=0 dub test --compiler=ldc2
ANTFARM_HUGE_PAGES=0 dub run -c stress --compiler=ldc2 --build=release
```

## Run one lane on the current thread

[examples/hello_fibers.d](examples/hello_fibers.d) is the smallest complete
program. Its essential lifecycle is:

```d
auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                           DEFAULT_SMALL_TABLE_THRESHOLD, false);
scope (exit) farm.destroy();

auto domain = new FiberDomain(farm);
auto token = farm.registerProducer(Tier.small);
scope (exit) farm.unregisterProducer(token);

ConsumerView consumer;
subscribeOrThrow(consumer, farm);
scope (exit) consumer.unsubscribe();

domain.spawn(&fiberBody);
drainUntilEmpty(domain, token, consumer, 256, 0);

auto completed = domain.takeCompletions();
domain.releaseAll(completed);
```

`drainUntilEmpty` takes `ConsumerView` by reference and also polls timers. It
waits indefinitely if a task is parked on an application signal that nobody
will deliver; signal or cancel such tasks before shutdown drain.

Completed tasks are not automatically recycled. Drain them with
`takeCompletions`, inspect their outcomes, then call `release` or `releaseAll`.
Later spawns reuse both the task and its stack with `Fiber.reset`.

## Fiber bodies

A body may return normally, throw, or suspend at scheduler operations:

```d
void fiberBody()
{
    scope (exit) cleanup();

    FiberDomain.yieldReady();
    FiberDomain.sleepFor(msecs(1));
    FiberDomain.await(applicationSignal);
}
```

- Return produces `FiberOutcome.completed`.
- An uncaught exception produces `FiberOutcome.failed` and is retained on the
  task; it does not escape through the Farm callback.
- Cooperative cancellation unwinds through `FiberCancelled` at the next
  scheduler boundary.
- `FiberDomain.yieldReady()` republishes the task.
- `await`, `FiberEvent.wait`, `FiberSemaphore.wait`, `sleep*`, and
  `TaskHandle.joinFiber` park without blocking the worker.

Do not use `core.thread.Fiber.yield()` as a scheduler operation. A body that
does so is failed rather than left stranded. `Thread.sleep` blocks the entire
worker; use `FiberDomain.sleepFor` or `sleepUntil`.

## Bind lanes to the cache-aware pool

The intended concurrent embedding uses one `FiberLane` per LLC and one shared
`FiberDomain` for application-wide lifecycle ownership.

```d
auto topology = CacheAwarePool.topology();
auto domain = new FiberDomain;
AntFarm*[] farms;
FiberLane[] lanes;

foreach (llc; 0 .. topology.llcCount)
{
    immutable workers = workersOnLlc(topology, llc);
    auto farm = makeFarm(workers);
    farms ~= farm;
    lanes ~= new FiberLane(domain, farm);
}

PoolOptions options;
applyFiberTopologyPreset(
    FiberTopologyPreset.fiberThroughput, lanes, options);

auto pool = new CacheAwarePool(options);
installFiberLanes(lanes, pool);
scope (exit) uninstallFiberLanes();
pool.start();
scope (exit) pool.shutdown(true);

lanes[0].spawn(&fiberBody);
```

`applyFiberTopologyPreset` installs the managed-worker hooks and chooses a
measured starting point:

- `fiberThroughput`: all logical processors, `flushBatch=256`, `avgCost=0`.
- `payloadThroughput`: skip SMT siblings, `flushBatch=32`, `avgCost=2`.

These are starting points measured on a Ryzen 5 5500, not portable guarantees.
Mixed workloads should benchmark both.

Each worker keeps a persistent Farm consumer and producer token. Size every
lane's Farm for its native workers plus covering sweepers. Installation forms
a coverage barrier: a lane with workers but no responder fails explicitly.
Multi-LLC hardware validation remains required before making topology-specific
performance or failover claims.

## Wake policy

The default `FiberWakePolicy.broadcast` wakes the pool for external spawn,
signal, cancellation, and cancelling shutdown.

`FiberWakePolicy.directorOwned` instead records coalesced wake events. The
single thread that owns `pool.director()` consumes those events and selects
which LLC, worker class, or label to signal. An optional `@nogc nothrow`
notifier can wake that control thread's event loop when a lane mailbox changes
from empty to nonempty.

Configure wake policy, responder selection, and notifiers before starting the
pool. They are setup-time state, not concurrently mutable policy.

## Handles, joins, and cancellation

Raw `FiberTask` identity is valid only until `release`. Retain `task.handle` or
use `spawnHandle` when identity must survive recycling. A `TaskHandle` pairs a
slot with its generation and refuses stale reuse.

- `handle.poll()` reads outcome without blocking.
- `handle.join()` and timed join block an external OS thread.
- `handle.joinFiber()` and timed Fiber join suspend a managed Fiber.
- `requestCancel(handle)` queues a generation-specific request.
- The Director calls `drainCancellationRequests()` to apply queued requests.

Cancellation is cooperative. A running body that never reaches a scheduler
boundary cannot be forcibly unwound.

## Migration and TLS

A suspended Fiber may resume on another eligible OS thread. Do not carry a TLS
address, reference, cached access, `errno`, locale, affinity assumption, or
other thread-relative resource across a suspension. The same rule applies to
libraries called by the Fiber.

Reading TLS and fully consuming the result inside one non-suspending Fiber
slice or one bare payload callback is ordinary thread-local use. Pinning is a
separate application policy; the general scheduler does not infer it.

## Lifecycle and shutdown

`beginShutdown(false)` stops admission and drains cooperative work.
`beginShutdown(true)` also requests cancellation and wakes indefinite waiters
so cleanup scopes can unwind. Fibers that never cooperate still cannot be
forced to stop.

Optional lifecycle events provide bounded admitted, cancellation, terminal,
and failure records. Enable them before spawning, drain them on the Director
thread, and retire both completion and lifecycle records before releasing a
task.

## Benchmarks and internals

Performance tools live under `benchmarks/`; their DUB configuration names are:

```text
dub run -c benchmark --build=release --compiler=ldc2
dub run -c benchmark_mt --build=release --compiler=ldc2
dub run -c benchmark_vs_druntime --build=release --compiler=ldc2
dub run -c benchmark_farm_embed --build=release --compiler=ldc2
```

The current scheduler model and synchronization invariants live in
[DESIGN.md](DESIGN.md). Benchmark evidence and tuning guidance live in
[PERFORMANCE.md](PERFORMANCE.md). Release priorities live in the repository
[roadmap](../ROADMAP.md).
