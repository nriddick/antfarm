# Ant Farm fibers

`ant_fibers` runs freely migrating DRuntime Fibers on Ant Farm work lanes.
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
dub test --compiler=dmd
dub test --compiler=ldc2
dub run -c unittest --compiler=dmd
dub run -c unittest --compiler=ldc2
dub run -c stress --compiler=ldc2 --build=release
```

Both `dub test` and `dub run -c unittest` run this package's module
`unittest` blocks (including the remote-service fixtures in
`threadpool_driver.d`) and then the smoke executable (`tests/smoke.d`),
covering lifecycle backlog, cancellation, generation-trigger contention, and
worker integration. The `unittest` configuration compiles those blocks under
any build type, so Dub prints a warning about `buildOptions "unittests"`.
The `stress` configuration runs
`tests/stress.d`. Each suite prints a final `passed` line. Both configurations
require contracts, so test `assert` checks stay enabled even with
`--build=release`. The task-release guards use `fatal` independently of those
test settings: releasing a null, foreign, live, already-pooled, or still-rooted
task aborts in debug and ordinary `-release` builds. Completion and lifecycle
events must be drained before recycling. Other scheduler assertions remain
assertion-build diagnostics. On Linux, the smoke suite re-executes child cases
and requires each release violation's fatal message; `--release-death` runs only
these cases.

The deterministic semaphore cancellation regression can be selected with
`dub run -c unittest --compiler=ldc2 -- --semaphore-cancellation-only`. It covers
cancellation after an ordinary wake and after consuming a permit during wait
registration, including waking a second waiter that has already re-parked.

The deterministic event cancellation regression can be selected with
`dub run -c unittest --compiler=ldc2 -- --event-cancellation-only`. It covers
ordinary wake cancellation, auto-reset consumption during registration with a
second waiter already re-parked, a retained refund, and manual-reset behavior.

## Run one lane on the current thread

[examples/hello_fibers.d](examples/hello_fibers.d) is the smallest complete
program. Its essential lifecycle is:

```d
auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                           DEFAULT_SMALL_TABLE_THRESHOLD, false);
scope (exit) farm.destroy();

auto domain = new FiberDomain(farm);
auto token = farm.registerProducer(Tier.small);
scope (exit) farm.unregisterProducer(token);

ConsumerView consumer;
subscribeOrThrow(consumer, farm);
scope (exit) consumer.unsubscribe();

domain.spawn(&fiberBody);
drainUntilEmpty(domain, token, consumer, 256, avgCostForChunk(32));

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
- If `FiberSemaphore.wait` throws `FiberCancelled`, it retains no permit: a
  permit acquired during wait registration is returned and other waiters are
  woken. An immediate successful acquisition can still return before observing
  cancellation; a later cancellation does not undo a completed wait.
- If auto-reset `FiberEvent.wait` throws `FiberCancelled`, a set consumed
  during registration is restored and waiters are woken. The refund acts as
  another `set`: it can coalesce with a set or follow a concurrent reset.
  Immediate acquisition may return before observing cancellation; a later
  cancellation does not undo a completed wait. Manual-reset waits consume
  nothing and never restore the flag on cancellation.
- `ActorWaveTrigger.waitNext` parks until the next wave using a counted edge,
  so completion which precedes the wait or coalesces in the worker handoff is
  not lost.

Do not use `core.thread.Fiber.yield()` as a scheduler operation. A body that
does so is failed rather than left stranded. `Thread.sleep` blocks the entire
worker; use `FiberDomain.sleepFor` or `sleepUntil`.

An actor wave can wake its dependent orchestration Fiber without calling the
locked wait machinery from its `nothrow @nogc` completion callback:

```d
auto trigger = new ActorWaveTrigger(domain);
wave.begin(farm, trigger.hook);
// Publish all actor tables, then close the wave.
auto completion = wave.seal();
trigger.waitNext();
assert(completion.finished);
```

The managed threadpool driver and `drainUntilEmpty` deliver these deferred
edges automatically. A custom scheduler pump must call
`domain.pollGenerationTriggers()` in its service loop.

## Bind lanes to the cache-aware pool

The intended concurrent embedding binds one `FiberLane` to each accepted LLC
group of the pool and shares one `FiberDomain` for application-wide lifecycle
ownership. A `FiberServicePlan` makes that binding explicit and derives each
Farm's sizing before the Farms exist. This fragment assumes a `makeFarm`
helper that takes the consumer and producer counts:

```d
enum preset = FiberTopologyPreset.fiberThroughput;
PoolOptions options;
applyFiberTopologyPreset(preset, null, options);   // worker selection first
auto pool = new CacheAwarePool(options);

auto plan = new FiberServicePlan(pool.resolve());
auto domain = new FiberDomain;
AntFarm*[] farms;
FiberLane[] lanes;
foreach (ref group; plan.resolution.acceptedGroups)
{
    auto need = plan.requirement(group.llcIndex);
    auto farm = makeFarm(need.consumerViews, need.smallProducerSlots);
    farms ~= farm;
    lanes ~= new FiberLane(domain, farm);
    plan.bind(group.llcIndex, lanes[$ - 1]);
}
applyFiberTopologyPreset(preset, lanes, options);  // then lane batching

installFiberPlan(plan, pool);   // validates; throws FiberPlanException
scope (exit) uninstallFiberPlan();
pool.start();
scope (exit) pool.shutdown();

lanes[0].spawn(&fiberBody);
```

Service is native-only by default: each lane is consumed by the accepted
workers of its own group, even when lanes share a domain. Cross-group
consumers exist only through explicit edges on the plan. `rescue(g, by)`
serves a group with no accepted workers. `assist(g, by)` and
`assistRing(groups)` are experimental and opt-in: they add consumers to
staffed groups. `plan.problems()` lists every mistake together, and
`plan.toString` prints the per-group service, edges and sizing. Groups
without accepted workers may stay unbound (`absent`), and `disable(g)` closes
a group whose workers then idle.

`applyFiberTopologyPreset` installs the managed-worker hooks and chooses a
measured starting point:

- `fiberThroughput`: all logical processors, `flushBatch=256`, `avgCost=2`
  (runs of 32 entries).
- `payloadThroughput`: skip SMT siblings, `flushBatch=32`, `avgCost=4` (runs of
  8 entries).

`avgCost` is a Farm work-cost class, `0 .. 7`: consumers claim `128 >> avgCost`
entries at a time, and the default is 3 (16). Lower classes are for very cheap
uniform work, higher ones for expensive work.

These are starting points measured on a Ryzen 5 5500, not portable guarantees.
Mixed workloads should benchmark both.

Each worker keeps a persistent Farm consumer and producer token, and each
responder of a serving group one more of each per served lane:
`plan.requirement(g)` counts them. An unstaffed or disabled lane refuses
spawns with `FiberLaneUnavailable`, before and after start. A domain whose
served lanes have no responder fails explicitly (`lane.state == failed`), and
a lane whose servers all stopped is `stopped` and refuses spawns.
Multi-LLC hardware validation remains required before making topology-specific
performance or failover claims.

## Wake policy

The default `FiberWakePolicy.broadcast` wakes the pool for external spawn,
signal, cancellation, and cancelling shutdown.

`FiberWakePolicy.servingGroups` (opt-in) wakes only the physical groups that
serve the lane: its own group and its `rescue`/`assist` server groups. A fiber
that arms a new earliest timer on a worker which is not a responder of its
domain also wakes the groups hosting the domain's native lanes, so timers still
reach a responder. With one busy lane on a shared domain, the other group's
workers stay parked.

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
- Any thread may call `drainCancellationRequests()` to apply queued requests.
  The managed threadpool pump does this automatically, including waking parked
  workers under `directorOwned`; the constructing thread need not poll.
  An explicit drain returns only the requests it applied, since another worker
  may already have drained them. Observe task outcomes or lifecycle records
  when the pump owns delivery.

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

The constructing thread is the domain's Director. `release`, `releaseAll`,
`beginShutdown`, and synchronous `directorCancel` remain Director-only; draining
queued requests does not transfer that role. Release waits for in-flight request
application and refuses release while queued requests remain.

`beginShutdown(false)` stops admission and drains cooperative work.
`beginShutdown(true)` also requests cancellation and wakes indefinite waiters
so cleanup scopes can unwind. Fibers that never cooperate still cannot be
forced to stop.

Optional lifecycle events provide bounded admitted, cancellation, terminal,
and failure records. Enable them before spawning, drain them on the Director
thread, and retire both completion and lifecycle records before releasing a
task.

## Benchmarks and internals

The benchmark sources live under [tools/perf/ant_fibers/](../tools/perf/ant_fibers/);
their DUB configuration names are:

```text
dub run -c benchmark --build=release --compiler=ldc2
dub run -c benchmark_mt --build=release --compiler=ldc2 -- 1000000 private 8
dub run -c benchmark_vs_druntime --build=release --compiler=ldc2
dub run -c benchmark_farm_embed --build=release --compiler=ldc2
```

`benchmark_mt` takes the fibers per burst, the domain mode (`private`,
`shared` or `assist`) and an optional worker count, which pins exactly that
many pool workers in placement order (one physical group at a time, cores
before SMT siblings); without it the pool keeps one worker per core on every
group.

The current scheduler model and synchronization invariants live in
[docs/design/ant_fibers.md](../docs/design/ant_fibers.md). Benchmark evidence and
tuning guidance live in the [Fibers section of PERFORMANCE.md](../PERFORMANCE.md#fibers).
Release priorities live in the repository
[roadmap](../ROADMAP.md).
