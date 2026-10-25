# Ant Farm fibers

`ant_fibers` runs freely migrating DRuntime Fibers on Ant Farm work lanes.
It supplies Fiber lifecycle, waits, timers, cancellation, joins, completion,
records and stack recycling. Workers come from the embedding: a caller's own
loop, a `FarmHost`, or the pool through `ant_fibers.groups` on a `GroupHost`.

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
`unittest` blocks and then the smoke executable (`tests/smoke.d`), covering
lifecycle records and backpressure, cancellation, generation-trigger
contention, multi-lane domains, and Fibers on a `GroupHost`. The `unittest` configuration compiles those blocks under
any build type, so Dub prints a warning about `buildOptions "unittests"`.
The `stress` configuration runs
`tests/stress.d`. Each suite prints a final `passed` line. Both configurations
require contracts, so test `assert` checks stay enabled even with
`--build=release`. Scheduler assertions are assertion-build diagnostics;
retiring a task twice or a non-terminal task is a `fatal` scheduler bug in
every build.

The deterministic semaphore regression can be selected with
`dub run -c unittest --compiler=ldc2 -- --semaphore-cancellation-only`. It covers
one wake per permit, a woken waiter cancelled before it runs (its wake passes
to the next waiter), the refund of a permit consumed during wait registration,
and timed waits.

The deterministic event regression can be selected with
`dub run -c unittest --compiler=ldc2 -- --event-cancellation-only`. It covers
one wake per auto-reset set, pass-on from a cancelled woken waiter, auto-reset
consumption during registration with a retained refund, a refund that must wake
a waiter already re-parked, manual-reset behavior, and timed waits.

## Run one lane on the current thread

[examples/hello_fibers.d](examples/hello_fibers.d) is the smallest complete
program. Its essential lifecycle is:

```d
auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                           DEFAULT_SMALL_TABLE_THRESHOLD, false);
scope (exit) farm.destroy();

FiberOutcome outcome;
FiberDomainOptions options;
options.onCompletion = (FiberTask task) { outcome = task.outcome; };
auto domain = new FiberDomain(farm, options);
auto token = farm.registerProducer(Tier.small);
scope (exit) farm.unregisterProducer(token);

ConsumerView consumer;
subscribeOrThrow(consumer, farm);
scope (exit) consumer.unsubscribe();

domain.spawn(&fiberBody);
drainUntilEmpty(domain, token, consumer, 256, avgCostForChunk(32));
assert(outcome == FiberOutcome.completed);
```

`drainUntilEmpty` takes `ConsumerView` by reference. Each pass is one
`serve` (cancellation requests, timers, triggers, flush, retirement) followed
by a consume. It waits indefinitely if a task is parked on an application
signal that nobody will deliver; signal or cancel such tasks before a shutdown
drain.

A domain is the opt-in to Fibers; `FiberDomainOptions` configures it once, at
construction. `retire` hands every lifecycle record to `onRecord` (or drops
it), then every terminal task to `onCompletion` while it is still valid, and
recycles the task when the handler returns. Later spawns reuse both the task
and its stack with `Fiber.reset`. Every host retires for you: the caller's own
loop through `drainUntilEmpty` or `serve`, a `FarmHost` in each `step()`, a
managed pool's workers under `FiberRetirement.serviceThreads`. Under the
default `FiberRetirement.director`, a managed pool's Director calls `retire`
itself: retirement latency is its polling cadence, and nothing wakes it, so
poll `retirementPending` on each pass. Do not call `retire` from a Fiber
body: on the Director thread it is accepted and runs the handlers on that
Fiber's stack.

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
  `TaskHandle.joinFiber` park without blocking the worker. The event and
  semaphore waits and `joinFiber` also take a `Duration` (events and
  semaphores also `waitUntil(MonoTime)`); the timed event and semaphore forms
  return false on timeout.
- `FiberSemaphore.post(n)` and an auto-reset `FiberEvent.set` wake one waiter
  per unit, oldest first; a manual-reset `set` wakes every waiter. A woken
  waiter that is cancelled before it takes the unit passes its wake on.
- If `FiberSemaphore.wait` throws `FiberCancelled`, it retains no permit: a
  permit acquired during wait registration is returned and wakes a waiter. An immediate successful acquisition can still return before observing
  cancellation; a later cancellation does not undo a completed wait.
- If auto-reset `FiberEvent.wait` throws `FiberCancelled`, a set consumed
  during registration is restored and wakes a waiter. The refund acts as
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

Every `serve` pass (`FiberGroups` on a `GroupHost`, `attachFiberDomain`,
`drainUntilEmpty`) delivers these deferred edges. A custom scheduler pump
must call `domain.pollGenerationTriggers()` in its service loop.

## Run on a GroupHost

`ant_fibers.groups.FiberGroups` attaches a domain to a `threadpool.groups`
`GroupHost`, the owner of the pool, Farms and tokens that the fork-join
dispatcher runs on. Construct the domain without a lane, put the attachment
in the host options, construct the host, then spawn:

```d
auto domain = new FiberDomain(options);      // the caller is the Director
auto fibers = new FiberGroups(domain);
GroupHostOptions o;
o.groups = HostGroups.all();
o.farmPerGroup = true;
o.attachments = [fibers.attachment];         // beside the dispatcher's, if any
auto host = new GroupHost(o);
fibers.spawn({ FiberDomain.sleepFor(1.msecs); });
fibers.spawnOn(host.groups[1], &body);       // prefer another group's Farm
```

The host builds one `GroupFiberLane` per Farm when it installs the
attachment and runs its managed lane: every worker's pump serves its home
lane (`serve`: cancellation requests, timers, triggers, flush with the
worker's group token, retirement when it may) and parks until the earliest
timer deadline. Activations are Farm payloads, so whichever view consumes
the group Farm resumes them: the host's own, or the dispatcher's. Spawns,
signals and cancellations wake the Farm's workers; a new earliest timer
wakes every worker. Under `FiberRetirement.director` the Director (the
constructing thread) calls `domain.retire()`; under `serviceThreads` the
workers retire. Drain and retire before `host.close()`.

## Handles, joins, and cancellation

Raw `FiberTask` identity is valid only until retirement: until the
Director's `retire` hands the task to `onCompletion` under
`FiberRetirement.director`, and only until the task terminates under
`serviceThreads`. Retain `task.handle` or use `spawnHandle` when identity must
survive recycling. A `TaskHandle` pairs a
slot with its generation and refuses stale reuse.

- `handle.poll()` reads outcome without blocking.
- `handle.join()` and timed join block an external OS thread.
- `handle.joinFiber()` and timed Fiber join suspend a managed Fiber.
- `requestCancel(handle)` queues a generation-specific request.
- Any thread may call `drainCancellationRequests()` to apply queued requests.
  Every `serve` pass does so, and a request wakes the lane's parked workers,
  so the constructing thread need not poll. An explicit drain returns only
  the requests it applied, since another worker
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

The constructing thread is the domain's Director. `beginShutdown`, synchronous
`directorCancel` and, under `FiberRetirement.director`, `retire` are
Director-only; draining queued requests does not transfer that role.
Retirement applies queued cancellation requests first, so a request for a
retiring generation reports `alreadyTerminal` rather than a stale handle.

`beginShutdown(false)` stops admission and drains cooperative work.
`beginShutdown(true)` also requests cancellation and wakes indefinite waiters
so cleanup scopes can unwind. Fibers that never cooperate still cannot be
forced to stop.

With an `onRecord` handler, a domain records admitted, accepted-cancellation,
terminal and failure events in a bounded ring of
`FiberDomainOptions.recordRetention` records (default 16,384, about 1.2 MiB);
without one no record is emitted, and admission still reserves as if they
were. Spawn reserves room for a task's worst case
(4 records), held while the task is live and until its records are retired,
and throws `FiberLifecycleBackpressure` past the limit. Retention therefore
bounds concurrent fibers as well as unretired ones: the default admits 4,096,
and a domain that is never retired stops admitting at that many tasks. Raise
`recordRetention` for more (about 72 bytes per record). Records are values
with their own failure reference: they never delay recycling, and a record may
outlive its task, whose handle inside it then reports stale. A task's terminal
record is published before its completion, and `retire` delivers records
before completions.

## Benchmarks and internals

The benchmark sources live under [tools/perf/ant_fibers/](../tools/perf/ant_fibers/);
their DUB configuration names are:

```text
dub run -c benchmark --build=release --compiler=ldc2
dub run -c benchmark_mt --build=release --compiler=ldc2 -- 1000000 private 8
dub run -c benchmark_vs_druntime --build=release --compiler=ldc2
dub run -c benchmark_farm_embed --build=release --compiler=ldc2
```

`benchmark_mt` takes the fibers per burst, the Farm layout (`private`: one
Farm and lane per group; `shared`: one Farm for every group) and an optional
worker count, which pins exactly that many pool workers in placement order
(one physical group at a time, cores before SMT siblings); without it the
pool keeps one worker per core on every group.

The current scheduler model and synchronization invariants live in
[docs/design/ant_fibers.md](../docs/design/ant_fibers.md). Benchmark evidence and
tuning guidance live in the [Fibers section of PERFORMANCE.md](../PERFORMANCE.md#fibers).
Release priorities live in the repository
[roadmap](../ROADMAP.md).
