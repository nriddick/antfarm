# Run actors on a FarmHost

`FarmHost` (`import threadpool.host;`) owns what every actor program needs
around its actors:
- one Farm sized for its participants;
- the calling thread's producer `Token`;
- pinned pool workers that consume the Farm;
- the runtimes and parks it creates;
- a progress loop with an opt-in deadline;
- the documented shutdown order.

A `std.concurrency` port needs three lines of setup and a teardown guard.
The module lives in the threadpool package, which depends on `antfarm`;
`import threadpool;` re-exports it.

## Setup

```d
import antfarm.actors;
import threadpool.host;
import std.stdio : writeln;

struct Counter { long total; }

void add(scope ref ActorBorrow!Counter actor, scope ref ActorContext context)
    nothrow @nogc @system
{
    for (auto node = context.popInbox(); node !is null; node = context.popInbox())
    {
        actor.value.total += *node.payloadAs!long;
        node.complete();
    }
}

void main()
{
    auto host = new FarmHost(2);    // two pinned workers in one LLC
    scope (exit) host.closeOrAbandon();

    auto runtime = host.createRuntime(1);
    auto owner = runtime.createActor!(Counter, add)(Counter(0));
    long value = 5;
    ActorInboxNode node;
    node.initialize(&value, 0);
    owner.handle.send(&node);
    host.waitUntil(() => node.available); // "receive"
    host.retire(owner);                   // "join"
    writeln(host.summary());
}
```

Build it with `-Ithreadpool/source` beside `-I.`, as every program in
[examples/](../../examples/README.md) does.

| std.concurrency | FarmHost |
| --- | --- |
| `spawn` | `host.createRuntime` (or `createPark`), then `createActor` |
| `send` | `handle.send(&node)` with a caller-owned `ActorInboxNode` |
| `receive` | `host.waitUntil(() => node.available)` |
| join, thread exit | `host.retire(owners...)`, then `close()` |

Use `scope (exit) host.closeOrAbandon();`.
The exit guard always tries ordered teardown. If `close()` throws,
`closeOrAbandon()` stops the consumers with `abandon()` before the error
propagates. This also covers normal return with undrained work. Pool workers
are non-daemon threads, so a host with workers that is neither closed nor
abandoned hangs process exit.

## Placement

`FarmHost.resolve(options)` returns the worker selection that the
constructor will start, as a `PoolResolution`: the workers are accepted,
every other CPU that passed the `options.pool` filters is
`Reject.notSelected`, and each physical group reports its `candidates` beside
its `accepted` count. It changes nothing, but it does warm topology
discovery (which freezes the OS scope), including when it throws
`FarmHostResolveException`. Call it once before concurrent construction or
resolve calls: that first discovery is an unlocked setup operation. On Linux
a racing first call is benign; on Windows it can spuriously refuse the
caller with `outsideOsScope`.

A live host reports the same selection, now with every worker's pin outcome,
as `host.resolution` (the pool's own `pool.resolution`), and the caller's
confine or pin set as `host.callerCpus`. `host.summary()` prints workers,
caller placement, Farm participants and sizing in one line, and
`host.toString` adds every compromise note and the resolution summary.
The defaults are:

- **One physical group.** Workers come from the accepted LLC with the most
  accepted CPUs. The host never spills into another group: asking for more
  workers than the group has is a `FarmHostResolveException` that names `span`.
- **One worker per core.** `skipSmtSiblings` is on in
  `defaultHostPoolOptions`.
- **Caller confined** (`CallerPlacement.confine`). The constructing thread's
  affinity is restricted to the home group's CPUs that no worker owns, SMT
  siblings included. With `autoWorkers`, the group's last accepted CPU is
  left free for it. `close()` and `abandon()` restore the previous affinity.
  Threads the caller creates meanwhile inherit the confined mask.
  `CallerPlacement.pin` uses one CPU instead. `CallerPlacement.unchanged`
  leaves the caller alone and notes it as an external participant.
- **`workers == 0`** starts no pool. The calling thread is the only consumer.

`HostGroups.only(g)` pins the choice to one group.

## Scaling across groups

Everything about a second LLC group, the cost of crossing one, the host
loop against a hand-written consumer loop and the child-batch tuning is on
its own page: [scaling across groups](scaling-groups.md). In short: with
`FarmHostOptions.groups = HostGroups.all()` every group supplies workers,
and `farmPerGroup` gives each group its own Farm, with `createRuntime` and
`createPark` taking the group a runtime or park runs on.

## Progress and waking

Work advances only while the control thread (the one that constructed the
host) is inside `step` or `waitUntil`, or while a holder of an extra token from
`registerProducer` flushes. The host has no background emitter. Actor callbacks
and other threads may `send` and `wake`, but someone still has to flush or
pump.

`step()` does the following, in order:
1. Flushes every host runtime and pumps every host park and attachment, in
   rounds. It stops after a round publishes nothing, or after
   `FarmHostOptions.maxEmitRounds` rounds (default **16**, must be positive).
2. Wakes parked workers, even if nothing was published, to request consumption
   and reclamation progress.
3. Consumes once on the caller's view, or yields when there is no caller view.

`waitUntil(done)` runs `done()` before every step, so the predicate is the
place to drain `takeOutcome`. Neither call ever infers idle from a zero flush,
pump or `consumeNext`.

Each round visits every emitter in registration order (host-owned emitters,
then `HostAttachment`s, the control-thread emitters registered with
`attach`; the `GroupAttachment`s of `FarmHostOptions.attachments` are
worker-loop participants, not emitters), including emitters that returned
zero in an earlier round.
This batches publications before a wake without draining one busy emitter
indefinitely. The bound counts rounds, not tables or elapsed time: a park may
publish several pending child tables per pump and a runtime flushes up to four
times per visit. Attachments must return promptly, including on backpressure,
and must report the number of items they actually published. An attachment may
be called several times in one step. Set `maxEmitRounds = 1` for the previous
single-round behavior or when the control thread must check outcomes and
deadlines more frequently. `flush`, `pump` and `publish` retain their individual
publication/wake behavior.

Idle workers park by default (`WorkerIdle.park`, `idleTimeout = 1.msecs`).
The host's own wrappers wake them as soon as they publish: `step`, `flush`,
`pump`, `publish!op` and park pumps, including child-only publications.
Publications through the raw token (`runtime.flush(host.token)`) skip the
wake and are picked up within `idleTimeout`, or call `host.wake()`.
`idleTimeout = Duration.max` parks until woken, so raw-token users must then
call `wake()`. `WorkerIdle.spin` never parks and burns every worker's CPU
for the host's life. `wake()` may be called from any thread while the host is
open. It may overlap `close()`, `abandon()` or a worker-failure shutdown:
teardown stores a non-open state and waits for in-flight wakes before
stopping the pool.

A worker can park while another worker still runs a callback in an epoch it
holds. Completing that callback does not itself wake the parked worker to
release its reference. A full ring can therefore refuse publication even
after all callbacks finish. Keep calling `step()` on publication backpressure:
each step requests another worker poll, including when no emitter publishes.
One wake or a fixed number of polls before the peer finishes is insufficient.
Raw `host.farm.write` loops must wake after accepted writes and call `step()`
before retrying refused writes. `step()` respects `callerConsumes = false`;
it does not run callbacks on that caller. This progress guarantee requires the
control thread to keep stepping and callbacks eventually to return.

## Deadlines and failures

`FarmHostOptions.waitTimeout` (default `Duration.max`: none) starts a fresh
deadline on each `waitUntil`. When it expires, `FarmHostTimeout` is thrown and
the host stays open with its consumers running. Escalate, for example with
`requestRetire(ActorRetireMode.cancel)` or `park.cancelAll()`, and wait again,
or `abandon()`. A worker failure, such as an `assert` in a callback in a debug
build, surfaces as `FarmHostFailed` from the next `step` or `waitUntil`. The
host is then `failed`, and only `close()` (which acts as `abandon()`) remains.
After `FarmHostFailed`, the caller stays confined until `close()` or
`abandon()` restores affinity.

`FarmHostFailed.workerFailures` contains owned diagnostic snapshots, so an
assertion's thread-local error object is never used after its worker exits.
The Error/Exception distinction is preserved; the original subclass, chain
and stack trace are not. See the [worker failure capture contract](../design/threadpool.md#managed-worker-hooks)
for message/file limits and `sourceType`/`truncated` fields.

## Teardown

`close()` first checks, with no side effects:
- every runtime is `quiescent`;
- every park has `live == 0`;
- every attachment is drained;
- no extra producer is outstanding.

If any check fails, it throws and the host stays usable: drain and call it
again. Otherwise it performs the canonical
[shutdown order](../reference/actor-lifecycle.md#shutdown-order), finishing
attachments and restoring caller affinity as part of teardown.

`abandon()` stops the consumers, restores affinity and leaks everything that
did not drain. After `abandon()` or a failed host, call `leak()` on any
`ActorOwner` you cannot reclaim. Otherwise an assertion build prints a
"dropped without reclaim" diagnostic when the owner goes out of scope,
including while `FarmHostFailed` unwinds.

Join every thread that may call `requestCancel` on a host park before
`close()`. The host cannot see those threads. Views subscribed outside the
host (`FarmSizing.extraConsumers`) must unsubscribe first as well.

While it has workers, a host runs on a `GroupHost` (`host.groupHost`) and so
owns the process's single `CacheAwarePool`. A second host with workers, a
`GroupHost` (the fork-join dispatcher's, say), or a host started beside an
application-started pool, is rejected with a `FarmHostException`, and the
running one is left alone. Hosts with `workers == 0` start no pool and can
coexist with any of them.

Fibers join a host two ways. `ant_fibers.host.attachFiberDomain(host,
new FiberDomain(host.farm))` serves a one-lane domain from the control
thread's `step()` and runs its Fibers on the host's consumers; use it when
the control thread already drives the host
([examples/fiber_cancel_at.d](../../examples/fiber_cancel_at.d)). To have the
pool workers serve the domain (timers, retirement under `serviceThreads`,
one lane per group with `farmPerGroup`), put `FiberGroups.attachment` in
`FarmHostOptions.attachments` instead
([ant_fibers/README.md](../../ant_fibers/README.md#run-on-a-grouphost)).

## Parallel regions

Blocking loops run on the fork-join dispatcher (`forkJoinFor`,
`forkJoinReduce` in [forkjoin](../../forkjoin/README.md#loops-and-reductions)),
whose join never blocks a worker. For an asynchronous Farm region on the
host's Farm, pass `host.parallelOptions(options)` to `parallelFor` so every
accepted write wakes parked workers, then drive it with `step()` until the
handle reports completion; keep the host alive through completion. See
[parallel loops](parallel-loops.md) for ranges and grain sizing.
