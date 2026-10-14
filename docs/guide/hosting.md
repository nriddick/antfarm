# Run actors on a FarmHost

`FarmHost` (`import farmhost;`) owns what every actor program needs
around its actors:
- one Farm sized for its participants;
- the calling thread's producer `Token`;
- pinned pool workers that consume the Farm;
- the runtimes and parks it creates;
- a progress loop with an opt-in deadline;
- the documented shutdown order.

A `std.concurrency` port needs three lines of setup and a teardown guard.
It is the [farmhost](../../farmhost/README.md) package, which depends on
`antfarm` and `threadpool`; `import threadpool;` does not provide it.

## Setup

```d
import antfarm.actors;
import farmhost;
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

Build it with `-Ithreadpool/source -Ifarmhost/source` beside `-I.`, as every program in
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

`HostGroups.span([home, other...])` is the explicit opt-in to consume one Farm
from several groups. `host.summary()` labels it `crossGroup` and counts the
other groups' workers as foreign consumers. `HostGroups.only(g)` pins the choice to
one group.

## Progress and waking

Work advances only while the control thread (the one that constructed the
host) is inside `step` or `waitUntil`, or while a holder of an extra token from
`registerProducer` flushes. The host has no background emitter. Actor callbacks
and other threads may `send` and `wake`, but someone still has to flush or
pump.

`step()` does the following, in order:
1. Flushes every host runtime and pumps every host park and attachment.
2. Wakes parked workers if any of them published.
3. Consumes once on the caller's view, or yields when there is no caller view.

`waitUntil(done)` runs `done()` before every step, so the predicate is the
place to drain `takeOutcome`. Neither call ever infers idle from a zero flush,
pump or `consumeNext`.

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

While it has workers, a host owns the process's single `CacheAwarePool`. A
second host with workers, or a host started beside an application-started
pool, is rejected with a `FarmHostException`, and the running one is left
alone. Hosts with `workers == 0` start no pool and can coexist.

A caller-driven `FiberDomain` joins the same host with
`ant_fibers.host.attachFiberDomain(host, new FiberDomain(host.farm))`.
[examples/fiber_cancel_at.d](../../examples/fiber_cancel_at.d) shows it.

## Performance and tuning

A host's consumers run on the threadpool's worker loop, not a loop you
wrote. On one measured host, that cost was invisible except where consumers
were the bottleneck and often found nothing.

The comparison ran [tools/perf/actor_churn](../../tools/perf/README.md#actor_churn)
on each workload twice: with `ANTFARM_CHURN_HOST=1`, where a `FarmHost` owns
the Farm, the control token and the pinned consumers, and without it, on the
benchmark's own hand-written consumers. The controller and every consumer CPU
sat in one LLC of a Ryzen 9 5900XT; each configuration was measured as twenty
paired runs and reported as throughput change (negative is slower than the
hand-written loop) with a bootstrap confidence interval. These numbers are
from one host, not a universal constant; re-measure on yours.

The differences for `actor` and `wave` at 2 and 6 workers, and for `private`
and `tree` at 2 workers, were within the comparison boundary or in
`FarmHost`'s favor. The fine-grained private modes at 6 workers were slower:

| Mode | Throughput change | Bootstrap CI |
| --- | --- | --- |
| `private` | -7.8% | [-8.1, -7.6] |
| `tree` | -7.1% | [-8.0, -5.9] |

The cause is the idle path. A hand-written consumer that finds nothing does
one `pause()` and retries. A pool worker that finds nothing arms its wait slot
with an atomic exchange (`prepareWorkerWait`), consumes a second time, and
only then applies the idle policy: under `WorkerIdle.spin` — the policy the
benchmark selects, matching the hand-written loop's always-polling behavior —
it spins `PoolOptions.pSpinIters` times (128 by default on performance-class
workers), each iteration a `pause` plus an `atomicFetchAdd` on a
process-wide spin counter, before the loop starts over. The exchange, the
second consume and the spin bookkeeping cost throughput only where consumers
are the bottleneck and miss often: many workers on private- and tree-style
fine-grained workloads.

What to tune:

- **Fewer workers** when consumers are the bottleneck: fewer consumers polling
  the same Farm miss less often. `actor` and `wave` were unaffected at the
  same worker counts, so size by measurement, not by core count.
- **The idle policy**: `FarmHostOptions.idle` selects `WorkerIdle.park` (the
  default: after its final empty check the worker parks at once, for up to
  `FarmHostOptions.idleTimeout`, 1 ms, or until the host's publication
  wrappers wake it) or `WorkerIdle.spin`; the spin count is
  `FarmHostOptions.pool.pSpinIters` (and `eSpinIters` on efficiency-class
  workers). The measurement above used `spin` with the default counts; `park`
  avoids the spin burn at the cost of wake latency. The publication wrappers
  wake a parked worker at once, so their latency stays small; a raw-token
  publication that no wrapper announces waits up to `idleTimeout` for the
  worker to recheck. `park` was not measured here.
- **A hand-written consumer loop** on the raw Farm, for maximum throughput on
  consumer-bound fine-grained workloads, as the benchmark's non-host path
  does: subscribe a `ConsumerView`, then loop
  `if (!view.consumeNext()) pause();`. You then own the pinning, the producer
  token and the teardown that the host otherwise provides.

To measure on your own machine, run the same command with and without the
variable (`ANTFARM_CHURN_HOST=1` needs the small tier, `consumers > 0` and a
CPU list naming the controller followed by each consumer);
[tools/perf/README.md](../../tools/perf/README.md#actor_churn) has the full
argument list.
