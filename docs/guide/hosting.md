# Run actors on a FarmHost

`FarmHost` (`import threadpool.host;`) owns what every actor program needs
around its actors:
- one Farm sized for its participants;
- the calling thread's producer `Token`;
- pinned pool workers that consume the Farm;
- the runtimes and parks it creates;
- a progress loop with an opt-in deadline;
- the documented shutdown order.

A `std.concurrency` port needs three lines of setup and a `scope` pair.
The module lives in the threadpool package, which depends on `antfarm`.
`import threadpool;` does not re-export it.

## Setup

```d
import actors;
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
    scope (exit) host.close();      // ordered teardown
    scope (failure) host.abandon(); // on an exception: stop consumers first

    auto runtime = host.createRuntime(1);
    auto owner = runtime.createActor!(Counter, add)(Counter(0));
    long value = 5;
    ActorInboxNode node;
    node.initialize(&value, 0);
    owner.handle.send(&node);
    host.waitUntil(() => node.available); // "receive"
    host.retire(owner);                   // "join"
    writeln(host.plan.summary());
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

Declare `scope (exit) host.close();` and then `scope (failure)
host.abandon();`, in that order. On an exception, `abandon` runs first and
stops the consumers. The `close` that follows is a no-op. Without the
pair, an exception with live work leaves the host open. Pool workers are
non-daemon threads, so a host with workers that is neither closed nor
abandoned hangs process exit.

## Placement

`FarmHost.preview(options)` returns the `FarmHostPlan` that the constructor
will execute. It changes nothing. `plan.summary()` prints it in one line, and
`plan.notes` lists every compromise. The defaults are:

- **One physical group.** Workers come from the accepted LLC with the most
  accepted CPUs. The host never spills into another group: asking for more
  workers than the group has is a `FarmHostPlanException` that names `span`.
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
from several groups. The plan labels it `crossGroup` and counts the other
groups' workers as foreign consumers. `HostGroups.only(g)` pins the choice to
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
open, but not concurrently with `close()` or `abandon()`.

## Deadlines and failures

`FarmHostOptions.waitTimeout` (default `Duration.max`: none) starts a fresh
deadline on each `waitUntil`. When it expires, `FarmHostTimeout` is thrown and
the host stays open with its consumers running. Escalate, for example with
`requestRetire(ActorRetireMode.cancel)` or `park.cancelAll()`, and wait again,
or `abandon()`. A worker failure, such as an `assert` in a callback in a debug
build, surfaces as `FarmHostFailed` from the next `step` or `waitUntil`. The
host is then `failed`, and only `close()` (which acts as `abandon()`) remains.

## Teardown

`close()` first checks, with no side effects:
- every runtime is `quiescent`;
- every park has `live == 0`;
- every attachment is drained;
- no extra producer is outstanding.

If any check fails, it throws and the host stays usable: drain and call it
again. Otherwise it runs the
[shutdown order](../reference/actor-lifecycle.md#shutdown-order):
1. joins the consumers;
2. finishes the attachments;
3. destroys parks and runtimes;
4. restores the caller's affinity;
5. unregisters the token;
6. destroys the Farm.

`abandon()` stops the consumers, restores affinity and leaks everything that
did not drain.

Join every thread that may call `requestCancel` on a host park before
`close()`. The host cannot see those threads. Views subscribed outside the
host (`FarmSizing.extraConsumers`) must unsubscribe first as well.

While it has workers, a host owns the process's single `CacheAwarePool`. A
second host with workers, or a host started beside an application-started
pool, is rejected with a `FarmHostException`, and the running one is left
alone. Hosts with `workers == 0` start no pool and can coexist.

A caller-driven `FiberDomain` joins the same host with
`antfarm_fibers.host.attachFiberDomain(host, new FiberDomain(host.farm))`.
[examples/fiber_cancel_at.d](../../examples/fiber_cancel_at.d) shows it.
