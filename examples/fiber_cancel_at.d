/++
 + Fiber `cancelAt` adapter: a sleeping managed Fiber requests cancellation
 + of a tracked private root at a deadline. This is an example bridge between
 + the fibers timer facility (`FiberDomain.sleepUntil`) and the private
 + `PrivateRootHandle.requestCancel`; it is deliberately not library API.
 + Build notes: examples/README.md.
 +/
module fiber_cancel_at;

import antfarm;
import actors;
import antfarm_fibers;
import antfarm_fibers.host : attachFiberDomain;
import threadpool.host;
import core.time : MonoTime, msecs, seconds;
import std.conv : to;
import std.stdio : writeln;

struct Worker
{
    uint polls;
}

PrivateRootDisposition poll(scope ref PrivateBorrow!Worker worker,
    scope ref PrivateContext context) nothrow @nogc @system
{
    if (context.cancelled)
        return PrivateRootDisposition.finish;
    ++worker.value.polls;
    return PrivateRootDisposition.again;
}

// Example adapter, not library API. The domain owns the timer; the park owns
// the handle. `requestCancel` is callable from any thread, so it is safe from
// a migrated Fiber.
void cancelAt(FiberDomain domain, PrivateRootHandle root, MonoTime deadline)
{
    domain.spawn({
        FiberDomain.sleepUntil(deadline);
        root.requestCancel();
    });
}

void main(string[] args)
{
    FarmHostOptions hostOptions;
    hostOptions.workers = args.length > 1 ? to!uint(args[1]) : 0;
    hostOptions.waitTimeout = 30.seconds; // example policy: a stuck drain fails instead of hanging
    auto host = new FarmHost(hostOptions);
    scope (exit) host.closeOrAbandon();
    // The domain's timers, triggers and flush run in every host step; its
    // tasks run on the host's consumers.
    auto domain = new FiberDomain(host.farm);
    attachFiberDomain(host, domain);

    PrivateRootOptions options;
    options.tracking = true;
    auto park = host.createPark!(Worker, poll)(1, options);
    PrivateRootHandle root;
    // The root's own lazy deadline is far away; the fiber is the prompt timer.
    if (park.spawn(Worker(0), root, MonoTime.currTime + 5.seconds)
            != PrivateSpawnResult.spawned)
        fatal("root spawn failed");
    cancelAt(domain, root, MonoTime.currTime + 50.msecs);

    // Each step drives both emitters. Zero from either pump is not idle, so
    // the condition is the two drains.
    PrivateRootOutcome outcome;
    host.waitUntil(() {
        PrivateRootOutcome taken;
        while (park.takeOutcome(taken))
            outcome = taken;
        return park.live == 0 && domain.drained;
    });

    if (outcome.status != PrivateOutcomeStatus.cancelled
            || outcome.cause != PrivateCancelCause.requested)
        fatal("fiber cancelAt did not cancel the root");

    // host.close() recycles the terminal fiber (the attachment's finish)
    // after the consumers stop and before the Farm goes away.
    writeln("fiber cancelAt: sleeping fiber requested root cancellation"
        ~ ", workers=", host.resolution.workers.length);
}
