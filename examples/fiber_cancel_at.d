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
import example_host;
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
    uint workers = args.length > 1 ? to!uint(args[1]) : 0;
    auto host = new ExampleHost(workers);
    auto token = host.farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");
    auto domain = new FiberDomain(host.farm);

    PrivateRootOptions options;
    options.tracking = true;
    auto park = PrivateRootPark!(Worker, poll).create(host.farm, 1, options);
    if (park is null) fatal("root park allocation failed");
    PrivateRootHandle root;
    // The root's own lazy deadline is far away; the fiber is the prompt timer.
    if (park.spawn(Worker(0), root, MonoTime.currTime + 5.seconds)
            != PrivateSpawnResult.spawned)
        fatal("root spawn failed");
    cancelAt(domain, root, MonoTime.currTime + 50.msecs);

    // Drive both emitters until the root is collected and the fiber terminal.
    // Zero from either pump is not idle, so the condition is the two drains.
    PrivateRootOutcome outcome;
    while (park.live != 0 || !domain.drained)
    {
        park.pump(token);
        domain.pollTimers();
        domain.pollGenerationTriggers();
        domain.flush(token);
        host.progress();
        PrivateRootOutcome taken;
        while (park.takeOutcome(taken))
            outcome = taken;
    }

    if (outcome.status != PrivateOutcomeStatus.cancelled
            || outcome.cause != PrivateCancelCause.requested)
        fatal("fiber cancelAt did not cancel the root");

    // Recycle the terminal fiber before the Farm goes away.
    domain.releaseAll(domain.takeCompletions());
    host.stopConsumers();
    park.destroy();
    park = null;
    host.farm.unregisterProducer(token);
    host.farm.destroy();
    writeln("fiber cancelAt: sleeping fiber requested root cancellation"
        ~ ", workers=", workers);
}
