/++
 + Request service with deadlines (U5): one tracked root per request, a
 + per-request deadline, cancellation from another thread, and a terminal
 + status/code read back through `takeOutcome`. Requests 4 and 5 run until the
 + shutdown `cancelAll()`. No hash map and no unbounded buffering: the park's
 + slot table is the identity, and untaken outcomes backpressure `spawn`.
 + Build notes: examples/README.md.
 +/
module request_deadline;

import antfarm;
import actors;
import example_host;
import core.thread : Thread;
import core.time : MonoTime, seconds;
import std.conv : to;
import std.stdio : writeln;

struct Request
{
    uint id;
    uint polls;
}

// id 1 fails, ids 4-5 poll until cancelled, the rest poll briefly.
PrivateRootDisposition serve(scope ref PrivateBorrow!Request request,
    scope ref PrivateContext context) nothrow @nogc @system
{
    if (context.cancelled)
        return PrivateRootDisposition.finish;
    if (request.value.id == 1)
    {
        context.fail(500 + request.value.id); // a value, not fatal
        return PrivateRootDisposition.finish;
    }
    if (request.value.id == 0 && ++request.value.polls < 3)
        return PrivateRootDisposition.again;
    if (request.value.id == 0)
        return PrivateRootDisposition.finish;
    return PrivateRootDisposition.again; // keep serving until cancelled
}

void main(string[] args)
{
    uint workers = args.length > 1 ? to!uint(args[1]) : 0;
    auto host = new ExampleHost(workers);
    auto token = host.farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");

    enum requestCount = 6;
    PrivateRootOptions options;
    options.tracking = true;
    auto park = PrivateRootPark!(Request, serve).create(host.farm, requestCount, options);
    if (park is null) fatal("root park allocation failed");

    PrivateRootHandle[requestCount] roots;
    // id 3 was already past its deadline at spawn; a lazy deadline becomes
    // cancellation the first time the root or the emitter observes it.
    auto expired = MonoTime.currTime - 1.seconds;
    foreach (uint id; 0 .. requestCount)
    {
        auto deadline = id == 3 ? expired : MonoTime.max;
        if (park.spawn(Request(id), roots[id], deadline)
                != PrivateSpawnResult.spawned)
            fatal("root spawn failed");
    }

    // Request id 2 is cancelled from another thread. The handle is only valid
    // while the park exists, so join the canceller before park.destroy().
    PrivateCancelResult cancelResult;
    auto canceller = new Thread({
        cancelResult = roots[2].requestCancel();
    });
    canceller.start();
    canceller.join();
    if (cancelResult != PrivateCancelResult.requested)
        fatal("external cancel was not requested");

    // Drain: pump publishes, consumers run, and takeOutcome recycles the
    // terminal slot. A tracked root stays live until its outcome is taken, so
    // takeOutcome belongs in the shutdown loop. pump() == 0 is not idle.
    PrivateRootOutcome[requestCount] outcomes;
    size_t collected;
    bool cancelledAll;
    while (park.live != 0)
    {
        park.pump(token);
        host.progress();
        PrivateRootOutcome outcome;
        while (park.takeOutcome(outcome))
        {
            foreach (id, ref root; roots)
                if (root == outcome.root)
                    outcomes[id] = outcome;
            ++collected;
        }
        // The four finite requests are collected; cancel the two long ones
        // rather than waiting for them.
        if (!cancelledAll && collected == 4)
        {
            if (park.cancelAll() != 2)
                fatal("cancelAll did not cancel the two long requests");
            cancelledAll = true;
        }
    }

    alias S = PrivateOutcomeStatus;
    alias C = PrivateCancelCause;
    if (outcomes[0].status != S.completed
        || outcomes[1].status != S.failed || outcomes[1].code != 501
        || outcomes[2].status != S.cancelled || outcomes[2].cause != C.requested
        || outcomes[3].status != S.cancelled || outcomes[3].cause != C.deadline
        || outcomes[4].status != S.cancelled || outcomes[4].cause != C.requested
        || outcomes[5].status != S.cancelled || outcomes[5].cause != C.requested)
        fatal("unexpected request outcomes");

    // Workers first, then the park. Every canceller is already joined.
    host.stopConsumers();
    park.destroy();
    park = null;
    host.farm.unregisterProducer(token);
    host.farm.destroy();
    writeln("request service: completed, failed 501, requested, deadline,"
        ~ " cancelAll, workers=", workers);
}
