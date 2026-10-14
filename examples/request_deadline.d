/++
 + Request service with deadlines (U5): one tracked root per request, a
 + per-request deadline, cancellation from another thread, and a terminal
 + status/code read back through `takeOutcome`. Requests 4 and 5 run until the
 + shutdown `cancelAll()`. No hash map and no unbounded buffering: handles are
 + compared with `==` (PrivateRootHandle hashing is not provided) and untaken
 + outcomes backpressure `spawn`.
 + Build notes: examples/README.md.
 +/
module request_deadline;

import antfarm;
import antfarm.actors;
import farmhost;
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
    FarmHostOptions hostOptions;
    hostOptions.workers = args.length > 1 ? to!uint(args[1]) : 0;
    hostOptions.waitTimeout = 30.seconds; // example policy: a stuck drain fails instead of hanging
    auto host = new FarmHost(hostOptions);
    scope (exit) host.closeOrAbandon();

    enum requestCount = 6;
    PrivateRootOptions options;
    options.tracking = true;
    auto park = host.createPark!(Request, serve)(requestCount, options);

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
    // while the park exists, so join the canceller before host.close().
    PrivateCancelResult cancelResult;
    auto canceller = new Thread({
        cancelResult = roots[2].requestCancel();
    });
    canceller.start();
    canceller.join();
    if (cancelResult != PrivateCancelResult.requested)
        fatal("external cancel was not requested");

    // Drain: each step pumps, consumers run, and takeOutcome recycles the
    // terminal slot. A tracked root stays live until its outcome is taken, so
    // takeOutcome belongs in the wait predicate. pump() == 0 is not idle.
    PrivateRootOutcome[requestCount] outcomes;
    size_t collected;
    bool cancelledAll;
    host.waitUntil(() {
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
        return park.live == 0;
    });

    alias S = PrivateOutcomeStatus;
    alias C = PrivateCancelCause;
    if (outcomes[0].status != S.completed
        || outcomes[1].status != S.failed || outcomes[1].code != 501
        || outcomes[2].status != S.cancelled || outcomes[2].cause != C.requested
        || outcomes[3].status != S.cancelled || outcomes[3].cause != C.deadline
        || outcomes[4].status != S.cancelled || outcomes[4].cause != C.requested
        || outcomes[5].status != S.cancelled || outcomes[5].cause != C.requested)
        fatal("unexpected request outcomes");

    // Every canceller is already joined; host.close() stops the workers,
    // then destroys the park.
    writeln("request service: completed, failed 501, requested, deadline,"
        ~ " cancelAll, workers=", host.resolution.workers.length);
}
