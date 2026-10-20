/+ dub.sdl:
    dependency "antfarm:threadpool" path=".."
+/
/++
 + Public inbox service: caller-owned nodes, typed send statuses, a bounded
 + drain, replies observed after `available`, node reuse, admission close,
 + idle retirement, and a stale handle. Build notes: examples/README.md.
 +/
module public_service;

import antfarm;
import antfarm.actors;
import threadpool.host;
import core.time : seconds;
import std.conv : to;
import std.stdio : writeln;

struct Request
{
    long delta;
    long balance;
}

struct Ledger
{
    long balance;
    uint callbacks;
    uint* closingCallbacks;
}

// At most two nodes per activation. Unpopped nodes stay private to the actor
// and force another activation; the handler does not have to republish.
void serve(scope ref ActorBorrow!Ledger actor, scope ref ActorContext context)
    nothrow @nogc @system
{
    ++actor.value.callbacks;
    foreach (_; 0 .. 2)
    {
        auto node = context.popInbox();
        if (node is null) break;
        auto request = node.payloadAs!Request;
        actor.value.balance += request.delta;
        request.balance = actor.value.balance;
        node.complete(); // last payload access; release makes the node reusable
    }
    // Not a destructor. Idle retirement can finish without another callback,
    // so required cleanup must not live only in this branch.
    if (context.closing)
        *actor.value.closingCallbacks = actor.value.callbacks;
}

void main(string[] args)
{
    FarmHostOptions hostOptions;
    hostOptions.workers = args.length > 1 ? to!uint(args[1]) : 0;
    hostOptions.waitTimeout = 30.seconds; // example policy: a stuck drain fails instead of hanging
    auto host = new FarmHost(hostOptions);
    scope (exit) host.closeOrAbandon();
    auto runtime = host.createRuntime(1);

    uint closingCallbacks;
    auto owner = runtime.createActor!(Ledger, serve)(Ledger(0, 0, &closingCallbacks));
    if (!owner.valid) fatal("actor creation failed");
    auto handle = owner.handle;

    // Caller-owned, address-stable nodes. initialize once, before any send;
    // calling it on a live node bypasses send/complete checks.
    ActorInboxNode[12] nodes;
    Request[12] requests;
    foreach (i; 0 .. nodes.length)
        nodes[i].initialize(&requests[i], i);

    long expected;
    foreach (round; 0 .. 3)
    {
        requests[0].delta = 1;
        // A still-running activation makes the first send coalesce instead of queue; both are acceptance.
        immutable first = handle.send(&nodes[0]);
        if (!first.accepted)
            fatal("send was rejected");
        // nodeBusy is deterministic only before any flush: no activation can have taken node 0.
        if (round == 0 && handle.send(&nodes[0]) != ActorSendResult.nodeBusy)
            fatal("overlapping node was accepted");
        foreach (i; 1 .. nodes.length)
        {
            requests[i].delta = cast(long)(i + 1);
            immutable result = handle.send(&nodes[i]);
            // The activation may finish and go idle between sends. queued and coalesced are both acceptance.
            if (!result.accepted)
                fatal("send was rejected");
        }
        foreach (i; 0 .. nodes.length)
        {
            host.waitUntil(() => nodes[i].available);
            // available is an acquire of complete's release. The reply is
            // visible; the node may now be reused. Do not read it earlier.
            if (nodes[i].outcome != ActorNodeOutcome.completed
                    || nodes[i].failureCode != 0)
                fatal("request outcome mismatch");
            expected += requests[i].delta;
            if (requests[i].balance != expected)
                fatal("reply order or value mismatch");
        }
    }

    // Senders are quiescent. Retire closes admission, then drains.
    immutable retiring = owner.requestRetire();
    if (!retiring.accepted)
        fatal("retirement rejected");
    if (handle.send(&nodes[0]) != ActorSendResult.closed)
        fatal("send after admission close was accepted");
    host.waitUntil(() => owner.retired);
    if (owner.reclaim() != ActorReclaimResult.reclaimed)
        fatal("reclaim failed");
    // valid() only checks that this copy still holds a slot and generation.
    // It stays true after reclaim. The operation status is the stale check.
    if (!handle.valid || handle.wake() != ActorWakeResult.staleHandle
            || handle.send(&nodes[0]) != ActorSendResult.staleHandle)
        fatal("stale handle check failed");

    // host.close() (scope exit) stops the consumers, then destroys the
    // runtime, unregisters the token and destroys the Farm, in that order.
    if (expected != 234)
        fatal("ledger total mismatch");
    writeln("public service: 36 ordered replies, balance=", expected,
        ", workers=", host.resolution.workers.length, ", closing-callback-count=", closingCallbacks);
}
