module host_live;

// Live FarmHost regression (`dub run -c host-test`). The worker cases need
// at least three accepted CPUs in one LLC (two workers plus the caller) and
// print SKIP otherwise; the workers-0 cases run anywhere.

import core.atomic : atomicLoad, atomicOp, atomicStore;
import core.thread : Thread;
import core.time : Duration, MonoTime, msecs, seconds;
import std.algorithm.sorting : sort;
import std.exception : enforce;
import std.stdio : writeln;

import actors;
import antfarm;
import threadpool.host;
import threadpool.os_scope : liveOsMask;
import threadpool.topology : ProcessorId, formatCpuList;

// ---- public actor: a ledger answering inbox requests ----------------------

struct Request
{
    long delta;
    long balance;
}

struct Ledger
{
    long balance;
}

void serve(scope ref ActorBorrow!Ledger actor, scope ref ActorContext context)
    nothrow @nogc @system
{
    for (auto node = context.popInbox(); node !is null; node = context.popInbox())
    {
        auto request = node.payloadAs!Request;
        actor.value.balance += request.delta;
        request.balance = actor.value.balance;
        node.complete();
    }
}

void publicReplies(FarmHost host)
{
    auto runtime = host.createRuntime(1);
    auto owner = runtime.createActor!(Ledger, serve)(Ledger(0));
    enforce(owner.valid, "actor creation");
    ActorInboxNode[4] nodes;
    Request[4] requests;
    foreach (i; 0 .. nodes.length)
    {
        nodes[i].initialize(&requests[i], i);
        requests[i].delta = i + 1;
        immutable sent = owner.handle.send(&nodes[i]);
        enforce(sent == ActorSendResult.queued || sent == ActorSendResult.coalesced, "send");
    }
    long expected;
    foreach (i; 0 .. nodes.length)
    {
        host.waitUntil(() => nodes[i].available);
        expected += requests[i].delta;
        enforce(nodes[i].outcome == ActorNodeOutcome.completed && requests[i].balance == expected,
            "reply order or value");
    }
    host.retire(owner);
    enforce(!owner.valid, "retire reclaims the owner");
}

// ---- private tree: a tracked root that waits on a child wave ---------------

struct Leaf
{
    ulong* output;
}

void leafStep(scope ref PrivateBorrow!Leaf leaf, scope ref PrivateContext) nothrow @nogc @system
{
    ++*leaf.value.output;
}

shared uint gChildWavesStarted;

struct Root
{
    PrivateChildPark!Leaf* leaves;
    ulong* outputs;
}

PrivateRootDisposition rootStep(scope ref PrivateBorrow!Root root, scope ref PrivateContext context)
    nothrow @nogc @system
{
    if (root.value.leaves !is null)
        return PrivateRootDisposition.finish;
    root.value.leaves = context.createChildren!Leaf(8);
    if (root.value.leaves is null)
        return PrivateRootDisposition.finish;
    foreach (i; 0 .. 8)
        cast(void) root.value.leaves.spawn(context, Leaf(root.value.outputs + i));
    cast(void) root.value.leaves.start!leafStep(context);
    atomicOp!"+="(gChildWavesStarted, 1u);
    return PrivateRootDisposition.waitChildren;
}

void privateTree(FarmHost host)
{
    PrivateRootOptions options;
    options.tracking = true;
    auto park = host.createPark!(Root, rootStep)(4, options);
    ulong[8][4] outputs;
    PrivateRootHandle[4] roots;
    foreach (i; 0 .. 4)
        enforce(park.spawn(Root(null, outputs[i].ptr), roots[i]) == PrivateSpawnResult.spawned, "spawn");
    uint completed;
    host.waitUntil(() {
        PrivateRootOutcome outcome;
        while (park.takeOutcome(outcome))
            if (outcome.status == PrivateOutcomeStatus.completed)
                ++completed;
        return park.live == 0;
    });
    enforce(completed == 4, "every root completed");
    foreach (ref row; outputs)
        foreach (x; row)
            enforce(x == 1, "every leaf ran once");
}

// With both workers parked until woken, the pump that publishes the child
// wave writes no root (roots == 0, tables == 1): only the table count wakes them.
void childOnlyWake(FarmHost host)
{
    PrivateRootOptions options;
    options.tracking = true;
    auto park = host.createPark!(Root, rootStep)(1, options);
    ulong[8] outputs;
    PrivateRootHandle root;
    atomicStore(gChildWavesStarted, 0u);
    enforce(park.spawn(Root(null, outputs.ptr), root) == PrivateSpawnResult.spawned, "spawn");
    host.step(); // publishes the root table and wakes a worker
    auto deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad(gChildWavesStarted) == 0) // no pump: the child wave stays queued
    {
        enforce(MonoTime.currTime < deadline, "root never started its child wave");
        Thread.yield();
    }
    Thread.sleep(50.msecs); // both workers park
    bool completed;
    host.waitUntil(() {
        PrivateRootOutcome outcome;
        while (park.takeOutcome(outcome))
            completed = outcome.status == PrivateOutcomeStatus.completed;
        return park.live == 0;
    });
    enforce(completed && outputs == [1, 1, 1, 1, 1, 1, 1, 1], "child-only publication drained");
}

// ---- wave: publish!advance and a completion join --------------------------

struct Phase
{
    ulong* cell;
}

void unusedWake(scope ref ActorBorrow!Phase, scope ref ActorContext) nothrow @nogc @system {}

void advance(scope ref ActorBorrow!Phase actor) nothrow @nogc @system
{
    *actor.value.cell += 1;
}

void wave(FarmHost host)
{
    auto runtime = host.createRuntime(3);
    ulong[3] cells;
    auto a = runtime.createActor!(Phase, unusedWake)(Phase(&cells[0]));
    auto b = runtime.createActor!(Phase, unusedWake)(Phase(&cells[1]));
    auto c = runtime.createActor!(Phase, unusedWake)(Phase(&cells[2]));
    ActorHandle!Phase[3] handles = [a.handle, b.handle, c.handle];
    ActorWave wave;
    wave.begin(host.farm);
    immutable admitted = host.publish!advance(wave, handles[]);
    auto completion = wave.seal();
    host.waitUntil(() => completion.finished);
    enforce(admitted && !completion.failed && cells == [1, 1, 1], "wave");
    host.retire(a, b, c);
}

// ---- raw token: picked up within idleTimeout without wake() -----------------

void rawToken(FarmHost host)
{
    auto runtime = host.createRuntime(1);
    auto owner = runtime.createActor!(Ledger, serve)(Ledger(0));
    ActorInboxNode node;
    Request request = Request(5);
    node.initialize(&request, 0);
    enforce(owner.handle.send(&node) == ActorSendResult.queued, "send");
    enforce(runtime.flush(host.token) == 1, "raw flush");
    host.waitUntil(() => node.available, 5.seconds);
    enforce(request.balance == 5, "raw reply");
    host.retire(owner);
}

// ---- cases -----------------------------------------------------------------

FarmHostOptions guarded(uint workers)
{
    FarmHostOptions options;
    options.workers = workers;
    options.waitTimeout = 10.seconds;
    return options;
}

ProcessorId[] sorted(const(ProcessorId)[] ids)
{
    auto copy = ids.dup;
    copy.sort();
    return copy;
}

void workersZero()
{
    auto host = new FarmHost(guarded(0));
    enforce(host.state == HostState.open && host.pool is null, "workers 0 host");
    publicReplies(host);
    privateTree(host);
    wave(host);
    host.close();
    enforce(host.state == HostState.closed, "closed");
    writeln("PASS 1: workers 0: public replies, tracked tree and wave, then close");
}

void workersTwo()
{
    // Duration.max parks until woken, so a missed child-publication wake hangs
    // (the 10 s waitTimeout turns that into a failure).
    auto options = guarded(2);
    options.idleTimeout = Duration.max;
    auto host = new FarmHost(options);
    enforce(host.plan.workers == 2 && !host.plan.crossGroup, "two workers in one group");
    publicReplies(host);
    privateTree(host);
    childOnlyWake(host);
    writeln("PASS 2: workers 2, park until woken: public replies, a tracked tree, and a "
        ~ "child-only publication that wakes parked workers");
    wave(host);
    writeln("PASS 3: publish!advance wave joined on completion.finished");
    host.close();
    enforce(host.state == HostState.closed, "closed");
}

void confineCaller()
{
    auto before = liveOsMask();
    auto host = new FarmHost(guarded(2));
    enforce(host.plan.caller == CallerPlacement.confine, "confine by default");
    auto during = liveOsMask();
    enforce(during == sorted(host.plan.callerCpus),
        "caller mask {" ~ formatCpuList(during) ~ "} is plan.callerCpus {"
        ~ formatCpuList(host.plan.callerCpus) ~ "}");
    host.close();
    enforce(liveOsMask() == before, "caller mask restored after close");
    writeln("PASS 4: caller confined to {", formatCpuList(during), "} and restored to {",
        formatCpuList(before), "}");
}

void rawTokenPickup()
{
    auto host = new FarmHost(guarded(2));
    rawToken(host);
    host.close();
    writeln("PASS 5: raw-token flush picked up within idleTimeout without wake()");
}

bool workersFit()
{
    try
        cast(void) FarmHost.preview(guarded(2));
    catch (FarmHostPlanException e)
    {
        writeln("SKIP worker cases: ", e.msg);
        return false;
    }
    return true;
}

int main()
{
    workersZero();
    if (!workersFit())
        return 0;
    writeln(FarmHost.preview(guarded(2)).summary());
    workersTwo();
    confineCaller();
    rawTokenPickup();
    return 0;
}
