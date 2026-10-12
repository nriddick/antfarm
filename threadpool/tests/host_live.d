module host_live;

// Live FarmHost regression (`dub run -c host-test`). The worker cases need
// at least three accepted CPUs in one LLC (two workers plus the caller) and
// print SKIP otherwise; the workers-0 cases run anywhere.

import core.atomic : atomicLoad, atomicOp, atomicStore;
import core.thread : Thread;
import core.time : Duration, MonoTime, msecs, seconds;
import std.algorithm.searching : canFind;
import std.algorithm.sorting : sort;
import std.exception : enforce;
import std.stdio : writeln;

import actors;
import antfarm;
import threadpool.host;
import threadpool.hybrid : WorkerSelf;
import threadpool.pool : CacheAwarePool, Director, PoolOptions;
import threadpool.topology : ProcessorId, formatCpuList;

// Windows liveOsMask reports process affinity, not the caller's group mask.
ProcessorId[] liveCallerMask()
{
    version (Windows)
    {
        import core.sys.windows.winbase : GetCurrentThread;
        import threadpool.sys.win_bindings : GROUP_AFFINITY, GetThreadGroupAffinity;
        GROUP_AFFINITY affinity;
        enforce(GetThreadGroupAffinity(GetCurrentThread(), &affinity) != 0,
            "query caller affinity");
        ProcessorId[] result;
        foreach (ushort lp; 0 .. 64)
            if (affinity.Mask & (1UL << lp))
                result ~= ProcessorId(affinity.Group, lp);
        return result;
    }
    else
    {
        import threadpool.os_scope : liveOsMask;
        return liveOsMask();
    }
}

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


// ---- failure paths ---------------------------------------------------------

/// The exception `run` throws, which must be (or derive from) E.
E expectThrow(E : Throwable)(scope void delegate() run, string what)
{
    try
        run();
    catch (E e)
        return e;
    throw new Exception("expected " ~ E.stringof ~ ": " ~ what);
}

// Never pops its inbox until cancelled, so a drain retirement never ends.
struct Stubborn
{
    uint activations;
}

void stubborn(scope ref ActorBorrow!Stubborn actor, scope ref ActorContext context)
    nothrow @nogc @system
{
    ++actor.value.activations;
    if (context.cancelling)
        cast(void) context.cancelInbox();
}

void closeBeforeDrain()
{
    auto host = new FarmHost(guarded(0));
    auto runtime = host.createRuntime(1);
    auto owner = runtime.createActor!(Ledger, serve)(Ledger(0));
    auto e = expectThrow!FarmHostException(() => host.close(), "close with a live actor");
    enforce(e.msg.canFind("runtime#0 live=1"), e.msg);
    enforce(host.state == HostState.open, "close before drain leaves the host open");
    publicReplies(host); // still usable
    host.retire(owner);
    host.close();
    enforce(host.state == HostState.closed, "closed after the drain");
    writeln("PASS 5.1: close before the drain throws, leaves the host usable, then closes");
}

void timeoutThenCancel(uint workers)
{
    auto options = guarded(workers);
    options.waitTimeout = 50.msecs;
    auto host = new FarmHost(options);
    auto runtime = host.createRuntime(1);
    auto owner = runtime.createActor!(Stubborn, stubborn)(Stubborn(0));
    ActorInboxNode node;
    node.initialize(null, 0);
    enforce(owner.handle.send(&node) == ActorSendResult.queued, "send");
    enforce(owner.requestRetire() == ActorRetireResult.requested, "drain request");
    auto e = expectThrow!FarmHostTimeout(() => host.waitUntil(() => owner.retired), "drain that never ends");
    enforce(e.waited == 50.msecs && host.state == HostState.open, "timeout leaves the host open");
    enforce(owner.requestRetire(ActorRetireMode.cancel) == ActorRetireResult.requested, "cancel request");
    host.waitUntil(() => owner.retired);
    enforce(node.available && node.outcome == ActorNodeOutcome.cancelled, "cancelled node");
    enforce(owner.reclaim() == ActorReclaimResult.reclaimed, "reclaim");
    host.close();
    writeln("PASS 5.2: workers ", workers, ": FarmHostTimeout keeps the host open; cancel escalation drains");
}

version (assert)
{
    void failing(scope ref ActorBorrow!Ledger, scope ref ActorContext) nothrow @nogc @system
    {
        assert(false, "host-test: injected actor failure");
    }

    void workerFailure()
    {
        auto host = new FarmHost(guarded(2));
        scope (exit) host.closeOrAbandon();
        auto runtime = host.createRuntime(1);
        auto owner = runtime.createActor!(Ledger, failing)(Ledger(0));
        enforce(owner.handle.wake() == ActorWakeResult.queued, "wake");
        auto e = expectThrow!FarmHostFailed(() => host.waitUntil(() => false), "assert in a callback");
        enforce(e.workerFailures.length != 0
            && e.workerFailures[0].msg == "host-test: injected actor failure", e.msg);
        import threadpool.failure : WorkerFailure;
        auto diagnostic = cast(WorkerFailure!Error) e.workerFailures[0];
        enforce(diagnostic !is null && diagnostic.sourceType == "core.exception.AssertError",
            "owned worker error diagnostic");
        enforce(host.state == HostState.failed, "failed state");
        expectThrow!FarmHostException(() => host.step(), "step after failure");
        host.close(); // acts like abandon: no second throw
        enforce(diagnostic.msg == "host-test: injected actor failure"
            && diagnostic.file.canFind("host_live.d") && diagnostic.line != 0,
            "worker diagnostic remains valid after close");
        enforce(host.state == HostState.abandoned, "close after failure abandons");
        writeln("PASS 5.3: an assert in a worker callback becomes FarmHostFailed; close() leaks quietly");
    }
}

shared uint gUserPumps;

bool userBody(WorkerSelf*) nothrow @nogc @system
{
    import core.atomic : atomicOp;
    atomicOp!"+="(gUserPumps, 1u);
    return false;
}

void singleton()
{
    // A second host with workers is rejected without disturbing the first.
    auto first = new FarmHost(guarded(2));
    auto confined = liveCallerMask();
    FarmHostException e;
    foreach (attempt; 0 .. 2) // a rejection must not release the first host's claim
    {
        e = expectThrow!FarmHostException(() { new FarmHost(guarded(2)); }, "second host");
        enforce(e.msg.canFind("another FarmHost owns the pool"), e.msg);
    }
    enforce(first.state == HostState.open && !first.pool.stopRequested, "first host untouched");
    enforce(liveCallerMask() == confined, "caller placement untouched");
    auto noWorkers = new FarmHost(guarded(0)); // workers 0 starts no pool
    noWorkers.close();
    publicReplies(first);
    first.close();
    writeln("PASS 5.4a: a second FarmHost(2) is rejected; the first keeps working and closes");

    // A user-started pool is rejected and neither shut down nor disturbed.
    PoolOptions options;
    options.processors = [first.plan.workerSelection.workers[0]];
    options.workerBody = &userBody;
    auto user = new CacheAwarePool(options);
    user.start();
    {
        auto director = user.director();
        director.spin();
        e = expectThrow!FarmHostException(() { new FarmHost(guarded(2)); }, "host beside a user pool");
        enforce(e.msg.canFind("a pool is already running"), e.msg);
        immutable before = atomicLoad(gUserPumps);
        Thread.sleep(20.msecs);
        enforce(!user.stopRequested && atomicLoad(gUserPumps) > before, "user pool still running");
    }
    user.shutdown();
    auto after = new FarmHost(guarded(2)); // the claim was released by the rejection
    after.close();
    writeln("PASS 5.4b: a FarmHost beside a user-started pool is rejected; that pool keeps running");
}

void startupFailure()
{
    import threadpool.host : hostTestFailStart;

    hostTestFailStart = 0;
    auto e = expectThrow!FarmHostFailed(() { new FarmHost(guarded(2)); }, "start hook failure");
    hostTestFailStart = -1;
    enforce(e.workerFailures.canFind!(f => f.msg == "FarmHost test hook: injected start failure"), e.msg);
    auto host = new FarmHost(guarded(2)); // claim, pool and globals were undone
    publicReplies(host);
    host.close();
    writeln("PASS 5.4c: a worker start failure ends the startup wait (no deadlock) and undoes everything");
}

void abandonLive()
{
    auto before = liveCallerMask();
    auto host = new FarmHost(guarded(2));
    auto runtime = host.createRuntime(1);
    auto owner = runtime.createActor!(Ledger, serve)(Ledger(0));
    enforce(owner.valid, "live actor");
    host.abandon();
    enforce(host.state == HostState.abandoned && host.pool.workerCount == 0, "consumers stopped");
    enforce(liveCallerMask() == before, "affinity restored");
    host.abandon(); // idempotent
    auto next = new FarmHost(guarded(2));
    next.close();
    writeln("PASS 5.5: abandon() with a live actor stops consumers, restores affinity, frees the pool");
}

void extraProducers()
{
    auto options = guarded(0);
    options.farm.extraSmallProducers = 1;
    auto host = new FarmHost(options);
    auto token = host.registerProducer();
    enforce(token.valid, "extra producer");
    expectThrow!FarmHostException(() { cast(void) host.registerProducer(); }, "beyond extraSmallProducers");
    expectThrow!FarmHostException(() { cast(void) host.registerProducer(Tier.bulk); }, "no bulk tier");
    auto e = expectThrow!FarmHostException(() => host.close(), "outstanding producer");
    enforce(e.msg.canFind("extra producer token outstanding") && host.state == HostState.open, e.msg);
    host.unregisterProducer(token);
    host.close();
    writeln("PASS 5.6: an outstanding extra producer blocks close(); planned slots are enforced");
}

void parkBatchZero()
{
    auto host = new FarmHost(guarded(0));
    PrivateRootOptions options;
    options.batchLimit = 0;
    expectThrow!FarmHostException(() { host.createPark!(Root, rootStep)(1, options); }, "batchLimit 0");
    host.close();
    writeln("PASS 5.7: createPark with batchLimit 0 throws instead of aborting");
}

void retireWaveOwned()
{
    auto host = new FarmHost(guarded(0));
    auto runtime = host.createRuntime(1);
    ulong cell;
    auto owner = runtime.createActor!(Phase, unusedWake)(Phase(&cell));
    ActorHandle!Phase[1] handles = [owner.handle];
    ActorWave wave;
    wave.begin(host.farm);
    enforce(host.publish!advance(wave, handles[]), "publish");
    auto completion = wave.seal();
    // Not stepped yet: the wave still holds the owner.
    auto e = expectThrow!FarmHostException(() => host.retire(owner), "retire a wave-held owner");
    enforce(e.msg.canFind("owner 0") && e.msg.canFind("waveOwned"), e.msg);
    host.waitUntil(() => completion.finished);
    host.retire(owner);
    host.close();
    writeln("PASS 5.8: retiring a wave-held owner names the owner and result");
}

void planErrorCreatesNothing()
{
    expectThrow!FarmHostPlanException(() { new FarmHost(guarded(100_000)); }, "impossible worker count");
    auto host = new FarmHost(guarded(0));
    host.close();
    writeln("PASS 5.9: a plan error creates nothing; FarmHost(0) follows");
}

void useAfterClose()
{
    auto host = new FarmHost(guarded(0));
    host.close();
    expectThrow!FarmHostException(() => host.step(), "step after close");
    expectThrow!FarmHostException(() { host.createRuntime(1); }, "createRuntime after close");
    host.close(); // closing a terminal host is a no-op
    host.abandon();
    enforce(host.state == HostState.closed, "still closed");
    writeln("PASS 5.10: use after close throws FarmHostException; close/abandon are no-ops");
}

// The teardown guard abandons when close refuses undrained work, both
// during application failure and during ordinary scope exit.
void failurePairing()
{
    foreach (applicationFailure; [false, true])
    {
        auto before = liveCallerMask();
        FarmHost escaped;
        bool refused;
        try
        {
            auto host = new FarmHost(guarded(2));
            scope (exit) host.closeOrAbandon();
            escaped = host;
            auto runtime = host.createRuntime(1);
            auto owner = runtime.createActor!(Ledger, serve)(Ledger(0));
            enforce(owner.valid, "live actor");
            if (applicationFailure)
                throw new Exception("host-test: application failure");
        }
        catch (Exception e)
        {
            bool foundApplicationFailure;
            for (Throwable error = e; error !is null; error = error.next)
            {
                refused |= error.msg.canFind("not drained: runtime#0 live=1");
                foundApplicationFailure |= error.msg == "host-test: application failure";
            }
            enforce(refused && foundApplicationFailure == applicationFailure,
                "close refusal and any application failure propagate");
        }
        enforce(refused, "undrained close throws");
        enforce(escaped.state == HostState.abandoned, "abandoned");
        enforce(liveCallerMask() == before, "affinity restored");
        escaped.close();
        auto next = new FarmHost(guarded(2)); // claim released
        next.close();
    }
    writeln("PASS 5.11: closeOrAbandon preserves errors, restores affinity and frees the pool");
}

// Re-execute main with a live actor and return normally. A timeout detects
// non-daemon workers left alive by an incorrectly placed failure guard.
void undrainedExit()
{
    import core.stdc.stdio : remove;
    import std.file : tempDir, thisExePath;
    import std.format : format;
    import std.path : buildPath;
    import std.process : Config, kill, spawnProcess, thisProcessID, tryWait, wait;
    import std.stdio : File, stdin;
    import std.string : toStringz;

    // Not File.tmpfile(): the MSVC CRT creates that in the drive root and
    // can fail for a non-elevated user. A file under tempDir() is portable.
    // C remove, not a catch inside scope(exit): LDC skips that catch while
    // an exception is already unwinding, and DMD rejects the syntax.
    static uint childSeq;
    auto path = buildPath(tempDir(),
        format("antfarm-host-live-%s-%s.log", thisProcessID, childSeq++));
    auto output = File(path, "w+");
    scope (exit)
    {
        output.close();
        remove(path.toStringz);
    }
    auto child = spawnProcess([thisExePath, "--undrained-exit"], stdin, output, output,
        null, Config.retainStdout | Config.retainStderr);
    auto result = tryWait(child);
    immutable deadline = MonoTime.currTime + 10.seconds;
    while (!result.terminated && MonoTime.currTime < deadline)
    {
        Thread.sleep(10.msecs);
        result = tryWait(child);
    }
    if (!result.terminated)
    {
        kill(child, 9);
        wait(child);
        enforce(false, "main returning with an undrained host hung at exit");
    }
    output.seek(0);
    string diagnostic;
    foreach (line; output.byLine())
        diagnostic ~= line;
    enforce(result.status != 0 && diagnostic.canFind("not drained: runtime#0 live=1"),
        "undrained main exits with the close error: " ~ diagnostic);
    writeln("PASS 5.12: main returning undrained exits with an error instead of hanging");
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
    auto before = liveCallerMask();
    auto host = new FarmHost(guarded(2));
    scope (exit) host.closeOrAbandon();
    enforce(host.plan.caller == CallerPlacement.confine, "confine by default");
    auto during = liveCallerMask();
    enforce(during == sorted(host.plan.callerCpus),
        "caller mask {" ~ formatCpuList(during) ~ "} is plan.callerCpus {"
        ~ formatCpuList(host.plan.callerCpus) ~ "}");
    host.close();
    enforce(liveCallerMask() == before, "caller mask restored after close");
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

int main(string[] args)
{
    if (args.length == 2 && args[1] == "--undrained-exit")
    {
        auto host = new FarmHost(guarded(2));
        scope (exit) host.closeOrAbandon();
        auto runtime = host.createRuntime(1);
        auto owner = runtime.createActor!(Ledger, serve)(Ledger(0));
        enforce(owner.valid, "live actor");
        return 0; // close refuses; closeOrAbandon must stop the workers
    }
    workersZero();
    closeBeforeDrain();
    timeoutThenCancel(0);
    extraProducers();
    parkBatchZero();
    retireWaveOwned();
    planErrorCreatesNothing();
    useAfterClose();
    if (!workersFit())
        return 0;
    writeln(FarmHost.preview(guarded(2)).summary());
    workersTwo();
    confineCaller();
    rawTokenPickup();
    timeoutThenCancel(2);
    version (assert) workerFailure();
    singleton();
    startupFailure();
    abandonLive();
    failurePairing();
    undrainedExit();
    return 0;
}
