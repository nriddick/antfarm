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

import antfarm.actors;
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
        import threadpool.pin : liveOsMask;
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
    host.pump(park); // exactly one pump: publish the root, leave its child wave queued
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
    auto e = expectThrow!FarmHostNotDrained(() => host.close(), "close with a live actor");
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
        owner.leak(); // the failed host leaked the runtime; reclaim is impossible
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
    options.processors = [first.resolution.workers[0]];
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
    owner.leak(); // abandon() leaked the runtime; reclaim is impossible
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
    auto e = expectThrow!FarmHostNotDrained(() => host.close(), "outstanding producer");
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
    expectThrow!FarmHostResolveException(() { new FarmHost(guarded(100_000)); }, "impossible worker count");
    auto host = new FarmHost(guarded(0));
    host.close();
    writeln("PASS 5.9: a resolve error creates nothing; FarmHost(0) follows");
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
            scope (exit) owner.leak(); // deliberately undrained: the guard abandons
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
    enforce(host.resolution.workers.length == 2 && host.resolution.acceptedGroupCount == 1,
        "two workers in one group");
    enforce(host.resolution is host.pool.resolution && host.resolution.started,
        "the host's resolution is the started pool's");
    enforce(host.summary().canFind("2 workers"), host.summary());
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
    enforce(host.callerCpus.length != 0, "confine by default");
    auto during = liveCallerMask();
    enforce(during == sorted(host.callerCpus),
        "caller mask {" ~ formatCpuList(during) ~ "} is callerCpus {"
        ~ formatCpuList(host.callerCpus) ~ "}");
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
        cast(void) FarmHost.resolve(guarded(2));
    catch (FarmHostResolveException e)
    {
        writeln("SKIP worker cases: ", e.msg);
        return false;
    }
    return true;
}

// A busy emitter must not monopolize a step or skip quieter emitters. The
// caller must regain control (and consume) after a bounded publication burst.
shared ulong emissionJobs;

extern(C) void emissionJob(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    atomicOp!"+="(emissionJobs, 1UL);
    return;
}

void emissionBursts()
{
    foreach (limit; [1u, 3u, FarmHostOptions.init.maxEmitRounds])
    {
        auto options = guarded(0);
        options.maxEmitRounds = limit;
        auto host = new FarmHost(options);
        scope (exit) host.closeOrAbandon();
        atomicStore(emissionJobs, 0UL);
        size_t remaining = limit + 2, published;
        string visits;
        Callback header;

        header = &emissionJob;
        host.attach(HostAttachment("busy emitter", (ref Token token) {
            visits ~= 'a';
            if (remaining == 0) return 0;
            immutable n = host.farm.write(header, ReclamationBodies([1], 1), 1, token);
            remaining -= n;
            published += n;
            return n;
        }));
        host.attach(HostAttachment("quiet emitter", (ref Token token) {
            visits ~= 'b';
            return 0;
        }));
        host.attach(HostAttachment("no emitter")); // null emit is allowed
        host.step();
        enforce(published == limit && remaining == 2, "busy emitter exceeded/missed burst bound");
        foreach (i; 0 .. limit)
            enforce(visits[2*i .. 2*i+2] == "ab", "every round visits each emitter in order");
        enforce(visits.length == 2*limit && atomicLoad(emissionJobs) > 0,
            "step must return to caller consumption after its burst");

        // Stop publishing. One empty round must return promptly, even when
        // previously published callbacks have not all finished.
        remaining = 0;
        visits = null;
        host.step();
        enforce(visits == "ab", "empty round ends burst without inferring completion");
        host.waitUntil(() => atomicLoad(emissionJobs) == published);

        if (limit > 1)
        {
            remaining = 1;
            visits = null;
            host.step();
            enforce(visits == "abab" && remaining == 0,
                "short burst stops at the first empty round");
            host.waitUntil(() => atomicLoad(emissionJobs) == published);
        }
    }
    writeln("PASS: emission bursts are bounded, fair, stop on no progress, and let the caller consume");
}

// Hold a peer after its armed, final empty poll while the first callback is
// still incomplete. This models preemption at the exact reclamation/park
// boundary, rather than relying on a sleep to make the race likely.
shared bool reclamationEntered, reclamationReleaseJob, reclamationHoldIdle;
shared bool reclamationIdleHeld, reclamationReleaseIdle;
shared uint reclamationWorker;
shared ulong reclamationFinished;

void awaitReclamation(scope bool delegate() ready, string diagnostic)
{
    immutable deadline = MonoTime.currTime + 5.seconds;
    for (;;)
    {
        if (ready()) return;
        enforce(MonoTime.currTime < deadline, diagnostic);
        Thread.yield();
    }
}

extern(C) void reclamationJob(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    auto body = bodyPtr[0 .. bodyWords];
    if (body[0] == 0)
    {
        atomicStore(reclamationEntered, true);
        while (!atomicLoad(reclamationReleaseJob)) Thread.yield();
    }
    atomicOp!"+="(reclamationFinished, 1UL);
    return;
}

struct ReclamationBodies
{
    ulong[1] value = [1];
    size_t count;
    @property bool empty() const nothrow @nogc { return count == 0; }
    @property const(ulong)[] front() const nothrow @nogc { return value[]; }
    void popFront() nothrow @nogc { --count; }
    @property ReclamationBodies save() const nothrow @nogc { return this; }
    @property size_t length() const nothrow @nogc { return count; }
}

version (Windows)
    import threadpool.sys.win_wait : gWait;
else version (linux)
    import threadpool.sys.linux_wait : gWait;

void holdReclamationIdle(WorkerSelf* self, ref ConsumerView view, bool consumed)
    nothrow @nogc
{
    if (!consumed && atomicLoad(reclamationHoldIdle)
        && view.oldestEi < view.newestEi
        && view.nextSeq == atomicLoad(view.F.Wt)
        && atomicLoad(gWait[self.workerIndex].wakeWord) == 1)
    {
        // There are only two workers; the other is blocked in the first job.
        atomicStore(reclamationHoldIdle, false);
        atomicStore(reclamationWorker, self.workerIndex);
        atomicStore(reclamationIdleHeld, true);
        while (!atomicLoad(reclamationReleaseIdle)) Thread.yield();
    }
}

void reclamationProgress()
{
    atomicStore(reclamationEntered, false);
    atomicStore(reclamationReleaseJob, false);
    atomicStore(reclamationHoldIdle, false);
    atomicStore(reclamationIdleHeld, false);
    atomicStore(reclamationReleaseIdle, false);
    atomicStore(reclamationFinished, 0UL);
    hostTestAfterConsume = &holdReclamationIdle;
    scope (exit) hostTestAfterConsume = null;
    auto options = guarded(2);
    options.callerConsumes = false;
    options.idleTimeout = Duration.max;
    auto host = new FarmHost(options);
    scope (exit)
    {
        // Release both gates before any failure unwinds into worker join.
        atomicStore(reclamationReleaseJob, true);
        atomicStore(reclamationReleaseIdle, true);
        host.closeOrAbandon();
    }
    Callback header;

    header = &reclamationJob;
    enforce(host.farm.write(header, ReclamationBodies([0], 1), 1, host.token, 3) == 1,
        "initial delayed callback publication");
    host.wake();
    awaitReclamation(() => atomicLoad(reclamationEntered), "delayed callback did not start");
    ulong published = 1;
    immutable fillDeadline = MonoTime.currTime + 5.seconds;
    for (;;)
    {
        enforce(MonoTime.currTime < fillDeadline, "ring did not reach backpressure");
        immutable n = host.farm.write(header, ReclamationBodies([1], 1_000_000),
            1, host.token, 3);
        if (n == 0) break;
        published += n;
        host.wake(); // every accepted publication already has its required wake
    }
    awaitReclamation(() => atomicLoad(reclamationFinished) == published - 1,
        "peer did not finish all unblocked callbacks");
    atomicStore(reclamationHoldIdle, true);
    host.wake();
    awaitReclamation(() => atomicLoad(reclamationIdleHeld), "peer missed armed idle boundary");
    atomicStore(reclamationReleaseJob, true);
    awaitReclamation(() => atomicLoad(reclamationFinished) == published,
        "delayed callback did not complete");
    // Callback return precedes epoch confirmation. Observe that independently
    // before asserting that completed storage, not unfinished work, is pinned.
    auto farm = host.farm;
    awaitReclamation(() {
        foreach (k; 0 .. farm.K - 1)
            if (atomicLoad(farm.stats[k].seqt) + atomicLoad(farm.stats[k].sd)
                < atomicLoad(farm.stats[k + 1].seqt)) return false;
        return true;
    }, "old epochs did not confirm");
    enforce(host.farm.write(header, ReclamationBodies([1], 1), 1, host.token, 3) == 0,
        "held peer must still pin the completed ring");
    host.step(); // no runtimes, parks, attachments, or caller consumption
    enforce(atomicLoad(gWait[atomicLoad(reclamationWorker)].wakeWord) == 0,
        "step without publications must notify the armed peer");
    atomicStore(reclamationHoldIdle, false);
    atomicStore(reclamationReleaseIdle, true);
    awaitReclamation(() {
        if (host.farm.write(header, ReclamationBodies([1], 1), 1, host.token, 3) == 1)
            return true;
        host.step();
        return false;
    }, "step did not restore publication progress");
    host.wake();
    host.waitUntil(() => atomicLoad(reclamationFinished) == published + 1);
    writeln("PASS reclamation: completed epochs released by step with no publications, forever waits");
}

__gshared Thread parallelCaller;
shared ulong parallelJobs, parallelWorkerJobs;
void hostedParallelJob(size_t) nothrow @nogc @system
{
    atomicOp!"+="(parallelJobs, 1UL);
    if (Thread.getThis() !is parallelCaller) atomicOp!"+="(parallelWorkerJobs, 1UL);
}

void parkedParallel()
{
    import std.range : iota;
    import antfarm.parallel : Options;
    auto options = guarded(2);
    options.callerConsumes = true;
    options.farm.ringMiB = 2;
    options.idleTimeout = Duration.max;
    auto host = new FarmHost(options);
    scope (exit) host.closeOrAbandon();
    parallelCaller = Thread.getThis();
    atomicStore(parallelJobs, 0UL);
    atomicStore(parallelWorkerJobs, 0UL);
    awaitReclamation(() => atomicLoad(gWait[0].wakeWord) == 1
        && atomicLoad(gWait[1].wakeWord) == 1, "workers did not arm for indefinite parking");
    // More two-word grains than the whole ring can hold. Only this caller's
    // step() drives progress: it must wake the parked workers, publish the
    // rest after backpressure, and reclaim ring space to finish.
    import antfarm.parallel : parallelFor;
    immutable count = cast(size_t) host.farm.Ln + 1003;
    auto handle = parallelFor!hostedParallelJob(host.farm, iota(count), host.token,
        host.parallelOptions(Options(1, 7)));
    while (!handle.submitted)
    {
        host.step();
        handle.publish(host.token);
    }
    while (!handle.complete) host.step();
    enforce(atomicLoad(parallelJobs) == count, "parked parallel loop lost work");
    enforce(atomicLoad(parallelWorkerJobs) > 0, "parked workers did not help the loop");
    host.close();
    writeln("PASS parallel: parked host, region exceeds ring, caller-only wait joins all work");
}


// ---- GroupHost: one Farm per group, tokens per thread, ordered teardown ----

import threadpool.groups : GroupHost, GroupHostOptions, GroupSlot, releaseThreadTokens;
import threadpool.bins : home;

__gshared shared(ulong) groupJobs;
__gshared shared(ulong)[8] groupJobsBy; // per llc
ConsumerView tGroupView;
bool tGroupSubscribed;

void groupJob(ulong llc) nothrow @nogc @system
{
    atomicOp!"+="(groupJobs, 1UL);
    if (llc < groupJobsBy.length) atomicOp!"+="(groupJobsBy[llc], 1UL);
}

bool groupBody(WorkerSelf* w) nothrow @nogc @system
{
    if (!tGroupSubscribed)
    {
        auto slot = home!GroupSlot();
        if (slot is null || slot.farm is null) return false;
        if (tGroupView.subscribe(slot.farm) < 0) return false;
        tGroupSubscribed = true;
    }
    return tGroupView.consumeNext();
}

void groupStopHook(WorkerSelf*) nothrow @nogc @system
{
    if (tGroupSubscribed) { tGroupView.unsubscribe(); tGroupSubscribed = false; }
}

/// Every accepted group gets its own Farm; the caller publishes a table into
/// each through its per-group token, that group's workers consume it, the
/// caller's affinity is restored and the pool is released for the next host.
void groupHostPerGroup()
{
    import antfarm.templates : payloadRange;
    import std.range : iota, repeat;
    auto before = liveCallerMask();
    GroupHostOptions o;
    o.groups = HostGroups.all();
    o.farmPerGroup = true;
    o.idleTimeout = Duration.max;
    o.farm.ringMiB = 2;
    auto host = new GroupHost(o, &groupBody, &groupStopHook);
    enforce(host.workers >= 2, "group host needs two workers");
    enforce(host.farms.length == host.groups.length, "one Farm per selected group");
    atomicStore(groupJobs, 0UL);
    foreach (ref c; groupJobsBy) atomicStore(c, 0UL);
    ulong expected;
    foreach (llc; host.groups)
    {
        auto farm = host.farmOf(llc);
        enforce(farm !is null, "each selected group has a Farm");
        auto jobs = payloadRange!groupJob(repeat(cast(ulong) llc, 3000));
        ulong left = 3000;
        while (left)
        {
            immutable w = farm.write(jobs, host.token(llc));
            if (w == 0) { host.wakeAll(); Thread.yield(); continue; }
            jobs.popFrontN(cast(size_t) w);
            left -= w;
            host.wakeOne();
        }
        expected += 3000;
    }
    auto deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad(groupJobs) < expected && MonoTime.currTime < deadline)
    {
        host.wakeAll();
        Thread.sleep(1.msecs);
    }
    enforce(atomicLoad(groupJobs) == expected, "every group's table ran");
    foreach (llc; host.groups)
        enforce(atomicLoad(groupJobsBy[llc]) == 3000, "a group's jobs ran on its own Farm");
    host.close();
    enforce(!host.isOpen, "closed");
    enforce(liveCallerMask() == before, "caller mask restored after GroupHost.close");
    // The pool is free again: a FarmHost can follow.
    auto next = new FarmHost(guarded(2));
    next.close();
    writeln("PASS group host: ", host.farms.length == 0 ? host.groups.length : host.farms.length,
        " group(s), one Farm each, tokens per thread, teardown and pool release");
}

/// Worker body that registers the worker's own token on its group Farm
/// before consuming, so a host holds worker tickets as well as the caller's.
__gshared shared(uint) groupWorkerTokens;
bool tGroupTokenTaken;

bool groupTokenBody(WorkerSelf* w) nothrow @nogc @system
{
    import threadpool.groups : groupToken;
    if (!tGroupTokenTaken)
    {
        if (!groupToken(w.llcIndex).valid) return false;
        tGroupTokenTaken = true;
        atomicOp!"+="(groupWorkerTokens, 1u);
    }
    return groupBody(w);
}

void groupTokenStopHook(WorkerSelf* w) nothrow @nogc @system
{
    tGroupTokenTaken = false;
    groupStopHook(w);
}

/// A second GroupHost is rejected before it acquires anything, so its
/// failure cleanup cannot release the first host's caller tokens or clear
/// its Farm registry: the first host's producer tickets stay registered,
/// its Farms keep running jobs, and its close (worker tokens released
/// after the stop hooks, then the Farms destroyed) does not meet a live
/// ticket. A host with no workers takes no pool claim but is rejected too,
/// because the registry is process-wide.
void groupHostSingleton()
{
    import antfarm.templates : payloadRange;
    import std.range : repeat;
    GroupHostOptions o;
    o.groups = HostGroups.all();
    o.farmPerGroup = true;
    o.idleTimeout = Duration.max;
    o.farm.ringMiB = 2;
    atomicStore(groupWorkerTokens, 0u);
    auto first = new GroupHost(o, &groupTokenBody, &groupTokenStopHook);
    auto deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad(groupWorkerTokens) < first.workers && MonoTime.currTime < deadline)
    {
        first.wakeAll();
        Thread.sleep(1.msecs);
    }
    enforce(atomicLoad(groupWorkerTokens) == first.workers, "every worker took a token");
    immutable llc = first.groups[0];
    auto farm = first.farmOf(llc);
    enforce(first.token(llc).valid, "caller token");
    immutable producers = atomicLoad(farm.Prsm);

    foreach (attempt; 0 .. 2)
    {
        auto e = expectThrow!FarmHostException(() { new GroupHost(o, &groupBody, &groupStopHook); },
            "second GroupHost");
        enforce(e.msg.canFind("another GroupHost is open") || e.msg.canFind("another host owns the pool"),
            e.msg);
    }
    enforce(first.isOpen && first.farmOf(llc) is farm, "first host's registry untouched");
    enforce(atomicLoad(farm.Prsm) == producers, "first host's tokens still registered");
    auto none = o;
    none.workers = 0;
    auto e = expectThrow!FarmHostException(() { new GroupHost(none, &groupBody, &groupStopHook); },
        "GroupHost without workers beside an open one");
    enforce(e.msg.canFind("another GroupHost is open"), e.msg);
    enforce(first.farmOf(llc) is farm && atomicLoad(farm.Prsm) == producers,
        "first host untouched by the rejected host without workers");

    atomicStore(groupJobs, 0UL);
    auto jobs = payloadRange!groupJob(repeat(cast(ulong) llc, 1000));
    ulong left = 1000;
    while (left)
    {
        immutable w = farm.write(jobs, first.token(llc));
        if (w == 0) { first.wakeAll(); Thread.yield(); continue; }
        jobs.popFrontN(cast(size_t) w);
        left -= w;
        first.wakeOne();
    }
    deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad(groupJobs) < 1000 && MonoTime.currTime < deadline)
    {
        first.wakeAll();
        Thread.sleep(1.msecs);
    }
    enforce(atomicLoad(groupJobs) == 1000, "first host still runs its group's jobs");
    first.close();
    auto next = new GroupHost(o, &groupBody, &groupStopHook); // the claim was released once
    next.close();
    writeln("PASS group host singleton: a rejected second host leaves the first's tokens, registry and close intact");
}

// ---- GroupHost attachments: raw composition, managed lane, deadline parks ----

import threadpool.groups : GroupAttachment, GroupServiceResult;

__gshared shared(ulong) countingSteps;

bool countingBody(WorkerSelf*) nothrow @nogc @system
{
    atomicOp!"+="(countingSteps, 1UL);
    return false; // never progress of its own
}

/// Two `@nogc` attachments, one consuming: the raw lane composes them, jobs
/// still run on the consuming one's view, the other is stepped. Then a
/// GC-enabled service attachment alone: the managed lane, the host's own
/// view consumes the jobs, and with workers parked until woken the service
/// hook's deadline is the only thing that ends a park, so it keeps being
/// called while the host is otherwise idle.
void groupHostAttachments()
{
    import antfarm.templates : payloadRange;
    import std.range : repeat;

    GroupHostOptions o;
    o.groups = HostGroups.all();
    o.idleTimeout = Duration.max;
    o.farm.ringMiB = 2;
    o.attachments = [GroupAttachment("consumer", &groupBody, &groupStopHook, true),
                     GroupAttachment("counter", &countingBody, null, false)];
    atomicStore(groupJobs, 0UL);
    atomicStore(countingSteps, 0UL);
    auto raw = new GroupHost(o);
    enforce(!raw.managedLane, "only @nogc attachments: the raw lane");
    enforce(raw.attachments.length == 2);
    immutable llc = raw.groups[0];
    auto jobs = payloadRange!groupJob(repeat(cast(ulong) llc, 1000));
    ulong left = 1000;
    while (left)
    {
        immutable w = raw.farmOf(llc).write(jobs, raw.token(llc));
        if (w == 0) { raw.wakeAll(); Thread.yield(); continue; }
        jobs.popFrontN(cast(size_t) w);
        left -= w;
        raw.wakeOne();
    }
    auto deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad(groupJobs) < 1000 && MonoTime.currTime < deadline)
    {
        raw.wakeAll();
        Thread.sleep(1.msecs);
    }
    enforce(atomicLoad(groupJobs) == 1000, "composed raw lane ran the jobs");
    enforce(atomicLoad(countingSteps) != 0, "the second attachment was stepped");
    raw.close();

    shared ulong serves, started, finished, installs;
    GroupAttachment service;
    service.name = "service";
    service.install = (GroupHost h) { atomicOp!"+="(installs, 1UL); enforce(h.farms.length != 0, "Farms exist at install"); };
    service.uninstall = (GroupHost h) { atomicOp!"-="(installs, 1UL); };
    service.start = (WorkerSelf*) { atomicOp!"+="(started, 1UL); };
    service.finish = (WorkerSelf*) { atomicOp!"+="(finished, 1UL); };
    service.serve = (WorkerSelf*) {
        atomicOp!"+="(serves, 1UL);
        // Wake again in 5 ms whether or not anything happened.
        return GroupServiceResult(false, (MonoTime.currTime + 5.msecs).ticks);
    };
    o.attachments = [service];
    atomicStore(groupJobs, 0UL);
    auto managed = new GroupHost(o);
    enforce(managed.managedLane, "a GC-enabled hook selects the managed lane");
    enforce(atomicLoad(installs) == 1, "install ran once on the control thread");
    deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad(started) < managed.workers && MonoTime.currTime < deadline)
        Thread.sleep(1.msecs);
    enforce(atomicLoad(started) == managed.workers, "start ran once per worker");
    jobs = payloadRange!groupJob(repeat(cast(ulong) llc, 1000));
    left = 1000;
    while (left)
    {
        immutable w = managed.farmOf(llc).write(jobs, managed.token(llc));
        if (w == 0) { managed.wakeAll(); Thread.yield(); continue; }
        jobs.popFrontN(cast(size_t) w);
        left -= w;
        managed.wakeOne();
    }
    deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad(groupJobs) < 1000 && MonoTime.currTime < deadline)
        Thread.sleep(1.msecs);
    enforce(atomicLoad(groupJobs) == 1000, "the host's view consumed the jobs");
    // Idle: no wakes from here on. Each worker must still come back from its
    // park for the 5 ms deadline the service hook reports.
    immutable before = atomicLoad(serves);
    Thread.sleep(100.msecs);
    immutable during = atomicLoad(serves) - before;
    enforce(during >= managed.workers * 5,
        "parked workers did not wake for the service deadline");
    managed.close();
    enforce(atomicLoad(finished) == managed.workers, "finish ran once per worker");
    enforce(atomicLoad(installs) == 0, "uninstall ran at close");
    writeln("PASS group host attachments: raw lane composed 2 attachments; managed lane, ",
        managed.workers, " workers woke ", during, " times in 100 ms idle for a 5 ms deadline");
}

__gshared shared(int) blockedEntered, blockedRelease, blockedStops;

bool blockedBody(WorkerSelf*) nothrow @nogc @system
{
    atomicStore(blockedEntered, 1);
    while (atomicLoad(blockedRelease) == 0) Thread.yield();
    return false;
}

void blockedStop(WorkerSelf*) nothrow @nogc @system
{
    atomicOp!"+="(blockedStops, 1);
}

/// shutdown on a pool that is not the live one (here rejected by start)
/// leaves the live pool alone: it neither requests stop nor clears the
/// shared worker records and hooks while the live pool's worker is still
/// inside its body, and the live pool's own shutdown still runs the stop
/// hook and joins.
void rejectedPoolShutdown()
{
    PoolOptions options;
    options.processors = [FarmHost.resolve(guarded(1)).workers[0]];
    options.workerBody = &blockedBody;
    options.workerStop = &blockedStop;
    atomicStore(blockedEntered, 0);
    atomicStore(blockedRelease, 0);
    atomicStore(blockedStops, 0);
    auto live = new CacheAwarePool(options);
    live.start();
    auto deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad(blockedEntered) == 0 && MonoTime.currTime < deadline)
        Thread.yield();
    enforce(atomicLoad(blockedEntered) == 1, "live pool worker entered its body");
    auto rejected = new CacheAwarePool(options);
    auto e = expectThrow!Exception(() { rejected.start(); }, "second pool start");
    enforce(e.msg.canFind("a pool is already running"), e.msg);
    rejected.shutdown();
    enforce(!live.stopRequested && live.workerCount == 1, "live pool still running");
    enforce(atomicLoad(blockedStops) == 0, "live worker not stopped by the rejected pool");
    atomicStore(blockedRelease, 1);
    live.shutdown();
    enforce(atomicLoad(blockedStops) == 1, "live pool's shutdown ran its stop hook");
    rejected.shutdown();
    live.shutdown();
    writeln("PASS pool shutdown: a rejected pool's shutdown leaves the live pool running");
}

int main(string[] args)
{
    if (args.length == 2 && args[1] == "--parallel-only")
    {
        enforce(workersFit(), "parallel test requires two worker CPUs");
        parkedParallel();
        return 0;
    }
    if (args.length == 2 && args[1] == "--groups-only")
    {
        enforce(workersFit(), "group host test requires two worker CPUs");
        groupHostPerGroup();
        groupHostSingleton();
        groupHostAttachments();
        rejectedPoolShutdown();
        return 0;
    }
    if (args.length == 2 && args[1] == "--reclamation-only")
    {
        enforce(workersFit(), "reclamation test requires two worker CPUs");
        reclamationProgress();
        return 0;
    }
    if (args.length == 2 && args[1] == "--undrained-exit")
    {
        auto host = new FarmHost(guarded(2));
        scope (exit) host.closeOrAbandon();
        auto runtime = host.createRuntime(1);
        auto owner = runtime.createActor!(Ledger, serve)(Ledger(0));
        enforce(owner.valid, "live actor");
        scope (exit) owner.leak(); // deliberately undrained: the guard abandons
        return 0; // close refuses; closeOrAbandon must stop the workers
    }
    workersZero();
    emissionBursts();
    closeBeforeDrain();
    timeoutThenCancel(0);
    extraProducers();
    parkBatchZero();
    retireWaveOwned();
    planErrorCreatesNothing();
    useAfterClose();
    if (!workersFit())
        return 0;
    writeln(FarmHost.resolve(guarded(2)).summary());
    workersTwo();
    parkedParallel();
    reclamationProgress();
    confineCaller();
    rawTokenPickup();
    groupHostPerGroup();
    groupHostSingleton();
    groupHostAttachments();
    rejectedPoolShutdown();
    timeoutThenCancel(2);
    version (assert) workerFailure();
    singleton();
    startupFailure();
    abandonLive();
    failurePairing();
    undrainedExit();
    return 0;
}
