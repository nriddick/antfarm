module write_protection;

import antfarm;
import core.atomic;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdlib : malloc, free;

version (AntfarmWriteAuditHooks) {} else
    static assert(false, "build with AntfarmWriteAuditHooks");

__gshared shared(long) calls;
__gshared shared(int) entered, releaseBody, finishWorker;
__gshared shared(ulong) bodyPasses;
__gshared Token* otherToken;
__gshared ConsumerView* drainer;
__gshared ConsumerView laggard;
__gshared WriteAuditPhase pausePhase;
__gshared bool fired;
__gshared ulong nestedWrites;
__gshared ulong injectValue, injectSegment;
__gshared bool blockBody;

void require(bool b, string s) nothrow @nogc @system
{
    if (!b) fatal(s);
}

ulong pattern(ulong i) nothrow @nogc @safe
{
    return 0xD31F_B9C4_28A6_750EUL ^ (i * 0x9E37_79B9_7F4A_7C15UL);
}

void verifyBody(PayloadBody body) nothrow @nogc @system
{
    foreach (i, word; body)
        if (word != pattern(i)) fatal("protected payload body changed");
}

long bodyCallback(PayloadHeader*, PayloadBody body, ulong)
    nothrow @nogc @system
{
    verifyBody(body);
    if (blockBody && body.length > 50_000)
    {
        atomicStore!(MemoryOrder.rel)(entered, 1);
        while (atomicLoad!(MemoryOrder.acq)(releaseBody) == 0)
        {
            verifyBody(body);
            atomicFetchAdd!(MemoryOrder.raw)(bodyPasses, 1UL);
        }
        verifyBody(body);
    }
    atomicFetchAdd!(MemoryOrder.rel)(calls, 1L);
    return 1;
}

// A one-payload, maxCs=done=1 table has 55 + 8*(SqCs-1) overhead words.
ulong writeWords(AntFarm* f, ref Token tok, ulong words)
    nothrow @nogc @system
{
    immutable cf = atomicLoad!(MemoryOrder.raw)(f.Cf);
    immutable sq = sqcsOf(cf > 0 ? cast(ulong) cf : 1);
    immutable overhead = 55 + 8 * (sq - 1);
    require(words > overhead, "test table too short");
    immutable n = cast(size_t)(words - overhead);
    auto mem = cast(ulong*) malloc(n * ulong.sizeof);
    require(mem !is null, "test allocation");
    scope (exit) free(mem);
    foreach (i; 0 .. n) mem[i] = pattern(i);
    PayloadHeader h;
    h.maxCs = h.done = 1;
    h.call = &bodyCallback;
    PayloadEntry[1] entries = [PayloadEntry(&h, mem[0 .. n])];
    immutable before = atomicLoad!(MemoryOrder.raw)(f.Wt);
    immutable result = f.write(entries[], tok);
    // Scheduling hooks may run other producers; only ordinary calls assert size.
    if (result && writeAuditHook is null)
        require(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + words,
            "test table geometry disagrees with implementation");
    return result;
}

void drain(ref ConsumerView c) nothrow @nogc @system
{
    uint guard;
    while (c.consumeNext())
        require(++guard < 1_000_000, "drain did not reach frontier");
}

void reset() nothrow @nogc @system
{
    calls = 0; entered = 0; releaseBody = 0; finishWorker = 0;
    bodyPasses = 0; nestedWrites = 0; fired = false; blockBody = false;
    writeAuditHook = null;
}

void geometry()
{
    foreach (uint k; [2u, 4u, 8u, 16u])
    {
        reset();
        auto f = AntFarm.create(1 << 18, k, 1, 1, (k - 1) * ((1 << 18) / k), 0, 0);
        ConsumerView c;
        require(c.subscribe(f) >= 0, "geometry subscribe");
        auto tok = f.registerProducer(Tier.bulk);
        ulong count;
        // Exact boundaries and repeated wrap, then varied multi-segment spans.
        foreach (i; 0 .. 3 * k)
        {
            require(writeWords(f, tok, f.segCap) == 1, "exact boundary write");
            ++count;
            drain(c);
        }
        foreach (i; 0 .. 80)
        {
            immutable size = 100 + (i * 7919UL) % (f.quotaBulk - 100);
            require(writeWords(f, tok, size) == 1, "spanning write");
            ++count;
            drain(c);
        }
        require(atomicLoad(calls) == count, "geometry exact callbacks");
        printf("geometry K=%u Wt=%llu callbacks=%llu largePages=%d PASS\n",
            k, atomicLoad(f.Wt), count, f.usedLargePages);
        c.unsubscribe();
        f.unregisterProducer(tok);
        f.destroy();
    }
}

// Hold a real callback while another producer exhausts every usable grant.
// Only the table's starting segment is pinned; its body extends across three
// following segments, so this also tests the indirect protection of the body.
void heldBody()
{
    reset();
    auto f = AntFarm.create(1 << 18, 8, 1, 1, 110_000, 1, 4096);
    auto big = f.registerProducer(Tier.bulk);
    auto small = f.registerProducer(Tier.small);
    ConsumerView c;
    require(c.subscribe(f) >= 0, "held body subscribe");
    blockBody = true;
    require(writeWords(f, big, 100_000) == 1, "held body publication");
    auto worker = new Thread({
        while (atomicLoad!(MemoryOrder.acq)(finishWorker) == 0)
            if (!c.consumeNext()) Thread.yield();
        drain(c);
    });
    worker.start();
    auto limit = MonoTime.currTime + 20.seconds;
    while (atomicLoad!(MemoryOrder.acq)(entered) == 0)
    {
        require(MonoTime.currTime < limit, "held callback start deadline");
        Thread.yield();
    }
    ulong sent = 1;
    while (writeWords(f, small, 1024))
        require(++sent < 10_000, "producer lapped blocked callback");
    immutable stopped = atomicLoad(f.Wt);
    foreach (i; 0 .. 10_000)
        require(writeWords(f, small, 1024) == 0, "renewed past held callback");
    require(atomicLoad(f.Wt) == stopped, "backpressure advanced Wt");
    require(stopped < f.Ln, "tail reached pinned epoch zero");
    require(atomicLoad(bodyPasses) != 0, "callback reread body under pressure");
    atomicStore!(MemoryOrder.rel)(releaseBody, 1);
    while (atomicLoad!(MemoryOrder.acq)(calls) < sent)
    {
        require(MonoTime.currTime < limit, "held callback drain deadline");
        Thread.yield();
    }
    atomicStore!(MemoryOrder.rel)(finishWorker, 1);
    worker.join();
    require(atomicLoad(calls) == sent, "held body exact callbacks");
    printf("held body Wt=%llu rejected=10000 bodyPasses=%llu callbacks=%llu PASS\n",
        stopped, atomicLoad(bodyPasses), sent);
    c.unsubscribe();
    f.unregisterProducer(big);
    f.unregisterProducer(small);
    f.destroy();
}

void delayHook(AntFarm* f, WriteAuditPhase phase, ulong a, ulong b)
    nothrow @nogc @system
{
    if (phase != pausePhase || fired) return;
    fired = true;
    auto hook = writeAuditHook;
    writeAuditHook = null;
    scope (exit) writeAuditHook = hook;
    if (phase != WriteAuditPhase.tailReserved)
    {
        // While A is stopped at its anchor/sweep, B and the consumer make
        // more than two laps. Then freeze a genuine old reference and let B
        // drain newer work until backpressure, leaving A's reserved headroom.
        while (atomicLoad(f.Wt) < 2 * f.Ln)
        {
            require(writeWords(f, *otherToken, 256) == 1, "delayed grant warmup");
            ++nestedWrites;
            drain(*drainer);
        }
        require(laggard.subscribe(f) >= 0, "laggard subscribe");
    }
    // If A is stopped after reservation, its unpublished table blocks the
    // consumer. B may still reserve/publish successors out of order.
    while (writeWords(f, *otherToken, 256))
    {
        require(++nestedWrites < 100_000, "delayed writer bypassed laggard");
        drain(*drainer);
    }
    printf("delay phase=%u anchor=%llu pausedEnd=%llu interveningWt=%llu held=%llu\n",
        cast(uint) phase, a, b, atomicLoad(f.Wt),
        laggard.hasRef ? laggard.oldestEi : drainer.oldestEi);
}

void delays()
{
    foreach (uint k; [2u, 4u, 8u, 16u])
    foreach (phase; [WriteAuditPhase.anchorProbed, WriteAuditPhase.quotaSwept,
                     WriteAuditPhase.tailReserved])
    {
        reset();
        immutable seg = (1 << 18) / k;
        // Both a sub-segment and a multi-segment runoff zone.
        foreach (ulong qa; [cast(ulong) seg / 2, cast(ulong)(k - 1) * seg - 1024])
        {
            reset();
            auto f = AntFarm.create(1 << 18, k, 2, 1, qa, 1, 1024);
            ConsumerView c;
            require(c.subscribe(f) >= 0, "delay subscribe");
            auto a = f.registerProducer(Tier.bulk);
            auto b = f.registerProducer(Tier.small);
            otherToken = &b; drainer = &c; pausePhase = phase;
            writeAuditHook = &delayHook;
            immutable result = writeWords(f, a, qa);
            writeAuditHook = null;
            require(fired, "delay hook did not fire");
            // A stale scan may legitimately fail; a previously granted write
            // must complete. Both outcomes must preserve the old reference.
            if (phase != WriteAuditPhase.anchorProbed)
                require(result == 1, "previously granted write failed");
            if (laggard.hasRef)
            {
                require((atomicLoad(f.Wt) >> f.segShift) < laggard.oldestEi + k,
                    "delayed grant reserved over held epoch");
                laggard.unsubscribe();
            }
            drain(c);
            require(atomicLoad(calls) == nestedWrites + result, "delays exact callbacks");
            printf("delay K=%u quota=%llu phase=%u result=%llu callbacks=%lld PASS\n",
                k, qa, cast(uint) phase, result, atomicLoad(calls));
            c.unsubscribe();
            f.unregisterProducer(a); f.unregisterProducer(b); f.destroy();
        }
    }
}

void subHook(AntFarm* f, WriteAuditPhase phase, ulong ki, ulong)
    nothrow @nogc @system
{
    if (phase != WriteAuditPhase.subscriberPinned || ki != 1 || fired) return;
    fired = true;
    writeAuditHook = null;
    // A legal spend of already-swept quota while the subscriber has only a
    // provisional SUB on a confirmed slot from the previous lap.
    require(writeWords(f, *otherToken, 200) == 1, "subscriber race publication");
}

void subscriptionRace()
{
    reset();
    auto f = AntFarm.create(1 << 18, 8, 2, 1, 65536, 0, 0);
    ConsumerView c, late;
    require(c.subscribe(f) >= 0, "subscriber race initial subscribe");
    auto tok = f.registerProducer(Tier.bulk);
    foreach (i; 0 .. 8)
    {
        require(writeWords(f, tok, f.segCap) == 1, "subscriber race warmup");
        drain(c);
    }
    require(writeWords(f, tok, f.segCap - 100) == 1, "subscriber race boundary setup");
    drain(c);
    otherToken = &tok;
    writeAuditHook = &subHook;
    require(late.subscribe(f) >= 0, "subscriber race late subscribe");
    writeAuditHook = null;
    require(fired, "subscriber race hook");
    drain(c); drain(late);
    require(atomicLoad(calls) == 10, "subscriber race exact callbacks");
    printf("subscription race: provisional SUB ignored; all 10 payloads intact PASS\n");
    late.unsubscribe(); c.unsubscribe(); f.unregisterProducer(tok); f.destroy();
}

void injectionHook(AntFarm* f, WriteAuditPhase phase, ulong, ulong)
    nothrow @nogc @system
{
    if (phase != WriteAuditPhase.quotaSwept || fired) return;
    fired = true;
    atomicStore(f.Rt[injectSegment][0], injectValue);
}

void injection(string mode)
{
    reset();
    auto f = AntFarm.create(1 << 18, 8, 1, 1, 110_000, 0, 0);
    auto tok = f.registerProducer(Tier.bulk);
    injectSegment = mode == "inject-second" ? 2 : 1;
    injectValue = mode == "inject-sub0" ? SUB0 : mode == "inject-both" ? SUB0 + 1 : 1;
    writeAuditHook = &injectionHook;
    // Exact boundary landing, or a later segment in a spanning reservation.
    writeWords(f, tok, mode == "inject-second" ? 100_000 : f.segCap);
    fatal("negative control did not trip allWriteCheck");
}

void main(string[] args)
{
    setvbuf(stdout, null, _IONBF, 0);
    printf("allWriteCheck ENABLED (unconditional)\n");
    if (args.length > 1 && args[1] == "subscription-race")
        return subscriptionRace();
    if (args.length > 1 && args[1].length >= 7 && args[1][0 .. 7] == "inject-")
        return injection(args[1]);
    geometry();
    heldBody();
    delays();
    subscriptionRace();
    printf("WRITE PROTECTION TESTS PASSED\n");
}
