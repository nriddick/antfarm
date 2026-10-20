// Controlled Farm lifetime interleavings. Hooks execute another actor's
// steps synchronously, so these logic failures need neither sleeps nor TSan.
module farm_ordering;
import antfarm;
import core.atomic;
import core.stdc.stdlib : malloc, free;
import core.stdc.stdio : puts;
version (AntfarmWriteAuditHooks) {} else static assert(false);

__gshared ConsumerView* consumer;
__gshared Token* producer;
__gshared bool fired;
__gshared ulong calls;

void require(bool ok, string message) nothrow @nogc @system
{
    if (!ok) fatal(message);
}

extern(C) void callback(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    ++calls;
    return;
}

struct PreparedRange
{
    PayloadEntry[] entries;
    bool permit;
    @property bool empty() const nothrow @nogc { return entries.length == 0; }
    @property PayloadEntry front() nothrow @nogc { return entries[0]; }
    void popFront() nothrow @nogc { entries = entries[1 .. $]; }
    @property PreparedRange save() nothrow @nogc { return this; }
    bool prepareTableFrontN(size_t n) nothrow @nogc { return permit; }
}

ulong writeWords(AntFarm* f, ulong words, bool permit = true) nothrow @nogc @system
{
    // Compact ST singleton (spec 2f): 37 words at SqCs=1 (at most two views).
    auto n = cast(size_t)(words - 37);
    auto body = cast(ulong*) malloc(n * ulong.sizeof);
    require(body !is null, "ordering allocation");
    scope(exit) free(body);
    body[0 .. n] = 0;
    Callback h;

    h = &callback;
    PayloadEntry[1] entries = [PayloadEntry(h, body[0 .. n])];
    return f.write(PreparedRange(entries[], permit), *producer);
}

void reclaimAndGrant(AntFarm* f) nothrow @nogc @system
{
    while (consumer.consumeNext()) {}
    require(consumer.oldestEi == 1, "consumer migrated to epoch one");
    require(atomicLoad(f.Rt[0][0]) == 0, "epoch zero reclaimed");
    // Reject AFTER refreshing an exhausted/insufficient balance. The grant
    // certifies slots 2,3,0, but no reservation or publication occurs yet.
    require(writeWords(f, 3 * f.segCap, false) == 0, "prepare rejects");
    require(atomicLoad(f.Wt) == f.segCap, "grant did not move tail");
}

void delayedSubscriber(AntFarm* f, WriteAuditPhase phase, ulong, ulong)
    nothrow @nogc @system
{
    if (phase != WriteAuditPhase.subscriberProbed || fired) return;
    fired = true;
    // S read Wt==0. While S is paused, P and C move to epoch one and P
    // certifies reuse of slot zero, all before S deposits its first SUB.
    require(writeWords(f, f.segCap) == 1, "publish epoch zero");
    reclaimAndGrant(f);
}

void staleFallback()
{
    auto f = AntFarm.create(2, 4, 1, 1, 3 * (1 << 16), 0, 0);
    scope(exit) f.destroy();
    auto tok = f.registerProducer(Tier.bulk);
    producer = &tok;
    scope(exit) f.unregisterProducer(tok);
    ConsumerView c;
    require(c.subscribe(f) == 0, "initial consumer");
    consumer = &c;
    scope(exit) c.unsubscribe();
    fired = false;
    calls = 0;
    writeAuditHook = &delayedSubscriber;
    ConsumerView s;
    auto result = s.subscribeOnce(f);
    writeAuditHook = null;
    require(fired, "subscriber pause reached");
    // cf2aa91 accepts epoch zero. Spending the grant then hits the always-on
    // fresh-segment guard. Keep that production diagnostic as the oracle.
    require(writeWords(f, 3 * f.segCap) == 1, "spend retained grant");
    require(result == SUBSCRIBE_RETRY, "stale fallback must retry");
    require(atomicLoad(f.Cf) == 1, "retry unwinds registration");
    foreach (i; 0 .. f.K)
        require((atomicLoad(f.Rt[i][0]) >> 32) == 0, "retry unwinds SUB");
    while (c.consumeNext()) {}
    require(calls == 2, "each table executes once");
    require(s.subscribe(f) >= 0, "retry attaches to live frontier");
    s.unsubscribe();
    puts("PASS stale fallback: retained grant, balanced retry, wrap and resubscribe");
}

__gshared ConsumerView laggard;
__gshared bool advanced;

void delayedUnsubscribe(AntFarm* f, WriteAuditPhase phase, ulong e, ulong)
    nothrow @nogc @system
{
    if (e != 0) return;
    if (phase == WriteAuditPhase.plantChecked && !advanced)
    {
        advanced = true;
        // U has dropped its own pins and checked incomplete epoch zero.
        // C now completes it and P certifies its reuse before U's plant.
        reclaimAndGrant(f);
    }
    else if (phase == WriteAuditPhase.plantPublished && advanced)
    {
        // Even a subsequently retracted pulse is unsafe: S can convert it
        // into a real pin before the retract. The fallback fix cannot help
        // this main-walk candidate selected through nonzero Rtlow.
        require(laggard.subscribe(f) == 0, "stale pulse selected epoch zero");
    }
}

void checkIncompletePins(AntFarm* f) nothrow @nogc @system
{
    // Quiescent only: no concurrent metadata writers or pin changes here.
    foreach (i; 0 .. f.K)
    {
        auto e = atomicLoad(f.stats[i].es);
        if (e < 0) continue;
        auto next = cast(uint)((e + 1) & f.kMask);
        if (atomicLoad(f.stats[next].es) != e + 1) continue;
        if (atomicLoad(f.stats[i].seqt) + atomicLoad(f.stats[i].sd) <
                atomicLoad(f.stats[next].seqt))
            require((atomicLoad(f.Rt[i][0]) & LOWMASK) != 0,
                "crossed incomplete epoch lost its pin or pulse");
    }
}

void staleUnsubscribe()
{
    auto f = AntFarm.create(2, 4, 1, 1, 3 * (1 << 16), 0, 0);
    scope(exit) f.destroy();
    auto tok = f.registerProducer(Tier.bulk);
    producer = &tok;
    scope(exit) f.unregisterProducer(tok);
    ConsumerView c, u;
    require(c.subscribe(f) == 0 && u.subscribe(f) == 0, "two initial consumers");
    consumer = &c;
    scope(exit) c.unsubscribe();
    calls = 0;
    require(writeWords(f, f.segCap) == 1, "publish before unsubscribe");
    advanced = false;
    writeAuditHook = &delayedUnsubscribe;
    u.unsubscribe();
    writeAuditHook = null;
    checkIncompletePins(f);
    if (!advanced) reclaimAndGrant(f);
    require(writeWords(f, 3 * f.segCap) == 1, "reuse after unsubscribe");
    while (c.consumeNext()) {}
    require(calls == 2, "unsubscribe preserves exact accounting");
    laggard.unsubscribe();
    checkIncompletePins(f);
    puts("PASS unsubscribe: no stale pulse can resurrect reclaimed epoch");
}

struct EarlyCompletion { bool returned; size_t notifications; }
extern(C) void earlyDone(void* context) nothrow @nogc @system
{
    auto state = cast(EarlyCompletion*) context;
    require(!state.returned && calls == 3, "finisher must join before write returns");
    ++state.notifications;
}
void consumeAtPublication(AntFarm*, WriteAuditPhase phase, ulong, ulong) nothrow @nogc @system
{
    if (phase == WriteAuditPhase.tablePublished) require(consumer.consumeNext(), "published table visible");
}
void earlyCompletion()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 256);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    require(view.subscribe(farm) >= 0, "early completion subscribe");
    scope(exit) view.unsubscribe();
    consumer = &view;
    EarlyCompletion state;
    auto hook = TableCompletionHook(&state, &earlyDone);
    PayloadBody[3] emptyBodies;
    calls = 0;
    writeAuditHook = &consumeAtPublication;
    scope(exit) writeAuditHook = null;
    auto n = farm.write(&callback, emptyBodies[], 0, token, MAX_AVG_COST, &hook);
    state.returned = true;
    require(n == 3 && state.notifications == 1, "empty-body indexed table hook exactly once");
    require(((atomicLoad(farm.buf[2]) >> 32) & 255) == 1, "zero bodies use kind one");
    puts("PASS completion: zero-body uniform table joins and notifies before write returns");
}

void main(string[] args)
{
    if (args.length == 1 || args[1] == "early") earlyCompletion();
    if (args.length == 1 || args[1] == "s1") staleFallback();
    if (args.length == 1 || args[1] == "s2") staleUnsubscribe();
}
