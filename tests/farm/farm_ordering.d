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
    // Singleton total: table base plus two words and the payload body.
    immutable cf = atomicLoad(f.Cf);
    immutable base = 27 + 8UL * sqcsOf(cf > 0 ? cast(ulong) cf : 1);
    auto n = cast(size_t)(words - base - 2);
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

void staleProbe()
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
    // S's first round probed Wt == 0 and pinned slot 0 after P had already
    // been granted it: a Sub on granted space protects nothing. Epoch zero is
    // discharged, so S must not attach there; its next round re-probes and
    // attaches at Live epoch one. Spending the grant passes the always-on
    // fresh-segment guard only if S left nothing on slot 0; keep that
    // production diagnostic as the oracle.
    require(result == 1, "stale probe attaches at the Live current epoch");
    require(writeWords(f, 3 * f.segCap) == 1, "spend retained grant");
    require(atomicLoad(f.Cf) == 2, "both views counted");
    foreach (i; 0 .. f.K)
        require((atomicLoad(f.Rt[i][0]) >> 32) == 0, "attach unwinds every Sub");
    while (c.consumeNext()) {}
    while (s.consumeNext()) {}
    require(calls == 2, "each table executes once");
    s.unsubscribe();
    puts("PASS stale probe: no attach on granted space, Subs unwound, exact accounting");
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
            require((atomicLoad(f.Rt[i][0]) & LIVE) != 0,
                "crossed incomplete epoch is not Live");
    }
}

void departureLeavesLive()
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
    // U leaves epoch zero incomplete and creates nothing: the epoch keeps
    // the Live it was constructed with, and C's reference.
    u.unsubscribe();
    checkIncompletePins(f);
    require((atomicLoad(f.Rt[0][0]) & LIVE) != 0, "incomplete epoch zero still Live");
    // C completes and discharges epoch zero; P certifies its slot for reuse.
    reclaimAndGrant(f);
    // A subscriber arriving now must not select the reclaimed, granted epoch.
    ConsumerView late;
    require(late.subscribe(f) == 1, "late subscriber attaches at Live epoch one");
    require(writeWords(f, 3 * f.segCap) == 1, "reuse after unsubscribe");
    while (c.consumeNext()) {}
    while (late.consumeNext()) {}
    require(calls == 2, "unsubscribe preserves exact accounting");
    late.unsubscribe();
    checkIncompletePins(f);
    puts("PASS unsubscribe: departure creates no protection; reclaimed epoch never selected");
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

// A subscriber attaches at a spanning table's interior and then advances on
// the hot path. D never acquired the spanning table's sentinel, but confirms
// epoch 0 after F finishes it. S therefore has Es[1] in its history, not
// necessarily Es[2]. The Seqt-epoch acquire in subscribe validation must make
// every crossed-epoch initialization visible before S's first moveRef(2).
void spannedAttach()
{
    auto farm = AntFarm.create(1, 4, 1, 1, 48000, 1, 16384, 64);
    scope(exit) farm.destroy();
    auto q = farm.registerProducer(Tier.small);
    auto b = farm.registerProducer(Tier.bulk);
    scope(exit) farm.unregisterProducer(q);
    scope(exit) farm.unregisterProducer(b);
    ConsumerView h, f, d, s;
    calls = 0;
    require(h.subscribe(farm) == 0 && f.subscribe(farm) == 0 && d.subscribe(farm) == 0,
        "span: three initial consumers");
    producer = &q;
    require(writeWords(farm, 15000) == 1 && writeWords(farm, 15000) == 1,
        "span: small fillers");
    while (f.consumeNext()) {}
    require(f.nextSeq == 30000 && !f.sweeperNext, "span: F parked without sweeper role");

    producer = &b;
    require(writeWords(farm, 66045 - 30000) == 1, "span: crossing table");
    require(farm.stats[1].seqt == 66045 && farm.stats[2].seqt == 66045,
        "span: epoch 1 is an interior");
    require(f.consumeNext(), "span: F passes crossing table");
    require(f.nextSeq == 66045 && f.newestEi == 0, "span: F remains on epoch 0");
    require(!f.consumeNext(), "span: F's idle re-walk finishes the table");
    require(f.oldestEi == 0 && f.newestEi == 0, "span: F parked on confirmed epoch 0");

    d.unsubscribe();
    require((atomicLoad(farm.Rt[0][0]) & LIVE) == 0, "span: D discharged epoch 0");
    h.unsubscribe();
    require((atomicLoad(farm.Rt[1][0]) & LIVE) != 0 &&
        (atomicLoad(farm.Rt[2][0]) & LIVE) != 0, "span: interior and frontier remain Live");
    immutable at = s.subscribe(farm);
    require(at == 1 && s.nextSeq == 66045, "span: S attaches at the interior");

    producer = &q;
    require(writeWords(farm, 1000) == 1, "span: next table from retained quota");
    require(s.consumeNext() && s.newestEi == 2,
        "span: first advance sees every crossed-epoch Es");
    while (s.consumeNext() || f.consumeNext()) {}
    s.unsubscribe();
    f.unsubscribe();
    require(calls == 4, "span: every table executes exactly once");
    puts("PASS spanned interior attach: Seqt validation publishes every crossed epoch");
}

void main(string[] args)
{
    if (args.length == 1 || args[1] == "early") earlyCompletion();
    if (args.length == 1 || args[1] == "s1") staleProbe();
    if (args.length == 1 || args[1] == "s2") departureLeavesLive();
    if (args.length == 1 || args[1] == "span") spannedAttach();
}
