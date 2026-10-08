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

long callback(PayloadHeader*, PayloadBody, ulong) nothrow @nogc @system
{
    ++calls;
    return 1;
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
    // Full-layout ST singleton: 55 words at SqCs=1 (at most two views).
    auto n = cast(size_t)(words - 55);
    auto body = cast(ulong*) malloc(n * ulong.sizeof);
    require(body !is null, "ordering allocation");
    scope(exit) free(body);
    body[0 .. n] = 0;
    PayloadHeader h;
    h.maxCs = h.done = 1;
    h.call = &callback;
    PayloadEntry[1] entries = [PayloadEntry(&h, body[0 .. n])];
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
    auto result = s.subscribe(f);
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

void main() { staleFallback(); }
