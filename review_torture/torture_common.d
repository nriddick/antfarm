/++
 + Shared harness for antfarm torture tests.
 +/
module torture_common;

import antfarm;
import core.atomic;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdlib : malloc, free, abort, exit;

enum DEFAULT_WATCHDOG_SECS = 90;

// Must be __gshared: consumer/producer threads need the same counters.
// (`shared T` alone is TLS-to-shared in D for module globals under some ABIs;
// match antfarm_test.d: __gshared shared(T).)
__gshared shared(long) g_totalCalls;
__gshared shared(long)* g_calls;
__gshared size_t g_npayloads;

void check(bool cond, const(char)[] msg, string file = __FILE__, int line = __LINE__)
{
    if (!cond)
    {
        fprintf(stderr, "CHECK FAILED %s:%d: %.*s\n",
            file.ptr, line, cast(int) msg.length, msg.ptr);
        abort();
    }
}

void allocCalls(size_t n)
{
    g_calls = cast(shared long*) malloc(n * long.sizeof);
    check(g_calls !is null, "malloc g_calls");
    g_npayloads = n;
    foreach (i; 0 .. n)
        atomicStore!(MemoryOrder.raw)(g_calls[i], 0L);
    atomicStore!(MemoryOrder.raw)(g_totalCalls, 0L);
}

void freeCalls()
{
    free(cast(void*) g_calls);
    g_calls = null;
    g_npayloads = 0;
}

long countingCb(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    // body[0] = payload index
    immutable idx = cast(size_t) b[0];
    if (g_calls !is null && idx < g_npayloads)
        atomicFetchAdd(g_calls[idx], 1L);
    atomicFetchAdd(g_totalCalls, 1L);
    return 1;
}

/// Slow-ish CPU burn callback used to keep multiple entrants concurrent.
long slowCountingCb(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    ulong x = iter ^ cast(ulong) b[0];
    foreach (i; 0 .. 20_000)
        x = x * 6364136223846793005UL + 1;
    return countingCb(h, b, iter) ^ cast(long)(x & 1);
}

struct PayloadBatch
{
    PayloadHeader* headers;
    ulong* bodyStore;   // contiguous backing for all bodies
    size_t bodyCap;
    size_t bodyUsed;
    PayloadEntry* entries;
    size_t count;
    long expectedCalls;
}

PayloadBatch makeBatch(size_t n, scope void delegate(size_t i, ref PayloadHeader h, ref size_t plen) fill)
{
    PayloadBatch b;
    b.count = n;
    b.headers = cast(PayloadHeader*) malloc(n * PayloadHeader.sizeof);
    b.entries = cast(PayloadEntry*) malloc(n * PayloadEntry.sizeof);
    // generous body arena: up to 4096 ulongs each worst-case; callers use less
    b.bodyCap = n * 4096;
    b.bodyStore = cast(ulong*) malloc(b.bodyCap * ulong.sizeof);
    b.bodyUsed = 0;
    b.expectedCalls = 0;
    check(b.headers !is null && b.entries !is null && b.bodyStore !is null, "batch alloc");

    foreach (i; 0 .. n)
    {
        b.headers[i] = PayloadHeader.init;
        size_t plen = 2;
        fill(i, b.headers[i], plen);
        if (b.headers[i].call is null)
            b.headers[i].call = &countingCb;
        check(b.bodyUsed + plen <= b.bodyCap, "body arena overflow");
        auto slice = b.bodyStore[b.bodyUsed .. b.bodyUsed + plen];
        // default body layout: [index, done]
        if (plen >= 1) slice[0] = i;
        if (plen >= 2) slice[1] = b.headers[i].done;
        foreach (k; 2 .. plen) slice[k] = k;
        b.entries[i].header = &b.headers[i];
        b.entries[i].body = slice;
        b.bodyUsed += plen;
        b.expectedCalls += b.headers[i].done;
    }
    return b;
}

void freeBatch(ref PayloadBatch b)
{
    free(b.headers);
    free(b.entries);
    free(b.bodyStore);
    b = PayloadBatch.init;
}

struct ProdCtx
{
    AntFarm* f;
    PayloadEntry* entries;
    size_t count;
    Tier tier;
    size_t maxBatch; // 0 = unlimited (write whole remainder)
    shared size_t* written; // optional progress
}

void producerMain(ProdCtx* c)
{
    auto tok = c.f.registerProducer(c.tier);
    check(tok.valid, "producer overregistration");
    ulong exi = 0;
    size_t off;
    size_t stall;
    while (off < c.count)
    {
        immutable remain = c.count - off;
        immutable take = (c.maxBatch == 0 || c.maxBatch >= remain) ? remain : c.maxBatch;
        immutable n = c.f.write(c.entries[off .. off + take], exi, tok);
        off += n;
        if (c.written !is null)
            atomicStore(*c.written, off);
        if (n == 0)
        {
            ++stall;
            // cooperative backoff; avoid tight livelock under heavy backpressure
            if ((stall & 63) == 0)
                Thread.yield();
            else
                Thread.yield();
        }
        else
            stall = 0;
    }
    c.f.unregisterProducer(tok);
}

struct ConsCtx
{
    AntFarm* f;
    long expected;
    MonoTime deadline;
    bool stopOnExpected = true;
}

void consumerMain(ConsCtx* c)
{
    ConsumerView v;
    check(v.subscribe(c.f) >= 0, "subscribe failed");
    for (;;)
    {
        if (!v.consumeNext())
        {
            if (c.stopOnExpected && atomicLoad!(MemoryOrder.acq)(g_totalCalls) >= c.expected)
                break;
            if (MonoTime.currTime > c.deadline)
            {
                fprintf(stderr, "consumer watchdog timeout total=%lld expected=%lld\n",
                    atomicLoad(g_totalCalls), c.expected);
                abort();
            }
            Thread.yield();
        }
    }
    v.unsubscribe();
}

void expectExactCalls(PayloadBatch* b, const(char)[] label)
{
    foreach (i; 0 .. b.count)
    {
        immutable got = atomicLoad!(MemoryOrder.raw)(g_calls[i]);
        if (got != b.headers[i].done)
        {
            fprintf(stderr, "%.*s payload %zu calls=%lld done=%u\n",
                cast(int) label.length, label.ptr, i, got, b.headers[i].done);
            abort();
        }
    }
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == b.expectedCalls, label);
}

uint countSub0Pulses(AntFarm* f)
{
    uint pulses;
    foreach (ki; 0 .. f.K)
        if ((atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]) & LOWMASK) >= SUB0)
            ++pulses;
    return pulses;
}

void expectNoLiveConsumerRefs(AntFarm* f, const(char)[] label)
{
    foreach (ki; 0 .. f.K)
    {
        immutable rt = atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]);
        if ((rt & COUNTMASK) != 0)
        {
            fprintf(stderr, "%.*s leaked count bits ki=%u rt=%llx\n",
                cast(int) label.length, label.ptr, ki, rt);
            abort();
        }
        foreach (l; 0 .. MAX_LEAVES)
        {
            immutable lv = atomicLoad!(MemoryOrder.raw)(f.Lt[ki * MAX_LEAVES + l][0]);
            if (lv != 0)
            {
                fprintf(stderr, "%.*s leaked leaf ki=%u l=%u val=%lld\n",
                    cast(int) label.length, label.ptr, ki, l, lv);
                abort();
            }
        }
    }
}

void say(const(char)[] msg)
{
    printf("%.*s\n", cast(int) msg.length, msg.ptr);
    fflush(stdout);
}
