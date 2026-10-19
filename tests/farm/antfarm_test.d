/++
 + Tests for the Ant Farm. Run from the repository root: dmd -g -O -inline -i -I. tests/farm/antfarm_test.d -of=antfarm_test && ./antfarm_test
 +/
module antfarm_test;

// Run the main correctness suite after imported module unittests as well.
extern(C) __gshared string[] rt_options = ["testmode=run-main"];

import antfarm.templates;
import antfarm.actors;
import antfarm.allocation : allocateAligned64, freeAligned64;
import core.atomic;
import core.lifetime : move;
import core.memory : GC;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdio : fflush, stdout;
import core.stdc.string : memset;
import std.range : hasLength, iota, isForwardRange, isInputRange, popFrontN;
import std.typecons : Tuple, tuple;
import core.stdc.stdlib : malloc, free;

static assert(!DEFAULT_HUGE_PAGES, "ordinary 4 KiB backing is the public default");

// ---------------------------------------------------------------------
// Shared test state
// ---------------------------------------------------------------------

__gshared shared(long)* g_calls;   // per-payload call counts
__gshared size_t g_npayloads;
__gshared shared(long) g_totalCalls;
__gshared ulong* g_keepalive;      // keeps GC away (we malloc instead)

struct ActorAllocCounts
{
    size_t allocations;
    size_t deallocations;
}

void* actorTestAllocate(void* context, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    if (alignment > 64) return null;
    auto counts = cast(ActorAllocCounts*) context;
    ++counts.allocations;
    return allocateAligned64(bytes);
}

void actorTestDeallocate(void* context, void* memory, size_t, size_t)
    nothrow @nogc @system
{
    auto counts = cast(ActorAllocCounts*) context;
    ++counts.deallocations;
    freeAligned64(memory);
}

struct ActorCounterState
{
    ulong mutations;
    ulong target;
}

__gshared shared(long) g_actorEntries;
__gshared shared(long) g_actorConcurrent;
__gshared shared(long) g_actorCalls;

void actorCounter(scope ref ActorBorrow!ActorCounterState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    immutable entered = atomicFetchAdd!(MemoryOrder.acq_rel)(
        g_actorEntries, 1L);
    if (entered != 0)
        atomicFetchAdd!(MemoryOrder.rel)(g_actorConcurrent, 1L);

    atomicFetchAdd!(MemoryOrder.rel)(g_actorCalls, 1L);
    ++actor.value.mutations;
    immutable finished = actor.value.mutations == actor.value.target;

    atomicFetchSub!(MemoryOrder.rel)(g_actorEntries, 1L);
    if (finished)
        context.retire();
    else
        context.republish();
}

private alias ScopedActorCounterHandler = void function(
        scope ref ActorBorrow!ActorCounterState, scope ref ActorContext)
    nothrow @nogc @system;
private alias UnscopedActorCounterHandler = void function(
        ref ActorBorrow!ActorCounterState, ref ActorContext)
    nothrow @nogc @system;
static assert(is(typeof(&actorCounter) : ScopedActorCounterHandler));
static assert(!is(UnscopedActorCounterHandler : ScopedActorCounterHandler),
    "unscoped actor handlers must not satisfy the callback contract");

struct ErasedActorResult
{
    ulong activations;
    ulong messages;
    ulong sum;
    size_t observedSize;
    size_t observedAlignment;
}

struct ErasedActorState
{
    ErasedActorResult* result;
    ulong target;
}

void erasedActor(scope ref ActorErasedBorrow actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    ref ErasedActorState state = actor.value!ErasedActorState();
    ++state.result.activations;
    state.result.observedSize = actor.stateSize;
    state.result.observedAlignment = actor.stateAlignment;
    while (true)
    {
        auto node = context.popInbox();
        if (node is null) break;
        ++state.result.messages;
        state.result.sum += node.tag;
        node.complete();
    }
    if (state.result.activations == state.target)
        context.retire();
    else
        context.republish();
}

private alias UnscopedErasedHandler = void function(
        ref ActorErasedBorrow, ref ActorContext) nothrow @nogc @system;
static assert(is(typeof(&erasedActor) : ActorErasedHandler));
static assert(!is(UnscopedErasedHandler : ActorErasedHandler),
    "unscoped erased handlers must not satisfy the callback contract");

struct ActorInboxState
{
    ulong consumed;
    ulong sum;
}

struct ActorInboxMessage
{
    ActorInboxNode node;
    ulong value;
}

__gshared shared(long) g_inboxEntries;
__gshared shared(long) g_inboxConcurrent;
__gshared shared(long) g_inboxActivations;
__gshared shared(long) g_inboxConsumed;
__gshared shared(ulong) g_inboxSum;
__gshared shared(long) g_inboxAccepted;
__gshared shared(long) g_inboxClosed;
__gshared shared(long) g_inboxUnexpected;

void inboxActor(scope ref ActorBorrow!ActorInboxState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    enum drainLimit = 7;
    immutable entered = atomicFetchAdd!(MemoryOrder.acq_rel)(
        g_inboxEntries, 1L);
    if (entered != 0)
        atomicFetchAdd!(MemoryOrder.rel)(g_inboxConcurrent, 1L);
    atomicFetchAdd!(MemoryOrder.rel)(g_inboxActivations, 1L);

    foreach (_; 0 .. drainLimit)
    {
        auto node = context.popInbox();
        if (node is null) break;
        auto message = cast(ActorInboxMessage*) node.payload;
        ++actor.value.consumed;
        actor.value.sum += message.value;
        atomicFetchAdd!(MemoryOrder.rel)(g_inboxConsumed, 1L);
        atomicFetchAdd!(MemoryOrder.rel)(g_inboxSum, message.value);
        node.complete();
    }

    atomicFetchSub!(MemoryOrder.rel)(g_inboxEntries, 1L);
}

struct ActorWaveShared
{
    ulong* values;
    ulong* observed;
    size_t count;
}

struct ActorWaveState
{
    ActorWaveShared* sharedState;
    size_t index;
    ulong localValue;
    ulong neighborValue;
}

__gshared shared(long) g_waveAutonomousCalls;

void actorWaveCompletion(void* context) nothrow @nogc @system
{
    auto completions = cast(shared(long)*) context;
    atomicFetchAdd!(MemoryOrder.rel)(*completions, 1L);
}

void actorWaveDormant(scope ref ActorBorrow!ActorWaveState,
        scope ref ActorContext) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.rel)(g_waveAutonomousCalls, 1L);
}

void actorWaveProduce(scope ref ActorBorrow!ActorWaveState actor)
    nothrow @nogc @system
{
    actor.value.localValue = cast(ulong) actor.value.index * 3UL + 11UL;
    actor.value.sharedState.values[actor.value.index] = actor.value.localValue;
}

void actorWaveObserve(scope ref ActorBorrow!ActorWaveState actor)
    nothrow @nogc @system
{
    immutable neighbor = (actor.value.index + 1)
        % actor.value.sharedState.count;
    actor.value.neighborValue = actor.value.sharedState.values[neighbor];
    actor.value.sharedState.observed[actor.value.index]
        = actor.value.neighborValue;
}

private long incCalls(size_t idx) nothrow @nogc @system
{
    atomicFetchAdd(g_calls[idx], 1L);
    atomicFetchAdd(g_totalCalls, 1L);
    return 1;
}

long testCb(size_t idx, ulong done) nothrow @nogc @system
{
    return incCalls(idx);
}

long testCb1(size_t idx) nothrow @nogc @system
{
    return incCalls(idx);
}

void allocCalls(size_t n)
{
    g_calls = cast(shared(long)*) malloc(n * long.sizeof);
    g_npayloads = n;
    foreach (i; 0 .. n)
        g_calls[i] = 0;
    atomicStore!(MemoryOrder.raw)(g_totalCalls, 0L);
}

void freeCalls() { free(cast(void*) g_calls); g_calls = null; }

void check(bool cond, const(char)[] msg, string file = __FILE__, int line = __LINE__)
{
    if (!cond)
    {
        fprintf(stderr, "CHECK FAILED %s:%d: %.*s\n", file.ptr, line, cast(int) msg.length, msg.ptr);
        import core.stdc.stdlib : abort;
        abort();
    }
}

ulong lowHalf(AntFarm* f, uint ki)
{
    return atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]) & LOWMASK;
}

ulong highHalf(AntFarm* f, uint ki)
{
    return atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]) >> 32;
}

void checkNoLeftoverSub(AntFarm* f, const(char)[] msg)
{
    foreach (ki; 0 .. f.K)
        check(highHalf(f, cast(uint) ki) == 0, msg);
}

// ---------------------------------------------------------------------
// Test 1: arithmetic
// ---------------------------------------------------------------------

void testMagicWrap()
{
    void probe(bool huge, const(char)[] label)
    {
        auto f = AntFarm.create(2, 8, 1, 1, 2048, 1, 512,
            DEFAULT_SMALL_TABLE_THRESHOLD, huge);
        scope (exit) f.destroy();
        if (huge && !f.usedLargePages)
            return; // ANTFARM_HUGE_PAGES=0
        auto p = cast(ulong*) f.buf;
        p[0] = 0x1111_2222_3333_4444UL;
        p[f.Ln - 1] = 0xAAAA_BBBB_CCCC_DDDDUL;
        check(p[f.Ln] == p[0], label);
        check(p[2 * f.Ln - 1] == p[f.Ln - 1], label);
        p[f.Ln] = 0x5555_6666_7777_8888UL;
        check(p[0] == 0x5555_6666_7777_8888UL, label);
    }
    probe(false, "wrap 4K");
    probe(true, "wrap large pages");
    printf("testMagicWrap OK\n"); fflush(stdout);
}

void testRingSizeUnits()
{
    foreach (mib; [2u, 4u, 8u])
    {
        auto f = AntFarm.create(ringMiB: mib, k: 8, expectedConsumers: 1,
                                maxBulk: 0, maxSmall: 1, quotaSmall: 512);
        check(f.Ln == cast(ulong) mib << ULONGS_PER_MIB_SHIFT,
            "ringMiB maps to Ln ulongs");
        check(f.bufBytes == cast(ulong) mib * 1024 * 1024, "ring bytes");
        f.destroy();
    }
    check(ringMiBFromUlongs(1UL << 18) == 2, "2^18 ulongs is 2 MiB");
    check(ringMiBFromUlongs(1UL << 20) == 8, "2^20 ulongs is 8 MiB");
    printf("testRingSizeUnits OK\n"); fflush(stdout);
}

void testCheckGeometry()
{
    void rejects(FarmCreateError e, string detail, string what)
    {
        check(e.kind == FarmCreateFailure.geometry, what);
        check(e.detail == detail, what);
        check(e.osError == 0, what);
    }
    enum kMsg = "K must be a power of 2 in [2, KMAX]";
    enum ringMsg = "ringMiB must be a power of 2 >= 2 (the ring size is in MiB, not ulongs)";
    enum exmaxMsg = "Exmax exceeds K-1 segments' capacities";
    check(AntFarm.checkGeometry(8, 8, 4, 2, 0, 16, 4096).kind == FarmCreateFailure.none,
        "create defaults accepted");
    check(AntFarm.checkGeometry(8, 8, 4, 2, 0, 16, 4096) == FarmCreateError.init,
        "accepted geometry is FarmCreateError.init");
    rejects(AntFarm.checkGeometry(8, 1, 4, 0, 0, 1, 4096), kMsg, "k 1");
    rejects(AntFarm.checkGeometry(8, 3, 4, 0, 0, 1, 4096), kMsg, "k 3");
    rejects(AntFarm.checkGeometry(8, KMAX * 2, 4, 0, 0, 1, 4096), kMsg, "k > KMAX");
    rejects(AntFarm.checkGeometry(1, 8, 4, 0, 0, 1, 4096), ringMsg, "ringMiB below minimum");
    rejects(AntFarm.checkGeometry(6, 8, 4, 0, 0, 1, 4096), ringMsg, "ringMiB not a power of 2");
    rejects(AntFarm.checkGeometry(MAX_RING_MIB * 2, 8, 4, 0, 0, 1, 4096),
        "ringMiB exceeds MAX_RING_MIB (the ring size is in MiB, not ulongs)", "ringMiB above maximum");
    // "segment capacity too small" is subsumed: MIN_RING_MIB with KMAX
    // segments still gives 2^14 ulongs per segment.
    check(AntFarm.checkGeometry(MIN_RING_MIB, KMAX, 1, 0, 0, 1, 64).kind == FarmCreateFailure.none,
        "smallest ring with KMAX segments");
    rejects(AntFarm.checkGeometry(8, 8, 0, 0, 0, 1, 4096), "expected consumers must be > 0", "consumers 0");
    rejects(AntFarm.checkGeometry(8, 8, 4, 0, 0, 1, 0), "small quota must be > 0", "small quota 0");
    rejects(AntFarm.checkGeometry(8, 8, 4, 0, 0, 0, 4096), "no producer capacity configured", "no producers");
    // 2 MiB, K 8: segCap 32768 ulongs, so Exmax may be at most 7 * 32768.
    check(AntFarm.checkGeometry(2, 8, 1, 0, 0, 1, 7 * 32768).kind == FarmCreateFailure.none,
        "Exmax at (K-1)*segCap");
    rejects(AntFarm.checkGeometry(2, 8, 1, 0, 0, 1, 7 * 32768 + 1), exmaxMsg, "Exmax above (K-1)*segCap");
    rejects(AntFarm.checkGeometry(2, 8, 1, 1, 0, 1, 6 * 32768 + 1), exmaxMsg,
        "auto bulk quota (segCap) counts toward Exmax");
    // "quota exceeds Exmax" needs a wrapped Exmax; overflow is rejected first.
    rejects(AntFarm.checkGeometry(8, 8, 4, 0, 0, uint.max, ulong.max), exmaxMsg, "small product overflow");
    rejects(AntFarm.checkGeometry(8, 8, 4, 2, ulong.max / 2 + 1, 0, 0), exmaxMsg, "bulk product overflow");
    rejects(AntFarm.checkGeometry(8, 8, 4, 1, ulong.max, 1, 1), exmaxMsg, "quota sum overflow");
    rejects(AntFarm.checkGeometry(8, 8, 4, uint.max, 1, 1, 1), exmaxMsg,
        "producer count sum does not wrap to zero");
    // Disabled tiers are normalized before validation.
    check(AntFarm.checkGeometry(8, 8, 4, 0, ulong.max, 1, 4096).kind == FarmCreateFailure.none,
        "disabled bulk quota ignored");
    check(AntFarm.checkGeometry(8, 8, 4, 1, 0, 0, ulong.max).kind == FarmCreateFailure.none,
        "disabled small quota ignored");
    printf("testCheckGeometry OK\n"); fflush(stdout);
}

void testTryCreate()
{
    FarmCreateError error;
    check(AntFarm.tryCreate(8, 3, 4, 0, 0, 1, 4096, DEFAULT_SMALL_TABLE_THRESHOLD, false, &error) is null,
        "tryCreate rejects K 3");
    check(error.kind == FarmCreateFailure.geometry, "tryCreate geometry kind");
    check(error.detail == "K must be a power of 2 in [2, KMAX]", "tryCreate geometry detail");
    check(AntFarm.tryCreate(8, 8, 0) is null, "tryCreate without an error pointer");

    version (Posix)
    {
        import core.stdc.stdlib : getenv;
        import core.sys.posix.stdlib : setenv, unsetenv;
        import std.string : fromStringz, toStringz;
        auto saved = getenv("ANTFARM_HUGE_PAGES");
        immutable savedValue = saved is null ? null : fromStringz(saved).idup;
        setenv("ANTFARM_HUGE_PAGES", "bogus", 1);
        auto bogus = AntFarm.tryCreate(2, 8, 1, 0, 0, 1, 4096,
            DEFAULT_SMALL_TABLE_THRESHOLD, false, &error);
        if (savedValue is null) unsetenv("ANTFARM_HUGE_PAGES");
        else setenv("ANTFARM_HUGE_PAGES", savedValue.toStringz, 1);
        check(bogus is null, "tryCreate rejects ANTFARM_HUGE_PAGES=bogus");
        check(error.kind == FarmCreateFailure.hugePagesSetting, "hugePagesSetting kind");
        check(error.detail == "ANTFARM_HUGE_PAGES must be 0 or 1", "hugePagesSetting detail");
    }

    auto f = AntFarm.tryCreate(2, 8, 1, 0, 0, 1, 4096, DEFAULT_SMALL_TABLE_THRESHOLD, false, &error);
    check(f !is null, "tryCreate success");
    check(error == FarmCreateError.init, "tryCreate success clears the error");
    check(f.Ln == 2UL << ULONGS_PER_MIB_SHIFT && f.maxSmall == 1 && f.quotaSmall == 4096,
        "tryCreate geometry");
    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "tryCreate Farm registers a producer");
    f.unregisterProducer(tok);
    f.destroy();
    printf("testTryCreate OK\n"); fflush(stdout);
}

void testArithmetic()
{
    check(sqcsOf(1) == 1, "sqcsOf(1)");
    check(sqcsOf(2) == 1, "sqcsOf(2)");
    check(sqcsOf(3) == 2, "sqcsOf(3)");
    check(sqcsOf(4) == 2, "sqcsOf(4)");
    check(sqcsOf(5) == 3, "sqcsOf(5)");
    check(sqcsOf(100) == 10, "sqcsOf(100)");
    check(sqcsOf(128) == 12, "sqcsOf(128)");
    check(sentinelOf(0) != sentinelOf(1), "sentinel distinct");
    check(sentinelOf(0) != 0, "sentinel nonzero");
    // shard partition (spec 7a) sums to Tlen
    foreach (tlen; 64 .. 200)
        foreach (sq; 1 .. 13)
        {
            immutable shbase = tlen / sq, shrm = tlen % sq;
            ulong sum = 0;
            foreach (shi; 0 .. sq)
                sum += shi < shrm ? shbase + 1 : shbase;
            check(sum == tlen, "shard partition");
        }
    printf("testArithmetic OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Test 2: single-threaded end to end, Sub0 lifecycle, resubscription
// ---------------------------------------------------------------------

void testSingleThreaded()
{
    auto f = AntFarm.create(2, 8, 2, 1, 8192, 4, 2048);
    scope (exit) f.destroy();

    // Epoch 0: Sub0 pulse present, blocking reclamation of segment 0.
    check(lowHalf(f, 0) == SUB0, "Sub0 at construction");
    foreach (ki; 1 .. 8)
        check(lowHalf(f, cast(uint) ki) == 0, "other Rt zero");

    enum N = 20;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    foreach (i; 0 .. N)
    {
        immutable uint done = 1;
        entries[i] = payloadEntry!testCb(&headers[i], bodies[i * 2 .. i * 2 + 2], i, done);
    }
    immutable expected = N;

    ConsumerView v;
    immutable r = v.subscribe(f);
    check(r == 0, "first subscribe returns epoch 0");
    // First subscriber cleared Sub0 and holds one consumer reference.
    check(lowHalf(f, 0) == 1, "Sub0 cleared, one consumer ref");
    checkNoLeftoverSub(f, "no leftover Sub after first subscribe");

    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "register small producer");
    check(f.write(entries[0 .. 10], tok) == 10, "write first 10");
    check(f.write(entries[10 .. 20], tok) == 10, "write second 10");

    // Consume the two tables; third call finds nothing published.
    check(v.consumeNext(), "consume table 1");
    check(v.consumeNext(), "consume table 2");
    check(!v.consumeNext(), "no third table");

    foreach (i; 0 .. N)
        check(g_calls[i] == 1, "exact call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == expected, "total calls");

    v.unsubscribe();
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf back to 0");
    // Last unsubscriber deposited Sub0 at its current segment: exactly one
    // pulse in the whole farm.
    uint pulses;
    foreach (ki; 0 .. 8)
        if (lowHalf(f, cast(uint) ki) >= SUB0) ++pulses;
    check(pulses == 1, "exactly one Sub0 pulse after unsubscribe");

    // Resubscribe: succeeds, clears Sub0 again, catches up to the write tail.
    ConsumerView v2;
    check(v2.subscribe(f) >= 0, "resubscribe");
    foreach (ki; 0 .. 8)
        check(lowHalf(f, cast(uint) ki) < SUB0, "Sub0 cleared on resubscribe");
    checkNoLeftoverSub(f, "no leftover Sub after resubscribe");
    while (v2.consumeNext()) {}
    // No payload was re-executed: tables were already complete.
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == expected, "no re-execution");
    v2.unsubscribe();
    f.unregisterProducer(tok);

    printf("testSingleThreaded OK\n"); fflush(stdout);
}

// Input-range write overload: a simple input range over the same malloc'd
// entries, not a D slice.
struct PayloadRange
{
    PayloadEntry* p;
    size_t len;
    size_t i;

    @property bool empty() const nothrow @nogc @safe { return i >= len; }
    @property ref PayloadEntry front() nothrow @nogc @system { return p[i]; }
    void popFront() nothrow @nogc @system { ++i; }
    @property PayloadRange save() nothrow @nogc @system { return this; }
}

// Copying this valid input range shares its cursor. It deliberately has no
// save checkpoint and must therefore be rejected by write's forward contract.
final class SharedPayloadInputRange
{
    PayloadEntry* p;
    size_t len;
    size_t i;

    this(PayloadEntry* entries, size_t count) nothrow @nogc
    {
        p = entries;
        len = count;
    }

    @property bool empty() const nothrow @nogc @safe { return i >= len; }
    @property ref PayloadEntry front() nothrow @nogc @system { return p[i]; }
    void popFront() nothrow @nogc @safe { ++i; }
}
static assert(isInputRange!SharedPayloadInputRange);
static assert(!isForwardRange!SharedPayloadInputRange);

struct CountingArgRange
{
    size_t i;
    size_t n;
    size_t* frontCalls;

    @property bool empty() const pure nothrow @nogc @safe { return i >= n; }
    @property size_t front() nothrow @nogc @system
    {
        ++*frontCalls;
        return i;
    }
    void popFront() pure nothrow @nogc @safe { ++i; }
    @property CountingArgRange save() pure nothrow @nogc @safe { return this; }
    @property size_t length() const pure nothrow @nogc @safe { return n - i; }
}

struct CountingBodyRange
{
    ulong* words;
    size_t i;
    size_t n;
    size_t* frontCalls;

    @property bool empty() const pure nothrow @nogc @safe { return i >= n; }
    @property PayloadBody front() nothrow @nogc @system
    {
        ++*frontCalls;
        return words[i .. i + 1];
    }
    void popFront() pure nothrow @nogc @safe { ++i; }
    @property CountingBodyRange save() pure nothrow @nogc @safe { return this; }
    @property size_t length() const pure nothrow @nogc @safe { return n - i; }
}

struct PreparingBodyRange
{
    ulong* words;
    size_t i;
    size_t n;
    size_t* frontCalls;
    size_t* prepareCalls;
    size_t* prepared;
    bool* permit;

    @property bool empty() const pure nothrow @nogc @safe { return i >= n; }
    @property PayloadBody front() nothrow @nogc @system
    {
        ++*frontCalls;
        return words[i .. i + 1];
    }
    void popFront() pure nothrow @nogc @safe { ++i; }
    @property PreparingBodyRange save() pure nothrow @nogc @safe
    {
        return this;
    }
    @property size_t length() const pure nothrow @nogc @safe { return n - i; }
    bool prepareTableFrontN(size_t count) nothrow @nogc @system
    {
        if (count > length) return false;
        ++*prepareCalls;
        if (*permit) *prepared += count;
        return *permit;
    }
}

void testInputRangeWrite()
{
    auto f = AntFarm.create(2, 8, 2, 1, 8192, 4, 2048);
    scope (exit) f.destroy();

    enum N = 20;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable uint done = 1;
        entries[i] = payloadEntry!testCb(&headers[i], bodies[i * 2 .. i * 2 + 2], i, done);
        expected += done;
    }

    ConsumerView v;
    check(v.subscribe(f) == 0, "subscribe before range write");
    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "register small producer");

    auto singlePass = new SharedPayloadInputRange(entries, N);
    static assert(!__traits(compiles, f.write(singlePass, tok)),
        "write must reject input ranges that cannot checkpoint");

    PayloadRange range = PayloadRange(entries, N);
    check(f.write(range, tok) == N, "write input range");
    check(v.consumeNext(), "consume range-written table");
    check(!v.consumeNext(), "no extra range table");

    foreach (i; 0 .. N)
        check(g_calls[i] == 1, "range exact call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == expected, "range total");

    v.unsubscribe();
    f.unregisterProducer(tok);
    printf("testInputRangeWrite OK\n"); fflush(stdout);
}

void testSeparateRanges()
{
    auto f = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope (exit) f.destroy();

    enum N = 40;
    allocCalls(N);
    scope (exit) freeCalls();
    PayloadHeader[N] headers;
    ulong[N] words;
    PayloadBody[N] bodies;
    foreach (i; 0 .. N)
    {
        initPayloadHeader!testCb1(&headers[i]);
        words[i] = i;
        bodies[i] = words[i .. i + 1];
    }

    ConsumerView v;
    check(v.subscribe(f) == 0, "subscribe before separate-range write");
    auto tok = f.registerProducer(Tier.small);
    check(f.write(headers[], bodies[], tok, DEFAULT_AVG_COST) == N,
          "write separate header/body ranges");

    check(v.consumeNext(), "consume separate-range table");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == N,
          "separate-range table completed");

    v.unsubscribe();
    f.unregisterProducer(tok);
    printf("testSeparateRanges OK\n"); fflush(stdout);
}

void readImmutableInt(immutable(int)* p) nothrow @nogc @system {}
void takeUbyte(ubyte b) nothrow @nogc @system {}
void takeUlong(ulong v) nothrow @nogc @system {}

// Packed arguments convert implicitly or not at all: no silent narrowing and
// no mutable pointer laundered into an immutable parameter.
static assert(!argsConvert!(readImmutableInt, int*));
static assert(argsConvert!(readImmutableInt, immutable(int)*));
static assert(!argsConvert!(takeUbyte, int));
static assert(argsConvert!(takeUbyte, ubyte));
static assert(argsConvert!(takeUlong, uint));
static assert(!__traits(compiles, {
    PayloadHeader h; ulong[1] buf; int x;
    cast(void) payloadEntry!readImmutableInt(&h, buf[], &x);
}));
static assert(!__traits(compiles, {
    PayloadHeader h; ulong[1] buf;
    cast(void) payloadEntry!takeUbyte(&h, buf[], 1000);
}));

void testPayloadRange()
{
    auto f = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope (exit) f.destroy();

    enum N = 40;
    allocCalls(2 * N);
    scope (exit) freeCalls();

    // Tuple form: fields map onto (idx, done) of testCb.
    struct PairRange
    {
        size_t i, n;
        @property bool empty() const pure nothrow @nogc @safe
        {
            return i >= n;
        }
        @property Tuple!(size_t, ulong) front() pure nothrow @nogc @safe
        {
            return tuple(i + N, 1UL);
        }
        void popFront() pure nothrow @nogc @safe { ++i; }
        @property PairRange save() pure nothrow @nogc @safe { return this; }
    }

    ConsumerView v;
    check(v.subscribe(f) == 0, "subscribe before payloadRange write");
    auto tok = f.registerProducer(Tier.small);

    // Single-parameter form: element is the packed parameter itself.
    // ElementType of std.range.iota plugs straight into the single-param path.
    auto singles = payloadRange!testCb1(iota(0, N));
    check(f.write(singles, tok) == N, "payloadRange single-param write");

    // Tuple form: any arity, fields in declaration order.
    auto pairs = payloadRange!testCb(PairRange(0, N));
    check(f.write(pairs, tok) == N, "payloadRange tuple write");

    while (v.consumeNext()) {}
    foreach (i; 0 .. 2 * N)
        check(g_calls[i] == 1, "payloadRange exact per-payload call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == 2 * N,
          "payloadRange total");

    v.unsubscribe();
    f.unregisterProducer(tok);
    printf("testPayloadRange OK\n"); fflush(stdout);
}

void testUniformRanges()
{
    // N and P exceed one 4096-word small-producer table even in the
    // index-free uniform ST layout (1 word per one-word payload, SPEC 2g).
    enum N = 5000;
    enum M = 24;
    enum F = 24;
    enum K = 8;
    enum P = 5000;
    allocCalls(N + M + F + K + P);
    scope (exit) freeCalls();

    auto f = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope (exit) f.destroy();
    ConsumerView v;
    check(v.subscribe(f) == 0, "subscribe before uniform writes");
    auto tok = f.registerProducer(Tier.small);

    size_t frontCalls;
    auto jobs = payloadRange!testCb1(CountingArgRange(0, N, &frontCalls));
    static assert(isForwardRange!(typeof(jobs)));
    static assert(hasLength!(typeof(jobs)));

    size_t written;
    size_t tables;
    while (!jobs.empty)
    {
        immutable n = f.write(jobs, tok);
        if (n == 0)
        {
            while (v.consumeNext()) {}
            continue;
        }
        ++tables;
        written += n;
        check(jobs.popFrontN(n) == n, "uniform popFrontN");
        while (v.consumeNext()) {}
    }
    check(written == N && tables > 1, "uniform partial-write retry");
    check(frontCalls == N, "uniform sizing never evaluates front");

    PayloadHeader mutableHeader;
    initPayloadHeader!testCb1(&mutableHeader);
    const PayloadHeader commonHeader = mutableHeader;
    ulong[M + F] words;
    PayloadBody[M] bodies;
    foreach (i; 0 .. M)
    {
        words[i] = N + i;
        bodies[i] = words[i .. i + 1];
    }
    auto broadcast = broadcastPayloads(commonHeader, bodies[]);
    static assert(isForwardRange!(typeof(broadcast)));
    static assert(hasLength!(typeof(broadcast)));
    auto constPair = pairPayloads((&commonHeader)[0 .. 1], bodies[0 .. 1]);
    static assert(isForwardRange!(typeof(constPair)));
    check(constPair.front.header.call is mutableHeader.call,
        "const header range pairing");
    auto runtimeConfigured = payloadRange!testCb1(iota(0, 0), 1u);
    static assert(isForwardRange!(typeof(runtimeConfigured)));
    check(runtimeConfigured.empty, "runtime payloadRange configuration");
    check(f.write(commonHeader, bodies[], tok) == M,
        "const common-header broadcast write");
    while (v.consumeNext()) {}

    foreach (i; 0 .. F)
        words[M + i] = N + M + i;
    size_t bodyFrontCalls;
    auto fixedBodies = CountingBodyRange(
        words.ptr + M, 0, F, &bodyFrontCalls);
    check(f.write(commonHeader, fixedBodies, 1, tok) == F,
        "constant call-and-length write");
    check(bodyFrontCalls == F,
        "fixed-length sizing never evaluates body front");
    while (v.consumeNext()) {}

    // A transactional source is prepared only for the exact prefix which
    // fits the physical table, after sizing and before ring reservation.
    ulong[P] preparingWords;
    foreach (i; 0 .. P)
        preparingWords[i] = N + M + F + K + i;
    size_t preparingFrontCalls;
    size_t prepareCalls;
    size_t prepared;
    bool permitPreparation = true;
    auto preparing = PreparingBodyRange(preparingWords.ptr, 0, P,
        &preparingFrontCalls, &prepareCalls, &prepared,
        &permitPreparation);
    size_t preparingTables;
    while (!preparing.empty)
    {
        immutable count = cast(size_t) f.write(commonHeader, preparing,
            1, tok);
        check(count != 0, "transactional range write progress");
        check(preparing.popFrontN(count) == count,
            "transactional range popFrontN");
        ++preparingTables;
        while (v.consumeNext()) {}
    }
    check(preparingTables > 1 && prepareCalls == preparingTables,
        "transactional range prepared once per physical table");
    check(prepared == P && preparingFrontCalls == P,
        "transactional range prepared only emitted payloads");

    ulong rejectedWord;
    size_t rejectedFrontCalls;
    size_t rejectedPrepareCalls;
    size_t rejectedPrepared;
    bool rejectPreparation;
    auto rejected = PreparingBodyRange(&rejectedWord, 0, 1,
        &rejectedFrontCalls, &rejectedPrepareCalls, &rejectedPrepared,
        &rejectPreparation);
    immutable tailBeforeReject = atomicLoad!(MemoryOrder.acq)(f.Wt);
    check(f.write(commonHeader, rejected, 1, tok) == 0,
        "transactional range may reject before publication");
    check(atomicLoad!(MemoryOrder.acq)(f.Wt) == tailBeforeReject
            && rejectedPrepareCalls == 1 && rejectedPrepared == 0
            && rejectedFrontCalls == 0,
        "rejected transactional range leaves Farm untouched");

    Tuple!(size_t, ulong)[K] tupleArgs;
    foreach (i; 0 .. K)
        tupleArgs[i] = tuple(cast(size_t)(N + M + F + i), 2UL);
    auto tupleJobs = payloadRange!testCb(tupleArgs[]);
    // The quota left over from the uniform writes may admit only a prefix.
    size_t tupleWritten;
    while (tupleWritten < K)
    {
        immutable got = cast(size_t) f.write(tupleJobs, tok);
        check(got != 0, "uniform tuple index write");
        check(tupleJobs.popFrontN(got) == got, "uniform tuple popFrontN");
        tupleWritten += got;
        while (v.consumeNext()) {}
    }

    foreach (i; 0 .. N + M + F)
        check(g_calls[i] == 1, "uniform exact call count");
    foreach (i; N + M + F .. N + M + F + K)
        check(g_calls[i] == 1, "uniform tuple exact call count");
    foreach (i; N + M + F + K .. N + M + F + K + P)
        check(g_calls[i] == 1, "transactional range exact call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls)
            == N + M + F + K + P,
        "uniform total");

    v.unsubscribe();
    f.unregisterProducer(tok);
    printf("testUniformRanges OK\n"); fflush(stdout);
}

 // ---------------------------------------------------------------------
 // Test 3: concurrent producers and consumers, exact call accounting
 // ---------------------------------------------------------------------

// ---------------------------------------------------------------------
// Test 3: concurrent producers and consumers, exact call accounting
// ---------------------------------------------------------------------

struct ProdCtx
{
    AntFarm* f;
    PayloadEntry* entries;
    size_t count;
    Tier tier;
}

struct ConsCtx
{
    AntFarm* f;
    long expected;
    MonoTime deadline;
}

void producerMain(ProdCtx* c)
{
    auto tok = c.f.registerProducer(c.tier);
    if (!tok.valid) fatal("producer overregistration");
    size_t off;
    while (off < c.count)
    {
        immutable n = c.f.write(c.entries[off .. c.count], tok);
        off += n;
        if (n == 0)
            Thread.yield();
    }
    c.f.unregisterProducer(tok);
}

void consumerMain(ConsCtx* c)
{
    ConsumerView v;
    if (v.subscribe(c.f) < 0) fatal("subscribe failed");
    for (;;)
    {
        if (!v.consumeNext())
        {
            if (atomicLoad!(MemoryOrder.acq)(g_totalCalls) >= c.expected)
                break;
            if (MonoTime.currTime > c.deadline)
            {
                fprintf(stderr, "consumer watchdog timeout\n");
                import core.stdc.stdlib : abort;
                abort();
            }
            Thread.yield();
        }
    }
    v.unsubscribe();
}

void testConcurrent()
{
    auto f = AntFarm.create(2, 8, 4, 1, 16384, 8, 4096);
    scope (exit) f.destroy();

    enum N = 6000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable uint done = 1;
        entries[i] = payloadEntry!testCb(&headers[i], bodies[i * 2 .. i * 2 + 2], i, done);
        expected += done;
    }

    auto p1 = new Thread({ producerMain(&p1ctx); });
    auto p2 = new Thread({ producerMain(&p2ctx); });
    p1ctx = ProdCtx(f, entries, N / 2, Tier.bulk);
    p2ctx = ProdCtx(f, entries + N / 2, N - N / 2, Tier.small);

    Thread[3] consumers;
    ConsCtx[3] cctx;
    foreach (i; 0 .. 3)
    {
        cctx[i] = ConsCtx(f, expected, MonoTime.currTime + 60.seconds);
        auto cc = &cctx[i];
        consumers[i] = new Thread({ consumerMain(cc); });
    }
    foreach (t; consumers) t.start();
    p1.start();
    p2.start();
    p1.join();
    p2.join();
    foreach (t; consumers) t.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == 1, "concurrent exact call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == expected, "concurrent total");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained");
    uint pulses;
    foreach (ki; 0 .. 8)
        if (lowHalf(f, cast(uint) ki) >= SUB0) ++pulses;
    check(pulses == 1, "Sub0 pulse after all consumers left");

    printf("testConcurrent OK\n"); fflush(stdout);
}

__gshared ProdCtx p1ctx, p2ctx;

// ---------------------------------------------------------------------
// Test 4: wraparound over several laps; consumer starts late (producer
// stalls on the Sub0 pulse until the consumer arrives and keeps up).
// ---------------------------------------------------------------------

void testWraparound()
{
    auto f = AntFarm.create(2, 8, 1, 1, 2048, 2, 1024);
    scope (exit) f.destroy();

    // Enough mixed-width ST entries to lap the 2^18-word ring repeatedly.
    enum N = 300000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 3 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable uint done = 1;
        immutable plen = 1 + i % 3;
        auto body = bodies[i * 3 .. i * 3 + plen];
        if (plen == 1)
            entries[i] = payloadEntry!testCb1(&headers[i], body, i);
        else
        {
            entries[i] = payloadEntry!testCb(&headers[i], body, i, done);
            entries[i].body = body;
        }
        expected += done;
    }

    p1ctx = ProdCtx(f, entries, N, Tier.small);
    auto p = new Thread({ producerMain(&p1ctx); });
    auto cctx = ConsCtx(f, expected, MonoTime.currTime + 60.seconds);
    auto c = new Thread({ consumerMain(&cctx); });

    p.start();
    Thread.sleep(50.msecs); // let the producer run ahead and stall on Sub0
    check(atomicLoad!(MemoryOrder.raw)(f.Wt) > 0, "producer ran ahead");
    check(atomicLoad!(MemoryOrder.raw)(f.Wt) < f.Ln, "producer blocked before lapping Sub0");
    c.start();
    p.join();
    c.join();

    check(atomicLoad!(MemoryOrder.raw)(f.Wt) > 2 * f.Ln, "wrapped multiple laps");
    foreach (i; 0 .. N)
        check(g_calls[i] == 1, "wraparound exact call count");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained");

    printf("testWraparound OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Test 5: subscription cap (spec 4c, 6b) and immediate reuse of slots
// ---------------------------------------------------------------------

void testSubscriptionCap()
{
    auto f = AntFarm.create(2, 8, 4, 1, 2048, 2, 1024);
    scope (exit) f.destroy();

    auto views = cast(ConsumerView*) malloc(MAX_CONSUMERS_LIMIT * ConsumerView.sizeof);
    scope (exit) free(views);
    foreach (i; 0 .. MAX_CONSUMERS_LIMIT)
    {
        views[i] = ConsumerView.init;
        check(views[i].subscribe(f) >= 0, "subscribe within cap");
    }
    ConsumerView extra;
    check(extra.subscribe(f) == SUBSCRIBE_FULL, "oversubscription reports full");
    check(views[0].subscribe(f) == SUBSCRIBE_INVALID,
        "resubscribing a live view reports invalid");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == MAX_CONSUMERS_LIMIT, "Cf at cap");
    foreach (i; 0 .. MAX_CONSUMERS_LIMIT)
        views[i].unsubscribe();
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained");
    uint pulses;
    foreach (ki; 0 .. 8)
        if (lowHalf(f, cast(uint) ki) >= SUB0) ++pulses;
    check(pulses == 1, "single Sub0 pulse after mass unsubscribe");
    // Slot reuse works after leaving.
    check(views[0].subscribe(f) >= 0, "resubscribe after cap");
    views[0].unsubscribe();
    printf("testSubscriptionCap OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Test 6: subscribe/unsubscribe churn while production is ongoing.
// Steady consumers drain the stream; churners keep joining and leaving
// (exercising the first/last subscriber and Sub0 handoff paths).
// ---------------------------------------------------------------------

__gshared shared(int) g_churnStop;

struct ChurnCtx { AntFarm* f; MonoTime deadline; }

void churnerMain(ChurnCtx* c)
{
    while (atomicLoad!(MemoryOrder.raw)(g_churnStop) == 0)
    {
        ConsumerView v;
        if (v.subscribe(c.f) >= 0)
        {
            foreach (_; 0 .. 50)
            {
                v.consumeNext();
                if (MonoTime.currTime > c.deadline)
                {
                    fprintf(stderr, "churner watchdog timeout\n");
                    import core.stdc.stdlib : abort;
                    abort();
                }
            }
            v.unsubscribe();
        }
        Thread.yield();
    }
}

void testChurn(size_t nsteady)
{
    auto f = AntFarm.create(2, 8, 6, 1, 4096, 4, 2048);
    scope (exit) f.destroy();

    enum N = 4000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable uint done = 1;
        entries[i] = payloadEntry!testCb(&headers[i], bodies[i * 2 .. i * 2 + 2], i, done);
        expected += done;
    }

    atomicStore!(MemoryOrder.raw)(g_churnStop, 0);
    p1ctx = ProdCtx(f, entries, N, Tier.bulk);
    auto p = new Thread({ producerMain(&p1ctx); });

    Thread[] steady = new Thread[nsteady];
    ConsCtx[] sctx = new ConsCtx[nsteady];
    auto deadline = MonoTime.currTime + 90.seconds;
    // With the tertiary sweep, even a single steady consumer drains every
    // table; with more, shard coverage is a performance property, not a
    // completion requirement.
    foreach (i; 0 .. nsteady)
    {
        sctx[i] = ConsCtx(f, expected, deadline);
        auto cc = &sctx[i];
        steady[i] = new Thread({ consumerMain(cc); });
    }
    Thread[3] churners;
    ChurnCtx[3] hctx;
    foreach (i; 0 .. 3)
    {
        hctx[i] = ChurnCtx(f, deadline);
        auto hc = &hctx[i];
        churners[i] = new Thread({ churnerMain(hc); });
    }

    foreach (t; steady) t.start();
    foreach (t; churners) t.start();
    p.start();
    p.join();
    foreach (t; steady) t.join();   // steady consumers leave once all work is done
    atomicStore!(MemoryOrder.raw)(g_churnStop, 1);
    foreach (t; churners) t.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == 1, "churn exact call count");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained after churn");
    uint pulses;
    foreach (ki; 0 .. 8)
        if (lowHalf(f, cast(uint) ki) >= SUB0) ++pulses;
    check(pulses == 1, "single Sub0 pulse after churn");

    printf("testChurn(%zu) OK\n", nsteady); fflush(stdout);
}

// ---------------------------------------------------------------------
// Test 7: lossless backlog. Producer outruns consumers; A consumes part
// and unsubscribes (planting Sub0 at its earliest unconfirmed segment);
// B subscribes later and drains the rest. Every payload executes exactly
// its Done times.
// ---------------------------------------------------------------------

void testBacklog()
{
    auto f = AntFarm.create(2, 8, 2, 1, 2048, 2, 1024);
    scope (exit) f.destroy();

    enum N = 3000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 3 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable uint done = 1;
        immutable plen = 1 + i % 3;
        auto body = bodies[i * 3 .. i * 3 + plen];
        if (plen == 1)
            entries[i] = payloadEntry!testCb1(&headers[i], body, i);
        else
        {
            entries[i] = payloadEntry!testCb(&headers[i], body, i, done);
            entries[i].body = body;
        }
        expected += done;
    }

    p1ctx = ProdCtx(f, entries, N, Tier.small);
    auto p = new Thread({ producerMain(&p1ctx); });
    p.start();
    Thread.sleep(30.msecs); // producer fills the buffer and stalls on Sub0

    ConsumerView a;
    check(a.subscribe(f) >= 0, "A subscribes");
    foreach (_; 0 .. 20)
        a.consumeNext();
    a.unsubscribe();
    // Last-releaser Sub0: every abandoned unconfirmed segment may pulse.
    uint pulses;
    foreach (ki; 0 .. 8)
        if (lowHalf(f, cast(uint) ki) >= SUB0) ++pulses;
    check(pulses >= 1, "Sub0 planted on abandoned unconfirmed segments");

    ConsumerView b;
    check(b.subscribe(f) >= 0, "B subscribes");
    auto deadline = MonoTime.currTime + 60.seconds;
    while (atomicLoad!(MemoryOrder.acq)(g_totalCalls) < expected)
    {
        b.consumeNext();
        if (MonoTime.currTime > deadline)
        {
            fprintf(stderr, "backlog watchdog timeout\n");
            import core.stdc.stdlib : abort;
            abort();
        }
    }
    b.unsubscribe();
    p.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == 1, "backlog exact call count");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained");

    printf("testBacklog OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Test 8: tables spanning segment boundaries, including one that fully
// spans a segment. Verifies exact execution and that no references leak.
// ---------------------------------------------------------------------

void testSpannedTables()
{
    // segCap = 2^18/8 = 32768; bulk quota 110000 allows a 100k table to
    // span ~3 segments and a 34k table to span a boundary.
    auto f = AntFarm.create(2, 8, 2, 1, 110000, 2, 1024);
    scope (exit) f.destroy();

    enum N = 64;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    scope (exit) free(headers);
    // One huge body (~100000 ulongs -> its table fully spans segments),
    // one large body (~34000 -> spans a boundary), the rest small.
    auto huge = cast(ulong*) malloc(100000 * ulong.sizeof);
    auto large = cast(ulong*) malloc(34000 * ulong.sizeof);
    auto small = cast(ulong*) malloc((N - 2) * 2 * ulong.sizeof);
    scope (exit) { free(huge); free(large); free(small); }
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) free(entries);

    long expected;
    foreach (i; 0 .. N)
    {
        initPayloadHeader!testCb(&headers[i]);
        ++expected;
    }
    foreach (i; 0 .. 100000) huge[i] = (i == 0) ? 0 : 1;    // payload 0: body[0] = index
    foreach (i; 0 .. 34000) large[i] = (i == 0) ? 1 : 1;    // payload 1
    huge[0] = 0; large[0] = 1;
    entries[0].header = &headers[0]; entries[0].body = huge[0 .. 100000];
    entries[1].header = &headers[1]; entries[1].body = large[0 .. 34000];
    foreach (i; 2 .. N)
    {
        small[(i - 2) * 2] = i;
        small[(i - 2) * 2 + 1] = 1;
        entries[i].header = &headers[i]; entries[i].body = small[(i - 2) * 2 .. (i - 2) * 2 + 2];
    }
    // Order: some small tables, the huge one, the large one, more small.
    // (entries already in that order: 0 huge, 1 large, then small.)

    auto cctx = ConsCtx(f, expected, MonoTime.currTime + 60.seconds);
    auto c = new Thread({ consumerMain(&cctx); });
    c.start();
    auto tok = f.registerProducer(Tier.bulk);
    check(tok.valid, "register bulk");
    size_t off;
    while (off < N)
    {
        immutable n = f.write(entries[off .. N], tok);
        off += n;
        if (n == 0) { Thread.yield(); }
    }
    f.unregisterProducer(tok);
    c.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == 1, "spanned exact call count");
    foreach (ki; 0 .. 8)
        check((atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]) & COUNTMASK) == 0, "no leaked refs");
    printf("testSpannedTables OK\n"); fflush(stdout);
}

// Writes at most 12 entries per call, so every table is a small table.
void smallProducerMain(ProdCtx* c)
{
    auto tok = c.f.registerProducer(c.tier);
    if (!tok.valid) fatal("producer overregistration");
    size_t off;
    while (off < c.count)
    {
        immutable batch = c.count - off < 12 ? c.count - off : 12;
        immutable n = c.f.write(c.entries[off .. off + batch], tok);
        off += n;
        if (n == 0)
            Thread.yield();
    }
    c.f.unregisterProducer(tok);
}

// Small tables only: starvation of the single shard 0 under churn, covered
// by the carried sweeper role (and the idle re-walk backstop).
void testSmallTableChurn()
{
    auto f = AntFarm.create(2, 8, 6, 1, 4096, 4, 2048);
    scope (exit) f.destroy();

    enum N = 4000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable uint done = 1;
        entries[i] = payloadEntry!testCb(&headers[i], bodies[i * 2 .. i * 2 + 2], i, done);
        expected += done;
    }

    atomicStore!(MemoryOrder.raw)(g_churnStop, 0);
    p1ctx = ProdCtx(f, entries, N, Tier.bulk);
    auto p = new Thread({ smallProducerMain(&p1ctx); });

    Thread[2] steady;
    ConsCtx[2] sctx;
    auto deadline = MonoTime.currTime + 90.seconds;
    foreach (i; 0 .. 2)
    {
        sctx[i] = ConsCtx(f, expected, deadline);
        auto cc = &sctx[i];
        steady[i] = new Thread({ consumerMain(cc); });
    }
    Thread[2] churners;
    ChurnCtx[2] hctx;
    foreach (i; 0 .. 2)
    {
        hctx[i] = ChurnCtx(f, deadline);
        auto hc = &hctx[i];
        churners[i] = new Thread({ churnerMain(hc); });
    }

    foreach (t; steady) t.start();
    foreach (t; churners) t.start();
    p.start();
    p.join();
    foreach (t; steady) t.join();
    atomicStore!(MemoryOrder.raw)(g_churnStop, 1);
    foreach (t; churners) t.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == 1, "small-table churn exact call count");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained after small-table churn");
    foreach (ki; 0 .. 8)
        check((atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]) & COUNTMASK) == 0, "no leaked refs");

    printf("testSmallTableChurn OK\n"); fflush(stdout);
}

// H1: two small producers after the C1 idle-pin migrate. Previously a stale
// position pin on a spanning table's start starved refreshQuota.
void testMultiSmallProducers()
{
    auto f = AntFarm.create(2, 8, 2, 1, 8192, 2, 4096);
    scope (exit) f.destroy();

    enum N = 2000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable uint done = 1;
        entries[i] = payloadEntry!testCb(&headers[i], bodies[i * 2 .. i * 2 + 2], i, done);
        expected += done;
    }

    p1ctx = ProdCtx(f, entries, N / 2, Tier.small);
    p2ctx = ProdCtx(f, entries + N / 2, N - N / 2, Tier.small);
    auto p1 = new Thread({ producerMain(&p1ctx); });
    auto p2 = new Thread({ producerMain(&p2ctx); });
    ConsCtx[2] cctx;
    Thread[2] consumers;
    foreach (i; 0 .. 2)
    {
        cctx[i] = ConsCtx(f, expected, MonoTime.currTime + 60.seconds);
        auto cc = &cctx[i];
        consumers[i] = new Thread({ consumerMain(cc); });
    }
    foreach (t; consumers) t.start();
    p1.start();
    p2.start();
    p1.join();
    p2.join();
    foreach (t; consumers) t.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == 1, "multi-small exact call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == expected, "multi-small total");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained");
    printf("testMultiSmallProducers OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Experimental actor payload: non-GC storage, serial mutation, retirement
// ---------------------------------------------------------------------

class ActorConsumerJob
{
    AntFarm* farm;
    shared int* stop;
    MonoTime deadline;

    this(AntFarm* actorFarm, shared int* stopFlag, MonoTime limit)
    {
        farm = actorFarm;
        stop = stopFlag;
        deadline = limit;
    }

    void run()
    {
        ConsumerView view;
        if (view.subscribe(farm) < 0)
            fatal("actor consumer subscribe failed");
        while (atomicLoad!(MemoryOrder.acq)(*stop) == 0)
        {
            if (!view.consumeNext())
            {
                if (MonoTime.currTime >= deadline)
                    fatal("actor consumer timeout");
                Thread.yield();
            }
        }
        while (view.consumeNext()) {}
        view.unsubscribe();
    }
}

class ActorInboxProducerJob
{
    ActorHandle!ActorInboxState handle;
    ActorInboxMessage* messages;
    size_t acceptedCount;
    size_t closedCount;
    shared long* ready;
    shared int* releaseClosedPhase;

    this(ActorHandle!ActorInboxState actorHandle,
            ActorInboxMessage* first, size_t accepted,
            size_t closed, shared long* readyCount,
            shared int* releaseFlag)
    {
        handle = actorHandle;
        messages = first;
        acceptedCount = accepted;
        closedCount = closed;
        ready = readyCount;
        releaseClosedPhase = releaseFlag;
    }

    void run()
    {
        foreach (i; 0 .. acceptedCount)
        {
            immutable result = handle.send(&messages[i].node);
            if (result == ActorSendResult.queued
                    || result == ActorSendResult.coalesced)
                atomicFetchAdd!(MemoryOrder.rel)(g_inboxAccepted, 1L);
            else
                atomicFetchAdd!(MemoryOrder.rel)(g_inboxUnexpected, 1L);
        }

        atomicFetchAdd!(MemoryOrder.rel)(*ready, 1L);
        while (atomicLoad!(MemoryOrder.acq)(*releaseClosedPhase) == 0)
            Thread.yield();

        foreach (i; acceptedCount .. acceptedCount + closedCount)
        {
            immutable result = handle.send(&messages[i].node);
            if (result == ActorSendResult.closed)
                atomicFetchAdd!(MemoryOrder.rel)(g_inboxClosed, 1L);
            else
                atomicFetchAdd!(MemoryOrder.rel)(g_inboxUnexpected, 1L);
        }
    }
}

void testActorWave()
{
    enum actorCount = 64;
    enum tableActors = 7;
    enum consumerCount = 4;
    auto farm = AntFarm.create(2, 8, consumerCount,
        0, 0, 1, 4096);
    scope (exit) farm.destroy();

    ActorAllocCounts counts;
    auto allocator = ActorAllocator(&counts,
        &actorTestAllocate, &actorTestDeallocate);
    auto actors = ActorRuntime.create(farm, actorCount, allocator);
    check(actors !is null, "wave actor runtime allocation");

    ulong[actorCount] values;
    ulong[actorCount] observed;
    ActorWaveShared sharedState = ActorWaveShared(
        values.ptr, observed.ptr, actorCount);
    ActorOwner!ActorWaveState[actorCount] owners;
    ActorHandle!ActorWaveState[actorCount] handles;
    foreach (i; 0 .. actorCount)
    {
        auto initial = ActorWaveState(&sharedState, i, 0, 0);
        owners[i] = actors.createActor!(ActorWaveState, actorWaveDormant)(
            initial);
        check(owners[i].valid, "wave actor creation");
        handles[i] = owners[i].handle;
    }
    atomicStore!(MemoryOrder.raw)(g_waveAutonomousCalls, 0L);

    shared int stop;
    auto deadline = MonoTime.currTime + 30.seconds;
    ActorConsumerJob[consumerCount] consumerJobs;
    Thread[consumerCount] consumers;
    foreach (i; 0 .. consumerCount)
    {
        consumerJobs[i] = new ActorConsumerJob(farm, &stop, deadline);
        consumers[i] = new Thread(&consumerJobs[i].run);
        consumers[i].start();
    }

    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "wave producer registration");
    ActorWave wave;
    shared long waveCompletions;

    // Membership outlives an individual table callback. Even after this
    // actor has returned to idle, it cannot be admitted a second time while
    // its aggregate wave remains open.
    wave.begin(farm);
    check(wave.publish!actorWaveProduce(handles[0 .. 1], token, avgCostForChunk(8)) == 1,
        "single actor wave table publication");
    while (wave.handle.progress != 1)
    {
        if (MonoTime.currTime >= deadline)
            fatal("open actor wave table completion timeout");
        Thread.yield();
    }
    check(handles[0].wake() == ActorWakeResult.waveOwned,
        "autonomous wake rejected while actor belongs to open wave");
    ActorInboxNode waveSendProbe;
    waveSendProbe.initialize();
    check(handles[0].send(&waveSendProbe) == ActorSendResult.waveOwned
            && waveSendProbe.available,
        "inbox send rejected without taking node from open wave");
    check(owners[0].requestRetire() == ActorRetireResult.waveOwned,
        "retirement rejected while actor belongs to open wave");
    check(wave.publish!actorWaveProduce(handles[0 .. 1], token, avgCostForChunk(8)) == 0
            && wave.handle.failed,
        "completed table retains aggregate wave membership");
    auto overlapRejected = wave.seal();
    while (!overlapRejected.finished) Thread.yield();
    check(overlapRejected.failed && overlapRejected.length == 1,
        "overlap rejection preserves prior table completion");

    wave.begin(farm, ActorWaveCompletionHook(
        cast(void*) &waveCompletions, &actorWaveCompletion));
    size_t offset;
    ulong firstTableCount;
    while (offset < actorCount)
    {
        immutable candidatePast = offset + tableActors < actorCount
            ? offset + tableActors : actorCount;
        immutable written = wave.publish!actorWaveProduce(
            handles[offset .. candidatePast], token, avgCostForChunk(8));
        if (written == 0)
        {
            check(!wave.handle.failed, "wave publication backpressure only");
            if (MonoTime.currTime >= deadline)
                fatal("actor wave publication timeout");
            Thread.yield();
            continue;
        }
        offset += written;
        ++firstTableCount;
        check(!wave.handle.finished,
            "open wave cannot finish at transient Wprogress equality");
    }
    check(firstTableCount > 1, "actor wave spans multiple Farm tables");
    auto first = wave.seal();
    while (!first.finished)
    {
        if (MonoTime.currTime >= deadline)
            fatal("first actor wave completion timeout");
        Thread.yield();
    }
    check(!first.failed, "first actor wave completes cleanly");
    while (atomicLoad!(MemoryOrder.acq)(waveCompletions) != 1)
    {
        if (MonoTime.currTime >= deadline)
            fatal("actor wave completion hook timeout");
        Thread.yield();
    }
    check(first.progress == first.length
            && first.length == firstTableCount,
        "wave aggregates exact physical table count");
    foreach (i; 0 .. actorCount)
        check(values[i] == cast(ulong) i * 3UL + 11UL,
            "first wave publishes actor writes");

    // Reusing the stable descriptor creates a new observation generation.
    // The second phase is published only after acquiring first.finished and
    // therefore may read every entity result produced by the first phase.
    wave.begin(farm);
    check(!first.valid, "reused actor wave invalidates old handle");
    offset = 0;
    while (offset < actorCount)
    {
        immutable candidatePast = offset + tableActors < actorCount
            ? offset + tableActors : actorCount;
        immutable written = wave.publish!actorWaveObserve(
            handles[offset .. candidatePast], token, avgCostForChunk(8));
        if (written == 0)
        {
            check(!wave.handle.failed,
                "dependent wave publication backpressure only");
            if (MonoTime.currTime >= deadline)
                fatal("dependent actor wave publication timeout");
            Thread.yield();
            continue;
        }
        offset += written;
    }
    auto second = wave.seal();
    while (!second.finished)
    {
        if (MonoTime.currTime >= deadline)
            fatal("dependent actor wave completion timeout");
        Thread.yield();
    }
    check(!second.failed, "dependent actor wave completes cleanly");
    foreach (i; 0 .. actorCount)
    {
        immutable neighbor = (i + 1) % actorCount;
        check(observed[i] == cast(ulong) neighbor * 3UL + 11UL,
            "dependent wave observes predecessor state");
    }

    // Reservation makes duplicate membership a structural failure before a
    // table becomes visible, and cancellation restores the actor to idle.
    wave.begin(farm);
    ActorHandle!ActorWaveState[2] duplicate;
    duplicate[0] = handles[0];
    duplicate[1] = handles[0];
    check(wave.publish!actorWaveProduce(duplicate[], token, avgCostForChunk(8)) == 0,
        "duplicate actor rejected from wave");
    auto rejected = wave.seal();
    check(rejected.finished && rejected.failed
            && rejected.length == 0 && rejected.progress == 0,
        "rejected empty wave finishes with failure");

    atomicStore!(MemoryOrder.rel)(stop, 1);
    foreach (thread; consumers) thread.join();
    farm.unregisterProducer(token);
    check(atomicLoad!(MemoryOrder.acq)(g_waveAutonomousCalls) == 0,
        "wave dispatch does not enter autonomous actor handler");

    foreach (ref owner; owners)
    {
        check(owner.requestRetire() == ActorRetireResult.requested
                && owner.retired,
            "wave actor retires from restored idle state");
        check(owner.reclaim() == ActorReclaimResult.reclaimed,
            "wave actor reclamation");
    }
    check(actors.live == 0 && actors.ready == 0,
        "wave actor runtime drained");
    actors.destroy();
    check(counts.allocations == counts.deallocations,
        "wave actor allocator pairing");

    printf("testActorWave OK\n"); fflush(stdout);
}

void testRuntimeQuiescent()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope (exit) farm.destroy();
    auto actors = ActorRuntime.create(farm, 1);
    check(actors !is null, "quiescent runtime allocation");
    check(actors.quiescent, "empty runtime is quiescent");

    auto owner = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, 3));
    check(owner.valid, "quiescent actor creation");
    check(!actors.quiescent, "a live actor is not quiescent");
    check(owner.handle.wake() == ActorWakeResult.queued, "quiescent actor wake");
    check(actors.ready == 1 && !actors.quiescent, "a queued actor is not quiescent");

    ConsumerView view;
    check(view.subscribe(farm) >= 0, "quiescent consumer subscribe");
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "quiescent producer registration");
    auto deadline = MonoTime.currTime + 30.seconds;
    while (!owner.retired)
    {
        actors.flush(token);
        while (view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            fatal("quiescent actor retirement timeout");
    }
    check(!actors.quiescent, "a retired, unreclaimed actor is still live");
    view.unsubscribe();
    farm.unregisterProducer(token);
    check(owner.reclaim() == ActorReclaimResult.reclaimed, "quiescent actor reclaim");
    check(actors.quiescent, "reclaimed runtime with joined consumers is quiescent");
    actors.destroy();
    printf("testRuntimeQuiescent OK\n"); fflush(stdout);
}

void testActorPayload()
{
    enum consumerCount = 4;
    enum target = 500;
    auto farm = AntFarm.create(2, 8, consumerCount,
        0, 0, 1, 4096);
    scope (exit) farm.destroy();

    ActorAllocCounts counts;
    auto allocator = ActorAllocator(&counts,
        &actorTestAllocate, &actorTestDeallocate);
    auto actors = ActorRuntime.create(farm, 1, allocator);
    check(actors !is null, "actor runtime allocation");

    atomicStore!(MemoryOrder.raw)(g_actorEntries, 0L);
    atomicStore!(MemoryOrder.raw)(g_actorConcurrent, 0L);
    atomicStore!(MemoryOrder.raw)(g_actorCalls, 0L);
    auto owner = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, target));
    check(owner.valid, "actor creation");
    auto exhausted = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, 1));
    check(!exhausted.valid, "actor capacity exhaustion is explicit");
    auto oldHandle = owner.handle;
    immutable warmAllocations = counts.allocations;
    check(oldHandle.wake() == ActorWakeResult.queued,
        "first actor wake queues");

    shared int stop;
    auto deadline = MonoTime.currTime + 30.seconds;
    ActorConsumerJob[consumerCount] consumerJobs;
    Thread[consumerCount] consumers;
    foreach (i; 0 .. consumerCount)
    {
        consumerJobs[i] = new ActorConsumerJob(farm, &stop, deadline);
        consumers[i] = new Thread(&consumerJobs[i].run);
        consumers[i].start();
    }

    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "actor producer registration");
    GC.disable();
    while (!owner.retired)
    {
        actors.flush(token, 32, avgCostForChunk(8));
        // Exercise coalescing while the actor is already queued or running.
        immutable wakeResult = oldHandle.wake();
        check(wakeResult == ActorWakeResult.coalesced
                || wakeResult == ActorWakeResult.closed,
            "actor repeated wake coalesces");
        if (MonoTime.currTime >= deadline)
            fatal("actor retirement timeout");
        Thread.yield();
    }
    GC.enable();
    atomicStore!(MemoryOrder.rel)(stop, 1);
    foreach (thread; consumers) thread.join();
    farm.unregisterProducer(token);

    check(atomicLoad!(MemoryOrder.acq)(g_actorEntries) == 0,
        "actor entry counter drained");
    check(atomicLoad!(MemoryOrder.acq)(g_actorConcurrent) == 0,
        "one exclusive actor borrow at a time");
    check(atomicLoad!(MemoryOrder.acq)(g_actorCalls) == target,
        "actor external state mutated exactly once per activation");
    check(counts.allocations == warmAllocations,
        "actor warm path performs no allocator calls");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "actor explicit reclamation");
    check(oldHandle.wake() == ActorWakeResult.staleHandle,
        "reclaimed actor handle is stale");

    // Reuse the stable slot and prove the old generation cannot affect it.
    auto replacement = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, 1));
    check(replacement.valid, "actor slot reuse");
    auto replacementHandle = replacement.handle;
    check(replacementHandle.generation != oldHandle.generation,
        "actor generation advances on reuse");
    check(oldHandle.wake() == ActorWakeResult.staleHandle,
        "old actor generation stays stale after reuse");

    ConsumerView replacementView;
    check(replacementView.subscribe(farm) >= 0,
        "replacement actor consumer subscribe");
    token = farm.registerProducer(Tier.small);
    check(replacementHandle.wake() == ActorWakeResult.queued,
        "replacement actor wake");
    while (!replacement.retired)
    {
        actors.flush(token);
        while (replacementView.consumeNext()) {}
    }
    replacementView.unsubscribe();
    farm.unregisterProducer(token);
    check(replacement.reclaim() == ActorReclaimResult.reclaimed,
        "replacement actor reclamation");

    // Retirement of a scheduled actor closes it without entering user code.
    auto closed = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, 10));
    auto closedHandle = closed.handle;
    immutable callsBeforeClose = atomicLoad!(MemoryOrder.acq)(g_actorCalls);
    check(closedHandle.wake() == ActorWakeResult.queued,
        "scheduled actor before close");
    check(closed.requestRetire() == ActorRetireResult.requested,
        "scheduled actor retirement requested");
    ConsumerView closedView;
    check(closedView.subscribe(farm) >= 0,
        "closed actor consumer subscribe");
    token = farm.registerProducer(Tier.small);
    while (!closed.retired)
    {
        actors.flush(token);
        while (closedView.consumeNext()) {}
    }
    closedView.unsubscribe();
    farm.unregisterProducer(token);
    check(atomicLoad!(MemoryOrder.acq)(g_actorCalls) == callsBeforeClose,
        "closed scheduled actor does not enter user code");
    check(closed.reclaim() == ActorReclaimResult.reclaimed,
        "closed actor reclamation");

    // An idle actor retires synchronously because no activation owns it.
    auto idle = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, 1));
    check(idle.requestRetire() == ActorRetireResult.requested
            && idle.retired,
        "idle actor retirement");
    check(idle.reclaim() == ActorReclaimResult.reclaimed,
        "idle actor reclamation");
    check(actors.live == 0 && actors.ready == 0,
        "actor runtime drained");
    actors.destroy();
    check(counts.allocations == counts.deallocations,
        "actor allocator pairing");

    printf("testActorPayload OK\n"); fflush(stdout);
}

void testActorErasedAdapter()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    scope (exit) farm.destroy();

    ActorAllocCounts counts;
    auto allocator = ActorAllocator(&counts,
        &actorTestAllocate, &actorTestDeallocate);
    auto actors = ActorRuntime.create(farm, 1, allocator);
    check(actors !is null, "erased actor runtime allocation");
    auto adapter = actors.erasedAdapter;
    check(adapter.valid, "erased actor adapter validity");

    ActorErasedAdapter emptyAdapter;
    ErasedActorResult result;
    auto initial = ErasedActorState(&result, 3);
    check(!emptyAdapter.createActor(&initial, initial.sizeof,
            initial.alignof, &erasedActor).valid,
        "empty erased adapter rejects creation");
    check(!adapter.createActor(&initial, initial.sizeof, 3,
            &erasedActor).valid,
        "erased adapter rejects non-power-of-two alignment");
    check(!adapter.createActor(null, initial.sizeof, initial.alignof,
            &erasedActor).valid,
        "erased adapter rejects null initial state");

    auto owner = adapter.createActor(&initial, initial.sizeof,
        initial.alignof, &erasedActor);
    check(owner.valid, "erased actor creation");
    auto handle = owner.handle;
    immutable warmAllocations = counts.allocations;

    ActorInboxNode message;
    message.initialize(null, 0x1234_5678UL);
    immutable sent = handle.send(&message);
    check(sent == ActorSendResult.queued
            || sent == ActorSendResult.coalesced,
        "erased actor inbox send");

    ConsumerView view;
    check(view.subscribe(farm) >= 0, "erased actor consumer subscribe");
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "erased actor producer registration");
    auto deadline = MonoTime.currTime + 30.seconds;
    while (!owner.retired)
    {
        actors.flush(token);
        while (view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            fatal("erased actor retirement timeout");
    }
    view.unsubscribe();
    farm.unregisterProducer(token);

    check(result.activations == initial.target && result.messages == 1
            && result.sum == message.tag,
        "erased actor dispatch and inbox state");
    check(result.observedSize == ErasedActorState.sizeof
            && result.observedAlignment == ErasedActorState.alignof,
        "erased borrow preserves state metadata");
    check(message.available, "erased actor completes inbox node");
    check(counts.allocations == warmAllocations,
        "erased actor warm path performs no allocator calls");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "erased actor reclamation");
    check(handle.wake() == ActorWakeResult.staleHandle,
        "erased actor handle becomes stale");

    // A typed module can hand its unique owner to a resident registry without
    // retaining T in that registry's layout.
    auto typed = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, 1));
    check(typed.valid, "typed actor before owner erasure");
    auto typedHandle = typed.handle;
    ActorErasedHandle erasedHandle = typedHandle.erased;
    auto erasedOwner = typed.intoErased();
    check(!typed.valid && erasedOwner.valid
            && erasedHandle.generation == erasedOwner.generation,
        "typed owner transfers to erased registry form");
    check(erasedOwner.requestRetire() == ActorRetireResult.requested
            && erasedOwner.retired,
        "erased owner retires typed idle actor");
    check(erasedOwner.reclaim() == ActorReclaimResult.reclaimed,
        "erased owner reclaims typed actor");
    check(erasedHandle.wake() == ActorWakeResult.staleHandle,
        "erased typed handle becomes stale");

    check(actors.live == 0 && actors.ready == 0,
        "erased actor runtime drained");
    actors.destroy();
    check(counts.allocations == counts.deallocations,
        "erased actor allocator pairing");

    printf("testActorErasedAdapter OK\n"); fflush(stdout);
}

void testActorInbox()
{
    enum consumerCount = 4;
    enum producerCount = 4;
    enum acceptedPerProducer = 256;
    enum closedPerProducer = 128;
    enum messagesPerProducer = acceptedPerProducer + closedPerProducer;
    enum messageCount = producerCount * messagesPerProducer;

    auto farm = AntFarm.create(2, 8, consumerCount,
        0, 0, 1, 4096);
    scope (exit) farm.destroy();

    ActorAllocCounts counts;
    auto allocator = ActorAllocator(&counts,
        &actorTestAllocate, &actorTestDeallocate);
    auto actors = ActorRuntime.create(farm, 1, allocator);
    check(actors !is null, "inbox actor runtime allocation");
    auto owner = actors.createActor!(ActorInboxState, inboxActor)(
        ActorInboxState.init);
    check(owner.valid, "inbox actor creation");
    auto handle = owner.handle;
    immutable warmAllocations = counts.allocations;

    auto messages = cast(ActorInboxMessage*) malloc(
        messageCount * ActorInboxMessage.sizeof);
    check(messages !is null, "inbox message allocation");
    scope (exit) free(messages);
    memset(messages, 0, messageCount * ActorInboxMessage.sizeof);
    ulong expectedSum;
    foreach (producer; 0 .. producerCount)
    {
        foreach (i; 0 .. messagesPerProducer)
        {
            immutable index = producer * messagesPerProducer + i;
            messages[index].value = index + 1;
            messages[index].node.initialize(&messages[index], index);
            if (i < acceptedPerProducer)
                expectedSum += messages[index].value;
        }
    }

    atomicStore!(MemoryOrder.raw)(g_inboxEntries, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxConcurrent, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxActivations, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxConsumed, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxSum, 0UL);
    atomicStore!(MemoryOrder.raw)(g_inboxAccepted, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxClosed, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxUnexpected, 0L);

    ActorInboxMessage busyProbe;
    busyProbe.value = messageCount + 1;
    busyProbe.node.initialize(&busyProbe, messageCount);
    immutable probeResult = handle.send(&busyProbe.node);
    check(probeResult == ActorSendResult.queued
            || probeResult == ActorSendResult.coalesced,
        "first inbox node submission accepted");
    atomicFetchAdd!(MemoryOrder.rel)(g_inboxAccepted, 1L);
    expectedSum += busyProbe.value;
    check(handle.send(&busyProbe.node) == ActorSendResult.nodeBusy,
        "owned inbox node cannot be submitted twice");

    shared int stopConsumers;
    shared int releaseClosedPhase;
    shared long producersReady;
    auto deadline = MonoTime.currTime + 30.seconds;
    ActorConsumerJob[consumerCount] consumerJobs;
    Thread[consumerCount] consumers;
    foreach (i; 0 .. consumerCount)
    {
        consumerJobs[i] = new ActorConsumerJob(
            farm, &stopConsumers, deadline);
        consumers[i] = new Thread(&consumerJobs[i].run);
    }
    ActorInboxProducerJob[producerCount] producerJobs;
    Thread[producerCount] producers;
    foreach (i; 0 .. producerCount)
    {
        producerJobs[i] = new ActorInboxProducerJob(handle,
            messages + i * messagesPerProducer,
            acceptedPerProducer, closedPerProducer,
            &producersReady, &releaseClosedPhase);
        producers[i] = new Thread(&producerJobs[i].run);
    }

    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "inbox producer registration");
    foreach (thread; consumers) thread.start();
    foreach (thread; producers) thread.start();

    GC.disable();
    while (atomicLoad!(MemoryOrder.acq)(producersReady) != producerCount)
    {
        actors.flush(token, 32, avgCostForChunk(8));
        if (MonoTime.currTime >= deadline)
            fatal("actor inbox producer timeout");
        Thread.yield();
    }

    check(owner.requestRetire() == ActorRetireResult.requested,
        "inbox actor retirement closes admission");
    atomicStore!(MemoryOrder.rel)(releaseClosedPhase, 1);
    while (!owner.retired)
    {
        actors.flush(token, 32, avgCostForChunk(8));
        if (MonoTime.currTime >= deadline)
            fatal("actor inbox retirement timeout");
        Thread.yield();
    }
    GC.enable();

    foreach (thread; producers) thread.join();
    atomicStore!(MemoryOrder.rel)(stopConsumers, 1);
    foreach (thread; consumers) thread.join();
    farm.unregisterProducer(token);

    immutable acceptedTotal = producerCount * acceptedPerProducer + 1;
    immutable closedTotal = producerCount * closedPerProducer;
    check(atomicLoad!(MemoryOrder.acq)(g_inboxUnexpected) == 0,
        "inbox sends have deterministic outcomes");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxAccepted) == acceptedTotal,
        "all open-generation sends accepted");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxClosed) == closedTotal,
        "all post-close sends rejected");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxConsumed) == acceptedTotal,
        "all accepted interactions consumed exactly once");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxSum) == expectedSum,
        "inbox payload contents published to actor");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxEntries) == 0
            && atomicLoad!(MemoryOrder.acq)(g_inboxConcurrent) == 0,
        "inbox actor retains exclusive callback ownership");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxActivations) > 1,
        "bounded inbox drain republishes activations");
    foreach (i; 0 .. messageCount)
        check(messages[i].node.available,
            "accepted and rejected inbox nodes returned to caller");
    check(busyProbe.node.available,
        "busy-probe inbox node returned to caller");
    check(handle.send(&messages[0].node) == ActorSendResult.closed,
        "retired generation rejects an available node");
    check(counts.allocations == warmAllocations,
        "inbox warm path performs no actor allocator calls");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "inbox actor explicit reclamation");
    check(handle.send(&messages[0].node) == ActorSendResult.staleHandle,
        "reclaimed inbox handle is stale");

    // Leave one accepted interaction queued while retirement closes the
    // generation. With no consumer active, RETIRED must wait for that node.
    auto drainOwner = actors.createActor!(ActorInboxState, inboxActor)(
        ActorInboxState.init);
    check(drainOwner.valid, "inbox actor slot reuse");
    auto drainHandle = drainOwner.handle;
    ActorInboxMessage retirementProbe;
    retirementProbe.value = 1;
    retirementProbe.node.initialize(&retirementProbe);
    immutable drainSend = drainHandle.send(&retirementProbe.node);
    check(drainSend == ActorSendResult.queued
            || drainSend == ActorSendResult.coalesced,
        "pre-close inbox interaction accepted");
    check(drainOwner.requestRetire() == ActorRetireResult.requested
            && !drainOwner.retired,
        "retirement waits for accepted inbox interaction");
    check(drainHandle.send(&messages[0].node) == ActorSendResult.closed,
        "closing reused generation rejects new interaction");

    ConsumerView drainView;
    check(drainView.subscribe(farm) >= 0,
        "closing inbox actor consumer subscribe");
    token = farm.registerProducer(Tier.small);
    check(token.valid, "closing inbox producer registration");
    while (!drainOwner.retired)
    {
        actors.flush(token);
        while (drainView.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            fatal("closing inbox drain timeout");
    }
    drainView.unsubscribe();
    farm.unregisterProducer(token);
    check(retirementProbe.node.available,
        "accepted interaction completed before retirement");
    check(drainOwner.reclaim() == ActorReclaimResult.reclaimed,
        "closing inbox actor reclamation");
    check(actors.live == 0 && actors.ready == 0,
        "inbox actor runtime drained");
    actors.destroy();
    check(counts.allocations == counts.deallocations,
        "inbox actor allocator pairing");

    printf("testActorInbox OK\n"); fflush(stdout);
}

private long quotaProbeCallback(PayloadHeader*, PayloadBody)
    nothrow @nogc @system
{
    return 1;
}

void testSweptQuotaBalance()
{
    enum ulong quota = 512;
    auto f = AntFarm.create(2, 8, 1, 0, 0, 1, quota);
    scope (exit) f.destroy();
    ConsumerView view;
    check(view.subscribe(f) >= 0, "quota balance subscription");
    scope (exit) view.unsubscribe();
    auto token = f.registerProducer(Tier.small);
    scope (exit) f.unregisterProducer(token);

    // The documented producer-slot layout exposes the authoritative ledger.
    // A fresh registration does not authorize an unchecked write excursion.
    auto ledger = f.prodHashSmall + token.slot * 8 + 1;
    check(atomicLoad(*ledger) == 0, "registration waits for a checked grant");
    PayloadHeader header;

    header.call = &quotaProbeCallback;
    PayloadEntry[1] entries = [PayloadEntry(&header, null)];
    ulong spent;
    foreach (i; 0 .. 6)
    {
        immutable before = atomicLoad(f.Wt);
        check(f.write(entries[], token) == 1, "quota balance publication");
        spent += atomicLoad(f.Wt) - before;
        check(spent < quota, "probe remains within the first grant");
        check(atomicLoad(*ledger) == quota - spent,
            "publication spends quota without automatic renewal");
    }
    while (view.consumeNext()) {}
    printf("testSweptQuotaBalance OK\n"); fflush(stdout);
}

__gshared shared(int) g_producerLifecycleReady;
__gshared shared(int) g_producerLifecycleStop;

private final class ProducerLifecycleConsumer
{
    AntFarm* farm;
    this(AntFarm* farm) { this.farm = farm; }
    void run()
    {
        ConsumerView view;
        check(view.subscribe(farm) >= 0, "producer lifecycle subscription");
        atomicFetchAdd(g_producerLifecycleReady, 1);
        while (atomicLoad(g_producerLifecycleStop) == 0)
            if (!view.consumeNext()) Thread.yield();
        while (view.consumeNext()) {}
        view.unsubscribe();
    }
}

private final class ProducerLifecycleJob
{
    AntFarm* farm;
    Tier tier;
    this(AntFarm* farm, Tier tier) { this.farm = farm; this.tier = tier; }
    void run()
    {
        PayloadEntry[] emptyBatch;
        PayloadHeader header;

        header.call = &quotaProbeCallback;
        PayloadEntry[1] entries = [PayloadEntry(&header, null)];
        auto deadline = MonoTime.currTime + 30.seconds;
        foreach (i; 0 .. 10_000)
        {
            auto token = farm.registerProducer(tier);
            check(token.valid, "producer lifecycle has spare capacity");
            while (farm.write(entries[], token) == 0)
            {
                check(MonoTime.currTime < deadline,
                    "producer lifecycle publication timeout");
                Thread.yield();
            }
            // Even an empty write validates the authoritative quota ledger.
            // Reusing a released hash must not let the departing producer
            // clear the new registration's ledger after this call starts.
            check(farm.write(emptyBatch, token) == 0,
                "fresh producer ticket remains valid under churn");
            farm.unregisterProducer(token);
        }
    }
}

void testProducerLifecycleChurn()
{
    auto f = AntFarm.create(2, 8, 4, 8, 4096, 8, 4096);
    scope (exit) f.destroy();
    atomicStore(g_producerLifecycleReady, 0);
    atomicStore(g_producerLifecycleStop, 0);
    ProducerLifecycleConsumer[4] consumers;
    Thread[4] consumerThreads;
    foreach (i; 0 .. consumerThreads.length)
    {
        consumers[i] = new ProducerLifecycleConsumer(f);
        consumerThreads[i] = new Thread(&consumers[i].run);
        consumerThreads[i].start();
    }
    auto deadline = MonoTime.currTime + 5.seconds;
    while (atomicLoad(g_producerLifecycleReady) != consumerThreads.length)
    {
        check(MonoTime.currTime < deadline, "producer lifecycle startup timeout");
        Thread.yield();
    }
    ProducerLifecycleJob[8] jobs;
    Thread[8] threads;
    foreach (i; 0 .. threads.length)
    {
        jobs[i] = new ProducerLifecycleJob(f, i < 4 ? Tier.small : Tier.bulk);
        threads[i] = new Thread(&jobs[i].run);
    }
    foreach (thread; threads) thread.start();
    foreach (thread; threads) thread.join();
    atomicStore(g_producerLifecycleStop, 1);
    foreach (thread; consumerThreads) thread.join();
    printf("testProducerLifecycleChurn OK\n"); fflush(stdout);
}

// Cross-Farm provenance, positive side: with every token now carrying its
// issuing Farm's identity, independent Farms must keep working normally --
// ordinary writes, token transfer, slot reuse after unregister, and
// concurrent operation. (The fatal side lives in the tests/private
// death harness: cross-farm-write / cross-farm-flush / cross-farm-unregister.)
class CrossFarmWorker
{
    AntFarm* farm;
    this(AntFarm* f) { farm = f; }
    void run()
    {
        ConsumerView v;
        check(v.subscribe(farm) >= 0, "cross-farm worker subscribe");
        scope (exit) v.unsubscribe();
        auto tok = farm.registerProducer(Tier.small);
        check(tok.valid, "cross-farm worker register");
        scope (exit) farm.unregisterProducer(tok);
        PayloadHeader h;
        ulong[2] body;
        PayloadEntry[1] e = [payloadEntry!testCb(&h, body[0 .. 2], 0, 1)];
        foreach (_; 0 .. 64)
        {
            while (farm.write(e[], tok) == 0) pause();
            while (v.consumeNext()) {}
        }
    }
}
void testTokenMoveOnly()
{
    // A copy (explicit, by-value parameter, or through an algorithm that
    // takes elements by value) must not compile: the copy-then-invalidate
    // of the old transfer-on-copy token silently invalidated the source.
    static assert(!__traits(compiles,
    {
        Token src;
        Token dst = src;
    }));
    static assert(!__traits(compiles,
    {
        Token src;
        Token dst;
        dst = src;
    }));
    static assert(!__traits(compiles,
    {
        void takesByValue(Token taken) {}
        Token src;
        takesByValue(src);
    }));
    // Transfer is explicit: move into a local or an rvalue parameter, and
    // move-assignment into an invalid destination, all compile.
    static assert(__traits(compiles,
    {
        Token src;
        Token dst = move(src);
        Token dst2;
        dst2 = move(src);
        void takesByValue(Token taken) {}
        takesByValue(move(dst));
    }));

    enum N = 4;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    foreach (i; 0 .. N)
        entries[i] = payloadEntry!testCb(&headers[i], bodies[i * 2 .. i * 2 + 2], i, 1);

    auto f = AntFarm.create(2, 8, 2, 1, 8192, 4, 2048);
    scope (exit) f.destroy();
    ConsumerView v;
    check(v.subscribe(f) >= 0, "subscribe for move-only token test");

    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "move-only test registers a token");
    Token moved = move(tok);
    check(!tok.valid, "move consumes the source token");
    check(moved.valid, "moved-into token stays valid");
    check(f.write(entries[0 .. N], moved) == N, "write with moved token");
    while (v.consumeNext()) {}
    Token reassigned;
    reassigned = move(moved);
    check(!moved.valid, "move-assignment consumes its source");
    check(reassigned.valid, "move-assignment destination valid");
    check(f.write(entries[0 .. N], reassigned) == N,
        "write with reassigned token");
    while (v.consumeNext()) {}
    f.unregisterProducer(reassigned);
    check(!reassigned.valid, "unregistered moved token invalid");
    v.unsubscribe();
    printf("testTokenMoveOnly OK\n"); fflush(stdout);
}
void testCrossFarmProvenance()
{
    enum N = 8;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    foreach (i; 0 .. N)
        entries[i] = payloadEntry!testCb(&headers[i], bodies[i * 2 .. i * 2 + 2], i, 1);

    auto fa = AntFarm.create(2, 8, 2, 1, 8192, 4, 2048);
    scope (exit) fa.destroy();
    auto fb = AntFarm.create(2, 8, 2, 1, 8192, 4, 2048);
    scope (exit) fb.destroy();

    ConsumerView va;
    check(va.subscribe(fa) >= 0, "subscribe farm A");
    ConsumerView vb;
    check(vb.subscribe(fb) >= 0, "subscribe farm B");

    auto ta = fa.registerProducer(Tier.small);
    check(ta.valid, "register on farm A");
    auto tb = fb.registerProducer(Tier.small);
    check(tb.valid, "register on farm B");
    // Identical registration histories still produce identical slot hashes;
    // provenance, not the hash, is what distinguishes the tickets now.
    check(ta.hash == tb.hash, "matching histories collide in the slot hash");

    // Ordinary writes on each farm, consumed by that farm's own view.
    check(fa.write(entries[0 .. 4], ta) == 4, "write farm A");
    check(fb.write(entries[4 .. 8], tb) == 4, "write farm B");
    while (va.consumeNext()) {}
    while (vb.consumeNext()) {}
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == N, "both farms executed");

    // Token transfer stays a same-farm transfer: the source is consumed and
    // the destination carries the provenance.
    Token ta2 = move(ta);
    check(!ta.valid, "transfer consumes source");
    check(ta2.valid, "transfer destination valid");
    check(fa.write(entries[0 .. 2], ta2) == 2, "write with transferred token");
    while (va.consumeNext()) {}

    // Unregister then re-register: slot reuse on each farm keeps working.
    fa.unregisterProducer(ta2);
    check(!ta2.valid, "unregistered token invalid");
    fb.unregisterProducer(tb);
    auto ta3 = fa.registerProducer(Tier.small);
    check(ta3.valid, "re-register farm A");
    auto tb3 = fb.registerProducer(Tier.small);
    check(tb3.valid, "re-register farm B");
    check(fa.write(entries[2 .. 4], ta3) == 2, "write after reuse on farm A");
    check(fb.write(entries[6 .. 8], tb3) == 2, "write after reuse on farm B");
    while (va.consumeNext()) {}
    while (vb.consumeNext()) {}
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == N + 2 + 4, "reuse executed");

    // Concurrent Farms: two threads each register, write, consume, and
    // unregister on their own Farm at the same time.
    auto wa = new CrossFarmWorker(fa);
    auto wb = new CrossFarmWorker(fb);
    auto tha = new Thread(&wa.run);
    auto thb = new Thread(&wb.run);
    tha.start();
    thb.start();
    tha.join();
    thb.join();
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == N + 2 + 4 + 128,
        "concurrent farms executed");

    va.unsubscribe();
    vb.unsubscribe();
    fa.unregisterProducer(ta3);
    fb.unregisterProducer(tb3);
    printf("testCrossFarmProvenance OK\n"); fflush(stdout);
}

// Same-shard plain-write completion join.
//
// Both consumers claim different 1-payload runs of shard 0 (smallThreshold
// is uint.max, so the table is not split across shards; MAX_AVG_COST makes
// chunk size 1). The handshake before the plain writes is relaxed. There is
// no test-side release after those writes, so a missing Farm join cannot hide
// behind the test.
//
// Mutation check, do not leave applied: in processShard, change
//   atomicFetchAdd!(MemoryOrder.acq_rel)(*shc, cast(ulong) runlen)
// back to MemoryOrder.raw, rebuild the Farm TSan binary, and run
//   ANTFARM_HUGE_PAGES=0 TSAN_OPTIONS=history_size=7:halt_on_error=1 \
//     ./antfarm_test_tsan join-only
// ThreadSanitizer reports a data race in joined (exit 66). Acquire/release
// passes with no TSan warning.
struct JoinProbe { shared uint entered, done; ulong[2] values; }
struct JoinBodies
{
    JoinProbe* probe; size_t index; ulong[2] scratch;
    @property bool empty() const nothrow @nogc { return index == 2; }
    @property size_t length() const nothrow @nogc { return 2 - index; }
    @property JoinBodies save() nothrow @nogc { return this; }
    @property PayloadBody front() nothrow @nogc @system
    { scratch[0] = cast(ulong) probe; scratch[1] = index; return scratch[]; }
    void popFront() nothrow @nogc { ++index; }
}
void joinAbort() nothrow @nogc @system
{
    fprintf(stderr, "FAIL same-shard join\n");
    import core.stdc.stdlib : abort;
    abort();
}
long joinWork(PayloadHeader*, PayloadBody body) nothrow @nogc @system
{
    auto probe = cast(JoinProbe*) atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*) body.ptr);
    auto index = atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*) (body.ptr + 1));
    atomicFetchAdd!(MemoryOrder.raw)(probe.entered, 1u);
    while (atomicLoad!(MemoryOrder.raw)(probe.entered) != 2) pause();
    probe.values[index] = index + 1; // no test release after this write
    return 1;
}
long joinWorkAlt(PayloadHeader* header, PayloadBody body) nothrow @nogc @system
{
    return joinWork(header, body);
}
void joined(void* p) nothrow @nogc @system
{
    auto probe = cast(JoinProbe*) p;
    if (probe.values[0] != 1 || probe.values[1] != 2) joinAbort();
    if (atomicFetchAdd!(MemoryOrder.rel)(probe.done, 1u) != 0) joinAbort();
}
void testSameShardPlainWriteJoin()
{
    foreach (acrossShards; [false, true])
    {
        auto farm = AntFarm.create(2, 8, 9, 0, 0, 1, 8192,
            acrossShards ? 1 : uint.max);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView[7] observers;
        if (acrossShards) foreach (ref observer; observers)
            check(observer.subscribe(farm) >= 0, "join observer subscribe");
        scope(exit) if (acrossShards) foreach (ref observer; observers) observer.unsubscribe();
        shared uint stop, started;
        Thread[2] threads;
        foreach (i; 0 .. threads.length)
        {
            threads[i] = new Thread({
                ConsumerView view;
                if (view.subscribe(farm) < 0) joinAbort();
                scope(exit) view.unsubscribe();
                atomicFetchAdd(started, 1u);
                while (atomicLoad(stop) == 0)
                    if (!view.consumeNext()) pause();
                while (view.consumeNext()) {}
            });
            threads[i].start();
        }
        auto startDeadline = MonoTime.currTime + 10.seconds;
        while (atomicLoad(started) < threads.length)
            check(MonoTime.currTime < startDeadline, "join consumers did not start");
        foreach (_; 0 .. 30)
        {
            JoinProbe probe;
            TableCompletionHook hook = TableCompletionHook(&probe, &joined);
            PayloadHeader[2] headers;
            headers[0].call = &joinWork;
            headers[1].call = &joinWorkAlt;
            ulong[3][2] words = [[cast(ulong) &probe, 0, 0], [cast(ulong) &probe, 1, 0]];
            PayloadBody[2] bodies = [words[0][0 .. 2], words[1][]];
            PayloadEntry[2] entries = [PayloadEntry(&headers[0], bodies[0]),
                PayloadEntry(&headers[1], bodies[1])];
            ulong n;
            final switch (_ % 6)
            {
            case 0: n = farm.writeTracked(headers[0], JoinBodies(&probe), 2, &hook, token, MAX_AVG_COST); break;
            case 1: n = farm.write(entries[], token, MAX_AVG_COST, &hook); break;
            case 2:
                import std.algorithm.iteration : map;
                n = farm.write(entries[].map!(e => e), token, MAX_AVG_COST, &hook); break;
            case 3: n = farm.write(headers[], bodies[], token, MAX_AVG_COST, &hook); break;
            case 4: n = farm.write(headers[0], bodies[], token, MAX_AVG_COST, &hook); break;
            case 5: n = farm.write(headers[0], JoinBodies(&probe), 2, token, MAX_AVG_COST, &hook); break;
            }

            check(n == 2, "join table was not fully admitted");
            auto deadline = MonoTime.currTime + 10.seconds;
            while (atomicLoad!(MemoryOrder.acq)(probe.done) == 0)
                check(MonoTime.currTime < deadline, "join timeout");
        }
        atomicStore(stop, 1u);
        foreach (t; threads) t.join();
    }
    printf("PASS same/cross-shard plain-write completion join, all write forms\n"); fflush(stdout);
}

// Spec 7a chunk boundaries. A wholesale table (smallThreshold is uint.max) is
// claimed in runs of `128 >> avgCost` entries by the consumers of shard 0. The
// callback of entry 0 holds its run open until a second thread has executed an
// entry of the table, so that thread owns the second claim and no thread can
// take two runs. A table of three chunks must then split exactly as
// [0, chunk) on one thread and [chunk, 2 * chunk) on the other: a smaller
// chunk would hand the first run's tail to the second thread, and a larger
// one would fold the second run into the first. The expected chunks below are
// literals on purpose: they pin MAX_CHUNK = 128 and the default class.
__gshared size_t g_chunkTableN;
shared ulong[3 * 128] g_chunkOwner;
shared uint g_chunkCalls;
shared ulong g_chunkNextId;
ulong t_chunkId; // thread-local: set once per consumer thread
void chunkAbort() nothrow @nogc @system
{
    fprintf(stderr, "FAIL chunk boundary probe timed out\n");
    import core.stdc.stdlib : abort;
    abort();
}
bool chunkOtherThreadRan(ulong me) nothrow @nogc @system
{
    foreach (j; 1 .. atomicLoad!(MemoryOrder.raw)(g_chunkTableN))
    {
        immutable owner = atomicLoad!(MemoryOrder.acq)(g_chunkOwner[j]);
        if (owner != 0 && owner != me) return true;
    }
    return false;
}
long chunkProbeWork(PayloadHeader*, PayloadBody body) nothrow @nogc @system
{
    immutable i = cast(size_t) body[0];
    immutable me = t_chunkId;
    atomicStore!(MemoryOrder.rel)(g_chunkOwner[i], me);
    if (i == 0)
    {
        immutable deadline = MonoTime.currTime + 10.seconds;
        while (!chunkOtherThreadRan(me))
        {
            if (MonoTime.currTime > deadline) chunkAbort();
            pause();
        }
    }
    atomicFetchAdd!(MemoryOrder.rel)(g_chunkCalls, 1u);
    return 1;
}
shared uint g_chunkHookDone;
void chunkHookDone(void*) nothrow @nogc @system
{
    atomicStore!(MemoryOrder.rel)(g_chunkHookDone, 1u);
}
final class ChunkRig
{
    AntFarm* farm;
    Token token;
    shared uint stop, started;
    Thread[2] threads;
    PayloadHeader header;
    ulong[3 * 128] ids;
    PayloadEntry[3 * 128] entries;
    PayloadHeader*[3 * 128] headerPtrs;
    PayloadBody[3 * 128] bodies;
    TableCompletionHook hook;

    this()
    {
        farm = AntFarm.create(2, 8, 2, 0, 0, 1, 8192, uint.max);
        token = farm.registerProducer(Tier.small);
        foreach (i; 0 .. threads.length)
        {
            threads[i] = new Thread({
                t_chunkId = atomicFetchAdd(g_chunkNextId, 1UL) + 1;
                ConsumerView view;
                if (view.subscribe(farm) < 0) chunkAbort();
                scope(exit) view.unsubscribe();
                atomicFetchAdd(started, 1u);
                while (atomicLoad(stop) == 0)
                    if (!view.consumeNext()) pause();
                while (view.consumeNext()) {}
            });
            threads[i].start();
        }
        auto startDeadline = MonoTime.currTime + 10.seconds;
        while (atomicLoad(started) < threads.length)
            check(MonoTime.currTime < startDeadline, "chunk consumers did not start");

        header.plen = 1;
        header.call = &chunkProbeWork;
        hook = TableCompletionHook(null, &chunkHookDone);
        foreach (i; 0 .. entries.length)
        {
            ids[i] = i;
            bodies[i] = ids[i .. i + 1];
            headerPtrs[i] = &header;
            entries[i] = PayloadEntry(&header, bodies[i]);
        }
    }

    void finish()
    {
        atomicStore(stop, 1u);
        foreach (t; threads) t.join();
        farm.unregisterProducer(token);
        farm.destroy();
    }

    // Runs one table of `3 * chunk` entries through `write` and checks the
    // split. `write` returns the entries admitted.
    void expectSplit(size_t chunk, scope size_t delegate(size_t n) write,
                     const(char)[] label)
    {
        immutable n = 3 * chunk;
        atomicStore!(MemoryOrder.raw)(g_chunkTableN, n);
        foreach (ref o; g_chunkOwner) atomicStore!(MemoryOrder.raw)(o, 0UL);
        atomicStore!(MemoryOrder.raw)(g_chunkCalls, 0u);
        atomicStore!(MemoryOrder.raw)(g_chunkHookDone, 0u);
        check(write(n) == n, label);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (atomicLoad!(MemoryOrder.acq)(g_chunkCalls) != n)
            check(MonoTime.currTime < deadline, "chunk table timeout");
        immutable first = atomicLoad!(MemoryOrder.raw)(g_chunkOwner[0]);
        immutable second = atomicLoad!(MemoryOrder.raw)(g_chunkOwner[chunk]);
        check(first != 0 && second != 0 && first != second, label);
        foreach (i; 0 .. chunk)
            check(atomicLoad!(MemoryOrder.raw)(g_chunkOwner[i]) == first, label);
        foreach (i; chunk .. 2 * chunk)
            check(atomicLoad!(MemoryOrder.raw)(g_chunkOwner[i]) == second, label);
    }
}
void testChunkBoundariesPerClass()
{
    auto rig = new ChunkRig;
    // Class k claims 128 >> k entries: classes 0 .. 7.
    static immutable size_t[8] chunks = [128, 64, 32, 16, 8, 4, 2, 1];
    foreach (avgCost; 0 .. chunks.length)
        rig.expectSplit(chunks[avgCost],
            (size_t n) => rig.farm.write(rig.entries[0 .. n], rig.token, cast(uint) avgCost),
            "explicit avgCost chunk split");
    rig.finish();
    printf("PASS chunk boundaries for cost classes 0..7\n");
    fflush(stdout);
}
// Every write overload without an avgCost argument publishes the default
// class: claims of exactly 16 entries (the literal pins DEFAULT_AVG_COST = 3).
void testDefaultAvgCostChunk()
{
    import std.algorithm.iteration : map;
    auto rig = new ChunkRig;
    enum chunk = 16;
    rig.expectSplit(chunk, (size_t n) => rig.farm.write(rig.entries[0 .. n], rig.token),
        "default write(PayloadEntry[]) chunk");
    rig.expectSplit(chunk,
        (size_t n) => rig.farm.write(iota(n).map!(i => rig.entries[i]), rig.token),
        "default write(range) chunk");
    rig.expectSplit(chunk,
        (size_t n) => rig.farm.write(rig.headerPtrs[0 .. n], rig.bodies[0 .. n], rig.token),
        "default write(headers, bodies) chunk");
    rig.expectSplit(chunk,
        (size_t n) => rig.farm.write(rig.header, rig.bodies[0 .. n], rig.token),
        "default broadcast write chunk");
    rig.expectSplit(chunk,
        (size_t n) => rig.farm.write(rig.header, rig.bodies[0 .. n], 1UL, rig.token),
        "default broadcast write(bodyWords) chunk");
    rig.expectSplit(chunk,
        (size_t n) => rig.farm.writeTracked(rig.header, rig.bodies[0 .. n], 1UL,
            &rig.hook, rig.token),
        "default writeTracked chunk");
    auto doneDeadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad!(MemoryOrder.acq)(g_chunkHookDone) == 0)
        check(MonoTime.currTime < doneDeadline, "default writeTracked hook did not run");
    rig.finish();
    printf("PASS default write overloads claim 16 entries\n");
    fflush(stdout);
}
// Hardcoded pins for the class constants and the chunk-to-class helper.
static assert(MAX_CHUNK == 128 && MAX_AVG_COST == 7 && DEFAULT_AVG_COST == 3);
static assert(avgCostForChunk(128) == 0 && avgCostForChunk(1000) == 0);
static assert(avgCostForChunk(32) == 2 && avgCostForChunk(16) == 3);
static assert(avgCostForChunk(8) == 4 && avgCostForChunk(1) == 7);
static assert(avgCostForChunk(0) == 7 && avgCostForChunk(24) == 3);

// Regression: Wt reservation must be acq_rel (spec 5a, 9). P spends a quota
// balance granted before Q drives the ring through several laps; the
// consumer's callback reads ring body words plainly. With a release-only
// reservation, P's ring stores have no happens-before with those reads (TSan
// reported races with the historical full-header fixture). Handshakes are
// relaxed so the test supplies no synchronization of its own. The ST fixture
// uses enough compact records for several laps; its TSan negative control
// must be re-established on Linux before claiming equivalent race sensitivity.
__gshared AntFarm* g_staleFarm;
shared int g_stalePReady, g_staleQDone;
shared long g_staleCalls;
shared ulong g_staleObserved;
enum STALE_N = 400_000;
long staleQuotaCb(PayloadHeader*, PayloadBody body) nothrow @nogc @system
{
    atomicStore!(MemoryOrder.raw)(g_staleObserved, body[0] + body[1]); // plain ring reads
    atomicFetchAdd!(MemoryOrder.raw)(g_staleCalls, 1L);
    return 1;
}
void staleQuotaPaused()
{
    auto tok = g_staleFarm.registerProducer(Tier.small);
    PayloadHeader h;
    h.plen = 2;
    h.call = &staleQuotaCb;
    ulong[2] body = [1, 2];
    PayloadEntry[1] e = [PayloadEntry(&h, body[])];
    while (g_staleFarm.write(e[], tok) == 0) Thread.yield();
    atomicStore!(MemoryOrder.raw)(g_stalePReady, 1);
    while (atomicLoad!(MemoryOrder.raw)(g_staleQDone) == 0) Thread.yield();
    // Spends the original grant after Q has lapped the ring.
    check(g_staleFarm.write(e[], tok) == 1, "stale quota write");
    g_staleFarm.unregisterProducer(tok);
}
void staleQuotaMoving()
{
    while (atomicLoad!(MemoryOrder.raw)(g_stalePReady) == 0) Thread.yield();
    auto tok = g_staleFarm.registerProducer(Tier.small);
    PayloadHeader h;
    h.plen = 2;
    h.call = &staleQuotaCb;
    ulong[2] body = [3, 4];
    auto entries = cast(PayloadEntry*) malloc(STALE_N * PayloadEntry.sizeof);
    scope (exit) free(entries);
    foreach (i; 0 .. STALE_N) entries[i] = PayloadEntry(&h, body[]);
    size_t off;
    while (off < STALE_N)
    {
        off += g_staleFarm.write(entries[off .. STALE_N], tok);
        Thread.yield();
    }
    atomicStore!(MemoryOrder.raw)(g_staleQDone, 1);
    g_staleFarm.unregisterProducer(tok);
}
void staleQuotaConsume()
{
    ConsumerView v;
    check(v.subscribe(g_staleFarm) >= 0, "stale quota subscribe");
    while (atomicLoad!(MemoryOrder.raw)(g_staleCalls) < STALE_N + 2)
        if (!v.consumeNext()) Thread.yield();
    v.unsubscribe();
}
void testStaleQuotaReuse()
{
    g_staleFarm = AntFarm.create(2, 8, 1, 0, 0, 2, 4096);
    atomicStore!(MemoryOrder.raw)(g_stalePReady, 0);
    atomicStore!(MemoryOrder.raw)(g_staleQDone, 0);
    atomicStore!(MemoryOrder.raw)(g_staleCalls, 0L);
    auto c = new Thread(&staleQuotaConsume);
    auto p = new Thread(&staleQuotaPaused);
    auto q = new Thread(&staleQuotaMoving);
    c.start(); p.start(); q.start();
    p.join(); q.join(); c.join();
    check(atomicLoad!(MemoryOrder.raw)(g_staleFarm.Wt) > 2 * g_staleFarm.Ln,
        "stale quota test lapped the ring");
    g_staleFarm.destroy();
    g_staleFarm = null;
    printf("testStaleQuotaReuse OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Compact ST tables (SPEC 2f)
// ---------------------------------------------------------------------

__gshared const(size_t)* g_compactLens;
__gshared size_t g_compactN;
__gshared shared(long) g_compactZero;
__gshared shared(long) g_compactBad;
__gshared shared(long) g_compactHeaderBad;
__gshared shared(long) g_compactCompletions;
__gshared ulong g_compactLastHead;

ulong compactWord(size_t idx, size_t k) pure nothrow @nogc @safe
{
    return k == 0 ? cast(ulong) idx : (cast(ulong) idx << 20) + k;
}

/// Exact compact table size for n payloads with total body length sumBody.
ulong compactTableSize(ulong n, ulong sumBody, ulong sq) pure nothrow @nogc @safe
{
    return 30 + 8 * sq + 2 * n + sumBody;
}

/// Thead word 6's high half (SPEC 2g): 0 per-payload Call, 1 uniform Call,
/// 2 uniform Call and length.
enum ulong KIND_PER_PAYLOAD = 0, KIND_UNIFORM = 1, KIND_UNIFORM_FIXED = 2;

/// Exact uniform-call table size (SPEC 2g): Tcall plus a Tindex, or Tcall,
/// Tblen and no Tindex when every body has the same nonzero length.
ulong uniformTableSize(ulong n, ulong sumBody, ulong sq, bool sameLen)
    pure nothrow @nogc @safe
{
    return sameLen && sumBody != 0
        ? 30 + 8 * sq + 2 + sumBody
        : 30 + 8 * sq + 1 + n + sumBody;
}

ulong uniformKind(ulong sumBody, bool sameLen) pure nothrow @nogc @safe
{
    return sameLen && sumBody != 0
        ? KIND_UNIFORM_FIXED : KIND_UNIFORM;
}

ulong tableKindAt(AntFarm* f, ulong seq) nothrow @nogc @system
{
    return atomicLoad!(MemoryOrder.raw)(f.buf[(seq & f.Lmask) + 6]) >> 32;
}

long compactBodyCb(PayloadHeader* head, PayloadBody body)
    nothrow @nogc @system
{
    g_compactLastHead = cast(ulong) cast(void*) head;
    if (head.plen != body.length)
        atomicFetchAdd!(MemoryOrder.raw)(g_compactBad, 1L);
    if (body.length == 0)
    {
        atomicFetchAdd!(MemoryOrder.raw)(g_compactZero, 1L);
        atomicFetchAdd!(MemoryOrder.raw)(g_totalCalls, 1L);
        return 1;
    }
    immutable idx = body[0];
    if (idx >= g_compactN || body.length != g_compactLens[idx])
    {
        atomicFetchAdd!(MemoryOrder.raw)(g_compactBad, 1L);
        atomicFetchAdd!(MemoryOrder.raw)(g_totalCalls, 1L);
        return 1;
    }
    bool ok = true;
    foreach (k, v; body)
        if (v != compactWord(idx, k)) ok = false;
    if (!ok) atomicFetchAdd!(MemoryOrder.raw)(g_compactBad, 1L);
    atomicFetchAdd(g_calls[idx], 1L);
    atomicFetchAdd!(MemoryOrder.raw)(g_totalCalls, 1L);
    return 1;
}

long compactOneCb(PayloadHeader* head, PayloadBody body)
    nothrow @nogc @system
{
    if (head.plen != 1 || body.length != 1)
        atomicFetchAdd!(MemoryOrder.raw)(g_compactBad, 1L);
    atomicFetchAdd(g_calls[body[0]], 1L);
    atomicFetchAdd!(MemoryOrder.raw)(g_totalCalls, 1L);
    return 1;
}

long compactSingle28Cb(PayloadHeader* head, PayloadBody body)
    nothrow @nogc @system
{
    bool ok = head.plen == 28 && body.length == 28 && head.call == &compactSingle28Cb;
    if (ok)
        foreach (k, w; body)
            if (w != k) ok = false;
    if (!ok) atomicFetchAdd!(MemoryOrder.raw)(g_compactBad, 1L);
    atomicFetchAdd(g_calls[0], 1L);
    atomicFetchAdd!(MemoryOrder.raw)(g_totalCalls, 1L);
    return 1;
}



long compactHeaderCb(PayloadHeader* head, PayloadBody body)
    nothrow @nogc @system
{
    if (head.plen != body.length || head.call != &compactHeaderCb)
        atomicFetchAdd!(MemoryOrder.raw)(g_compactHeaderBad, 1L);
    if (body.length > 0)
        atomicFetchAdd(g_calls[body[0]], 1L);
    atomicFetchAdd!(MemoryOrder.raw)(g_totalCalls, 1L);
    return 1;
}

__gshared size_t g_compactWrapN;
__gshared shared(long) g_compactWrapBad;

long compactWrapCb(PayloadHeader* head, PayloadBody body)
    nothrow @nogc @system
{
    if (body.length < 2 || body.length > 3 || head.plen != body.length)
    {
        atomicFetchAdd!(MemoryOrder.raw)(g_compactWrapBad, 1L);
        atomicFetchAdd!(MemoryOrder.raw)(g_totalCalls, 1L);
        return 1;
    }
    immutable idx = body[0];
    if (idx >= g_compactWrapN
        || body[1] != ((cast(ulong) idx) ^ 0xA5A5_A5A5_A5A5_A5A5UL)
        || (body.length == 3 && body[2] != ~cast(ulong) idx))
        atomicFetchAdd!(MemoryOrder.raw)(g_compactWrapBad, 1L);
    if (idx < g_compactWrapN)
        atomicFetchAdd(g_calls[idx], 1L);
    atomicFetchAdd!(MemoryOrder.raw)(g_totalCalls, 1L);
    return 1;
}

void compactCompletion(void* context) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(g_compactCompletions, 1L);
}

// Second Calls with the same checks. Alternating one with its original
// keeps a table's Calls non-uniform, so it stays compact (SPEC 2f) rather
// than uniform (SPEC 2g); g_compactAltCalls shows each record's own Call ran.
__gshared shared(long) g_compactAltCalls;

long compactBodyCbAlt(PayloadHeader* head, PayloadBody body)
    nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(g_compactAltCalls, 1L);
    return compactBodyCb(head, body);
}

long compactOneCbAlt(PayloadHeader* head, PayloadBody body)
    nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(g_compactAltCalls, 1L);
    return compactOneCb(head, body);
}

long compactWrapCbAlt(PayloadHeader* head, PayloadBody body)
    nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(g_compactAltCalls, 1L);
    return compactWrapCb(head, body);
}

long compactHeaderCbAlt(PayloadHeader* head, PayloadBody body)
    nothrow @nogc @system
{
    if (head.call != &compactHeaderCbAlt)
        atomicFetchAdd!(MemoryOrder.raw)(g_compactHeaderBad, 1L);
    atomicFetchAdd!(MemoryOrder.raw)(g_compactAltCalls, 1L);
    if (head.plen != body.length)
        atomicFetchAdd!(MemoryOrder.raw)(g_compactHeaderBad, 1L);
    if (body.length > 0)
        atomicFetchAdd(g_calls[body[0]], 1L);
    atomicFetchAdd!(MemoryOrder.raw)(g_totalCalls, 1L);
    return 1;
}

struct CompactBodies
{
    ulong* words;
    size_t i, n;
    @property bool empty() const nothrow @nogc @safe { return i >= n; }
    @property size_t length() const nothrow @nogc @safe { return n - i; }
    @property CompactBodies save() nothrow @nogc @safe { return this; }
    @property PayloadBody front() nothrow @nogc @system { return words[i .. i + 1]; }
    void popFront() nothrow @nogc @safe { ++i; }
}

/// One all-ST table, drained by one consumer; checks exact geometry,
/// The table kind, per-payload body/length/header and exactly-once.
/// Alternating Calls make it compact (SPEC 2f); one Call makes it uniform
/// (SPEC 2g), index-free when every length matches and is nonzero.
void compactMixedCase(const(size_t)[] lens, uint avgCost, const(char)[] label,
                      bool uniformCall = false)
{
    immutable N = lens.length;
    ulong totalBody;
    size_t zeros;
    bool sameLen = true;
    foreach (l; lens)
    {
        totalBody += l;
        if (l == 0) ++zeros;
        if (l != lens[0]) sameLen = false;
    }

    allocCalls(N);
    scope (exit) freeCalls();
    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    auto bodies = cast(ulong*) malloc((totalBody + 1) * ulong.sizeof);
    scope (exit) { free(headers); free(entries); free(bodies); }
    check(headers !is null && entries !is null && bodies !is null, label);

    g_compactLens = lens.ptr;
    g_compactN = N;
    atomicStore!(MemoryOrder.raw)(g_compactZero, 0L);
    atomicStore!(MemoryOrder.raw)(g_compactBad, 0L);
    atomicStore!(MemoryOrder.raw)(g_compactAltCalls, 0L);

    size_t off;
    foreach (i; 0 .. N)
    {
        headers[i] = PayloadHeader.init;

        headers[i].plen = lens[i];
        headers[i].call = !uniformCall && (i & 1) ? &compactBodyCbAlt : &compactBodyCb;
        auto b = bodies[off .. off + lens[i]];
        foreach (k; 0 .. lens[i]) b[k] = compactWord(i, k);
        entries[i] = PayloadEntry(&headers[i], b);
        off += lens[i];
    }

    auto f = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope (exit) f.destroy();
    ConsumerView v;
    check(v.subscribe(f) == 0, label);
    auto tok = f.registerProducer(Tier.small);

    immutable before = atomicLoad!(MemoryOrder.raw)(f.Wt);
    check(f.write(entries[0 .. N], tok, avgCost) == N, label);
    check(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + (uniformCall
            ? uniformTableSize(N, totalBody, 1, sameLen)
            : compactTableSize(N, totalBody, 1)), label);
    check((atomicLoad!(MemoryOrder.raw)(f.buf[(before & f.Lmask) + 2]) >> 32) == 0, label);
    check(tableKindAt(f, before) == (uniformCall
            ? uniformKind(totalBody, sameLen) : KIND_PER_PAYLOAD), label);

    while (v.consumeNext()) {}
    check(atomicLoad!(MemoryOrder.raw)(g_compactBad) == 0, label);
    check(atomicLoad!(MemoryOrder.raw)(g_compactAltCalls) == (uniformCall ? 0 : N / 2),
        label);
    check(atomicLoad!(MemoryOrder.raw)(g_compactZero) == zeros, label);
    foreach (i; 0 .. N)
        if (lens[i] > 0)
            check(g_calls[i] == 1, label);
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == N, label);

    v.unsubscribe();
    f.unregisterProducer(tok);
}

// Catches M2 (compact sizing miscount), M3 (last Plen off-by-one) and M4
// (non-last Plen off-by-one). One claimed run (12 payloads < chunk 16).
void testCompactMixedBodies()
{
    static immutable size_t[12] lens = [0, 1, 2, 7, 31, 3, 0, 5, 9, 1, 2, 47];
    compactMixedCase(lens[], DEFAULT_AVG_COST, "compact mixed body contract");
    printf("testCompactMixedBodies OK\n"); fflush(stdout);
}

// Catches M7 (non-final run end read from the wrong Tindex entry): chunk 4
// (`avgCostForChunk(4)`), so 40 heterogeneous payloads form 10 claimed runs and nine
// of them take runCompact's `endOff = Tindex[runend]` branch. The last
// payload is zero-length, pinning the Tsize - END_PAD end rule at Plen 0.
void testCompactMultiRunBodies()
{
    static immutable size_t[40] lens = [
        3, 0, 5, 1, 9, 2, 0, 4, 7, 1, 1, 6, 0, 2, 13, 3, 5, 0, 1, 8,
        2, 2, 0, 11, 1, 4, 3, 0, 6, 1, 9, 2, 5, 0, 1, 3, 7, 2, 4, 0];
    compactMixedCase(lens[], avgCostForChunk(4), "compact multi-run body contract");
    printf("testCompactMultiRunBodies OK\n"); fflush(stdout);
}

Thread compactProducerThread(ProdCtx* c)
{
    return new Thread({ producerMain(c); });
}

Thread compactConsumerThread(ConsCtx* c, bool late)
{
    if (late)
        return new Thread({ Thread.sleep(5.msecs); consumerMain(c); });
    return new Thread({ consumerMain(c); });
}

enum WrapLayout { compact, uniform, uniformFixed }

// Exercises compact and uniform Plen derivation (and index-free strides)
// across multi-run tables and ring laps under TSan (backup for M3/M4, U4,
// U5), with a lagging consumer. `compact` alternates two Calls; `uniform`
// shares one Call over 2- and 3-word bodies; `uniformFixed` shares one Call
// over 2-word bodies.
void testCompactWraparound(WrapLayout layout = WrapLayout.compact)
{
    auto f = AntFarm.create(2, 8, 4, 0, 0, 4, 4096);
    scope (exit) f.destroy();

    // Enough payloads to lap the 2 MiB ring twice in every layout.
    immutable size_t N = layout == WrapLayout.compact ? 160000 : 400000;
    enum NP = 3;
    enum NC = 4;
    allocCalls(N);
    scope (exit) freeCalls();
    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 3 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    check(headers !is null && bodies !is null && entries !is null, "compact wrap alloc");

    g_compactWrapN = N;
    atomicStore!(MemoryOrder.raw)(g_compactWrapBad, 0L);
    atomicStore!(MemoryOrder.raw)(g_compactAltCalls, 0L);
    foreach (i; 0 .. N)
    {
        immutable len = layout == WrapLayout.uniform && i % 3 == 0 ? 3 : 2;
        headers[i] = PayloadHeader.init;

        headers[i].plen = len;
        headers[i].call = layout == WrapLayout.compact && (i & 1)
            ? &compactWrapCbAlt : &compactWrapCb;
        bodies[i * 3] = i;
        bodies[i * 3 + 1] = cast(ulong) i ^ 0xA5A5_A5A5_A5A5_A5A5UL;
        bodies[i * 3 + 2] = ~cast(ulong) i;
        entries[i] = PayloadEntry(&headers[i], bodies[i * 3 .. i * 3 + len]);
    }

    auto deadline = MonoTime.currTime + 120.seconds;
    Thread[NC] consumers;
    ConsCtx[NC] cctx;
    foreach (j; 0 .. NC)
    {
        cctx[j] = ConsCtx(f, N, deadline);
        consumers[j] = compactConsumerThread(&cctx[j], j == NC - 1);
        consumers[j].start();
    }

    Thread[NP] producers;
    ProdCtx[NP] pctx;
    size_t cursor;
    foreach (j; 0 .. NP)
    {
        immutable remain = N - cursor;
        immutable share = remain / (NP - j);
        pctx[j] = ProdCtx(f, entries + cursor, share, Tier.small);
        cursor += share;
        producers[j] = compactProducerThread(&pctx[j]);
        producers[j].start();
    }
    foreach (t; producers) t.join();
    foreach (t; consumers) t.join();

    check(atomicLoad!(MemoryOrder.raw)(f.Wt) > 2 * f.Ln,
        "compact wraparound laps the ring");
    check(atomicLoad!(MemoryOrder.raw)(g_compactWrapBad) == 0,
        "compact wraparound body and header contract");
    foreach (i; 0 .. N)
        check(g_calls[i] == 1, "compact wraparound exact call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == N, "compact wraparound total");
    check(atomicLoad!(MemoryOrder.raw)(g_compactAltCalls)
            == (layout == WrapLayout.compact ? N / 2 : 0),
        "compact wraparound ran each record's own Call");
    printf("testCompactWraparound(%s) OK\n",
        layout == WrapLayout.compact ? "compact".ptr
            : layout == WrapLayout.uniform ? "uniform".ptr : "uniformFixed".ptr);
    fflush(stdout);
}

// Catches M2 (writer miscounts compact size) and U2 (writer mis-sizes an
// index-free table), and verifies the accepted prefix is prepared/emitted
// exactly; zero admission leaves Wt untouched. The heterogeneous prefix
// alternates Calls (compact, SPEC 2f); writeTracked and the broadcast
// overload share one Call (uniform, SPEC 2g).
void testCompactPartialAdmission()
{
    enum N = 20;
    enum K = 10;
    enum Q = 68; // compact table of 10 one-word bodies: 38 + 3*10.
    // Uniform table of 10 one-word bodies: 38 + 2 + 10 index-free.
    immutable QU = uniformTableSize(K, K, 1, true);

    // Heterogeneous accepted prefix.
    {
        allocCalls(N);
        scope (exit) freeCalls();
        auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
        auto bodies = cast(ulong*) malloc(N * ulong.sizeof);
        auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
        scope (exit) { free(headers); free(bodies); free(entries); }
        atomicStore!(MemoryOrder.raw)(g_compactBad, 0L);
        foreach (i; 0 .. N)
        {
            headers[i] = PayloadHeader.init;

            headers[i].plen = 1;
            headers[i].call = (i & 1) ? &compactOneCbAlt : &compactOneCb;
            bodies[i] = i;
            entries[i] = PayloadEntry(&headers[i], bodies[i .. i + 1]);
        }
        auto f = AntFarm.create(2, 8, 1, 0, 0, 1, Q);
        scope (exit) f.destroy();
        ConsumerView v;
        check(v.subscribe(f) == 0, "compact partial subscribe");
        auto tok = f.registerProducer(Tier.small);
        immutable before = atomicLoad!(MemoryOrder.raw)(f.Wt);
        atomicStore!(MemoryOrder.raw)(g_compactCompletions, 0L);
        TableCompletionHook hook = TableCompletionHook(null, &compactCompletion);
        check(f.write(entries[0 .. N], tok, DEFAULT_AVG_COST, &hook) == K, "compact partial admits exactly k");
        check(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + Q,
            "compact partial exact Wt advance");
        while (v.consumeNext()) {}
        foreach (i; 0 .. K) check(g_calls[i] == 1, "compact partial prefix ran");
        foreach (i; K .. N) check(g_calls[i] == 0, "compact partial suffix not run");
        check(atomicLoad!(MemoryOrder.raw)(g_compactCompletions) == 1,
            "mixed-callback accepted prefix invokes one hook");
        check(atomicLoad!(MemoryOrder.raw)(g_compactBad) == 0, "compact partial body");
        v.unsubscribe();
        f.unregisterProducer(tok);
    }

    // writeTracked with a completion hook.
    {
        allocCalls(N);
        scope (exit) freeCalls();
        static immutable size_t[N] ones = 1;
        g_compactLens = ones.ptr;
        g_compactN = N;
        atomicStore!(MemoryOrder.raw)(g_compactZero, 0L);
        atomicStore!(MemoryOrder.raw)(g_compactBad, 0L);
        ulong[N] words;
        foreach (i; 0 .. N) words[i] = i;
        auto f = AntFarm.create(2, 8, 1, 0, 0, 1, QU);
        scope (exit) f.destroy();
        ConsumerView v;
        check(v.subscribe(f) == 0, "compact tracked subscribe");
        auto tok = f.registerProducer(Tier.small);
        PayloadHeader h = PayloadHeader.init;

        h.plen = 1;
        h.call = &compactBodyCb;
        atomicStore!(MemoryOrder.raw)(g_compactCompletions, 0L);
        TableCompletionHook hook = TableCompletionHook(null, &compactCompletion);
        immutable before = atomicLoad!(MemoryOrder.raw)(f.Wt);
        immutable got = f.writeTracked(h, CompactBodies(words.ptr, 0, N), 1, &hook, tok);
        check(got == K, "compact tracked partial admits exactly k");
        check(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + QU,
            "compact tracked exact Wt advance");
        check(tableKindAt(f, before) == uniformKind(K, true), "tracked table is uniform");
        while (v.consumeNext()) {}
        check(atomicLoad!(MemoryOrder.raw)(g_compactCompletions) == 1,
            "compact tracked completion hook once");
        check(atomicLoad!(MemoryOrder.raw)(g_compactBad) == 0, "compact tracked body");
        v.unsubscribe();
        f.unregisterProducer(tok);
    }

    // Transactional prepareTableFrontN and zero admission.
    {
        allocCalls(N);
        scope (exit) freeCalls();
        ulong[N] words;
        foreach (i; 0 .. N) words[i] = i;
        size_t frontCalls, prepareCalls, prepared;
        bool permit = true;
        auto prep = PreparingBodyRange(words.ptr, 0, N, &frontCalls, &prepareCalls, &prepared, &permit);
        auto f = AntFarm.create(2, 8, 1, 0, 0, 1, QU);
        scope (exit) f.destroy();
        ConsumerView v;
        check(v.subscribe(f) == 0, "compact transactional subscribe");
        auto tok = f.registerProducer(Tier.small);
        PayloadHeader h = PayloadHeader.init;

        h.plen = 1;
        h.call = &compactOneCb;
        immutable before = atomicLoad!(MemoryOrder.raw)(f.Wt);
        immutable got = f.write(h, prep, 1, tok);
        check(got == K, "compact transactional partial admits exactly k");
        check(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + QU,
            "compact transactional exact Wt advance");
        check(prepared == K && prepareCalls == 1,
            "compact transactional prepared only the accepted prefix");
        while (v.consumeNext()) {}

        size_t rejectFrontCalls, rejectPrepareCalls, rejectPrepared;
        bool reject = false;
        auto rej = PreparingBodyRange(words.ptr, 0, 1, &rejectFrontCalls,
            &rejectPrepareCalls, &rejectPrepared, &reject);
        immutable beforeReject = atomicLoad!(MemoryOrder.raw)(f.Wt);
        check(f.write(h, rej, 1, tok) == 0, "compact transactional zero admission");
        check(atomicLoad!(MemoryOrder.raw)(f.Wt) == beforeReject,
            "compact transactional zero admission leaves Wt untouched");
        check(rejectPrepareCalls == 1 && rejectPrepared == 0 && rejectFrontCalls == 0,
            "compact transactional rejected prepare emitted nothing");
        v.unsubscribe();
        f.unregisterProducer(tok);
    }

    printf("testCompactPartialAdmission OK\n"); fflush(stdout);
}

// Catches M2 (compact sizing off-by-one) at the quota boundary.
void testCompactQuotaBoundaries()
{
    enum Q = 68;

    // Exact table: 10 one-word bodies -> 38 + 3*10 = 68.
    {
        allocCalls(10);
        scope (exit) freeCalls();
        auto headers = cast(PayloadHeader*) malloc(10 * PayloadHeader.sizeof);
        auto bodies = cast(ulong*) malloc(10 * ulong.sizeof);
        auto entries = cast(PayloadEntry*) malloc(10 * PayloadEntry.sizeof);
        scope (exit) { free(headers); free(bodies); free(entries); }
        atomicStore!(MemoryOrder.raw)(g_compactBad, 0L);
        foreach (i; 0 .. 10)
        {
            headers[i] = PayloadHeader.init;

            headers[i].plen = 1;
            headers[i].call = (i & 1) ? &compactOneCbAlt : &compactOneCb;
            bodies[i] = i;
            entries[i] = PayloadEntry(&headers[i], bodies[i .. i + 1]);
        }
        auto f = AntFarm.create(2, 8, 1, 0, 0, 1, Q);
        scope (exit) f.destroy();
        ConsumerView v;
        check(v.subscribe(f) == 0, "compact boundary subscribe");
        auto tok = f.registerProducer(Tier.small);
        immutable before = atomicLoad!(MemoryOrder.raw)(f.Wt);
        check(f.write(entries[0 .. 10], tok) == 10, "compact exact table admitted");
        check(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + Q, "compact exact table size");
        while (v.consumeNext()) {}
        check(atomicLoad!(MemoryOrder.raw)(g_compactBad) == 0, "compact exact table bodies");
        foreach (i; 0 .. 10) check(g_calls[i] == 1, "compact exact table calls");
        v.unsubscribe();
        f.unregisterProducer(tok);
    }

    // One word over: 11 one-word bodies -> 71 > 68, truncated to 10.
    {
        allocCalls(11);
        scope (exit) freeCalls();
        auto headers = cast(PayloadHeader*) malloc(11 * PayloadHeader.sizeof);
        auto bodies = cast(ulong*) malloc(11 * ulong.sizeof);
        auto entries = cast(PayloadEntry*) malloc(11 * PayloadEntry.sizeof);
        scope (exit) { free(headers); free(bodies); free(entries); }
        atomicStore!(MemoryOrder.raw)(g_compactBad, 0L);
        foreach (i; 0 .. 11)
        {
            headers[i] = PayloadHeader.init;

            headers[i].plen = 1;
            headers[i].call = (i & 1) ? &compactOneCbAlt : &compactOneCb;
            bodies[i] = i;
            entries[i] = PayloadEntry(&headers[i], bodies[i .. i + 1]);
        }
        auto f = AntFarm.create(2, 8, 1, 0, 0, 1, Q);
        scope (exit) f.destroy();
        ConsumerView v;
        check(v.subscribe(f) == 0, "compact boundary subscribe 2");
        auto tok = f.registerProducer(Tier.small);
        immutable before = atomicLoad!(MemoryOrder.raw)(f.Wt);
        check(f.write(entries[0 .. 11], tok) == 10, "compact over-quota table truncated");
        check(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + Q, "compact truncated size");
        while (v.consumeNext()) {}
        check(atomicLoad!(MemoryOrder.raw)(g_compactBad) == 0, "compact truncated bodies");
        foreach (i; 0 .. 10) check(g_calls[i] == 1, "compact truncated calls");
        check(g_calls[10] == 0, "compact truncated payload not run");
        v.unsubscribe();
        f.unregisterProducer(tok);
    }

    // Exact-quota compact singleton: body 28 -> 38 + 2 + 28 = 68.
    {
        allocCalls(1);
        scope (exit) freeCalls();
        ulong[28] body;
        PayloadHeader h = PayloadHeader.init;

        h.plen = 28;
        h.call = &compactSingle28Cb;
        foreach (i; 0 .. 28) body[i] = i;
        PayloadEntry[1] entries = [PayloadEntry(&h, body[])];
        atomicStore!(MemoryOrder.raw)(g_compactBad, 0L);
        auto f = AntFarm.create(2, 8, 1, 0, 0, 1, Q);
        scope (exit) f.destroy();
        ConsumerView v;
        check(v.subscribe(f) == 0, "compact singleton subscribe");
        auto tok = f.registerProducer(Tier.small);
        immutable before = atomicLoad!(MemoryOrder.raw)(f.Wt);
        check(f.write(entries[], tok) == 1, "compact exact-quota singleton admitted");
        check(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + Q, "compact singleton size");
        while (v.consumeNext()) {}
        check(atomicLoad!(MemoryOrder.raw)(g_compactBad) == 0 && g_calls[0] == 1,
            "compact exact-quota singleton body and header");
        v.unsubscribe();
        f.unregisterProducer(tok);
    }
    printf("testCompactQuotaBoundaries OK\n"); fflush(stdout);
}

// Checks the borrowed callback header under one and several consumers.
void testCompactCallbackHeader()
{
    enum N = 32;
    // Rounds 0/1: alternating Calls (compact); rounds 2/3: one Call
    // (uniform, SPEC 2g), whose scratch header carries Tcall.
    foreach (round; 0 .. 4)
    {
        immutable uniformCall = round >= 2;
        allocCalls(N);
        scope (exit) freeCalls();
        auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
        auto bodies = cast(ulong*) malloc(N * ulong.sizeof);
        auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
        scope (exit) { free(headers); free(bodies); free(entries); }
        atomicStore!(MemoryOrder.raw)(g_compactHeaderBad, 0L);
        atomicStore!(MemoryOrder.raw)(g_compactAltCalls, 0L);
        foreach (i; 0 .. N)
        {
            headers[i] = PayloadHeader.init;

            headers[i].plen = 1;
            headers[i].call = !uniformCall && (i & 1)
                ? &compactHeaderCbAlt : &compactHeaderCb;
            bodies[i] = i;
            entries[i] = PayloadEntry(&headers[i], bodies[i .. i + 1]);
        }
        auto f = AntFarm.create(2, 8, (round & 1) == 0 ? 1 : 4, 0, 0, 1, 4096);
        scope (exit) f.destroy();
        auto tok = f.registerProducer(Tier.small);
        if ((round & 1) == 0)
        {
            ConsumerView v;
            check(v.subscribe(f) == 0, "compact header single subscribe");
            check(f.write(entries[0 .. N], tok) == N, "compact header single write");
            while (v.consumeNext()) {}
            v.unsubscribe();
        }
        else
        {
            enum NC = 4;
            Thread[NC] threads;
            ConsCtx[NC] ctx;
            check(f.write(entries[0 .. N], tok) == N, "compact header multi write");
            foreach (j; 0 .. NC)
            {
                ctx[j] = ConsCtx(f, N, MonoTime.currTime + 60.seconds);
                threads[j] = compactConsumerThread(&ctx[j], false);
                threads[j].start();
            }
            foreach (t; threads) t.join();
        }
        check(atomicLoad!(MemoryOrder.raw)(g_compactHeaderBad) == 0,
            "compact callback header contract");
        foreach (i; 0 .. N)
            check(g_calls[i] == 1, "compact header exact call count");
        check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == N, "compact header total");
        check(atomicLoad!(MemoryOrder.raw)(g_compactAltCalls) == (uniformCall ? 0 : N / 2),
            "compact header each record's own Call");
        f.unregisterProducer(tok);
    }
    printf("testCompactCallbackHeader OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Uniform-call ST tables (SPEC 2g)
// ---------------------------------------------------------------------

/// Bodies for `lens`, word k of payload i = compactWord(i, k), plus one
/// header per payload whose Call is `calls[i]`. Owned by the caller.
struct UniformCase
{
    PayloadHeader* headers;
    PayloadEntry* entries;
    ulong* words;
    ulong totalBody;

    void build(const(size_t)[] lens, const(Callback)[] calls)
    {
        foreach (l; lens) totalBody += l;
        headers = cast(PayloadHeader*) malloc(lens.length * PayloadHeader.sizeof);
        entries = cast(PayloadEntry*) malloc(lens.length * PayloadEntry.sizeof);
        words = cast(ulong*) malloc((totalBody + 1) * ulong.sizeof);
        check(headers !is null && entries !is null && words !is null, "uniform case alloc");
        size_t off;
        foreach (i, l; lens)
        {
            headers[i] = PayloadHeader.init;

            headers[i].plen = l;
            headers[i].call = calls[i];
            foreach (k; 0 .. l) words[off + k] = compactWord(i, k);
            entries[i] = PayloadEntry(&headers[i], words[off .. off + l]);
            off += l;
        }
    }

    void release() nothrow @nogc @system
    {
        free(headers);
        free(entries);
        free(words);
    }
}

/// Write `entries` as one table on a fresh one-consumer farm, check the
/// expected kind and exact size, drain it, and check bodies, the scratch
/// header and that every payload ran its own Call exactly once.
void uniformDetectCase(const(size_t)[] lens, const(Callback)[] calls,
                       ulong kind, ulong size, const(char)[] label)
{
    immutable N = lens.length;
    UniformCase c;
    c.build(lens, calls);
    scope (exit) c.release();
    size_t alts, zeros;
    foreach (i; 0 .. N)
    {
        if (calls[i] == &compactBodyCbAlt) ++alts;
        if (lens[i] == 0) ++zeros;
    }
    allocCalls(N);
    scope (exit) freeCalls();
    g_compactLens = lens.ptr;
    g_compactN = N;
    atomicStore!(MemoryOrder.raw)(g_compactZero, 0L);
    atomicStore!(MemoryOrder.raw)(g_compactBad, 0L);
    atomicStore!(MemoryOrder.raw)(g_compactAltCalls, 0L);

    auto f = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope (exit) f.destroy();
    ConsumerView v;
    check(v.subscribe(f) == 0, label);
    auto tok = f.registerProducer(Tier.small);
    immutable before = atomicLoad!(MemoryOrder.raw)(f.Wt);
    check(f.write(c.entries[0 .. N], tok) == N, label);
    check(tableKindAt(f, before) == kind, label);
    check(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + size, label);
    check((atomicLoad!(MemoryOrder.raw)(f.buf[(before & f.Lmask) + 2]) >> 32) == 0, label);
    if (kind != KIND_PER_PAYLOAD)
        check(atomicLoad!(MemoryOrder.raw)(f.buf[(before & f.Lmask) + 8])
                == cast(ulong) cast(void*) calls[0], label);
    if (kind == KIND_UNIFORM_FIXED)
        check(atomicLoad!(MemoryOrder.raw)(f.buf[(before & f.Lmask) + 9]) == lens[0], label);
    while (v.consumeNext()) {}
    check(atomicLoad!(MemoryOrder.raw)(g_compactBad) == 0, label);
    check(atomicLoad!(MemoryOrder.raw)(g_compactZero) == zeros, label);
    check(atomicLoad!(MemoryOrder.raw)(g_compactAltCalls) == alts, label);
    foreach (i; 0 .. N)
        if (lens[i] > 0)
            check(g_calls[i] == 1, label);
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == N, label);
    v.unsubscribe();
    f.unregisterProducer(tok);
}

// Runtime detection on PayloadEntry ranges. Catches U1 (reader treats a
// non-uniform table as uniform), U3 (writer ignores a Call mismatch), U4
// (writer ignores a length mismatch) and U2 (index-free mis-sizing).
void testUniformDetection()
{
    alias A = compactBodyCb;
    alias B = compactBodyCbAlt;
    static immutable size_t[8] ones = 1;
    static immutable size_t[8] threes = 3;
    static immutable size_t[8] alt12 = [1, 2, 1, 2, 1, 2, 1, 2];
    static immutable size_t[8] lastLong = [1, 1, 1, 1, 1, 1, 1, 2];
    static immutable size_t[4] zeros = 0;
    Callback[8] same = &A;
    Callback[8] lastOther = &A;
    lastOther[7] = &B;
    Callback[8] firstOther = &A;
    firstOther[0] = &B;
    Callback[8] alternating;
    foreach (i; 0 .. 8) alternating[i] = (i & 1) ? &B : &A;

    uniformDetectCase(ones[], same[], uniformKind(8, true),
        uniformTableSize(8, 8, 1, true), "uniform one-word Call and length");
    uniformDetectCase(threes[], same[], uniformKind(24, true),
        uniformTableSize(8, 24, 1, true), "uniform three-word Call and length");
    uniformDetectCase(alt12[], same[], KIND_UNIFORM,
        uniformTableSize(8, 12, 1, false), "uniform Call, mixed lengths");
    uniformDetectCase(lastLong[], same[], KIND_UNIFORM,
        uniformTableSize(8, 9, 1, false), "uniform Call, last length differs");
    uniformDetectCase(zeros[], same[0 .. 4], KIND_UNIFORM,
        uniformTableSize(4, 0, 1, true), "uniform Call, zero-length bodies stay indexed");
    uniformDetectCase(ones[], alternating[], KIND_PER_PAYLOAD,
        compactTableSize(8, 8, 1), "alternating Calls stay compact");
    uniformDetectCase(ones[], lastOther[], KIND_PER_PAYLOAD,
        compactTableSize(8, 8, 1), "last Call differs: compact");
    uniformDetectCase(ones[], firstOther[], KIND_PER_PAYLOAD,
        compactTableSize(8, 8, 1), "first Call differs: compact");
    printf("testUniformDetection OK\n"); fflush(stdout);
}

// Explicit forms: broadcast without a length detects the length at run
// time, broadcast with a length and writeTracked are uniform by
// construction, and pairPayloads detects the Call at run time.
void testUniformExplicitForms()
{
    enum N = 12;
    static immutable size_t[N] ones = 1;
    static immutable size_t[N] alt12 = [1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2];
    allocCalls(N);
    scope (exit) freeCalls();
    auto f = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope (exit) f.destroy();
    ConsumerView v;
    check(v.subscribe(f) == 0, "uniform forms subscribe");
    auto tok = f.registerProducer(Tier.small);
    PayloadHeader h = PayloadHeader.init;

    h.call = &compactBodyCb;
    atomicStore!(MemoryOrder.raw)(g_compactBad, 0L);
    atomicStore!(MemoryOrder.raw)(g_compactZero, 0L);

    ulong[2 * N] words;
    PayloadBody[N] oneBodies, mixedBodies;
    size_t off;
    foreach (i; 0 .. N)
    {
        words[off] = i;
        if (alt12[i] == 2) words[off + 1] = compactWord(i, 1);
        mixedBodies[i] = words[off .. off + alt12[i]];
        oneBodies[i] = words[off .. off + 1];
        off += alt12[i];
    }

    void expect(ulong kind, ulong size, const(size_t)[] lens, ulong before,
                const(char)[] label)
    {
        check(tableKindAt(f, before) == kind, label);
        check(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + size, label);
        g_compactLens = lens.ptr;
        g_compactN = N;
        while (v.consumeNext()) {}
        check(atomicLoad!(MemoryOrder.raw)(g_compactBad) == 0, label);
    }

    ulong before = atomicLoad!(MemoryOrder.raw)(f.Wt);
    check(f.write(h, oneBodies[], tok) == N, "broadcast one-word");
    expect(uniformKind(N, true), uniformTableSize(N, N, 1, true), ones[], before,
        "broadcast without length detects a uniform length");

    before = atomicLoad!(MemoryOrder.raw)(f.Wt);
    check(f.write(h, mixedBodies[], tok) == N, "broadcast mixed");
    expect(KIND_UNIFORM, uniformTableSize(N, 18, 1, false), alt12[], before,
        "broadcast without length, mixed lengths, stays indexed");

    before = atomicLoad!(MemoryOrder.raw)(f.Wt);
    check(f.write(h, oneBodies[], 1, tok) == N, "broadcast fixed");
    expect(uniformKind(N, true), uniformTableSize(N, N, 1, true), ones[], before,
        "broadcast with length");

    TableCompletionHook hook = TableCompletionHook(null, &compactCompletion);
    atomicStore!(MemoryOrder.raw)(g_compactCompletions, 0L);
    before = atomicLoad!(MemoryOrder.raw)(f.Wt);
    check(f.writeTracked(h, oneBodies[], 1, &hook, tok) == N, "tracked");
    expect(uniformKind(N, true), uniformTableSize(N, N, 1, true), ones[], before,
        "writeTracked");
    check(atomicLoad!(MemoryOrder.raw)(g_compactCompletions) == 1, "uniform tracked hook once");
    check(atomicLoad!(MemoryOrder.raw)(f.buf[(before & f.Lmask) + 7])
            == cast(ulong) cast(void*) &hook, "uniform tracked hook word");

    PayloadHeader[N] hs = h;
    before = atomicLoad!(MemoryOrder.raw)(f.Wt);
    check(f.write(hs[], oneBodies[], tok) == N, "pair");
    expect(uniformKind(N, true), uniformTableSize(N, N, 1, true), ones[], before,
        "pairPayloads detects a uniform Call");

    hs[5].call = &compactBodyCbAlt;
    before = atomicLoad!(MemoryOrder.raw)(f.Wt);
    check(f.write(hs[], oneBodies[], tok) == N, "pair mixed");
    expect(KIND_PER_PAYLOAD, compactTableSize(N, N, 1), ones[], before,
        "pairPayloads with two Calls stays compact");

    foreach (i; 0 .. N) check(g_calls[i] == 6, "uniform forms exact calls");
    v.unsubscribe();
    f.unregisterProducer(tok);
    printf("testUniformExplicitForms OK\n"); fflush(stdout);
}

// Catches U5 (index-free stride or payload-area offset wrong), U6 (uniform
// last Plen off by one) and U9 (scratch header Plen/Call not set): multi-run
// tables in both uniform layouts, each claimed run starting mid-table.
void testUniformMultiRunBodies()
{
    static immutable size_t[40] lens = [
        3, 0, 5, 1, 9, 2, 0, 4, 7, 1, 1, 6, 0, 2, 13, 3, 5, 0, 1, 8,
        2, 2, 0, 11, 1, 4, 3, 0, 6, 1, 9, 2, 5, 0, 1, 3, 7, 2, 4, 1];
    compactMixedCase(lens[], avgCostForChunk(4), "uniform multi-run body contract", true);
    static immutable size_t[12] mixed = [0, 1, 2, 7, 31, 3, 0, 5, 9, 1, 2, 47];
    compactMixedCase(mixed[], DEFAULT_AVG_COST, "uniform single-run body contract", true);
    static immutable size_t[300] threes = 3;
    compactMixedCase(threes[], avgCostForChunk(4), "uniform fixed multi-run (chunk 4)", true);
    compactMixedCase(threes[], 0, "uniform fixed multi-run (chunk 128)", true);
    static immutable size_t[37] sevens = 7;
    compactMixedCase(sevens[], avgCostForChunk(8), "uniform fixed partial last run", true);
    printf("testUniformMultiRunBodies OK\n"); fflush(stdout);
}

// A prefix of a uniform range is uniform: the accepted prefix keeps the
// layout it was sized with when the next payload would degrade it and no
// longer fits. Catches U8 (the writer takes the rejected candidate's
// layout). Also zero admission on a full ring leaves Wt untouched.
void testUniformPartialAdmission()
{
    enum K = 10;
    // Ten one-word bodies, index-free: 38 + 2 + 10 (indexed: 38 + 1 + 20).
    immutable Q = uniformTableSize(K, K, 1, true);
    static immutable size_t[K + 1] onesThenOne = 1;
    static immutable size_t[K + 1] onesThenTwo = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2];
    Callback[K + 1] same = &compactBodyCb;
    Callback[K + 1] lastOther = &compactBodyCb;
    lastOther[K] = &compactBodyCbAlt;

    foreach (round; 0 .. 2)
    {
        const(size_t)[] lens = round == 0 ? onesThenOne[] : onesThenTwo[];
        const(Callback)[] calls = round == 0 ? lastOther[] : same[];
        immutable label = round == 0 ? "uniform prefix before a Call change"
                                     : "uniform prefix before a length change";
        UniformCase c;
        c.build(lens, calls);
        scope (exit) c.release();
        allocCalls(K + 1);
        scope (exit) freeCalls();
        g_compactLens = lens.ptr;
        g_compactN = K + 1;
        atomicStore!(MemoryOrder.raw)(g_compactBad, 0L);
        atomicStore!(MemoryOrder.raw)(g_compactAltCalls, 0L);
        auto f = AntFarm.create(2, 8, 1, 0, 0, 1, Q);
        scope (exit) f.destroy();
        ConsumerView v;
        check(v.subscribe(f) == 0, label);
        auto tok = f.registerProducer(Tier.small);
        immutable before = atomicLoad!(MemoryOrder.raw)(f.Wt);
        check(f.write(c.entries[0 .. K + 1], tok) == K, label);
        check(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + Q, label);
        check(tableKindAt(f, before) == uniformKind(K, true), label);
        while (v.consumeNext()) {}
        foreach (i; 0 .. K) check(g_calls[i] == 1, label);
        check(g_calls[K] == 0, label);
        // The rest goes out as its own (uniform singleton) table.
        immutable before2 = atomicLoad!(MemoryOrder.raw)(f.Wt);
        check(f.write(c.entries[K .. K + 1], tok) == 1, label);
        check(tableKindAt(f, before2) == uniformKind(lens[K], true), label);
        while (v.consumeNext()) {}
        check(g_calls[K] == 1, label);
        check(atomicLoad!(MemoryOrder.raw)(g_compactBad) == 0, label);
        check(atomicLoad!(MemoryOrder.raw)(g_compactAltCalls) == (round == 0 ? 1 : 0), label);
        v.unsubscribe();
        f.unregisterProducer(tok);
    }

    // Zero admission: a parked consumer pins the ring, so uniform writes
    // stop with write() == 0 and Wt unchanged; draining releases them.
    {
        enum N = 4096;
        static immutable size_t[1] one = [1];
        g_compactLens = one.ptr;
        allocCalls(N);
        scope (exit) freeCalls();
        auto f = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
        scope (exit) f.destroy();
        ConsumerView v;
        check(v.subscribe(f) == 0, "uniform zero admission subscribe");
        auto tok = f.registerProducer(Tier.small);
        PayloadHeader h = PayloadHeader.init;

        h.call = &compactOneCb;
        ulong[N] words;
        foreach (i; 0 .. N) words[i] = i;
        auto bodies = CompactBodies(words.ptr, 0, N);
        size_t written, zeroes;
        bool sawZero;
        for (size_t round = 0; written < N * 100 && !sawZero; ++round)
        {
            auto b = bodies.save;
            immutable wtBefore = atomicLoad!(MemoryOrder.raw)(f.Wt);
            immutable got = f.write(h, b, 1, tok);
            if (got == 0)
            {
                check(atomicLoad!(MemoryOrder.raw)(f.Wt) == wtBefore,
                    "uniform zero admission leaves Wt untouched");
                sawZero = true;
            }
            written += got;
        }
        check(sawZero, "uniform writes fill a pinned ring");
        atomicStore!(MemoryOrder.raw)(g_totalCalls, 0L);
        while (v.consumeNext()) {}
        check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == written,
            "uniform zero admission: every admitted payload ran");
        check(f.write(h, bodies.save, 1, tok) > 0, "uniform write after drain");
        while (v.consumeNext()) {}
        v.unsubscribe();
        f.unregisterProducer(tok);
    }
    printf("testUniformPartialAdmission OK\n"); fflush(stdout);
}

// Catches U2 and U4 sizing off-by-ones at the quota boundary, for both
// uniform layouts.
void testUniformQuotaBoundaries()
{
    static immutable size_t[11] ones = 1;
    static immutable size_t[11] alt12 = [1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1];
    Callback[11] same = &compactBodyCb;
    foreach (round; 0 .. 2)
    {
        const(size_t)[] lens = round == 0 ? ones[] : alt12[];
        immutable sameLen = round == 0;
        immutable Q = uniformTableSize(10, round == 0 ? 10 : 15, 1, sameLen);
        foreach (n; 10 .. 12)
        {
            immutable label = round == 0 ? "uniform fixed quota boundary"
                                         : "uniform indexed quota boundary";
            UniformCase c;
            c.build(lens[0 .. n], same[0 .. n]);
            scope (exit) c.release();
            allocCalls(n);
            scope (exit) freeCalls();
            g_compactLens = lens.ptr;
            g_compactN = n;
            atomicStore!(MemoryOrder.raw)(g_compactBad, 0L);
            auto f = AntFarm.create(2, 8, 1, 0, 0, 1, Q);
            scope (exit) f.destroy();
            ConsumerView v;
            check(v.subscribe(f) == 0, label);
            auto tok = f.registerProducer(Tier.small);
            immutable before = atomicLoad!(MemoryOrder.raw)(f.Wt);
            check(f.write(c.entries[0 .. n], tok) == 10, label);
            check(atomicLoad!(MemoryOrder.raw)(f.Wt) == before + Q, label);
            check(tableKindAt(f, before) == (sameLen ? uniformKind(10, true) : KIND_UNIFORM),
                label);
            while (v.consumeNext()) {}
            foreach (i; 0 .. 10) check(g_calls[i] == 1, label);
            if (n == 11) check(g_calls[10] == 0, label);
            check(atomicLoad!(MemoryOrder.raw)(g_compactBad) == 0, label);
            v.unsubscribe();
            f.unregisterProducer(tok);
        }
    }
    printf("testUniformQuotaBoundaries OK\n"); fflush(stdout);
}

void testCanFit()
{
    auto f = AntFarm.create(2, 4, 9, 1, 2048, 1, 512);
    scope(exit) f.destroy();
    PayloadHeader header;
    initPayloadHeader!testCb(&header);
    foreach (tier; [Tier.small, Tier.bulk])
    {
        immutable quota = tier == Tier.small ? 512UL : 2048UL;
        check(f.canFit(header, quota - 40, tier), "canFit exact boundary");
        check(!f.canFit(header, quota - 39, tier), "canFit one word over");
        foreach(words; [ulong.max, ulong.max - 16, ulong.max - 40])
            check(!f.canFit(header, words, tier), "canFit overflow");
        header.call = null;
        check(!f.canFit(header, 0, tier), "canFit null callback");
        initPayloadHeader!testCb(&header);
    }
    ConsumerView[9] views;
    foreach(ref view; views) check(view.subscribe(f) >= 0, "canFit subscribe");
    check(f.canFit(header, 456, Tier.small), "canFit grown consumer boundary");
    check(!f.canFit(header, 457, Tier.small), "canFit grown consumer overflow");
    foreach(ref view; views) view.unsubscribe();
    check(f.canFit(header, 472, Tier.small), "canFit after consumer departure");
    printf("testCanFit OK\n");
}

void main(string[] args)
{
    if (args.length > 1 && args[1] == "join-only")
    {
        testSameShardPlainWriteJoin();
        return;
    }
    if (args.length > 1 && args[1] == "stale-quota-only")
    {
        testStaleQuotaReuse();
        return;
    }
    if (args.length > 1 && args[1] == "compact-only")
    {
        testCompactMixedBodies();
        testCompactMultiRunBodies();
        testCompactWraparound();
        testCompactPartialAdmission();
        testCompactQuotaBoundaries();

        testCompactCallbackHeader();

        testUniformDetection();
        testUniformExplicitForms();
        testUniformMultiRunBodies();
        testCompactWraparound(WrapLayout.uniform);
        testCompactWraparound(WrapLayout.uniformFixed);
        testUniformPartialAdmission();
        testUniformQuotaBoundaries();
        printf("ALL COMPACT TESTS PASSED\n");
        return;
    }
    testMagicWrap();
    testRingSizeUnits();
    testCheckGeometry();
    testTryCreate();
    testArithmetic();
    testCanFit();
    testSweptQuotaBalance();
    testProducerLifecycleChurn();
    testSingleThreaded();
    testInputRangeWrite();
    testPayloadRange();
    testUniformRanges();
    testSeparateRanges();
    testConcurrent();
    testWraparound();
    testSubscriptionCap();
    testChurn(3);
    testChurn(1);
    testBacklog();
    testSpannedTables();
    testSmallTableChurn();
    testMultiSmallProducers();
    testTokenMoveOnly();
    testCrossFarmProvenance();
    testStaleQuotaReuse();
    testActorWave();
    testRuntimeQuiescent();
    testActorPayload();
    testActorErasedAdapter();
    testActorInbox();
    testSameShardPlainWriteJoin();
    testChunkBoundariesPerClass();
    testDefaultAvgCostChunk();
    testCompactMixedBodies();
    testCompactMultiRunBodies();
    testCompactWraparound();
    testCompactPartialAdmission();
    testCompactQuotaBoundaries();

    testCompactCallbackHeader();

    testUniformDetection();
    testUniformExplicitForms();
    testUniformMultiRunBodies();
    testCompactWraparound(WrapLayout.uniform);
    testCompactWraparound(WrapLayout.uniformFixed);
    testUniformPartialAdmission();
    testUniformQuotaBoundaries();
    printf("ALL TESTS PASSED\n");
}
