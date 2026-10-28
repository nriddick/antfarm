// Deterministic subscription schedules for the Live lifecycle (SPEC 3 I3,
// 6b, 6c, 6e). Each schedule holds actors at write-audit hooks, either
// inline on the main thread or, where a producer must stay paused inside its
// own write while others run, on a worker thread released by flags. A to I
// and P failed on some earlier protocol or prototype (SPEC 11); J1 to J3, Q,
// K, X, L and LN were added by the audit of the implementation and each
// fails on an ablation of the defect it targets (docs/design/farm-live-lifecycle.md).
//
// Oracles, per schedule:
//   safety     no producer enters the next lap of a slot whose work is still
//              unpublished or unexecuted (checked where the schedule can do it);
//   identity   each table carries one payload with a unique id, and every id
//              written executes exactly once (a loss cannot hide behind a
//              duplicate);
//   progress   after the schedule, only views that already exist drain while
//              producers keep writing. Stranded work, or a stalled producer
//              with the survivor idle at the tail, fails before any new
//              subscription is allowed; a rescue afterwards is diagnostic only.
//
// usage: subscription_live            (every schedule and trigger)
//        subscription_live <A|B|C|D1|D2|E|F|G|H|I|P|J1|J2|J3|Q|K|X|L|LN> [trigger 0..3 for D1/D2/E]
module subscription_live;

import antfarm;
import core.atomic;
import core.thread;
import core.stdc.stdio : printf, fflush, stdout;
import core.stdc.stdlib : malloc, free, exit;

version (AntfarmWriteAuditHooks) {} else
    static assert(false, "build with AntfarmWriteAuditHooks");

enum ulong MAX_IDS = 1 << 16;
__gshared shared(uint)[MAX_IDS] g_exec;
__gshared shared(ubyte)[MAX_IDS] g_written;
shared ulong g_nextId;

__gshared AntFarm* g_f;
__gshared Token g_q, g_p;
__gshared ulong g_seg;
__gshared int g_stage, g_reads, g_trigger;
__gshared char g_mode;
__gshared bool g_failed;

extern(C) void idCallback(const(ulong)* body, size_t words) nothrow @nogc @system
{
    if (words == 0 || body[0] >= MAX_IDS) fatal("payload id out of range");
    atomicFetchAdd!(MemoryOrder.raw)(g_exec[cast(size_t) body[0]], 1);
}

void require(bool ok, const(char)* msg) nothrow @nogc @system
{
    if (ok) return;
    printf("REQUIRE FAILED: %s\n", msg);
    fflush(stdout);
    exit(3);
}

// One single-payload table of exactly `words` ring words when `exact` (the
// uniform-fixed singleton layout). Its payload's first body word is a fresh id.
ulong put(ref Token tok, ulong words, bool exact) nothrow @nogc @system
{
    immutable cf = atomicLoad(g_f.Cf);
    immutable base = 27 + 8UL * sqcsOf(cf > 0 ? cast(ulong) cf : 1);
    require(words > base + 3, "table too short");
    immutable n = cast(size_t)(words - base - 2);
    auto body = cast(ulong*) malloc(n * ulong.sizeof);
    require(body !is null, "body allocation");
    scope (exit) free(body);
    body[0 .. n] = 0;
    immutable id = atomicFetchAdd(g_nextId, 1);
    require(id < MAX_IDS, "id space");
    body[0] = id;
    PayloadEntry[1] e = [PayloadEntry(&idCallback, body[0 .. n])];
    immutable w0 = atomicLoad(g_f.Wt);
    immutable r = g_f.write(e[], tok);
    if (r == 1)
    {
        atomicStore(g_written[cast(size_t) id], cast(ubyte) 1);
        if (exact) require(atomicLoad(g_f.Wt) - w0 == words, "unexpected table size");
    }
    return r;
}

void resetLedger() nothrow @nogc @system
{
    foreach (i; 0 .. MAX_IDS)
    {
        atomicStore(g_exec[i], 0u);
        atomicStore(g_written[i], cast(ubyte) 0);
    }
    atomicStore(g_nextId, 0UL);
    g_stage = 0;
    g_reads = 0;
    g_failed = false;
}

void newFarm() nothrow @nogc @system
{
    resetLedger();
    // 1 MiB, K = 4 (segCap 32768); two small producers of 16384: Exmax is
    // one segment.
    g_f = AntFarm.create(1, 4, 1, 0, 0, 2, 16384, 64);
    g_q = g_f.registerProducer(Tier.small);
    g_p = g_f.registerProducer(Tier.small);
    require(g_q.valid && g_p.valid, "two producers");
    g_seg = g_f.segCap;
}

// Every schedule must leave the Farm with no member, root reference,
// transient pin, reserved bit or leaf; only Live bits may remain.
void requireDetached() nothrow @nogc @system
{
    require(atomicLoad(g_f.Cf) == 0, "all scheduled views detached");
    foreach (ki; 0 .. g_f.K)
    {
        require((atomicLoad(g_f.Rt[ki][0]) & ~LIVE) == 0, "no root refs, transient pins or reserved bits");
        foreach (leaf; 0 .. MAX_LEAVES)
            require(atomicLoad(g_f.Lt[ki * MAX_LEAVES + leaf][0]) == 0, "no leaked leaf references");
    }
}

void endFarm() nothrow @nogc @system
{
    requireDetached();
    g_f.unregisterProducer(g_q);
    g_f.unregisterProducer(g_p);
    g_f.destroy();
    g_f = null;
}

void dump(const(char)* tag) nothrow @nogc @system
{
    auto f = g_f;
    printf("    %s: Wt=%llu (epoch %llu) Cf=%lld\n", tag, atomicLoad(f.Wt),
        atomicLoad(f.Wt) >> f.segShift, atomicLoad(f.Cf));
    foreach (uint i; 0 .. f.K)
    {
        immutable rt = atomicLoad(f.Rt[i][0]);
        printf("      slot %u: es=%lld refs=%llu live=%u subs=%llu\n", i,
            atomicLoad(f.stats[i].es), rt & COUNTMASK, (rt & LIVE) != 0 ? 1u : 0u, rt >> 32);
    }
}

// Every id written so far executed exactly once; ids whose write was refused
// never executed.
void tally(out ulong missing, out ulong duplicated) nothrow @nogc @system
{
    immutable n = atomicLoad(g_nextId);
    foreach (i; 0 .. cast(size_t) n)
    {
        immutable ex = atomicLoad(g_exec[i]);
        if (atomicLoad(g_written[i]) != 0)
        {
            if (ex == 0) ++missing;
            else if (ex > 1) ++duplicated;
        }
        else if (ex != 0) ++duplicated;
    }
}

// The progress oracle: only `survivor` (already attached) drains while Q keeps
// writing. A failure is decided here, before any new subscription; the
// rescue that follows only reports where the work was.
bool survivorProgress(ref ConsumerView survivor, const(char)* name) nothrow @nogc @system
{
    foreach (round; 0 .. 8)
    {
        foreach (_; 0 .. 8) while (survivor.consumeNext()) {}
        while (put(g_q, 15000, false) == 1) {}
    }
    foreach (_; 0 .. 8) while (survivor.consumeNext()) {}
    immutable probed = put(g_q, 15000, false) == 1;
    immutable stalled = !probed && survivor.nextSeq == atomicLoad(g_f.Wt);
    if (probed) foreach (_; 0 .. 8) while (survivor.consumeNext()) {}
    ulong missing, duplicated;
    tally(missing, duplicated);
    immutable ok = missing == 0 && duplicated == 0 && !stalled;
    printf("  %s: survivor alone ran %llu ids; missing=%llu duplicated=%llu; producer %s\n",
        name, atomicLoad(g_nextId), missing, duplicated, stalled ? "STALLED".ptr : "not stalled".ptr);
    if (!ok)
    {
        dump("before diagnostic rescue");
        ConsumerView rescue;
        immutable at = rescue.subscribe(g_f);
        foreach (_; 0 .. 8) while (rescue.consumeNext()) {}
        tally(missing, duplicated);
        printf("  %s: diagnostic rescue attached at epoch %lld; missing now %llu\n",
            name, at, missing);
        rescue.unsubscribe();
    }
    return ok;
}

bool verdict(const(char)* name, bool ok) nothrow @nogc @system
{
    printf(ok ? "PASS %s\n".ptr : "FAIL %s\n".ptr, name);
    fflush(stdout);
    return ok;
}

// ---------------------------------------------------------------------------
// A, B, C. L, the only consumer, is parked on confirmed epoch 0; P's crossing
// write from epoch 1 (which no one has entered) into 2 is paused: after its
// Live raise on 2 (A) or right after reserving (B, C). L leaves as soon as S
// is counted. S must attach before P's start epoch, and no producer may enter
// epoch 5, the next lap of slot 1, while P's table is unpublished. B resumes P
// at once; A and C let Q write first.
// ---------------------------------------------------------------------------

__gshared ConsumerView g_L, g_S;
__gshared long g_subResult;

void insideABC() nothrow @nogc @system
{
    g_subResult = g_S.subscribeOnce(g_f);
    printf("  S.subscribeOnce -> %lld\n", g_subResult);
    if (g_mode == 'B') return;
    immutable lap = 1 + g_f.K;
    while ((atomicLoad(g_f.Wt) >> g_f.segShift) < lap)
        if (put(g_q, 15000, false) == 0) break;
    if ((atomicLoad(g_f.Wt) >> g_f.segShift) >= lap)
    {
        printf("  VIOLATION: Q entered epoch %llu while P's table starting in epoch 1 is unpublished\n",
            atomicLoad(g_f.Wt) >> g_f.segShift);
        dump("at violation");
        g_failed = true;
    }
}

void hookABC(AntFarm*, WriteAuditPhase phase, ulong a1, ulong) nothrow @nogc @system
{
    immutable pausePhase = g_mode == 'A' ? WriteAuditPhase.liveRaised : WriteAuditPhase.tailReserved;
    immutable pauseArg = g_mode == 'A' ? 2 : 2 * g_seg - 300;
    if (g_stage == 1 && phase == pausePhase && a1 == pauseArg)
    {
        g_stage = 2;
        insideABC();
    }
    else if (g_stage == 2 && phase == WriteAuditPhase.subscriberCounted)
    {
        g_stage = 3;
        g_L.unsubscribe();
    }
}

bool scheduleABC(char which)
{
    newFarm();
    g_mode = which;
    immutable S = g_seg;
    // T27's parking: L ends on confirmed epoch 0 with Live not yet discharged.
    ConsumerView h, c, b;
    require(h.subscribe(g_f) == 0, "h");
    require(put(g_q, 11000, true) == 1, "w1");
    require(put(g_q, 11000, true) == 1, "w2");
    require(put(g_q, S - 300 - 22000, true) == 1, "w3");
    while (h.consumeNext()) {}
    require(g_L.subscribe(g_f) == 0 && c.subscribe(g_f) == 0 && b.subscribe(g_f) == 0, "views");
    require(put(g_q, 1000, true) == 1, "crossing into epoch 1");
    c.unsubscribe();
    h.unsubscribe();
    foreach (_; 0 .. 4) require(g_L.consumeNext(), "L consumes");
    foreach (_; 0 .. 4) require(b.consumeNext(), "b consumes");
    require(!g_L.consumeNext(), "idle rewalk completes epoch 0");
    require(g_L.newestEi == 0 && b.newestEi == 0, "both on epoch 0");
    b.unsubscribe();
    require(atomicLoad(g_f.Cf) == 1, "L alone");
    require(put(g_q, 15884, true) == 1, "fill 1");
    require(put(g_q, 15884, true) == 1, "fill 2");
    require(atomicLoad(g_f.Wt) == 2 * S - 300, "Wt before P");

    g_stage = 1;
    writeAuditHook = &hookABC;
    immutable np = put(g_p, 1000, false);
    writeAuditHook = null;
    require(np == 1 && g_stage == 3, "schedule ran");
    if (g_subResult < 0)
        require(g_S.subscribe(g_f) >= 0, "S resubscribes");
    printf("  S attached at epoch %llu\n", g_S.oldestEi);
    bool ok = !g_failed && g_S.oldestEi <= 1;
    ok = survivorProgress(g_S, "ABC") && ok;
    g_S.unsubscribe();
    endFarm();
    char[2] nm = [which, 0];
    return verdict(nm.ptr, ok);
}

// ---------------------------------------------------------------------------
// D. Lg, the only consumer, is idle on confirmed epoch 0; epoch 1 was entered,
// completed and released by other views; P's crossing from epoch 2 (unconsumed
// tables) into 3 is held after its Live raise on 3. At S's slot read number
// `trigger`, Lg moves 0 -> 1. D1: Lg leaves after S attached, then Q writes;
// no producer may enter epoch 6, the next lap of slot 2. D2: Lg leaves inside
// S's scan. A scan that finished before the trigger runs the move afterwards.
// ---------------------------------------------------------------------------

__gshared ConsumerView g_Lg;

void moverD() nothrow @nogc @system
{
    while (g_Lg.newestEi != 1) require(g_Lg.consumeNext(), "Lg advances");
    require(g_Lg.oldestEi == 1, "Lg released epoch 0");
    if (g_mode == '2') g_Lg.unsubscribe();
}

void insideD() nothrow @nogc @system
{
    g_subResult = g_S.subscribeOnce(g_f);
    require(g_subResult >= 0, "S subscribes");
    if (g_stage != 3)
    {
        g_stage = 3;
        printf("  scan finished before read #%d; Lg moves after S attached\n", g_trigger);
        moverD();
    }
    printf("  S attached at epoch %lld\n", g_subResult);
    if (g_mode == '1')
    {
        g_Lg.unsubscribe();
        immutable lap = 2 + g_f.K;
        while ((atomicLoad(g_f.Wt) >> g_f.segShift) < lap)
            if (put(g_q, 15000, false) == 0) break;
        if ((atomicLoad(g_f.Wt) >> g_f.segShift) >= lap)
        {
            printf("  VIOLATION: Q entered epoch %llu while P's table starting in epoch 2 is unpublished\n",
                atomicLoad(g_f.Wt) >> g_f.segShift);
            dump("at violation");
            g_failed = true;
        }
    }
}

void hookD(AntFarm*, WriteAuditPhase phase, ulong a1, ulong) nothrow @nogc @system
{
    if (g_stage == 1 && phase == WriteAuditPhase.liveRaised && a1 == 3)
    {
        g_stage = 2;
        insideD();
    }
    else if (g_stage == 2 && phase == WriteAuditPhase.subscriberRead)
    {
        if (g_reads++ != g_trigger) return;
        g_stage = 3;
        moverD();
    }
}

bool scheduleD(char mode, int trigger)
{
    newFarm();
    g_mode = mode;
    g_trigger = trigger;
    immutable S = g_seg;
    ConsumerView h0, m, d, n;
    require(h0.subscribe(g_f) == 0 && g_Lg.subscribe(g_f) == 0, "h0, Lg");     // IDc 0, 1
    require(put(g_q, 11000, true) == 1, "e0 t1");
    require(put(g_q, 11000, true) == 1, "e0 t2");
    require(put(g_q, S - 300 - 22000, true) == 1, "e0 t3");
    require(put(g_q, 1000, true) == 1, "e0 crossing");                         // S + 700
    while (h0.consumeNext()) {}
    require(h0.newestEi == 1 && g_Lg.newestEi == 0, "h0 on 1, Lg idle on 0");
    require(put(g_q, 15884, true) == 1, "e1 t1");
    require(put(g_q, 15883, true) == 1, "e1 t2");                              // 2S - 301
    while (h0.consumeNext()) {}
    require(m.subscribe(g_f) >= 0, "M");                                       // IDc 2
    require(d.subscribe(g_f) >= 0, "D");                                       // IDc 3
    d.unsubscribe();
    require(n.subscribe(g_f) >= 0, "N");                                       // IDc 4
    while (m.consumeNext()) {}
    while (n.consumeNext()) {}
    require(m.newestEi == 1 && n.newestEi == 1, "M, N on 1");
    h0.unsubscribe();
    // T at an odd sequence with SqCs 2: M and N pass it on the hot path; M's
    // idle re-walk completes it, leaving both parked on confirmed epoch 1.
    require(atomicLoad(g_f.Cf) == 3 && atomicLoad(g_f.Wt) == 2 * S - 301, "before T");
    require(put(g_q, 1000, true) == 1, "T crossing into 2");
    require(m.consumeNext() && n.consumeNext(), "M, N skip T");
    require(!m.consumeNext(), "M's re-walk completes epoch 1");
    m.unsubscribe();
    n.unsubscribe();
    require(atomicLoad(g_f.Cf) == 1, "Lg alone");
    require((atomicLoad(g_f.Rt[1][0]) & LOWMASK) == 0, "epoch 1 discharged and released");
    require(put(g_q, 15884, true) == 1, "e2 t1");
    require(put(g_q, 15884, true) == 1, "e2 t2");                              // 3S - 301

    g_stage = 1;
    writeAuditHook = &hookD;
    immutable np = put(g_p, 1000, false);
    writeAuditHook = null;
    require(np == 1 && g_stage == 3, "schedule ran");
    bool ok = !g_failed && g_S.oldestEi <= 2;
    if (g_Lg.hasRef) g_Lg.unsubscribe();
    ok = survivorProgress(g_S, "D") && ok;
    g_S.unsubscribe();
    endFarm();
    char[16] nm;
    nm[0 .. 4] = ['D', mode, '#', cast(char)('0' + trigger)];
    nm[4] = 0;
    return verdict(nm.ptr, ok);
}

// ---------------------------------------------------------------------------
// Worker-thread producers for E, F, G and I: each runs one write and holds at
// its configured phase until released.
// ---------------------------------------------------------------------------

struct Worker
{
    shared int start, paused, go, done;
    Token* tok;
    ulong words;
    WriteAuditPhase phase;
    ulong arg;
}
__gshared Worker[2] g_w;
int tlsWorker = -1; // thread-local

void resetWorkers() nothrow @nogc @system
{
    foreach (ref w; g_w)
    {
        atomicStore(w.start, 0); atomicStore(w.paused, 0);
        atomicStore(w.go, 0); atomicStore(w.done, 0);
    }
}

bool workerHold(WriteAuditPhase phase, ulong a1) nothrow @nogc @system
{
    if (tlsWorker < 0) return false;
    auto w = &g_w[tlsWorker];
    if (phase == w.phase && a1 == w.arg && atomicLoad(w.paused) == 0)
    {
        atomicStore(w.paused, 1);
        while (atomicLoad(w.go) == 0) Thread.yield();
    }
    return true;
}

void runWorker(int i)
{
    tlsWorker = i;
    auto w = &g_w[i];
    while (atomicLoad(w.start) == 0) Thread.yield();
    require(put(*w.tok, w.words, false) == 1, "worker wrote");
    atomicStore(w.done, 1);
}
void worker0() { runWorker(0); }
void worker1() { runWorker(1); }

void kick(int i) nothrow @nogc @system
{
    atomicStore(g_w[i].start, 1);
    while (atomicLoad(g_w[i].paused) == 0) Thread.yield();
}

void resume(int i) nothrow @nogc @system
{
    atomicStore(g_w[i].go, 1);
    while (atomicLoad(g_w[i].done) == 0) Thread.yield();
}

// ---------------------------------------------------------------------------
// E and I. C is the only consumer, at epoch 1, with P's table next. P has
// reserved [2S-300, 2S+700) into epoch 2 and is held before initializing it;
// Q then wrote three tables starting in epoch 2 and initialized epoch 3 ahead
// of P. E: at S's slot read number `trigger`, P finishes, C executes P's
// table, confirms epoch 1, moves to 2 and leaves. I: between the two loads of
// S's read of slot 2, P finishes and C leaves its unfinished epoch 1. S must
// attach at or before epoch 2. I is the case that defeats a scan stopping at
// the first discharged slot when it loads Rt before Es; the oldest-first scan
// stops at Live epoch 1 before it reaches slot 2, so for it I runs the
// after-attach path.
// ---------------------------------------------------------------------------

__gshared ConsumerView g_C;

void moverEI() nothrow @nogc @system
{
    resume(0);
    require(atomicLoad(g_f.stats[2].es) == 2, "P initialized epoch 2");
    if (g_mode == 'E')
    {
        require(g_C.consumeNext(), "C executes P's table");
        require(g_C.newestEi == 2 && g_C.oldestEi == 2, "C confirmed 1 and moved to 2");
    }
    g_C.unsubscribe();
}

void hookEI(AntFarm*, WriteAuditPhase phase, ulong a1, ulong) nothrow @nogc @system
{
    if (workerHold(phase, a1)) return;
    if (g_stage != 2) return;
    if (g_mode == 'E' && phase == WriteAuditPhase.subscriberRead)
    {
        if (g_reads++ != g_trigger) return;
        g_stage = 3;
        moverEI();
    }
    else if (g_mode == 'I' && phase == WriteAuditPhase.subscriberReadMid && a1 == 2)
    {
        g_stage = 3;
        moverEI();
    }
}

bool scheduleEI(char which, int trigger)
{
    newFarm();
    resetWorkers();
    g_mode = which;
    g_trigger = trigger;
    immutable S = g_seg;
    require(g_C.subscribe(g_f) == 0, "C");
    require(put(g_q, 11000, true) == 1, "e0 t1");
    require(put(g_q, 11000, true) == 1, "e0 t2");
    require(put(g_q, S - 300 - 22000, true) == 1, "e0 t3");
    require(put(g_q, 1000, true) == 1, "e0 crossing");                         // S + 700
    require(put(g_q, 15884, true) == 1, "e1 t1");
    require(put(g_q, 15884, true) == 1, "e1 t2");                              // 2S - 300
    while (g_C.consumeNext()) {}
    require(g_C.oldestEi == 1 && g_C.newestEi == 1, "C on epoch 1");

    g_w[0].tok = &g_p;
    g_w[0].words = 1000;
    g_w[0].phase = WriteAuditPhase.tailReserved;
    g_w[0].arg = 2 * S - 300;
    writeAuditHook = &hookEI;
    auto t0 = new Thread(&worker0);
    t0.start();
    kick(0);
    require(put(g_q, 15884, true) == 1, "e2 t1");
    require(put(g_q, 15884, true) == 1, "e2 t2");                              // 3S - 300
    require(put(g_q, 1000, true) == 1, "e2 crossing into 3");                  // 3S + 700
    require(atomicLoad(g_f.stats[3].es) == 3 && atomicLoad(g_f.stats[2].es) != 2,
        "epoch 3 initialized ahead of epoch 2");

    g_stage = 2;
    immutable r = g_S.subscribeOnce(g_f);
    if (g_stage != 3)
    {
        g_stage = 3;
        printf("  scan finished before its trigger; the mid-scan steps run after S attached\n");
        moverEI();
    }
    t0.join();
    writeAuditHook = null;
    require(r >= 0, "S subscribed");
    printf("  S attached at epoch %lld\n", r);
    bool ok = r <= 2;
    ok = survivorProgress(g_S, which == 'E' ? "E".ptr : "I".ptr) && ok;
    g_S.unsubscribe();
    endFarm();
    char[8] nm;
    if (which == 'E') { nm[0 .. 3] = ['E', '#', cast(char)('0' + trigger)]; nm[3] = 0; }
    else { nm[0] = 'I'; nm[1] = 0; }
    return verdict(nm.ptr, ok);
}

// ---------------------------------------------------------------------------
// F and G. C is the only consumer, at epoch 3, with that epoch's two fills
// unconsumed; epochs 0-2 are discharged; S probes the tail in epoch 3.
// F (the probe goes stale by two epochs): before S reads anything, P reserves
// into epoch 4 and is held before initializing it, Q fills epoch 4 and holds
// its crossing into 5. At S's read of slot 1, P then Q initialize, in order;
// C finishes epoch 3, moves to 4 and leaves. G (a new lap's Live beside the
// old Es): P reserves into 4, raises Live on slot 0 and is held before
// publishing Es[4]; at S's read of slot 1, P publishes and C leaves its
// unfinished epoch 3. S must attach at or before the earliest unfinished
// epoch: 4 in F, 3 in G.
// ---------------------------------------------------------------------------

void probedFG() nothrow @nogc @system
{
    kick(0);
    if (g_mode == 'F')
    {
        require(put(g_q, 15884, true) == 1, "e4 t1");
        require(put(g_q, 15884, true) == 1, "e4 t2");                          // 5S - 300
        kick(1);
        require(atomicLoad(g_f.Wt) == 5 * g_seg + 700, "both crossings reserved");
    }
}

void midFG() nothrow @nogc @system
{
    resume(0);
    if (g_mode == 'F')
    {
        resume(1);
        while (g_C.newestEi != 4) require(g_C.consumeNext(), "C advances");
        require(g_C.oldestEi == 4, "C discharged epoch 3");
    }
    g_C.unsubscribe();
}

void hookFG(AntFarm*, WriteAuditPhase phase, ulong a1, ulong) nothrow @nogc @system
{
    if (workerHold(phase, a1)) return;
    if (g_stage == 1 && phase == WriteAuditPhase.subscriberProbed)
    {
        g_stage = 2;
        probedFG();
    }
    else if (g_stage == 2 && phase == WriteAuditPhase.subscriberRead && a1 == 1)
    {
        g_stage = 3;
        midFG();
    }
}

bool scheduleFG(char which)
{
    newFarm();
    resetWorkers();
    g_mode = which;
    immutable S = g_seg;
    require(g_C.subscribe(g_f) == 0, "C");
    require(put(g_q, 11000, true) == 1, "e0 t1");
    require(put(g_q, 11000, true) == 1, "e0 t2");
    require(put(g_q, S - 300 - 22000, true) == 1, "e0 t3");
    require(put(g_q, 1000, true) == 1, "e0 crossing");
    foreach (e; 1 .. 3)
    {
        require(put(g_q, 15884, true) == 1, "fill");
        require(put(g_q, 15884, true) == 1, "fill");
        require(put(g_q, 1000, true) == 1, "crossing");
    }
    while (g_C.consumeNext()) {}
    require(g_C.oldestEi == 3 && g_C.newestEi == 3, "C at epoch 3");
    require(put(g_q, 15884, true) == 1, "e3 t1");
    require(put(g_q, 15884, true) == 1, "e3 t2");                              // 4S - 300
    require(atomicLoad(g_f.Wt) == 4 * S - 300, "before S");

    g_w[0].tok = &g_p;
    g_w[0].words = 1000;
    g_w[0].phase = which == 'F' ? WriteAuditPhase.tailReserved : WriteAuditPhase.liveRaised;
    g_w[0].arg = which == 'F' ? 4 * S - 300 : 4;
    g_w[1].tok = &g_q;
    g_w[1].words = 1000;
    g_w[1].phase = WriteAuditPhase.tailReserved;
    g_w[1].arg = 5 * S - 300;
    auto t0 = new Thread(&worker0);
    t0.start();
    Thread t1;
    if (which == 'F')
    {
        t1 = new Thread(&worker1);
        t1.start();
    }
    writeAuditHook = &hookFG;
    g_stage = 1;
    long r;
    int retries;
    while ((r = g_S.subscribeOnce(g_f)) == SUBSCRIBE_RETRY) ++retries;
    if (g_stage == 2)
    {
        g_stage = 3;
        printf("  scan never read slot 1; the mid-scan steps run after S attached\n");
        midFG();
    }
    writeAuditHook = null;
    t0.join();
    if (t1 !is null) t1.join();
    require(r >= 0, "S subscribed");
    printf("  S attached at epoch %lld after %d RETRY\n", r, retries);
    bool ok = r <= (which == 'F' ? 4 : 3);
    ok = survivorProgress(g_S, which == 'F' ? "F".ptr : "G".ptr) && ok;
    g_S.unsubscribe();
    endFarm();
    char[2] nm = [which, 0];
    return verdict(nm.ptr, ok);
}

// ---------------------------------------------------------------------------
// H. C holds epochs [1, 3]: epoch 1 is unconfirmed (C passed a small table T
// claimable only on shard 0, with SqCs 2), 2 is confirmed, 3 is C's position.
// C leaves and discharges 2, leaving Live epochs 1 and 3 with a discharged 2
// between them. An oldest-first scan must still reach epoch 1.
// ---------------------------------------------------------------------------

bool scheduleH()
{
    newFarm();
    immutable S = g_seg;
    ConsumerView h, x, c;
    require(h.subscribe(g_f) == 0, "h");                                       // IDc 0
    require(put(g_q, 11000, true) == 1, "e0 t1");
    require(put(g_q, 11000, true) == 1, "e0 t2");
    require(put(g_q, S - 300 - 22000, true) == 1, "e0 t3");
    require(put(g_q, 1000, true) == 1, "e0 crossing");                         // S + 700
    require(put(g_q, 15883, true) == 1, "odd filler");
    while (h.consumeNext()) {}
    require(h.oldestEi == 1 && h.newestEi == 1, "h on 1");
    require(x.subscribe(g_f) == 1 && c.subscribe(g_f) == 1, "x, c attach at 1"); // IDc 1, 2
    immutable tseq = atomicLoad(g_f.Wt);
    require((tseq & 1) == 1 && atomicLoad(g_f.Cf) == 3, "T placement");
    require(put(g_q, 1000, true) == 1, "T");
    h.unsubscribe();
    x.unsubscribe();
    require(c.consumeNext() && c.nextSeq == tseq, "c reaches T");
    require(c.consumeNext() && c.newestEi == 1, "c passes T");
    require(put(g_q, 2 * S - 300 - (tseq + 1000), true) == 1, "e1 fill");
    require(put(g_q, 1000, true) == 1, "e1 crossing");                         // 2S + 700
    require(c.consumeNext() && c.consumeNext() && c.newestEi == 1, "epoch 1 unconfirmed");
    require(put(g_q, 15884, true) == 1, "e2 t1");
    require(put(g_q, 15884, true) == 1, "e2 t2");
    require(put(g_q, 1000, true) == 1, "e2 crossing");                         // 3S + 700
    foreach (_; 0 .. 3) require(c.consumeNext(), "c runs epoch 2");
    require(c.oldestEi == 1 && c.newestEi == 3, "c holds [1, 3]");
    c.unsubscribe();
    require((atomicLoad(g_f.Rt[1][0]) & LIVE) != 0 && (atomicLoad(g_f.Rt[2][0]) & LIVE) == 0
        && (atomicLoad(g_f.Rt[3][0]) & LIVE) != 0, "Live 1, discharged 2, Live 3");
    immutable r = g_S.subscribe(g_f);
    printf("  S attached at epoch %lld\n", r);
    bool ok = r == 1;
    ok = survivorProgress(g_S, "H") && ok;
    g_S.unsubscribe();
    endFarm();
    return verdict("H", ok);
}

// ---------------------------------------------------------------------------
// P. A delayed epoch-0 probe: S samples Wt in epoch 0 and is paused before
// its first Sub. C then completes epoch 0 and moves to epoch 1, producers
// reach epoch 4 and reuse slot 0, and C leaves epochs 1-3 unfinished. S must
// not take epoch 4 in slot 0 as a candidate of its expired window; it must
// attach at epoch 1.
// ---------------------------------------------------------------------------

void hookP(AntFarm*, WriteAuditPhase phase, ulong, ulong round) nothrow @nogc @system
{
    if (g_stage != 1 || phase != WriteAuditPhase.subscriberProbed || round != 0) return;
    g_stage = 2;
    immutable S = g_seg;
    require(put(g_q, 11000, true) == 1, "e0 t1");
    require(put(g_q, 11000, true) == 1, "e0 t2");
    require(put(g_q, S - 300 - 22000, true) == 1, "e0 t3");
    require(put(g_q, 1000, true) == 1, "e0 crossing");                         // S + 700
    while (g_C.consumeNext()) {}
    require(g_C.oldestEi == 1 && g_C.newestEi == 1, "C on epoch 1");
    foreach (e; 1 .. 4)
    {
        require(put(g_q, 15884, true) == 1, "fill");
        require(put(g_q, 15884, true) == 1, "fill");
        require(put(g_q, 1000, true) == 1, "crossing");
    }
    require((atomicLoad(g_f.Wt) >> g_f.segShift) == 4 && atomicLoad(g_f.stats[0].es) == 4,
        "slot 0 reused for epoch 4");
    g_C.unsubscribe();
}

bool scheduleP()
{
    newFarm();
    require(g_C.subscribe(g_f) == 0, "C");
    g_stage = 1;
    writeAuditHook = &hookP;
    long r;
    int retries;
    while ((r = g_S.subscribeOnce(g_f)) == SUBSCRIBE_RETRY) ++retries;
    writeAuditHook = null;
    require(g_stage == 2 && r >= 0, "schedule ran");
    printf("  S attached at epoch %lld after %d RETRY\n", r, retries);
    bool ok = r == 1;
    ok = survivorProgress(g_S, "P") && ok;
    g_S.unsubscribe();
    endFarm();
    return verdict("P", ok);
}

// ---------------------------------------------------------------------------
// J and Q. C, the only consumer, idles on epoch 1 with that epoch's three
// tables unconsumed; epoch 2 is already initialized, so epoch 1 confirms the
// moment C drains it. S's candidate is epoch 1, confirmed-but-Live at the
// instant the schedule chooses:
//   J1  C drains and discharges 1 at S's pinned re-read of slot 1. The
//       re-read finds Live clear; S drops that Sub and attaches at 2.
//   J2  C drains at S's validation, after the re-read saw Live. S attaches
//       at the now discharged epoch 1: its Sub covered the slot throughout,
//       so its reference lands on protected space.
//   J3  C is paused inside its discharge, between the load and the CAS; S
//       subscribes completely and attaches at 1 while it is Live and
//       confirmed. S lands on a second leaf, so C's CAS fails against the
//       new count, reloads, and clears Live under S's reference.
//   Q   C is paused the same way; a Sub lands on slot 1 and stays. C's CAS
//       retries against it and clears Live with the Sub intact.
// ---------------------------------------------------------------------------

__gshared int g_clears;

void setupJ(bool twoLeaves) nothrow @nogc @system
{
    immutable S = g_seg;
    require(g_C.subscribe(g_f) == 0, "C");                                     // IDc 0
    ConsumerView h1, h2;
    if (twoLeaves)
        require(h1.subscribe(g_f) == 0 && h2.subscribe(g_f) == 0, "h1, h2"); // IDc 1, 2
    require(put(g_q, 11000, true) == 1, "e0 t1");
    require(put(g_q, 11000, true) == 1, "e0 t2");
    require(put(g_q, S - 300 - 22000, true) == 1, "e0 t3");
    require(put(g_q, 1000, true) == 1, "e0 crossing");                         // S + 700
    if (twoLeaves)
    {
        // Epoch 1 was sized for three consumers: SqCs 2, two leaves.
        require(atomicLoad(g_f.stats[1].sqcs) == 2, "two leaves on epoch 1");
        h1.unsubscribe();
        h2.unsubscribe();
    }
    while (g_C.consumeNext()) {}
    require(g_C.oldestEi == 1 && g_C.newestEi == 1, "C on epoch 1");
    require(put(g_q, 15884, true) == 1, "e1 t1");
    require(put(g_q, 15884, true) == 1, "e1 t2");                              // 2S - 300
    require(put(g_q, 1000, true) == 1, "e1 crossing into 2");                  // 2S + 700
    require((atomicLoad(g_f.Rt[0][0]) & LOWMASK) == 0, "epoch 0 discharged");
    require((atomicLoad(g_f.Rt[1][0]) & LOWMASK) == (LIVE | 1), "epoch 1 Live, held by C");
    require(atomicLoad(g_f.Rt[2][0]) == LIVE, "epoch 2 Live, unentered");
}

void hookJ(AntFarm*, WriteAuditPhase phase, ulong a1, ulong a2) nothrow @nogc @system
{
    if (g_stage != 1 && !(g_mode == '1' && g_stage == 2)) return;
    final switch (g_mode)
    {
    case '1':
        if (phase == WriteAuditPhase.subscriberRead && a1 == 1 && a2 == 1 && ++g_reads == 2)
        {
            g_stage = 2;
            while (g_C.consumeNext()) {}
            require(g_C.newestEi == 2 && g_C.oldestEi == 2, "C discharged 1 at the re-read");
        }
        else if (g_stage == 2 && phase == WriteAuditPhase.subscriberRead && a1 == 2 && a2 == 2)
        {
            // The rejected candidate's fresh Sub is already gone; the tail's stays.
            require((atomicLoad(g_f.Rt[1][0]) >> 32) == 0, "rejected candidate pin released");
            require((atomicLoad(g_f.Rt[2][0]) >> 32) == 1, "tail pin retained");
            g_stage = 3;
        }
        break;
    case '2':
        if (phase == WriteAuditPhase.subscriberValidating && a1 == 1 && a2 == 1)
        {
            g_stage = 2;
            while (g_C.consumeNext()) {}
            require(g_C.newestEi == 2 && g_C.oldestEi == 2, "C discharged 1 under S's Sub");
        }
        break;
    case '3':
        if (phase == WriteAuditPhase.liveClearing && a1 == 1)
        {
            if (++g_clears == 1)
            {
                require(((a2 & LIVE) != 0) && (a2 & COUNTMASK) == 1, "C loaded Live with its own count");
                g_subResult = g_S.subscribe(g_f);
                require(g_subResult == 1, "S attached at Live epoch 1 during the discharge");
                require((atomicLoad(g_f.Rt[1][0]) & LOWMASK) == (LIVE | 2), "second leaf took a root count");
            }
            else
            {
                g_stage = 2;
                require((a2 & COUNTMASK) == 2, "C's CAS retried against the new count");
            }
        }
        break;
    case 'Q':
        if (phase == WriteAuditPhase.liveClearing && a1 == 1)
        {
            if (++g_clears == 1)
                atomicFetchAdd!(MemoryOrder.rel)(g_f.Rt[1][0], SUB);
            else
            {
                g_stage = 2;
                require((a2 >> 32) == 1 && (a2 & LIVE) != 0, "C's CAS retried against the Sub");
            }
        }
        break;
    }
}

bool scheduleJ(char mode)
{
    newFarm();
    g_mode = mode;
    g_clears = 0;
    setupJ(mode == '3');
    g_stage = 1;
    writeAuditHook = &hookJ;
    long r;
    if (mode == '1' || mode == '2')
    {
        r = g_S.subscribe(g_f);
        writeAuditHook = null;
        require(g_stage == (mode == '1' ? 3 : 2), "schedule ran");
    }
    else
    {
        // C drains alone; its discharge of epoch 1 is where the schedule acts.
        while (g_C.consumeNext()) {}
        writeAuditHook = null;
        require(g_stage == 2 && g_clears == 2, "discharge CAS retried exactly once");
        r = mode == '3' ? g_subResult : -1;
    }
    bool ok = true;
    immutable rt1 = atomicLoad(g_f.Rt[1][0]);
    if (mode == 'Q')
        printf("  discharge retried against the Sub; slot 1 Rt=%llx\n", rt1);
    else
        printf("  S attached at epoch %lld; slot 1 Rt=%llx\n", r, rt1);
    final switch (mode)
    {
    case '1':
        ok = r == 2 && (rt1 & LOWMASK) == 0;
        break;
    case '2':
        ok = r == 1 && (rt1 & LIVE) == 0 && (rt1 & COUNTMASK) == 1;
        break;
    case '3':
        ok = r == 1 && (rt1 & LIVE) == 0 && (rt1 & COUNTMASK) == 1;
        break;
    case 'Q':
        ok = (rt1 & LIVE) == 0 && (rt1 >> 32) == 1 && (rt1 & COUNTMASK) == 0;
        atomicFetchSub!(MemoryOrder.rel)(g_f.Rt[1][0], SUB);
        ok = ok && atomicLoad(g_f.Rt[1][0]) == 0;
        break;
    }
    foreach (uint i; 0 .. g_f.K)
        ok = ok && (atomicLoad(g_f.Rt[i][0]) >> 32) == 0;
    if (mode == 'Q')
    {
        require(g_S.subscribe(g_f) == 2, "S attaches at the frontier");
    }
    g_C.unsubscribe();
    char[8] nm = ['J', mode, 0, 0, 0, 0, 0, 0];
    if (mode == 'Q') nm[0 .. 2] = ['Q', 0];
    ok = survivorProgress(g_S, nm.ptr) && ok;
    g_S.unsubscribe();
    endFarm();
    return verdict(nm.ptr, ok);
}

// ---------------------------------------------------------------------------
// K. Two subscribers interleaved on one candidate. In J's setup, S1 has just
// pinned slot 1 (its candidate) when S2 subscribes completely: S2 pins the
// same frontier and candidate slots, attaches at 1, and drops its Subs. S1
// then re-reads under the remaining Sub, validates, and attaches at 1 too.
// Every Sub must be gone, both references must be on epoch 1, and the root
// count must equal the number of occupied leaves.
// ---------------------------------------------------------------------------

__gshared ConsumerView g_S2;

void hookK(AntFarm*, WriteAuditPhase phase, ulong a1, ulong) nothrow @nogc @system
{
    if (g_stage != 1 || phase != WriteAuditPhase.subscriberPinned || a1 != 1) return;
    g_stage = 2;
    require((atomicLoad(g_f.Rt[1][0]) >> 32) == 1, "S1's Sub is on slot 1");
    g_subResult = g_S2.subscribe(g_f);
    require(g_subResult == 1, "S2 attached at 1 while S1 holds its Sub");
    require((atomicLoad(g_f.Rt[1][0]) >> 32) == 1, "S2 dropped its Subs, S1's remains");
}

bool scheduleK()
{
    newFarm();
    setupJ(true);
    g_stage = 1;
    writeAuditHook = &hookK;
    immutable r = g_S.subscribe(g_f);
    writeAuditHook = null;
    require(g_stage == 2, "schedule ran");
    printf("  S1 attached at epoch %lld, S2 at %lld\n", r, g_subResult);
    bool ok = r == 1 && g_subResult == 1;
    foreach (uint i; 0 .. g_f.K)
        ok = ok && (atomicLoad(g_f.Rt[i][0]) >> 32) == 0;
    // C (IDc 0) and S2 (IDc 2) share leaf 0; S1 (IDc 1) is alone on leaf 1.
    immutable rt1 = atomicLoad(g_f.Rt[1][0]);
    ok = ok && (rt1 & LOWMASK) == (LIVE | 2)
        && atomicLoad(g_f.Lt[1 * MAX_LEAVES + 0][0]) == 2
        && atomicLoad(g_f.Lt[1 * MAX_LEAVES + 1][0]) == 1;
    while (g_S2.consumeNext()) {}
    g_S2.unsubscribe();
    g_C.unsubscribe();
    ok = survivorProgress(g_S, "K") && ok;
    g_S.unsubscribe();
    endFarm();
    return verdict("K", ok);
}

// ---------------------------------------------------------------------------
// X. An exact boundary landing. The last table of epoch 0 ends at Wt == S, so
// the write initializes epoch 1 with Seqt[1] == S before any table exists
// there. C migrates onto that empty frontier and discharges 0. S must attach
// at 1 with nextSeq == S, both while C holds the frontier and after C has
// left; the next table then starts exactly at the boundary.
// ---------------------------------------------------------------------------

bool scheduleX()
{
    newFarm();
    immutable S = g_seg;
    require(g_C.subscribe(g_f) == 0, "C");
    require(put(g_q, 11000, true) == 1, "e0 t1");
    require(put(g_q, 11000, true) == 1, "e0 t2");
    require(put(g_q, S - 300 - 22000, true) == 1, "e0 t3");
    require(put(g_q, 300, true) == 1, "exact landing");
    require(atomicLoad(g_f.Wt) == S && atomicLoad(g_f.stats[1].es) == 1
        && atomicLoad(g_f.stats[1].seqt) == S, "epoch 1 initialized at the boundary");
    while (g_C.consumeNext()) {}
    require(g_C.oldestEi == 1 && g_C.newestEi == 1 && g_C.nextSeq == S, "C idle on the empty frontier");
    require(atomicLoad(g_f.Rt[0][0]) == 0, "epoch 0 discharged");
    immutable r1 = g_S.subscribe(g_f);
    printf("  S attached at epoch %lld, nextSeq %llu (boundary %llu)\n", r1, g_S.nextSeq, S);
    bool ok = r1 == 1 && g_S.nextSeq == S;
    g_C.unsubscribe();
    g_S.unsubscribe();
    require(atomicLoad(g_f.Rt[1][0]) == LIVE, "empty Farm: only the boundary frontier Live");
    immutable r2 = g_S.subscribe(g_f);
    ok = ok && r2 == 1 && g_S.nextSeq == S;
    require(put(g_q, 1000, true) == 1, "table at the boundary");
    require(g_S.consumeNext() && g_S.nextSeq == S + 1000, "S reads the boundary table");
    ok = survivorProgress(g_S, "X") && ok;
    g_S.unsubscribe();
    endFarm();
    return verdict("X", ok);
}

// ---------------------------------------------------------------------------
// L and LN. A bulk table spans three boundaries (0 -> 3), so epochs 1 and 2
// are interiors with Seqt == 3S + 300. C finishes it, migrates to 3 and
// discharges 0; while C is paused inside its discharge of 1, S attaches at
// interior epoch 1. Its first table lies in epoch 3, so validation must
// acquire Es[3] before attaching (SPEC 6b-c). LN is the negative control
// for that load: the hook hides Es[3] during S's first validation, which
// must reject the round; the next round, with Es[3] visible again, attaches.
// ---------------------------------------------------------------------------

__gshared int g_probes;

void newFarmBulk() nothrow @nogc @system
{
    resetLedger();
    // 1 MiB, K = 4; one bulk producer whose quota is three segments.
    g_f = AntFarm.create(1, 4, 1, 1, 3 * 32768, 0, 0);
    g_q = g_f.registerProducer(Tier.bulk);
    require(g_q.valid, "bulk producer");
    g_seg = g_f.segCap;
}

void endFarmBulk() nothrow @nogc @system
{
    requireDetached();
    g_f.unregisterProducer(g_q);
    g_f.destroy();
    g_f = null;
}

void hookL(AntFarm*, WriteAuditPhase phase, ulong a1, ulong a2) nothrow @nogc @system
{
    if (g_stage == 1 && phase == WriteAuditPhase.liveClearing && a1 == 1)
    {
        g_stage = 2;
        g_subResult = g_S.subscribe(g_f);
        atomicStore(g_f.stats[3].es, 3L);           // LN: whatever S did, restore Es[3]
        g_stage = 3;
        return;
    }
    if (g_stage != 2) return;
    if (phase == WriteAuditPhase.subscriberProbed) ++g_probes;
    if (g_mode != 'N') return;
    // Instrumentation negative control: Es[3] reads as unpublished during S's
    // first validation only. Nothing else runs meanwhile (C is paused on this
    // thread), and the value is restored before S's first advance.
    if (phase == WriteAuditPhase.subscriberValidating && a1 == 1 && a2 == 1 && g_probes == 1)
        atomicStore(g_f.stats[3].es, -1L);
    else if (phase == WriteAuditPhase.subscriberProbed && g_probes == 2)
        atomicStore(g_f.stats[3].es, 3L);
}

bool scheduleL(char mode)
{
    newFarmBulk();
    g_mode = mode;
    g_probes = 0;
    immutable S = g_seg;
    require(g_C.subscribe(g_f) == 0, "C");
    require(put(g_q, S - 300, true) == 1, "e0 fill");
    require(put(g_q, 2 * S + 600, true) == 1, "spanning 0 -> 3");                 // 3S + 300
    require(put(g_q, 1000, true) == 1, "first table of epoch 3");                 // 3S + 1300
    foreach (e; 1 .. 4)
        require(atomicLoad(g_f.stats[e].es) == e && atomicLoad(g_f.stats[e].seqt) == 3 * S + 300,
            "interiors share Seqt");
    g_stage = 1;
    writeAuditHook = &hookL;
    while (g_C.consumeNext()) {}
    writeAuditHook = null;
    require(g_stage == 3, "schedule ran");
    require(g_C.oldestEi == 3 && g_C.newestEi == 3, "C on epoch 3");
    printf("  S attached at epoch %lld after %d probe(s), nextSeq %llu (epoch %llu)\n",
        g_subResult, g_probes, g_S.nextSeq, g_S.nextSeq >> g_f.segShift);
    bool ok = g_subResult == 1 && g_S.nextSeq == 3 * S + 300
        && g_probes == (mode == 'N' ? 2 : 1);
    // C cleared Live(1) under S's reference and released 1 and 2.
    ok = ok && (atomicLoad(g_f.Rt[1][0]) & LOWMASK) == 1
        && atomicLoad(g_f.Rt[2][0]) == 0
        && (atomicLoad(g_f.Rt[3][0]) & LOWMASK) == (LIVE | 1);
    // S's first advance pins 2 and 3 through the epoch-3 table, then releases
    // the confirmed prefix.
    require(g_S.consumeNext(), "S reads the epoch-3 table");
    ok = ok && g_S.oldestEi == 3 && g_S.newestEi == 3
        && atomicLoad(g_f.Rt[1][0]) == 0 && atomicLoad(g_f.Rt[2][0]) == 0;
    g_C.unsubscribe();
    ok = survivorProgress(g_S, mode == 'N' ? "LN".ptr : "L".ptr) && ok;
    g_S.unsubscribe();
    endFarmBulk();
    return verdict(mode == 'N' ? "LN".ptr : "L".ptr, ok);
}

immutable string[] SCHEDULES = ["A", "B", "C", "D1", "D2", "E", "F", "G", "H", "I", "P",
    "J1", "J2", "J3", "Q", "K", "X", "L", "LN"];

int main(string[] args)
{
    bool valid = args.length == 1;
    if (args.length >= 2)
        foreach (name; SCHEDULES)
            if (args[1] == name) valid = true;
    if (args.length == 3)
        valid = valid && (args[1] == "D1" || args[1] == "D2" || args[1] == "E")
            && args[2].length == 1 && args[2][0] >= '0' && args[2][0] <= '3';
    if (!valid || args.length > 3)
    {
        printf("usage: subscription_live [<schedule> [trigger 0..3 for D1/D2/E]]; schedules: ");
        foreach (name; SCHEDULES) printf("%s ", name.ptr);
        printf("\n");
        return 64;
    }
    bool all = args.length == 1;
    bool ok = true;
    bool want(string name) { return all || args[1] == name; }
    int trigger(int dflt) { return args.length > 2 ? args[2][0] - '0' : dflt; }
    foreach (name; ["A", "B", "C"])
        if (want(name)) { printf("schedule %s\n", name.ptr); ok &= scheduleABC(name[0]); }
    foreach (name; ["D1", "D2"])
        if (want(name))
            foreach (t; 0 .. 4)
                if (all || args.length < 3 || t == trigger(0))
                {
                    printf("schedule %s, mover at read #%d\n", name.ptr, t);
                    ok &= scheduleD(name[1], t);
                }
    if (want("E"))
        foreach (t; 0 .. 4)
            if (all || args.length < 3 || t == trigger(0))
            {
                printf("schedule E, mover at read #%d\n", t);
                ok &= scheduleEI('E', t);
            }
    if (want("F")) { printf("schedule F\n"); ok &= scheduleFG('F'); }
    if (want("G")) { printf("schedule G\n"); ok &= scheduleFG('G'); }
    if (want("H")) { printf("schedule H\n"); ok &= scheduleH(); }
    if (want("I")) { printf("schedule I\n"); ok &= scheduleEI('I', 0); }
    if (want("P")) { printf("schedule P\n"); ok &= scheduleP(); }
    foreach (name; ["J1", "J2", "J3", "Q"])
        if (want(name)) { printf("schedule %s\n", name.ptr); ok &= scheduleJ(name == "Q" ? 'Q' : name[1]); }
    if (want("K")) { printf("schedule K\n"); ok &= scheduleK(); }
    if (want("X")) { printf("schedule X\n"); ok &= scheduleX(); }
    if (want("L")) { printf("schedule L\n"); ok &= scheduleL('L'); }
    if (want("LN")) { printf("schedule LN\n"); ok &= scheduleL('N'); }
    printf(ok ? "SUBSCRIPTION LIVE SCHEDULES PASSED\n".ptr : "SUBSCRIPTION LIVE SCHEDULES FAILED\n".ptr);
    return ok ? 0 : 1;
}
