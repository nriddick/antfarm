/++
 + Off-CCD scaling driver: one Farm, a pinned home group on one L3 group,
 + plus off-CCD consumers and/or producers on another. One run per process,
 + one JSON object on stdout.
 +
 +   ./offccd --plan-only --off-c 8
 +   ./offccd --off-c 1 --cost touch --batch 1700 --mode stream
 +   ./offccd --off-p 4 --mode phased --window-ms 300
 +
 + Placement comes from `placement.planRoles` (eligible primaries per L3
 + group from the threadpool snapshot). A request that does not fit prints
 + `status:"skipped"` with the planner's reason. Every thread pins itself
 + through `pinProcessor`; a refused or moved pin makes the run `invalid`.
 + Usage errors exit 2; ok, skipped and invalid runs exit 0.
 +/
module offccd;

import antfarm;
import placement;
import threadpool.pin : liveOsMask;
import threadpool.topology : CpuListException, ProcessorId, TopologySnapshot, discover,
    formatCpuList, parseCpuList;

import core.atomic;
import core.memory : GC;
import core.thread;
import core.time;
import std.algorithm : max, min, sort, sum;
import std.array : array;
import std.conv : to;
import std.format : format;
import std.json : JSONValue;
import std.stdio : stderr, stdout, writeln;

enum Layout { uniform, compact, mt }
enum Mode { stream, phased }

enum uint ringMiB = 16;
enum uint ringK = 8;

struct Opts
{
    RoleRequest req = RoleRequest(2, 5);
    string cost = "touch";
    uint batch = 1700;
    uint bodyWords = 1;
    Layout layout;
    uint ac = DEFAULT_AVG_COST;
    Mode mode;
    uint warmMs = 300, windowMs = 1000, subwindows = 10;
    double roundFrac = 0.25;
    uint latEvery;
    bool planOnly;
    ulong seed;
}

// ---- shared state ---------------------------------------------------------

align(128) struct Line
{
    shared ulong v;
}

// Controller-owned words, each on its own line.
__gshared Line g_phase;     // (round << 4) | code
__gshared Line g_flipTicks; // MonoTime ticks of the last PUBLISH/CONSUME flip
__gshared Line g_target;    // cumulative consumer-callback target
__gshared Line g_ready;

enum : uint
{
    phSetup,
    phRun,
    phStop,
    phFinish,
    phAbort,
    phPublish,
    phConsume,
}

uint codeOf(ulong ph) { return cast(uint)(ph & 15); }
ulong roundOf(ulong ph) { return ph >> 4; }

void setPhase(uint code, ulong round = 0)
{
    atomicStore!(MemoryOrder.rel)(g_phase.v, (round << 4) | code);
}

ulong loadPhase() nothrow @nogc
{
    return atomicLoad!(MemoryOrder.acq)(g_phase.v);
}

/// Per-thread record, written only by its owner. `items`, `doneRound` and
/// `finished` are read by the controller (and, phased, by consumers) during
/// the run; the rest only after join.
align(128) struct Slot
{
    shared ulong items;     // consumers: callbacks; producers: payloads accepted
    shared ulong doneRound; // producers, phased
    shared ulong finished;  // producers, stream
    ulong visitsTrue, visitsFalse, emptyVisits;
    ulong writeCalls, writeZero, shortWrites, phaseOverflow;
    ulong sink;
    uint subscribeRetries;
    bool setupFailed;
    bool pinAttempted;
    PinOutcome pin;
    ProcessorId observedStart = unobserved, observedEnd = unobserved;
}

enum unobserved = ProcessorId(ushort.max, ushort.max);

__gshared Opts g_opts;
__gshared const(TopologySnapshot)* g_snap;
__gshared AntFarm* g_farm;
__gshared Slot* g_slots;          // threads in plan order, then the controller
__gshared void* g_slotMem;
__gshared PlacedThread[] g_threads;
__gshared size_t[] g_consumers;   // slot indices
__gshared size_t[] g_producers;
__gshared ulong g_share;          // phased: payloads per producer per round
__gshared ulong g_costIters;
__gshared ulong g_calSink;
__gshared PayloadHeader g_hdr, g_hdrAlt, g_hdrMt;
__gshared long[][] g_firstCb;     // per slot, phased
__gshared long[][] g_lat;         // per slot, latency slice

// ---- callbacks ------------------------------------------------------------

ulong tlsCalls;
ulong tlsSink;
bool tlsWantFirst;
long tlsFirst;
long* tlsLat;
size_t tlsLatN, tlsLatCap;

enum ulong latFlag = 1UL << 63;

long nowTicks() nothrow @nogc @system
{
    return MonoTime.currTime.ticks;
}

long ticksToNs(long dt) nothrow @nogc @system
{
    if (dt < 0) return 0;
    immutable tps = MonoTime.ticksPerSecond;
    return dt / tps * 1_000_000_000L + (dt % tps) * 1_000_000_000L / tps;
}

/// The dependent LCG chain used as callback cost: no memory traffic, no
/// clock reads.
ulong chain(ulong x, ulong n) nothrow @nogc @system
{
    foreach (i; 0 .. n)
        x = x * 6364136223846793005UL + 1442695040888963407UL;
    return x;
}

long cbBody(bool alt)(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    ulong x = tlsSink;
    foreach (w; b)
        x ^= w;
    if (g_costIters)
        x ^= chain(x | 1, g_costIters);
    tlsSink = x;
    if (tlsWantFirst)
    {
        tlsFirst = nowTicks();
        tlsWantFirst = false;
    }
    if (tlsLat !is null && (b[0] & latFlag) && tlsLatN < tlsLatCap)
        tlsLat[tlsLatN++] = nowTicks() - cast(long)(b[0] & ~latFlag);
    ++tlsCalls;
    return alt ? 2 : 1;
}

alias cbTouch = cbBody!false;
alias cbTouchAlt = cbBody!true;

// ---- thread helpers -------------------------------------------------------

void pause() nothrow @nogc
{
    version (LDC)
    {
        import ldc.gccbuiltins_x86 : __builtin_ia32_pause;
        __builtin_ia32_pause();
    }
    else version (D_InlineAsm_X86_64)
        asm nothrow @nogc { rep; nop; }
}

ProcessorId observed()
{
    auto p = g_snap.current();
    return p is null ? unobserved : ProcessorId.of(*p);
}

/// Pin the calling thread to slot `i`'s planned CPU. False if refused.
bool pinSelf(size_t i, ProcessorId cpu)
{
    auto s = &g_slots[i];
    auto p = g_snap.byId(cpu);
    s.pin = p is null ? PinOutcome.unknownProcessor : pinProcessor(*p);
    s.pinAttempted = true;
    if (s.pin != PinOutcome.pinned)
        return false;
    s.observedStart = observed();
    return true;
}

/// Join the ready barrier and wait for the first release. False on abort.
bool readyAndWait()
{
    atomicOp!"+="(g_ready.v, 1);
    ulong ph;
    while (codeOf(ph = loadPhase()) == phSetup)
        pause();
    return codeOf(ph) != phAbort;
}

bool sumReached(ulong target) nothrow @nogc
{
    ulong n;
    foreach (i; g_consumers)
        n += atomicLoad!(MemoryOrder.acq)(g_slots[i].items);
    return n >= target;
}

final class Job
{
    size_t i;
    this(size_t i) { this.i = i; }
    void run()
    {
        if (g_threads[i].role == "consumer")
            consumerMain(i);
        else
            producerMain(i);
    }
}

// ---- consumers ------------------------------------------------------------

void consumerMain(size_t i)
{
    auto s = &g_slots[i];
    tlsCalls = 0;
    tlsSink = 0;
    if (g_opts.latEvery)
    {
        g_lat[i] = new long[1 << 16];
        tlsLat = g_lat[i].ptr;
        tlsLatCap = g_lat[i].length;
    }
    if (!pinSelf(i, g_threads[i].cpu.id))
    {
        readyAndWait();
        return;
    }
    ConsumerView v;
    long e;
    while ((e = v.subscribe(g_farm)) == SUBSCRIBE_RETRY)
    {
        ++s.subscribeRetries;
        pause();
    }
    if (e < 0)
    {
        s.setupFailed = true;
        readyAndWait();
        return;
    }
    if (readyAndWait())
    {
        if (g_opts.mode == Mode.stream)
            streamConsume(v, s);
        else
            phasedConsume(v, s, i);
    }
    s.observedEnd = observed();
    s.sink = tlsSink;
    if (tlsLat !is null)
        g_lat[i] = g_lat[i][0 .. tlsLatN];
    v.unsubscribe();
}

void streamConsume(ref ConsumerView v, Slot* s)
{
    for (;;)
    {
        immutable code = codeOf(loadPhase());
        if (code == phFinish || code == phAbort)
            break;
        immutable before = tlsCalls;
        if (v.consumeNext())
        {
            ++s.visitsTrue;
            if (tlsCalls == before)
                ++s.emptyVisits;
        }
        else
        {
            ++s.visitsFalse;
            pause();
        }
        atomicStore!(MemoryOrder.rel)(s.items, tlsCalls);
    }
}

// A consumer counts only on a false return, so no counter traffic while work
// flows. It leaves a round once the round target is met *and* its own last
// consumeNext returned false: that return migrates its position to the
// frontier, so the next round's producers can reclaim the ring.
void phasedConsume(ref ConsumerView v, Slot* s, size_t i)
{
    ulong lastRound;
    for (;;)
    {
        immutable ph = loadPhase();
        immutable code = codeOf(ph);
        if (code == phFinish || code == phAbort)
            break;
        if (code != phConsume || roundOf(ph) == lastRound)
        {
            pause();
            continue;
        }
        immutable flip = cast(long) atomicLoad!(MemoryOrder.acq)(g_flipTicks.v);
        immutable target = atomicLoad!(MemoryOrder.acq)(g_target.v);
        tlsWantFirst = true;
        tlsFirst = 0;
        for (;;)
        {
            immutable before = tlsCalls;
            if (v.consumeNext())
            {
                ++s.visitsTrue;
                if (tlsCalls == before)
                    ++s.emptyVisits;
                continue;
            }
            ++s.visitsFalse;
            atomicStore!(MemoryOrder.rel)(s.items, tlsCalls);
            if (sumReached(target) || codeOf(loadPhase()) == phAbort)
                break;
            pause();
        }
        g_firstCb[i] ~= tlsWantFirst ? -1 : ticksToNs(tlsFirst - flip);
        tlsWantFirst = false;
        lastRound = roundOf(ph);
    }
}

// ---- producers ------------------------------------------------------------

struct Writer
{
    PayloadBody[] bodies;
    PayloadEntry[] entries;
    ulong[] storage;
    ulong calls;
}

ulong writeSome(ref Writer w, ref Token tok, size_t span)
{
    immutable stamp = g_opts.latEvery && w.calls % g_opts.latEvery == 0;
    if (stamp)
        w.storage[0] = latFlag | cast(ulong) nowTicks();
    ++w.calls;
    ulong wrote;
    final switch (g_opts.layout)
    {
    case Layout.uniform:
        wrote = g_farm.write(g_hdr, w.bodies[0 .. span], g_opts.bodyWords, tok, g_opts.ac);
        break;
    case Layout.compact:
        wrote = g_farm.write(w.entries[0 .. span], tok, g_opts.ac);
        break;
    case Layout.mt:
        wrote = g_farm.write(g_hdrMt, w.bodies[0 .. span], g_opts.bodyWords, tok, g_opts.ac);
        break;
    }
    if (stamp)
        w.storage[0] = 0;
    return wrote;
}

void producerMain(size_t i)
{
    auto s = &g_slots[i];
    immutable batch = g_opts.batch;
    immutable bw = g_opts.bodyWords;
    // Per-producer bodies, first touched on the producer's own CPU.
    Writer w;
    w.storage = new ulong[batch * bw];
    w.bodies = new PayloadBody[batch];
    w.entries = new PayloadEntry[batch];
    foreach (k; 0 .. batch)
    {
        w.storage[k * bw] = k;
        w.bodies[k] = w.storage[k * bw .. (k + 1) * bw];
        w.entries[k].header = (k & 1) ? &g_hdrAlt : &g_hdr;
        w.entries[k].body = w.bodies[k];
    }
    if (!pinSelf(i, g_threads[i].cpu.id))
    {
        readyAndWait();
        return;
    }
    auto tok = g_farm.registerProducer(Tier.small);
    if (!tok.valid)
    {
        s.setupFailed = true;
        readyAndWait();
        return;
    }
    if (readyAndWait())
    {
        if (g_opts.mode == Mode.stream)
            streamProduce(w, tok, s);
        else
            phasedProduce(w, tok, s);
    }
    s.observedEnd = observed();
    g_farm.unregisterProducer(tok);
    atomicStore!(MemoryOrder.rel)(s.finished, 1UL);
}

void streamProduce(ref Writer w, ref Token tok, Slot* s)
{
    ulong items;
    while (codeOf(loadPhase()) == phRun)
    {
        immutable wrote = writeSome(w, tok, g_opts.batch);
        ++s.writeCalls;
        if (wrote == 0)
        {
            ++s.writeZero;
            pause();
            continue;
        }
        if (wrote < g_opts.batch)
            ++s.shortWrites;
        items += wrote;
        atomicStore!(MemoryOrder.rel)(s.items, items);
    }
}

void phasedProduce(ref Writer w, ref Token tok, Slot* s)
{
    ulong lastRound, items;
    for (;;)
    {
        immutable ph = loadPhase();
        immutable code = codeOf(ph);
        if (code == phFinish || code == phAbort)
            return;
        if (code != phPublish || roundOf(ph) == lastRound)
        {
            pause();
            continue;
        }
        ulong done;
        while (done < g_share)
        {
            immutable take = cast(size_t) min(g_share - done, g_opts.batch);
            immutable wrote = writeSome(w, tok, take);
            ++s.writeCalls;
            if (wrote == 0)
            {
                // The round budget should always fit; count and retry.
                ++s.phaseOverflow;
                ++s.writeZero;
                if (codeOf(loadPhase()) == phAbort)
                    return;
                pause();
                continue;
            }
            if (wrote < take)
                ++s.shortWrites;
            done += wrote;
        }
        items += done;
        atomicStore!(MemoryOrder.rel)(s.items, items);
        lastRound = roundOf(ph);
        atomicStore!(MemoryOrder.rel)(s.doneRound, lastRound);
    }
}

// ---- table sizing and claim scan ------------------------------------------

/// Ring words of one table of `n` payloads (the Farm's size arithmetic for
/// the layout each producer write selects).
ulong tableWords(Layout layout, ulong n, uint sq, uint bodyWords)
{
    immutable base = 8 + 7 + 1 + 7 + 8UL * sq + 7; // Thead, pads, Tprogress, Tcount, end pad
    final switch (layout)
    {
    case Layout.uniform: return base + 2 + n * bodyWords;        // uniformFixed
    case Layout.compact: return base + n * (2 + bodyWords);      // Tindex + Call + body
    case Layout.mt: return base + n * (2 + 16 + bodyWords);      // full: 2 index words, Phead
    }
}

struct ScanResult
{
    ulong tables, items, failed;
    bool progressOk = true, executorOk = true;
}

// Executor check (antfarm/package.d writeImpl/consumeNext): a table's sequence is the
// Wt value its write reserved, its Tsent word holds sentinelOf(seq), and Tnext
// is seq + Tsize, so walking Tnext from the pre-PUBLISH Wt visits exactly the
// round's tables and lands on the post-round Wt. Offsets follow the table
// layout: Thead[8], extension words (uniform 1, uniformFixed 2), Tindex
// (none for uniformFixed, Tlen + Tmt for full), 7 pad, Tprogress, 7 pad,
// then Tcount shards at a stride of 8 words. Tcount high 32 bits count claim
// attempts; attempts beyond Shiter = ceil(Shlen / chunk) failed.
void scanRound(ulong from, ulong to, ref ScanResult r)
{
    auto f = g_farm;
    auto bp = cast(ulong*) f.buf;
    ulong seq = from;
    while (seq < to)
    {
        immutable idx = seq & f.Lmask;
        if (bp[idx] != sentinelOf(seq))
        {
            r.executorOk = false;
            return;
        }
        immutable w2 = bp[idx + 2];
        immutable tlen = cast(uint) w2;
        immutable tmt = cast(uint)(w2 >> 32);
        immutable sq = cast(uint) bp[idx + 4];
        immutable w6 = bp[idx + 6];
        immutable kind = cast(uint)(w6 >> 32);
        immutable ac = cast(uint) w6;
        immutable ext = kind == 2 ? 2 : kind == 1 ? 1 : 0;
        immutable index = kind == 2 ? 0 : (kind == 0 && tmt != 0) ? cast(ulong) tlen + tmt : tlen;
        immutable progOff = idx + 8 + ext + index + 7;
        immutable tcountOff = progOff + 8;
        if (bp[progOff] != tlen)
            r.progressOk = false;
        immutable chunk = MAX_CHUNK >> min(ac, 7u);
        immutable auto_ = sq * chunk < 16 ? 16 : sq * chunk > 256 ? 256 : sq * chunk;
        immutable thresh = f.smallThreshold ? f.smallThreshold : auto_;
        immutable shards = tlen < thresh ? 1 : sq;
        foreach (shi; 0 .. shards)
        {
            immutable shlen = tlen < thresh ? tlen : tlen / sq + (shi < tlen % sq ? 1 : 0);
            immutable shiter = (shlen + chunk - 1) / chunk;
            immutable claims = bp[tcountOff + shi * 8] >> 32;
            if (claims > shiter)
                r.failed += claims - shiter;
        }
        ++r.tables;
        r.items += tlen;
        seq = bp[idx + 1];
    }
    if (seq != to)
        r.executorOk = false;
}

// ---- statistics -----------------------------------------------------------

JSONValue pctJson(long[] xs)
{
    auto s = xs.dup.sort().array;
    JSONValue j;
    j["n"] = s.length;
    if (s.length)
    {
        j["p50"] = s[s.length / 2];
        j["p99"] = s[(s.length - 1) * 99 / 100];
        j["p999"] = s[(s.length - 1) * 999 / 1000];
        j["max"] = s[$ - 1];
    }
    return j;
}

/// Normalized shares `share_i * n` with home/off means, min, max and Jain.
JSONValue sharesJson(const double[] xs, const string[] locs)
{
    JSONValue j;
    immutable total = xs.sum;
    if (xs.length == 0 || total <= 0)
        return JSONValue(null);
    double sq = 0;
    double[] norm;
    foreach (x; xs)
    {
        norm ~= x / total * xs.length;
        sq += x * x;
    }
    double meanOf(bool delegate(string) pick)
    {
        double s = 0;
        size_t n;
        foreach (k, v; norm)
            if (pick(locs[k])) { s += v; ++n; }
        return n ? s / n : double.nan;
    }
    auto home = meanOf(l => l == "home" || l == "home-extra");
    auto off = meanOf(l => l == "off");
    auto smt = meanOf(l => l == "smt");
    j["home_mean"] = home == home ? JSONValue(home) : JSONValue(null);
    j["off_mean"] = off == off ? JSONValue(off) : JSONValue(null);
    j["smt_mean"] = smt == smt ? JSONValue(smt) : JSONValue(null);
    j["min"] = norm.dup.sort()[0];
    j["max"] = norm.dup.sort()[$ - 1];
    j["jain"] = total * total / (xs.length * sq);
    j["normalized"] = norm;
    return j;
}

double median(double[] xs)
{
    if (xs.length == 0) return double.nan;
    auto s = xs.dup.sort().array;
    return s.length % 2 ? s[$ / 2] : (s[$ / 2 - 1] + s[$ / 2]) / 2;
}

JSONValue mhzOf(ProcessorId id)
{
    version (linux)
    {
        import std.file : readText;
        import std.string : strip;

        try
            return JSONValue(readText(format("/sys/devices/system/cpu/cpu%d/cpufreq/scaling_cur_freq",
                id.lp)).strip.to!double / 1000.0);
        catch (Exception)
            return JSONValue(null);
    }
    else
        return JSONValue(null);
}

JSONValue observedJson(ProcessorId id)
{
    return id == unobserved ? JSONValue(null) : JSONValue(cpuText(id));
}

// ---- main -----------------------------------------------------------------

int usage(string msg)
{
    stderr.writeln("offccd: ", msg);
    stderr.writeln("usage: offccd [--home-llc auto|N] [--home-p N] [--home-c N]",
        " [--home-extra-p N] [--home-extra-c N] [--off-p N] [--off-c N] [--smt-p N] [--smt-c N]",
        " [--cpus LIST] [--cost touch|w100|w1000|NS] [--batch N] [--body N]",
        " [--layout uniform|compact|mt] [--ac N] [--mode stream|phased] [--warm-ms N]",
        " [--window-ms N] [--subwindows N] [--round-frac F] [--lat-every N] [--plan-only] [--seed N]");
    return 2;
}

double costNs(string cost)
{
    switch (cost)
    {
    case "touch": return 0;
    case "w100": return 100;
    case "w1000": return 1000;
    default: return cost.to!double;
    }
}

/// Chain iterations per ns on the calling (pinned) thread; best of 5.
double calibrate()
{
    double best = 0;
    ulong x = 1;
    foreach (rep; 0 .. 5)
    {
        enum n = 1UL << 22;
        immutable t0 = nowTicks();
        x = chain(x, n);
        immutable ns = ticksToNs(nowTicks() - t0);
        if (ns > 0)
            best = max(best, cast(double) n / ns);
    }
    g_calSink = x; // keep the chain live
    return best;
}

int main(string[] args)
{
    Opts o;
    try
    {
        for (size_t i = 1; i < args.length; ++i)
        {
            string a = args[i];
            string next()
            {
                if (i + 1 >= args.length)
                    throw new Exception("missing value for " ~ a);
                return args[++i];
            }
            switch (a)
            {
            case "--home-llc":
                auto v = next();
                o.req.homeLlc = v == "auto" ? -1 : v.to!int;
                break;
            case "--home-p": o.req.homeP = next().to!uint; break;
            case "--home-c": o.req.homeC = next().to!uint; break;
            case "--home-extra-p": o.req.homeExtraP = next().to!uint; break;
            case "--home-extra-c": o.req.homeExtraC = next().to!uint; break;
            case "--off-p": o.req.offP = next().to!uint; break;
            case "--off-c": o.req.offC = next().to!uint; break;
            case "--smt-p": o.req.smtP = next().to!uint; break;
            case "--smt-c": o.req.smtC = next().to!uint; break;
            case "--cpus": o.req.cpus = parseCpuList(next()); break;
            case "--cost": o.cost = next(); costNs(o.cost); break;
            case "--batch": o.batch = next().to!uint; break;
            case "--body": o.bodyWords = next().to!uint; break;
            case "--layout": o.layout = next().to!Layout; break;
            case "--ac": o.ac = next().to!uint; break;
            case "--mode": o.mode = next().to!Mode; break;
            case "--warm-ms": o.warmMs = next().to!uint; break;
            case "--window-ms": o.windowMs = next().to!uint; break;
            case "--subwindows": o.subwindows = next().to!uint; break;
            case "--round-frac": o.roundFrac = next().to!double; break;
            case "--lat-every": o.latEvery = next().to!uint; break;
            case "--plan-only": o.planOnly = true; break;
            case "--seed": o.seed = next().to!ulong; break;
            default:
                return usage("unknown argument " ~ a);
            }
        }
    }
    catch (CpuListException e)
        return usage(e.msg);
    catch (Exception e)
        return usage(e.msg);
    if (o.batch == 0 || o.bodyWords == 0 || o.subwindows == 0 || o.windowMs == 0
        || o.ac > MAX_AVG_COST || !(o.roundFrac > 0 && o.roundFrac <= 0.5))
        return usage("batch, body, subwindows and window-ms must be positive, ac <= 7,"
            ~ " 0 < round-frac <= 0.5");

    auto snap = discover();
    g_snap = &snap;
    auto live = liveOsMask(); // before any pin: Linux reports the calling thread's mask
    auto plan = planRoles(snap, o.req);
    g_opts = o;

    JSONValue out_;
    out_["topology"] = topologyJson(snap, plan, live);
    out_["plan"] = planJson(plan);
    out_["config"] = configJson(o);
    if (!plan.ok)
    {
        out_["status"] = "skipped";
        out_["reason"] = plan.reason;
        return emit(out_);
    }
    if (o.planOnly)
    {
        out_["status"] = "plan";
        out_["reason"] = JSONValue(null);
        return emit(out_);
    }
    run(plan, out_);
    return emit(out_);
}

int emit(JSONValue j)
{
    writeln(j.toString);
    stdout.flush();
    return 0;
}

JSONValue configJson(ref const Opts o)
{
    JSONValue c;
    c["home_llc"] = o.req.homeLlc;
    c["home_p"] = o.req.homeP;
    c["home_c"] = o.req.homeC;
    c["home_extra_p"] = o.req.homeExtraP;
    c["home_extra_c"] = o.req.homeExtraC;
    c["off_p"] = o.req.offP;
    c["off_c"] = o.req.offC;
    c["smt_p"] = o.req.smtP;
    c["smt_c"] = o.req.smtC;
    c["cpus_restriction"] = o.req.cpus.length
        ? JSONValue(formatCpuList(o.req.cpus.dup.sort().array)) : JSONValue(null);
    c["cost"] = o.cost;
    c["cost_ns"] = costNs(o.cost);
    c["batch"] = o.batch;
    c["body"] = o.bodyWords;
    c["layout"] = o.layout.to!string;
    c["ac"] = o.ac;
    c["mode"] = o.mode.to!string;
    c["warm_ms"] = o.warmMs;
    c["window_ms"] = o.windowMs;
    c["subwindows"] = o.subwindows;
    c["round_frac"] = o.roundFrac;
    c["lat_every"] = o.latEvery;
    c["seed"] = o.seed;
    return c;
}

void run(ref RolePlan plan, ref JSONValue out_)
{
    auto o = g_opts;
    g_threads = plan.threads ~ plan.controller;
    immutable n = plan.threads.length;
    immutable ctl = n; // controller slot
    foreach (k, th; plan.threads)
        (th.role == "consumer" ? g_consumers : g_producers) ~= k;
    immutable totalC = cast(uint) g_consumers.length;
    immutable totalP = cast(uint) g_producers.length;

    // GC arrays are only 16-byte aligned; align the slots by hand. Slots
    // hold no GC pointers.
    g_slotMem = GC.malloc((n + 1) * Slot.sizeof + Slot.alignof, GC.BlkAttr.NO_SCAN);
    g_slots = cast(Slot*)((cast(size_t) g_slotMem + Slot.alignof - 1) & ~(Slot.alignof - 1));
    foreach (k; 0 .. n + 1)
        g_slots[k] = Slot.init;
    g_firstCb = new long[][](n + 1);
    g_lat = new long[][](n + 1);

    void finish(string status, string reason)
    {
        out_["status"] = status;
        out_["reason"] = reason.length ? JSONValue(reason) : JSONValue(null);
    }

    // Controller: pin, then calibrate the cost chain on its CPU.
    auto cs = &g_slots[ctl];
    if (!pinSelf(ctl, plan.controller.cpu.id))
    {
        out_["threads"] = threadsJson(n, null);
        return finish("invalid", "pin-" ~ cs.pin.to!string);
    }
    immutable itersPerNs = calibrate();
    g_costIters = cast(ulong)(costNs(o.cost) * itersPerNs);

    if (totalC == 0 || totalP == 0)
    {
        out_["threads"] = threadsJson(n, null);
        return finish("skipped", "needs-producer-and-consumer");
    }
    immutable sq = sqcsOf(totalC);
    immutable quotaSmall = max(4096UL, tableWords(o.layout, o.batch, sq, o.bodyWords));
    auto f = AntFarm.create(ringMiB, ringK, totalC, 0, 0, totalP, quotaSmall,
        DEFAULT_SMALL_TABLE_THRESHOLD);
    g_farm = f;
    scope (exit) f.destroy();

    g_hdr = PayloadHeader.init;
    g_hdr.maxCs = 1;
    g_hdr.done = 1;
    g_hdr.call = &cbTouch;
    g_hdrAlt = g_hdr;
    g_hdrAlt.call = &cbTouchAlt;
    g_hdrMt = g_hdr;
    g_hdrMt.maxCs = 2;
    g_hdrMt.done = 2;
    immutable done = o.layout == Layout.mt ? 2UL : 1UL;

    // Phased round budget: whole batches per producer, with one extra table
    // header per write as margin for quota splits.
    immutable budgetWords = cast(ulong)(o.roundFrac * f.Ln);
    immutable perWrite = tableWords(o.layout, o.batch, sq, o.bodyWords)
        + tableWords(o.layout, 0, sq, o.bodyWords);
    g_share = max(1UL, budgetWords / totalP / perWrite) * o.batch;
    immutable roundItems = g_share * totalP;

    atomicStore(g_phase.v, 0UL);
    atomicStore(g_ready.v, 0UL);
    atomicStore(g_target.v, 0UL);

    GC.collect();
    GC.disable();
    scope (exit) GC.enable();
    auto jobs = new Job[n];
    auto threads = new Thread[n];
    foreach (k; 0 .. n)
    {
        jobs[k] = new Job(k);
        threads[k] = new Thread(&jobs[k].run);
        threads[k].start();
    }

    string failure;
    immutable setupDeadline = MonoTime.currTime + 5.seconds;
    while (atomicLoad!(MemoryOrder.acq)(g_ready.v) < n)
    {
        if (MonoTime.currTime > setupDeadline) { failure = "setup-timeout"; break; }
        Thread.yield();
    }
    if (!failure.length)
        foreach (k; 0 .. n)
        {
            if (g_slots[k].pin != PinOutcome.pinned)
                failure = "pin-" ~ g_slots[k].pin.to!string;
            else if (g_slots[k].setupFailed)
                failure = g_threads[k].role == "consumer" ? "subscribe-failed" : "register-failed";
            if (failure.length) break;
        }
    // A setup failure leaves threads unpinned or unobserved, which the pin
    // verdict would report as a mismatch; the setup reason is the cause.
    immutable setupFailure = failure;
    immutable cf = atomicLoad(f.Cf);

    JSONValue agg, phased, scanJ;
    ulong[] winItems = new ulong[n];
    double windowSecs = 0;
    JSONValue[] mhz = new JSONValue[n + 1];
    mhz[] = JSONValue(null);
    void sampleMhz()
    {
        foreach (k; 0 .. n + 1)
            mhz[k] = mhzOf(g_threads[k].cpu.id);
    }
    ulong[] snapshot()
    {
        auto r = new ulong[n];
        foreach (k; 0 .. n)
            r[k] = atomicLoad!(MemoryOrder.acq)(g_slots[k].items);
        return r;
    }
    ulong sumOf(const ulong[] v, const size_t[] idx)
    {
        ulong s;
        foreach (k; idx) s += v[k];
        return s;
    }

    if (failure.length)
        setPhase(phAbort);
    else if (o.mode == Mode.stream)
    {
        setPhase(phRun);
        Thread.sleep(o.warmMs.msecs);
        auto t0 = MonoTime.currTime;
        auto base = snapshot();
        auto prev = base;
        auto tPrev = t0;
        double[] subRates;
        immutable sub = o.windowMs * 1000 / o.subwindows; // usecs
        foreach (w; 1 .. o.subwindows + 1)
        {
            auto until = t0 + (w * sub).usecs;
            auto now = MonoTime.currTime;
            if (until > now)
                Thread.sleep(until - now);
            auto cur = snapshot();
            auto tNow = MonoTime.currTime;
            immutable dt = (tNow - tPrev).total!"nsecs" / 1e9;
            subRates ~= cast(double)(sumOf(cur, g_consumers) - sumOf(prev, g_consumers)) / done / dt;
            prev = cur;
            tPrev = tNow;
            if (w == (o.subwindows + 1) / 2)
                sampleMhz();
        }
        windowSecs = (tPrev - t0).total!"nsecs" / 1e9;
        foreach (k; 0 .. n)
            winItems[k] = prev[k] - base[k];
        setPhase(phStop);
        immutable drainDeadline = MonoTime.currTime + 10.seconds;
        bool producersDone;
        while (!producersDone)
        {
            producersDone = true;
            foreach (k; g_producers)
                if (atomicLoad!(MemoryOrder.acq)(g_slots[k].finished) == 0)
                    producersDone = false;
            if (MonoTime.currTime > drainDeadline) { failure = "drain-timeout"; break; }
            Thread.yield();
        }
        ulong produced;
        foreach (k; g_producers)
            produced += atomicLoad!(MemoryOrder.acq)(g_slots[k].items);
        atomicStore!(MemoryOrder.rel)(g_target.v, produced * done);
        while (!failure.length && !sumReached(produced * done))
        {
            if (MonoTime.currTime > drainDeadline) { failure = "drain-timeout"; break; }
            Thread.yield();
        }
        setPhase(failure.length ? phAbort : phFinish);
        agg["subwindow_rates"] = subRates;
        agg["subwindow_median_rate"] = median(subRates);
    }
    else
    {
        // Phased: PUBLISH(r) then CONSUME(r), repeated through warmup and the
        // window; only window rounds count. The claim scan runs between
        // rounds, untimed.
        immutable start = MonoTime.currTime;
        bool measuring;
        MonoTime windowStart;
        ulong[] base;
        ulong r, target;
        long pubNs, conNs, gateNs;
        ulong rounds, polls, pollNs, maxWords, sumWords;
        bool mhzDone;
        ScanResult scan;
        bool waitFor(bool delegate() cond)
        {
            immutable deadline = MonoTime.currTime + 2.seconds;
            immutable t0 = nowTicks();
            ulong p;
            while (!cond())
            {
                foreach (k; 0 .. 64) pause();
                ++p;
                if ((p & 1023) == 0 && MonoTime.currTime > deadline)
                    return false;
            }
            if (measuring)
            {
                polls += p;
                pollNs += ticksToNs(nowTicks() - t0);
            }
            return true;
        }
        for (;;)
        {
            auto now = MonoTime.currTime;
            if (!measuring && now - start >= o.warmMs.msecs)
            {
                measuring = true;
                windowStart = now;
                base = snapshot();
            }
            if (measuring && now - windowStart >= o.windowMs.msecs)
                break;
            if (measuring && !mhzDone && now - windowStart >= (o.windowMs / 2).msecs)
            {
                sampleMhz();
                mhzDone = true;
            }
            ++r;
            immutable wtStart = atomicLoad(f.Wt);
            immutable flip1 = nowTicks();
            atomicStore!(MemoryOrder.rel)(g_flipTicks.v, cast(ulong) flip1);
            setPhase(phPublish, r);
            immutable rr = r;
            if (!waitFor(() {
                foreach (k; g_producers)
                    if (atomicLoad!(MemoryOrder.acq)(g_slots[k].doneRound) != rr)
                        return false;
                return true;
            }))
            {
                failure = "phase-timeout";
                break;
            }
            immutable pubEnd = nowTicks();
            target += roundItems * done;
            atomicStore!(MemoryOrder.rel)(g_target.v, target);
            immutable flip2 = nowTicks();
            atomicStore!(MemoryOrder.rel)(g_flipTicks.v, cast(ulong) flip2);
            setPhase(phConsume, r);
            immutable tgt = target;
            if (!waitFor(() => sumReached(tgt)))
            {
                failure = "phase-timeout";
                break;
            }
            immutable conEnd = nowTicks();
            immutable wtEnd = atomicLoad(f.Wt);
            if (measuring)
            {
                ++rounds;
                pubNs += ticksToNs(pubEnd - flip1);
                gateNs += ticksToNs(flip2 - pubEnd);
                conNs += ticksToNs(conEnd - flip2);
                maxWords = max(maxWords, wtEnd - wtStart);
                sumWords += wtEnd - wtStart;
                scanRound(wtStart, wtEnd, scan);
            }
        }
        if (measuring && base.length)
        {
            auto end = snapshot();
            foreach (k; 0 .. n)
                winItems[k] = end[k] - base[k];
            windowSecs = (MonoTime.currTime - windowStart).total!"nsecs" / 1e9;
        }
        setPhase(failure.length ? phAbort : phFinish);
        immutable B = cast(double) roundItems;
        immutable rPub = pubNs ? B * rounds / (pubNs / 1e9) : 0.0;
        immutable rCon = conNs ? B * rounds / (conNs / 1e9) : 0.0;
        immutable e2eNs = pubNs + conNs + gateNs;
        phased["rounds"] = rounds;
        phased["round_items"] = roundItems;
        phased["share_per_producer"] = g_share;
        phased["R_pub"] = rPub;
        phased["R_con"] = rCon;
        phased["R_e2e"] = e2eNs ? B * rounds / (e2eNs / 1e9) : 0.0;
        phased["R_overlap"] = min(rPub, rCon);
        phased["publish_ns"] = pubNs;
        phased["consume_ns"] = conNs;
        phased["gate_ns"] = gateNs;
        phased["wt_words_per_round_max"] = maxWords;
        phased["wt_words_per_round_mean"] = rounds ? cast(double) sumWords / rounds : 0.0;
        phased["round_budget_words"] = budgetWords;
        phased["poll_interval_ns"] = polls ? cast(double) pollNs / polls : 0.0;
        scanJ["tables"] = scan.tables;
        scanJ["items"] = scan.items;
        scanJ["failed_claims"] = scan.failed;
        scanJ["failed_per_table"] = scan.tables ? cast(double) scan.failed / scan.tables : 0.0;
        scanJ["failed_per_1k_items"] = scan.items ? scan.failed * 1000.0 / scan.items : 0.0;
        scanJ["tprogress_ok"] = scan.progressOk;
        scanJ["executor_ok"] = scan.executorOk;
    }

    foreach (t; threads)
        t.join();
    cs.observedEnd = observed();

    // Pin verdict over every thread plus the controller.
    PinOutcome[] outcomes;
    ProcessorId[] planned, os, oe;
    foreach (k; 0 .. n + 1)
    {
        outcomes ~= g_slots[k].pin;
        planned ~= g_threads[k].cpu.id;
        os ~= g_slots[k].observedStart;
        oe ~= g_slots[k].observedEnd;
    }
    auto verdict = pinVerdict(outcomes, planned, os, oe);

    ulong consumed, produced;
    foreach (k; g_consumers) consumed += atomicLoad(g_slots[k].items);
    foreach (k; g_producers) produced += atomicLoad(g_slots[k].items);

    JSONValue[] consJ, prodJ;
    double[] cItems, pItems;
    string[] cLocs, pLocs;
    double homeC = 0, offC = 0;
    foreach (k; g_consumers)
    {
        auto s = &g_slots[k];
        JSONValue j;
        j["idx"] = g_threads[k].idx;
        j["loc"] = g_threads[k].loc;
        j["items"] = winItems[k];
        j["items_total"] = atomicLoad(s.items);
        j["visits_true"] = s.visitsTrue;
        j["visits_false"] = s.visitsFalse;
        j["empty_visits"] = s.emptyVisits;
        j["empty_visits_per_1k_items"] = atomicLoad(s.items)
            ? s.emptyVisits * 1000.0 / atomicLoad(s.items) : 0.0;
        j["subscribe_retries"] = s.subscribeRetries;
        if (o.mode == Mode.phased)
            j["first_cb_after_flip_ns"] = pctJson(g_firstCb[k]);
        if (o.latEvery)
            j["latency_ns"] = pctJson(ticksToNsArr(g_lat[k].dup));
        consJ ~= j;
        cItems ~= winItems[k];
        cLocs ~= g_threads[k].loc;
        if (g_threads[k].loc == "off") offC += winItems[k];
        else homeC += winItems[k];
    }
    foreach (k; g_producers)
    {
        auto s = &g_slots[k];
        JSONValue j;
        j["idx"] = g_threads[k].idx;
        j["loc"] = g_threads[k].loc;
        j["items"] = winItems[k];
        j["items_total"] = atomicLoad(s.items);
        j["write_calls"] = s.writeCalls;
        j["write_zero"] = s.writeZero;
        j["short_writes"] = s.shortWrites;
        j["items_per_call"] = s.writeCalls ? cast(double) atomicLoad(s.items) / s.writeCalls : 0.0;
        j["phase_overflow"] = s.phaseOverflow;
        prodJ ~= j;
        pItems ~= winItems[k];
        pLocs ~= g_threads[k].loc;
    }
    out_["consumers"] = consJ;
    out_["producers"] = prodJ;

    JSONValue perSec(double x)
    {
        return windowSecs > 0 ? JSONValue(x / windowSecs) : JSONValue(null);
    }
    immutable winConsumed = cast(double) sumOf(winItems, g_consumers) / done;
    agg["window_secs"] = windowSecs;
    agg["rate_consumed"] = o.mode == Mode.phased ? phased["R_e2e"] : perSec(winConsumed);
    agg["rate_window_consumed"] = perSec(winConsumed);
    agg["rate_produced"] = perSec(sumOf(winItems, g_producers));
    agg["home_consumer_rate"] = perSec(homeC / done);
    agg["off_consumer_rate"] = perSec(offC / done);
    agg["consumer_share"] = sharesJson(cItems, cLocs);
    agg["producer_share"] = sharesJson(pItems, pLocs);
    agg["consumed_total"] = consumed;
    agg["produced_total"] = produced;
    agg["callbacks_per_payload"] = done;
    out_["aggregate"] = agg;
    if (o.mode == Mode.phased)
    {
        out_["phased"] = phased;
        out_["claim_scan"] = scanJ;
    }

    out_["config"]["cost_iters"] = g_costIters;
    out_["config"]["iters_per_ns"] = itersPerNs;
    out_["config"]["ring"] = JSONValue([
        "ring_mib": JSONValue(ringMiB), "k": JSONValue(ringK), "ln": JSONValue(f.Ln),
        "seg_cap": JSONValue(f.segCap), "quota_small": JSONValue(quotaSmall),
        "max_small": JSONValue(totalP), "expected_consumers": JSONValue(totalC),
        "small_threshold": JSONValue(f.smallThreshold)]);
    out_["config"]["cf"] = cf;
    out_["config"]["sqcs"] = sqcsOf(cf > 0 ? cast(ulong) cf : 1);
    out_["addresses"] = JSONValue([
        "wt": JSONValue(format("0x%x", cast(size_t) &f.Wt)),
        "cf": JSONValue(format("0x%x", cast(size_t) &f.Cf)),
        "buf": JSONValue(format("0x%x", cast(size_t) f.buf)),
        "ln": JSONValue(f.Ln)]);
    out_["threads"] = threadsJson(n, mhz);

    if (setupFailure.length)
        return finish("invalid", setupFailure);
    if (!verdict.ok)
        return finish("invalid", verdict.reason);
    if (failure.length)
        return finish("invalid", failure);
    if (consumed != produced * done)
        return finish("invalid", "conservation");
    finish("ok", null);
}

long[] ticksToNsArr(long[] xs)
{
    foreach (ref x; xs)
        x = ticksToNs(x);
    return xs;
}

JSONValue threadsJson(size_t n, JSONValue[] mhz)
{
    JSONValue[] r;
    foreach (k; 0 .. n + 1)
    {
        auto j = threadJson(g_threads[k]);
        j["pin_outcome"] = g_slots[k].pinAttempted
            ? JSONValue(g_slots[k].pin.to!string) : JSONValue(null);
        j["observed_start"] = observedJson(g_slots[k].observedStart);
        j["observed_end"] = observedJson(g_slots[k].observedEnd);
        j["mhz_mid"] = mhz.length ? mhz[k] : JSONValue(null);
        r ~= j;
    }
    return JSONValue(r);
}
