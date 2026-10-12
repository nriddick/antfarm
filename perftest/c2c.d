/++
 + Core-to-core calibration for the off-CCD harness. No Farm.
 +
 +   ./c2c --mode pingpong --pairs classes
 +   ./c2c --mode rmw --rmw-configs 1:0,7:1,7:8
 +   ./c2c --mode pull
 +   ./c2c --mode pingpong --pair 0,8     (debug: unplanned pair)
 +
 + CPUs come from `placement.planRoles` (eligible primaries of the home and
 + remote L3 groups) unless `--pair` names two directly; those are pinned
 + as given, so the frozen OS scope is what refuses an out-of-scope CPU.
 + Output is JSON lines: a topology header, one object per measurement, and
 + per-class summaries. Exit 2 on a usage error or any invalid measurement.
 +/
module c2c;

import placement;
import threadpool.os_scope : liveOsMask;
import threadpool.topology : CpuListException, ProcessorId, TopologySnapshot, discover,
    parseCpuList;

import core.atomic;
import core.thread;
import core.time;
import std.algorithm : map, sort, sum;
import std.array : array, split;
import std.conv : to;
import std.json : JSONValue;
import std.math : sqrt;
import std.stdio : stderr, stdout, writeln;

struct Opts
{
    string mode = "pingpong";
    uint batches = 7;
    uint iters = 200_000;
    uint ms = 200;
    string pairs = "all";
    uint[2][] rmwConfigs = [[1, 0], [2, 0], [4, 0], [7, 0], [7, 1], [7, 4], [7, 8], [4, 4]];
    ProcessorId[] pair;
    ProcessorId[] cpus;
    int homeLlc = -1;
}

__gshared const(TopologySnapshot)* g_snap;
__gshared bool g_anyInvalid;

align(128) struct Line
{
    shared ulong v;
}

// Start gate and per-run lines; each on its own 128-byte line.
__gshared Line g_go;      // 0 wait, 1 run, 2 abort
__gshared Line g_ready;
__gshared Line g_stop;
__gshared Line g_ping;
__gshared Line g_rmw;
__gshared Line g_flag;

/// One pinned worker: pins, reports, then waits for the gate.
align(128) struct Worker
{
    ProcessorId cpu;
    PinOutcome pin;
    ProcessorId observedStart = unobserved, observedEnd = unobserved;
    ulong ops;
    double[] samples;
    uint role;
}

enum unobserved = ProcessorId(ushort.max, ushort.max);

ProcessorId observed()
{
    auto p = g_snap.current();
    return p is null ? unobserved : ProcessorId.of(*p);
}

JSONValue observedJson(ProcessorId id)
{
    return id == unobserved ? JSONValue(null) : JSONValue(cpuText(id));
}

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

long nowNs()
{
    return (MonoTime.currTime - MonoTime.zero).total!"nsecs";
}

/// Pin, record, join the ready count and wait for the gate. False on abort.
bool enter(Worker* w)
{
    auto p = g_snap.byId(w.cpu);
    w.pin = p is null ? PinOutcome.unknownProcessor : pinProcessor(*p);
    if (w.pin == PinOutcome.pinned)
        w.observedStart = observed();
    atomicOp!"+="(g_ready.v, 1);
    ulong go;
    while ((go = atomicLoad!(MemoryOrder.acq)(g_go.v)) == 0)
        pause();
    return go == 1;
}

void leave(Worker* w)
{
    w.observedEnd = observed();
}

final class Job
{
    Worker* w;
    void delegate(Worker*) fn;
    this(Worker* w, void delegate(Worker*) fn) { this.w = w; this.fn = fn; }
    void run() { fn(w); }
}

/// Start `fns` on threads, wait until each has pinned, then open or abort
/// the gate. Returns false (and joins) when any pin failed.
bool launch(Worker[] ws, void delegate(Worker*)[] fns, ref Thread[] threads)
{
    atomicStore(g_go.v, 0UL);
    atomicStore(g_ready.v, 0UL);
    atomicStore(g_stop.v, 0UL);
    // Stable job objects: a closure over loop locals would alias every
    // thread to the last worker.
    threads = new Thread[ws.length];
    auto jobs = new Job[ws.length];
    foreach (i; 0 .. ws.length)
    {
        jobs[i] = new Job(&ws[i], fns[i]);
        threads[i] = new Thread(&jobs[i].run);
        threads[i].start();
    }
    while (atomicLoad!(MemoryOrder.acq)(g_ready.v) < ws.length)
        Thread.yield();
    bool ok = true;
    foreach (ref w; ws)
        if (w.pin != PinOutcome.pinned)
            ok = false;
    atomicStore!(MemoryOrder.rel)(g_go.v, ok ? 1UL : 2UL);
    return ok;
}

void joinAll(Thread[] threads)
{
    foreach (t; threads)
        t.join();
}

/// Pin fields of a measurement plus its verdict; sets status/reason.
void addPins(ref JSONValue j, Worker[] ws)
{
    PinOutcome[] outcomes;
    ProcessorId[] planned, s, e;
    JSONValue[] cpus;
    foreach (ref w; ws)
    {
        outcomes ~= w.pin;
        planned ~= w.cpu;
        s ~= w.observedStart;
        e ~= w.observedEnd;
        cpus ~= JSONValue(["cpu": JSONValue(cpuText(w.cpu)),
            "pin_outcome": JSONValue(w.pin.to!string),
            "observed_start": observedJson(w.observedStart),
            "observed_end": observedJson(w.observedEnd)]);
    }
    auto v = pinVerdict(outcomes, planned, s, e);
    j["threads"] = cpus;
    j["status"] = v.ok ? "ok" : "invalid";
    j["reason"] = v.ok ? JSONValue(null) : JSONValue(v.reason);
    if (!v.ok)
        g_anyInvalid = true;
}

JSONValue spread(double[] xs)
{
    auto s = xs.dup.sort().array;
    JSONValue j;
    j["n"] = s.length;
    if (s.length == 0)
        return j;
    immutable mean = s.sum / s.length;
    double var = 0;
    foreach (x; s)
        var += (x - mean) * (x - mean);
    j["median"] = s.length % 2 ? s[$ / 2] : (s[$ / 2 - 1] + s[$ / 2]) / 2;
    j["min"] = s[0];
    j["max"] = s[$ - 1];
    j["stddev"] = s.length > 1 ? sqrt(var / (s.length - 1)) : 0.0;
    return j;
}

void emit(JSONValue j)
{
    writeln(j.toString);
    stdout.flush();
}

JSONValue skipped(string kind, string reason)
{
    JSONValue j;
    j["kind"] = kind;
    j["status"] = "skipped";
    j["reason"] = reason;
    return j;
}

// ---- pingpong -------------------------------------------------------------

/// One-way ns per batch for the pair (a, b): A writes odd values, B answers
/// with the next even one on the same line.
JSONValue pingpong(ProcessorId a, ProcessorId b, string cls, ref const Opts o)
{
    Worker[2] ws;
    ws[0].cpu = a;
    ws[1].cpu = b;
    immutable iters = o.iters, batches = o.batches;
    void side(Worker* w)
    {
        if (!enter(w))
            return;
        immutable first = w is &ws[0];
        foreach (bi; 0 .. batches)
        {
            immutable base = cast(ulong) bi * iters * 2;
            immutable t0 = nowNs();
            foreach (i; 0 .. iters)
            {
                immutable mine = base + 2 * i + (first ? 1 : 2);
                if (first)
                {
                    atomicStore!(MemoryOrder.rel)(g_ping.v, mine);
                    while (atomicLoad!(MemoryOrder.acq)(g_ping.v) != mine + 1)
                        pause();
                }
                else
                {
                    while (atomicLoad!(MemoryOrder.acq)(g_ping.v) != mine - 1)
                        pause();
                    atomicStore!(MemoryOrder.rel)(g_ping.v, mine);
                }
            }
            if (first)
                w.samples ~= cast(double)(nowNs() - t0) / iters / 2;
        }
        leave(w);
    }
    atomicStore(g_ping.v, 0UL);
    Thread[] threads;
    launch(ws[], [&side, &side], threads);
    joinAll(threads);

    JSONValue j;
    j["kind"] = "pingpong";
    j["class"] = cls;
    j["a"] = cpuText(a);
    j["b"] = cpuText(b);
    j["batches"] = batches;
    j["iters"] = iters;
    addPins(j, ws[]);
    j["oneway_ns"] = spread(ws[0].samples);
    return j;
}

string classOf(ProcessorId a, ProcessorId b)
{
    auto pa = g_snap.byId(a), pb = g_snap.byId(b);
    if (pa is null || pb is null) return "unknown";
    if (pa.group == pb.group && pa.packageIndex == pb.packageIndex && pa.coreIndex == pb.coreIndex)
        return "smt";
    return pa.llcIndex == pb.llcIndex ? "intra-llc" : "cross-llc";
}

void runPingpong(ref const Opts o, ProcessorId[] home, ProcessorId[] remote, bool singleLlc)
{
    ProcessorId[2][] smt, intra, cross;
    ProcessorId sibOf(ProcessorId id)
    {
        auto s = eligibleSiblings(*g_snap, id, o.cpus);
        return s.length ? s[0] : unobserved;
    }
    auto prims = home ~ remote;
    if (o.pairs == "classes")
    {
        if (home.length && sibOf(home[0]) != unobserved)
            smt ~= [home[0], sibOf(home[0])];
        foreach (ij; [[size_t(0), 1], [size_t(0), home.length - 1], [size_t(1), 2]])
            if (home.length > 2)
                intra ~= [home[ij[0]], home[ij[1]]];
        if (remote.length)
            foreach (k; [0, 1, remote.length - 1])
                if (k < home.length && k < remote.length)
                    cross ~= [home[k], remote[k]];
    }
    else
    {
        foreach (i, a; prims)
        {
            auto s = sibOf(a);
            if (s != unobserved)
                smt ~= [a, s];
            foreach (b; prims[i + 1 .. $])
                (classOf(a, b) == "cross-llc" ? cross : intra) ~= [a, b];
        }
    }

    double[][string] medians;
    foreach (cls; ["smt", "intra-llc", "cross-llc"])
    {
        auto list = cls == "smt" ? smt : cls == "intra-llc" ? intra : cross;
        if (list.length == 0)
        {
            auto j = skipped("pingpong-class", cls == "cross-llc" && singleLlc
                ? "single-llc" : "no-pairs");
            j["class"] = cls;
            emit(j);
            continue;
        }
        foreach (pr; list)
        {
            auto j = pingpong(pr[0], pr[1], cls, o);
            emit(j);
            if (j["status"].str == "ok")
                medians[cls] ~= j["oneway_ns"]["median"].floating;
        }
        auto summary = spread(medians.get(cls, null));
        // A class whose every pair was invalid has nothing to summarize.
        immutable valid = medians.get(cls, null).length != 0;
        JSONValue j;
        j["kind"] = "pingpong-class";
        j["status"] = valid ? "ok" : "invalid";
        j["reason"] = valid ? JSONValue(null) : JSONValue("no-valid-pairs");
        j["class"] = cls;
        j["pairs"] = list.length;
        j["oneway_ns_over_pair_medians"] = summary;
        if (!valid)
            g_anyInvalid = true;
        emit(j);
    }
}

// ---- rmw ------------------------------------------------------------------

void runRmw(ref const Opts o, ProcessorId[] home, ProcessorId[] remote, bool singleLlc)
{
    foreach (cfg; o.rmwConfigs)
    {
        immutable h = cfg[0], r = cfg[1];
        string name = h.to!string ~ ":" ~ r.to!string;
        if (r > 0 && singleLlc)
        {
            auto j = skipped("rmw", "single-llc");
            j["config"] = name;
            emit(j);
            continue;
        }
        if (h > home.length || r > remote.length || h + r == 0)
        {
            auto j = skipped("rmw", "insufficient-primaries");
            j["config"] = name;
            emit(j);
            continue;
        }
        auto ws = new Worker[h + r];
        foreach (i; 0 .. h) ws[i].cpu = home[i];
        foreach (i; 0 .. r) ws[h + i].cpu = remote[i];
        void hammer(Worker* w)
        {
            if (!enter(w))
                return;
            ulong n;
            while (atomicLoad!(MemoryOrder.raw)(g_stop.v) == 0)
            {
                atomicFetchAdd(g_rmw.v, 1UL);
                ++n;
            }
            w.ops = n;
            leave(w);
        }
        atomicStore(g_rmw.v, 0UL);
        Thread[] threads;
        auto fns = new void delegate(Worker*)[ws.length];
        fns[] = &hammer;
        immutable ok = launch(ws, fns, threads);
        immutable t0 = nowNs();
        if (ok)
            Thread.sleep(o.ms.msecs);
        atomicStore(g_stop.v, 1UL);
        immutable secs = (nowNs() - t0) / 1e9;
        joinAll(threads);
        ulong total;
        foreach (ref w; ws)
            total += w.ops;
        JSONValue j;
        j["kind"] = "rmw";
        j["config"] = name;
        j["home"] = h;
        j["remote"] = r;
        addPins(j, ws);
        j["mops"] = total / secs / 1e6;
        j["ns_per_op"] = total ? secs * 1e9 / total : 0.0;
        emit(j);
    }
}

// ---- pull -----------------------------------------------------------------

/// A writer fills 1 MiB and release-stores a flag; the reader acquires it and
/// reads one word per 64-byte line. Reader time per batch.
void runPull(ref const Opts o, ProcessorId[] home, ProcessorId[] remote, bool singleLlc)
{
    enum bytes = 1 << 20;
    enum words = bytes / 8;
    auto buf = new ulong[words];
    struct Dir { string name; ProcessorId w, r; bool needRemote; }
    Dir[] dirs;
    if (home.length >= 2)
        dirs ~= Dir("H->H", home[0], home[1], false);
    dirs ~= Dir("H->R", home.length ? home[0] : ProcessorId.init,
        remote.length ? remote[0] : ProcessorId.init, true);
    dirs ~= Dir("R->H", remote.length ? remote[0] : ProcessorId.init,
        home.length ? home[0] : ProcessorId.init, true);
    foreach (d; dirs)
    {
        if (d.needRemote && (singleLlc || remote.length == 0 || home.length == 0))
        {
            auto j = skipped("pull", singleLlc ? "single-llc" : "no-remote-primaries");
            j["direction"] = d.name;
            emit(j);
            continue;
        }
        Worker[2] ws;
        ws[0].cpu = d.w;
        ws[1].cpu = d.r;
        immutable batches = o.batches;
        void writer(Worker* w)
        {
            if (!enter(w))
                return;
            foreach (b; 0 .. batches)
            {
                while (atomicLoad!(MemoryOrder.acq)(g_flag.v) != 2 * b)
                    pause();
                foreach (i; 0 .. words)
                    buf[i] = b + i;
                atomicStore!(MemoryOrder.rel)(g_flag.v, 2UL * b + 1);
            }
            leave(w);
        }
        void reader(Worker* w)
        {
            if (!enter(w))
                return;
            ulong sink;
            foreach (b; 0 .. batches)
            {
                while (atomicLoad!(MemoryOrder.acq)(g_flag.v) != 2 * b + 1)
                    pause();
                immutable t0 = nowNs();
                for (size_t i = 0; i < words; i += 8)
                    sink += atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*) &buf[i]);
                w.samples ~= cast(double)(nowNs() - t0);
                atomicStore!(MemoryOrder.rel)(g_flag.v, 2UL * b + 2);
            }
            w.ops = sink;
            leave(w);
        }
        atomicStore(g_flag.v, 0UL);
        Thread[] threads;
        launch(ws[], [&writer, &reader], threads);
        joinAll(threads);
        JSONValue j;
        j["kind"] = "pull";
        j["direction"] = d.name;
        j["bytes"] = bytes;
        addPins(j, ws[]);
        j["gbps"] = spread(ws[1].samples.map!(ns => bytes / ns).array);
        j["ns_per_line"] = spread(ws[1].samples.map!(ns => ns / (bytes / 64)).array);
        emit(j);
    }
}

// ---- main -----------------------------------------------------------------

int usage(string msg)
{
    stderr.writeln("c2c: ", msg);
    stderr.writeln("usage: c2c [--mode pingpong|rmw|pull] [--batches N] [--iters N] [--ms N]",
        " [--pairs all|classes] [--rmw-configs H:R,...] [--pair A,B] [--cpus LIST]",
        " [--home-llc auto|N]");
    return 2;
}

int main(string[] args)
{
    Opts o;
    bool pairGiven;
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
            case "--mode": o.mode = next(); break;
            case "--batches": o.batches = next().to!uint; break;
            case "--iters": o.iters = next().to!uint; break;
            case "--ms": o.ms = next().to!uint; break;
            case "--pairs": o.pairs = next(); break;
            case "--rmw-configs":
                o.rmwConfigs = null;
                foreach (c; next().split(","))
                {
                    auto hr = c.split(":");
                    if (hr.length != 2)
                        throw new Exception("bad --rmw-configs item " ~ c);
                    o.rmwConfigs ~= [hr[0].to!uint, hr[1].to!uint];
                }
                break;
            case "--pair": o.pair = parseCpuList(next()); pairGiven = true; break;
            case "--cpus": o.cpus = parseCpuList(next()); break;
            case "--home-llc":
                auto v = next();
                o.homeLlc = v == "auto" ? -1 : v.to!int;
                break;
            default:
                return usage("unknown argument " ~ a);
            }
        }
    }
    catch (CpuListException e)
        return usage(e.msg);
    catch (Exception e)
        return usage(e.msg);
    if (o.mode != "pingpong" && o.mode != "rmw" && o.mode != "pull")
        return usage("bad --mode " ~ o.mode);
    if (o.pairs != "all" && o.pairs != "classes")
        return usage("bad --pairs " ~ o.pairs);
    if (pairGiven && (o.pair.length != 2 || o.mode != "pingpong"))
        return usage("--pair takes exactly two CPUs and --mode pingpong");
    if (o.batches == 0 || o.iters == 0)
        return usage("--batches and --iters must be positive");

    auto snap = discover();
    g_snap = &snap;
    auto live = liveOsMask();
    RoleRequest req;
    req.homeLlc = o.homeLlc;
    req.cpus = o.cpus;
    auto plan = planRoles(snap, req);

    JSONValue header;
    header["kind"] = "topology";
    header["mode"] = o.mode;
    header["topology"] = topologyJson(snap, plan, live);
    header["cpus_restriction"] = o.cpus.length ? JSONValue(formatCpuListOf(o.cpus)) : JSONValue(null);
    emit(header);

    if (pairGiven)
    {
        auto j = pingpong(o.pair[0], o.pair[1], classOf(o.pair[0], o.pair[1]), o);
        emit(j);
        return j["status"].str == "ok" ? 0 : 2;
    }
    if (!plan.ok)
    {
        emit(skipped(o.mode, plan.reason));
        return 0;
    }
    auto home = eligiblePrimaries(snap, plan.home, o.cpus);
    auto remote = plan.remote >= 0
        ? eligiblePrimaries(snap, cast(ushort) plan.remote, o.cpus) : null;
    if (o.mode == "pingpong")
        runPingpong(o, home, remote, plan.singleLlc);
    else if (o.mode == "rmw")
        runRmw(o, home, remote, plan.singleLlc);
    else
        runPull(o, home, remote, plan.singleLlc);
    return g_anyInvalid ? 2 : 0;
}

string formatCpuListOf(const(ProcessorId)[] ids)
{
    import threadpool.topology : formatCpuList;

    return formatCpuList(ids.dup.sort().array);
}
