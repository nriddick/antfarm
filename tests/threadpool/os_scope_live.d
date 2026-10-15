module os_scope_live;

// Live frozen-OS-scope regression (`dub run -c scope-test`). Linux only.
//
// The program re-executes itself under `taskset -c <subset>` (the first
// physical group with eligible CPUs on multi-LLC hosts, otherwise the first
// half of the eligible CPUs), and the child checks the frozen scope, the first-caller regression,
// pin validation and the pool resolution against that subset.

version (linux)
{
import core.atomic : atomicLoad, atomicStore;
import core.thread : Thread;
import core.time : msecs;
import std.algorithm : canFind, startsWith;
import std.file : thisExePath;
import std.process : ProcessException, execute;
import std.stdio : stderr, writeln;

import threadpool;

shared int gRan;

bool markRan(WorkerSelf*) nothrow @nogc @system
{
    atomicStore(gRan, 1);
    return false;
}

int main(string[] args)
{
    foreach (a; args[1 .. $])
        if (a.startsWith("--child="))
            return child(parseCpuList(a["--child=".length .. $]));

    auto snap = discover();
    ProcessorId[] eligible;
    foreach (ref p; snap.processors)
        if (snap.isEligible(p))
            eligible ~= ProcessorId.of(p);
    if (eligible.length < 2)
    {
        writeln("SKIP scope-test: needs at least 2 eligible CPUs");
        return 0;
    }
    ProcessorId[] subset;
    if (snap.eligibleLlcCount >= 2)
        foreach (g; 0 .. snap.llcCount)
        {
            subset = snap.eligibleProcessorsInLlc(cast(ushort) g);
            if (subset.length) break;
        }
    else
        subset = eligible[0 .. eligible.length / 2];
    auto list = formatCpuList(subset);
    try
    {
        auto r = execute(["taskset", "-c", list, thisExePath, "--child=" ~ list]);
        stderr.write(r.output);
        return r.status;
    }
    catch (ProcessException e)
    {
        writeln("SKIP scope-test: taskset unavailable: ", e.msg);
        return 0;
    }
}

int child(ProcessorId[] expected)
{
    import core.sys.linux.sched : CPU_ALLOC, CPU_ALLOC_SIZE, CPU_FREE, cpu_mask,
        sched_setaffinity;

    // 1-2. The launch mask was captured before main: narrow main before
    // discovery, and the snapshot's scope must not follow it.
    assert(cachedSnapshotIsNull(), "discovery ran before the first-caller check");
    {
        auto lp = expected[0].lp;
        auto count = cast(size_t) lp + 1;
        auto setsize = CPU_ALLOC_SIZE(count);
        auto set = CPU_ALLOC(count);
        scope (exit) CPU_FREE(set);
        auto words = (cast(cpu_mask*) set)[0 .. setsize / cpu_mask.sizeof];
        words[] = 0;
        words[lp / (cpu_mask.sizeof * 8)] |= cast(cpu_mask) 1 << (lp % (cpu_mask.sizeof * 8));
        assert(sched_setaffinity(0, setsize, set) == 0);
    }
    assert(liveOsMask() == [expected[0]], "liveOsMask must report main's narrowed mask");
    auto snap = discover();
    assert(snap.osScopeSource == OsScopeSource.processLaunch);
    assert(snap.osScopeAllowed == expected, "frozen scope differs from the taskset set");
    ProcessorId[] eligible;
    foreach (ref p; snap.processors)
    {
        assert(p.affinityExcluded == !snap.inOsScope(ProcessorId.of(p)));
        if (snap.isEligible(p))
            eligible ~= ProcessorId.of(p);
    }
    assert(eligible == expected, "discovery followed the first caller's mask");

    bool late;
    try
        scopeOsToCallingThread();
    catch (Exception)
        late = true;
    assert(late, "scopeOsToCallingThread must refuse after discovery");

    // 3. Pin validation.
    ProcessorId excluded;
    bool haveExcluded;
    foreach (ref p; snap.processors)
        if (!expected.canFind(ProcessorId.of(p)))
        {
            excluded = ProcessorId.of(p);
            haveExcluded = true;
            break;
        }
    assert(haveExcluded);
    assert(pinProcessor(excluded) == PinOutcome.outsideOsScope);
    assert(pinProcessor(ProcessorId(0, 60000)) == PinOutcome.unknownProcessor);
    assert(liveOsMask() == [expected[0]], "a rejected pin must not change the mask");

    immutable homeGroup = snap.byId(expected[0]).llcIndex;
    immutable wholeGroup = snap.llcCount >= 2 && snap.processorsInLlc(homeGroup) == expected;

    // 4. A default pool starts only inside the scope.
    PoolOptions opt;
    opt.workerBody = &markRan;
    auto pool = new CacheAwarePool(opt);
    auto preview = pool.resolve();
    pool.start();
    {
        scope (exit) pool.shutdown();
        auto res = pool.resolution;
        assert(res.started && res.sameSelection(preview));
        assert(res.workers == expected);
        assert(pool.workerCount == expected.length);
        foreach (ref c; res.cpus)
        {
            assert(!(c.reasons & Reject.notRequested));
            if (expected.canFind(c.id))
                assert(c.accepted && c.start == StartState.pinned);
            else
                assert((c.reasons & Reject.osExcluded) && c.start == StartState.notStarted);
        }

        // 5. Group level, on multi-LLC hardware only.
        if (wholeGroup)
        {
            assert(res.acceptedGroupCount == 1);
            assert(res.groups[homeGroup].accepted == expected.length);
            foreach (g; res.groups)
                if (g.llcIndex != homeGroup)
                    assert(g.common == Reject.osExcluded && g.accepted == 0);
        }
        writeln(res.summary());
        foreach (_; 0 .. 400)
        {
            if (atomicLoad(gRan)) break;
            Thread.sleep(msecs(5));
        }
        assert(atomicLoad(gRan), "no worker ran");
    }
    writeln("PASS scope-test: scope ", formatCpuList(expected),
        wholeGroup ? " (group-level checks on)" : " (group-level checks skipped)");
    return 0;
}

bool cachedSnapshotIsNull()
{
    import threadpool.topology : cachedSnapshot;
    return cachedSnapshot() is null;
}
}
else
{
    void main()
    {
        import std.stdio : writeln;
        writeln("SKIP scope-test: Linux only");
    }
}
