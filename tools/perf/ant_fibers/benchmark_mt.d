/++ Multithreaded fiber throughput benchmark.
 +
 + One `FiberGroups` attachment on a `GroupHost` over every accepted LLC
 + group: one `GroupFiberLane` per host Farm, served by the pool's workers
 + through the host's managed lane. The control thread burst-spawns
 + round-robin across lanes (waking the pool once per burst — per-spawn
 + wakes would measure futex traffic, not the scheduler), then waits for the
 + domain to drain. Fibers never suspend, so no cross-worker fiber migration
 + occurs.
 +
 + Second generation re-spawns from the recycling pool: zero GC allocation,
 + zero stack mmap per fiber.
 +
 + Usage: benchmark_mt [tasks] [private|shared] [workers]
 +   private  (default) `farmPerGroup`: one Farm and one lane per accepted
 +            group, each consumed only by that group's workers;
 +   shared   one Farm and one lane consumed by every group's workers
 +            (`HostGroups.all` without `farmPerGroup`): the cross-group A/B
 +            against `private`.
 +   workers  pin exactly this many pool workers, filling one physical group
 +            (cores, then its SMT siblings) before the next; 0 or absent
 +            keeps the host's default of one worker per core everywhere.
 +
 + Port note (GroupHost): the former `FiberLane`/`FiberServicePlan` path is
 + gone. `private` used to run one `FiberDomain` per lane; a `FiberGroups`
 + attachment serves one domain over every lane, so both modes now share a
 + single domain (the admission mutex is per domain, the ready queues per
 + lane). The former `assist` mode (an explicit cross-group assist ring of
 + lane edges) has no GroupHost equivalent and was dropped; `shared` is the
 + cross-group case. The report's `edges=` field became `farms=`. The domain's
 + lifecycle record ring is sized to the burst (`recordRetention = 4 * tasks`),
 + since the default admits only 4096 unretired tasks.
 +/
module ant_fibers.benchmark_mt;

import antfarm;
import ant_fibers;
import core.atomic;
import core.thread : Thread;
import core.time : Duration, MonoTime;
import std.conv : to;
import std.exception : enforce;
import std.stdio : writefln;
import threadpool;

shared long ran;

/// Print one timed phase. A phase whose work had already finished when its
/// timer started, or a zero interval, has no rate: report it as such
/// instead of dividing by (almost) nothing.
void report(string label, size_t count, Duration elapsed, bool measurable = true)
{
    immutable seconds = cast(double) elapsed.total!"nsecs" / 1_000_000_000.0;
    if (!measurable || seconds == 0)
        writefln("%s %s fibers: not measurable (finished before the timer started)",
                 label, count);
    else
        writefln("%s %s fibers in %.9f s: %.2f fibers/s",
                 label, count, seconds, count / seconds);
}

/// The first `count` eligible logical processors in placement order: one
/// physical group at a time, its cores (faster classes first) before its SMT
/// siblings. More workers than processors is an error: the pool pins.
ProcessorId[] pickWorkers(const TopologySnapshot topology, size_t count)
{
    import std.algorithm.sorting : sort;
    auto processors = topology.processors.dup;
    sort!((a, b) => a.llcIndex != b.llcIndex ? a.llcIndex < b.llcIndex
        : a.smtSibling != b.smtSibling ? !a.smtSibling
        : a.efficiencyClass != b.efficiencyClass ? a.efficiencyClass > b.efficiencyClass
        : (a.group != b.group ? a.group < b.group : a.lpIndex < b.lpIndex))(processors);
    ProcessorId[] order;
    foreach (ref p; processors)
        if (topology.isEligible(p)) order ~= ProcessorId.of(p);
    enforce(count <= order.length, "benchmark_mt: more workers than eligible processors");
    return order[0 .. count];
}

void main(string[] args)
{
    immutable taskCount = args.length > 1 ? args[1].to!size_t : 100_000;

    immutable mode = args.length > 2 ? args[2] : "private";
    enforce(mode != "assist",
        "benchmark_mt: the `assist` mode (lane assist edges) has no GroupHost "
        ~ "equivalent; use `shared` for the cross-group case");
    enforce(mode == "private" || mode == "shared",
        "usage: benchmark_mt [tasks] [private|shared] [workers]");

    immutable workers = args.length > 3 ? args[3].to!size_t : 0;
    GroupHostOptions options;
    options.groups = HostGroups.all();
    options.farmPerGroup = mode == "private";
    // The control thread stays unpinned and reserves no CPU, as before.
    options.caller = CallerPlacement.unchanged;
    // Park until woken: the pool's former default, one wake per burst.
    options.idleTimeout = Duration.max;
    if (workers != 0)
    {
        options.pool.processors = pickWorkers(CacheAwarePool.topology(), workers);
        options.pool.skipSmtSiblings = false;
        options.workers = cast(uint) workers;
    }

    // Size each Farm as before: a 2^22-word ring (32 MiB) in 8 segments, one
    // small producer per worker (flush token) plus the control thread, and
    // the small quota capped at 16384. ANT_FIBERS_RING_WORDS overrides the
    // ring length (a power of two, at least 2^18) for ring-size experiments.
    import std.process : environment;
    immutable ln = environment.get("ANT_FIBERS_RING_WORDS", "4194304").to!ulong;
    enum k = 8;
    immutable segCap = ln / k;
    immutable planned = GroupHost.resolve(options).workers.length;
    immutable slots = planned + 1;
    immutable quotaHeadroom = (k - 1) * segCap / slots;
    options.farm.ringMiB = ringMiBFromUlongs(ln);
    options.farm.k = k;
    options.farm.quotaSmall = quotaHeadroom > 16_384 ? 16_384 : quotaHeadroom;

    // A burst holds every task's lifecycle reservation (4 records) until the
    // Director retires it, so the record ring must cover the whole burst
    // (the default, 16384 records, admits 4096 concurrent tasks). Sized at
    // construction, outside every timed region.
    FiberDomainOptions domainOptions;
    domainOptions.recordRetention = 4 * taskCount + 4;
    auto domain = new FiberDomain(domainOptions);
    auto fibers = new FiberGroups(domain);
    options.attachments = [fibers.attachment];
    auto host = new GroupHost(options);
    scope (exit) host.close();
    scope (exit) domain.beginShutdown();
    auto lanes = fibers.lanes;
    domain.reserve(taskCount);
    writefln("workers: %s across %s LLC group(s) hugePages=%s mode=%s farms=%s",
             host.workers, host.groups.length, host.farms[0].usedLargePages, mode,
             host.farms.length);

    void burstSpawn()
    {
        void function() body = { atomicFetchAdd(ran, 1L); };
        foreach (i; 0 .. taskCount)
            domain.spawnOn(lanes[i % lanes.length], body);
        host.wakeAll();
    }

    void waitDrained()
    {
        while (!domain.drained)
            Thread.yield();
    }

    // Workers may drain the burst while it is still being spawned; then the
    // run phase measured nothing.
    auto createStart = MonoTime.currTime;
    burstSpawn();
    auto runStart = MonoTime.currTime;
    immutable runMeasurable = !domain.drained;
    waitDrained();
    auto end = MonoTime.currTime;
    report("create:", taskCount, runStart - createStart);
    report("run:   ", taskCount, end - runStart, runMeasurable);
    report("total: ", taskCount, end - createStart);

    // Second generation: every spawn recycles a pooled task/fiber/stack.
    domain.retire();
    auto warmCreateStart = MonoTime.currTime;
    burstSpawn();
    auto warmRunStart = MonoTime.currTime;
    immutable warmRunMeasurable = !domain.drained;
    waitDrained();
    auto warmEnd = MonoTime.currTime;
    report("create (warm pool):", taskCount, warmRunStart - warmCreateStart);
    report("run    (warm pool):", taskCount, warmEnd - warmRunStart,
           warmRunMeasurable);
    report("total  (warm pool):", taskCount, warmEnd - warmCreateStart);

    assert(atomicLoad(ran) == 2 * taskCount, "fiber executions lost");
    domain.retire();
}
