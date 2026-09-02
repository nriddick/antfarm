/++
 + Actor-wave generation benchmark.
 +
 + Two independent actor sets run under one top-down committed generation.
 + The benchmark compares one orchestration Fiber producing both waves with
 + two producer Fibers, one per wave. The two-Fiber case rendezvous only to
 + commit the next public generation after both waves have finished.
 +
 + Actor work is deliberately an identity projection. Each actor reads its
 + active public cache line, writes the same value through its private cache
 + line, and fills the inactive public cache line. This represents the
 + private/two-public memory traffic without adding game logic.
 +
 + `--smt` adds sibling logical processors after the physical-core set.
 + `--sweep` runs every worker-count prefix of the selected topology.
 +/
module actor_wave;

import antfarm;
import actors;
import antfarm_fibers;
import core.atomic : MemoryOrder, atomicFetchAdd, atomicLoad, atomicStore;
import core.time : MonoTime, minutes;
import std.conv : to;
import std.exception : enforce;
import std.stdio : writefln;
import threadpool;

enum size_t cacheLineSize = 64;

align(64) struct PaddedWord
{
    ulong value;
    ubyte[cacheLineSize - ulong.sizeof] padding;
}
static assert(PaddedWord.sizeof == cacheLineSize);
static assert(PaddedWord.alignof == cacheLineSize);

/// One private cache line and two public projection cache lines. Generation
/// selection is top-down rather than repeated in every actor.
align(64) struct ProjectionActor
{
    PaddedWord privateState;
    PaddedWord[2] publicState;
}
static assert(ProjectionActor.sizeof == 3 * cacheLineSize);
static assert(ProjectionActor.alignof == cacheLineSize);

align(64) struct PublishedGeneration
{
    shared ulong value;
    ubyte[cacheLineSize - ulong.sizeof] padding;
}
static assert(PublishedGeneration.sizeof == cacheLineSize);

align(64) struct PairBarrier
{
    shared uint arrivals;
    ubyte[cacheLineSize - uint.sizeof] padding;
}
static assert(PairBarrier.sizeof == cacheLineSize);

private __gshared PublishedGeneration publishedGeneration;

/// Autonomous activation is unused by this benchmark.
private void dormantActor(scope ref ActorBorrow!ProjectionActor,
        scope ref ActorContext) nothrow @nogc @system
{
}

/// Preserve the visible value while touching all three modeled state lines.
private void identityProjection(scope ref ActorBorrow!ProjectionActor actor)
        nothrow @nogc @system
{
    immutable generation = atomicLoad!(MemoryOrder.acq)(
        publishedGeneration.value);
    immutable active = cast(size_t)(generation & 1UL);
    immutable staging = active ^ 1;
    immutable visible = actor.value.publicState[active].value;
    actor.value.privateState.value = visible;
    actor.value.publicState[staging].value = actor.value.privateState.value;
}

private final class OneFiberRun
{
    AntFarm* farm;
    Token producer;
    ActorHandle!ProjectionActor[][] sets;
    ActorWave[2] waves;
    ActorWaveTrigger[2] triggers;
    uint avgCost;

    ulong cooperativeYields;
    ulong physicalTables;
    long elapsedNsecs;

    this(FiberDomain domain, AntFarm* farm, Token producer,
            ActorHandle!ProjectionActor[][] sets, uint avgCost)
    {
        this.farm = farm;
        this.producer = producer;
        this.sets = sets;
        this.avgCost = avgCost;
        foreach (ref trigger; triggers)
            trigger = new ActorWaveTrigger(domain);
    }

    void yieldToWorkers()
    {
        ++cooperativeYields;
        FiberDomain.yieldReady();
    }

    void runGeneration()
    {
        foreach (i; 0 .. waves.length)
            waves[i].begin(farm, triggers[i].hook);

        size_t[2] offsets;
        while (offsets[0] != sets[0].length
                || offsets[1] != sets[1].length)
        {
            bool madeProgress;
            foreach (i; 0 .. 2)
            {
                if (offsets[i] == sets[i].length) continue;
                immutable written = waves[i].publish!identityProjection(
                    sets[i][offsets[i] .. $], producer, avgCost);
                enforce(!waves[i].handle.failed,
                    "actor-wave publication rejected an actor");
                if (written != 0)
                {
                    offsets[i] += written;
                    madeProgress = true;
                }
            }
            if (!madeProgress) yieldToWorkers();
        }

        ActorWaveHandle[2] completions;
        foreach (i; 0 .. 2)
            completions[i] = waves[i].seal();
        foreach (trigger; triggers)
            trigger.waitNext();
        enforce(!completions[0].failed && !completions[1].failed,
            "actor wave failed after publication");

        foreach (completion; completions)
            physicalTables += completion.length;
        immutable current = atomicLoad!(MemoryOrder.acq)(
            publishedGeneration.value);
        atomicStore!(MemoryOrder.rel)(publishedGeneration.value, current + 1);
    }

    void run(size_t warmupGenerations, size_t measuredGenerations)
    {
        foreach (_; 0 .. warmupGenerations)
            runGeneration();
        cooperativeYields = 0;
        physicalTables = 0;
        auto started = MonoTime.currTime;
        foreach (_; 0 .. measuredGenerations)
            runGeneration();
        elapsedNsecs = (MonoTime.currTime - started).total!"nsecs";
    }
}

private final class TwoFiberRun
{
    AntFarm* farm;
    Token[2] producers;
    ActorHandle!ProjectionActor[][] sets;
    ActorWave[2] waves;
    ActorWaveTrigger[2] triggers;
    FiberGenerationTrigger[2] generationPublished;
    uint avgCost;
    PairBarrier generationBarrier;
    PairBarrier startBarrier;
    shared uint startOpen;

    ulong[2] cooperativeYields;
    ulong[2] physicalTables;
    long startedTicks;
    long endedTicks;

    this(FiberDomain domain, AntFarm* farm, Token producerA, Token producerB,
            ActorHandle!ProjectionActor[][] sets, uint avgCost)
    {
        this.farm = farm;
        producers[0] = producerA;
        producers[1] = producerB;
        this.sets = sets;
        this.avgCost = avgCost;
        foreach (ref trigger; triggers)
            trigger = new ActorWaveTrigger(domain);
        foreach (ref trigger; generationPublished)
            trigger = new FiberGenerationTrigger(domain);
    }

    void yieldToWorkers(size_t setIndex)
    {
        ++cooperativeYields[setIndex];
        FiberDomain.yieldReady();
    }

    void awaitMeasuredStart(size_t setIndex)
    {
        immutable before = atomicFetchAdd!(MemoryOrder.acq_rel)(
            startBarrier.arrivals, 1u);
        enforce(before < 2, "measurement start barrier overflow");
        if (before == 1)
        {
            startedTicks = MonoTime.currTime.ticks;
            atomicStore!(MemoryOrder.rel)(startOpen, 1u);
        }
        else
        {
            while (atomicLoad!(MemoryOrder.acq)(startOpen) == 0)
                yieldToWorkers(setIndex);
        }
        cooperativeYields[setIndex] = 0;
        physicalTables[setIndex] = 0;
    }

    void runWave(size_t setIndex, ulong target, bool finalGeneration)
    {
        auto wave = &waves[setIndex];
        wave.begin(farm, triggers[setIndex].hook);
        size_t offset;
        while (offset != sets[setIndex].length)
        {
            immutable written = wave.publish!identityProjection(
                sets[setIndex][offset .. $], producers[setIndex], avgCost);
            enforce(!wave.handle.failed,
                "independent actor-wave publication rejected an actor");
            if (written == 0)
                yieldToWorkers(setIndex);
            else
                offset += written;
        }

        auto completion = wave.seal();
        triggers[setIndex].waitNext();
        enforce(!completion.failed,
            "independent actor wave failed after publication");
        physicalTables[setIndex] += completion.length;

        immutable before = atomicFetchAdd!(MemoryOrder.acq_rel)(
            generationBarrier.arrivals, 1u);
        enforce(before < 2, "generation barrier arrival overflow");
        if (before == 1)
        {
            atomicStore!(MemoryOrder.raw)(generationBarrier.arrivals, 0u);
            if (finalGeneration)
                endedTicks = MonoTime.currTime.ticks;
            atomicStore!(MemoryOrder.rel)(publishedGeneration.value, target);
            generationPublished[setIndex ^ 1].advance();
        }
        else
        {
            generationPublished[setIndex].waitNext();
            enforce(atomicLoad!(MemoryOrder.acq)(publishedGeneration.value)
                    >= target, "generation event preceded publication");
        }
    }

    void runSet(size_t setIndex, size_t warmupGenerations,
            size_t measuredGenerations)
    {
        foreach (generation; 1 .. warmupGenerations + 1)
            runWave(setIndex, cast(ulong) generation, false);
        awaitMeasuredStart(setIndex);
        foreach (generation; 1 .. measuredGenerations + 1)
        {
            immutable target = cast(ulong)(warmupGenerations + generation);
            runWave(setIndex, target,
                generation == measuredGenerations);
        }
    }

    @property long elapsedNsecs() const
    {
        return (endedTicks - startedTicks) * 1_000_000_000L
            / MonoTime.ticksPerSecond;
    }
}

private ushort[] pickWorkerLps(size_t maximum, bool includeSmt)
{
    auto topology = CacheAwarePool.topology();
    ushort[] result;
    immutable passes = includeSmt ? 2 : 1;
    foreach (pass; 0 .. passes)
    {
        foreach (ref processor; topology.processors)
        {
            version (linux)
                if (processor.parkedAtDiscovery) continue;
            if (processor.smtSibling != (pass == 1)) continue;
            result ~= processor.lpIndex;
            if (maximum != 0 && result.length == maximum)
                return result;
        }
    }
    return result;
}

private size_t countSmtWorkers(scope const(ushort)[] workerLps)
{
    auto topology = CacheAwarePool.topology();
    size_t result;
    foreach (lp; workerLps)
    {
        auto processor = topology.byLp(0, lp);
        if (processor !is null && processor.smtSibling) ++result;
    }
    return result;
}

private void joinOrThrow(FiberTask task)
{
    auto joined = task.handle.join(10.minutes);
    if (joined.status == FiberJoinStatus.failed && joined.failure !is null)
        throw joined.failure;
    enforce(joined.status == FiberJoinStatus.completed,
        "orchestration Fiber did not complete");
}

private void retireActors(ActorOwner!ProjectionActor[] owners)
{
    foreach (ref owner; owners)
    {
        enforce(owner.requestRetire() == ActorRetireResult.requested,
            "actor retirement failed after completed benchmark waves");
        enforce(owner.retired, "idle actor did not retire synchronously");
        enforce(owner.reclaim() == ActorReclaimResult.reclaimed,
            "retired actor could not be reclaimed");
    }
}

private void report(string label, size_t actorsPerSet,
        size_t measuredGenerations, long elapsedNsecs, ulong physicalTables,
        ulong cooperativeYields, ulong resumes, ulong migrations)
{
    immutable elapsedSeconds = cast(double) elapsedNsecs / 1.0e9;
    immutable actorDispatches = cast(double) actorsPerSet * 2.0
        * cast(double) measuredGenerations;
    writefln("\n%s", label);
    writefln("  elapsed: %.6f s", elapsedSeconds);
    writefln("  rate: %.2f generations/s  %.2f waves/s  %.2f actors/s",
        measuredGenerations / elapsedSeconds,
        measuredGenerations * 2.0 / elapsedSeconds,
        actorDispatches / elapsedSeconds);
    writefln("  cost: %.2f ns/actor  tables=%s (%.2f/generation)",
        cast(double) elapsedNsecs / actorDispatches,
        physicalTables,
        cast(double) physicalTables / measuredGenerations);
    writefln("  orchestration: lifetime-resumes=%s lifetime-migrations=%s backpressure-yields=%s (%.2f/generation)",
        resumes, migrations, cooperativeYields,
        cast(double) cooperativeYields / measuredGenerations);
}

private struct BenchmarkConfig
{
    size_t actorsPerSet;
    size_t measuredGenerations;
    size_t warmupGenerations;
    uint avgCost;
    size_t ringMiB;
}

private struct SweepResult
{
    size_t workers;
    size_t smtWorkers;
    double oneFiberMillions;
    double twoFiberMillions;
    double ratio;
}

private SweepResult runBenchmark(BenchmarkConfig config,
        ushort[] workerLps)
{
    immutable actorsPerSet = config.actorsPerSet;
    immutable measuredGenerations = config.measuredGenerations;
    immutable warmupGenerations = config.warmupGenerations;
    immutable avgCost = config.avgCost;
    immutable ringMiB = config.ringMiB;

    enforce(workerLps.length != 0, "actor-wave benchmark needs workers");
    enforce(measuredGenerations != 0,
        "measured generations must be nonzero");

    enum uint segmentCount = 8;
    immutable ringBytes = cast(ulong) ringMiB * 1024UL * 1024UL;
    enforce((ringBytes & (ringBytes - 1)) == 0
            && ringBytes >= segmentCount * ulong.sizeof,
        "Farm ring MiB must produce a power-of-two ring");
    immutable ringLength = ringBytes / ulong.sizeof;
    immutable segmentCapacity = ringLength / segmentCount;
    immutable producerSlots = workerLps.length + 2;
    immutable quotaHeadroom = (segmentCount - 1) * segmentCapacity
        / producerSlots;
    immutable quotaSmall = quotaHeadroom > 16_384
        ? 16_384 : quotaHeadroom;

    auto farm = AntFarm.create(ringLength, segmentCount,
        cast(uint) workerLps.length, 0, 0, cast(uint) producerSlots,
        quotaSmall);
    scope (exit) farm.destroy();

    immutable totalActors = actorsPerSet * 2;
    auto runtime = ActorRuntime.create(farm, totalActors);
    enforce(runtime !is null, "actor runtime allocation failed");
    auto owners = new ActorOwner!ProjectionActor[totalActors];
    auto setA = new ActorHandle!ProjectionActor[actorsPerSet];
    auto setB = new ActorHandle!ProjectionActor[actorsPerSet];
    foreach (i; 0 .. totalActors)
    {
        ProjectionActor initial;
        immutable seed = cast(ulong) i + 1;
        initial.privateState.value = seed;
        initial.publicState[0].value = seed;
        initial.publicState[1].value = seed;
        owners[i] = runtime.createActor!(ProjectionActor, dormantActor)(
            initial);
        enforce(owners[i].valid, "actor allocation failed");
        if (i < actorsPerSet)
            setA[i] = owners[i].handle;
        else
            setB[i - actorsPerSet] = owners[i].handle;
    }
    auto sets = [setA, setB];

    auto lane = new FiberLane(farm);
    lane.flushBatch = 32;
    lane.avgCost = 2;
    PoolOptions options;
    options.onlyLps = workerLps;
    options.managedWorker = fiberWorkerHooks();
    auto pool = new CacheAwarePool(options);
    installFiberLanes([lane], pool);
    scope (exit) uninstallFiberLanes();
    pool.start();
    scope (exit) pool.shutdown();

    // Keep workers polling for this throughput characterization. Parking and
    // wake policy are a separate effect that would dominate short waves.
    auto director = pool.director();
    director.spin();

    writefln("actor wave: sets=2 actors/set=%s generations=%s warmup=%s",
        actorsPerSet, measuredGenerations, warmupGenerations);
    immutable smtWorkers = countSmtWorkers(workerLps);
    writefln("workers=%s smt-workers=%s worker-lps=%s avgCost=%s ring=%s MiB hugePages=%s state=%s bytes/actor",
        workerLps.length, smtWorkers, workerLps, avgCost, ringMiB,
        farm.usedLargePages, ProjectionActor.sizeof);

    atomicStore!(MemoryOrder.raw)(publishedGeneration.value, 0UL);
    auto oneProducer = farm.registerProducer(Tier.small);
    enforce(oneProducer.valid, "one-Fiber producer registration failed");
    auto one = new OneFiberRun(lane.domain, farm, oneProducer, sets, avgCost);
    auto oneTask = lane.spawn({
        one.run(warmupGenerations, measuredGenerations);
    });
    joinOrThrow(oneTask);
    immutable expectedGeneration = cast(ulong)(
        warmupGenerations + measuredGenerations);
    enforce(atomicLoad!(MemoryOrder.acq)(publishedGeneration.value)
            == expectedGeneration,
        "one-Fiber published generation mismatch");
    report("one Fiber producing both waves", actorsPerSet,
        measuredGenerations, one.elapsedNsecs, one.physicalTables,
        one.cooperativeYields, oneTask.resumeCount,
        oneTask.migrationCount);
    farm.unregisterProducer(one.producer);
    lane.backend.releaseAll(lane.backend.takeCompletions());

    atomicStore!(MemoryOrder.raw)(publishedGeneration.value, 0UL);
    auto producerA = farm.registerProducer(Tier.small);
    auto producerB = farm.registerProducer(Tier.small);
    enforce(producerA.valid && producerB.valid,
        "two-Fiber producer registration failed");
    auto two = new TwoFiberRun(lane.domain, farm, producerA, producerB,
        sets, avgCost);
    auto taskA = lane.spawn({
        two.runSet(0, warmupGenerations, measuredGenerations);
    });
    auto taskB = lane.spawn({
        two.runSet(1, warmupGenerations, measuredGenerations);
    });
    joinOrThrow(taskA);
    joinOrThrow(taskB);
    enforce(atomicLoad!(MemoryOrder.acq)(publishedGeneration.value)
            == expectedGeneration,
        "two-Fiber published generation mismatch");
    report("two Fibers producing one wave each", actorsPerSet,
        measuredGenerations, two.elapsedNsecs,
        two.physicalTables[0] + two.physicalTables[1],
        two.cooperativeYields[0] + two.cooperativeYields[1],
        taskA.resumeCount + taskB.resumeCount,
        taskA.migrationCount + taskB.migrationCount);
    writefln("\ntwo-Fiber / one-Fiber throughput: %.3fx",
        cast(double) one.elapsedNsecs / two.elapsedNsecs);
    farm.unregisterProducer(two.producers[0]);
    farm.unregisterProducer(two.producers[1]);
    lane.backend.releaseAll(lane.backend.takeCompletions());

    writefln("\npublished generation=%s active-public=%s",
        expectedGeneration, expectedGeneration & 1UL);

    immutable actorDispatches = cast(double) actorsPerSet * 2.0
        * cast(double) measuredGenerations;
    SweepResult result;
    result.workers = workerLps.length;
    result.smtWorkers = smtWorkers;
    result.oneFiberMillions = actorDispatches * 1_000.0
        / cast(double) one.elapsedNsecs;
    result.twoFiberMillions = actorDispatches * 1_000.0
        / cast(double) two.elapsedNsecs;
    result.ratio = cast(double) one.elapsedNsecs / two.elapsedNsecs;

    retireActors(owners);
    enforce(runtime.live == 0 && runtime.ready == 0,
        "actor runtime did not drain");
    runtime.destroy();
    return result;
}

void main(string[] args)
{
    bool includeSmt;
    bool sweep;
    bool help;
    string[] positional;
    foreach (arg; args[1 .. $])
    {
        if (arg == "--smt")
            includeSmt = true;
        else if (arg == "--sweep")
            sweep = true;
        else if (arg == "--help" || arg == "-h")
            help = true;
        else
        {
            enforce(arg.length == 0 || arg[0] != '-',
                "unknown actor-wave benchmark option: " ~ arg);
            positional ~= arg;
        }
    }
    if (help)
    {
        writefln("usage: actor_wave [actors [generations [worker-cap [warmup [avgCost [ring-MiB]]]]]] [--smt] [--sweep]");
        writefln("  --smt    include SMT siblings after one worker per physical core");
        writefln("  --sweep  run every prefix from one worker through the selected worker set");
        return;
    }
    enforce(positional.length <= 6,
        "too many actor-wave benchmark positional arguments");

    BenchmarkConfig config;
    config.actorsPerSet = positional.length > 0
        ? positional[0].to!size_t : 32_768;
    config.measuredGenerations = positional.length > 1
        ? positional[1].to!size_t : 200;
    immutable maximumWorkers = positional.length > 2
        ? positional[2].to!size_t : 0;
    config.warmupGenerations = positional.length > 3
        ? positional[3].to!size_t : 10;
    config.avgCost = positional.length > 4
        ? positional[4].to!uint : 0;
    config.ringMiB = positional.length > 5
        ? positional[5].to!size_t : 8;

    enforce(config.actorsPerSet != 0,
        "actors per set must be nonzero");
    enforce(config.measuredGenerations != 0,
        "measured generations must be nonzero");
    enforce(config.avgCost <= MAX_AVG_COST,
        "avgCost exceeds MAX_AVG_COST");
    enforce(config.ringMiB != 0
            && config.ringMiB <= size_t.max / (1024 * 1024),
        "Farm ring MiB is out of range");
    enforce(config.actorsPerSet <= size_t.max / 2,
        "actor count overflows runtime capacity");

    auto availableWorkerLps = pickWorkerLps(maximumWorkers, includeSmt);
    enforce(availableWorkerLps.length != 0,
        includeSmt ? "no logical processors available"
                   : "no non-SMT logical processors available");

    SweepResult[] results;
    immutable firstCount = sweep ? 1 : availableWorkerLps.length;
    foreach (workerCount; firstCount .. availableWorkerLps.length + 1)
    {
        if (sweep)
            writefln("\n=== actor-wave worker sweep %s/%s ===",
                workerCount, availableWorkerLps.length);
        results ~= runBenchmark(config,
            availableWorkerLps[0 .. workerCount]);
    }

    if (sweep)
    {
        writefln("\nactor-wave worker sweep summary (%s):",
            includeSmt ? "SMT siblings included" : "physical cores only");
        writefln("workers  SMT  one-Fiber Mactor/s  two-Fiber Mactor/s  two/one");
        foreach (result; results)
            writefln("%7s  %3s  %18.2f  %18.2f  %7.3f",
                result.workers, result.smtWorkers,
                result.oneFiberMillions, result.twoFiberMillions,
                result.ratio);
    }
}
