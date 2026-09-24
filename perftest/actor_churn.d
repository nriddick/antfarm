/++
 + Sustained create -> dispatch -> retire -> reclaim benchmark.
 + Farm, runtime, metadata, and consumer threads survive across cycles; actor
 + state is allocated and reclaimed every cycle. See README.md for timing details.
 +/
module actor_churn;

import actors;
import antfarm : ConsumerView;
import core.atomic : MemoryOrder, atomicLoad, atomicStore, pause;
import core.thread : Thread;
import core.time : MonoTime, seconds;
import std.conv : to;
import std.array : split;
import std.exception : enforce;
import std.math : isFinite;
import std.stdio : writefln;
import threadpool.pin : PinTarget, pinToLogicalProcessor;

version (AntfarmMimallocV3)
{
    import actors.mimalloc : mimallocV3ActorAllocator;

    // Diagnostic control: match cRuntime's rounded size and 64-byte alignment.
    // The ordinary mimalloc mode uses the adapter's exact size/alignment.
    private void* mimalloc64Allocate(void*, size_t bytes, size_t alignment)
        nothrow @nogc @system
    {
        if (alignment > 64) return null;
        immutable rounded = (bytes + 63) & ~cast(size_t) 63;
        return mimallocV3ActorAllocator().allocate(null, rounded, 64);
    }

    private void mimalloc64Deallocate(void*, void* memory, size_t bytes,
            size_t alignment) nothrow @nogc @system
    {
        immutable rounded = (bytes + 63) & ~cast(size_t) 63;
        mimallocV3ActorAllocator().deallocate(null, memory, rounded, 64);
    }
}

// Separate completion lines avoid one contended global callback counter.
align(64) private struct Completion
{
    shared ulong stamp;
    ulong calls;
    ubyte[48] padding;
}

private struct State
{
    Completion* completion;
    ulong cycle;
}

// Native malloc control: avoid forcing 64-byte alignment on the small state.
// Runtime and slot allocations retain the ordinary aligned policy. On Linux,
// LD_PRELOAD can replace these C allocation functions process-wide.
private void* mallocAllocate(void*, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    import core.stdc.stdlib : malloc;
    if (bytes == State.sizeof && alignment == State.alignof)
        return malloc(bytes);
    return ActorAllocator.cRuntime().allocate(null, bytes, alignment);
}

private void mallocDeallocate(void*, void* memory, size_t bytes,
        size_t alignment) nothrow @nogc @system
{
    import core.stdc.stdlib : free;
    if (bytes == State.sizeof && alignment == State.alignof)
        free(memory);
    else
        ActorAllocator.cRuntime().deallocate(null, memory, bytes, alignment);
}

// A controller-owned bump arena for one cohort. Individual state frees do
// nothing; reset is legal only after every owner has been reclaimed. The
// runtime and stable slots outlive the reset and use the C-runtime policy.
private struct StateArena
{
    void* memory;
    size_t capacity;
    size_t used;

    bool initialize(size_t count) nothrow @nogc @system
    {
        if (count == 0 || count > size_t.max / State.sizeof) return false;
        capacity = count * State.sizeof;
        memory = ActorAllocator.cRuntime().allocate(null, capacity, State.alignof);
        return memory !is null;
    }

    void reset() nothrow @nogc @safe { used = 0; }

    void destroy() nothrow @nogc @system
    {
        if (memory !is null)
            ActorAllocator.cRuntime().deallocate(null, memory,
                capacity, State.alignof);
    }
}

private void* arenaAllocate(void* context, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    if (bytes != State.sizeof || alignment != State.alignof)
        return ActorAllocator.cRuntime().allocate(null, bytes, alignment);
    auto arena = cast(StateArena*) context;
    // The backing block and State stride already satisfy State.alignof.
    if (bytes > arena.capacity - arena.used) return null;
    auto block = cast(ubyte*) arena.memory + arena.used;
    arena.used += bytes;
    return block;
}

private void arenaDeallocate(void* context, void* memory, size_t bytes,
        size_t alignment) nothrow @nogc @system
{
    if (bytes != State.sizeof || alignment != State.alignof)
        ActorAllocator.cRuntime().deallocate(null, memory, bytes, alignment);
    else
    {
        auto arena = cast(StateArena*) context;
        immutable offset = cast(size_t) memory - cast(size_t) arena.memory;
        assert(offset < arena.used && offset % State.sizeof == 0);
    }
}

unittest
{
    StateArena arena;
    assert(!arena.initialize(0));
    assert(!arena.initialize(size_t.max / State.sizeof + 1));
    assert(arena.initialize(3));
    scope (exit) arena.destroy();
    void*[3] blocks;
    foreach (i; 0 .. blocks.length)
    {
        blocks[i] = arenaAllocate(&arena, State.sizeof, State.alignof);
        assert(blocks[i] !is null
            && cast(size_t) blocks[i] % State.alignof == 0);
        (cast(State*) blocks[i]).cycle = i + 1;
    }
    assert(arenaAllocate(&arena, State.sizeof, State.alignof) is null);
    foreach (i; 0 .. blocks.length)
    {
        assert((cast(State*) blocks[i]).cycle == i + 1);
        arenaDeallocate(&arena, blocks[i], State.sizeof, State.alignof);
    }
    // Individual frees cannot recycle storage before the cohort reset.
    assert(arenaAllocate(&arena, State.sizeof, State.alignof) is null);
    arena.reset();
    assert(arenaAllocate(&arena, State.sizeof, State.alignof) == blocks[0]);
    arenaDeallocate(&arena, blocks[0], State.sizeof, State.alignof);
    // Stable runtime metadata must survive state-arena resets.
    auto stable = arenaAllocate(&arena, 128, 64);
    assert(stable !is null && cast(size_t) stable % 64 == 0);
    *cast(ulong*) stable = 1234;
    arena.reset();
    assert(*cast(ulong*) stable == 1234);
    arenaDeallocate(&arena, stable, 128, 64);
}

version (linux)
private string allocationProvider(const(char)* symbol)
{
    import core.sys.posix.dlfcn : Dl_info, dladdr, dlsym;
    import core.sys.linux.dlfcn : RTLD_DEFAULT;
    import std.string : fromStringz;
    Dl_info info;
    auto address = dlsym(RTLD_DEFAULT, symbol);
    enforce(address !is null && dladdr(address, &info) != 0,
        "cannot identify C allocator provider");
    return fromStringz(info.dli_fname).idup;
}

// Caller-side policy example, not a runtime optimization. The benchmark's
// controller owns all allocation/free calls, so this fixed-size pool needs
// no locks. Runtime/slot allocations still use the C-runtime policy.
private struct StatePool
{
    void* memory;
    void* freeHead;
    size_t capacity;
    size_t available;

    bool initialize(size_t count) nothrow @nogc @system
    {
        if (count > size_t.max / State.sizeof) return false;
        memory = ActorAllocator.cRuntime().allocate(null,
            count * State.sizeof, State.alignof);
        if (memory is null) return false;
        capacity = available = count;
        foreach_reverse (i; 0 .. count)
        {
            auto block = cast(ubyte*) memory + i * State.sizeof;
            *cast(void**) block = freeHead;
            freeHead = block;
        }
        return true;
    }

    void destroy() nothrow @nogc @system
    {
        if (memory !is null)
            ActorAllocator.cRuntime().deallocate(null, memory,
                capacity * State.sizeof, State.alignof);
    }
}

private void* pooledAllocate(void* context, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    if (bytes != State.sizeof || alignment != State.alignof)
        return ActorAllocator.cRuntime().allocate(null, bytes, alignment);
    auto pool = cast(StatePool*) context;
    auto block = pool.freeHead;
    if (block is null) return null;
    pool.freeHead = *cast(void**) block;
    --pool.available;
    return block;
}

private void pooledDeallocate(void* context, void* memory, size_t bytes,
        size_t alignment) nothrow @nogc @system
{
    auto pool = cast(StatePool*) context;
    immutable address = cast(size_t) memory;
    immutable base = cast(size_t) pool.memory;
    if (address >= base && address - base < pool.capacity * State.sizeof)
    {
        assert(bytes == State.sizeof && alignment == State.alignof
            && (address - base) % State.sizeof == 0);
        *cast(void**) memory = pool.freeHead;
        pool.freeHead = memory;
        ++pool.available;
    }
    else
        ActorAllocator.cRuntime().deallocate(null, memory, bytes, alignment);
}

private void operate(scope ref ActorBorrow!State actor) nothrow @nogc @system
{
    auto completion = actor.value.completion;
    ++completion.calls;
    atomicStore!(MemoryOrder.rel)(completion.stamp, actor.value.cycle);
}

private void activate(scope ref ActorBorrow!State actor,
        scope ref ActorContext) nothrow @nogc @system
{
    operate(actor);
}

private class Consumer
{
    AntFarm* farm;
    shared int started;
    shared int stop;
    int cpu;

    this(AntFarm* farm, int cpu) { this.farm = farm; this.cpu = cpu; }

    void run()
    {
        if (cpu >= 0 && !pinToLogicalProcessor(PinTarget(0, 0, cast(ushort) cpu)))
        {
            atomicStore!(MemoryOrder.rel)(started, -1);
            return;
        }
        ConsumerView view;
        if (view.subscribe(farm) < 0)
        {
            atomicStore!(MemoryOrder.rel)(started, -1);
            return;
        }
        scope (exit) view.unsubscribe();
        atomicStore!(MemoryOrder.rel)(started, 1);
        while (atomicLoad!(MemoryOrder.acq)(stop) == 0)
            if (!view.consumeNext()) pause();
        while (view.consumeNext()) {}
    }
}

private double elapsedSeconds(long ticks)
{
    return cast(double) ticks / MonoTime.ticksPerSecond;
}

void main(string[] args)
{
    enforce(args.length >= 2 && args.length <= 9
            && (args[1] == "actor" || args[1] == "wave"),
        "usage: actor_churn actor|wave [actors=4096] [seconds=3] [consumers=0] [batch=256] [warmups=5] [crt|malloc|mimalloc|mimalloc64|pool|arena] [controller-cpu,consumer-cpus|-]");
    immutable waveMode = args[1] == "wave";
    immutable count = args.length > 2 ? to!size_t(args[2]) : 4096;
    immutable duration = args.length > 3 ? to!double(args[3]) : 3.0;
    immutable consumers = args.length > 4 ? to!uint(args[4]) : 0;
    immutable batch = args.length > 5 ? to!size_t(args[5]) : 256;
    immutable warmups = args.length > 6 ? to!size_t(args[6]) : 5;
    version (AntfarmMimallocV3) enum defaultAllocator = "mimalloc";
    else enum defaultAllocator = "crt";
    immutable allocatorName = args.length > 7 ? args[7] : defaultAllocator;
    enforce(count > 0 && count <= uint.max && isFinite(duration)
            && duration > 0 && consumers <= 64 && batch > 0
            && (waveMode || batch <= 256),
        "actors/seconds/batch must be positive; consumers must be 0..64; actor batch must be <=256");
    ushort[] cpus;
    if (args.length > 8 && args[8] != "-")
    {
        foreach (cpu; args[8].split(",")) cpus ~= cpu.to!ushort;
        enforce(cpus.length == consumers + 1,
            "CPU list must name the controller followed by each consumer");
        enforce(pinToLogicalProcessor(PinTarget(0, 0, cpus[0])),
            "controller CPU pin failed");
    }
    StatePool pool;
    scope (exit) pool.destroy();
    StateArena arena;
    scope (exit) arena.destroy();
    auto allocator = ActorAllocator.cRuntime();
    if (allocatorName == "arena")
    {
        enforce(arena.initialize(count), "state-arena allocation failed");
        allocator = ActorAllocator(&arena, &arenaAllocate, &arenaDeallocate);
    }
    else if (allocatorName == "malloc")
        allocator = ActorAllocator(null, &mallocAllocate, &mallocDeallocate);
    else if (allocatorName == "pool")
    {
        enforce(pool.initialize(count), "state-pool allocation failed");
        allocator = ActorAllocator(&pool, &pooledAllocate, &pooledDeallocate);
    }
    else if (allocatorName != "crt")
    {
        enforce(allocatorName == "mimalloc" || allocatorName == "mimalloc64",
            "allocator must be crt, malloc, mimalloc, mimalloc64, pool, or arena");
        version (AntfarmMimallocV3)
            allocator = allocatorName == "mimalloc"
                ? mimallocV3ActorAllocator()
                : ActorAllocator(null, &mimalloc64Allocate, &mimalloc64Deallocate);
        else
            enforce(false, "mimalloc requires the actor_churn_mimalloc build");
    }

    auto farm = AntFarm.create(1 << 20, 8, consumers ? consumers : 1,
        0, 0, 1, 16384);
    enforce(farm !is null, "Farm allocation failed");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, count, allocator);
    enforce(runtime !is null, "actor runtime allocation failed");
    scope (exit) runtime.destroy();
    auto owners = new ActorOwner!State[count];
    auto handles = new ActorHandle!State[count];
    auto completions = new Completion[count];
    auto token = farm.registerProducer(Tier.small);
    enforce(token.valid, "producer registration failed");
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    if (consumers == 0)
        enforce(view.subscribe(farm) >= 0, "consumer subscription failed");
    scope (exit) if (consumers == 0) view.unsubscribe();
    auto jobs = new Consumer[consumers];
    auto threads = new Thread[consumers];
    scope (exit)
    {
        foreach (job; jobs)
            if (job !is null) atomicStore!(MemoryOrder.rel)(job.stop, 1);
        foreach (thread; threads)
            if (thread !is null) thread.join();
    }
    foreach (i; 0 .. consumers)
    {
        jobs[i] = new Consumer(farm, cpus.length == 0 ? -1 : cast(int) cpus[i + 1]);
        threads[i] = new Thread(&jobs[i].run);
        threads[i].start();
        while (atomicLoad!(MemoryOrder.acq)(jobs[i].started) == 0)
            Thread.yield();
        enforce(atomicLoad!(MemoryOrder.acq)(jobs[i].started) == 1,
            "worker pin or subscription failed");
    }

    ActorWave wave;
    ulong cycle;
    long createTicks, dispatchTicks, retireTicks;
    void consume()
    {
        if (consumers == 0)
            while (view.consumeNext()) {}
        else
            pause();
    }

    void runCycle(bool measured)
    {
        ++cycle;
        immutable start = MonoTime.currTime;
        immutable deadline = start + 60.seconds;
        foreach (i; 0 .. count)
        {
            owners[i] = runtime.createActor!(State, activate)(
                State(&completions[i], cycle));
            enforce(owners[i].valid, "actor creation failed");
            handles[i] = owners[i].handle;
        }
        immutable created = MonoTime.currTime;
        if (waveMode)
        {
            wave.begin(farm);
            size_t published;
            while (published < count)
            {
                immutable limit = count - published < batch
                    ? count : published + batch;
                published += wave.publish!operate(handles[published .. limit],
                    token, 0);
                enforce(!wave.handle.failed, "wave publication failed");
                consume();
                enforce(MonoTime.currTime < deadline, "wave publication timed out");
            }
            auto completion = wave.seal();
            while (!completion.finished)
            {
                consume();
                enforce(MonoTime.currTime < deadline, "wave completion timed out");
            }
            enforce(!completion.failed, "wave execution failed");
        }
        else
        {
            foreach (handle; handles)
                enforce(handle.wake() == ActorWakeResult.queued, "wake failed");
            size_t published;
            while (published < count)
            {
                published += runtime.flush(token, batch, 0);
                consume();
                enforce(MonoTime.currTime < deadline, "actor publication timed out");
            }
            foreach (ref completion; completions)
                while (atomicLoad!(MemoryOrder.acq)(completion.stamp) != cycle)
                {
                    consume();
                    enforce(MonoTime.currTime < deadline, "actor completion timed out");
                }
        }
        immutable dispatched = MonoTime.currTime;
        foreach (ref owner; owners)
            enforce(owner.requestRetire() == ActorRetireResult.requested,
                "actor retirement failed");
        foreach (i, ref owner; owners)
        {
            ActorReclaimResult result;
            while ((result = owner.reclaim()) == ActorReclaimResult.busy)
            {
                runtime.flush(token, batch, 0);
                consume();
                enforce(MonoTime.currTime < deadline, "actor reclamation timed out");
            }
            enforce(result == ActorReclaimResult.reclaimed && !owner.valid,
                "actor reclamation failed");
            enforce(atomicLoad!(MemoryOrder.acq)(completions[i].stamp) == cycle
                    && completions[i].calls == cycle,
                "callback count or generation mismatch");
        }
        enforce(runtime.live == 0 && runtime.ready == 0
                && runtime.staleActivations == 0,
            "cycle left live, queued, or stale actors");
        enforce(allocatorName != "pool" || pool.available == count,
            "cycle did not return all pooled actor state");
        if (allocatorName == "arena")
        {
            enforce(arena.used == arena.capacity,
                "cycle did not allocate exactly one arena cohort");
            // Every owner is reclaimed and runtime.live is zero above. This
            // bulk reuse belongs to the timed retirement/reclamation phase.
            arena.reset();
        }
        immutable retired = MonoTime.currTime;
        if (measured)
        {
            createTicks += created.ticks - start.ticks;
            dispatchTicks += dispatched.ticks - created.ticks;
            retireTicks += retired.ticks - dispatched.ticks;
        }
    }

    foreach (_; 0 .. warmups) runCycle(false);
    immutable start = MonoTime.currTime;
    ulong rounds;
    double elapsed;
    do
    {
        runCycle(true);
        ++rounds;
        elapsed = elapsedSeconds(MonoTime.currTime.ticks - start.ticks);
    } while (elapsed < duration);
    immutable actorCycles = cast(double) count * rounds;
    writefln("mode=%s actors=%s consumers=%s batch=%s warmups=%s state=%s stateAlign=%s allocator=%s ringMiB=8 hugePages=%s cpus=%s",
        args[1], count, consumers, batch, warmups, State.sizeof, State.alignof,
        allocatorName, farm.usedLargePages, cpus);
    writefln("rounds=%s elapsed_s=%.6f Mactor_cycles/s=%.6f ns/actor_cycle=%.2f cycles/s=%.2f",
        rounds, elapsed, actorCycles / elapsed / 1e6,
        elapsed * 1e9 / actorCycles, rounds / elapsed);
    writefln("create_s=%.6f dispatch_s=%.6f retire_reclaim_verify_s=%.6f verified_actor_cycles=%s",
        elapsedSeconds(createTicks), elapsedSeconds(dispatchTicks),
        elapsedSeconds(retireTicks), cycle * count);
    version (linux)
        writefln("malloc_provider=%s free_provider=%s aligned_alloc_provider=%s",
            allocationProvider("malloc"), allocationProvider("free"),
            allocationProvider("aligned_alloc"));
}
