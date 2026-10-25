// SPDX-License-Identifier: MIT
// Experimental dispatch mechanics only: deliberately not a replacement scheduler.
module warm_fibers;
import antfarm;
import ant_fibers.scheduler : FiberDomain, FiberDomainOptions, FiberTask;
import core.atomic;
import core.memory : GC;
import core.thread : Thread, Fiber;
import core.time : MonoTime;
import forkjoin.deque : Deque;
import forkjoin.node : Node;
import std.algorithm : sort, equal;
import std.conv : to;
import std.exception : enforce;
import std.json : JSONValue;
import std.stdio : writeln;
import threadpool.pool : CacheAwarePool;
import threadpool.pin : pinProcessor, PinOutcome;
import threadpool.topology : ProcessorId;

private enum Mode { local, ring, deque, fair, domain }
private __gshared Mode mode;
private __gshared uint threads, slotsPerThread = 64, yieldCount, ringChunk = 8;
private __gshared ulong iterations;
private __gshared Ticket[] tickets;
private __gshared Worker[] workers;
private __gshared AntFarm* farm;
private shared uint ready, go, finishedThreads, remaining, stop;
private Worker* self;

// A real Fiber object/stack per ticket; all are reachable from this GC array.
// Queue payloads are pointers to stable tickets, never borrowed ring bodies.
private struct Ticket
{
    Node node;
    Fiber fiber;
    ulong completed, resumes, checksum, target, migrations;
    uint id, lastWorker = uint.max;
    void body() nothrow @nogc
    {
        checksum += id + 1;
        foreach (_; 0 .. yieldCount) { Fiber.yield(); checksum += id + 1; }
    }
    void managedBody()
    {
        checksum += id + 1;
        foreach (_; 0 .. yieldCount) { FiberDomain.yieldReady(); checksum += id + 1; }
        completed++;
    }
}
private align(64) struct Worker
{
    Deque* queue;
    Node*[] staging;
    size_t staged;
    ConsumerView view;
    Token token;
    ulong completions, resumes, steals, attempts, polls, tables, refused, gcBytes;
    long endTicks;
    uint id, phase;
    ProcessorId cpu;
}
private struct Pointers
{
    Node*[] values;
    bool empty() const nothrow @nogc { return values.length == 0; }
    size_t length() const nothrow @nogc { return values.length; }
    Pointers save() const nothrow @nogc { return Pointers(cast(Node*[])values); }
    PayloadBody front() const nothrow @nogc { return (cast(const(ulong)*)values.ptr)[0 .. 1]; }
    void popFront() nothrow @nogc { values = values[1 .. $]; }
}
private void stage(Ticket* t) nothrow @nogc
{
    if (self.staged == self.staging.length) fatal("benchmark staging overflow");
    self.staging[self.staged++] = &t.node;
}
private void resumeTicket(Ticket* t) nothrow @nogc
{
    // The exclusive queue claim owns the Fiber until call has returned.
    // Only THEN reset or publish it. No worker touches a ticket after publish.
    auto id = self.id;
    if (t.lastWorker != uint.max && t.lastWorker != id) ++t.migrations;
    t.lastWorker = id;
    if (t.fiber.call!(Fiber.Rethrow.no)() !is null) fatal("benchmark Fiber failed");
    ++t.resumes; ++self.resumes;
    if (t.fiber.state == Fiber.State.TERM)
    {
        ++t.completed; ++self.completions;
        if (t.completed == t.target) { atomicOp!"-="(remaining, 1u); return; }
        t.fiber.reset();
    }
    stage(t);
}
private extern(C) void ringCall(const(ulong)* body, size_t n) nothrow @nogc
{
    if (n != 1) fatal("benchmark ring payload");
    resumeTicket(cast(Ticket*)body[0]);
}
private void flushRing() nothrow @nogc
{
    if (!self.staged) return;
    auto n = farm.write(&ringCall, Pointers(self.staging[0 .. self.staged]), 1,
        self.token, avgCostForChunk(ringChunk));
    if (n) ++self.tables;
    if (n != self.staged) ++self.refused;
    self.staged -= n;
    foreach (i; 0 .. self.staged) self.staging[i] = self.staging[n + i];
}
private void flushDeque() nothrow @nogc
{
    if (!self.staged) return;
    long depth;
    auto n = self.queue.pushAll(self.staging[0 .. self.staged], depth);
    if (n != self.staged) fatal("benchmark deque capacity proof failed");
    self.staged = 0;
}
private bool step() nothrow @nogc
{
    if (mode == Mode.ring)
    {
        flushRing(); ++self.polls;
        return self.view.consumeNext();
    }
    flushDeque();
    Node* n;
    // Bound LIFO monopolization: every 32nd dispatch samples the oldest item
    // on the local deque. This is a measurement policy, not a fairness proof.
    if (mode == Mode.fair && ++self.phase % 32 == 0) n = self.queue.steal();
    if (n is null) n = self.queue.pop();
    if (n is null)
    {
        foreach (offset; 1 .. threads)
        {
            const victim = (self.id + offset) % threads;
            ++self.attempts;
            n = workers[victim].queue.steal();
            if (n !is null) { ++self.steals; break; }
        }
    }
    if (n is null) return false;
    resumeTicket(cast(Ticket*)n);
    return true;
}
private void warmWorker(uint id)
{
    self = &workers[id];
    enforce(pinProcessor(self.cpu) == PinOutcome.pinned, "pin failed");
    if (mode == Mode.ring || mode == Mode.domain)
    {
        enforce(self.view.subscribe(farm) >= 0, "subscribe failed");
        if (mode == Mode.ring) { self.token = farm.registerProducer(Tier.small); enforce(self.token.valid); }
    }
    if (mode != Mode.domain)
    {
        // First-touch and four full executions warm every retained stack.
        foreach (ref t; tickets[id * slotsPerThread .. (id + 1) * slotsPerThread])
        {
            t.fiber = new Fiber(&t.body, 64 * 1024);
            t.fiber.allowMigration(); // body retains no TLS pointer across yield
            foreach (_; 0 .. 4)
            {
                do { enforce(t.fiber.call!(Fiber.Rethrow.no)() is null); }
                while (t.fiber.state != Fiber.State.TERM);
                t.fiber.reset();
            }
            t.completed = t.resumes = t.checksum = 0;
            t.target = iterations;
            if (mode != Mode.local) stage(&t);
        }
    }
}
private void runWorker(uint id)
{
    warmWorker(id);
    atomicOp!"+="(ready, 1u);
    while (!atomicLoad!(MemoryOrder.acq)(go)) Thread.yield();
    auto gcBefore = GC.allocatedInCurrentThread();
    if (mode == Mode.domain)
    {
        while (!atomicLoad!(MemoryOrder.acq)(stop))
            if (!self.view.consumeNext()) Thread.yield();
    }
    else if (mode == Mode.local)
    {
        foreach (_; 0 .. iterations)
            foreach (ref t; tickets[id * slotsPerThread .. (id + 1) * slotsPerThread])
            {
                do
                {
                    if (t.fiber.call!(Fiber.Rethrow.no)() !is null) fatal("local Fiber failed");
                    ++t.resumes; ++self.resumes;
                }
                while (t.fiber.state != Fiber.State.TERM);
                ++t.completed; ++self.completions;
                if (t.completed < iterations) t.fiber.reset();
            }
    }
    else
        while (atomicLoad!(MemoryOrder.acq)(remaining))
            if (!step()) Thread.yield();
    self.endTicks = MonoTime.currTime.ticks;
    self.gcBytes = GC.allocatedInCurrentThread() - gcBefore;
    if (mode == Mode.ring) farm.unregisterProducer(self.token);
    if (mode == Mode.ring || mode == Mode.domain) self.view.unsubscribe();
    atomicOp!"+="(finishedThreads, 1u);
}
int main(string[] args)
{
    enforce(args.length == 7, "MODE THREADS ITERATIONS_PER_SLOT SLOTS_PER_THREAD YIELDS RING_CHUNK");
    const name = args[1];
    switch (name)
    {
        case "local": mode = Mode.local; break;
        case "ring": mode = Mode.ring; break;
        case "deque": mode = Mode.deque; break;
        case "deque-fair": mode = Mode.fair; break;
        case "domain": mode = Mode.domain; break;
        default: throw new Exception("invalid mode");
    }
    threads = args[2].to!uint; iterations = args[3].to!ulong;
    slotsPerThread = args[4].to!uint; yieldCount = args[5].to!uint; ringChunk = args[6].to!uint;
    enforce(threads && threads <= 128 && iterations && slotsPerThread && slotsPerThread <= 1024 && ringChunk && ringChunk <= 128);
    auto topo = CacheAwarePool.topology();
    ProcessorId[] cpus;
    foreach (ref p; topo.processors) if (topo.isEligible(p)) cpus ~= ProcessorId.of(p);
    enforce(cpus.length >= threads);
    const total = threads * slotsPerThread;
    tickets = new Ticket[total];
    foreach (i, ref t; tickets) t.id = cast(uint)i;
    workers = new Worker[threads];
    foreach (id, ref w; workers)
    {
        w.id = cast(uint)id; w.cpu = cpus[id];
        w.staging = new Node*[total];
        if (mode == Mode.deque || mode == Mode.fair) w.queue = Deque.create(total);
    }
    if (mode == Mode.ring || mode == Mode.domain)
        farm = AntFarm.create(32, 8, threads, 0, 0, threads + 1, 8192, DEFAULT_SMALL_TABLE_THRESHOLD, false);
    remaining = total;
    // Disable collection, not allocation; report measured GC bytes separately.
    GC.disable();
    Thread[] pool;
    foreach (id; 1 .. threads)
    {
        auto thread = new Thread((uint index) { return { runWorker(index); }; }(id));
        pool ~= thread; thread.start();
    }
    warmWorker(0);
    while (atomicLoad(ready) != threads - 1) Thread.yield();
    ulong domainMigrations;
    size_t[] warmAddresses, finalAddresses;
    long startTicks;
    if (mode == Mode.domain)
    {
        // Retirement hands each terminal task to onCompletion, then recycles
        // it; the preallocated buffer keeps the warm path allocation-free.
        auto completed = new FiberTask[total];
        size_t completedCount;
        FiberDomainOptions options;
        options.recordRetention = total * 4;
        options.onCompletion = (FiberTask t) { completed[completedCount++] = t; };
        auto domain = new FiberDomain(farm, options);
        domain.reserve(total);
        auto token = farm.registerProducer(Tier.small); enforce(token.valid);
        scope(exit) farm.unregisterProducer(token);
        atomicStore!(MemoryOrder.rel)(go, 1u);
        void batch()
        {
            completedCount = 0;
            foreach (ref t; tickets) domain.spawn(&t.managedBody, 64 * 1024);
            while (!domain.drained)
            {
                domain.flush(token, 256, avgCostForChunk(ringChunk));
                self.view.consumeNext();
            }
            enforce(domain.retire() == total && completedCount == total,
                "missing managed completions");
        }
        foreach (_; 0 .. 4) batch();
        foreach (t; completed) warmAddresses ~= cast(size_t)cast(void*)t;
        foreach (ref t; tickets) t.completed = t.checksum = 0;
        auto gcBefore = GC.allocatedInCurrentThread();
        startTicks = MonoTime.currTime.ticks;
        foreach (_; 0 .. iterations) batch();
        workers[0].endTicks = MonoTime.currTime.ticks;
        workers[0].gcBytes = GC.allocatedInCurrentThread() - gcBefore;
        // Identity check only: no task state is read after retirement.
        foreach (t; completed) finalAddresses ~= cast(size_t)cast(void*)t;
        enforce(equal(sort(warmAddresses), sort(finalAddresses)), "warm Fiber pool did not reuse its slots");
        atomicStore!(MemoryOrder.rel)(stop, 1u);
        self.view.unsubscribe();
    }
    else
    {
        // Main also dispatches. Avoid warming it twice through runWorker.
        auto gcBefore = GC.allocatedInCurrentThread();
        startTicks = MonoTime.currTime.ticks;
        atomicStore!(MemoryOrder.rel)(go, 1u);
        if (mode == Mode.local)
        {
            foreach (_; 0 .. iterations)
                foreach (ref t; tickets[0 .. slotsPerThread])
                {
                    do
                    {
                        enforce(t.fiber.call!(Fiber.Rethrow.no)() is null);
                        ++t.resumes; ++self.resumes;
                    }
                    while (t.fiber.state != Fiber.State.TERM);
                    ++t.completed; ++self.completions;
                    if (t.completed < iterations) t.fiber.reset();
                }
        }
        else while (atomicLoad!(MemoryOrder.acq)(remaining)) if (!step()) Thread.yield();
        self.endTicks = MonoTime.currTime.ticks;
        self.gcBytes = GC.allocatedInCurrentThread() - gcBefore;
        if (mode == Mode.ring) { farm.unregisterProducer(self.token); self.view.unsubscribe(); }
    }
    foreach (t; pool) t.join();
    long endTicks = workers[0].endTicks;
    if (mode != Mode.domain) foreach (ref w; workers) if (w.endTicks > endTicks) endTicks = w.endTicks;
    ulong gcBytes, migrations, steals, attempts, tables, refused, resumes;
    foreach (ref t; tickets)
    {
        enforce(t.completed == iterations, "wrong completion count");
        enforce(t.checksum == iterations * (yieldCount + 1) * (t.id + 1), "wrong body checksum");
        if (mode != Mode.domain) enforce(t.resumes == iterations * (yieldCount + 1), "wrong resume count");
        migrations += t.migrations; resumes += t.resumes;
    }
    foreach (ref w; workers)
    {
        gcBytes += w.gcBytes; steals += w.steals; attempts += w.attempts; tables += w.tables; refused += w.refused;
        if (w.queue !is null) w.queue.destroy();
    }
    if (farm !is null) farm.destroy();
    const seconds = cast(double)(endTicks - startTicks) / MonoTime.ticksPerSecond;
    JSONValue row;
    row["mode"] = name; row["threads"] = threads; row["iterations_per_slot"] = iterations;
    row["warm_fibers"] = total; row["yields"] = yieldCount; row["ring_chunk"] = ringChunk;
    row["seconds"] = seconds; row["completions"] = iterations * total;
    row["dispatches"] = iterations * total * (yieldCount + 1);
    row["fibers_per_second"] = iterations * total / seconds;
    row["dispatches_per_second"] = iterations * total * (yieldCount + 1) / seconds;
    row["gc_bytes"] = gcBytes; row["steals"] = steals; row["steal_attempts"] = attempts;
    row["migrations"] = mode == Mode.domain ? JSONValue(null) : JSONValue(migrations);
    row["ring_tables"] = tables; row["ring_refusals"] = refused;
    JSONValue[] cpuRows;
    foreach (id; cpus[0 .. threads]) cpuRows ~= JSONValue(["group": JSONValue(id.group), "cpu": JSONValue(id.lp), "llc": JSONValue(topo.byId(id).llcIndex)]);
    row["cpus"] = cpuRows;
    writeln(row.toString());
    return 0;
}
