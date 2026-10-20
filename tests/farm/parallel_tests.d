module parallel_tests;

version (TSan) extern(C) __gshared string[] rt_options = ["gcopt=parallel:0"];

import antfarm.parallel;
import antfarm.templates : payloadRange;
import core.atomic;
import core.thread;
import core.stdc.stdio : puts;
import std.range : iota, isForwardRange;
import std.typecons : Tuple;

__gshared uint[] hits;
shared uint notified;
shared uint entered;
shared uint release;
__gshared ConsumerView* nestedView;
__gshared Token* nestedToken;

void check(bool condition) nothrow @nogc @system
{
    if (!condition) fatal("parallel test failed");
}

void visit(size_t index) nothrow @nogc @system { ++hits[index]; }
shared uint rawCompleted;
void countedVisit(size_t index) nothrow @nogc @system
{
    visit(index);
    atomicFetchAdd!(MemoryOrder.acq_rel)(rawCompleted, 1U);
}
shared uint zeroCalls;
void zeroVisit() nothrow @nogc @system { atomicFetchAdd!(MemoryOrder.raw)(zeroCalls, 1U); }
void visitAmount(size_t index, uint amount) nothrow @nogc @system { hits[index] += amount; }
void noop(size_t) nothrow @nogc @system {}
void signedVisit(int index) nothrow @nogc @system { ++hits[index]; }
void hold(size_t index) nothrow @nogc @system
{
    visit(index);
    if (index == 0)
    {
        atomicStore!(MemoryOrder.rel)(entered, 1U);
        while (!atomicLoad!(MemoryOrder.acq)(release)) Thread.yield();
    }
}
extern(C) void joined(void*) nothrow @nogc @system
{
    foreach (value; hits) check(value == 1);
    check(atomicFetchAdd!(MemoryOrder.acq_rel)(notified, 1U) == 0);
}
void nested(size_t) nothrow @nogc @system
{
    parallelForWait!noop(iota(size_t(1)), *nestedToken, *nestedView);
}
extern(C) void nestedHook(void*) nothrow @nogc @system { nested(0); }

void subscribe(ref ConsumerView view, AntFarm* farm)
{
    check(view.subscribe(farm) >= 0);
}

void borrowedNoGC(ref ConsumerView view, ref Token token) nothrow @nogc @system
{
    uint[1031] local;
    foreach (i; parallel(view, token, iota(local.length),
        Options(13, 7)))
        local[i] = cast(uint) i + 1;
    foreach (i, value; local) check(value == i + 1);
}

void basic()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 256);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    subscribe(view, farm);
    scope(exit) view.unsubscribe();

    foreach (n; [size_t(0), 1, 7, 513, 1031, 50_003])
    foreach (grain; [size_t(1), 64, 1024])
    {
        hits = new uint[n];
        auto options = Options(grain, MAX_AVG_COST);
        parallelForWait!visit(iota(n), token, view, options);
        foreach (hit; hits) check(hit == 1);

        hits[] = 0;
        atomicStore!(MemoryOrder.raw)(notified, 0U);
        auto hook = TableCompletionHook(null, &joined);
        auto handle = parallelFor!visit(farm, iota(n), token, options, &hook);
        while (!handle.submitted)
        {
            view.consumeNext();
            handle.publish(token);
        }
        while (!handle.complete) view.consumeNext();
        check(atomicLoad!(MemoryOrder.acq)(notified) == 1);
    }

    // Force a suffix beyond the entire ring, not just a per-write quota cut.
    hits = new uint[300_007];
    {
        auto handle = parallelFor!visit(farm, iota(hits.length), token);
        check(!handle.submitted && !handle.complete);
        while (!handle.publish(token)) view.consumeNext();
        while (!handle.complete) view.consumeNext();
        foreach (hit; hits) check(hit == 1);
    }

    // Forward checkpoints must preserve exactly the unadmitted suffix.
    hits = new uint[1031];
    auto entries = payloadRange!visit(iota(hits.length), 13);
    static assert(isForwardRange!(typeof(entries)));
    auto saved = entries.save;
    check(saved.length == 80);
    while (!entries.empty)
    {
        immutable accepted = farm.write(entries, token, MAX_AVG_COST);
        entries.popFrontN(cast(size_t) accepted);
        view.consumeNext();
    }
    while (view.consumeNext()) {}
    check(saved.length == 80);
    foreach (hit; hits) check(hit == 1);

    // Tuple grains expand named fields without changing argument conversion.
    alias Pair = Tuple!(size_t, "index", uint, "amount");
    immutable Pair[] pairs = [Pair(0, 1), Pair(1, 2), Pair(2, 3),
        Pair(3, 4), Pair(4, 5), Pair(5, 6), Pair(6, 7)];
    hits = new uint[pairs.length];
    auto tuples = payloadRange!visitAmount(pairs, 3);
    check(tuples.length == 3);
    while (!tuples.empty)
    {
        tuples.popFrontN(cast(size_t) farm.write(tuples, token));
        view.consumeNext();
    }
    while (view.consumeNext()) {}
    foreach (i, hit; hits) check(hit == i + 1);

    // Empty, zero-argument and maximal grains preserve exact element counts.
    atomicStore!(MemoryOrder.raw)(zeroCalls, 0U);
    auto empty = payloadRange!zeroVisit(iota(size_t(0)), size_t.max);
    check(empty.empty && empty.length == 0 && farm.write(empty, token) == 0);
    foreach (grain; [size_t(4), size_t.max])
    {
        auto zero = payloadRange!zeroVisit(iota(size_t(9)), grain);
        check(zero.length == (grain == 4 ? 3 : 1));
        while (!zero.empty)
        {
            zero.popFrontN(cast(size_t) farm.write(zero, token));
            view.consumeNext();
        }
        while (view.consumeNext()) {}
    }
    check(atomicLoad!(MemoryOrder.raw)(zeroCalls) == 18);
    static assert(!__traits(compiles, payloadRange!visit((size_t[]).init, 2)));
    static assert(!__traits(compiles, payloadRange!signedVisit(iota(size_t(4)), 2)));

    static assert(__traits(compiles, parallelRange!visit((size_t[]).init)));
    static assert(!__traits(compiles, parallelRange!visit((size_t[]).init, 2)));
    size_t[3] borrowed = [0, 1, 2];
    hits = new uint[3];
    parallelForWait!visit(borrowed[], token, view, Options(2));
    foreach (hit; hits) check(hit == 1);

    // Preserve nonzero starts and strides, including a descending iota.
    hits = new uint[100];
    parallelForWait!signedVisit(iota(99, 0, -3), token, view);
    foreach (i, hit; hits) check(hit == (i > 0 && i % 3 == 0 ? 1 : 0));

    // Borrow the caller's frame, mutable range, and data without allocating a closure.
    uint[1031] output;

    {
        output[] = 0;
        foreach (i; parallel(view, token, iota(output.length), Options(64, 7)))
            output[i] = cast(uint) i + 1;
        foreach (i, value; output) check(value == i + 1);
    }
    size_t calls;
    foreach (value; parallel(view, token, output[], Options(1, 7)))
        ++calls;
    check(calls == output.length);
    borrowedNoGC(view, token);

    // A fully published handle can go away before consumers finish.
    hits = new uint[31];
    atomicStore!(MemoryOrder.raw)(notified, 0U);
    auto hook = TableCompletionHook(null, &joined);
    {
        auto handle = parallelFor!visit(farm, iota(hits.length), token, Options(64), &hook);
        while (!handle.publish(token)) view.consumeNext();
    }
    while (!atomicLoad!(MemoryOrder.acq)(notified)) view.consumeNext();
}

void concurrent()
{
    auto farm = AntFarm.create(2, 8, 5, 0, 0, 1, 256);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    subscribe(view, farm);
    scope(exit) view.unsubscribe();
    shared uint stopped;
    shared uint ready;
    Thread[] workers;
    foreach (worker; 0 .. 4)
    {
        auto thread = new Thread({
            ConsumerView local;
            subscribe(local, farm);
            atomicFetchAdd!(MemoryOrder.rel)(ready, 1U);
            while (!atomicLoad!(MemoryOrder.acq)(stopped))
                if (!local.consumeNext()) Thread.yield();
            local.unsubscribe();
        });
        workers ~= thread;
        thread.start();
    }
    scope(exit)
    {
        atomicStore!(MemoryOrder.rel)(stopped, 1U);
        foreach (thread; workers) thread.join();
    }
    while (atomicLoad!(MemoryOrder.acq)(ready) != 4) Thread.yield();
    borrowedNoGC(view, token);

    foreach (grain; [size_t(1), 64])
    {
        hits = new uint[32783];
        atomicStore!(MemoryOrder.raw)(notified, 0U);
        auto hook = TableCompletionHook(null, &joined);
        auto handle = parallelFor!visit(farm, iota(hits.length), token,
            Options(grain, MAX_AVG_COST), &hook);
        while (!handle.publish(token))
            if (!view.consumeNext()) Thread.yield();
        while (!handle.complete)
            if (!view.consumeNext()) Thread.yield();
        check(atomicLoad!(MemoryOrder.acq)(notified) == 1);
    }

    // Direct helper grains use copied descriptors under concurrent partial admission.
    hits = new uint[50003];
    atomicStore!(MemoryOrder.raw)(rawCompleted, 0U);
    auto raw = payloadRange!countedVisit(iota(hits.length), 127);
    while (!raw.empty)
    {
        raw.popFrontN(cast(size_t) farm.write(raw, token, MAX_AVG_COST));
        if (!view.consumeNext()) Thread.yield();
    }
    while (atomicLoad!(MemoryOrder.acq)(rawCompleted) != hits.length)
        if (!view.consumeNext()) Thread.yield();
    foreach (hit; hits) check(hit == 1);

    // A grain callback stays in flight after other grains have returned.
    // The aggregate hook must wait for that callback's plain output write.
    hits = new uint[4097];
    atomicStore!(MemoryOrder.raw)(notified, 0U);
    atomicStore!(MemoryOrder.raw)(entered, 0U);
    atomicStore!(MemoryOrder.raw)(release, 0U);
    auto hook = TableCompletionHook(null, &joined);
    auto handle = parallelFor!hold(farm, iota(hits.length), token,
        Options(13, 7), &hook);
    check(handle.submitted);
    while (!atomicLoad!(MemoryOrder.acq)(entered)) Thread.yield();
    check(!handle.complete && atomicLoad!(MemoryOrder.acq)(notified) == 0);
    atomicStore!(MemoryOrder.rel)(release, 1U);
    while (!handle.complete)
        if (!view.consumeNext()) Thread.yield();
    check(atomicLoad!(MemoryOrder.acq)(notified) == 1);
}

struct HookAdmission { size_t callbacks, notifications; }
extern(C) void admissionWork(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    auto body = bodyPtr[0 .. bodyWords];
    auto state = cast(HookAdmission*) body[0];
    ++state.callbacks;
    return;
}
extern(C) void admissionDone(void* context) nothrow @nogc @system
{
    auto state = cast(HookAdmission*) context;
    check(state.callbacks == ++state.notifications);
}
void hookAdmission()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 256);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    HookAdmission state;
    auto hook = TableCompletionHook(&state, &admissionDone);
    Callback header;
    header = &admissionWork;
    ulong[1] body = [cast(ulong) &state];
    PayloadEntry[1] entry = [PayloadEntry(header, body[])];
    check(farm.write(entry[0 .. 0], token, DEFAULT_AVG_COST, &hook) == 0);
    size_t accepted;
    while (farm.write(entry[], token, DEFAULT_AVG_COST, &hook) != 0)
        check(++accepted < 10000);
    check(accepted > 0 && state.callbacks == 0 && state.notifications == 0);
    // A refused table and empty input must leave no phantom notification.
    check(farm.write(entry[], token, DEFAULT_AVG_COST, &hook) == 0);
    ConsumerView view;
    subscribe(view, farm);
    while (view.consumeNext()) {}
    check(state.callbacks == accepted && state.notifications == accepted);
    check(farm.write(entry[0 .. 0], token, DEFAULT_AVG_COST, &hook) == 0);
    while (view.consumeNext()) {}
    check(state.notifications == accepted);
    view.unsubscribe();
}

struct GrainCompletion { size_t previous, notifications; }
extern(C) void grainDone(void* context) nothrow @nogc @system
{
    auto state = cast(GrainCompletion*) context;
    auto completed = atomicLoad!(MemoryOrder.acq)(rawCompleted);
    check(completed > state.previous);
    state.previous = completed;
    ++state.notifications;
}
void partialGrainTables()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 64);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    subscribe(view, farm);
    scope(exit) view.unsubscribe();
    hits = new uint[1031];
    atomicStore(rawCompleted, 0U);
    GrainCompletion state;
    auto hook = TableCompletionHook(&state, &grainDone);
    auto grains = payloadRange!countedVisit(iota(hits.length), 7);
    size_t tables;
    while (!grains.empty)
    {
        auto accepted = farm.write(grains, token, MAX_AVG_COST, &hook);
        check(accepted > 0);
        grains.popFrontN(cast(size_t) accepted);
        ++tables;
        while (view.consumeNext()) {}
        check(state.notifications == tables);
    }
    check(tables > 1 && state.previous == hits.length);
    foreach (hit; hits) check(hit == 1);
}

void main(string[] args)
{
    if (args.length > 1)
    {
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 256);
        auto token = farm.registerProducer(Tier.small);
        ConsumerView view;
        subscribe(view, farm);
        nestedView = &view;
        nestedToken = &token;
        if (args[1] == "drop")
        {
            auto handle = parallelFor!noop(farm, iota(size_t(1_000_000)), token);
            check(!handle.submitted);
        }
        else if (args[1] == "break")
        {
            foreach (i; parallel(view, token, iota(size_t(1)))) break;
        }
        else if (args[1] == "invalid-hook")
        {
            TableCompletionHook hook;
            auto entries = payloadRange!noop(iota(size_t(1)));
            farm.write(entries, token, DEFAULT_AVG_COST, &hook);
        }
        else if (args[1] == "cost-class")
        {
            parallelForWait!noop(iota(size_t(0)), token, view, Options(1, MAX_AVG_COST + 1));
        }
        else if (args[1] == "grain-zero")
        {
            auto entries = payloadRange!noop(iota(size_t(1)), 0);
        }
        else if (args[1] == "nested-hook")
        {
            auto hook = TableCompletionHook(null, &nestedHook);
            auto handle = parallelFor!noop(farm, iota(size_t(1)), token, Options.init, &hook);
            while (!handle.complete) view.consumeNext();
        }
        else
        {
            auto entries = parallelRange!nested(iota(size_t(1)));
            check(farm.write(entries, token) == 1);
            view.consumeNext();
        }
        fatal("death test returned without rejecting nested wait");
    }
    basic();
    concurrent();
    hookAdmission();
    partialGrainTables();
    puts("parallel tests passed");
}
