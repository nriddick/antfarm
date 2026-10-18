module parallel_tests;

version (TSan) extern(C) __gshared string[] rt_options = ["gcopt=parallel:0"];

import antfarm.parallel;
import core.atomic;
import core.thread;
import core.stdc.stdio : puts;
import std.range : iota, isForwardRange;

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
void joined(void*) nothrow @nogc @system
{
    foreach (value; hits) check(value == 1);
    check(atomicFetchAdd!(MemoryOrder.acq_rel)(notified, 1U) == 0);
}
void nested(size_t) nothrow @nogc @system
{
    parallelForWait!noop(iota(size_t(1)), *nestedToken, *nestedView);
}
void nestedHook(void*) nothrow @nogc @system { nested(0); }

void subscribe(ref ConsumerView view, AntFarm* farm)
{
    while (true)
    {
        immutable result = view.subscribe(farm);
        if (result >= 0) return;
        check(result == SUBSCRIBE_RETRY);
        Thread.yield();
    }
}

void borrowedNoGC(ref ConsumerView view, ref Token token) nothrow @nogc @system
{
    uint[1031] local;
    foreach (i; parallel(view, token, iota(local.length),
        Options(13, 7, ParallelForm.mtSlices)))
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

    foreach (form; [ParallelForm.automatic, ParallelForm.st, ParallelForm.mtSlices])
    foreach (n; [size_t(0), 1, 7, 513, 1031, 50_003])
    foreach (grain; [size_t(1), 64, 1024])
    {
        hits = new uint[n];
        auto options = Options(grain, MAX_AVG_COST, form, 7);
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
    auto entries = parallelRange!visit(iota(hits.length), 13);
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

    hits[] = 0;
    auto slices = parallelSlices!visit(farm, iota(hits.length), 7);
    check(slices.length == 7);
    check(slices.front.header.maxCs > 1);
    while (!slices.empty)
    {
        slices.popFrontN(cast(size_t) farm.write(slices, token));
        view.consumeNext();
    }
    while (view.consumeNext()) {}
    foreach (hit; hits) check(hit == 1);

    // Preserve nonzero starts and strides, including a descending iota.
    hits = new uint[100];
    parallelForWait!signedVisit(iota(99, 0, -3), token, view);
    foreach (i, hit; hits) check(hit == (i > 0 && i % 3 == 0 ? 1 : 0));

    // Borrow the caller's frame, mutable range, and data without allocating a closure.
    uint[1031] output;
    foreach (form; [ParallelForm.st, ParallelForm.mtSlices])
    {
        output[] = 0;
        foreach (i; parallel(view, token, iota(output.length), Options(64, 7, form)))
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

    foreach (form; [ParallelForm.st, ParallelForm.mtSlices])
    foreach (grain; [size_t(1), 64])
    {
        hits = new uint[32783];
        atomicStore!(MemoryOrder.raw)(notified, 0U);
        auto hook = TableCompletionHook(null, &joined);
        auto handle = parallelFor!visit(farm, iota(hits.length), token,
            Options(grain, MAX_AVG_COST, form), &hook);
        while (!handle.publish(token))
            if (!view.consumeNext()) Thread.yield();
        while (!handle.complete)
            if (!view.consumeNext()) Thread.yield();
        check(atomicLoad!(MemoryOrder.acq)(notified) == 1);
    }

    // An MT callback stays in flight after other iterations have returned.
    // The aggregate hook must wait for that callback's plain output write.
    hits = new uint[4097];
    atomicStore!(MemoryOrder.raw)(notified, 0U);
    atomicStore!(MemoryOrder.raw)(entered, 0U);
    atomicStore!(MemoryOrder.raw)(release, 0U);
    auto hook = TableCompletionHook(null, &joined);
    auto handle = parallelFor!hold(farm, iota(hits.length), token,
        Options(1, 7, ParallelForm.mtSlices, 1), &hook);
    check(handle.submitted);
    while (!atomicLoad!(MemoryOrder.acq)(entered)) Thread.yield();
    check(!handle.complete && atomicLoad!(MemoryOrder.acq)(notified) == 0);
    atomicStore!(MemoryOrder.rel)(release, 1U);
    while (!handle.complete)
        if (!view.consumeNext()) Thread.yield();
    check(atomicLoad!(MemoryOrder.acq)(notified) == 1);
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
        else if (args[1] == "nested-mt")
        {
            auto entries = parallelSlices!nested(farm, iota(size_t(2)), 1);
            check(farm.write(entries, token) == 1);
            view.consumeNext();
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
    puts("parallel tests passed");
}
