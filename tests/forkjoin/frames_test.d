// SPDX-License-Identifier: MIT
/// Frame pool: classes and alignment, pattern integrity, LIFO reuse without
/// new chunks, large requests, cross-thread frees drained by the owner,
/// slots adopted across dispatcher start/stop cycles, and a free into a
/// parked slot.
module frames_test;

import forkjoin;
import core.atomic;
import core.thread;
import core.stdc.stdio : printf;

private struct Fib
{
    Node node;
    Fib* kids;
    ulong result;
    uint n;
}

private Node* fibRun(Node* node) nothrow @nogc @system
{
    auto f = cast(Fib*) node;
    if (f.n < 2)
    {
        f.result = f.n;
        return complete(node);
    }
    auto kids = allocFrames!Fib(2);
    kids[0] = Fib.init;
    kids[0].n = f.n - 1;
    kids[0].node.run = &fibRun;
    kids[1] = Fib.init;
    kids[1].n = f.n - 2;
    kids[1].node.run = &fibRun;
    f.kids = kids;
    f.node.join = &fibJoin;
    Node*[2] nodes = [&kids[0].node, &kids[1].node];
    return fork(node, nodes[]);
}

private Node* fibJoin(Node* node) nothrow @nogc @system
{
    auto f = cast(Fib*) node;
    f.result = f.kids[0].result + f.kids[1].result;
    freeFrame(f.kids);
    return null;
}

private ulong fibOnDispatcher(uint n)
{
    Fib root;
    root.n = n;
    root.node.run = &fibRun;
    runRoot(&root.node);
    return root.result;
}

bool runFramesTest()
{
    bool ok = true;
    void fail(string what) { printf("frames_test: FAILED: %.*s\n", cast(int) what.length, what.ptr); ok = false; }

    // 1. Every class: alignment, pattern integrity, and reuse without carving.
    foreach (size; [1, 15, 16, 17, 112, 500, 1024])
    {
        enum count = 3000;
        auto blocks = new ubyte*[count];
        foreach (i; 0 .. count)
        {
            blocks[i] = cast(ubyte*) allocFrame(size);
            if (blocks[i] is null || (cast(size_t) blocks[i] & (frameGranule - 1)) != 0) fail("alignment");
            blocks[i][0 .. size] = cast(ubyte) (i & 0xff);
        }
        foreach (i; 0 .. count)
            foreach (b; blocks[i][0 .. size])
                if (b != cast(ubyte) (i & 0xff)) { fail("pattern"); break; }
        immutable chunks = frameStats().chunks;
        foreach (i; 0 .. count) freeFrame(blocks[i]);
        foreach (i; 0 .. count) blocks[i] = cast(ubyte*) allocFrame(size);
        if (frameStats().chunks != chunks) fail("reuse carved a chunk");
        foreach (i; 0 .. count) freeFrame(blocks[i]);
    }

    // 2. Large requests get their own chunk and give it back.
    {
        immutable before = frameStats().large;
        auto p = cast(ubyte*) allocFrame(frameMax + 1);
        auto q = cast(ubyte*) allocFrame(300_000);
        if (p is null || q is null) fail("large null");
        p[0 .. frameMax + 1] = 7;
        q[0 .. 300_000] = 9;
        if (frameStats().large != before + 2) fail("large count");
        freeFrame(p);
        freeFrame(q);
        if (frameStats().large != before) fail("large not returned");
    }

    // 3. Blocks freed by another thread come back to the owner without a new chunk.
    {
        enum count = 20_000;
        __gshared void*[count] handed;
        foreach (i; 0 .. count) handed[i] = allocFrame(112);
        immutable chunks = frameStats().chunks;
        auto t = new Thread({ foreach (i; 0 .. count) freeFrame(handed[i]); });
        t.start();
        t.join();
        foreach (i; 0 .. count) handed[i] = allocFrame(112);
        if (frameStats().chunks != chunks) fail("remote frees were not drained");
        foreach (i; 0 .. count) freeFrame(handed[i]);
    }

    // 4. Frames on the dispatcher, twice: slots and chunks are adopted, not grown.
    {
        import core.time : MonoTime, msecs, seconds;
        import std.exception : enforce;
        import threadpool.groups : GroupHost, GroupHostOptions;
        import threadpool.host : HostGroups;
        import threadpool.hybrid : WorkerSelf;

        Dispatcher d;
        FrameStats runOnce()
        {
            shared uint ready;
            auto a = d.attachment(DispatcherOptions.perGroup());
            auto body = a.body;
            a.start = (WorkerSelf* worker) {
                // Enter the dispatcher before allocating, so its stop hook
                // owns the release even if no Fibonacci work reaches us.
                body(worker);
                auto p = allocFrames!Fib(2);
                enforce(p !is null, "frame warmup allocation failed");
                freeFrame(p);
                atomicOp!"+="(ready, 1u);
            };
            GroupHostOptions o;
            o.groups = HostGroups.all();
            o.farmPerGroup = true;
            o.farm.extraSmallProducers = 1;
            o.pool.skipSmtSiblings = false;
            o.workers = cast(uint) GroupHost.resolve(o).workers.length;
            if (o.workers >= maxParticipants) o.workers = maxParticipants - 1;
            o.callerConsumes = o.workers == 0;
            o.attachments = [a];
            auto host = new GroupHost(o);
            scope (exit) host.close();
            d.start(host);
            scope (exit) d.stop();
            auto p = allocFrames!Fib(2);
            enforce(p !is null, "caller frame warmup allocation failed");
            freeFrame(p);
            immutable deadline = MonoTime.currTime + 10.seconds;
            while (atomicLoad(ready) != host.workers)
            {
                enforce(MonoTime.currTime < deadline, "frame warmup timed out");
                Thread.sleep(1.msecs);
            }
            if (fibOnDispatcher(25) != 75_025) fail("fib result");
            d.stop();
            host.close();
            return frameStats();
        }

        // Fibonacci alone need not allocate on every participant: e.g. 19
        // threads on the first run and 20 on the second is legitimate growth.
        // Warm every participant on both runs before checking slot adoption.
        immutable after1 = runOnce();
        immutable after2 = runOnce();
        if (after2.slots != after1.slots) fail("slots grew across a restart");
        if (after2.chunks > after1.chunks + after1.slots) fail("chunks grew across a restart");
    }

    // 5. A free into a parked slot, then a new thread adopts that slot.
    {
        __gshared void* orphan;
        auto t = new Thread({ orphan = allocFrame(64); releaseFrames(); });
        t.start();
        t.join();
        freeFrame(orphan);             // remote push into a parked slot
        immutable chunks = frameStats().chunks;
        auto u = new Thread({ auto p = allocFrame(64); freeFrame(p); releaseFrames(); });
        u.start();
        u.join();
        if (frameStats().chunks > chunks + 1) fail("adopter carved more than one chunk");
    }

    printf("frames_test: %s\n", ok ? "ok".ptr : "FAILED".ptr);
    return ok;
}
