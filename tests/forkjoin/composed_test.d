/// The dispatcher as an attachment of a host the application owns: alone on
/// a raw-lane GroupHost; beside a GC-enabled service attachment, which puts
/// the host on its managed lane while the dispatcher's body still consumes;
/// and inside a FarmHost through `FarmHostOptions.attachments`, where the
/// FarmHost's own consuming body and the dispatcher's share each worker.
module composed_test;

import core.atomic;
import core.stdc.stdio : printf;
import core.time : Duration, MonoTime, msecs;
import forkjoin;
import threadpool.groups : GroupAttachment, GroupHost, GroupHostOptions, GroupServiceResult;
import threadpool.host : FarmHost, FarmHostOptions, HostGroups;
import threadpool.hybrid : WorkerSelf;

struct Fib
{
    Node node;
    Fib* kids;
    ulong result;
    uint n;
}

Node* fibRun(Node* node) nothrow @nogc @system
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

Node* fibJoin(Node* node) nothrow @nogc @system
{
    auto f = cast(Fib*) node;
    f.result = f.kids[0].result + f.kids[1].result;
    freeFrame(f.kids);
    return null;
}

ulong fib(uint n)
{
    Fib root;
    root.n = n;
    root.node.run = &fibRun;
    runRoot(&root.node);
    return root.result;
}

private GroupHostOptions hostOptions()
{
    GroupHostOptions o;
    o.groups = HostGroups.all();
    o.idleTimeout = Duration.max;
    o.farm.ringMiB = 2;
    o.farm.extraSmallProducers = 1; // the dispatcher's submit token
    return o;
}

bool runComposedTest()
{
    GroupHostOptions probe;
    probe.groups = HostGroups.all();
    if (GroupHost.resolve(probe).workers.length < 2)
    {
        printf("composed_test: SKIP (fewer than two pool workers)\n");
        return true;
    }

    // 1. Alone on an application host: raw lane, same body as start().
    {
        Dispatcher d;
        auto o = hostOptions();
        o.attachments = [d.attachment(DispatcherOptions.oneFarm())];
        auto host = new GroupHost(o);
        if (host.managedLane) { printf("composed_test: dispatcher alone selected the managed lane\n"); return false; }
        d.start(host);
        immutable r = fib(27);
        if (r != 196_418) { printf("composed_test: fib(27) on an owned host = %llu\n", r); return false; }
        d.stop();
        host.close();
        printf("composed_test: dispatcher alone on an application host, raw lane, %u workers\n", host.workers);
    }

    // 2. Beside a service attachment: managed lane; the dispatcher's view
    //    still consumes its Farm, the service hook is stepped and its
    //    deadline parks the workers between fib runs.
    {
        Dispatcher d;
        shared ulong serves;
        GroupAttachment service;
        service.name = "service";
        service.serve = (WorkerSelf*) {
            atomicOp!"+="(serves, 1UL);
            return GroupServiceResult(false, (MonoTime.currTime + 5.msecs).ticks);
        };
        auto o = hostOptions();
        o.attachments = [d.attachment(DispatcherOptions.perGroup()), service];
        auto host = new GroupHost(o);
        if (!host.managedLane) { printf("composed_test: service attachment did not select the managed lane\n"); return false; }
        d.start(host);
        foreach (_; 0 .. 3)
            if (fib(25) != 75_025) { printf("composed_test: fib(25) beside a service attachment\n"); return false; }
        if (atomicLoad(serves) == 0) { printf("composed_test: service hook never stepped\n"); return false; }
        d.stop();
        // The host stays open and its workers keep serving: the dispatcher's
        // tables are freed only by close.
        immutable before = atomicLoad(serves);
        while (atomicLoad(serves) == before) {}
        host.close();
        printf("composed_test: dispatcher beside a service attachment, managed lane, %llu service steps\n",
            atomicLoad(serves));
    }

    // 3. Inside a FarmHost: two consuming attachments per worker (the host's
    //    body and the dispatcher's), fib on the control thread.
    {
        Dispatcher d;
        FarmHostOptions fo;
        fo.workers = 2;
        fo.idleTimeout = Duration.max;
        fo.farm.ringMiB = 2;
        fo.farm.extraSmallProducers = 1;
        fo.attachments = [d.attachment(DispatcherOptions.oneFarm())];
        auto farmHost = new FarmHost(fo);
        d.start(farmHost.groupHost);
        immutable r = fib(27);
        if (r != 196_418) { printf("composed_test: fib(27) inside a FarmHost = %llu\n", r); return false; }
        d.stop();
        farmHost.step();
        farmHost.close();
        printf("composed_test: dispatcher inside a FarmHost (two consuming attachments per worker)\n");
    }
    printf("composed_test: ok\n");
    return true;
}
