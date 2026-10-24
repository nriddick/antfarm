// SPDX-License-Identifier: MIT (see forkjoin/LICENSE)
/// Dispatcher test: idle workers park (near-zero CPU while nothing runs),
/// roots submitted from a thread that is not a participant complete with
/// correct results while workers are parked, a participant root runs through
/// `runRoot`, and shutdown drains parked workers. Uses the first eligible
/// processors of the topology; prints a notice and passes with fewer than
/// two.
module admission_test;

import forkjoin;
import threadpool;
import core.atomic;
import core.thread;
import core.time;
import core.stdc.stdio : printf;
import core.stdc.stdlib : malloc, free;
import core.sys.posix.sys.resource : getrusage, rusage, RUSAGE_SELF;

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
    auto kids = cast(Fib*) malloc(2 * Fib.sizeof);
    kids[0] = Fib.init; kids[0].n = f.n - 1; kids[0].node.run = &fibRun;
    kids[1] = Fib.init; kids[1].n = f.n - 2; kids[1].node.run = &fibRun;
    f.kids = kids;
    f.node.join = &fibJoin;
    Node*[2] nodes = [&kids[0].node, &kids[1].node];
    return fork(node, nodes[]);
}

private Node* fibJoin(Node* node) nothrow @nogc @system
{
    auto f = cast(Fib*) node;
    f.result = f.kids[0].result + f.kids[1].result;
    free(f.kids);
    return null;
}

private double cpuSeconds()
{
    rusage u;
    getrusage(RUSAGE_SELF, &u);
    return u.ru_utime.tv_sec + u.ru_utime.tv_usec * 1e-6 + u.ru_stime.tv_sec + u.ru_stime.tv_usec * 1e-6;
}

private immutable ulong[] fibs = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987,
    1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811, 514229, 832040];

private __gshared int g_failures;
private __gshared int g_submitted;

private void submitter()
{
    foreach (round; 0 .. 40)
    {
        uint n = 20 + round % 8;
        Fib root;
        root.n = n;
        root.node.run = &fibRun;
        RootCompletion c;
        submitRoot(&root.node, &c);
        waitRoot(&c);
        g_submitted++;
        if (root.result != fibs[n])
        {
            printf("admission_test: round %d: fib(%u) = %llu, expected %llu\n", round, n, root.result, fibs[n]);
            g_failures++;
        }
        if (round % 4 == 3) Thread.sleep(15.msecs); // let every worker park
    }
}

/// Returns true on success. `maxParticipants` caps the processors used.
bool runAdmissionTest(size_t maxParticipantsWanted = 8)
{
    auto topo = CacheAwarePool.topology();
    ProcessorId[] cpus;
    foreach (ref p; topo.processors)
        if (topo.isEligible(p) && cpus.length < maxParticipantsWanted)
            cpus ~= ProcessorId.of(p);
    if (cpus.length < 2)
    {
        printf("admission_test: NOTICE fewer than two eligible processors, nothing to test\n");
        return true;
    }
    g_failures = 0;
    g_submitted = 0;
    Dispatcher pool;
    pool.start(cpus);
    discardCounters();

    {
        Fib root; root.n = 25; root.node.run = &fibRun;
        runRoot(&root.node);
        if (root.result != fibs[25]) { printf("admission_test: runRoot wrong\n"); pool.stop(); return false; }
    }
    Thread.sleep(50.msecs); // spin budgets expire, workers park
    auto before = cpuSeconds();
    Thread.sleep(300.msecs);
    auto idleCpu = cpuSeconds() - before;
    printf("admission_test: %zu participants, idle cpu over 300 ms: %.1f ms\n", cpus.length, idleCpu * 1e3);
    bool idleOk = idleCpu < 0.030;

    auto t = new Thread(&submitter);
    t.start();
    t.join();
    Thread.sleep(50.msecs);
    pool.stop();
    auto s = counters();
    printf("admission_test: foreign submissions %d, failures %d, parks %llu, wakes %llu\n",
        g_submitted, g_failures, s.parks, s.wakes);
    return idleOk && g_failures == 0 && g_submitted == 40;
}
