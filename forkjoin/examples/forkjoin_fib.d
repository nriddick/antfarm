/+ dub.sdl:
    dependency "antfarm:forkjoin" path="../.."
+/
/++
 + SPDX-License-Identifier: MIT
 +
 + Smallest complete fork-join program: fib(30) on the calling thread plus
 + one pinned worker per remaining eligible processor of the first LLC group.
 + Every call is a heap frame beginning with a `Node`; `fork` pushes the
 + first child onto this participant's deque and returns the second to run
 + next; the last child to finish runs the parent's join.
 +
 +   dub run :forkjoin -c fib
 +/
module forkjoin_fib;

import forkjoin;
import threadpool;
import core.stdc.stdio : printf;

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
    auto kids = allocFrames!Fib(2); // the dispatcher's frame pool
    kids[0] = Fib.init; kids[0].n = f.n - 1; kids[0].node.run = &fibRun;
    kids[1] = Fib.init; kids[1].n = f.n - 2; kids[1].node.run = &fibRun;
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

void main()
{
    auto topo = CacheAwarePool.topology();
    ProcessorId[] cpus;
    foreach (ref p; topo.processors)
        if (topo.isEligible(p) && (cpus.length == 0 || p.llcIndex == topo.byId(cpus[0]).llcIndex))
            cpus ~= ProcessorId.of(p);
    Dispatcher pool;
    pool.start(cpus);
    scope (exit) pool.stop();

    Fib root;
    root.n = 30;
    root.node.run = &fibRun;
    runRoot(&root.node);
    printf("fib(30) = %llu on %zu participants\n", root.result, cpus.length);
}
