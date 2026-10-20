/+ dub.sdl:
    dependency "antfarm:threadpool" path=".."
+/
/++
 + Fail-fast fold (U3): a leaf reports a domain failure with `context.fail`
 + and stops its siblings with `context.cancelPeers`. The emitter truncates
 + the cancelled wave at a table boundary, the root reads the aggregate
 + through `outcome` after the join, and propagates with its own root `fail`.
 + Build notes: examples/README.md.
 +/
module fail_fast_fold;

import antfarm;
import antfarm.actors;
import threadpool.host;
import core.atomic : MemoryOrder, atomicFetchAdd, atomicLoad;
import core.time : seconds;
import std.conv : to;
import std.stdio : writeln;

struct Leaf
{
    ulong input;
    shared uint* ran;
}

// One failing leaf stops the rest. A failed result member still returns an R;
// the fold does not use it because the wave reports a failure.
ulong compute(scope ref PrivateBorrow!Leaf actor, scope ref PrivateContext context)
    nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.rel)(*actor.value.ran, 1u);
    if (actor.value.input == 5)
    {
        context.fail(7);       // report, then keep the wave from doing more
        context.cancelPeers(); // undispatched siblings are skipped
        return ulong.init;
    }
    return actor.value.input * actor.value.input;
}

struct Fold
{
    PrivateChildPark!(Leaf, ulong)* leaves;
    ulong* total;
    size_t* dispatched;
    uint* failures;
    shared uint* ran;
}

PrivateRootDisposition fold(scope ref PrivateBorrow!Fold actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    if (actor.value.leaves is null)
    {
        // Batch limit 1 makes the table-boundary truncation visible on a
        // same-thread drain: the failing member is published before its
        // undispatched siblings.
        actor.value.leaves = context.createChildren!(Leaf, ulong)(8,
            PrivateCohortOptions(1, false));
        if (actor.value.leaves is null)
        {
            context.fail(1); // partial construction: no wave is active yet
            return PrivateRootDisposition.finish;
        }
        foreach (i; 0 .. 8)
            if (actor.value.leaves.spawn(context, Leaf(i + 1, actor.value.ran))
                    != PrivateSpawnResult.spawned)
            {
                context.fail(2); // some members exist, no wave is active
                return PrivateRootDisposition.finish;
            }
        if (actor.value.leaves.start!compute(context) != PrivateStartResult.started)
            fatal("fail-fast wave rejected");
        return PrivateRootDisposition.waitChildren;
    }
    auto outcome = actor.value.leaves.outcome(context);
    *actor.value.dispatched = outcome.dispatched;
    *actor.value.failures = outcome.failures;
    if (outcome.failures != 0)
    {
        context.fail(outcome.firstFailureCode); // explicit upward propagation
        return PrivateRootDisposition.finish;
    }
    foreach (value; actor.value.leaves.results(context))
        *actor.value.total += value;
    return PrivateRootDisposition.finish;
}

void main(string[] args)
{
    FarmHostOptions hostOptions;
    hostOptions.workers = args.length > 1 ? to!uint(args[1]) : 0;
    hostOptions.waitTimeout = 30.seconds; // example policy: a stuck drain fails instead of hanging
    auto host = new FarmHost(hostOptions);
    scope (exit) host.closeOrAbandon();

    PrivateRootOptions options;
    options.tracking = true;
    auto park = host.createPark!(Fold, fold)(1, options);

    shared uint ran;
    ulong total;
    size_t dispatched;
    uint failures;
    if (park.spawn(Fold(null, &total, &dispatched, &failures, &ran))
            != PrivateSpawnResult.spawned)
        fatal("root spawn failed");

    // Root-to-application output stays application-owned; read it only after
    // the root terminal outcome is taken. A tracked drain pumps and takes.
    PrivateRootOutcome outcome;
    host.waitUntil(() {
        PrivateRootOutcome taken;
        while (park.takeOutcome(taken))
            outcome = taken;
        return park.live == 0;
    });

    immutable leafRuns = atomicLoad!(MemoryOrder.acq)(ran);
    if (outcome.status != PrivateOutcomeStatus.failed || outcome.code != 7)
        fatal("root did not propagate the leaf failure");
    // The failing member is at position 4, so dispatch is the prefix 0..4 at
    // least; each dispatched member runs once before the wave joins.
    if (failures != 1 || dispatched < 5 || leafRuns != dispatched)
        fatal("fold aggregate mismatch");

    writeln("fail-fast fold: code=", outcome.code, ", failures=", failures,
        ", dispatched=", dispatched, "/8, leaves run=", leafRuns,
        ", workers=", host.resolution.workers.length);
}
