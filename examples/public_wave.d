/++
 + ActorWave over externally selected actors on one Farm, possibly two
 + runtimes. `finished` does not join the completion hook. See examples/README.md.
 +/
module public_wave;

import antfarm;
import actors;
import threadpool.host;
import core.atomic : MemoryOrder, atomicLoad, atomicStore;
import core.time : seconds;
import std.conv : to;
import std.stdio : writeln;

struct Phase
{
    ulong* cell;
}

// Never woken. The wave operation is a different function and has no ActorContext.
void unusedWake(scope ref ActorBorrow!Phase, scope ref ActorContext) nothrow @nogc @system {}

void advance(scope ref ActorBorrow!Phase actor) nothrow @nogc @system
{
    *actor.value.cell += 1;
}

struct HookState
{
    ulong marker;
    shared ulong* observed;
    shared uint* returned;
}

// Runs after `finished` is release-published, possibly on another thread.
// The stores below are this hook's last access. It must not destroy the
// ActorWave, and it must not touch a descriptor the orchestrator has reused.
void onWave(void* context) nothrow @nogc @system
{
    auto hook = cast(HookState*) context;
    atomicStore!(MemoryOrder.rel)(*hook.observed, hook.marker);
    atomicStore!(MemoryOrder.rel)(*hook.returned, 1u);
}

void main(string[] args)
{
    FarmHostOptions hostOptions;
    hostOptions.workers = args.length > 1 ? to!uint(args[1]) : 0;
    hostOptions.waitTimeout = 30.seconds; // example policy: a stuck drain fails instead of hanging
    auto host = new FarmHost(hostOptions);
    scope (exit) host.closeOrAbandon();

    // External selection may span runtimes. The wave requires the same Farm
    // and matching state size/alignment, not one ActorRuntime.
    auto runtimeA = host.createRuntime(2);
    auto runtimeB = host.createRuntime(1);

    ulong[3] values;
    auto selectedA = runtimeA.createActor!(Phase, unusedWake)(Phase(&values[0]));
    auto unselected = runtimeA.createActor!(Phase, unusedWake)(Phase(&values[2]));
    auto selectedB = runtimeB.createActor!(Phase, unusedWake)(Phase(&values[1]));
    if (!selectedA.valid || !unselected.valid || !selectedB.valid)
        fatal("actor creation failed");
    ActorHandle!Phase[2] selected = [selectedA.handle, selectedB.handle];

    shared ulong hookObserved;
    shared uint hookReturned;
    HookState hook;
    hook.marker = 7;
    hook.observed = &hookObserved;
    hook.returned = &hookReturned;

    ActorWave wave;
    wave.begin(host.farm, ActorWaveCompletionHook(&hook, &onWave));
    // publish steps on backpressure and returns false if the wave failed.
    // Seal even then: a failed publish still owns a generation.
    immutable admitted = host.publish!advance(wave, selected[]);
    auto completion = wave.seal();
    host.waitUntil(() => completion.finished);
    if (!admitted || completion.failed)
        fatal("wave admission or completion failed");

    // finished joins actor writes, not the hook. Do not free hook or this
    // ActorWave, and do not begin again, until the hook's last access.
    if (values[0] != 1 || values[1] != 1 || values[2] != 0)
        fatal("selected writes were not visible at finished");
    host.waitUntil(() => atomicLoad!(MemoryOrder.acq)(hookReturned) != 0);
    if (atomicLoad!(MemoryOrder.acq)(hookObserved) != hook.marker)
        fatal("hook context was not published by its last access");

    // Reuse is allowed now. The old handle's finished flag becomes false.
    wave.begin(host.farm);
    if (completion.valid || completion.finished)
        fatal("wave reuse left the old handle looking current");
    if (!host.publish!advance(wave, selected[]))
        fatal("second wave admission failed");
    auto second = wave.seal();
    host.waitUntil(() => second.finished);
    if (second.failed || values[0] != 2 || values[1] != 2 || values[2] != 0)
        fatal("dependent wave mismatch");

    // Owners are move-only: retire takes them by ref (never copy them into an
    // array). It requests retirement of each, waits for all, reclaims all.
    host.retire(selectedA, unselected, selectedB);
    writeln("public wave: two runtimes, hook joined after finished, workers=",
        host.resolution.workers.length);
}
