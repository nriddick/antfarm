/++
 + ActorWave over externally selected actors on one Farm, possibly two
 + runtimes. `finished` does not join the completion hook. See examples/README.md.
 +/
module public_wave;

import antfarm;
import actors;
import example_host;
import core.atomic : MemoryOrder, atomicLoad, atomicStore;
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

bool retire(T)(ref ActorOwner!T owner) nothrow @nogc @system
{
    immutable result = owner.requestRetire();
    return result == ActorRetireResult.requested || result == ActorRetireResult.alreadyRetired;
}

// Seal even if this returns false: a failed publish still owns a generation.
bool publishSelected(ref ActorWave wave, ActorHandle!Phase[] actors,
        ref Token token, ExampleHost host)
{
    size_t offset;
    while (offset < actors.length)
    {
        immutable written = wave.publish!advance(actors[offset .. $], token);
        if (written == 0)
        {
            if (wave.handle.failed) return false;
            host.progress();
            continue;
        }
        offset += written;
    }
    return true;
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
    uint workers = args.length > 1 ? to!uint(args[1]) : 0;
    auto host = new ExampleHost(workers);
    auto token = host.farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");

    // External selection may span runtimes. The wave requires the same Farm
    // and matching state size/alignment, not one ActorRuntime.
    auto runtimeA = ActorRuntime.create(host.farm, 2);
    auto runtimeB = ActorRuntime.create(host.farm, 1);
    if (runtimeA is null || runtimeB is null) fatal("runtime allocation failed");

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
    immutable admitted = publishSelected(wave, selected[], token, host);
    auto completion = wave.seal();
    while (!completion.finished)
        host.progress();
    if (!admitted || completion.failed)
        fatal("wave admission or completion failed");

    // finished joins actor writes, not the hook. Do not free hook or this
    // ActorWave, and do not begin again, until the hook's last access.
    if (values[0] != 1 || values[1] != 1 || values[2] != 0)
        fatal("selected writes were not visible at finished");
    while (atomicLoad!(MemoryOrder.acq)(hookReturned) == 0)
        host.progress();
    if (atomicLoad!(MemoryOrder.acq)(hookObserved) != hook.marker)
        fatal("hook context was not published by its last access");

    // Reuse is allowed now. The old handle's finished flag becomes false.
    wave.begin(host.farm);
    if (completion.valid || completion.finished)
        fatal("wave reuse left the old handle looking current");
    if (!publishSelected(wave, selected[], token, host))
        fatal("second wave admission failed");
    auto second = wave.seal();
    while (!second.finished)
        host.progress();
    if (second.failed || values[0] != 2 || values[1] != 2 || values[2] != 0)
        fatal("dependent wave mismatch");

    // Owners are move-only. Do not put them in an array: copying transfers
    // the authority and leaves the source unable to observe retirement.
    if (!retire(selectedA) || !retire(unselected) || !retire(selectedB))
        fatal("retirement rejected");
    while (!selectedA.retired || !unselected.retired || !selectedB.retired)
    {
        runtimeA.flush(token);
        runtimeB.flush(token);
        host.progress();
    }
    if (selectedA.reclaim() != ActorReclaimResult.reclaimed
            || unselected.reclaim() != ActorReclaimResult.reclaimed
            || selectedB.reclaim() != ActorReclaimResult.reclaimed)
        fatal("reclaim failed");

    host.stopConsumers();
    runtimeA.destroy();
    runtimeB.destroy();
    host.farm.unregisterProducer(token);
    host.farm.destroy();
    writeln("public wave: two runtimes, hook joined after finished, workers=",
        workers);
}
