/++
 + Private hierarchy: typed result ports, root `waitChildren`, a child
 + continuation fold, a subset wave, non-POD state with a destructor, and a
 + drain to `live == 0`. Build notes: examples/README.md.
 +/
module private_tree;

import antfarm;
import actors;
import example_host;
import core.atomic : MemoryOrder, atomicLoad;
import core.lifetime : move;
import core.stdc.stdlib : malloc;
import std.conv : to;
import std.stdio : writeln;

struct Leaf
{
    ulong input;
}

ulong square(scope ref PrivateBorrow!Leaf actor, scope ref PrivateContext)
    nothrow @nogc @system
{
    return actor.value.input * actor.value.input;
}

struct Folder
{
    PrivateChildPark!(Leaf, ulong)* leaves;
}

// One enclosing wave: launch a subset, suspend, then fold those results.
PrivateChildStep!ulong fold(scope ref PrivateBorrow!Folder actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    if (actor.value.leaves is null)
    {
        actor.value.leaves = context.createChildren!(Leaf, ulong)(4,
            PrivateCohortOptions(0, true));
        if (actor.value.leaves is null)
        {
            context.fail(1); // no wave of this member is active yet
            return typeof(return).done(0);
        }
        foreach (i; 0 .. 4)
            if (actor.value.leaves.spawn(context, Leaf(i + 1))
                    != PrivateSpawnResult.spawned)
            {
                context.fail(2);
                return typeof(return).done(0);
            }
        uint[2] members = [3, 1]; // inputs 4 and 2, in that list order
        if (actor.value.leaves.startSubset!square(context, members[])
                != PrivateStartResult.started)
            fatal("subset wave rejected");
        return typeof(return).waitChildren();
    }
    auto values = actor.value.leaves.results(context);
    if (values.length != 2 || values[0] != 16 || values[1] != 4)
        fatal("subset results are not in list order");
    ulong sum;
    foreach (value; values) sum += value;
    return typeof(return).done(sum);
}

struct Tree
{
    ulong* scratch;
    shared uint* freed;
    uint* callbacks;
    PrivateChildPark!(Folder, ulong)* folders;
    ulong* total;
    @disable this(this);
    // Runs for the stored root and for every moved-from .init argument.
    // .init has a null scratch; only the stored instance frees it.
    ~this() nothrow @nogc
    {
        import core.stdc.stdlib : free;
        import core.atomic : MemoryOrder, atomicFetchAdd;
        if (scratch is null) return;
        free(scratch);
        atomicFetchAdd!(MemoryOrder.rel)(*freed, 1u);
    }
}

PrivateRootDisposition reduce(scope ref PrivateBorrow!Tree actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    ++*actor.value.callbacks;
    if (actor.value.folders is null)
    {
        actor.value.folders = context.createChildren!(Folder, ulong)(2);
        if (actor.value.folders is null) fatal("folder park allocation failed");
        foreach (_; 0 .. 2)
            if (actor.value.folders.spawn(context, Folder.init)
                    != PrivateSpawnResult.spawned)
                fatal("folder spawn failed");
        // Terminal continuation: descendants are allowed; retirement waits
        // for done and their join, then destroys folder state on a consumer.
        if (actor.value.folders.startFinal!fold(context) != PrivateStartResult.started)
            fatal("folder wave rejected");
        return PrivateRootDisposition.waitChildren;
    }
    auto values = actor.value.folders.results(context);
    if (values.length != 2) fatal("folder result count mismatch");
    *actor.value.scratch = 0;
    foreach (value; values) *actor.value.scratch += value;
    *actor.value.total = *actor.value.scratch;
    return PrivateRootDisposition.finish;
}

void main(string[] args)
{
    uint workers = args.length > 1 ? to!uint(args[1]) : 0;
    auto host = new ExampleHost(workers);
    auto token = host.farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");
    auto park = PrivateRootPark!(Tree, reduce).create(host.farm, 1);
    if (park is null) fatal("root park allocation failed");

    auto scratch = cast(ulong*) malloc(ulong.sizeof);
    if (scratch is null) fatal("scratch allocation failed");
    *scratch = 0;
    shared uint freed;
    uint callbacks;
    ulong total;
    auto tree = Tree(scratch, &freed, &callbacks, null, &total);
    if (park.spawn(move(tree)) != PrivateSpawnResult.spawned)
        fatal("root spawn failed");

    // pump publishes root activations only. Zero does not mean the folders
    // or leaves are idle, and it is not a reason to stop. live includes
    // waiting roots and notification tails; drain until collection drops it.
    uint zeroPumps;
    while (park.live != 0)
    {
        if (park.pump(token) == 0) ++zeroPumps;
        host.progress();
    }
    if (total != 40 || callbacks != 2 || atomicLoad!(MemoryOrder.acq)(freed) != 1)
        fatal("private tree result mismatch");

    host.stopConsumers();
    park.destroy();
    park = null; // destroy(park) would not free the park
    host.farm.unregisterProducer(token);
    host.farm.destroy();
    writeln("private tree: subset fold 40, root callbacks=", callbacks,
        ", workers=", workers, ", pumps publishing zero roots=", zeroPumps);
}
