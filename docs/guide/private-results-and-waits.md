# Private results, waits, and waves

A private parent drives its child cohorts with `start`, `startSubset`, or
`startFinal`; joins with `finished(context)` or a root `waitChildren`; and reads
joined results with `results(context)`. The API and ownership contract are in
[private-actors.md](private-actors.md); the proof chains behind the joins are in
[../design/private-actor-ordering.md](../design/private-actor-ordering.md).

## A conductor with child results

Each child returns its value into cohort-owned storage. The parent reads the
slots in spawn order on its resumption after `waitChildren`, even after
`startFinal` has freed member state. Only the root's total remains owned by the
application, which keeps it alive until the root is collected.

```d
import antfarm;
import actors;
import std.stdio : writeln;

struct Item { ulong input; }
ulong square(scope ref PrivateBorrow!Item actor, scope ref PrivateContext context)
    nothrow @nogc @system
{
    return actor.value.input * actor.value.input;
}
struct Conductor
{
    PrivateChildPark!(Item, ulong)* children;
    ulong* total;
}
PrivateRootDisposition conduct(scope ref PrivateBorrow!Conductor actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    if (actor.value.children is null)
    {
        actor.value.children = context.createChildren!(Item, ulong)(4);
        if (actor.value.children is null) fatal("child park allocation failed");
        foreach (i; 0 .. 4)
            if (actor.value.children.spawn(context, Item(i + 1)) != PrivateSpawnResult.spawned)
                fatal("child spawn failed");
        if (actor.value.children.startFinal!square(context) != PrivateStartResult.started)
            fatal("child wave failed");
        return PrivateRootDisposition.waitChildren;
    }
    foreach (r; actor.value.children.results(context)) *actor.value.total += r;
    return PrivateRootDisposition.finish;
}
void main()
{
    ulong total; // Root-to-application output stays application-owned.
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");
    ConsumerView view;
    if (view.subscribe(farm) < 0) fatal("consumer subscription failed");
    auto park = PrivateRootPark!(Conductor, conduct).create(farm, 1);
    if (park is null) fatal("private park allocation failed");
    auto spawned = park.spawn(Conductor(null, &total));
    if (spawned != PrivateSpawnResult.spawned) fatal("private spawn failed");
    if (park.live != 1) fatal("expected one live root after spawn");
    writeln("live=1 after spawn");
    while (park.live != 0)
    {
        park.pump(token);
        while (view.consumeNext()) {}
    }
    if (total != 30) fatal("joined result mismatch");
    park.destroy();
    park = null;
    view.unsubscribe();
    farm.unregisterProducer(token);
    farm.destroy();
}
```

## Result lifetime and waves

Slots are allocated eagerly with the cohort through its inherited allocator and
freed when the cohort is disposed. Results are empty before the first nonempty
wave. A result-producing wave replaces the participant slots; a `void` wave,
including a `PrivateChildDisposition` operation, clears the result length. An
empty start preserves previous results, as do rejected busy or retiring starts.
Spawning another member preserves the previous slice; the next whole wave
includes that member at its new spawn index.

`results(context)` is fatal while the wave is active. Polling parents retry
`finished(context)` before reading. Wrong-parent and default contexts are fatal
in debug and release. The slice is valid only within the authorized parent's
callback and until the next nonempty `start`, `startFinal`, or `startSubset`;
retaining it beyond that boundary violates the `@system` contract. Pointers
inside a POD result follow application ownership rules; any pointer into child
state must not be returned as a result value.

A result must not refer to a resource its member's destructor releases. To
transfer ownership, clear the owning field in `T` before returning the pointer
as `R`. Result slots outlive member retirement and die with the cohort.

## Peers' previous-wave projections

The [Conway's Game of Life sample](../../examples/README.md#conways-game-of-life)
is a worked example of application-owned double buffering: one private actor
per cell reads the previous generation's neighbours and writes its own next
projection slot. A root joins with `waitChildren` before swapping buffers and
starting the next wave. The buffers outlive every cohort, and the application
reads/edits them only after the tracked root drains. See the sample for the
join-to-next-publication ordering argument and the POSIX terminal front end.

## Pipelined tree reduction

A child-level parent launches grandchildren in wave g and folds their results
in wave g+1. The enclosing wave cannot join until the grandchildren join, so
those results are idle on the next callback. For a root → middle → leaf tree:

1. The root starts a `void` middle operation that creates leaf cohorts and
   calls `leaves.startFinal!leafValue(context)`, then returns `waitChildren`.
2. On resumption it calls `mids.startFinal!midFold(context)` and waits again.
   Each retiring middle callback sums `leaves.results(context)` and returns that
   sum into its own result slot. Reading in a retiring context is allowed.
3. The root folds `mids.results(context)` and finishes.

The nested test [../../private_actor_tests/main.d](../../private_actor_tests/main.d)
exercises this three-level pipeline on five workers. Each parent reads only its
direct children's results. External result channels remain the route for
root-to-application output and application-defined cross-level routing; they
require their own ownership and synchronization contracts.

## Resumable child folds

`PrivateChildDisposition` has `done` and `waitChildren`. For result cohorts,
`PrivateChildStep!R.done(value)` supplies the final value and
`PrivateChildStep!R.waitChildren()` suspends without writing a result. The same
operation is called again, with phase stored in `T`; no borrow or context
survives the call. Returning another type is a compile error. A cohort's `R`
cannot itself be `PrivateChildDisposition` or any `PrivateChildStep` type.

A wait joins every wave that member started in this enclosing wave, including
descendants and notification tails. With no outstanding work it yields to one
later emitter publication, never an inline call. `done` can return while
children are active: no more user callbacks run, but the enclosing wave still
waits for those descendants. The parent's `finished` stays false and `results`
is fatal throughout suspension. Whole and subset waves share this behavior;
subset results stay in participant order. Members can alternate plain, result,
and continuation operations across waves.

`startFinal` allows continuation members to create and launch descendants.
Retirement happens after `done` and their join. If the join is deferred, its
final owner queues an internal cleanup activation, even when that owner is the
emitter seal. Only a consumer destroys stored state, once, post-order, after
writing the final result. The cleanup activation does not call user code.

This example folds leaves inside each middle member's single enclosing wave.
The root waits once and reads its direct children's results.

```d
import antfarm;
import actors;

struct Leaf { ulong input; }
ulong square(scope ref PrivateBorrow!Leaf b, scope ref PrivateContext c)
    nothrow @nogc @system { return b.value.input * b.value.input; }
struct Mid { PrivateChildPark!(Leaf, ulong)* leaves; }
PrivateChildStep!ulong fold(scope ref PrivateBorrow!Mid b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.leaves is null)
    {
        b.value.leaves = c.createChildren!(Leaf, ulong)(4);
        if (b.value.leaves is null)
        {
            c.fail(1); // no wave of this member is active yet
            return typeof(return).done(0);
        }
        foreach (i; 1 .. 5)
            if (b.value.leaves.spawn(c, Leaf(i)) != PrivateSpawnResult.spawned)
            {
                c.fail(2);
                return typeof(return).done(0);
            }
        if (b.value.leaves.startFinal!square(c) != PrivateStartResult.started)
            fatal("leaf wave failed");
        return typeof(return).waitChildren();
    }
    ulong sum;
    foreach (value; b.value.leaves.results(c)) sum += value;
    return typeof(return).done(sum);
}
struct Root { PrivateChildPark!(Mid, ulong)* mids; ulong* total; }
PrivateRootDisposition rootStep(scope ref PrivateBorrow!Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.mids is null)
    {
        b.value.mids = c.createChildren!(Mid, ulong)(2);
        if (b.value.mids is null) fatal("middle park allocation failed");
        foreach (_; 0 .. 2)
            if (b.value.mids.spawn(c, Mid.init) != PrivateSpawnResult.spawned)
                fatal("middle spawn failed");
        if (b.value.mids.startFinal!fold(c) != PrivateStartResult.started)
            fatal("middle wave failed");
        return PrivateRootDisposition.waitChildren;
    }
    foreach (value; b.value.mids.results(c)) *b.value.total += value;
    return PrivateRootDisposition.finish;
}
void main()
{
    ulong total;
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");
    ConsumerView view;
    if (view.subscribe(farm) < 0) fatal("consumer subscription failed");
    auto park = PrivateRootPark!(Root, rootStep).create(farm, 1);
    if (park is null) fatal("root park allocation failed");
    if (park.spawn(Root(null, &total)) != PrivateSpawnResult.spawned)
        fatal("root spawn failed");
    while (park.live != 0)
    {
        park.pump(token);
        while (view.consumeNext()) {}
    }
    if (total != 60) fatal("child fold mismatch");
    park.destroy();
    view.unsubscribe();
    farm.unregisterProducer(token);
    farm.destroy();
}
```

The child custody/lost-wake argument behind these joins is in
[../design/private-actor-ordering.md](../design/private-actor-ordering.md#private-child-continuation-ordering).

## Subset waves

Create a cohort with `context.createChildren!(T, R)(capacity,
PrivateCohortOptions(batchLimit, true))` to reserve subset storage. A zero
options batch limit inherits the creating context's effective limit. The two
existing creation overloads keep their behavior; their cohorts have no subset
storage. Creation returns null on allocation failure, overflowing allocation
sizes, zero capacity, or reserved capacity above `uint.max`.

`startSubset!operation(context, members)` copies a list of `uint` spawn indices
into the reserved storage and authorizes only those members. The emitter
publishes them in list order; consumer execution can run concurrently and
complete in a different order. Out-of-range indices, duplicates, and an
unreserved cohort are fatal in debug and release, including an empty list on an
unreserved cohort. Wrong-parent and default contexts are also fatal.

There is one active wave per cohort. A busy start returns `waveActive` before
validating or copying the list and changes nothing. A retiring context returns
`retiring`. An empty list returns `empty`, takes no credit, and preserves both
results and the last selection. `subsetMembers(context)` returns that selection
for the most recent nonempty subset, or empty after a nonempty `start` or
`startFinal`. Its slice is valid within the authorized callback until the next
nonempty start; the input list may be reused as soon as `startSubset` returns.

For result cohorts, `results(context)[j]` belongs to
`subsetMembers(context)[j]`. A void subset clears the result length. Unselected
members retain their state, and subsets never destroy members. There is no
`startFinalSubset` or `startSubsetFinal`; whole-cohort `startFinal` and parent
disposal retire every member. Descendant joins and root `waitChildren` use the
same Wave join as whole waves. On 64-bit targets the fixed cohort metadata grows
by 16 bytes (one selection pointer and a count, with the subset flag in existing
padding); Entry remains 40 bytes. When every member is needed, use `start` to
avoid subset copying and indirection.

This complete example selects members 3 and 1, then reads their squares in that
order after the join. Parent retirement also disposes unselected members.

```d
import antfarm;
import actors;

struct Item { ulong input; }
ulong square(scope ref PrivateBorrow!Item actor, scope ref PrivateContext context)
    nothrow @nogc @system
{
    return actor.value.input * actor.value.input;
}
struct Conductor { PrivateChildPark!(Item, ulong)* children; ulong* total; }
PrivateRootDisposition conduct(scope ref PrivateBorrow!Conductor actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    if (actor.value.children is null)
    {
        actor.value.children = context.createChildren!(Item, ulong)(4,
            PrivateCohortOptions(0, true));
        if (actor.value.children is null) fatal("child park allocation failed");
        foreach (i; 0 .. 4)
            if (actor.value.children.spawn(context, Item(i + 1)) != PrivateSpawnResult.spawned)
                fatal("child spawn failed");
        uint[2] members = [3, 1];
        if (actor.value.children.startSubset!square(context, members[]) != PrivateStartResult.started)
            fatal("subset wave failed");
        return PrivateRootDisposition.waitChildren;
    }
    auto members = actor.value.children.subsetMembers(context);
    auto values = actor.value.children.results(context);
    if (members.length != 2 || members[0] != 3 || members[1] != 1
        || values.length != 2 || values[0] != 16 || values[1] != 4)
        fatal("subset results order mismatch");
    foreach (value; values) *actor.value.total += value;
    return PrivateRootDisposition.finish;
}
void main()
{
    ulong total;
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");
    ConsumerView view;
    if (view.subscribe(farm) < 0) fatal("consumer subscription failed");
    auto park = PrivateRootPark!(Conductor, conduct).create(farm, 1);
    if (park is null) fatal("private park allocation failed");
    if (park.spawn(Conductor(null, &total)) != PrivateSpawnResult.spawned)
        fatal("private spawn failed");
    while (park.live != 0)
    {
        park.pump(token);
        while (view.consumeNext()) {}
    }
    if (total != 20) fatal("subset total mismatch");
    park.destroy();
    view.unsubscribe();
    farm.unregisterProducer(token);
    farm.destroy();
}
```
