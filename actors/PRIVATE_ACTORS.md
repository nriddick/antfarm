# Private actors: roots and parent-owned cohorts

`actors.private_actor` implements a separate ownership model for actors that
have no public identity. A root has no external application owner. A child
has exactly one private parent, which can authorize whole-cohort or subset waves but
cannot inspect the child's state. Children can create children of their own.

A single emitter owns each root park and its Farm producer token. Multiple
parks and emitters can share a Farm. They do not share an actor free-list gate,
ready-drain gate, or live counter. Each park also emits its parents' requested
child waves. The emitter inspects scheduling metadata only.

This API supports structs and scalars satisfying `stateAllowed!T` and synchronous callbacks. The existing
`ActorOwner`, `ActorHandle`, inbox, and public `ActorWave` APIs remain available.
It does not change DRuntime Fibers. Farm setup, tokens, pumping, allocators,
storage lifetime, shutdown, and the ranked misuse hazards shared with public
actors are in [LIFECYCLE.md](LIFECYCLE.md).

## Conductors, outputs, and general actors

A private parent is a conductor inside an exclusive hierarchy. Its own state
remains visible only during its dispatches, and it cannot inspect child state
even after a wave joins. Communication uses separately owned results or
signals. To report outward, a callback may copy a result into a result channel,
transfer ownership of an independent result buffer, or publish a completion
signal. Observers receive that output without acquiring a parent-state borrow
or a capability to schedule the parent. Result storage and notification
contexts must outlive their receivers' use; a pointer into retiring actor
state is not an output channel.

General actors also hide their state. Their extra machinery supports outsiders
holding stable submission capabilities, waking the same actor, and racing
retirement or replacement. That model fits independently addressed services,
connections with several event sources, or actors selected by external phase
orchestrators. A general endpoint can serve a private hierarchy without giving
external callers identities for its internal actors. Public waves are useful
when external code selects the actor set; a private conductor instead owns its
child cohort's membership and scheduling authority.

Application-defined result channels are supported. A root can return
`waitChildren` to resume once all its child work joins, without polling in
repeated callbacks. Child cohorts can carry typed per-member results;
root-to-application outputs and library reductions remain future work.
Continuation operations can suspend and resume within the same child wave.

## API

The complete programs below are compiled and run by `make check`, with LDC
and DMD in debug and release. Creation and dispatch happen outside assertions.

```d
import antfarm;
import actors;
import std.stdio : writeln;

struct Work
{
    ulong remaining;
    ulong* resource;
    @disable this(this);
    ~this() nothrow @nogc
    {
        import core.stdc.stdlib : free;
        free(resource);
    }
}
PrivateRootDisposition step(scope ref PrivateBorrow!Work actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    return --actor.value.remaining == 0
        ? PrivateRootDisposition.finish : PrivateRootDisposition.again;
}

void main()
{
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");
    ConsumerView view;
    if (view.subscribe(farm) < 0) fatal("consumer subscription failed");
    auto park = PrivateRootPark!(Work, step).create(farm, 4096);
    if (park is null) fatal("private park allocation failed");
    import core.stdc.stdlib : malloc;
    auto resource = cast(ulong*) malloc(ulong.sizeof);
    if (resource is null) fatal("resource allocation failed");
    auto spawned = park.spawn(Work(4, resource));
    if (spawned != PrivateSpawnResult.spawned) fatal("private spawn failed");
    if (park.live != 1) fatal("expected one live root after spawn");
    writeln("live=1 after spawn");
    while (park.live != 0)
    {
        park.pump(token);
        while (view.consumeNext()) {}
    }
    park.destroy();
    park = null;
    view.unsubscribe();
    farm.unregisterProducer(token);
    farm.destroy();
}
```

Only a private callback can call `context.createChildren!(T, R)(capacity)`. It
stores the resulting opaque `PrivateChildPark!(T, R)*` in its own state. `R` is
a POD result type; its default, `void`, creates a signal-only cohort with no
result storage. `PrivateChildPark!T` is the same type as `PrivateChildPark!(T, void)`. The parent may:

- `children.spawn(context, initializer)` while the cohort is idle;
- `children.start!operation(context)` to authorize a whole-cohort wave;
- `children.startSubset!operation(context, members)` to authorize only the listed
  members, after reserving subset storage at creation;
- `children.subsetMembers(context)` to read that selection in list order;
- `children.finished(context)` to acquire its completion;
- `children.results(context)` to acquire a `const(R)[]` of joined results, when
  `R` is not `void`;
- `children.startFinal!operation(context)` to execute a terminal wave and
  free every member after its final callback.

An operation accepts `scope ref PrivateBorrow!T` and `scope ref PrivateContext`
and returns `void`, the cohort's result type `R`, `PrivateChildDisposition`, or
`PrivateChildStep!R`. The latter two opt into resumable activations. The runtime stores
each return value in participant order: spawn order for whole waves, list order
for subsets. It may launch
descendant waves and return immediately.
Its enclosing wave remains active until those descendants finish. Parent
callbacks must not block waiting for child work. A root returns one of:

| `PrivateRootDisposition` | Next action |
| --- | --- |
| `again` | Eligible for another callback when its physical table returns; child work may still be active. |
| `waitChildren` | Join every direct wave this root started and all their descendants, including notification tails from earlier `again` activations; schedule exactly one later callback. Empty or already-joined work also gets one later callback, never an inline call. |
| `finish` | Retire now; an active child wave is a fatal contract error. Entry reuse waits for any outstanding notification tails. |
| `finishAfterChildren` | Make the same join as `waitChildren`, then retire without another user callback. One internal Farm activation disposes child parks on a consumer and applies the root reclamation policy. |

Joined child writes to externally owned result storage are visible on a
`waitChildren` resumption without calling `finished(context)`. This grants no
access to child state. Plain and result-only terminal child callbacks cannot
launch waves or create children. Terminal continuation callbacks can do both;
retirement waits for `done` and the descendant join, then disposes child parks.

The nested example in [the tests](../private_actor_tests/main.d) runs three
waves through root → child → grandchild trees, followed by child retirement.
The [lifecycle harness](../perftest/actor_churn.d) demonstrates parent-created
cohorts with terminal child waves.

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

### Result lifetime and waves

Slots are allocated eagerly with the cohort through its inherited allocator
and freed when the cohort is disposed. Results are empty before the first
nonempty wave. A result-producing wave replaces the participant slots; a `void`
wave, including a `PrivateChildDisposition` operation, clears the result length. An empty start preserves previous results, as do
rejected busy or retiring starts. Spawning another member preserves the previous
slice; the next whole wave includes that member at its new spawn index.

`results(context)` is fatal while the wave is active. Polling parents retry
`finished(context)` before reading. Wrong-parent and default contexts are fatal
in debug and release. The slice is valid only within the authorized parent's
callback and until the next nonempty `start`, `startFinal` or `startSubset`;
retaining it beyond that
boundary violates the `@system` contract. Pointers inside a POD result follow
application ownership rules; any pointer into child state must not be returned
as a result value.

### Pipelined tree reduction

A child-level parent launches grandchildren in wave g and folds their results
in wave g+1. The enclosing wave cannot join until the grandchildren join, so
those results are idle on the next callback. For a root → middle → leaf tree:

1. The root starts a `void` middle operation that creates leaf cohorts and
   calls `leaves.startFinal!leafValue(context)`, then returns `waitChildren`.
2. On resumption it calls `mids.startFinal!midFold(context)` and waits again.
   Each retiring middle callback sums `leaves.results(context)` and returns
   that sum into its own result slot. Reading in a retiring context is allowed.
3. The root folds `mids.results(context)` and finishes.

The [ports-nested test](../private_actor_tests/main.d) exercises this three-level
pipeline on five workers. Each parent reads only its direct children's results.
External result channels remain the route for root-to-application output and
application-defined cross-level routing; they require their own ownership and
synchronization contracts.

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
subset results stay in participant order. Members can alternate plain, result
and continuation operations across waves.

`startFinal` allows continuation members to create and launch descendants.
Retirement happens after `done` and their join. If the join is deferred, its
final owner queues an internal cleanup activation, even when that owner is
the emitter seal. Only a consumer destroys stored state, once, post-order,
after writing the final result. The cleanup activation does not call user code.

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

### Child state machine and lost-wake argument

Let `c` count activation custody plus waves started by this member; `H` is its
activation hold on enclosing wave `W`, and `P` is the resume descriptor's
producer hold. Every initial or resumed callback also has a tracked W table
credit. Completion allocates nothing.

```text
IDLE -> RUNNING (c := 1)
RUNNING -- done, c == 1 --> COMPLETE (retire inline if terminal)
RUNNING -- wait / done with tails --> take H if absent; release custody
    -> SUSPENDED(wait) / DRAINING(done or doneRetire)
unique final c decrement:
    wait       -> RETURN_QUEUED
    doneRetire -> RETURN_QUEUED(cleanup)
    done       -> clear flags; release H -> COMPLETE
RETURN_QUEUED -> emitter FIFO (take P) -> W-credited resume table
    -> RUNNING / CLEANUP (no user call)
final resumed done / cleanup -> clear flags; release H -> COMPLETE
FIFO fully published -> clear scheduled flag; release P (last park access)
```

H is taken before custody release, while the current table pins W. Children
finishing first leave custody as the final credit; an activation finishing
first leaves a descendant as final owner. The counter's modification order
selects exactly one decrement from one to zero. Only a running member with
custody can retain credits, so no increment occurs after zero. Every nonfinal
decrement is its holder's last Entry access. The final owner's release push
is its last Entry and Service access. The emitter acquire-detaches the list
and release-publishes a new table, carrying both prior activation state and
joined descendant writes to the resumed consumer.

The immediate-done path leaves dormant `c == 1`; final-owner completion leaves
zero. The next activation resets it exclusively: resumed ownership comes
through the return queue, and a new wave follows the previous W join. The
immediate-done acquire load of one already proved all notification tails gone.

`ResumeBodies` keeps its own cursor. `prepareTableFrontN` detaches only the
accepted prefix and clears the tail on full drain before publication. Farm's
copy advances the independent cursor before visibility; the emitter never
reads an accepted Entry's `next` afterward. Zero admission leaves the FIFO
unchanged and rolls back only the table credit. P covers all post-write park
access and is released last. H keeps suspended members, ancestors and Service
alive. See [the ordering chain](ACTOR_MEMORYORDER.md#private-child-continuation-ordering).

Debug builds enforce empty FIFO head/count/tail equivalence and one result
write per participating slot. Result write flags share the existing allocation
(one extra byte per capacity slot in debug, none in release). Counter overflow
and underflow, wrong contexts, early result reads, and strict root finish with
suspended children are fatal; no detection is promised for a member that never
returns `done`.

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
complete in a different order. Out-of-range indices, duplicates and an
unreserved cohort are fatal in debug and release, including an empty list on
an unreserved cohort. Wrong-parent and default contexts are also fatal.

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
same Wave join as whole waves, with no new atomics. Its idle acquire fences
selection reuse between generations; the last finisher accesses only saved
parent/upstream pointers after publishing idle.

On 64-bit targets the fixed cohort metadata grows by 16 bytes (one selection
pointer and a count, with the subset flag in existing padding): a signal-only
cohort is 280 bytes instead of 264 (376 with the child Service pointer,
continuation descriptors, wave-outcome fields, and cohort root pointer below).
Entry remains 40 bytes. Unreserved cohorts
allocate no per-member subset storage and use the existing whole-wave dispatch
path. Reserved cohorts make one extra eager allocation of 8 bytes per member
of capacity, containing both the selection and its inverse positions. Starting
a subset copies and validates k indices in O(k), without scanning or clearing
the capacity array; emission and result placement use one index indirection.
When every member is needed, use `start` to avoid subset copying and indirection.

This complete example selects members 3 and 1, then reads their squares in
that order after the join. Parent retirement also disposes unselected members.

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

## Failures and outcomes

A domain failure is a value, not `fatal`. `context.fail(code)` records it and
returns; the callback still returns its normal step. It is valid in plain,
result, continuation, and result-continuation operations. Call it at most once
per member per wave. Extra calls are counted and not diagnosed. A failed
result member still writes the `R` it returns; the usual convention is
`R.init` or a partial value. The debug unwritten-slot rule is unchanged.

The enclosing wave aggregates reports. `park.outcome(context)` is the snapshot
after the join: participants, dispatched, the report count, the first
failure's code and participant position, and whether this wave's own scope was
cancelled. Before the first nonempty start every field is zero. `succeeded`
means no reports and a full dispatch. Per-member detail belongs in `R`. Which
concurrent report supplies the representative pair is unspecified; the code
and position always belong to the same report. A whole wave's position is the
spawn index. A subset's position is the selection index, valid until the next
nonempty start.

Nothing fails a parent automatically. After the join the parent reads
`outcome` and chooses: recover, or propagate with its own `context.fail`.
Propagation toward the application is explicit at each level. A root's
`fail` sets its [tracked](#root-tracking-and-deadlines) outcome to `failed`;
the first code wins across all of its activations. It is fatal on an
untracked park, where a root reports through application storage instead.
Wrong or default context is fatal for both `fail` and `outcome`. `outcome` and `results` while a wave is active are fatal.
`uint.max` reports in one wave are fatal before the count wraps.

A rejected start preserves the last nonempty wave. `start`, `startFinal`, and
`startSubset` reset the aggregate only after the active, retiring, and empty
rejections, and before the enqueue. An empty start, a busy start, and a void
operation after a result wave therefore leave the previous snapshot in place
until a new nonempty wave is actually published. `cancelled` and a
`dispatched` prefix shorter than `participants` come from
[cancellation](#cancellation).

Partial construction needs no extra API. If `createChildren` returns null or
`spawn` does not return `spawned`, a member calls `fail`. If no wave of that
actor is active it returns `done`; otherwise it calls `cancelChildren()` and
returns `done`, which still drains the cancelled waves. Never-dispatched
members are destroyed once when the owning actor retires. The fold above uses
that pattern instead of `fatal` on a leaf spawn failure.

## Cancellation

Cancellation is cooperative and advisory. It never interrupts a callback and
never wakes a waiting root or suspended member early. It shortens the
existing joins: the emitter stops publishing a cancelled wave's remaining
tables, and running or resumed callbacks see `context.cancelled` and finish
early. Every wait still ends through the unchanged credit chain.

- `park.cancel(context)` cancels that cohort's active wave and returns true.
  It returns false, with no effect, when it observes the cohort idle. Only the
  park's parent may call it.
- `context.cancelChildren()` cancels the active wave of every cohort the
  actor owns. Idle cohorts are unaffected. Roots and members may call it.
- `context.cancelPeers()` cancels the member's enclosing wave: its siblings
  and, through them, their descendants. It is fatal on a root context.
- `context.cancelled` is true when a wave on the path from this activation to
  its root is cancelled, or when the tracked root itself is (a handle,
  `cancelAll`, or an expired deadline; see
  [root tracking](#root-tracking-and-deadlines)). It may become true at any
  time and never becomes false within an activation. A root context sees only
  root-level cancellation, so on an untracked park a root's `cancelled` is
  false.

What a cancelled wave does:

1. **Undispatched work is skipped at table granularity.** Before each initial
   table the emitter checks the wave's scope. Once it is cancelled, the
   published prefix is final. A non-retiring wave seals at once. Its
   remaining members do not run, and their state, results and children are
   untouched, so they stay ordinary live members for a later wave or for
   disposal. A `startFinal` wave publishes the remaining members in cleanup
   tables. These retire each member on a consumer without calling the
   operation or writing a result, so `startFinal` still retires every member
   exactly once. Subsets never retire.
2. **Resumptions are always delivered.** A suspended continuation is resumed
   into its operation, sees `cancelled`, and should return `done` (possibly
   after `fail`). Skipping it would leave its phase in `T` inconsistent and
   its result unwritten.
3. **Roots are always dispatched.** A root is the outcome authority. To stop
   its tree it calls `cancelChildren()` and returns `finishAfterChildren`, or
   `finish` if nothing is active. `finish` with an active child wave stays
   fatal.
4. **The join is never cut short.** The parent owns the child state, so it must
   join before it may finish or dispose anyway.

`outcome.dispatched` is the prefix that ran the operation, and
`results(context)` returns only that prefix (`min` of the result count and
`dispatched`). The debug unwritten-slot check covers the same prefix. A
retiring wave's prefix survives `count_ = 0`. `outcome.cancelled` means this
wave's own scope was cancelled, by its parent or a peer. It reports that a
cancel was requested during the generation, not that work was skipped. A
cancel that lands just as the wave finishes reports `cancelled` with a full
`dispatched`. A wave skipped because an ancestor was cancelled has
`dispatched < participants` with `cancelled` false. The wave flag is reset by
the next nonempty start. A per-root summary byte is sticky until the root
slot is recycled. While it is clear, a check costs one acquire load. After the
first in-tree cancel, checks in that root's tree walk the ancestor waves.

Recipes:

- Fail-fast (one failure stops the siblings and their descendants):
  `c.fail(code); c.cancelPeers();` then return as usual. The resumed parent
  reads `outcome` and propagates with its own `fail`, `cancelPeers`, or
  `cancelChildren`.
- First-wins search: the finder calls `c.cancelPeers()`. A polling root calls
  `park.cancel(c)` and keeps returning `again` until `finished`.

**Termination is eventual, not bounded by depth.** A cancelled tree drains
under fair pumping and consumption. The drain still includes cleanup tables
for truncated `startFinal` waves (N members at batch limit b take about N/b
pumps), resume tables for members that were already suspended, Farm
backpressure, root collection waiting for a whole physical batch, and any
running callback until it next checks `cancelled` or returns. Shutdown
deadlines must budget for that volume. A callback that ignores `cancelled`, or
a member that never returns `done`, is not interrupted.

## Root tracking and deadlines

Root tracking is one opt-in switch, `PrivateRootOptions.tracking`, passed to
`PrivateRootPark.create(farm, capacity, options)`. It gives each root slot a
record, which enables:

- **Identity.** `spawn(initial, out PrivateRootHandle root, deadline =
  MonoTime.max)` returns a copyable, non-owning handle to that root
  generation. `root.index` is a dense slot index below `capacity`; a recycled
  slot keeps its index under a new generation, so compare handles with `==`.
  `spawn(initial)` on a tracked park still tracks the root, with no deadline,
  and discards the handle.
- **Cancellation from any thread.** `root.requestCancel()` is the one park
  operation another thread may call. It returns `requested`, or
  `alreadyRequested`, `finished` (the root already reached terminal
  collection), or `staleHandle` (its outcome was taken, or a default handle).
  It only sets the root's cancel bit: the root and its descendants observe it
  through `context.cancelled`, and the emitter stops publishing initial tables
  in that tree. Writes made before a successful request are visible to an
  activation that observes it. Join every thread that may call `requestCancel`
  before the park's `destroy`; after that a handle is undefined.
- **`cancelAll()`.** On the emitter, cancels every live, unfinished root and
  returns how many it cancelled. It costs O(capacity).
- **Lazy deadlines.** A deadline other than `MonoTime.max` arms the root. It is
  not a timer: the deadline becomes cancellation, with cause `deadline`, only
  when something observes it. Observers are the root or a descendant reading
  `context.cancelled`, the emitter before an initial child table in that tree,
  and `cancelAll`. A root that never checks and never emits after its deadline
  completes normally. Only armed, not-yet-cancelled roots read the clock.
- **Root `fail`.** `context.fail(code)` on a root makes its outcome `failed`.
- **Outcomes (X1a).** A terminal root keeps its slot, and its place in `live`,
  until `takeOutcome(out PrivateRootOutcome outcome)` pops it in collection
  order and recycles the slot. The outcome names the root's handle (already
  stale) and its `status`. For `cancelled` it also carries the `cause`
  (`requested` or `deadline`); for `failed`, the first `code`.

The status is decided once, when the emitter collects the terminal root:
`failed` if the root called `fail`, else `cancelled` if a cancel was requested
before that collection, else `completed`. A cancel that arrives after the
root's work finished but before collection still makes it `cancelled`; the
application asked for it. A later request returns `finished`.

`takeOutcome` does not collect; call `pump` or `collectCompleted` first. Writes
that the root's activations made to application storage are visible on the
emitter thread once `takeOutcome` returns that root; another thread needs
ordinary synchronization with the emitter. Delivery is pull-based, so the
application may spawn a follow-up root between takes.

Untaken outcomes are the backpressure. A park whose slots all hold outcomes
returns `capacityFull` from `spawn`, and `destroy` with an untaken outcome is
the usual `live != 0` fatal. **A tracked drain must pump and take outcomes**
until `live == 0`; pumping alone never gets there. A cancel-mode shutdown is
`cancelAll()`, then that same loop. The shared recipe for both actor models is
in [LIFECYCLE.md](LIFECYCLE.md#shutdown-recipe); its compiled forms are
[request_deadline.d](../examples/request_deadline.d),
[fail_fast_fold.d](../examples/fail_fast_fold.d), and
[fiber_cancel_at.d](../examples/fiber_cancel_at.d). Tracking-only calls
(`spawn` with a handle, `takeOutcome`, `cancelAll`, root `fail`) are fatal on
an untracked park.

```d
import antfarm;
import actors;
import core.time : MonoTime, seconds;
import std.stdio : writeln;

struct Request { uint id; uint polls; }
// Request 0 finishes, request 2 fails, the rest run until cancelled.
PrivateRootDisposition serve(scope ref PrivateBorrow!Request request,
    scope ref PrivateContext context) nothrow @nogc @system
{
    if (context.cancelled) return PrivateRootDisposition.finish;
    if (request.value.id == 2)
    {
        context.fail(404);
        return PrivateRootDisposition.finish;
    }
    if (request.value.id == 0 && ++request.value.polls == 3)
        return PrivateRootDisposition.finish;
    return PrivateRootDisposition.again;
}

void main()
{
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");
    ConsumerView view;
    if (view.subscribe(farm) < 0) fatal("consumer subscription failed");
    PrivateRootOptions options;
    options.tracking = true;
    auto park = PrivateRootPark!(Request, serve).create(farm, 8, options);
    if (park is null) fatal("private park allocation failed");
    PrivateRootHandle[5] roots;
    foreach (uint id; 0 .. 5)
    {
        auto deadline = id == 3 ? MonoTime.currTime - 1.seconds : MonoTime.max;
        if (park.spawn(Request(id), roots[id], deadline) != PrivateSpawnResult.spawned)
            fatal("private spawn failed");
    }
    // Any thread may do this while the park exists.
    if (roots[1].requestCancel() != PrivateCancelResult.requested) fatal("cancel failed");
    foreach (_; 0 .. 8)
    {
        park.pump(token);
        while (view.consumeNext()) {}
    }
    // Shutdown: cancel the rest, then pump AND take outcomes until live == 0.
    park.cancelAll();
    PrivateRootOutcome[5] outcomes;
    while (park.live != 0)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        PrivateRootOutcome outcome;
        while (park.takeOutcome(outcome))
            foreach (id, root; roots)
                if (root == outcome.root) outcomes[id] = outcome;
    }
    alias S = PrivateOutcomeStatus;
    alias C = PrivateCancelCause;
    if (outcomes[0].status != S.completed
        || outcomes[1].status != S.cancelled || outcomes[1].cause != C.requested
        || outcomes[2].status != S.failed || outcomes[2].code != 404
        || outcomes[3].status != S.cancelled || outcomes[3].cause != C.deadline
        || outcomes[4].status != S.cancelled || outcomes[4].cause != C.requested)
        fatal("unexpected root outcomes");
    writeln("completed, cancelled, failed 404, deadline, cancelled at shutdown");
    park.destroy();
    park = null;
    view.unsubscribe();
    farm.unregisterProducer(token);
    farm.destroy();
}
```

Cost: an untracked park pays one `records` null branch in `spawn`, terminal
collection and each cancel check, and the park is 32 bytes larger. A tracked
park allocates 32 bytes per root slot and pays, per root, four record stores
at `spawn`, one CAS at terminal collection and one store at `takeOutcome`.
Each cancel check and each initial child table in a tracked tree adds one
acquire load of the record, plus a clock read only for an armed root. After a
root is cancelled, those checks return at the record without walking the tree.
`index` is 32-bit, so a tracked park's capacity is at most `uint.max`; more
returns null from `create`. Typed root outputs (X1b) are not provided: put
them in application storage reached from `T` and read them after
`takeOutcome`.

## Progress and outcomes

| Operation | Result / contract |
| --- | --- |
| Root `spawn` | `PrivateSpawnResult.spawned`, `capacityFull`, or `allocationFailed`; moves non-POD state (copies POD bytes) and returns no handle. On a tracked park the root is tracked with no deadline. |
| Root `spawn(initial, out root, deadline)` | Tracked parks only (fatal otherwise). The same results; on `spawned`, `root` names this generation and `deadline` (default `MonoTime.max`, none) arms a lazy deadline. |
| `PrivateRootHandle.requestCancel()` | Any thread, while the park exists: `requested`, `alreadyRequested`, `finished` (already collected terminal), or `staleHandle` (outcome taken, or a default handle). |
| `takeOutcome(out outcome)` | Tracked parks only. Pops one collected terminal root in collection order and recycles its slot; false if none. `live` falls here. The root's application writes are visible on the emitter. |
| `cancelAll()` | Tracked parks only. Cancels every live unfinished root (cause `deadline` if its deadline passed, else `requested`); returns how many. O(capacity). |
| Child `spawn` | The same outcomes, plus `waveActive` or `retiring`. |
| `start` / `startFinal` / `startSubset` | `PrivateStartResult.started`, `empty`, `waveActive`, or `retiring`. Empty starts are already complete and take no credit. |
| `PrivateChildDisposition.done` / `PrivateChildStep!R.done(value)` | Finish this member's participation; outstanding descendants still hold the wave active. The result form writes its slot once. |
| `PrivateChildDisposition.waitChildren` / `PrivateChildStep!R.waitChildren()` | Join this member's descendants, then publish exactly one later activation of the same operation, even if nothing is outstanding. |
| `context.fail(code)` | Member: counts one report on the enclosing wave and, for the first report, records that code and participant position. Does not end the activation. Root: makes the tracked outcome `failed` with the first code; fatal on an untracked park. Wrong or default context is fatal. A count of `uint.max` is fatal. |
| `outcome(context)` | `PrivateWaveOutcome` of the last nonempty wave, acquired after the join; all zero before the first such wave. Fatal while active or with wrong/default authority. Valid until the next nonempty start. |
| `results(context)` | `const(R)[]` in participant order, acquired after the join; only the dispatched prefix of a cancelled wave. Fatal while active or with wrong/default authority. Available only for non-void `R`. |
| `park.cancel(context)` | True if the cohort's active wave was cancelled; false, with no effect, if it was observed idle. Wrong/default authority is fatal. |
| `context.cancelChildren()` / `context.cancelPeers()` | Cancel every active owned cohort / the member's enclosing wave. `cancelPeers` on a root is fatal; default context is fatal for both. |
| `context.cancelled` | Advisory: true once a wave between this activation and its root is cancelled, or the tracked root is (handle, `cancelAll`, observed deadline). Always false on an untracked root. Default context is fatal. |
| `subsetMembers(context)` | `const(uint)[]` in list order for the most recent nonempty subset; empty after a whole wave. Wrong/default authority is fatal. |
| `finished(context)` | True before the first start and after the current wave joins; a root notification tail may still be pending. Wrong-parent or default authority is fatal, including when the cohort is idle. |
| `collectCompleted()` / `poll()` | Acquires completed root tables and wake returns, reclaims terminal roots and appends ready roots; never publishes work or runs consumers. |
| `pump(token)` | Collects completions and emits at most one root table and one initial table and one resume table per pending child wave; returns only the number of root activations published, including internal deferred cleanup. Zero does not mean idle. The emitter must keep pumping to resume waiting roots and children. |
| `live` / `capacity` | Emitter-local root counts; `live` includes waiting roots and terminal roots with outstanding notification tails. It falls only on collection after the join, or, on a tracked park, only at `takeOutcome`. Neither getter synchronizes another thread. |
| `destroy()` | Collects once, then requires no live roots (including untaken outcomes) or pending work. It does not drain or cancel waiting roots. Use `park.destroy(); park = null;`, because `destroy(park)` resolves to D's pointer destruction and does not free the park. |

## Configuring batches and Farm quota

`PrivateRootPark.create(farm, capacity, batchLimit = defaultPrivateBatchLimit,
allocator = ActorAllocator.cRuntime())` fixes the maximum root batch size.
`create(farm, capacity, PrivateRootOptions)` takes the same two settings plus
[`tracking`](#root-tracking-and-deadlines).
The park allocates each batch's entry storage through its allocator at creation,
using `storageLimit = min(batchLimit, capacity)` entries per batch. It reserves
`ceil(capacity / storageLimit)` root table slots, equivalent to
`ceil(capacity / batchLimit)` for every positive capacity. A configured limit
larger than capacity therefore does not reserve unusable entry storage.
Allocation sizes that overflow return null. A zero configured limit is fatal.

`pump(token)` uses that configured limit. `pump(token, limit, avgCost = 0)`
accepts 1 through the configured maximum; zero or an oversized override is
fatal. Choosing a smaller per-call limit does not increase the table pool.
Choose a smaller configured limit when more small tables must be in flight.

`context.createChildren!T(capacity)` inherits the immediate parent's effective
limit: a root's children use the root park's configured `batchLimit`, while a
child's children use that child's cohort limit. An explicit override therefore
propagates down the hierarchy until another override replaces it.
`context.createChildren!T(capacity, childBatchLimit)` supplies a positive
override, with no fixed cap. The child's existing member
array supplies each accepted prefix, so even a large limit does not allocate
extra child dispatch storage. Zero is fatal; overflowing member capacity
returns null. Values above 256 are legal for both roots and children.

The Farm producer tier bounds actual admission. Its default small quota is
4096 ulongs, enough for about 225 one-word private entries in a table with one
shard. A larger batch is split into accepted prefixes and requeued suffixes.
To admit larger tables, configure a larger `quotaSmall`, or configure a bulk
producer (`maxBulk` / `quotaBulk`) and register a `Tier.bulk` token. All quotas
must still fit the Farm's segment and total producer-quota constraints.

Larger batches amortize table overhead, but more roots wait behind one slow
callback because a whole physical table returns together. Smaller batches
limit that delay and reserve more table slots. See the recorded batch/quota/
consumer sweep and default rationale in
[PRIVATE_LIFECYCLE.md](../perftest/PRIVATE_LIFECYCLE.md).

## Ownership contract

This API is `@system`. Private fields and disabled ordinary copying of parks,
borrows, and contexts prevent some mistakes, but do not prove uniqueness of
raw pointers or transitive state. In particular:

- Borrows, contexts, and pointers/references into them must not escape their
  callback. A saved context is invalid even if an entry address is reused.
- Spawning moves non-POD state and copies POD bytes. On a failed root spawn,
  the live argument is destroyed inside the call on the caller's thread with
  the root reentrancy guard active; the moved-from `.init` parameter is
  destroyed on return. Child failure arguments follow normal language rules
  on the calling consumer. An initializer must not leave external mutable aliases to the resulting
  state. No parent, sibling, emitter, or external observer may read or mutate
  private actor state. Library result slots belong to the cohort and survive
  member retirement until cohort disposal. External result channels need their
  own ownership/synchronization; a pointer into child state is not a result.
- Allocator-managed actor state is not automatically scanned or rooted by the
  GC. Use `ActorAllocator.gcRooted` or external rooting for GC references.
  Descendants inherit the root allocator and its context, which must outlive
  the hierarchy.
- Every operation on a root park, including observation and destruction, is
  single-emitter and non-reentrant. Callbacks and allocator hooks must not
  re-enter that park. Debug builds diagnose reentry into operational methods;
  this does not make concurrent calls safe. An emitter may hand over only after
  external quiescence.
- The allocator must support concurrent allocation/free on the threads that
  run callbacks; the allocating thread need not be the freeing thread. The
  same `ActorAllocator` interface supports C runtime and mimalloc policies.
- Destroy a root park only after `live == 0` has been reached through `collectCompleted`
  or `pump`. This includes terminal frees, retirement accounting, and root
  notification tails. `finished(context)` can become true before a notification
  releases its root credit. Even strict-finished roots can therefore retain
  their slot (`spawn` returns `capacityFull`) and `live` count until that tail
  joins. User callback/result counters alone are not a destruction fence. Stop/join
  consumers and release producer tokens before destroying their Farm.

Hierarchy depth has no fixed limit. Completion and disposal recurse, so the
available thread stack bounds usable depth; applications must size it accordingly.

External wake/inbox operations and reparenting are not implemented. Child
continuations retain their enclosing wave until the final activation and all
its descendants join. The emitter must keep pumping; completion does not wake
a sleeping emitter.

## State destruction and results

`stateAllowed!T` rejects classes, interfaces, context-carrying nested
structs and structs containing them, and destructors or moves that cannot be
called from `nothrow @nogc`. Postblits and copy constructors are allowed, but
the library never copies non-POD state. Pass non-copyable values as rvalues or
with `move(value)`. On DMD, non-copyable state with an alignment of 32 bytes
or more cannot be `move`d into a by-value `spawn` argument: because of a
compiler/druntime limit, `core.lifetime.move` tries to copy such structs and
runs the destructor during the move. Public actors retain their existing POD
restriction; `R` also remains POD.

The stored instance is destroyed exactly once on a consumer. Caller-side
parameters, temporaries and moved-from sources are excluded from that promise.
Every successful spawn destroys its moved-from `.init` parameter on the caller;
destructors must tolerate `.init`, including observable actions on that value.

| Retirement path | Consumer sequence | Byte free |
| --- | --- | --- |
| Root `finish` | user callback → child cohorts recursively → `~T` | emitter or consumer policy |
| Root `finishAfterChildren` | joined internal cleanup activation → child cohorts → `~T` | emitter or consumer policy |
| Child `startFinal`, plain or result op | op → result slot write if any → child cohorts → `~T` | consumer |
| Child `startFinal`, continuation op | final `done` result write → descendant join → child cohorts → `~T`; deferred retirement uses a consumer cleanup activation | consumer |
| Parent disposes a cohort | each live member's child cohorts → `~T`, including never-dispatched members | consumer |
| Root park `destroy()` | requires no live state; no implicit retirement | emitter metadata free |

Order across siblings is unspecified. Destructors must not read other actors'
state, block, or call park APIs. Root-park reentry from an owning failed-spawn
argument destructor is fatal in debug; release builds leave reentry unchecked.
The guard does not cover moved-from parameter destruction after return.

Result slots belong to the cohort and outlive members. A result must not refer
to a resource its member's destructor releases. To transfer ownership, clear
the owning field in `T` before returning the pointer as `R`.

## GC references

`ActorAllocator.gcRooted(&inner)` registers each allocation with `GC.addRange`
before returning it and removes the range before freeing, after destruction.
The `inner` allocator object and its context must outlive all allocations and
the hierarchy. It can wrap C runtime or mimalloc allocation and also registers
cohort metadata and result slots. Registration takes the global GC lock per
allocation/free; omit it for state holding only manually managed resources.
Callback threads must be druntime-registered. The threadpool workers already
are; raw native threads must use `thread_attachThis`. The emitter
(root-spawning) thread must also be registered, not only callback threads: a
root `spawn` argument holding GC references sits on its stack while it pumps,
and under the default emitter reclamation it is the thread that removes the
GC ranges when freeing.

## Publication and reclamation

Root entries have no admission/exit CAS. An untracked park has no public
generation or handle; a tracked park's handle names a generation in a
separate per-slot record, which only cancellation and outcome delivery use. The sole emitter consumes entries from a FIFO, snapshots at most its configured batch limit of
entry addresses into a detached batch, and publishes one-word single-shot
Farm payloads. The Farm's primary claims provide exclusive dispatch.

Only the accepted prefix changes custody. The emitter requeues the suffix;
it does not dereference accepted actor entries until their completion returns
custody. With consumer reclamation, callback completion can free the actor
before `writeTracked` returns. Root state and entry metadata
are separate allocations, so accepted-pointer source storage stays valid.
A terminal callback destroys stored state after disposing its descendants.
By default, the table notification returns metadata to the emitter, which
frees the raw bytes without inspecting state.
Allocation and reclamation therefore occur on the same emitter. The optional
third template argument `PrivateRootReclamation.consumer` frees state inside
the terminal callback instead. This is a measured policy choice: simultaneous
remote frees substantially hurt the small-allocation workload on this host.
Child terminal waves still free members on their executing consumers.

The emitter acquires completed batch metadata and appends `again` roots to the
ready FIFO. For a wait or deferred retirement it releases table custody and
recycles the Batch immediately, even while the root sleeps. Other roots can use
that Batch. A slow callback still delays peers in its original physical table.
Joined roots go to the back of the ready FIFO when their wake is collected.
Each detached wake list preserves FIFO order of successful publication; there
is no global completion-order guarantee across wake lists, table returns and
newly spawned roots.

A child cohort has one active wave at a time. The parent's callback sets it
active before enqueueing its metadata. The emitter holds a producer credit
until publication is sealed; every table receives a credit before it is
published, with rollback on zero admission. A descendant wave takes one credit before becoming visible: on its enclosing
wave for a plain/result member, or on a continuation member's own join counter. Consequently callback return
alone cannot release an ancestor's execution authority.

Wave completion joins all table and descendant credits with acquire/release
RMWs. The last finisher snapshots the ancestor and parent pointers, completes
metadata updates, and release-publishes idle as its final cohort access. It then
releases the saved ancestor-wave credit or the parent's root/member join credit.
The saved credit pins the upstream metadata while the cohort can already be
reused or disposed. Notifications never borrow actor state or a producer token.

The Farm snapshots a completion hook's function and context before invoking
it and never dereferences either afterward. A hook may therefore transfer or
free its storage as its final access. Root return publication and final wave
release rely on this exact rule; observing a flag earlier in a callback would
not justify freeing storage that callback still uses.

## Root join state machine and lost-wake argument

Each root owns one scheduling/table-custody credit while ready, running or
awaiting physical-table collection. Each nonempty direct wave retains one
additional credit before enqueueing; descendants aggregate through their
existing enclosing-wave credits:

```text
pending = custody (0 or 1) + outstanding direct-wave notification credits

FREE -> READY (pending = 1) -> RUNNING -> TABLE_HELD
TABLE_HELD -- again --> READY (keep custody)
TABLE_HELD -- waitChildren / finishAfterChildren --> release custody
    remaining child credits -> WAITING
    zero credits            -> RETURN_QUEUED
WAITING -- last child credit --> RETURN_QUEUED
RETURN_QUEUED -- emitter collection --> READY or CLEANUP_READY (pending = 1)
CLEANUP_READY -- internal consumer activation --> TABLE_HELD (finish)
TABLE_HELD (finish) -- collection and any notification tails joined --> FREE
```

The collector releases custody only after acquiring the physical table, as its
last Entry access. Every decrement is acquire/release. Only the decrement from
one to zero publishes the root to the service's MPSC wake list; its successful
release CAS is its final Entry and Service access. The emitter acquire-detaches
and reverses the list, restores custody, and publishes through its normal Farm
path. Strict leaf finish keeps its direct reclamation path without a new RMW.

If children finish first, custody keeps `pending` at one until table collection;
the collector then owns the wake. If collection happens first, the last child
notification owns it. Concurrent releases have exactly one old-value-one owner.
Only a running root with custody can start a direct wave, so no new credit can
appear after zero. Idle flags alone cannot replace these credits: a wave may
publish idle and pause before its notification, including across earlier
`again` generations. The counter pins the root Entry and Service through that
tail even if the root state has already been freed under consumer reclamation.

The common child Entry remains 40 bytes. Root-only disposition, flags and a
checked 32-bit counter occupy the child enclosing-pointer union; static asserts
verify size and offsets under LDC and DMD. Existing 64-bit wave increments retain
their acquire/release fetch-add operations. No waiting-root scan is required.

### Entry layout and Service access (internal)

An Entry's word at offset 16 is a union. A root holds its `Service*` there. A
child member holds continuation metadata there (`childStep`, `childFlags`,
`childReserved`, `childPending`). Its Service pointer lives in its cohort's
`ChildBase.service`, set at creation from the creating context. The cohort's
`parentIsRoot_` records whether that context had no enclosing wave.

Access rule: code with only an Entry must never read `entry.service` directly.
Roots call `rootService(entry, enclosing)` with identity proved by their calling
context; members use `ChildBase.service`. Debug builds reject non-null
`enclosing` and null service words. The guard does not infer identity from the
word's bits: `releaseRoot` and root dispatch supply null because those paths
structurally own roots. A mistaken caller supplying null for a child would
violate that proof. Audited reads are the root branch of
`PrivateContext.service`, `releaseRoot`, root dispatch, and the test accessor;
root creation alone writes the Service word.

A wave selects one immutable credit target at start: enclosing wave, root
`pending`, or the continuation member's `childPending` when `ownJoin_` is set.
The finisher snapshots that target and parent kind before publishing idle.
`Service.childReturns` carries completed child waits. Each cohort embeds two
`Emittable` descriptors: initial emission and resumption. Its resume FIFO,
counter and scheduled flag are emitter-owned and reserved at cohort creation.
This adds 48 bytes per cohort on 64-bit targets. Wave outcomes add 16 bytes
to the wave and two participant counters, and cancellation adds the immutable
`ChildBase.root` pointer, for 376 bytes in a signal-only cohort; there is no
additional per-member continuation, outcome or cancellation allocation. A root
Entry keeps the sticky `subtreeCancel` byte at offset 35, formerly padding;
child Entries are unchanged.
Entry stays 40 bytes and `PrivateContext` stays four words. Plain/result
dispatch adds no RMW, and an empty child-return list costs one raw load per pump.

The existing emitter pump remains mandatory. Consumers hand roots and children
back through their wake lists; they never publish to the Farm and do not wake a
sleeping emitter. An empty wake-list hint can postpone a concurrent arrival to
the next collection. A root with no child work can use `waitChildren` as a yield,
but repeatedly doing so can still create an application busy loop.

## Farm completion prerequisite

Plain callback writes must join within each shard before the shard finisher
joins `Tprogress`. The 1.7.0-rc.5 prerequisite changed the **completion** RMW on `Shc` from
relaxed to acquire/release; claim increments remain relaxed. Acquire/release
on `Tprogress` alone does not acquire other consumers' writes within the same
shard. The two-consumer test forces separate runs in a single shard and has
the final table hook read both consumers' plain writes, with no intervening
test-side release. Reverting only that RMW produces a ThreadSanitizer race;
the corrected implementation passes.

## Validation and measurements

Run `make check` (or the individual private `run`, `run-dmd`, `run-tsan`,
`doc-example`, and `death` targets). Coverage includes, but is not limited to, FIFO
progress, immediate completion/free before publication returns, allocation
failure cleanup, partial-prefix and zero-admission recovery, repeat dispatch,
three-level joins, authority rejection, terminal waves, allocation balance,
and four independent creators sharing a Farm. E1 adds exactly-once waits,
empty/already-joined waves, both custody orders, late strict-finish tails,
root-ready CAS retry, emitter-owned final sealing, and counter-exhaustion death
tests. E2-4 adds continuation trees, both final-owner orders, deferred terminal
cleanup on consumers, resume admission/FIFO reuse, subsets, repeated generations,
and all C-M mutation checks. F2 adds the C1–C13 cancellation schedules, each
with its mutation, and a random fail/cancel tree torture. F3 adds the T1–T9
root-tracking schedules (stale handles across recycling, cancel against the
terminal CAS in both orders, emitter-observed deadlines, clock-read counts,
held slots, the outcome visibility edge under TSan, a 1000-root `cancelAll`
shutdown, handle identity, generation wrap), each with its mutation, and a
tracked torture with deadlines, a foreign canceller and `cancelAll`. [The test comments](../private_actor_tests/main.d)
identify the bug each deterministic schedule catches. It tests ordinary backing with
LDC and DMD; ThreadSanitizer uses LDC.

The Farm suite and public actor torture suite, including its ThreadSanitizer
configuration, also pass. Sustained benchmarking exposed an additional public
wave completion weakness: progress equality was being treated as proof that
all table hooks had stopped accessing the descriptor. Public waves now also
hold a producer credit and release each table credit as its final access.
Deterministic tests reproduce the old failure and verify descriptor reuse;
see [ACTOR_MEMORYORDER.md](ACTOR_MEMORYORDER.md). See
[PRIVATE_LIFECYCLE.md](../perftest/PRIVATE_LIFECYCLE.md) for measured throughput,
thread counts, workload differences, and reproduction commands. These results
are full lifecycle tests, not a general actor-system performance claim.
