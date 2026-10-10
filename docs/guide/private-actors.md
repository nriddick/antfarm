# Private actors

`actors.private_actor` implements a separate ownership model for actors that
have no public identity. A **root** has no external application owner. A
**child** has exactly one private parent, which can authorize whole-cohort or
subset waves but cannot inspect the child's state. Children can create children
of their own.

A single **emitter** owns each root park and its Farm producer token. Multiple
parks and emitters can share a Farm; they do not share an actor free-list gate,
ready-drain gate, or live counter. Each park also emits its parents' requested
child waves. The emitter inspects scheduling metadata only.

This API supports structs and scalars satisfying `stateAllowed!T` and
synchronous callbacks. The existing `ActorOwner`, `ActorHandle`, inbox, and
public `ActorWave` APIs remain available. Farm setup, tokens, pumping,
allocators, storage lifetime, shutdown, and the ranked misuse hazards shared
with public actors are in
[../reference/actor-lifecycle.md](../reference/actor-lifecycle.md). The shared
public/private choice table is in [choosing-work.md](choosing-work.md).

## A root

Only a private callback can call `context.createChildren!(T, R)(capacity)`. It
stores the resulting opaque `PrivateChildPark!(T, R)*` in its own state. `R` is
a POD result type; its default, `void`, creates a signal-only cohort with no
result storage. `PrivateChildPark!T` is the same type as
`PrivateChildPark!(T, void)`.

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

`spawn` moves non-POD state (copies POD bytes) into the park. `park.pump(token)`
collects completions and emits at most one root table, plus one initial and one
resume table per pending child wave; it returns only the number of root
activations published, including internal deferred cleanup. It does not
consume and it does not drain: keep pumping and consuming while roots sit in
`waitChildren`. `park.live == 0` means the emitter has collected every root,
including waiters and notification tails, and on a tracked park that every
outcome was taken. `park.destroy()` requires that; use
`park.destroy(); park = null;` because `destroy(park)` resolves to D's pointer
destruction and does not free the park.

The nested test in [../../private_actor_tests/main.d](../../private_actor_tests/main.d)
runs three waves through root → child → grandchild trees. The lifecycle
harness [../../perftest/actor_churn.d](../../perftest/actor_churn.d)
demonstrates parent-created cohorts with terminal child waves.

## Conductors, outputs, and general actors

A private parent is a conductor inside an exclusive hierarchy. Its own state
remains visible only during its dispatches, and it cannot inspect child state
even after a wave joins. Communication uses separately owned results or
signals. To report outward, a callback may copy a result into a result channel,
transfer ownership of an independent result buffer, or publish a completion
signal. Observers receive that output without acquiring a parent-state borrow
or a capability to schedule the parent. Result storage and notification
contexts must outlive their receivers' use; a pointer into retiring actor state
is not an output channel.

Application-defined result channels are supported. A root can return
`waitChildren` to resume once all its child work joins, without polling in
repeated callbacks. Child cohorts can carry typed per-member results; typed
root-to-application outputs (X1b) and library reductions remain future work;
tracked parks report each root's terminal status through `takeOutcome`.
Continuation operations can suspend and resume within the same child wave.

General actors also hide their state, but their extra machinery supports
outsiders holding stable submission capabilities, waking the same actor, and
racing retirement or replacement. That model fits independently addressed
services, connections with several event sources, or actors selected by
external phase orchestrators. A general endpoint can serve a private hierarchy
without giving external callers identities for its internal actors. See
[public-actors.md](public-actors.md).

## The cohort API

Only the private callback's `PrivateContext` may create and drive cohorts. A
parent owns one `PrivateChildPark!(T, R)*` per cohort and may:

- `children.spawn(context, initializer)` while the cohort is idle;
- `children.start!operation(context)` to authorize a whole-cohort wave;
- `children.startSubset!operation(context, members)` to authorize only the
  listed members, after reserving subset storage at creation;
- `children.subsetMembers(context)` to read that selection in list order;
- `children.finished(context)` to acquire completion;
- `children.results(context)` to acquire a `const(R)[]` of joined results, when
  `R` is not `void`;
- `children.startFinal!operation(context)` to run a terminal wave and retire
  every member after its final callback.

The three wave starts take an optional trailing `avgCost` work-cost class for
the wave's tables. The default claims 32 entries per run;
`startFinal!operation(context, avgCostForChunk(128))` opts into larger claims
for cheap callbacks. A class above `MAX_AVG_COST` is fatal at the start call.
See [sizing-and-allocation.md](sizing-and-allocation.md#work-cost-hints).

A third template argument selects member state storage:
`PrivateChildPark!(T, R, PrivateStateStorage.cohortSlab)` reserves every
member's state at creation instead of allocating it at spawn; see
[sizing-and-allocation.md](sizing-and-allocation.md#cohort-state-storage).

An operation accepts `scope ref PrivateBorrow!T` and `scope ref PrivateContext`
and returns `void`, the cohort's result type `R`, `PrivateChildDisposition`, or
`PrivateChildStep!R`. The last two opt into resumable activations. The runtime
stores each return value in participant order: spawn order for whole waves,
list order for subsets. An operation may launch descendant waves and return
immediately; its enclosing wave stays active until those descendants finish.
Parent callbacks must not block waiting for child work, and every operation
must be `nothrow @nogc @system`.

A root returns one of:

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

The operation-result table, result lifetime, subsets, and resumable
continuations are in
[private-results-and-waits.md](private-results-and-waits.md).

## Ownership contract

This API is `@system`. Private fields and disabled ordinary copying of parks,
borrows, and contexts prevent some mistakes but do not prove uniqueness of raw
pointers or transitive state. In particular:

- Borrows, contexts, and pointers/references into them must not escape their
  callback. A saved context is invalid even if an entry address is reused.
- Spawning moves non-POD state and copies POD bytes. On a failed root spawn, the
  live argument is destroyed inside the call on the caller's thread with the
  root reentrancy guard active; the moved-from `.init` parameter is destroyed on
  return. Child failure arguments follow normal language rules on the calling
  consumer. An initializer must not leave external mutable aliases to the
  resulting state. No parent, sibling, emitter, or external observer may read
  or mutate private actor state. Library result slots belong to the cohort and
  survive member retirement until cohort disposal. External result channels
  need their own ownership/synchronization; a pointer into child state is not a
  result.
- Allocator-managed actor state is not automatically scanned or rooted by the
  GC. Use `ActorAllocator.gcRooted` or external rooting for GC references.
  Descendants inherit the root allocator and its context, which must outlive
  the hierarchy. See [sizing-and-allocation.md](sizing-and-allocation.md).
- Every operation on a root park, including observation and destruction, is
  single-emitter and non-reentrant. Callbacks and allocator hooks must not
  re-enter that park. Debug builds diagnose reentry into operational methods;
  this does not make concurrent calls safe. An emitter may hand over only after
  external quiescence.
- The allocator must support concurrent allocation/free on the threads that run
  callbacks; the allocating thread need not be the freeing thread.
- Destroy a root park only after `live == 0` has been reached through
  `collectCompleted` or `pump`. This includes terminal frees, retirement
  accounting, and root notification tails. `finished(context)` can become true
  before a notification releases its root credit, so even strict-finished roots
  can retain their slot and `live` count until that tail joins. User
  callback/result counters alone are not a destruction fence.

Hierarchy depth has no fixed limit. Completion and disposal recurse, so the
available thread stack bounds usable depth; applications must size it
accordingly. External wake/inbox operations and reparenting are not
implemented. Child continuations retain their enclosing wave until the final
activation and all its descendants join. The emitter must keep pumping;
completion does not wake a sleeping emitter.

The stored instance is destroyed exactly once on a consumer. Caller-side
parameters, temporaries, and moved-from sources are excluded from that promise.
Every successful spawn destroys its moved-from `.init` parameter on the caller;
destructors must tolerate `.init`, including observable actions on that value.
The full destruction and result-storage table is in
[../design/private-actor-layout.md](../design/private-actor-layout.md).

## Where to go next

- Results, subsets, continuations and folds:
  [private-results-and-waits.md](private-results-and-waits.md).
- Failures, tracked roots, deadlines and cancellation:
  [outcomes-and-cancellation.md](outcomes-and-cancellation.md).
- Quotas, batches, work-cost hints, allocators and slabs:
  [sizing-and-allocation.md](sizing-and-allocation.md).
- Why the join and reclamation chains hold:
  [../design/private-actor-ordering.md](../design/private-actor-ordering.md)
  and [../design/private-actor-layout.md](../design/private-actor-layout.md).
- Runnable programs: [../../examples/README.md](../../examples/README.md).
