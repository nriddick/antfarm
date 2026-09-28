# Private actors: experimental roots and parent-owned cohorts

`actors.private_actor` implements a separate ownership model for actors that
have no public identity. A root has no external application owner. A child
has exactly one private parent, which can authorize whole-cohort waves but
cannot inspect the child's state. Children can create children of their own.

A single emitter owns each root park and its Farm producer token. Multiple
parks and emitters can share a Farm. They do not share an actor free-list gate,
ready-drain gate, or live counter. Each park also emits its parents' requested
child waves. The emitter inspects scheduling metadata only.

This is a prototype for POD state and synchronous callbacks. The existing
`ActorOwner`, `ActorHandle`, inbox, and public `ActorWave` APIs remain available.
It does not change DRuntime Fibers.

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

This first slice permits application-defined result channels; it does not
provide a typed signal-port API. Nonpolling conductor resumption after a join
and resumable child continuations are also future work. Roots currently return
`again` to observe completion on a later dispatch. Those extensions must
preserve the existing hierarchy's exclusive execution authority.

## API

```d
import actors;

struct Work { ulong remaining; }

PrivateDisposition step(scope ref PrivateBorrow!Work actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    // The only application access to this actor's state is here.
    return --actor.value.remaining == 0
        ? PrivateDisposition.finish : PrivateDisposition.again;
}

// Given an existing Farm and single-emitter producer token:
auto park = PrivateRootPark!(Work, step).create(farm, 4096, allocator);
assert(park !is null);
assert(park.spawn(Work(4))); // transfers initializer; returns no actor handle
while (park.live != 0)
{
    park.pump(token);       // one root table + one table per pending child wave
    // A same-thread consumer must also call consumeNext() here.
}
park.destroy();
```

Only a private callback can call `context.createChildren!T(capacity)`. It stores
the resulting opaque `PrivateChildPark!T*` in its own state. The parent may:

- `children.spawn(context, initializer)` while the cohort is idle;
- `children.start!operation(context)` to authorize a whole-cohort wave;
- `children.finished(context)` to acquire its completion;
- `children.start!operation(context, true)` to execute a terminal wave and
  free every member after its final callback.

An operation accepts `scope ref PrivateBorrow!T` and `scope ref PrivateContext`
and returns `void`. It may launch descendant waves and return immediately.
Its enclosing wave remains active until those descendants finish. Parent
callbacks must not block waiting for child work; roots can return `again` and
observe completion on a later dispatch. Finishing a root with an active child
wave is a fatal contract error. Terminal child callbacks cannot launch further
waves or create children. Retirement also disposes idle descendant parks.

The nested example in [the tests](../private_actor_tests/main.d) runs three
waves through root → child → grandchild trees, followed by child retirement.
The [lifecycle harness](../perftest/actor_churn.d) demonstrates parent-created
cohorts with terminal child waves.

## Ownership contract

This API is `@system`. Private fields and disabled ordinary copying of parks,
borrows, and contexts prevent some mistakes, but do not prove uniqueness of
raw pointers or transitive state. In particular:

- Borrows, contexts, and pointers/references into them must not escape their
  callback. A saved context is invalid even if an entry address is reused.
- An initializer must not leave external mutable aliases to the resulting
  state. No parent, sibling, emitter, or external observer may read or mutate
  private actor state. Result channels need their own ownership/synchronization;
  a pointer back into a child's state is not a result channel.
- Every operation on a root park, including observation and destruction, is
  single-emitter and non-reentrant. Callbacks and allocator hooks must not
  re-enter that park. An emitter may hand over only after external quiescence.
- The allocator must support concurrent allocation/free on the threads that
  run callbacks; the allocating thread need not be the freeing thread. The
  same `ActorAllocator` interface supports C runtime and mimalloc policies.
- Destroy a root park only after `live == 0` has been acquired through `poll`
  or `pump`. This includes terminal frees and retirement accounting. Stop/join
  consumers and release producer tokens before destroying their Farm.

General destructors, arbitrary subset waves, external wake/inbox operations,
reparenting, and resumable child continuations are not implemented. Children
may launch nested work, but cannot resume themselves later in the same wave.
A later continuation design must retain its ancestor's lease until its last
continuation and descendant finish.

## Publication and reclamation

Root entries have no admission/exit CAS, public generation, or stable public
handle. The sole emitter consumes entries from a FIFO, snapshots at most 256
entry addresses into a detached batch, and publishes one-word single-shot
Farm payloads. The Farm's primary claims provide exclusive dispatch.

Only the accepted prefix changes custody. The emitter requeues the suffix;
it does not dereference accepted actor entries until their completion returns
custody. With consumer reclamation, callback completion can free the actor
before `writeTracked` returns. Root state and entry metadata
are separate allocations, so accepted-pointer source storage stays valid.
By default, a terminal callback marks its metadata and the table notification
returns it to the emitter, which frees the POD state without inspecting it.
Allocation and reclamation therefore occur on the same emitter. The optional
third template argument `PrivateRootReclamation.consumer` frees state inside
the terminal callback instead. This is a measured policy choice: simultaneous
remote frees substantially hurt the small-allocation workload on this host.
Child terminal waves still free members on their executing consumers.

The emitter acquires completed batch metadata, recycles terminal entries, and
appends continuing roots to the FIFO. Returns wait for their physical table,
not the entire park. A slow callback can therefore delay other roots in its table.

A child cohort has one active wave at a time. The parent's callback sets it
active before enqueueing its metadata. The emitter holds a producer credit
until publication is sealed; every table receives a credit before it is
published, with rollback on zero admission. A descendant wave takes a credit
on its enclosing wave before becoming visible. Consequently callback return
alone cannot release an ancestor's execution authority.

Wave completion joins all table and descendant credits with acquire/release
RMWs. The last finisher snapshots the ancestor pointer, completes metadata
updates, and release-publishes idle as its final access to the cohort. It then
releases the saved ancestor credit. This permits immediate parent reclamation
without a trailing access to reclaimed cohort storage.

The Farm snapshots a completion hook's function and context before invoking
it and never dereferences either afterward. A hook may therefore transfer or
free its storage as its final access. Root return publication and final wave
release rely on this exact rule; observing a flag earlier in a callback would
not justify freeing storage that callback still uses.

## Farm completion prerequisite

Plain callback writes must join within each shard before the shard finisher
joins `Tprogress`. This prototype changes the **completion** RMW on `Shc` from
relaxed to acquire/release; claim increments remain relaxed. Acquire/release
on `Tprogress` alone does not acquire other consumers' writes within the same
shard. The two-consumer test forces separate runs in a single shard and has
the final table hook read both consumers' plain writes, with no intervening
test-side release. Reverting only that RMW produces a ThreadSanitizer race;
the corrected implementation passes.

## Validation and measurements

Run `make -C private_actor_tests run run-dmd run-tsan`. The suite covers FIFO
progress, immediate completion/free before publication returns, allocation
failure cleanup, partial-prefix and zero-admission recovery, repeat dispatch,
three-level joins, authority rejection, terminal waves, allocation balance,
and four independent creators sharing a Farm. It tests ordinary backing with
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
