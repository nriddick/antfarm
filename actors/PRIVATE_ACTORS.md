# Private actors: roots and parent-owned cohorts

`actors.private_actor` implements a separate ownership model for actors that
have no public identity. A root has no external application owner. A child
has exactly one private parent, which can authorize whole-cohort waves but
cannot inspect the child's state. Children can create children of their own.

A single emitter owns each root park and its Farm producer token. Multiple
parks and emitters can share a Farm. They do not share an actor free-list gate,
ready-drain gate, or live counter. Each park also emits its parents' requested
child waves. The emitter inspects scheduling metadata only.

This API supports POD state and synchronous callbacks. The existing
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

Application-defined result channels are supported. A root can return
`waitChildren` to resume once all its child work joins, without polling in
repeated callbacks. Typed signal ports and resumable child continuations remain
future work; children still run one synchronous `void` operation per wave.

## API

The complete programs below are compiled and run by `make check`, with LDC
and DMD in debug and release. Creation and dispatch happen outside assertions.

```d
import antfarm;
import actors;
import std.stdio : writeln;

struct Work { ulong remaining; }
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
    auto spawned = park.spawn(Work(4));
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

Only a private callback can call `context.createChildren!T(capacity)`. It stores
the resulting opaque `PrivateChildPark!T*` in its own state. The parent may:

- `children.spawn(context, initializer)` while the cohort is idle;
- `children.start!operation(context)` to authorize a whole-cohort wave;
- `children.finished(context)` to acquire its completion;
- `children.startFinal!operation(context)` to execute a terminal wave and
  free every member after its final callback.

An operation accepts `scope ref PrivateBorrow!T` and `scope ref PrivateContext`
and returns `void`. It may launch descendant waves and return immediately.
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
access to child state. Terminal child callbacks cannot launch further waves or
create children. Retirement also disposes idle descendant parks.

The nested example in [the tests](../private_actor_tests/main.d) runs three
waves through root → child → grandchild trees, followed by child retirement.
The [lifecycle harness](../perftest/actor_churn.d) demonstrates parent-created
cohorts with terminal child waves.

## A conductor with owned results

Each child writes a distinct slot in application-owned storage. The parent
reads those slots in its single resumption after `waitChildren`. The application
keeps both the slots and total alive until the root is collected. The parent
never reads child state. Returning the wait disposition lets consumers run the
children; no completion-polling callback or extra `finished(context)` is needed.

```d
import antfarm;
import actors;
import std.stdio : writeln;

struct Item { ulong input; ulong* output; }
void square(scope ref PrivateBorrow!Item actor, scope ref PrivateContext context)
    nothrow @nogc @system
{
    *actor.value.output = actor.value.input * actor.value.input;
}
struct Conductor
{
    PrivateChildPark!Item* children;
    ulong* outputs;
    ulong* total;
}
PrivateRootDisposition conduct(scope ref PrivateBorrow!Conductor actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    if (actor.value.children is null)
    {
        actor.value.children = context.createChildren!Item(4);
        if (actor.value.children is null) fatal("child park allocation failed");
        foreach (i; 0 .. 4)
            if (actor.value.children.spawn(context,
                    Item(i + 1, actor.value.outputs + i)) != PrivateSpawnResult.spawned)
                fatal("child spawn failed");
        if (actor.value.children.startFinal!square(context) != PrivateStartResult.started)
            fatal("child wave failed");
        return PrivateRootDisposition.waitChildren;
    }
    foreach (i; 0 .. 4) *actor.value.total += actor.value.outputs[i];
    return PrivateRootDisposition.finish;
}
void main()
{
    ulong[4] outputs; // Externally owned: each child writes one distinct slot.
    ulong total;
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");
    ConsumerView view;
    if (view.subscribe(farm) < 0) fatal("consumer subscription failed");
    auto park = PrivateRootPark!(Conductor, conduct).create(farm, 1);
    if (park is null) fatal("private park allocation failed");
    auto spawned = park.spawn(Conductor(null, outputs.ptr, &total));
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

## Progress and outcomes

| Operation | Result / contract |
| --- | --- |
| Root `spawn` | `PrivateSpawnResult.spawned`, `capacityFull`, or `allocationFailed`; copies POD bytes and returns no handle. |
| Child `spawn` | The same outcomes, plus `waveActive` or `retiring`. |
| `start` / `startFinal` | `PrivateStartResult.started`, `empty`, `waveActive`, or `retiring`. Empty starts are already complete and take no credit. |
| `finished(context)` | True before the first start and after the current wave joins; a root notification tail may still be pending. Wrong-parent or default authority is fatal, including when the cohort is idle. |
| `collectCompleted()` / `poll()` | Acquires completed root tables and wake returns, reclaims terminal roots and appends ready roots; never publishes work or runs consumers. |
| `pump(token)` | Collects completions and emits at most one root table and one table per pending child wave; returns only the number of root activations published, including internal deferred cleanup. Zero does not mean idle. The emitter must keep pumping to resume waiting roots. |
| `live` / `capacity` | Emitter-local root counts; `live` includes waiting roots and terminal roots with outstanding notification tails. It falls only on collection after the join. Neither getter synchronizes another thread. |
| `destroy()` | Collects once, then requires no live roots or pending work. It does not drain or cancel waiting roots. Use `park.destroy(); park = null;`, because `destroy(park)` resolves to D's pointer destruction and does not free the park. |

## Configuring batches and Farm quota

`PrivateRootPark.create(farm, capacity, batchLimit = defaultPrivateBatchLimit,
allocator = ActorAllocator.cRuntime())` fixes the maximum root batch size.
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
- Spawning copies POD bytes; it is not a language-level move. A failed spawn
  transfers nothing. An initializer must not leave external mutable aliases to the resulting
  state. No parent, sibling, emitter, or external observer may read or mutate
  private actor state. Result channels need their own ownership/synchronization;
  a pointer back into a child's state is not a result channel.
- Allocator-managed actor state is not automatically scanned or rooted by the
  GC. Keep any referenced GC allocations rooted externally for their required
  lifetime. Descendants inherit the root allocator and its context, which must
  outlive the hierarchy.
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

General destructors, arbitrary subset waves, external wake/inbox operations,
reparenting, and resumable child continuations are not implemented. Children
may launch nested work, but cannot resume themselves later in the same wave.
A later continuation design must retain its ancestor's lease until its last
continuation and descendant finish.

## Publication and reclamation

Root entries have no admission/exit CAS, public generation, or stable public
handle. The sole emitter consumes entries from a FIFO, snapshots at most its configured batch limit of
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
published, with rollback on zero admission. A descendant wave takes a credit
on its enclosing wave before becoming visible. Consequently callback return
alone cannot release an ancestor's execution authority.

Wave completion joins all table and descendant credits with acquire/release
RMWs. The last finisher snapshots the ancestor and parent pointers, completes
metadata updates, and release-publishes idle as its final cohort access. It then
releases the saved ancestor-wave credit or, for a direct wave, its root credit.
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

The existing emitter pump remains mandatory. Child consumers only hand roots
back through the wake list; they never publish to the Farm and do not wake a
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
tests. [The test comments](../private_actor_tests/main.d)
identifies the bug each deterministic schedule catches. It tests ordinary backing with
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
