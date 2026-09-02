# Actor payload roadmap

## Objective

Add an explicit actor payload facility which lets one Farm activation borrow
and mutate state outside the ring. The facility must preserve the existing
read-only `PayloadBody`, the ordinary template shim's rejection of unshared
mutable aliases, and Ant Farm's fixed-memory hot path.

Non-GC allocation is the baseline. Actor creation, storage, retirement, and
reclamation must work with a caller-supplied `nothrow @nogc` allocator and with
the D garbage collector disabled. A GC-scanned storage policy may be added as
an opt-in adapter; it is not part of the core ownership model.

The actor package owns activation and per-actor lifetime mechanics. Engine
concepts such as module generations, worlds, regions, systems, assets, and hot
reload remain above it. The package supplies retirement tickets/fences which
an engine can aggregate before unloading code or reclaiming an arena.

## Current actor and wave spike

As of 2026-08-31, the actor code is deliberately split into
`antfarm_actors/actor.d` and `antfarm_actors/wave.d`. The former contains the
autonomous actor runtime and inbox; the latter contains phase-oriented bulk
dispatch:

- a fixed-capacity runtime and stable cache-line slots allocated entirely
  through `ActorAllocator`;
- POD state copied into a separate allocator-owned allocation;
- typed handles, transfer-only owners, and callback-local `scope ref` borrows
  and context;
- a resident-facing, non-template `ActorErasedAdapter` with erased handles,
  transfer-only owners, and callback-local borrows for module registries;
- an intrusive allocation-free ready queue and fixed two-word Farm bodies;
- coalesced wake, republish, external/self retirement, explicit reclamation,
  generation reuse, and stale-handle rejection;
- an allocator-neutral intrusive MPSC inbox, per-generation submission gate,
  bounded actor-private draining, explicit node completion, and close-before-
  drain retirement;
- a four-consumer test with a counted allocator, GC-disabled warm loop, exact
  mutation/serial-entry checks, concurrent inbox producers, and deterministic
  post-close rejection; and
- a separate actor torture suite with forced send/close interleavings,
  multi-actor generation churn, and an opt-in mimalloc v3 adapter contract;
- actor-only waves composed from one or more physical Farm tables, where each
  table's `Tprogress == Tlen` transition advances `Wprogress`; and
- reusable generation-tagged wave handles, sealed completion, phase-specific
  actor operations, structural duplicate/overlap rejection, and dependent-wave
  visibility tests.

This is evaluation code, not a stable public contract. Adopted/arena-owned
state, non-POD destruction, retirement groups, custom inbox adapters, real
mimalloc benchmarking, and engine wake integration remain open below.

## Invariants

### Ring and shim boundary

- The ring body remains `const(ulong)[]` and contains identity words only.
- A permanent host-owned trampoline is the only callback stored in actor
  payload headers. Reloadable module callbacks never appear directly in the
  ring.
- Every actor payload is single-shot and serial: `MaxCs = 1`, `Done = 1`.
- `antfarm_templates` keeps its current no-unshared-aliasing rule. The actor
  API is a separate, explicit escape hatch rather than a new ordinary packed
  parameter category.

### Identity and exclusive borrow

- An `ActorHandle!T` names a stable control slot and a generation encoded with
  the lifecycle state in one atomic word (56 generation bits in the A1 layout);
  it never points directly at reclaimable actor state and cannot dereference
  `T`.
- Only a successful `scheduled -> running` transition can construct an
  `ActorBorrow!T`. The borrow is valid only for the dynamic extent of the
  actor callback.
- The application handler must declare both `ActorBorrow!T` and `ActorContext`
  as `scope ref`. This makes callback-local capability intent part of the
  handler function type, so an unqualified handler is rejected at actor
  creation.
- This remains an `@system` API. The qualifiers do not prevent a handler from
  using casts or raw pointers to escape `ActorBorrow.value`, and they are not
  a substitute for the lifecycle state machine or a retirement fence.
- At most one activation for a generation is queued, published, or running.
  Wakes coalesce through a pending bit rather than publishing duplicates.
- Different actors remain independently runnable and may execute in parallel.
- A wave reserves each actor with an unqueued `IDLE -> SCHEDULED` transition
  before publishing its table. Intrusive membership and a submission-gate pin
  remain until aggregate wave completion; duplicate actors and overlapping
  unfinished waves are therefore rejected before publication.
- Wave operations receive only `scope ref ActorBorrow!T`. They do not receive
  `ActorContext`; republish, inbox draining, and self-retirement remain the
  autonomous actor mode.

### Lifetime and allocation

- Stable control slots are retained until their `ActorRuntime` is destroyed.
  Actor state is a separate allocation with a shorter, explicit lifetime.
- Allocation and deallocation are supplied through an `@nogc` policy. The
  baseline adapter uses the C runtime; pools, slabs, high-performance malloc
  libraries, and arenas are first-class alternatives.
- Actor state is not conservatively scanned. References to GC allocations
  require an explicit higher-level retention/scanning policy.
- An `ActorOwner!T` is the move-only authority for retirement and reclamation.
  Copied handles are non-owning capabilities and become stale after reuse.
- State cannot be destroyed until admission is closed and every queued,
  published, running, and submitting use of that generation has retired.
- Warm wake, queue, flush, callback, and republish paths allocate nothing.

### Ordering

The complete A1/A2 charts and handoff-edge table are in
[ACTOR_MEMORYORDER.md](ACTOR_MEMORYORDER.md).

The intended handoff is:

```text
state initialization
  -> release idle-to-scheduled / ready-queue publication
  -> Farm sentinel publication
  -> consumer acquire and scheduled-to-running
  -> exclusive mutation
  -> release running-to-idle or running-to-scheduled
```

`ActorContext.republish()` records intent only. The trampoline performs the
transition and queue publication after the application callback, and therefore
its exclusive borrow, has ended.

A wake racing callback completion participates in the same atomic
phase/pending word. It either sets pending before the callback chooses its
exit state or observes idle and elects the next publisher. There is no separate
check-then-sleep window.

For waves, `Wprogress == Wlen` is authoritative only after the orchestrator
seals publication. A table may finish before `ActorWave.publish` returns, so
`Wlen` is incremented before Farm publication and rolled back if backpressure
accepts no table. The sealer and last table finisher race through one CAS to
publish exactly one finished state. An acquire observation of that state joins
the actor writes from every table in the wave.

## API direction

The concrete spelling is experimental, but the responsibilities should remain
separate:

```d
auto runtime = ActorRuntime.create(farm, capacity, allocator);
auto owner = runtime.createActor!(State, runActor)(constructorArgs);
ActorHandle!State handle = owner.handle;

handle.wake();                    // queued/coalesced/wave-owned/stale/closed
handle.send(&interaction.node);   // transfers caller-owned node if accepted
runtime.flush(token, maximum);    // fixed-width Farm publication

owner.requestRetire();
if (owner.retired)
    owner.reclaim();              // POD storage allocator release
```

The application callback has this exact shape:

```d
void runActor(scope ref ActorBorrow!State actor,
        scope ref ActorContext context) nothrow @nogc @system;
```

It may mutate `T`, request another activation, or request retirement. The
`scope ref` qualifiers make the dynamic-extent contract visible and reject
handlers that omit it, but they do not prove that an `@system` handler cannot
stash a pointer or reference. A handler cannot suspend; suspendable work
remains a Fiber responsibility.

A wave supplies a different operation for each phase while reusing the same
typed actor handles:

```d
void movement(scope ref ActorBorrow!State actor) nothrow @nogc @system
{
    actor.value.integrate();
}

ActorWave wave;
wave.begin(farm);
size_t offset;
while (offset < handles.length)
{
    immutable written = wave.publish!movement(handles[offset .. $], token);
    if (written == 0)
    {
        if (wave.handle.failed) break; // stale, duplicate, active, wrong Farm
        continue;                      // Farm backpressure
    }
    offset += written;
}
ActorWaveHandle completion = wave.seal();
while (!completion.finished) {}        // or park an orchestrating Fiber
```

Each successful `publish` call creates one physical table and may accept only
a prefix. The operation and any module code it reaches must remain resident
until the completion handle finishes. A dependent phase may begin after an
acquire observation of `finished`; actors within the same wave remain
parallel and must not rely on one another's same-wave writes.

### Resident type-erased adapter

A resident engine can pass `runtime.erasedAdapter` across its reloadable-module
boundary. The adapter carries opaque runtime context and a resident creation
function pointer. Creation therefore uses a non-template boundary and returns
ownership whose layout does not depend on the module's `State`:

```d
void moduleActor(scope ref ActorErasedBorrow actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    ref State state = actor.value!State();
    // Mutate state and process interactions.
}

State initial;
ActorErasedAdapter adapter = runtime.erasedAdapter;
ActorErasedOwner owner = adapter.createActor(&initial,
    State.sizeof, State.alignof, &moduleActor);
ActorErasedHandle handle = owner.handle;
```

`ActorErasedBorrow.value!State()` checks the recorded size and alignment before
returning the callback-local reference. The erased creation call remains an
`@system` POD byte-copy contract: it cannot inspect a type that has already
been erased, run a destructor, or make GC references safe in unscanned memory.
The adapter is non-owning and must not outlive its runtime.

The dispatch pointer can still name reloadable module code. Type erasure makes
the resident registry and creation/reclamation path independent of `State`; it
does not itself authorize unloading that code. The engine must keep the module
generation resident until its erased owners have retired and been reclaimed.
A typed owner can transfer into the same registry representation with
`intoErased()`, while `ActorHandle!T.erased` makes a non-owning erased copy.

The built-in inbox is an accumulator-style interaction protocol. Any producer
may initialize freshly allocated storage with `ActorInboxNode.initialize` and
submit it; the actor is the only consumer and processes nodes while it
exclusively owns its state. A wake is only a coalescing notification, not one
unit of work. The node and payload remain caller-allocated, and a successful
send owns the node until the actor calls `complete` or consumes its storage.
This makes malloc, slab, pool, and arena storage equally usable without adding
a GC or allocation policy to the actor runtime.

## First third-party allocator candidate: mimalloc v3

`antfarm_actors/mimalloc.d` is a deliberately thin, optional mimalloc v3
adapter. It calls the public C ABI
[`mi_malloc_aligned(size, alignment)`](https://microsoft.github.io/mimalloc/group__aligned.html)
and
[`mi_free_size_aligned(pointer, size, alignment)`](https://microsoft.github.io/mimalloc/group__posix.html).
The latter is a particularly good match for `ActorAllocator`: actor allocation
already preserves the exact requested byte count and alignment through
reclamation, so the adapter can use v3's sized-free path without adding
allocation headers or allocator-specific data to actor slots.

The adapter does not replace process `malloc`, own mimalloc heap objects, or
change the default allocator. Compile with `AntfarmMimallocV3` and link
mimalloc, or select Dub's opt-in `mimalloc-v3` configuration:

```d
import antfarm_actors.mimalloc;

auto runtime = ActorRuntime.create(farm, capacity,
    mimallocV3ActorAllocator());
```

The upstream source is pinned as a submodule at v3.5.0, commit
`18b08671c9302247bfb682286e6bf3cc1773f801`. The actor torture binary keeps its
fast local C-ABI stub, and adds a real static-link lane which asserts
`mi_version() == 30500`. With `MI_OVERRIDE=OFF`, only the explicit actor policy
uses mimalloc; the D runtime and ordinary `malloc` remain unchanged.

The real lane allocates runtime storage and 512 128-byte-aligned actor states
on one thread, reclaims the states across eight other threads, and destroys the
runtime and stable-slot allocation on a ninth. It repeats this for 16 runtime
generations: 8,192 cross-thread state frees plus 16 cross-thread runtime/slot
frees. Both release mimalloc and `MI_DEBUG=FULL` pass, as do DMD and LDC links.
The Clang-built mimalloc ThreadSanitizer lane also passes with the LDC actor
binary, so the cross-thread frees are visible to one sanitizer runtime on both
sides of the C ABI.

Long-running resident-memory behavior and throughput/latency benchmarks are
deliberately deferred. Platform-specific static/shared link coverage also
remains before promotion. Explicit `mi_heap_t` or arena ownership belongs in
the later arena phase because its destroy/reset fence must be tied to aggregate
engine retirement, not inferred from one actor runtime.

## Phases

### A1: non-GC vertical slice

- Fixed-capacity runtime with stable cache-line control slots.
- Caller-supplied allocate/deallocate function pointers plus a C-runtime
  default.
- Typed generation handles, move-only owners, and dynamic-extent borrows.
- Intrusive allocation-free ready queue and a fixed-width actor payload range.
- Permanent trampoline, exclusive state machine, coalesced wake, explicit
  republish, retirement, and state reclamation.
- DMD and LDC tests for mutation, serial entry, stale handles, generation
  reuse, partial publication, and allocator accounting.

The current implementation deliberately restricts construction to POD state.
General placement construction and `@nogc` destruction are deferred until
real actor-state use provides enough evidence to choose their ownership and
failure semantics.

### A2: mailbox admission

- [x] Add a per-generation submission reservation gate.
- [x] Define `send` as: reserve live generation, claim the intrusive node,
  release-publish it to the MPSC inbox, signal the actor, release the
  reservation.
- [x] Close submission admission before retirement and wait for extant
  submitters and accepted inbox nodes.
- [x] Detach producer nodes into an actor-private FIFO carry so bounded drains
  republish without producer/consumer link races.
- [x] Test concurrent producers against entry/drain, bounded republishing,
  close-before-drain retirement, exact-once completion, and stale handles.
- [x] Add targeted forced-interleaving and partial-publication torture beyond
  the current contention test.
- [ ] Evaluate adapters for application-specific bounded and externally owned
  mailbox implementations.

The provided intrusive inbox is a useful default and protocol reference, not
a requirement that every application use one universal message container.
Bounded and application-specific mailboxes must remain possible through a
future adapter surface without weakening generation admission or retirement.

### A3: allocator and arena completion

- Revisit placement construction/destruction for non-POD state only after
  actual actor-state use clarifies the needed ownership and failure semantics;
  any eventual design must retain `nothrow @nogc` enforcement.
- Add adoption of caller-owned stable state.
- Support no-op per-object deallocation for arenas.
- Add retirement groups/fences so a region can close admission, retire its
  actors, and bulk-reset its arena only after quiescence.
- Keep arena and scratch allocator parameters `scope`-qualified where useful,
  while treating the engine-owned region owner, admission state, and aggregate
  retirement fence as the authority for reset. Do not base arena safety on
  transitive compiler lifetime inference.
- Add reserve/prewarm APIs and a fixed-capacity mode with explicit exhaustion
  results rather than hidden fallback allocation.
- [x] Pin a real mimalloc v3 release and test cross-thread frees through the
  global actor allocator adapter.
- [ ] Characterize long-running resident-memory behavior.
- [ ] Benchmark the global adapter before evaluating explicit mimalloc
  heap/arena owners.

### A4: integration and characterization

- [x] Add a resident-facing type-erased creation/owner/handle adapter without
  placing reloadable dispatch pointers in the Farm ring.
- Provide threadpool wake notification hooks without making actor lifetime a
  threadpool concern.
- Exercise multiple actor runtimes and Farms, including migration and covering
  consumers.
- Measure actor activation against a raw single-shot payload and Fiber ready
  activation.
- Run root torture and supported ThreadSanitizer configurations.
- Run the actor-specific deterministic/contention torture suite under DMD,
  optimized LDC, and supported LDC ThreadSanitizer configurations.
- Verify Windows DMD/LDC allocation pairing with the same rigor as the core
  aligned allocator.

This integration is deliberately lower priority than completing the allocator,
construction, and retirement contracts in A3. Explicit `flush()` followed by
the application's existing threadpool notification remains sufficient while
the actor surface is experimental.

There are two notification boundaries to keep distinct. An idle actor becoming
scheduled first publishes a slot to `ActorRuntime`'s ready queue; this may need
to wake a thread capable of calling `flush()`. A successful `flush()` then
publishes one or more activations into the Farm; this may need to wake workers
which consume that Farm. One integrated worker pump may perform both roles, but
the ready-queue and Farm publication edges remain separate.

Wake targeting should normally belong to an engine-defined execution lane: a
runtime/Farm is associated at setup with an immutable, non-owning set of native
and optional covering workers. Any actor in that runtime can notify the same
set. Actors requiring different locality should use another lane/runtime/Farm
or an explicit higher-level route rather than embedding mutable threadpool
ownership in each actor slot. Waking a subset is only a scheduling preference;
it does not prevent an already-running consumer of the same Farm from claiming
the activation, so strict affinity requires separate routing.

The first implementation can use the threadpool's producer-safe `wakeAll()`.
Narrow notification should eventually use a setup-time, producer-safe wake-set
handle rather than the single-owner `Director` selection API. A notification
must occur after the corresponding ready-queue or Farm release-publication and
is only a nudge: workers still acquire and recheck the actual queue/Farm state.
One runnable actor needs at most one executing worker, while a backlog of
distinct ready actors may justify waking more of the eligible set.

## Engine-owned module generations

Module-generation retirement is deliberately not an `antfarm_actors` hierarchy.
An engine-level owner should aggregate actor retirement fences with every other
reference to reloadable code: systems, event subscriptions, jobs, resources,
and message destructors. A safe unload sequence is:

1. Install the new module generation.
2. Close admission to the old generation.
3. Migrate selected state or create replacements explicitly.
4. Retire the old generation's regions and actors.
5. Wait for actor and non-actor retirement fences.
6. Remove dispatch tables and registrations.
7. Unload code and reclaim generation arenas.

Old handles continue to name the old generation and report stale or closed;
they never redirect silently to the replacement generation.

The actor torture suite contains an executable, test-side model of this
engine-owned fence. A single atomic admission gate linearizes close against
old-generation code claims, while a resident registry aggregates those claims
with type-erased actor owners. Four deterministic arms stall a running actor
callback, a module-owned resource/message destructor, a sender, and a popped-
but-uncompleted inbox node independently. The sender stalls after its actor
submission reservation has been released, and the node is deliberately
transferred out of the actor callback. In both cases the actor retires and is
reclaimed first, proving that actor quiescence alone cannot authorize unload.

This harness validates the shape of the engine contract without promoting it
into `antfarm_actors`. Its actor registry is coordinator-driven, it simulates
rather than performs a dynamic-library unload, and its destructor claim is
non-actor module work. It does not settle non-POD actor-state construction or
destruction.

## Promotion gates

The experimental actor API is ready for broader use only when:

- The default shim policy and ring ABI are unchanged.
- Actor handlers use the exact `scope ref ActorBorrow!T` and
  `scope ref ActorContext` callback contract; documentation does not claim
  this makes an `@system` escape impossible.
- A long run with the GC disabled completes without actor-path allocation.
- Allocator counters show zero warm-path allocations and balanced cold-path
  reclamation.
- Concurrent entry instrumentation never observes two borrows for one actor.
- A stale handle remains safe after state reclamation and slot reuse.
- Mailbox retirement races have deterministic, documented outcomes.
- Arena reset is guarded by an aggregate retirement fence.
- Shutdown detects live owners, queued activations, producer submissions, or
  unreclaimed state rather than silently freeing underneath them.

## Appendix: Wave usage and published actor state

An actor wave is the actor-specific bulk-synchronous layer over Farm dispatch.
It is intended for workloads such as game simulation, where many entities can
run the same phase in parallel, but later phases must observe a coherent result
from the earlier phase. A tick can therefore be expressed as a sequence such as
intent, movement, collision, and combat, with one wave boundary wherever
intermediate state must become visible. Actors in a wave never observe one
another's timing-dependent, partially updated state.

This model also clarifies the relationship between `ActorHandle` and
`ActorBorrow`. The handle remains the stable, shareable identity and the borrow
remains the scoped exclusive capability. They need not become the same type.
Instead, wave admission and Farm payload gating acquire that capability on
behalf of the operation: each dispatched operation receives mutable access to
its own actor, and the orchestrator structurally prevents another unfinished
wave over the same actors. This gives callers one handle-oriented dispatch
surface without making an exclusive borrow storable or transferable.

### Aggregate completion

A wave may span several Farm tables. Every table contributes once when its
`Tprogress` reaches `Tlen`; that event increments `Wprogress`. Once publication
has been sealed and `Wprogress == Wlen`, one participant claims the transition
to the finished state. Sealing is necessary because equality while the wave is
still open only means that all tables published so far have completed, not that
no more tables will be published. Table publication is counted before it is
released to consumers, since a small table may finish before `publish()`
returns; a failed publication must roll that count back.

The progress chain is the completion barrier:

```text
actor writes -> table completion -> wave progress -> FINISHED -> waiter
```

Release/acquire ordering across that chain makes actor writes visible to a
waiter that observes the finished wave. The finishing transition is exactly
once, regardless of whether it is claimed by the sealer or the last table
finisher. A failed operation marks the wave failed and must prevent publication
of its prospective shared state.

`ActorWaveHandle` is the authoritative, pollable completion object. A thread or
Fiber may spin briefly, park, or arrange a notification, then acquire and
recheck the handle before publishing a dependent wave. Notifications are only
hints. A future direct Fiber adapter must use a lost-wake-safe
register-then-recheck protocol and preserve the actor path's no-allocation
contract.

The intended orchestration loop is:

1. Begin a wave over a non-overlapping actor set.
2. Publish table-sized prefixes, retrying later when Farm backpressure returns
   an unconsumed suffix.
3. Seal the wave after the final table is admitted.
4. On final success, commit its public generation before exposing `FINISHED`;
   on failure, leave the prior generation committed.
5. Acquire completion, reject a failed result, then publish any dependent wave.

An actor belongs to at most one unfinished wave. Persistent wave membership and
submission pins preserve both exclusivity and lifetime until aggregate
completion; autonomous wake, send, and retirement paths must not bypass that
ownership. Typed wave operation code must likewise remain resident until the
wave finishes.

### Private canonical state and public projections

The preferred state model is not two freely mutable copies of the whole actor.
Each actor has private canonical state, mutated only through its exclusive
borrow, plus a deliberately shaped public projection that other actors may
read. The projection is double-buffered:

```text
wave G reads public[G & 1]
       mutates private state
       writes complete projection into public[(G + 1) & 1]
wave completion publishes generation G + 1
```

All actors in a wave therefore read the same frozen public generation. They may
freely change private state and fill their own staging projection, but they may
not inspect another actor's staging slot. On successful aggregate completion,
the finisher release-publishes the next public generation before exposing
`FINISHED`. The old read buffer then becomes the staging buffer for the next
generation. On failure, the generation does not advance and partial staging
writes remain invisible.

Wave operations should receive an interface conceptually like
`ActorAccess!(Private, Public)`: a mutable `privateState`, a frozen
`publicState` view for the current generation, and `publish(Public)` or a
projector that writes a complete next value. Publishing a complete value is
preferred to exposing a mutable public reference, because a slot not fully
overwritten could otherwise leak data from two generations ago. Keeping the
projection separate also permits compact or structure-of-arrays layouts
without exposing actor internals.

The current `ActorWave` implementation provides scoped private borrowing,
exclusive membership, aggregate completion, and the pollable handle. The
double-buffered public projection is the proposed next layer, not an existing
API guarantee.

### Generation and membership rules

A monotonically increasing public generation is the commit authority; its
parity selects the physical buffer. The full generation value must be checked,
not only the buffer index, so delayed readers cannot mistake a later reuse for
the generation they requested. Per-entry actor generations still validate
handles and slot reuse independently.

If every live actor publishes a complete projection in every generation, one
global public generation is sufficient. Sparse waves require an explicit
policy. They can copy forward untouched projections, or store per-entry public
generation metadata and let readers select the newest committed version for
that actor. Such per-entry tags validate data but do not replace the global
commit barrier. Membership additions, removals, and slot reuse must also become
visible only at a defined wave boundary.

Several public tables that form one logical snapshot should share one commit
epoch, even if their work is spread across several Farm tables. State that is
intentionally visible at different phase boundaries should use separate wave
domains instead.

Public views are naturally callback- or wave-scoped. Ordinary actor-to-actor
reads finish before the next flip, so two buffers suffice. Readers that may
outlive a wave boundary require an additional lifetime mechanism such as epoch
pinning, a third buffer, or RCU-style retirement; double buffering alone cannot
protect them.

Open design work is therefore concentrated in the public projection schema,
sparse-publication semantics, boundary-safe membership changes, long-lived
reader pinning, and the direct Fiber completion adapter. None of those should
weaken the core rule: actors mutate only private state during a wave, observe
only a frozen committed public generation, and expose the next generation only
through successful aggregate wave completion.

The executable characterization in
[`fibers/benchmarks/actor_wave.d`](fibers/benchmarks/actor_wave.d) models two
independent actor sets with one private and two public cache lines per actor.
It compares one Fiber publishing both waves against two Fibers publishing one
wave each; the latter have distinct Farm producer tokens and rendezvous only
to release-publish the shared top-down generation. Wave finish now advances a
preallocated, counted Fiber-generation trigger. The payload callback performs
only atomics and an intrusive deferred enqueue; a managed worker converts that
edge into an ordinary cancellable Fiber wake after leaving `nothrow @nogc`
payload dispatch. This replaces handle polling without admitting locks or
allocation into actor execution, and coalesced or early completions retain
their generation count.
The first optimization pass established that wave publication must reserve only
the exact prefix a physical Farm table will accept. Reserving the caller's full
remaining slice and cancelling its tail on every partial write was quadratic;
the fixed-width Farm path now offers a pre-publication transactional range hook
which lets actor membership acquire only the already-sized table prefix.
