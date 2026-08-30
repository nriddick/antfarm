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

## Current A1/A2 spike

As of 2026-08-30, `antfarm_actor.d` contains two deliberately narrow vertical
slices:

- a fixed-capacity runtime and stable cache-line slots allocated entirely
  through `ActorAllocator`;
- POD state copied into a separate allocator-owned allocation;
- typed handles, transfer-only owners, callback-local borrows and context;
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
  multi-actor generation churn, and an opt-in mimalloc v3 adapter contract.

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
- At most one activation for a generation is queued, published, or running.
  Wakes coalesce through a pending bit rather than publishing duplicates.
- Different actors remain independently runnable and may execute in parallel.

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

## API direction

The concrete spelling is experimental, but the responsibilities should remain
separate:

```d
auto runtime = ActorRuntime.create(farm, capacity, allocator);
auto owner = runtime.createActor!(State, runActor)(constructorArgs);
ActorHandle!State handle = owner.handle;

handle.wake();                    // queued, coalesced, stale, or closed
handle.send(&interaction.node);   // transfers caller-owned node if accepted
runtime.flush(token, maximum);    // fixed-width Farm publication

owner.requestRetire();
if (owner.retired)
    owner.reclaim();              // destructor plus allocator release
```

The application callback receives an `ActorBorrow!T` and `ActorContext`. It
may mutate `T`, request another activation, or request retirement. It cannot
suspend; suspendable work remains a Fiber responsibility.

The built-in inbox is an accumulator-style interaction protocol. Any producer
may initialize freshly allocated storage with `ActorInboxNode.initialize` and
submit it; the actor is the only consumer and processes nodes while it
exclusively owns its state. A wake is only a coalescing notification, not one
unit of work. The node and payload remain caller-allocated, and a successful
send owns the node until the actor calls `complete` or consumes its storage.
This makes malloc, slab, pool, and arena storage equally usable without adding
a GC or allocation policy to the actor runtime.

## First third-party allocator candidate: mimalloc v3

`antfarm_actor_mimalloc.d` is a deliberately thin, optional mimalloc v3
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
import antfarm_actor_mimalloc;

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

The initial implementation may restrict construction to POD state while the
state machine is being proved. General placement construction and `@nogc`
destruction must be addressed before declaring the API stable.

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

- Add placement construction/destruction for non-POD state without losing
  `nothrow @nogc` enforcement.
- Add adoption of caller-owned stable state.
- Support no-op per-object deallocation for arenas.
- Add retirement groups/fences so a region can close admission, retire its
  actors, and bulk-reset its arena only after quiescence.
- Add reserve/prewarm APIs and a fixed-capacity mode with explicit exhaustion
  results rather than hidden fallback allocation.
- [x] Pin a real mimalloc v3 release and test cross-thread frees through the
  global actor allocator adapter.
- [ ] Characterize long-running resident-memory behavior.
- [ ] Benchmark the global adapter before evaluating explicit mimalloc
  heap/arena owners.

### A4: integration and characterization

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

## Engine-owned module generations

Module-generation retirement is deliberately not an `antfarm_actor` hierarchy.
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

## Promotion gates

The experimental actor API is ready for broader use only when:

- The default shim policy and ring ABI are unchanged.
- A long run with the GC disabled completes without actor-path allocation.
- Allocator counters show zero warm-path allocations and balanced cold-path
  reclamation.
- Concurrent entry instrumentation never observes two borrows for one actor.
- A stale handle remains safe after state reclamation and slot reuse.
- Mailbox retirement races have deterministic, documented outcomes.
- Arena reset is guarded by an aggregate retirement fence.
- Shutdown detects live owners, queued activations, producer submissions, or
  unreclaimed state rather than silently freeing underneath them.
