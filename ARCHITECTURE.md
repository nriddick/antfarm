# Ant Farm architecture

Ant Farm 1.7.0-rc.1 is one repository with four cooperating packages. They
separate transport, worker placement, stateful identities, and suspendable
control flow so an application can choose the narrowest representation for
each kind of work.

```text
application
    |-- short @nogc work -----------------------------+
    |-- actor wake/inbox --> actors -- ready flush ---+--> antfarm
    |-- actor phase ------> actors -- wave tables ----+    work tables
    |-- waiting/yielding Fiber --> fibers -- resume --+       ^
    |                                                        |
    +<-- wave completion --> Fiber trigger                    |
                                                             |
                                                        threadpool
                                                 pinned worker pumps
```

## Responsibilities

### Ant Farm

The Farm owns a fixed-size, virtually double-mapped ring. Producers reserve
quota and publish complete tables; consumers independently claim chunks. Work
is not FIFO and a false `consumeNext()` result may be a temporary hole.

The hot API is `@nogc nothrow`. Payload callbacks receive a copied 128-byte
header, a body of packed ulongs, and an iteration number. A payload may be
serial or countable/multiconsumer, but construction and completion policy stay
with the caller.

### Actors

The actor runtime owns a fixed-capacity set of stable control slots and
separately allocated POD state. A move-only `ActorOwner` controls retirement
and reclamation; copyable `ActorHandle`s carry identity without extending state
lifetime. Autonomous wakes and inbox sends coalesce through a ready queue, and
`ActorRuntime.flush` publishes those activations as fixed-width Farm payloads.

An `ActorWave` is the bulk-synchronous path for a phase over a set of actors.
Admission reserves exclusive borrowing before publication, rejects duplicate
or overlapping membership, and may split the set across multiple Farm tables.
The sealed wave finishes once every table finishes. An acquire observation of
that generation's completion makes all of its actor writes visible to the
orchestrator. A dependent phase proceeds only if the completed handle is not
failed.

Wave peers are still parallel: they cannot use same-wave mutation as an
ordering channel. Cross-actor reads should come from an immutable public
projection selected at a prior wave boundary, while each actor mutates its
private state or an inactive projection. The application owns that projection
layout; `actors` supplies exclusive scoped borrowing and the publication
boundary.

### Threadpool

The pool discovers CPU/cache topology, pins persistent workers, and associates
application-owned state with LLC and NUMA domains. It does not impose a job
queue. A worker body can pump a Farm, an application queue, or any other source.

One owner controls Director policy. Workers may spin, wait, sleep, or wake on a
cadence; ordinary producers use `wakeAll()`.

### Fibers

The Fiber scheduler turns one runnable Fiber activation into one serial,
one-word Farm payload. It owns generations, waits, timers, cancellation,
completion, lifecycle records, GC roots, and stack recycling. The Farm sees
only ready activations.

Each activation shares one callback/header and has a fixed one-word body—the
task address—so publication uses the Farm's specialized common-header,
fixed-length write path. A Fiber may resume on any eligible worker, and must
not retain TLS-derived state across suspension.

## Choosing the work representation

Use a bare payload when the work is short, `nothrow @nogc`, and cannot suspend.
This is the lowest-overhead path and permits a payload to expose independent
iterations through its header.

Use an autonomous actor when work is organized around a stable identity with
explicit non-GC state lifetime, coalesced activation, or an intrusive inbox.
The actor handler receives an exclusive callback-local borrow and may request
republishing through `ActorContext`; the owner retains retirement authority,
and the handler cannot suspend.

Use an actor wave when many idle actors should run the same phase operation
and a later phase must see the completed phase as one generation. A wave
operation receives only the exclusive borrow, not `ActorContext`, and the same
actor cannot belong to overlapping unfinished waves.

Use a managed Fiber when the work needs ordinary D exceptions, allocation,
`scope(exit)`, timer sleep, application signals, joins, or cooperative
cancellation. A Fiber which never suspends is usually better represented as a
payload.

The project intentionally has no general parallel-loop abstraction in 1.7.
Callers can construct countable Farm payloads directly when their ownership and
completion contract is clear. A higher-level bounded parallel API may be
designed later without preserving the removed experimental surface.

## The `iota_sum` path

[examples/iota_sum.d](examples/iota_sum.d) is the compile-and-run integration
example. It exercises the complete non-Fiber path:

1. Discover processor and LLC topology.
2. Create one Farm sized for the selected worker count.
3. Install a `FarmBin` so every pinned worker can locate that Farm.
4. Start a `CacheAwarePool` whose worker body subscribes one persistent
   `ConsumerView` and calls `consumeNext()`.
5. Adapt `iota(1, n + 1)` with `payloadRange!sumJob`. The generated range has
   one common callback and a compile-time fixed packed width.
6. Publish a prefix, pop exactly the returned count, and retry on backpressure.
7. Wait for application completion, then unwind pool, consumer, token, and Farm
   ownership in that order.

The source remains a real example rather than a README-only snippet so release
checks can compile and execute the exact onboarding program.

## The actor-wave path

Actor dispatch reuses the same Farm and worker pump as ordinary payloads:

1. Create an `ActorRuntime` for one Farm and an explicit slot capacity.
2. Create POD actor state through an `ActorOwner`; distribute only its
   generation-tagged handles.
3. For autonomous work, wake or send to handles and call `runtime.flush` from
   a producer until the ready queue is admitted to Farm tables.
4. For a phase, begin an `ActorWave`, publish handle prefixes while respecting
   Farm backpressure, then seal it.
5. Poll the returned `ActorWaveHandle`, or attach an `ActorWaveTrigger` so a
   Fiber can publish the dependent phase after completion.
6. Stop senders, finish all wave membership, request retirement, drain any
   resulting activation, reclaim every owner, and only then destroy the actor
   runtime.

The actor operation and any code it reaches must remain resident until its
activation or wave completes. Full examples and the private/public projection
model are in [actors/ACTOR_ROADMAP.md](actors/ACTOR_ROADMAP.md); the exact
happens-before edges are in
[actors/ACTOR_MEMORYORDER.md](actors/ACTOR_MEMORYORDER.md).

## Sizing one Farm

`AntFarm.create` needs a ring length, segment count, expected consumer count,
and capacity/quota for bulk and small producer tiers.

- Ring length is in ulongs, is a power of two, and should generally fit the
  intended LLC working set.
- Four or eight segments are the characterized choices.
- Consumer capacity must cover every simultaneously subscribed view, including
  any remote covering sweeper.
- Producer capacity must cover every worker/control producer that can hold a
  token simultaneously.
- Publish useful batches. Single-item writes pay table overhead every time.
- `avgCost` controls consumer chunk size: cheap callbacks use a smaller value;
  expensive callbacks use a larger value.
- Ordinary 4 KiB backing is the default. Huge pages are an explicit option for
  high-volume payload table walks and should be selected from measurements.

Common-header and fixed-length writes reduce producer-side sizing overhead for
homogeneous batches. They do not change the ring layout or consumer contract.

## One LLC and many LLCs

The software model supports one lane per LLC and shared domain-wide Fiber
lifecycle. Covering responders form a sweep graph so runnable work is not
owned by one locality.

Current characterization hardware has one LLC. Multi-LLC validation is a
bridge that must be crossed before topology-specific performance, loss of a
native worker group, or remote-sweeper claims are treated as release evidence.
It is not emulated away and it is not a blocker for the 1.7 release-candidate
single-LLC contract.

## Shutdown order

For a full stack:

1. Stop admitting application work.
2. Seal admitted actor waves and let their orchestrating Fibers observe
   completion.
3. Stop actor inbox senders, retire and reclaim actors, then destroy actor
   runtimes after their ready queues and wave memberships drain.
4. Signal or cooperatively cancel other indefinite Fiber waiters.
5. Drain runnable work and completion/lifecycle records.
6. Release completed Fiber tasks.
7. Shut down and join the worker pool.
8. Uninstall lane/bin mappings.
9. Unsubscribe any control-thread consumers and unregister tokens.
10. Destroy Farms only after no consumer or producer can touch them.

The owning application remains responsible for bounded shutdown of user code
that never reaches a scheduler boundary.

## Maintainer documents

- [SPEC.md](SPEC.md): Farm algorithm and memory ordering.
- [threadpool/DESIGN.md](threadpool/DESIGN.md): topology, worker, Director, and
  registry contracts.
- [fibers/DESIGN.md](fibers/DESIGN.md): Fiber architecture, ownership, and
  synchronization invariants.
- [fibers/PERFORMANCE.md](fibers/PERFORMANCE.md): Fiber benchmark evidence and
  tuning guidance.
- [ACTOR_ROADMAP.md](actors/ACTOR_ROADMAP.md): non-GC actor ownership,
  retirement, and phase-oriented wave plan.
- [ACTOR_MEMORYORDER.md](actors/ACTOR_MEMORYORDER.md): actor lifecycle, ready-queue,
  Farm, wave completion, inbox, retirement, and handoff edges.
- [actor_torture/README.md](actor_torture/README.md): forced actor
  interleavings, sustained generation churn, and allocator-adapter checks.
- [ROADMAP.md](ROADMAP.md): current release gates and later work.
