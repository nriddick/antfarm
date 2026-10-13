# Ant Farm architecture

Ant Farm's current release is 1.8.0. It is one repository
with four cooperating packages. They separate transport, worker placement,
stateful identities, and suspendable control flow so an application can choose
the narrowest representation for each kind of work.

```text
application
    |-- short @nogc work -----------------------------+
    |-- actor wake/inbox --> actors -- ready flush ---+--> antfarm
    |-- actor phase ------> actors -- wave tables ----+    work tables
    |-- private hierarchy -> root park -- pump -------+
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

The hot API is `@nogc nothrow`. Payload callbacks receive a
`PayloadHeader*` (the type is 128 bytes), a body of packed ulongs, and an
iteration number. Multi-threaded payloads and mixed tables keep the full header
in the ring. Tables whose payloads are all single-threaded store compact
records without a ring header, or only the bodies when the table's payloads
share one callback, so the callback instead sees a temporary consumer-local
header: callbacks must not modify it or retain the pointer after returning
([SPEC.md sections 2f](SPEC.md#2f-compact-st-tables) and
[2g](SPEC.md#2g-uniform-call-st-tables)). A payload may be
serial or countable/multiconsumer, but construction and completion policy stay
with the caller.

### Actors

The actor runtime owns a fixed-capacity set of stable control slots and
separately allocated POD state. A move-only `ActorOwner` controls retirement
and reclamation; copyable `ActorHandle`s carry identity without extending state
lifetime. Autonomous wakes and inbox sends coalesce through a ready queue, and
`ActorRuntime.flush` publishes those activations as fixed-width Farm payloads.
Inbox nodes report `completed`, `failed(code)`, or `cancelled`; cancellation
closes admission and is cooperative, with no forced callback interruption.

An `ActorWave` is the bulk-synchronous path for a phase over a set of actors.
Admission reserves exclusive borrowing before publication, rejects duplicate
or overlapping membership, and may split the set across multiple Farm tables.
The sealed wave finishes once every table finishes. An acquire observation of
that generation's completion makes all of its actor writes visible to the
orchestrator. It does not join an optional completion hook. A dependent phase
proceeds only if the completed handle is not failed.

Wave peers are still parallel: they cannot use same-wave mutation as an
ordering channel. Cross-actor reads should come from an immutable public
projection selected at a prior wave boundary, while each actor mutates its
private state or an inactive projection. The application owns that projection
layout; `actors` supplies exclusive scoped borrowing and the publication
boundary.

Private roots and parent-owned cohorts are a separate path in
[`actors.private_actor`](docs/guide/private-actors.md). A single emitter owns a
root park and publishes root and child tables through `pump`; subscribed
consumers execute the callbacks. Parents authorize whole-cohort or subset
waves, read cohort-owned result ports or externally owned buffers after the
join, and may fold child work inside one enclosing wave. Domain failures are
values (`context.fail`), in-tree cancellation is cooperative
(`park.cancel`, `cancelChildren`, `cancelPeers`), and an opt-in tracked park
adds thread-safe root cancellation, lazy deadlines, and pull-based terminal
outcomes. State may be non-POD; the stored instance is destroyed exactly once.
There is no general submission handle, no external
identity for a child, and no access to child state; a tracked root exposes only
a cancellation/status handle. Use this path for an exclusive hierarchy; use public actors for
independently addressed services and externally selected waves. Shared
lifecycle rules are in [docs/reference/actor-lifecycle.md](docs/reference/actor-lifecycle.md).

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

The project intentionally has no general parallel-loop abstraction.
Callers can construct countable Farm payloads directly when their ownership and
completion contract is clear. A higher-level bounded parallel API may be
designed later without preserving the removed experimental surface.

The full work-representation table and the public-vs-private actor comparison
are in [docs/guide/choosing-work.md](docs/guide/choosing-work.md).

## The `iota_sum` path

[examples/iota_sum.d](examples/iota_sum.d) is the compile-and-run integration
example. It exercises the complete non-Fiber path:

1. Discover processor and LLC topology.
2. Create one Farm sized for the selected worker count.
3. Install a `FarmBin` so every pinned worker can locate that Farm.
4. Start a `CacheAwarePool` whose worker body subscribes one persistent
   thread-local `ConsumerView` and calls `consumeNext()`, and whose
   `workerStop` hook unsubscribes that view.
5. Adapt `iota(1, n + 1)` with `payloadRange!sumJob`. The generated range has
   one common callback and a compile-time fixed packed width.
6. Publish a prefix, pop exactly the returned count, and retry on backpressure.
7. Wait for application completion (a callback-maintained counter), then shut
   down the pool, which runs every worker's `workerStop` and joins it, and only
   then uninstall the bins and destroy the Farm.

The source remains a real example rather than a README-only snippet. Build it
with the command in the README. The Linux `make check` matrix does not compile
it; that matrix does compile and run the actor programs under
[examples/](examples/README.md).

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
6. Stop senders, finish all wave membership, request retirement (or escalate
   to cancel), drain any resulting activation, reclaim every owner, and only
   then destroy the actor runtime.

The actor operation and any code it reaches must remain resident until its
activation or wave completes. Actors in one wave share the Farm and the
operation's state metadata; they may come from more than one runtime.
`finished` joins actor writes, not a completion hook's later access. Compiled
programs are [examples/public_service.d](examples/public_service.d),
[examples/public_wave.d](examples/public_wave.d), and
[examples/private_tree.d](examples/private_tree.d). The private/public
projection model is in [docs/guide/public-actors.md](docs/guide/public-actors.md#waves); the
exact happens-before edges are in
[docs/design/public-actor-ordering.md](docs/design/public-actor-ordering.md).

## Sizing one Farm

`AntFarm.create` takes nine parameters, all defaulted. Pass them by name:

```d
auto farm = AntFarm.create(ringMiB: 8, k: 8, expectedConsumers: workers,
                           maxBulk: 0, maxSmall: producers, quotaSmall: 4096);
```

| Parameter | Default | Meaning |
| --- | --- | --- |
| `ringMiB` | `8` | ring size in MiB; power of two, `>= 2` (at most `MAX_RING_MIB`, 64 GiB) |
| `k` | `8` | segment count; power of two in `[2, 16]` |
| `expectedConsumers` | `4` | initial sharding hint, not a cap |
| `maxBulk` | `2` | simultaneous bulk-tier tokens; `0` disables the tier |
| `quotaBulk` | `0` | per-token bulk quota in ulongs; `0` means one segment |
| `maxSmall` | `16` | simultaneous small-tier tokens; `0` disables the tier |
| `quotaSmall` | `4096` | per-token small quota in ulongs; must be `> 0` if enabled |
| `smallThreshold` | `64` | tables shorter than this are not sharded; `0` selects the auto rule |
| `hugePages` | `false` | request the platform huge-page path |

Invalid combinations are fatal at construction. In particular the sum of all
quotas, `maxBulk * quotaBulk + maxSmall * quotaSmall`, must not exceed `k - 1`
segments. The ring holds `ringMiB * 131072` ulongs, so that limit is
`(k - 1) * ringMiB * 131072 / k` ulongs, and every payload must fit one table
within its producer's quota.

### How many entries fit a quota

A table spends its quota on fixed overhead plus a per-entry cost. The overhead
is the table header and padding and grows with the table's work-shard count
(eight ulongs per shard counter), which follows the consumer count. The
per-entry cost depends on what the accepted prefix of one `write` call shares:

| Accepted prefix | Per-entry cost | Layout |
| --- | --- | --- |
| single-threaded, one callback, one nonzero body length | the body words | [SPEC 2g](SPEC.md#2g-uniform-call-st-tables), index-free |
| single-threaded, one callback | one index word and the body words | [SPEC 2g](SPEC.md#2g-uniform-call-st-tables) |
| single-threaded, several callbacks | one index word, one callback word and the body words | [SPEC 2f](SPEC.md#2f-compact-st-tables) |
| contains a multi-threaded payload | one index word, the 16-word payload header and the body words, plus a second index word per multi-threaded entry | [SPEC 2e](SPEC.md#2e-table-layout) |

A shared callback is stored once per table, and a shared length once more. A
one-word body therefore costs 1, 2, 3 or 18 ring words per entry. The number of
entries admitted per quota depends on the shard count, the body width, and which
row the prefix falls in. Do not copy a count from another configuration:
compute it from the table layouts in [SPEC.md](SPEC.md#2-data-layout), or probe
the configuration you intend to run. `write() == 0` and a short accepted prefix
are ordinary backpressure, and the surplus is written by a later call.

- The ring should generally fit the intended LLC working set. Quotas and
  payload sizes remain in ulongs (8 bytes); only the ring size is in MiB.
  `ringMiBFromUlongs` converts a former ulong length exactly.
- Four or eight segments are the characterized choices.
- `expectedConsumers` seeds the first epoch's leaf sharding; later epochs use
  the live count. The hard limit is `MAX_CONSUMERS_LIMIT` (128) simultaneously
  subscribed views, beyond which `subscribe()` returns `SUBSCRIBE_FULL`. A
  count far from reality costs performance, never safety.
- Producer capacity must cover every worker/control producer that can hold a
  token simultaneously.
- Publish useful batches. Single-item writes pay table overhead every time.
- `avgCost` (0 through 7) controls consumer chunk size as `128 >> avgCost`:
  the default 3 claims 16 entries at a time; very cheap uniform callbacks can
  use 0 through 2 (chunks 128, 64, 32) to amortize claims; expensive callbacks
  use a larger value for shorter visits.
- Ordinary 4 KiB backing is the default. Huge pages are an explicit option for
  high-volume payload table walks and should be selected from measurements.

Common-header and fixed-length writes reduce producer-side sizing overhead for
homogeneous batches. They produce the same ring layout a `PayloadEntry` range
with the same contents would, and do not change the consumer contract.

## One LLC and many LLCs

A `FiberServicePlan` binds one lane to each accepted LLC group of the pool
resolution, and lanes may share one domain-wide Fiber lifecycle. Service is
native-only by default: domain membership adds no cross-group consumers. A
group without accepted workers is either refused (`unstaffed`, spawns throw)
or served through an explicit `rescue` edge. `assist` edges, including the
explicit `assistRing`, are experimental and opt-in for staffed groups. The
plan derives each Farm's consumer and producer sizing from these edges.

Native-only service, unused-group admission and rescue are validated on a
two-LLC host (Ryzen 9 5900XT). The default broadcast wake still reaches
every worker; the opt-in `FiberWakePolicy.servingGroups` wakes only the groups
serving a lane. Topology-specific performance and
lost-worker-group claims still need their own evidence before they are treated
as release evidence.

## Shutdown order

For a full stack:

1. Stop admitting application work.
2. Seal admitted actor waves and let their orchestrating Fibers observe
   completion. If a wave installed a completion hook, do not reuse or destroy
   that wave storage until the hook's last access.
3. Stop actor inbox senders. A public service calls `requestRetire` (drain)
   and may escalate with `requestRetire(ActorRetireMode.cancel)` after an
   application deadline; handlers then call `context.cancelInbox()` while
   `context.cancelling`. A private park drains with `pump` or
   `collectCompleted` until `live == 0`, or calls `cancelAll()` first; a
   tracked park must also take outcomes. Do not destroy runtimes or parks yet.
   `pump() == 0` is not that drain. The full recipe is
   [docs/reference/actor-lifecycle.md](docs/reference/actor-lifecycle.md#shutdown-recipe).
4. Signal or cooperatively cancel other indefinite Fiber waiters.
5. Drain runnable work and completion/lifecycle records.
6. Release completed Fiber tasks.
7. Shut down and join the worker pool. Its `workerStop` hooks unsubscribe
   worker-owned consumers before `shutdown()` returns.
8. Uninstall lane/bin mappings.
9. Unsubscribe any control-thread consumers. Together with the worker
   unsubscribe in step 7, this is what gets every consumer out of the trampoline.
10. Destroy actor runtimes, and call `park.destroy()` on each drained private
    park. `destroy(park)` does not free the park.
11. Unregister producer tokens.
12. Destroy Farms only after no consumer or producer can touch them.

The owning application remains responsible for bounded shutdown of user code
that never reaches a scheduler boundary.

## Maintainer documents

- [SPEC.md](SPEC.md): Farm algorithm and memory ordering.
- [docs/reference/actor-lifecycle.md](docs/reference/actor-lifecycle.md): shared actor setup, drain, and
  shutdown contracts, including the [shutdown recipe](docs/reference/actor-lifecycle.md#shutdown-recipe).
- [examples/README.md](examples/README.md): compiled public and private actor
  programs, including outcomes, cancellation, and deadline examples.
- [threadpool/DESIGN.md](threadpool/DESIGN.md): topology, worker, Director, and
  registry contracts.
- [fibers/DESIGN.md](fibers/DESIGN.md): Fiber architecture, ownership, and
  synchronization invariants.
- [fibers/PERFORMANCE.md](fibers/PERFORMANCE.md): Fiber benchmark evidence and
  tuning guidance.
- [docs/guide/public-actors.md](docs/guide/public-actors.md): public actor ownership, retirement, and
  phase-oriented waves.
- [docs/design/public-actor-ordering.md](docs/design/public-actor-ordering.md): actor lifecycle, ready-queue,
  Farm, wave completion, inbox, retirement, and handoff edges.
- [docs/design/private-actor-ordering.md](docs/design/private-actor-ordering.md): private join, outcome and
  cancellation ordering.
- [docs/design/private-actor-layout.md](docs/design/private-actor-layout.md): private Entry/Service/cohort
  layout and slab reuse.
- [actor_torture/README.md](actor_torture/README.md): forced actor
  interleavings, sustained generation churn, and allocator-adapter checks.
- [ROADMAP.md](ROADMAP.md): current release gates and later work.
