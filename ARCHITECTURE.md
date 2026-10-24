# Ant Farm architecture

Ant Farm (version in [VERSION](VERSION)) is one repository
with five cooperating packages. They separate transport, worker placement,
stateful identities, and suspendable control flow so an application can choose
the narrowest representation for each kind of work.

```text
application
    |-- short @nogc work -----------------------------+
    |-- actor wake/inbox --> actors -- ready flush ---+--> antfarm
    |-- actor phase ------> actors -- wave tables ----+    work tables
    |-- private hierarchy -> root park -- pump -------+
    |-- fork-join tree ----> forkjoin deques -- spill +
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

The hot API is `@nogc nothrow`. Each payload callback runs exactly once and
receives a borrowed body pointer and word count through a C-linkage void
callback. The body must not be modified or retained after return. Tables store compact records or bare bodies when
callbacks are shared ([SPEC 2f](SPEC.md#2f-compact-tables) and
[2g](SPEC.md#2g-uniform-call-tables)). Independent payloads can execute
concurrently. Grain adapters group source elements into single-shot payloads;
construction, referenced-data lifetime and aggregate completion stay with
the caller.

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
layout; `antfarm.actors` supplies exclusive scoped borrowing and the publication
boundary.

Private roots and parent-owned cohorts are a separate path in
[`antfarm.actors.private_actor`](docs/guide/private-actors.md). A single emitter owns a
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

### Fork-join

`forkjoin` dispatches recursive fork-join work on the threadpool's workers:
one bounded Chase-Lev deque per participant holds forked children, the owner
pops the newest and thieves take the oldest, and the Farm is the admission
channel for roots from other threads and the overflow when a deque is full.
Every task is a heap frame beginning with a `Node`; `run` and `join` return
continuations, so the worker loop is a trampoline and the join never blocks a
thread. Idle workers park through the Director and are woken by a push onto
an empty deque, a spill or a submission. The contract and measurements are
in [docs/design/forkjoin.md](docs/design/forkjoin.md).

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

Each activation shares one callback and has a fixed one-word body—the
task address—so publication uses the Farm's specialized common-callback,
fixed-length write path. A Fiber may resume on any eligible worker, and must
not retain TLS-derived state across suspension.

## Choosing the work representation

Use the [work-representation guide](docs/guide/choosing-work.md) to choose
payloads, public actors, actor waves, private hierarchies, fork-join trees,
or Fibers. It owns
the comparison and the public-vs-private decision.

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
with the command in [First payload](docs/guide/quickstart.md#what-the-dispatcher-does-for-you-the-raw-ring-one-thread).
The Linux `make check` matrix does not compile it; that matrix does compile
and run the actor programs under
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
| single-threaded, one callback, one nonzero body length | the body words | [SPEC 2g](SPEC.md#2g-uniform-call-tables), index-free |
| single-threaded, one callback | one index word and the body words | [SPEC 2g](SPEC.md#2g-uniform-call-tables) |
| single-threaded, several callbacks | one index word, one callback word and the body words | [SPEC 2f](SPEC.md#2f-compact-tables) |

A shared callback is stored once per table, and a shared length once more. A
one-word body therefore costs 1, 2 or 3 ring words per entry. The number of
entries admitted per quota depends on the shard count, the body width, and which
row the prefix falls in. Do not copy a count from another configuration:
compute it from the table layouts in [SPEC.md](SPEC.md#2-data-layout), or probe
the configuration you intend to run. `write() == 0` and a short accepted prefix
are ordinary backpressure, and the surplus is written by a later call.
Use `AntFarm.canFit(callback, bodyWords, tier)` to pre-check one payload's singleton
cost against its tier quota; its answer uses the current consumer-count snapshot
and can change as consumers join or leave (see [SPEC 5b.3](SPEC.md#5b-write-call-and-return)).

Quotas and payload sizes are in ulongs (8 bytes); only the ring size is in MiB.
`ringMiBFromUlongs` converts a former ulong length exactly.
`expectedConsumers` seeds the first epoch's leaf sharding; later epochs use
the live count. The hard limit is `MAX_CONSUMERS_LIMIT` (128) simultaneously
subscribed views, beyond which `subscribe()` returns `SUBSCRIBE_FULL`.

For ring, tier, batch, work-cost, and page-backing decisions, use
[the sizing guide](docs/guide/sizing-and-allocation.md).

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

The same rule holds below the Fiber layer: a Farm's consumers belong to one
group, and a second group is reached by giving it its own Farm and publishing
into it one-way, in large tables of payloads that carry real work. The
measurements and the token, quota and ring-size consequences are in
[several physical groups](docs/guide/scaling-groups.md#several-physical-groups).

## Shutdown order

Stop admission and drain while producers and consumers can still make progress.
Then follow the canonical [shutdown order](docs/reference/actor-lifecycle.md#shutdown-order),
including the Fiber steps there; `FarmHost.close()` implements the host's teardown.

## Maintainer documents

- [SPEC.md](SPEC.md): Farm algorithm and memory ordering.
- [docs/reference/actor-lifecycle.md](docs/reference/actor-lifecycle.md): shared actor setup, drain, and
  shutdown contracts, including the [shutdown recipe](docs/reference/actor-lifecycle.md#shutdown-recipe).
- [examples/README.md](examples/README.md): compiled public and private actor
  programs, including outcomes, cancellation, and deadline examples.
- [docs/design/threadpool.md](docs/design/threadpool.md): topology, worker, Director, and
  registry contracts.
- [docs/design/ant_fibers.md](docs/design/ant_fibers.md): Fiber architecture, ownership, and
  synchronization invariants.
- [PERFORMANCE.md](PERFORMANCE.md#fibers): Fiber benchmark evidence and
  tuning guidance.
- [docs/guide/public-actors.md](docs/guide/public-actors.md): public actor ownership, retirement, and
  phase-oriented waves.
- [docs/design/public-actor-ordering.md](docs/design/public-actor-ordering.md): actor lifecycle, ready-queue,
  Farm, wave completion, inbox, retirement, and handoff edges.
- [docs/design/private-actor-ordering.md](docs/design/private-actor-ordering.md): private join, outcome and
  cancellation ordering.
- [docs/design/private-actor-layout.md](docs/design/private-actor-layout.md): private Entry/Service/cohort
  layout and slab reuse.
- [tests/actors/README.md](tests/actors/README.md): forced actor
  interleavings, sustained generation churn, and allocator-adapter checks.
- [ROADMAP.md](ROADMAP.md): remaining and later work.
