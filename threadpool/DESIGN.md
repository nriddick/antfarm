# Threadpool design

`threadpool` discovers cache and processor topology, pins persistent workers,
and lets an application associate its own state with LLC and NUMA locality. It
does not own a task queue or define a work-stealing policy. A worker is a
locator and execution context for application-owned work.

This document records the current 1.6 design. Start with
[README.md](README.md) for usage and [../ARCHITECTURE.md](../ARCHITECTURE.md)
for composition with Ant Farm and `antfarm-fibers`.

## Responsibilities

The package provides:

- one immutable process topology snapshot;
- one persistent worker for each selected logical processor;
- lifetime pinning to the selected processor;
- raw `@nogc nothrow` and managed GC-enabled worker hooks;
- a single-owner Director for idle and wake policy;
- type-specific LLC and NUMA registries for caller-owned objects;
- Windows and Linux topology, affinity, wait, and wake backends;
- counters and retained managed-worker failures.

The application provides:

- queues, Farms, counters, or other work sources;
- ownership and synchronization of installed objects;
- the worker pump and its retry/idle decision;
- publication wakeups and useful-work completion;
- admission and drain policy before pool shutdown.

This separation lets Ant Farm, a private queue, or another scheduler use the
same topology and worker machinery without imposing a common task format.

## Topology snapshot

`discover()` constructs and caches a `TopologySnapshot`. First discovery is a
setup operation; after publication, concurrent readers treat the snapshot as
immutable. `CacheAwarePool.topology()` exposes the same snapshot.

The model records:

- logical processor identity, processor group, CPU-set id, and SMT sibling;
- physical core, L2 cluster, LLC, NUMA node, and package membership;
- cache sizes and line sizes where the OS reports them;
- efficiency class and the dense P/E table class;
- parked state at discovery where available.

`llcIndex` is a process-wide dense index used to address arrays. It is not a
raw Windows cache id or a Linux sysfs identifier. Processor groups and sockets
which reuse an OS-local cache number therefore do not alias in the table.

`efficiencyClass` preserves the OS value. `tableClassIndex` reduces the
topology to the package's dense P/E table classes; higher efficiency class is
treated as the performance class when heterogeneous classes are present. A
homogeneous machine has one effective class.

Each worker receives a `WorkerSelf` containing its LLC, NUMA, class, CPU-set,
worker index, and NUMA-local LLC neighborhood. `currentWorker()` returns a TLS
pointer to that record on pool workers and `null` elsewhere. It identifies the
currently pinned worker; applications must not retain it beyond worker
lifetime.

## Worker selection and pinning

`PoolOptions` filters the topology before start:

- `onlyProcessors` selects exact `(group, lp)` identities;
- otherwise `onlyLps` selects matching in-group LP numbers;
- `enablePCores` and `enableECores` select classes;
- `skipSmtSiblings` keeps the first LP of each physical core;
- `pinScope` is currently required to be `logicalProcessor`.

The exact processor list wins over the shorthand LP list. Linux also omits
processors reported offline or unavailable by the discovery backend.

Start fails if no processor is selected, another pool is already live, or any
worker cannot be pinned. The implementation does not silently continue with a
migrating worker. Windows applies CPU-set affinity and optional performance or
EcoQoS policy. Linux uses scheduler affinity and its native wait backend.

Only one `CacheAwarePool` may be running in the process. This matches the
process-wide TLS locator, worker table, installed registries, and wake slots.

## Worker execution

The pool offers two mutually exclusive pump styles.

### Raw worker body

`WorkerBody` is:

```d
bool function(WorkerSelf*) nothrow @nogc @system
```

The body locates application state, attempts useful work, and returns:

- `true` to run again immediately;
- `false` to apply the worker's current Director idle policy.

This path cannot allocate or throw through the worker loop. It is appropriate
for a bare Ant Farm pump or another hot, non-throwing source.

### Managed worker hooks

`ManagedWorkerHooks` provides `start`, `pump`, and `stop` callbacks on the
pinned worker. A successful start is paired with exactly one stop on that same
thread. `WorkerSelf.context` is reserved for caller-owned persistent state.

`pump` returns `ManagedPumpResult`:

- `again` retries immediately;
- `idle` applies Director policy;
- `until(deadlineTicks)` applies Director policy but bounds the park by an
  absolute `MonoTime` deadline.

Managed callbacks may allocate and throw. An escaping failure stops the worker
system and is retained in `workerFailures()` after shutdown. The Fiber package
uses managed hooks to hold persistent Farm registrations and timer state.

## Worker lifecycle

`start()` performs the following transaction:

1. Validate options and claim the process-wide live-pool slot.
2. Discover topology and select logical processors.
3. Create the worker table and OS wait slots.
4. Spawn one thread per selected processor.
5. Pin every worker and run any managed start hook.
6. Wait for the pin/start handshake from all workers.
7. Release the workers into their pump loops.

Failure during setup requests stop, joins created workers, destroys wait
state, and releases the process-wide slot.

`shutdown()` requests stop and joins all workers, then captures failures and
clears process-wide worker state. The pool cannot generically drain because it
does not own application work. Applications must close admission and drain
their queue, Farm, or Fiber domain before shutting the pool down. The retained
`drain` argument does not create a queue-drain contract.

## Idle policy and the Director

A running pool grants one move-only `Director`. The Director is the sole owner
of policy mutation; acquiring a second Director while one is live fails.
Selections filter workers by LLC, P/E class, string label, or integer tag.

For the selected workers, the Director can choose:

- `spin`: retry after a bounded pause loop;
- `wait`: park until signalled;
- `sleep`: park for a relative duration;
- `sleepUntil`: park until an absolute time;
- `cadence`: align work visits to a repeating time grid;
- `signal`: wake without changing policy.

Cadence can skip missed beats or let a late worker pull immediately. `nudge`
applies a one-shot phase shift; `drift` temporarily slews the period through a
sequenced update. Policy fields are published before a worker's epoch is
advanced and its OS wait primitive is signalled.

Producer threads do not need Director ownership. `pool.wakeAll()` is a
thread-safe, non-owning notification which leaves each worker's idle policy
unchanged. Work publication must occur before the wake in the application's
own queue or Farm synchronization protocol.

## Installed LLC state

The bin registry is parameterized by the installed type, so unrelated
application components can install independent tables.

Two layouts are supported:

- `BinAxis.llc`: one object per dense LLC index;
- `BinAxis.llcAndClass`: one object per `(LLC, dense class)` pair.

Workers call `home!T()` to find the slot for their pinned LLC and class.
Control or producer threads call `at!T(llc, class)` for direct addressing.
`search!T(label)` searches labeled slots in the current worker's NUMA-local LLC
neighborhood; off a pool worker it searches the complete installed table.

The registry stores references to caller-owned arrays. It does not allocate
those objects in LLC- or NUMA-local memory and does not synchronize their
contents. Install, label, and uninstall are exclusive setup/teardown
operations. Lookup is concurrent only while the installed table and labels are
unchanged.

## Cross-NUMA exchange registry

The exchange registry is a separate type-parameterized table with one ingress
object per NUMA node:

- `exchangeHome!T()` finds the current worker's ingress object;
- `exchangeTo!T(remote)` finds a remote node's ingress object;
- `atExchange!T(numa)` addresses a slot from a control thread;
- `exchangeSearch!T(label)` searches remote labeled slots from a worker.

It is only an addressing seam. The package does not transfer bytes, choose a
remote worker, or drain these objects. Pairwise source/destination exchange
tables are not implemented and fail explicitly. Installation and labeling are
setup-only, with the same lifetime rules as LLC bins.

## Ant Farm embedding

The usual non-Fiber embedding installs one Farm-bearing object per LLC. Each
worker then owns a persistent `ConsumerView` and calls `consumeNext()` from its
pump. A producer publishes to the desired Farm and calls `wakeAll` or uses the
Director to signal a narrower worker selection.

The pool does not infer Farm sizing. The application must reserve enough
consumer slots for every native and covering consumer and enough producer slots
for every simultaneous token owner. Farm teardown occurs only after workers
are joined and all registrations have been released.

For managed Fibers, `antfarm-fibers` supplies the hook implementation and lane
coverage rules. Those scheduler contracts live in
[../fibers/DESIGN.md](../fibers/DESIGN.md).

## Thread-safety and ownership

| Operation | Ownership |
| --- | --- |
| first topology discovery | exclusive setup |
| bin/exchange install, label, uninstall | exclusive setup or teardown |
| pool start and shutdown | one lifecycle owner |
| Director policy changes and selected signals | the sole Director owner |
| `home`, `at`, `search`, and exchange lookup | concurrent during stable installation |
| `currentWorker` | calling worker only |
| `wakeAll` | concurrent producers |
| application objects found through registries | defined by the application |

Do not relabel or uninstall state while workers can look it up. Do not start or
shut down the same pool concurrently. Managed per-worker context is initialized
and destroyed on its pinned worker.

## OS backends

Windows topology is built from CPU-set and logical-processor relationship
records. Affinity uses CPU Sets so processor groups and hybrid scheduling
policy remain visible to the OS. Wait-on-address is resolved dynamically with
an event fallback for supported Windows versions.

Linux topology is built from sysfs and scheduler-visible CPUs, then pinning is
applied with the native affinity API. Linux wait/wake uses its native backend.
Both implementations normalize their results into the same immutable dense
topology model. Unsupported operating systems fail explicitly.

The supported product surface is 64-bit Windows and Linux on x86-64. The
control, topology, and Ant Farm integrations assume LP64 and have no 32-bit
contract.

## Observability and validation

`snapshotStats()` reports aggregate worker execution, spin, park, wake, and
related counters without transferring queue ownership. Managed failures are
available only after workers have joined.

The test suite covers captured Windows topology records, synthetic processor
groups and LLC identities, Linux topology parsing, bin/class lookup, NUMA-local
search, exchange addressing, worker selection, pin/start failure, Director
ownership and filtering, cadence arithmetic, wake behavior, managed hook
lifecycle, and live topology/pinning where the host supports it.

The topology hello is the portable inspection example. The Ant Farm hello is
the integration example. `benchmarks/live_hybrid.d` is hardware
characterization, not an onboarding contract.

Multi-LLC behavior is represented in the model and parser tests, but hardware
validation remains a later bridge. No 1.6 performance or failover guarantee is
made from a single-LLC host.
