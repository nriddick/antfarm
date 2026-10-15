# Threadpool design

`threadpool` discovers cache and processor topology, pins persistent workers,
and lets an application associate its own state with LLC and NUMA locality. It
does not own a task queue or define a work-stealing policy. A worker is a
locator and execution context for application-owned work.

This document records the current design. Start with
[threadpool/README.md](../../threadpool/README.md) for usage and [ARCHITECTURE.md](../../ARCHITECTURE.md)
for composition with Ant Farm and `ant_fibers`.

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
- OS-scope exclusion (`affinityExcluded`), Linux offline state
  (`offlineAtDiscovery`) and informational Windows CPU-set parking
  (`parkedAtDiscovery`).

`llcIndex` is a process-wide dense index over the discovered physical groups
(last-level sharing domains), used to address arrays. It is not a raw Windows
cache id or a Linux sysfs identifier. Processor groups and sockets which reuse
an OS-local cache number therefore do not alias in the table. Physical groups
never disappear or renumber because of OS scope or pool filters.

Each `LlcDomain` records its `GroupingSource`: `l3` for a shared L3,
`l2Fallback` or `singletonFallback` when Linux reports no L3 (or no shared
cache), and `cpuSetLlcIndex` when Windows has no L3 record and the CPU-set
`LastLevelCacheIndex` is used. `TopologySnapshot.groupingSource` is the weakest
source present. A group is a CCD only when its source is `l3` on a chiplet
part; callers that mean "CCD" should check the source.

The CPU identity is `ProcessorId(group, lp)`: the Windows processor group and
the group-relative LP number, or group 0 and the system CPU number on Linux.
Its text form is `g:lp`. The membership lists of `PhysicalCore`, `LlcDomain`,
`NumaNode` and `Package` are group-qualified `processors` arrays sorted by
`(group, lp)`; `processorsInLlc`, `eligibleProcessorsInLlc` and
`processorsOfClass` return the same identity. `parseCpuList` reads
`0-7,16-23` (group 0) and `g:a-b`, preserving order and duplicates;
`formatCpuList` prints the canonical form back.

A processor is eligible (`isEligible`) when it is inside the OS scope and, on
Linux, online at discovery. Windows parking never makes a processor
ineligible.

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

- `processors` selects exact `(group, lp)` identities; build them with
  `parseCpuList` or the topology helpers;
- `enablePCores` and `enableECores` select classes;
- `skipSmtSiblings` keeps the first LP of each physical core.

Both platforms omit processors marked `affinityExcluded` by discovery, even
when explicitly listed in the options. Windows OS parking alone does not
exclude a worker; Linux also omits offline (`offlineAtDiscovery`) processors.
The physical topology counts remain unchanged.

### Resolution and disposition

`resolvePool(snapshot, options)` is a pure function that produces a
`PoolResolution`; the OS scope comes from the snapshot, so a hand-built
snapshot is the whole test seam. `CacheAwarePool.resolve()` previews it
without starting anything; `start()` uses the same function, so the preview
equals the start-time selection. After `start`, `pool.resolution` adds each worker's
`StartState` and `PinOutcome`; after a failed start or a shutdown it keeps the
last attempt's report.

The report covers everything that was considered:

- `cpus`: every known CPU in snapshot order, with the union of every `Reject`
  reason that applies (`osExcluded`, `offline`, `notRequested`,
  `classDisabled`, `smtSecondary`) rather than the first one;
- `requests`: one `RequestDisposition` per `processors` entry in request
  order, marking unknown ids and duplicates (a duplicate never adds a worker);
- `groups`: one `GroupDisposition` per physical group with physical,
  eligible, candidates, accepted and started counts, and the reasons common
  to every member of a group that has no workers;
- `workers`: the accepted CPUs; the index is the `workerIndex`.

`Reject.notSelected` is a policy reason, not a filter: the CPU passed every
filter, but a placement policy above the pool left it out. `resolvePool`
never sets it and it is never combined with another reason. `FarmHost`
narrows its filter-only resolution to the CPUs it chose, so one resolution
answers both "would the filters allow it?" (`reasons & ~notSelected` is
`none`) and "is it a worker?". `GroupDisposition.candidates` counts the
filter-passing CPUs (accepted plus `notSelected`); a group with candidates
but no workers was eligible but not chosen. The host's pool starts from that
narrowed resolution, so `host.pool.resolution is host.resolution`.

Filtering is silent at start by design. `enforceRequested()` is the opt-in
contract check: it throws `ResolutionException` naming every unknown, rejected
(with reasons) or duplicate request. `toString` prints a group table and every
CPU; `summary()` is one line and is included in start-failure messages.
`acceptedCpus`, `rejectedCpus`, `acceptedGroups`, `rejectedGroups`,
`unknownRequests` and `workersIn(llc)` separate the report for callers.

### OS scope

Eligibility is frozen to one process-level OS scope, carried by the snapshot
as `osScopeSource` and `osScopeAllowed` (sorted, empty when unrestricted; it
may name CPUs the snapshot does not know). Discovery marks every processor
outside it `affinityExcluded` from the same capture, and
`TopologySnapshot.inOsScope(id)` tests one id:

| | Linux | Windows |
|---|---|---|
| Scope | the launch mask, read by `sched_getaffinity` in a module constructor before `main` (what `taskset`, `numactl` or a cgroup cpuset produced at exec) | the single-group process affinity mask at first discovery; a multi-group process is `unrestricted` |
| `OsScopeSource` | `processLaunch` | `processAffinity` |
| Refresh | none | none |
| Pin validation | `pinProcessor` rejects CPUs outside the scope (`outsideOsScope`); the kernel's cpuset check still applies (`osRejected`) | `pinProcessor` rejects CPUs outside the scope (`outsideOsScope`); the live process-mask recheck still applies (`osRejected`) |

The scope is never a thread's current mask unless the program opts in with
`scopeOsToCallingThread()` (source `callingThread`), which is allowed only
before the first `discover()`. A program that narrows `main`'s mask before
discovery either calls it or filters with `PoolOptions.processors`.
`liveOsMask()` (`threadpool.pin`) reports what the OS says now, for
diagnostics only.

`pinProcessor(LogicalProcessor)` (`@nogc`) and `pinProcessor(ProcessorId)`
(looks the id up first; `unknownProcessor` if absent) are the only pin
functions, and pool workers use the same path. A pin outside the frozen scope
never happens, including a raw pin from application code, so this library
cannot escape a launch `taskset`. If the OS narrows affinity after discovery
the scope does not shrink; the next pin fails with `osRejected` and pool
startup unwinds with that outcome in `pool.resolution`. Windows group affinity
can otherwise widen a thread beyond the process mask, which the recheck
prevents. Cgroup cpusets are honoured at launch (the launch mask is already a
subset) and enforced live by the kernel; dynamic cpuset tracking is deferred.
Windows job-object hard affinity shows up in the process mask. Multi-group
Windows processes stay unrestricted; that path still needs hardware
validation.

Start fails if no processor is selected, another pool is already live, or any
worker cannot be pinned. The first message includes `summary()`; the pin
message names each failing `g:lp` with its `PinOutcome`. The implementation does not silently continue with a
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

An optional `WorkerStop` (`void function(WorkerSelf*) nothrow @nogc @system`)
runs once on each worker after its body loop exits during shutdown. It is the
place to release thread-local state the body created, such as a subscribed
`ConsumerView`; relying on a final body visit is racy because a stopped worker
does not call its body again.

### Managed worker hooks

`ManagedWorkerHooks` provides `start`, `pump`, and `stop` callbacks on the
pinned worker. A successful start is paired with exactly one stop on that same
thread. `WorkerSelf.context` is reserved for caller-owned persistent state.
The optional `poolStopped` callback runs once per run on the thread that calls
`shutdown` (or whose `start` failed), after every worker has joined, even when
no worker reached `start`; use it to settle state that only a worker's stop
would otherwise settle.

`pump` returns `ManagedPumpResult`:

- `again` retries immediately;
- `idle` applies Director policy;
- `until(deadlineTicks)` applies Director policy but bounds the park by an
  absolute `MonoTime` deadline.

Managed callbacks may allocate and throw. An escaping failure stops the worker
system. Both this lane and FarmHost capture the first failure immediately into
storage allocated before the worker starts: druntime errors can live in TLS
and be overwritten by a later stop-hook error or invalidated at thread exit.
After shutdown joins the workers, `workerFailures()` returns owned
`WorkerFailure!Error` or `WorkerFailure!Exception` diagnostics. They preserve
the Error/Exception distinction, original type name (`sourceType`), source
line, message (up to 1024 bytes) and file name (up to 512 bytes). `truncated`
reports either limit being exceeded. They do not retain the original object,
subclass, exception chain or stack trace. Recording is `@nogc nothrow`; the
worker alone writes its snapshot and the controller reads only after join.
The Fiber package uses managed hooks to hold persistent Farm registrations
and timer state.

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
clears process-wide worker state. Each `WorkerBody` worker finishes its current
visit and then runs the optional `WorkerStop` hook on its own thread, so
thread-local resources such as a Farm `ConsumerView` are released before
`shutdown()` returns. The pool cannot generically drain because it does not own
application work. Applications must close admission and drain their queue,
Farm, or Fiber domain before shutting the pool down. The old
`shutdown(bool drain)` overload is deprecated; it never drained anything.

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
unchanged; `pool.wakeLlc(g)` is the same notification for the workers
assigned to physical group `g` only. Work publication must occur before the wake in the application's
own queue or Farm synchronization protocol.

Each worker owns one atomic ACTIVE/ARMED wait slot. After an idle pump it
exchanges ACTIVE to ARMED with acquire/release ordering, checks stop, and
pumps again. Only a second idle result may enter the OS wait, comparing against
ARMED. A scope guard exchanges back to ACTIVE on every path, including a
managed exception. The final managed result supplies the park deadline.

Every notification performs an acquire/release exchange to ACTIVE, including
ACTIVE-to-ACTIVE notifications; the previous value decides whether an OS wake
is necessary. A notification before arming publishes into the next arming
RMW's acquire chain. A notification after arming either prevents the conditional
wait or wakes it. Shutdown publishes stop and follows the same protocol.
Windows waits on the fixed ARMED value with `WaitOnAddress`; Linux uses futex.
The atomic slot retains a notification delivered before the conditional wait.
Spurious returns always lead back to a work check.

## Installed LLC state

The bin registry is parameterized by the installed type, so unrelated
application components can install independent tables.

Tables are keyed by the physical `llcIndex` and validated against a
`PoolResolution`; there is no unvalidated install. Two layouts are supported:

- `install(bins, res)`: one object per physical group, so `bins.length` must
  equal `res.physicalGroupCount`;
- `install(bins, res, classCount)`: one object per `(group, dense class)` pair,
  `bins.length == physicalGroupCount * classCount`.

For class or pointer types, every slot an accepted worker resolves to must be
non-null; groups without accepted workers may hold null. A compacted table
(one slot per accepted group) or a missing slot is rejected with the groups
named. `makeBins!T(res, make)` builds a correctly sized table, calling `make`
once per physical group with its `GroupDisposition`. The install records which
groups were accepted, readable as `binStaffed!T(llc)`.

Workers call `home!T()` to find the slot for their pinned LLC and class.
`home!T` is valid only on pool-managed threads and returns null elsewhere. It
names the group the worker was assigned at start, not the CPU the thread is
running on now: it does not follow re-pinning or OS migration.
Control or producer threads call `at!T(llc, class)` for direct addressing.
`search!T(label)` searches labeled slots in the current worker's NUMA-local LLC
neighborhood, which may include groups without accepted workers; off a pool
worker it searches the complete installed table.

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

The usual non-Fiber embedding installs one Farm-bearing object per accepted
physical group, sized for that group's workers (`res.workersIn(g).length` or
`GroupDisposition.accepted` native consumers). Each worker then owns a
persistent `ConsumerView` and calls `consumeNext()` from its pump. A table
that aliases one Farm into every slot is legal but deliberately cross-group:
every accepted worker, including those of other groups, is then a consumer of
that Farm. Threadpool cannot detect a raw `subscribe` on another group's Farm;
`home!T` is native by construction and `at!T(llc)` is the explicit spelling
for anything else. A producer publishes to the desired Farm and calls `wakeAll` or `wakeLlc(g)`,
or uses the Director to signal a narrower worker selection.

The pool does not infer Farm sizing. The application must reserve enough
consumer slots for every native and covering consumer and enough producer slots
for every simultaneous token owner. Farm teardown occurs only after workers
are joined and all registrations have been released.

For managed Fibers, `ant_fibers` supplies the hook implementation and lane
coverage rules. Those scheduler contracts live in
[the Fiber scheduler design](ant_fibers.md).

## Thread-safety and ownership

| Operation | Ownership |
| --- | --- |
| first topology discovery | exclusive setup |
| bin/exchange install, label, uninstall | exclusive setup or teardown |
| pool start and shutdown | one lifecycle owner |
| Director policy changes and selected signals | the sole Director owner |
| `home`, `at`, `search`, and exchange lookup | concurrent during stable installation |
| `currentWorker` | calling worker only |
| `wakeAll`, `wakeLlc` | concurrent producers |
| application objects found through registries | defined by the application |

Do not relabel or uninstall state while workers can look it up. Do not start or
shut down the same pool concurrently. Managed per-worker context is initialized
and destroyed on its pinned worker.

## OS backends

Windows topology is built from CPU-set and logical-processor relationship
records. Affinity uses CPU Sets so processor groups and hybrid scheduling
policy remain visible to the OS. `WaitOnAddress` is resolved dynamically and
required on the supported Windows 10 1803+ floor. If it is unavailable, pool
startup throws before allocating wait slots or starting workers; there is no
event fallback.

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
validation remains a later bridge. No performance or failover guarantee is
made from a single-LLC host.
