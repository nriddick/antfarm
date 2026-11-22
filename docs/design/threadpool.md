# Threadpool design

`threadpool` discovers cache and processor topology, runs persistent workers
that each belong to one LLC group,
and lets an application associate its own state with LLC and NUMA locality. It
does not own a task queue or define a work-stealing policy. A worker is a
locator and execution context for application-owned work.

This document records the current design. Start with
[threadpool/README.md](../../threadpool/README.md) for usage and [ARCHITECTURE.md](../../ARCHITECTURE.md)
for composition with Ant Farm and `ant_fibers`.

## Responsibilities

The package provides:

- one immutable process topology snapshot;
- persistent workers in sets, one set per selected physical group;
- a placement for them: unconfined, masked to their group, or pinned;
- raw `@nogc nothrow` and managed GC-enabled worker hooks;
- a single-owner Director for idle and wake policy, readiness for expected
  bursts, and the workers' timing records;
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

`numaIndex` is the system-wide node: the Linux node id, or on Windows the
`NodeNumber` of the `RelationNumaNode` record that covers the processor. The
CPU-set `NumaNodeIndex` is relative to its processor group (two groups can
both say 0 for different nodes, and one node can span groups), so it is used
only for a processor no record covers, and then qualified by group when there
is more than one.

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

Each worker receives a `WorkerSelf` containing its LLC, NUMA node, worker
index, and NUMA-local LLC neighborhood. `currentWorker()` returns a TLS
pointer to that record on pool workers and `null` elsewhere. It identifies the
calling worker; applications must not retain it beyond worker lifetime. A
worker has no P/E class and no processor of its own: which CPU it runs on is
the OS's choice within its placement.

## Worker selection and placement

`PoolOptions` filters the topology before start:

- `processors` selects exact `(group, lp)` identities; build them with
  `parseCpuList` or the topology helpers;
- `enablePCores` and `enableECores` select classes;
- `skipSmtSiblings` keeps the first LP of each physical core.

The CPUs of one physical group that pass the filters are that group's **set**.
A set has workers, and they are interchangeable: a worker is identified by its
group and an ordinal, never by a CPU.

`PoolOptions.placement` (`WorkerPlacement`) is what the OS is told:

| value | a worker may run on | Windows | Linux |
| --- | --- | --- | --- |
| `free` | anything the process may | nothing: a new thread has the process's affinity | `sched_setaffinity` over the frozen OS scope, because a thread inherits its creator's mask and a host narrows its caller before it starts the pool |
| `group` | its set | one hard `GROUP_AFFINITY` mask over the set | `sched_setaffinity` over the set |
| `pin` | one CPU of the set | a CPU-set preference and a one-CPU mask | a one-CPU mask |
| `automatic` (default) | `free` when the process can reach one physical group, `group` otherwise | | |

- Under `free` the filters decide the size of the set, and so the worker
  count, and nothing else: no mask keeps a worker off an E-core or an SMT
  sibling. `group` is how a filter is enforced.
- `group` uses a hard mask and not CPU Sets, which are a preference that did
  not hold when measured.
- One mask names one Windows processor group. A set that spans two cannot be
  masked, and `start` says so before it creates a thread.
- Under `pin` the workers take the set's first CPUs.

The automatic worker count of a set of `n` CPUs is `autoWorkersOf(n)`: all of
one or two, otherwise `n` less one in eight (rounded down) and at least one
less. It is the same under every placement. A masked set with as many busy
workers as CPUs was as late as a pin when measured, and parking workers in
one had park overshoots of 5 to 27 ms; the measurements and their limits are
in [the timekeeping note](cadence.md). The rule is a default:
`PoolOptions.workers` gives an exact total instead, handed out in group
order (a group the count does not reach gets no set), and `start` throws
when the accepted CPUs are fewer. Hosts set their own count and require
`pool.workers` left alone.

Every worker has the same thread QoS (`PoolOptions.qos`: `high`, `eco` or
`unchanged`) and the same spin count (`spinIters`).

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
`StartState` and placement outcome; after a failed start or a shutdown it keeps the
last attempt's report.

The report covers everything that was considered:

- `cpus`: every known CPU in snapshot order, with the union of every `Reject`
  reason that applies (`osExcluded`, `offline`, `notRequested`,
  `classDisabled`, `smtSecondary`) rather than the first one;
- `requests`: one `RequestDisposition` per `processors` entry in request
  order, marking unknown ids and duplicates (a duplicate never adds a worker);
- `groups`: one `GroupDisposition` per physical group with physical,
  eligible, candidates, accepted, workers and started counts, and the reasons
  common to every member of a group that has no workers;
- `sets`: one `SetDisposition` per group with workers: its accepted CPUs,
  its worker count, and the index of its first worker;
- `workers`: one `WorkerDisposition` per worker, set by set; the index is
  the `workerIndex`. It names the worker's group and ordinal, its CPU when
  pinned, and after `start` its `StartState`;
- `placement`: what `automatic` resolved to.

An accepted CPU is one in a set. `CpuDisposition.workerIndex` names the worker
pinned there under `pin`, and is `uint.max` otherwise.

`Reject.notSelected` is a policy reason, not a filter: the CPU passed every
filter, but a placement policy above the pool left it out. `resolvePool`
never sets it and it is never combined with another reason. `FarmHost`
narrows its filter-only resolution to the groups it chose and the worker
count of each (and takes a pinned caller's CPU out of its set), so one
resolution answers both "would the filters allow it?" (`reasons &
~notSelected` is `none`) and "is it in a set?". A smaller worker count keeps
the whole set: there are fewer workers on the same CPUs.
`GroupDisposition.candidates` counts the filter-passing CPUs (accepted plus
`notSelected`); a group with candidates but no workers was eligible but not
chosen. The host's pool starts from that narrowed resolution, so
`host.pool.resolution is host.resolution`.

Filtering is silent at start by design. `enforceRequested()` is the opt-in
contract check: it throws `ResolutionException` naming every unknown, rejected
(with reasons) or duplicate request. `toString` prints a group table and every
CPU; `summary()` is one line and is included in start-failure messages.
`acceptedCpus`, `rejectedCpus`, `acceptedGroups`, `rejectedGroups`,
`unknownRequests`, `workerCountIn(llc)`, `processorsIn(llc)` and `setOf(llc)`
separate the report for callers.

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
functions, and `confineCallingThread(cpus, previous)` the only mask; pinned
and masked pool workers use the same paths. A pin or a mask outside the
frozen scope never happens, including a raw pin from application code, so
this library cannot escape a launch `taskset`. If the OS narrows affinity
after discovery the scope does not shrink; the next pin or mask fails with
`osRejected` and pool startup unwinds with that outcome in
`pool.resolution`. An unconfined worker asks for nothing, so it starts, and
stays inside whatever the process now has. Windows group affinity
can otherwise widen a thread beyond the process mask, which the recheck
prevents. Cgroup cpusets are honoured at launch (the launch mask is already a
subset) and enforced live by the kernel; dynamic cpuset tracking is deferred.
Windows job-object hard affinity shows up in the process mask. Multi-group
Windows processes stay unrestricted; that path still needs hardware
validation.

Start fails if no processor is selected, another pool is already live, or any
worker's placement is refused. The first message includes `summary()`; the
placement message names each failing worker with its `PinOutcome`. The
implementation never falls back to another placement. Windows also applies
the thread QoS; Linux has none to apply.

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
worker. A successful start is paired with exactly one stop on that same
thread. `WorkerSelf.context` is reserved for caller-owned persistent state.
The optional `poolStopped` callback runs once per run on the thread that calls
`shutdown` (or whose `start` failed), after every worker has joined, even when
no worker reached `start`; use it to settle state that only a worker's stop
would otherwise settle.

`pump` returns `ManagedPumpResult`:

- `again` retries immediately;
- `idle` applies Director policy;
- `until(deadlineTicks)` applies Director policy but bounds the park by an
  absolute `MonoTime` deadline, which the park holds precisely (below).

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
2. Discover topology and resolve the sets and their worker counts.
3. Create the worker table, the timing records and the OS wait slots.
4. Spawn one thread per worker.
5. Place every worker and run any managed start hook.
6. Wait for the placement/start handshake from all workers.
7. Release the workers into their pump loops.

Failure during setup requests stop, joins created workers, destroys wait
state, and releases the process-wide slot.

`shutdown()` requests stop and joins all workers, then captures failures and
clears process-wide worker state. Only the live pool (the instance whose
`start` succeeded) owns that state; `shutdown()` on any other instance returns
without touching it. Each `WorkerBody` worker finishes its current
visit and then runs the optional `WorkerStop` hook on its own thread, so
thread-local resources such as a Farm `ConsumerView` are released before
`shutdown()` returns. The pool cannot generically drain because it does not own
application work. Applications must close admission and drain their queue,
Farm, or Fiber domain before shutting the pool down. The old
`shutdown(bool drain)` overload is deprecated; it never drained anything.

## Idle policy and the Director

A running pool grants one move-only `Director`. The Director is the sole owner
of policy mutation; acquiring a second Director while one is live fails.
Selections filter workers by LLC, string label or integer number, and take
so many of the matches with `take(n)` and `skip(n)`; `label` and `number`
mark the selected workers.

For the selected workers, the Director can choose:

- `spin`: retry after a bounded pause loop, never parking and never arming
  the wake slot;
- `wait`: park until signalled;
- `sleep`: park for a relative duration, a bound on how long a publication
  made without a wake can wait;
- `signal`: wake without changing policy.

Policy fields are published before a worker's epoch is advanced and its OS
wait primitive is signalled.

### Readiness for an expected burst

`expect(atNanos)` tells the selected workers when the next burst of
publication is due, as an instant on `MonoTime` in nanoseconds
(`monoNanos()`). It is an overlay on `wait` and `sleep`, not a policy:

1. A worker in a window it has entered does one round of PAUSEs and pumps
   again.
2. Inside `[instant - lead, instant + hold)` an idle worker enters the
   window and does the same.
3. Before it, the worker parks precisely until the lead before the instant.
   Work, `signal`, a new `expect` that needs it, a policy change or stop ends
   the park at once; an earlier managed deadline bounds it, and so does a
   `sleep` policy's duration.
4. With nothing expected, or after the hold, the worker follows its policy.

`readiness(leadMicros, holdMicros)` sets the window; the defaults are 1,500
and 250. The tests of rules 1 and 2 sit in both worker loops ahead of the
wake-slot arm, so a polling worker is never armed. `expect` stores the
instant and wakes the worker unless the instant it replaces is still ahead
and no later than the new one; waking a worker that is not armed is one
exchange with no system call.

The pool keeps no period, no beat number and no slew. The caller owns the
grid, if there is one, and gives each instant as it knows it: an instant is
converted once and its error does not add up, bursts need not be periodic,
and a publisher that stops calling leaves the workers on their policy. The
shipped `cadence`, `nudge` and `drift` verbs kept a copy of the grid in each
worker; they are removed, with the three faults that lived in that copy (a
spin lead that ignored wakes, two pump rounds per beat, a `drift` that
jumped the phase). `cadenceGrid` remains as a pure function for callers that
keep a grid.

A window costs processor time, so `expect` is for a chosen few workers
(`take`). Workers that wake together and poll can also hold each other up:
under a group mask a worker's park sometimes ends only when another worker
of its set stops polling. [The timekeeping note](cadence.md) has the
measurements.

### Timing records

Each worker keeps a record that only it writes, on its own cache lines, so
no worker's path has a shared counter. `Selection.timing()` sums the records
of the workers it matches into a `TimingStats` while they run;
`resetTiming()` asks each worker to clear its own.

- Per expected instant: `ready` (the worker looked within 100 µs of it),
  `busy` (it was running work found before it), `parkedLate` (its park
  returned more than 100 µs after it; `missedWindows` when after the whole
  window) and `spinningLate` (it was polling and next looked more than
  100 µs after it), each with a count over 1 ms and the worst.
- `parkOvershoot`: deadline parks that ran to their deadline, by how far
  past it they returned. For choosing the lead.
- `burstArrival` and `emptyWindows`: when a window's first work came. For
  choosing the hold.
- `stalls`: for a worker under `spin`, gaps of more than 100 µs between idle
  polls with no work between them, sampled about every 256 PAUSEs.
- `parks` and `timedParks`.

Producer threads do not need Director ownership. `pool.wakeAll()` is a
thread-safe, non-owning notification which leaves each worker's idle policy
unchanged; `pool.wakeLlc(g)` is the same notification for the workers
assigned to physical group `g` only; `pool.wakeOne()` wakes one armed or
parked worker and reports whether it found one, and `pool.anyWorkerArmed()`
is the cheap test a hot path makes before calling it. The pool keeps one
bit per worker, set while its wait slot is armed; `wakeOne` clears a bit
before waking so two producers pick two workers, and a bit left set by a
wake from elsewhere costs one spurious exchange on an unarmed word. A woken
worker that then finds more work than it can run wakes the next: a chain
wake with no broadcast on the first producer's path. Work publication must
occur before the wake in the application's own queue or Farm
synchronization protocol.

The spin and park counters (`PoolStats`) are compiled only with
`version (ThreadpoolStats)`: a process-wide atomic add per pause cost 7.8%
of throughput on consumer-bound workloads under `WorkerIdle.spin`.

Each worker owns one atomic wait slot. After an idle pump that is not a
polling round it exchanges ACTIVE to ARMED with acquire/release ordering,
checks stop, and pumps again. Only a second idle result may enter the OS
wait, comparing against ARMED. A scope guard exchanges back to ACTIVE on
every path, including a managed exception. The final managed result supplies
the park deadline.

A park with a deadline (a managed deadline, or the lead before an expected
instant; the earlier when there are both) holds it precisely. A `sleep`
policy's duration is not a deadline: it is the same wait's timeout, which
ends on the system timer tick, so one park ends at whichever comes first.

- **Windows.** The worker waits on a high-resolution waitable timer and a
  wake event of its own. It first moves its slot from ARMED to a third
  state, TIMED; a wake that finds TIMED signals the event where one that
  finds ARMED calls `WakeByAddressSingle`. A wake that lands before the move
  makes it fail and the park return at once. The event is reset before the
  move and never after, so a signal meant for this park is never cleared; a
  signal left by a waker of an earlier park that arrives after the reset
  ends this park early, and the loop's work check absorbs it. The handles
  are kept per worker index for the life of the process, so a waker that
  outlives its pool signals a live handle. Where the system has no
  high-resolution timer an ordinary one is used, and a worker index of
  1,024 or more falls back to a millisecond timeout.
- **Linux.** One futex wait with an absolute `CLOCK_MONOTONIC` deadline
  (`FUTEX_WAIT_BITSET`), the clock `MonoTime` reads. The slot and the wake
  path are unchanged.

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
`PoolResolution`; there is no unvalidated install. `install(bins, res)` takes
one object per physical group, so `bins.length` must equal
`res.physicalGroupCount`.

For class or pointer types, the slot of every group with workers must be
non-null; groups without workers may hold null. A compacted table
(one slot per accepted group) or a missing slot is rejected with the groups
named. `makeBins!T(res, make)` builds a correctly sized table, calling `make`
once per physical group with its `GroupDisposition`. The install records which
groups were accepted, readable as `binStaffed!T(llc)`.

Workers call `home!T()` to find the slot of their group.
`home!T` is valid only on pool-managed threads and returns null elsewhere. It
names the group the worker belongs to, not the CPU the thread is running on
now: under `free` on a machine with several groups the two can differ.
Control or producer threads call `at!T(llc)` for direct addressing.
`search!T(label)` searches labeled slots in the current worker's NUMA-local LLC
neighborhood, which may include groups without accepted workers; off a pool
worker it searches the complete installed table. The neighborhood holds at
most 16 LLCs (`maxNumaLlcs`): the worker's own, then the others on its NUMA
node in processor order. A node that reports more, such as a dual-socket
system with memory interleaving presenting one node across every CCD, omits
the remainder from `search` without diagnostic.

The registry stores references to caller-owned arrays. It does not allocate
those objects in LLC- or NUMA-local memory and does not synchronize their
contents. Install, label, and uninstall are exclusive setup/teardown
operations. Lookup is concurrent only while the installed table and labels are
unchanged.

## Group host

`threadpool.groups.GroupHost` is the one owner of the pool, the caller's
placement, the Farms and the tokens for a program with pool workers. The
fork-join dispatcher is built on it, and so is a `FarmHost` with workers: it
constructs a `GroupHost` with one shared Farm (or one per group with
`FarmHostOptions.farmPerGroup`) and its consuming worker body as the single
raw attachment, keeps its own control token, caller view, state machine,
`step()`/`waitUntil()` and owned runtimes and parks, and closes or abandons
the group host in its own teardown (`abandon` leaks the Farm through
`GroupHost.abandon`). With several Farms each owned runtime or park is bound
to one group's Farm at creation and the host emits it through that Farm's
control token (`GroupHost.token(llc)` on the control thread), waking that
group; `farm` and `token` are the caller group's, and `farmOf`, `farms`,
`tokenOf` and `groups` expose the rest. `FarmHost.groupHost` exposes it. Without
workers a `FarmHost` is a standalone Farm that needs no pool, registry or
placement, so it may exist beside an open host of either kind. It resolves workers with
`planHost` (`FarmHostOptions` mapped from `GroupHostOptions`), so group
selection, `autoWorkers`, caller confinement and the reasons for unused
groups are the same as `FarmHost`'s; `HostGroups.all` selects every group
with an accepted CPU, home first, and `callerProcessor` pins the caller to
one exact CPU kept out of the worker sets.

`planHost` hands workers to sets. `autoWorkers` gives every selected group
its automatic count; an explicit count fills the home group first and then
the next, and may leave a set with no unstaffed CPU, which the plan notes.
A confined caller has the home group's eligible CPUs, SMT siblings included,
and is one more thread in the group. A pinned caller takes the home set's
last CPU out of the set when the workers are masked or pinned and the count
does not need it; under `free` nothing keeps workers off it, which the plan
notes. The plan also notes workers that are not confined to their groups
(`free` on several) and filters that set the count only.

With `farmPerGroup` it creates one Farm per selected group, sized for that
group's workers (plus the caller's view when the caller consumes in that
group), and installs one `GroupSlot {farm, llc, farmIndex}` per group
through `makeBins`/`install`: a worker finds its Farm with
`home!GroupSlot()`, a control thread with `farmOf(llc)`. Without it, one
Farm serves every selected group and every slot names it. Every Farm plans
one small-tier producer for the caller, one per worker with `workerTokens`
(the default; a `FarmHost` without further attachments turns it off, since
its workers only consume, so the Farm itself refuses a producer beyond
`extraSmallProducers`), plus
`FarmSizing.extraSmallProducers`;
`groupToken(llc)` (or `host.token(llc)`) registers the calling thread's
token on that Farm lazily, and `releaseThreadTokens` returns them: after a
worker's stop hook, in `close` for the caller, and by hand on any other
thread that published.

The worker loop is composed from `GroupHostOptions.attachments`, each a
`GroupAttachment`: a `@nogc` per-worker `body` and `stop`, a `consumes` flag
(the body subscribes the worker's own view of its home Farm; the host plans
one consumer per worker per consuming attachment), control-thread hooks
`install`/`uninstall` (after the Farms and registry exist and before the pool
starts; after the pool stopped and before the Farms die; either lane), and
the GC-enabled per-worker hooks `start`/`finish` (once per worker) and
`serve` (per step, returning progress and the earliest deadline). The `(options, body, stop)`
constructor is one consuming `@nogc` attachment.

The host picks the pool lane at start. With only `@nogc` attachments it runs
the raw `WorkerBody` lane: a single attachment's body is installed as the
pool's body, so the fork-join dispatcher's burst loop is byte-identical to a
pool it ran alone on (fib(39) at 32 threads measured unchanged, 101.5 vs
101.9 ms interleaved); several are stepped in order by one composed body. A
GC-enabled per-worker hook selects the managed lane, whose pump consumes the home Farm
through the host's own view when no attachment consumes, steps every body,
runs every `serve`, and returns `again` on progress, `until(deadline)` for
the earliest reported deadline, or `idle`; the pool's arm/recheck protocol
and `applyIdlePolicy` then park the worker until that deadline or a wake.
Fiber lanes attach this way ([ant_fibers.groups](ant_fibers.md)). Payloads
are self-describing, so a consuming attachment's view resumes another
attachment's work (Fiber activations beside the dispatcher).

Workers run their first body call inside the constructor, so everything a
body reads must be built before `new GroupHost` (the dispatcher builds its
tables from `GroupHost.resolve`; a managed attachment builds per-Farm state
in `install`). `close` is the ordered teardown: pool shutdown (finish and
stop hooks, then token release), the attachments' `uninstall` in reverse,
the caller's tokens, the registry, the Farms, the caller's affinity, the
pool claim. The registry and the per-thread tokens are process-wide, so one
`GroupHost` is open at a time: a second is rejected before it acquires
anything, with or without workers (a host without workers takes no pool
claim), and the open host is left as it was. `wakeOne`, `anyWorkerArmed`, `wakeAll` and `wakeLlc` pass through to
the pool.

## Ant Farm embedding

The usual non-Fiber embedding installs one Farm-bearing object per accepted
physical group, sized for that group's workers (`res.workerCountIn(g)` or
`GroupDisposition.workers` native consumers). Each worker then owns a
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
| bin install, label, uninstall | exclusive setup or teardown |
| pool start and shutdown | one lifecycle owner |
| Director policy changes and selected signals | the sole Director owner |
| `home`, `at`, `search` | concurrent during stable installation |
| `currentWorker` | calling worker only |
| `wakeAll`, `wakeLlc` | concurrent producers |
| application objects found through registries | defined by the application |

Do not relabel or uninstall state while workers can look it up. Do not start or
shut down the same pool concurrently. Managed per-worker context is initialized
and destroyed on its worker.

## OS backends

Windows topology is built from CPU-set and logical-processor relationship
records. A pin is a CPU-set preference plus a one-CPU mask; a group mask is
a `GROUP_AFFINITY` over the set. `WaitOnAddress` is resolved dynamically and
required on the supported Windows 10 1803+ floor. If it is unavailable, pool
startup throws before allocating wait slots or starting workers. Deadline
parks wait on a timer and an event instead, as above.

Linux topology is built from sysfs and scheduler-visible CPUs, and placement
is applied with `sched_setaffinity`. Linux wait/wake is the futex.
Both implementations normalize their results into the same immutable dense
topology model. Unsupported operating systems fail explicitly.

The supported product surface is 64-bit Windows and Linux on x86-64. The
control, topology, and Ant Farm integrations assume LP64 and have no 32-bit
contract.

## Observability and validation

`Selection.timing()` reports the workers' own records (above).
`snapshotStats()` reports the process-wide spin and park counters, which are
compiled only with `version (ThreadpoolStats)`. Managed failures are
available only after workers have joined.

The test suite covers captured Windows topology records, synthetic processor
groups and LLC identities, a simulated 256-processor, four-group, two-node
Windows host whose group-relative indices all alias, Linux topology parsing, bin lookup, NUMA-local
search, sets and worker counts, every placement with each worker's affinity
read back, placement/start failure, Director ownership and filtering, the
readiness window and its counts, wakes and stops against deadline parks,
managed hook lifecycle, and live topology where the host supports it.

The topology hello is the portable inspection example. The Ant Farm hello is
the integration example. `benchmarks/live_hybrid.d` is hardware
characterization, not an onboarding contract.

Multi-LLC behavior is represented in the model and parser tests, but hardware
validation remains a later bridge. No performance or failover guarantee is
made from a single-LLC host.
