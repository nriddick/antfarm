# Ant Farm threadpool

`threadpool` creates persistent workers, each belonging to one LLC group, and
maps application-owned objects onto LLC and NUMA topology. A worker is a
locator and execution context, not a queue: your application chooses what each
worker pumps and how idle workers are woken. Where a worker may run is a
choice (`PoolOptions.placement`): anywhere the process may, anywhere in its
group, or on one processor.

Use it independently for topology-aware workers, or combine it with Ant Farm
and `ant_fibers` as described in [../ARCHITECTURE.md](../ARCHITECTURE.md).

Supported targets are Windows x64 (Windows 10 1803+) and Linux x86-64 with DMD
2.112+ or LDC 1.42+.

To run actors on pool workers without writing the hooks yourself, use
`FarmHost` (`threadpool.host`, re-exported by `import threadpool;`). It runs
one Farm's workers on a `GroupHost`, keeps the caller's token and the actor
runtimes, places everything on one LLC, and tears down in order; see
[../docs/guide/hosting.md](../docs/guide/hosting.md). Because of it, the
threadpool dub package depends on the `antfarm` package.

## See this machine

```text
dub run -c hello
```

The command prints logical processors, cores, SMT siblings, LLC domains, NUMA
nodes, cache sizes, and efficiency classes. The same program is
[examples/hello_topology.d](examples/hello_topology.d).

```d
import std.stdio;
import threadpool;

void main()
{
    auto topology = CacheAwarePool.topology();
    writeln("logical processors: ", topology.logicalProcessorCount);
    writeln("LLC domains: ", topology.llcCount);
    foreach (ref cpu; topology.processors)
        writefln("LP %s core %s LLC %s NUMA %s SMT sibling %s",
            cpu.lpIndex, cpu.coreIndex, cpu.llcIndex,
            cpu.numaIndex, cpu.smtSibling);
}
```

For processors with cache-domain membership, `llcIndex` is a dense, process-wide
index over physical groups (last-level sharing domains), suitable for addressing
an array. Linux CPUs offline at discovery remain in `processors` and answer
`byId`, but have `llcIndex == noLlcIndex` and are omitted from `llcDomains`.
They are ineligible for workers and do not affect `groupingSource`.
`llcIndex` is not a raw Windows
cache index or Linux sysfs cache id. `LlcDomain.source` says how each group was
formed: `l3`, or a fallback (`l2Fallback`, `singletonFallback`,
`cpuSetLlcIndex`) that is not a real CCD. Higher `efficiencyClass` values
identify the faster class when the OS exposes hybrid classes.

CPUs are identified by `ProcessorId(group, lp)` everywhere: Windows processor
group plus group-relative LP, or group 0 and the CPU number on Linux. Domain
membership lists are `ProcessorId[] processors`. `parseCpuList` and
`formatCpuList` convert to and from the `0-7,16-23` / `g:a-b` text form:

```d
auto ccd0 = parseCpuList("0-7,16-23");          // group 0
assert(formatCpuList(topology.processorsInLlc(0)) == "0-7,16-23"); // on a 5900X
auto eligible = topology.eligibleProcessorsInLlc(0); // in OS scope and online
```

Worker eligibility uses a frozen, process-level OS scope: on Linux the CPU
mask the process was launched with (`taskset -c 0-7 ./app`), on Windows the
process affinity mask at first discovery. The snapshot carries it:
`topology.osScopeSource` says where it came from and `topology.osScopeAllowed`
lists the allowed CPUs (empty when unrestricted), and
`topology.inOsScope(id)` tests one id. Narrowing `main`'s own mask later
does not change it; call `scopeOsToCallingThread()` before the first
`discover()` to opt into the calling thread's mask instead. `liveOsMask()`
reports the live mask for diagnostics only. Pin threads with
`pinProcessor(id)`; a CPU outside the scope returns
`PinOutcome.outsideOsScope` instead of escaping the launch restriction.

Linux affinity masks and cgroup cpusets are respected, but CFS CPU bandwidth
quotas (`cpu.max` on cgroup v2, `cpu.cfs_quota_us` / `cpu.cfs_period_us` on v1)
are not detected. Quota-limited containers can therefore start too many workers
and suffer oversubscription and throttling. Size the pool explicitly for its
CPU budget: set `PoolOptions.processors` to a suitably sized list of eligible
CPUs, or set `FarmHostOptions.workers` when using `FarmHost`.

## Start workers

The smallest pool provides a `WorkerBody`. Returning `true` asks the worker to
run again immediately; returning `false` applies the Director's idle policy.

```d
import threadpool;

bool pump(WorkerSelf* worker) nothrow @nogc @system
{
    // Locate application state and do one useful unit of work.
    return false;
}

void stop(WorkerSelf* worker) nothrow @nogc @system
{
    // Release per-worker state, e.g. unsubscribe a thread-local ConsumerView.
}

void main()
{
    PoolOptions options;
    options.workerBody = &pump;
    options.workerStop = &stop;
    options.skipSmtSiblings = true;

    auto pool = new CacheAwarePool(options);
    pool.start();
    scope (exit) pool.shutdown();

    pool.director().spin();
    // Publish application work here.
    pool.wakeAll();
}
```

Preview the selection before starting, or inspect it afterwards. Every known
CPU, requested id and physical group is reported with the reasons it was or
was not used:

```d
PoolOptions options;
options.processors = parseCpuList("0-7,16-23");
auto pool = new CacheAwarePool(options);
auto plan = pool.resolve();      // what start() will use
writeln(plan.summary());         // one line
writeln(plan);                   // group table and every CPU
plan.enforceRequested();         // optional: throw on unknown/rejected/duplicate ids
pool.start();
assert(pool.resolution.sameSelection(plan));
```

### Where workers run

The accepted CPUs of one physical group are that group's **set**, and its
workers are interchangeable: a worker has a group and a number, not a
processor. `PoolOptions.placement` says what the OS is told about the set:

| `WorkerPlacement` | a worker may run on | how |
| --- | --- | --- |
| `free` | anything the process may | nothing on Windows; the frozen OS scope on Linux, where a thread would otherwise inherit its creator's mask |
| `group` | its set | one hard affinity mask over the set |
| `pin` | one CPU of its set | as `pinProcessor` |
| `automatic` (default) | `free` when the process can reach one physical group, `group` otherwise | |

- The filters (`processors`, `enablePCores`, `enableECores`,
  `skipSmtSiblings`) decide which CPUs a set holds. Under `free` no mask is
  applied, so they decide how many workers there are and nothing else; ask
  for `group` to have them enforced.
- The automatic worker count leaves some of a set's CPUs without a worker:
  none of one or two, otherwise one in eight and at least one
  (`autoWorkersOf`). A group mask with as many busy workers as CPUs was as
  late as a pin when measured; see
  [the design note](../docs/design/cadence.md). `PoolOptions.workers` sets
  an exact total instead, filling the sets in group order; a host has its
  own `workers` option.
- A set that spans two Windows processor groups cannot be masked: `start`
  throws and names `free` and `pin`.
- Workers carry one thread QoS (`PoolOptions.qos`: `high` by default, `eco`,
  or `unchanged`) and one spin count (`spinIters`).

`plan.sets` lists each set with its CPUs and worker count, `plan.workers` each
worker with its group (and its CPU when pinned), `plan.placement` the placement
`automatic` resolved to, and `plan.workerCountIn(llc)` and
`plan.processorsIn(llc)` answer for one group.

`shutdown()` stops every worker after its current `pump` call, runs
`workerStop` once on each worker thread, and joins them. The pool owns no
queue, so finish or cancel application work before calling it. On a pool
that is not the running one (never started, rejected by `start`, already
shut down) it returns at once.

The default policy after `start()` is `wait`. One owner thread controls the
Director and may select workers by LLC or application label, and so many of
them with `take(n)` and `skip(n)`, before choosing `spin`, `wait` or `sleep`.
`label(name)` and `number(n)` mark the selected workers for `labeled` and
`numbered` to find later. Other producer threads use `pool.wakeOne()`
to wake one armed worker (a woken worker that finds more work wakes the
next), `pool.wakeAll()`, or `pool.wakeLlc(g)` for the workers assigned to
one physical group, rather than acquiring Director ownership;
`pool.anyWorkerArmed()` is the hot-path test before `wakeOne`.

After an idle result, a worker arms its wait slot and pumps again before it
can sleep. Both worker lanes use this protocol; managed workers use the final
pump's deadline. Publish work before calling `wakeAll()`. Notification exchanges
the slot to active and only calls the OS wake primitive if it was armed. A
worker that polls instead of parking, under `spin` or inside a readiness
window, never arms its slot.

A park that has a deadline holds it precisely: a managed deadline, or the
lead before an expected instant. On Windows that is a high-resolution timer
and a wake event per worker, late by about half a millisecond at the median
and a millisecond at the 99th percentile where measured; on Linux, a futex
wait with an absolute deadline. A `sleep(d)` policy's re-poll bound is not a
deadline: it stays on the system timer tick, as the same wait's timeout.

### Be ready for a burst

When work arrives in bursts at times the publisher knows, tell the workers
that should be ready when the next one is due:

```d
auto director = pool.director();
auto ready = director.llc(group).take(4);  // not every worker: a window costs CPU
ready.readiness(1_500, 250);               // lead and hold, microseconds (the defaults)

// The publisher, each tick:
publishBurst();                            // through the Farm or queue, as usual
ready.expect(monoNanos() + periodNanos);   // when the next one is due
```

Each selected worker that is idle parks until the lead before the instant,
polls without parking until the hold after it, and goes back to its policy.
The pool keeps no period and no beat: a publisher that stops calling leaves
the workers on their policy, and one whose bursts are not on a fixed grid
gives each instant as it knows it. `monoNanos()` is `MonoTime` in nanoseconds,
the clock `ant_uv`'s `UvLoop.nanos` reads.

`director.timing()` (or any selection's) returns what the workers recorded,
summed, as a `TimingStats`: per expected instant, whether the worker was
ready, busy, late out of its park or away from its processor; a histogram of
how far deadline parks overshoot, for choosing the lead; a histogram of when a
window's first work arrived, for choosing the hold; and stalls of workers
under `spin`. `resetTiming()` starts the counts again.

## Install LLC-local state

Install one value per physical group (LLC) before starting the pool. The
table is keyed by `llcIndex` and never compacted: build it with `makeBins` from
the pool's resolution and install it with that resolution, which checks the
length and that every group with workers has a slot. Workers use
`home!T()` to find the value of their group; producer/control threads use
`at!T(llcIndex)`. `home!T` returns null off pool threads and names the group
the worker belongs to, not the CPU the thread is on now.

```d
final class WorkBin
{
    // Queue, Farm pointer, counters, or other application-owned state.
    this(uint consumers) { /* size for this group's workers */ }
}

auto pool = new CacheAwarePool(options);
auto res = pool.resolve();
auto bins = makeBins!WorkBin(res, (ref const g) =>
    g.isAccepted ? new WorkBin(g.workers) : null); // null: no workers there
install(bins, res);
scope (exit) uninstall!WorkBin();

bool pump(WorkerSelf* worker) nothrow @nogc @system
{
    auto bin = home!WorkBin();
    if (bin is null) return false;
    return runOne(bin);
}
```

`binStaffed!T(llc)` says whether a group had workers in the validating
resolution. `search` and labels extend lookup beyond the home LLC.
Install, label, and uninstall only during exclusive setup/teardown; lookup is
concurrent after installation.

## Several groups

`import threadpool.groups;` for the host that owns the pool, the caller's
placement and one Farm per accepted group: its worker loop is composed from
attachments (`@nogc` bodies, or GC-enabled service hooks on the pool's
managed lane), workers find their Farm with `home!GroupSlot()`, control
threads publish with `host.farmOf(llc)` and `host.token(llc)`, and
`host.close()` tears down in order. The fork-join dispatcher, a `FarmHost`
with workers and `ant_fibers.groups` all run on it. A `FarmHost` with
`FarmHostOptions.farmPerGroup` gets the same one Farm per group and binds
each runtime or park to a group (`createRuntime(capacity, llc)`,
`createPark!(...)(capacity, options, llc)`), emitting it through that
Farm's control token (`tokenOf(llc)`). See [the design](../docs/design/threadpool.md#group-host) and the
[hosting guide](../docs/guide/scaling-groups.md#several-physical-groups).

## Managed workers

Use `PoolOptions.managedWorker` when workers need allocation, exceptions, or
persistent per-worker state. `start` runs once on the worker, `pump`
runs repeatedly, and `stop` is paired with every successful start. The
optional `poolStopped` runs once on the shutting-down thread after every worker
has joined, even if no worker ever started.

```d
void start(WorkerSelf* worker)
{
    worker.context = new WorkerState;
}

ManagedPumpResult pump(WorkerSelf* worker)
{
    auto state = cast(WorkerState) worker.context;
    return state.runOne()
        ? ManagedPumpResult.again
        : ManagedPumpResult.idle;
}

void stop(WorkerSelf* worker)
{
    worker.context = null;
}

PoolOptions options;
options.managedWorker = ManagedWorkerHooks(&start, &pump, &stop);
```

Escaping worker failures stop the pool and are available through
`workerFailures()` after shutdown as owned, bounded diagnostics, not the
original throwable objects (see [failure capture](../docs/design/threadpool.md#managed-worker-hooks)).
`ant_fibers` supplies its own managed
hooks for Fiber lanes; that package is the reason this lane exists, and a
raw `WorkerBody` is the right choice for everything else.

## Thread-safety rules

- First topology discovery, `install`, labels, pool start/shutdown, and
  Director policy changes require exclusive ownership.
- `home`, `search`, `at`, `currentWorker`, `wakeAll` and `wakeLlc` are safe
  during the running phase.
- Do not uninstall or relabel state while workers can still look it up.
- A placement the OS refuses is fatal: `start` throws, and the pool never
  falls back to another placement.

## Commands and examples

```text
dub test --compiler=dmd
dub test --compiler=ldc2
dub run -c hello
dub run -c live-test
dub run -c wait-races
dub run -c affinity-test --compiler=dmd
dub run -c affinity-test --compiler=ldc2
dub run -c scope-test --compiler=ldc2
dub run -c confine-test --compiler=ldc2
dub run -c expect-test --compiler=ldc2
dub run -c host-test --compiler=ldc2
dub run -c wakefail --compiler=ldc2
```

`dub test` includes the pure FarmHost placement tests on synthetic snapshots,
on any host. `host-test` is the live FarmHost regression: it needs two accepted
worker CPUs in one LLC (one per core by default), or three for a free caller
CPU, and prints SKIP and exits 0 otherwise; `make check` treats that SKIP as a
failure unless `ALLOW_HOST_SKIP=1`. `wakefail` races `wake()` against a
worker-failure shutdown. The test sources live in
[../tests/threadpool/](../tests/threadpool/).

The Linux `scope-test` re-executes itself under `taskset -c` (one physical
group's CPUs on a multi-LLC host, otherwise half the CPUs) and checks the
frozen launch scope, that narrowing `main` before discovery does not shrink
it, `outsideOsScope`/`unknownProcessor` pin outcomes and the default pool's
resolution. Other platforms print a skip.

The Windows `affinity-test` restricts its own process to two initially allowed
CPUs and checks, under every placement, the worker selection, where callbacks
ran and the affinity each worker reads back. After the process is narrowed
further it checks that a pin and a group mask are refused, with startup
cleaned up, and that unconfined workers start and stay inside it. An optional
decimal mask can select another allowed subset (`-- 12288` selects LPs 12/13
on a machine that permits them). Other platforms and multi-group processes
print an explicit skip.

The Windows/Linux `confine-test` checks the calling thread's affinity after
confinement and exact restoration, rejected CPU lists, and `stopRequested` after
a managed worker failure. It starts a pool under each placement from a thread
narrowed to one CPU and has each worker read its own affinity back: the whole
OS scope when unconfined, its set under a group mask, one CPU when pinned.

`expect-test` gives some of a pool's workers the instant of each burst at
240 Hz under every placement. It asserts structure (windows entered, bursts
found without a wake, the other workers untouched, workers at rest once the
publisher stops) and prints the lateness it measured without judging it:
`dub run -c expect-test -- <seconds> <leadMicros>`.

On Windows `confine-test` queries thread group affinity and also
checks that the process mask stays unchanged; it needs two eligible CPUs in the
caller's processor group. On Linux also run it under a small `taskset` mask.
The [native Windows matrix](../docs/development/testing.md#windows-x64-integration)
runs it with both compilers alongside process-affinity and armed-wait checks.

- [examples/hello_topology.d](examples/hello_topology.d): topology only.
- [examples/hello_antfarm.d](examples/hello_antfarm.d): one Farm per accepted
  physical group, each exercised; groups without workers are reported.
- [benchmarks/live_hybrid.d](benchmarks/live_hybrid.d): P-only versus P+E
  characterization; treat it as a benchmark, not onboarding.

Current topology, worker, Director, registry, and OS-backend contracts live in
[docs/design/threadpool.md](../docs/design/threadpool.md).
