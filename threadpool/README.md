# Ant Farm threadpool

`threadpool` creates persistent workers pinned to logical processors and maps
application-owned objects onto LLC and NUMA topology. A worker is a locator and
execution context, not a queue: your application chooses what each worker
pumps and how idle workers are woken.

Use it independently for topology-aware workers, or combine it with Ant Farm
and `antfarm-fibers` as described in [../ARCHITECTURE.md](../ARCHITECTURE.md).

Supported targets are Windows x64 (Windows 10 1803+) and Linux x86-64 with DMD
2.112+ or LDC 1.42+.

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

`llcIndex` is a dense, process-wide index over physical groups (last-level
sharing domains), suitable for addressing an array. It is not a raw Windows
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
process affinity mask at first discovery. Narrowing `main`'s own mask later
does not change it; call `scopeOsToCallingThread()` before the first
`discover()` to opt into the calling thread's mask instead. Pin threads with
`pinProcessor(id)`; a CPU outside the scope returns
`PinOutcome.outsideOsScope` instead of escaping the launch restriction.

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

`shutdown()` stops every worker after its current `pump` call, runs
`workerStop` once on each worker thread, and joins them. The pool owns no
queue, so finish or cancel application work before calling it.

The default policy after `start()` is `wait`. One owner thread controls the
Director and may select workers by LLC, P/E class, or application label before
choosing `spin`, `wait`, `sleep`, `sleepUntil`, or `cadence`. Other producer
threads use `pool.wakeAll()` rather than acquiring Director ownership.

After an idle result, a worker arms its wait slot and pumps again before it
can sleep. Both worker lanes use this protocol; managed workers use the final
pump's deadline. Publish work before calling `wakeAll()`. Notification exchanges
the slot to active and only calls the OS wake primitive if it was armed.

## Install LLC-local state

Install one value per LLC before starting the pool. Workers use `home!T()` to
find the value associated with their pinned processor; producer/control
threads use `at!T(llcIndex)`. `home!T` returns null off pool threads and names
the group assigned at start, not the CPU the thread is on now.

```d
struct WorkBin
{
    // Queue, Farm pointer, counters, or other application-owned state.
}

auto topology = CacheAwarePool.topology();
auto bins = new WorkBin[](topology.llcCount);
install(bins);
scope (exit) uninstall!WorkBin();

bool pump(WorkerSelf* worker) nothrow @nogc @system
{
    auto bin = home!WorkBin();
    if (bin is null) return false;
    return runOne(bin);
}
```

`search`, labels, and the exchange APIs extend lookup beyond the home LLC.
Install, label, and uninstall only during exclusive setup/teardown; lookup is
concurrent after installation.

## Managed workers

Use `PoolOptions.managedWorker` when workers need allocation, exceptions, or
persistent per-worker state. `start` runs once on the pinned worker, `pump`
runs repeatedly, and `stop` is paired with every successful start.

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
`workerFailures()` after shutdown. `antfarm-fibers` supplies its own managed
hooks for Fiber lanes.

## Thread-safety rules

- First topology discovery, `install`, labels, pool start/shutdown, and
  Director policy changes require exclusive ownership.
- `home`, `search`, `at`, exchange lookup, `currentWorker`, and `wakeAll` are
  safe during the running phase.
- Do not uninstall or relabel state while workers can still look it up.
- Pinning failure is fatal; the pool never silently falls back to migrating
  workers.

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
```

The Linux `scope-test` re-executes itself under `taskset -c` (one physical
group's CPUs on a multi-LLC host, otherwise half the CPUs) and checks the
frozen launch scope, that narrowing `main` before discovery does not shrink
it, `outsideOsScope`/`unknownProcessor` pin outcomes and the default pool's
resolution. Other platforms print a skip.

The Windows `affinity-test` restricts its own process to two initially allowed
CPUs and checks default/explicit worker selection, actual callback placement,
direct pin rejection and startup cleanup after affinity narrows. An optional
decimal mask can select another allowed subset (`-- 12288` selects LPs 12/13
on a machine that permits them). Other platforms and multi-group processes
print an explicit skip.

- [examples/hello_topology.d](examples/hello_topology.d): topology only.
- [examples/hello_antfarm.d](examples/hello_antfarm.d): one Farm per LLC.
- [benchmarks/live_hybrid.d](benchmarks/live_hybrid.d): P-only versus P+E
  characterization; treat it as a benchmark, not onboarding.

Current topology, worker, Director, registry, and OS-backend contracts live in
[DESIGN.md](DESIGN.md).
