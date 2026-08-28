# threadpool

Windows-first D library that discovers how cores sit on L3 caches (and P vs E on Intel hybrid), pins a worker to one logical processor, and lets that worker **locate** concurrency objects mapped onto that CPU structure.

v1 is Windows x64 (Windows 10 1803+; `GetSystemCpuSetInformation` is required, no older-Windows fallback) and Linux x86_64 (DMD ≥ 2.112 or LDC ≥ 1.42).

## Hello topology

Print the map. No pool, no tasks.

```d
import std.stdio;
import threadpool;

void main()
{
    auto t = CacheAwarePool.topology();

    writeln("os ", t.os,
        "  L3 domains ", t.llcCount,
        "  NUMA nodes ", t.numaNodes.length,
        "  P-cores ", t.pCoreCount,
        "  E-cores ", t.eCoreCount,
        "  logical processors ", t.logicalProcessorCount,
        "  cache line ", t.cacheLineSize, " B");

    foreach (ref d; t.llcDomains)
    {
        writefln("  LLC %s  size %s KiB  line %s  LPs %s  P-cores %s  E-cores %s",
            d.llcIndex, d.l3SizeBytes / 1024, d.lineSize,
            d.lpIndices, d.pCoreIndices, d.eCoreIndices);
    }

    writeln();
    writeln("lp  group  core  llc  numa  class  smtSib  l2KiB  l3KiB  module");
    foreach (ref p; t.processors)
    {
        writefln("%2s  %5s  %4s  %3s  %4s  %5s  %6s  %5s  %5s  %6s",
            p.lpIndex, p.group, p.coreIndex, p.llcIndex, p.numaIndex,
            p.efficiencyClass, p.smtSibling,
            p.l2.sizeBytes / 1024, p.l3.sizeBytes / 1024, p.moduleIndex);
    }

    if (auto here = t.current())
        writefln("\nthis thread is on LP %s, LLC %s, NUMA %s, efficiencyClass %s",
            here.lpIndex, here.llcIndex, here.numaIndex, here.efficiencyClass);
}
```

From this directory:

```text
dub run -c hello
```

On the i7-12700H this was written against you should see **one** L3 over 20 logical processors, **6** SMT P-cores (`efficiencyClass` 1, LPs 0–11) and **8** E-cores (`efficiencyClass` 0, LPs 12–19) in two L2 modules.

On a Ryzen 5 5500 (Linux) you should see **one** 16 MiB L3 over 12 logical processors, **6** SMT cores (`efficiencyClass` 0, `classCount` 1 — there is no P/E split), L2 of 512 KiB per core, and SMT pairs **(0,6) (1,7) … (5,11)** rather than adjacent Windows-style pairs.

`llcIndex` is a process-wide id (not the group-relative Windows `LastLevelCacheIndex`, and not the sysfs cache `id`). Higher `efficiencyClass` is the performance set.

## Ant Farm examples

`examples/hello_antfarm.d` installs one farm per LLC and has workers publish and drain a few payloads. `examples/live_hybrid.d` pins P-only vs P+E consumers. This package now lives inside the authoritative Ant Farm repository, so both examples use the parent directory's Farm modules.

```text
dmd -g -i examples/hello_antfarm.d ../antfarm.d -Isource "-ofhello_antfarm.exe"
dmd -g -i examples/live_hybrid.d ../antfarm.d ../antfarm_templates.d -Isource "-oflive_hybrid.exe"
```

## After the map: bins and workers

A worker is a locator, not a queue. You install your own `C` types one-per-LLC, pass a worker body, start the pool, and from that body ask for the node’s `C`. Lookup returns `C*` because a D `ref` cannot be null. The pool does not own work items.

The default `WorkerBody` remains `@nogc nothrow`. Code that needs allocation,
exceptions, or persistent per-worker setup can instead provide
`PoolOptions.managedWorker` with `start`, `pump`, and `stop` function pointers.
The two lanes are mutually exclusive. A successful managed start is paired
with exactly one stop on the same pinned worker, and `WorkerSelf.context` is
preserved between those calls. An escaping `Throwable` stops the pool and is
available through `workerFailures()` after `shutdown()` joins the workers.
Managed `pump` returns `ManagedPumpResult`: immediate retry, ordinary idle, or
idle with an absolute `MonoTime.ticks` deadline. A deadline bounds rather than
replaces Director policy—the worker wakes at the earlier of the managed
deadline and the Director's sleep, sleep-until, or cadence deadline.

```d
void start(WorkerSelf* w) { w.context = makeWorkerState(w); }
ManagedPumpResult pump(WorkerSelf* w)
{
    return runOne(cast(MyState*) w.context)
        ? ManagedPumpResult.again : ManagedPumpResult.idle;
}
void stop(WorkerSelf* w)  { destroyWorkerState(cast(MyState*) w.context); }

PoolOptions opt;
opt.managedWorker = ManagedWorkerHooks(&start, &pump, &stop);
auto pool = new CacheAwarePool(opt);
```

```d
struct C1 { int x; }
struct C2 { int y; }

auto t = CacheAwarePool.topology();
auto c1s = new C1[](t.llcCount);
auto c2s = new C2[](t.llcCount);
install(c1s);
install(c2s);
setLabel!C1(cast(ushort) 0, "ingest");

bool pump(WorkerSelf* w)
{
    if (auto a = home!C1()) { /* push/pull on a */ }
    if (auto b = home!C2()) { /* … */ }
    auto named = search!(C1, string)("ingest");
    return false; // idle → director policy
}

PoolOptions opt;
opt.workerBody = &pump;
auto pool = new CacheAwarePool(opt);
pool.start();
scope (exit) pool.shutdown(true);
auto d = pool.director();
d.llc(0).classP.spin();
d.labeled("drain").wait();
```

`home!C()` is the current worker’s home LLC. `search!(C, L)(label)` from a worker stays in that worker’s NUMA node. `L` is a string or an integer. `PoolOptions.onlyLps` matches `lpIndex` in every processor group; on multi-group Windows use `onlyProcessors` (`ProcessorId(group, lp)`), which wins if set.

The worker body returns `true` to run again immediately, `false` to idle. Idle policy is a single-owner `Director`: `spin`, `sleep`, `sleepUntil`, `wait`, `cadence`, `nudge`, `drift`, plus `signal` to wake. Filters intersect LLC, P/E, and `pool.tag` labels/numbers. Default after `start` is `wait`. Producers that do not hold the director may `pool.wakeAll()`.

`cadence(period)` is a best-effort wake grid so idle workers yield instead of spinning, and still wake near when the owner is likely to publish. A late worker is just another consumer (it pulls now). `Cadence(skipMissed: true)` is opt-in tick-drop for a later design. Optional `spinLead` parks until `beat - lead` then spins.

```d
auto d = pool.director();
d.cadence(dur!"msecs"(1));
d.nudge(dur!"usecs"(-200));                 // one-shot phase step
d.drift(dur!"usecs"(50), dur!"msecs"(2));    // slew +50µs/beat until 2ms accumulated, then nominal
// owner thread: poll SDL / present timing / net clock; workers pull
```

`nudge` moves the grid once (large error). `drift` is a new sequence plus a per-beat addend and an `|period-delta|` budget; workers apply it themselves and stop when the budget is spent — the director does not have to correct back to zero. `signal()` wakes without moving the grid. Controller logic (PLL, `VK_EXT_present_timing`, net sync) lives elsewhere.

## Thread-safety

`install`, `setLabel`, `installExchange`, and the first `discover()` / `CacheAwarePool.topology()` are **not** thread-safe. Call them from `shared static this()` (DRuntime runs that single-threaded) or from one thread before `start()`. Then start the pool; workers and the SDL/main producer may use the lookup APIs concurrently.

| API | Thread-safe? |
| --- | --- |
| `discover` (first call), `CacheAwarePool.start` / `shutdown` | No — setup / exclusive |
| `install` / `uninstall` / `setLabel` | No — setup |
| `installExchange` / `uninstallExchange` / `setExchangeLabel` / `installExchangePairs` | No — setup |
| `home` / `search` / `at` / `homeFor` | Yes, after `install` until uninstall |
| `exchangeHome` / `exchangeTo` / `exchangeCount` / `atExchange` / `exchangeSearch` | Yes, after `installExchange` until uninstall |
| `Director` / `Selection` policy | Exclusive owner thread |
| `tag` | Owner thread, post-`start` |
| `wakeAll` | Yes, while the pool is running |
| `currentWorker` | Yes (TLS) |
| `cachedSnapshot`, `TopologySnapshot.current` | Yes, after a completed `discover` |
| `bindNumaNeighborhood` | No — `start` only |

Do not `setLabel` / `uninstall` while workers are in `home` / `search`. `at` / `atExchange` are the producer-side lookups (SDL thread); `home` / `exchangeTo` need a pool worker.

Example:

```d
shared static this()
{
    auto t = CacheAwarePool.topology();
    auto bins = new C1[](t.llcCount);
    install(bins);
    setLabel!C1(cast(ushort) 0, "ingest");
}

void main()
{
    PoolOptions opt;
    opt.workerBody = &pump;
    auto pool = new CacheAwarePool(opt);
    pool.start();
    // workers: home!C1() / search!(C1, string)("ingest")
    // this thread: at!C1(llc), pool.director()
}
```

## Commands

```text
dub test                  # captured topology blobs + bins (DMD)
dub test --compiler=ldc2
dub run -c hello          # print this machine’s map
dub run -c live-test      # pin P/E workers on i7-12700H (skips otherwise)
```

## Developer’s guide: new OS or topology scheme

The portable contract is `TopologySnapshot` in `source/threadpool/topology.d`. Discovery, pin, wait, and the worker loops are the OS-specific pieces. Bins (`install` / `home!C()`), labels, and `installExchange` only read the snapshot; they should not grow `#ifdef`s.

**Add an OS**

1. Keep filling the same fields: one `LogicalProcessor` per hardware thread (`group`, `lpIndex`, `coreIndex`, **process-wide** `llcIndex`, `llcIndexInGroup` if the OS has a group-relative LLC id, `numaIndex`, `efficiencyClass`, SMT, cache sizes/line). Then synthesize `cores`, `l2Clusters`, `llcDomains`, `numaNodes`, `packages`, `groups`. Set `os` to a short name.
2. `llcIndex` must be dense `0 .. llcCount-1` for the whole process. Do not use a socket-local cache id as the table row (Windows `LastLevelCacheIndex` is the cautionary example). Prefer “who shares this L3?” as the source of truth.
3. `efficiencyClass`: higher = more performant. One distinct value → `classCount == 1` (everyone runs the P-loop). Two or more → `classE = 0` (everything strictly below max), `classP = 1` (max). Collapse 3+ OS classes onto E in v1.
4. Hook `discover()` in `topology.d`: `version (Windows)` calls `discoverWindows()`; `version (linux)` calls `discoverLinux()`. Other OSes throw. **Never** start unpinned workers if discovery or pin fails.
5. Put the parser in `source/threadpool/sys/<os>_topology.d`. Bindings in `sys/<os>_bindings.d` if the platform headers are missing from Druntime. Pin lives in `pin.d` (or `sys/<os>_pin.d`); wait in `sys/<os>_wait.d`. `CacheAwarePool.start` / `worker.d` use `version (ThreadpoolOs)` — add the OS to that identifier rather than a silent fallback. Shared table assembly is `assembleSnapshot` in `topology.d`.
6. Linux (implemented): `parseSysfsTopology` reads `/sys/devices/system/cpu/cpuN/cache/indexN/{level,type,size,shared_cpu_list,coherency_line_size,ways_of_associativity,id}` for L3 groups (sharing set, not cache `id`); `topology/thread_siblings_list` for SMT; `nodeN/cpulist` for NUMA (`cpuN/numa_node` is often missing — do not require it; `cpu0/online` is also often missing and means online). Hybrid class is `/sys/devices/cpu_core/cpus` + `cpu_atom/cpus` when present, else mixed `cpu_capacity`. CPUID leaf `0x1A` is **not** used: it reports the *current* CPU, so discovery would have to pin every LP. Pin with `sched_setaffinity`; `current()` is `sched_getcpu`. WSL’s sysfs is **not** a faithful hybrid map; test on bare metal or a VM with topology passthrough.
7. Tests: parse **captured buffers** or a sysfs-shaped tree so CI does not need the gold machine. Add a synthetic multi-LLC (and two-group / two-NUMA) fixture — that is the only way the LLC and NUMA axes get coverage on a laptop with one L3. See `parseWindowsTopology` + `tests/topology_parse.d`, `parseSysfsTopology` + `tests/linux_topology_parse.d`, and `tests/captured/`.

**Add a topology scheme on an OS you already have**

Same snapshot, different grouping rule. Examples: treat L2 clusters as the shard instead of L3; expose dies/modules as `llcIndex`; dual-socket as two NUMA domains. Implement it inside the OS parser (or a post-pass over `LogicalProcessor[]`), then keep `llcDomains` / `numaNodes` consistent with whatever you chose as the shard. `install` stays `bins[llcIndex]`; `installExchange` stays `bins[numaIndex]`. Do not special-case a CPU model in library code — the machine is a test fixture (`dub run -c live-test`), not a `version (Sisyphus)`.

**Checklist**

- `TopologySnapshot.current()` uses the OS “what LP am I on?” API (`GetCurrentProcessorNumberEx` on Windows).
- Pin one LP for the worker’s life; P and E must not migrate onto each other.
- Cross-NUMA stays on the exchange table (`exchangeTo` / `exchangeHome`), not `home!C()` / `search`.

## License

BSL-1.0
