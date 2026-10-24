module threadpool;

// Windows and Linux. topology.discover / CacheAwarePool.start throw Error
// on any other OS.
//
// Thread-safety: install / setLabel / first discover / start / shutdown are
// setup or exclusive (e.g. shared static this, then start from main).
// home / search / at lookups are concurrent-safe after that setup. Director
// is single-owner. wakeAll is safe for producers.

public import threadpool.topology : TopologySnapshot, LogicalProcessor, LlcDomain,
    PhysicalCore, L2Cluster, CacheInfo, discover, tableClassIndex, classE, classP,
    maxNumaLlcs, fillNumaLlcs, ProcessorId, parseCpuList, formatCpuList,
    CpuListException, GroupingSource, noLlcIndex, OsScopeSource, scopeOsToCallingThread;
public import threadpool.pool : CacheAwarePool, PoolOptions,
    Director, Selection;
version (ThreadpoolCadence) public import threadpool.pool : Cadence;
public import threadpool.resolution : PoolResolution, resolvePool, Reject,
    StartState, CpuDisposition, RequestDisposition, GroupDisposition,
    ResolutionException, reasonsText;
public import threadpool.pin : PinOutcome, pinProcessor, liveOsMask;
public import threadpool.hybrid : WorkerSelf, currentWorker, bindNumaNeighborhood,
    WorkerBody, WorkerStop, ManagedWorkerHooks, ManagedPumpResult, IdleKind,
    anyArmed, wakeOneArmed, wakeOneArmedIn;
public import threadpool.bins : makeBins, install, uninstall, binStaffed, setLabel, home,
    search, at;
public import threadpool.stats : PoolStats;
public import threadpool.failure : WorkerFailure;
// The hosts: one Farm for actor programs, one Farm per group for raw bodies.
public import threadpool.host : FarmHost, FarmHostOptions, FarmSizing, HostGroups,
    CallerPlacement, WorkerIdle, HostState, HostAttachment, autoWorkers, defaultHostPoolOptions,
    FarmHostException, FarmHostNotDrained, FarmHostResolveException, FarmHostTimeout, FarmHostFailed;
public import threadpool.groups : GroupHost, GroupHostOptions, GroupSlot, groupFarmOf, groupToken,
    releaseThreadTokens;
