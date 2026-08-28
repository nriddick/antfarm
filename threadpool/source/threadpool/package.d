module threadpool;

// Windows and Linux. topology.discover / CacheAwarePool.start throw Error
// on any other OS.
//
// Thread-safety: install / setLabel / installExchange / first discover /
// start / shutdown are setup or exclusive (e.g. shared static this, then
// start from main). home / search / at / exchange* lookups are
// concurrent-safe after that setup. Director is single-owner. wakeAll is
// safe for producers.

public import threadpool.topology : TopologySnapshot, LogicalProcessor, LlcDomain,
    PhysicalCore, L2Cluster, CacheInfo, discover, tableClassIndex, classE, classP,
    maxNumaLlcs, fillNumaLlcs;
public import threadpool.pool : CacheAwarePool, PoolOptions, PinScope, ProcessorId,
    Director, Selection, Cadence;
public import threadpool.hybrid : WorkerSelf, currentWorker, bindNumaNeighborhood,
    WorkerBody, ManagedWorkerHooks, ManagedPumpResult, IdleKind;
public import threadpool.bins : BinAxis, install, uninstall, setLabel, home, search, at;
public import threadpool.exchange : installExchange, uninstallExchange, exchangeHome,
    exchangeTo, exchangeCount, atExchange, setExchangeLabel, exchangeSearch,
    installExchangePairs;
public import threadpool.stats : PoolStats;
