module threadpool.stats;

/// Counted only in builds with `version (ThreadpoolStats)`.
struct PoolStats
{
    ulong parks, spins;
}

/// Process-wide counters mutated by workers (@nogc). Snapshot copies them out.
shared ulong gParks;
shared ulong gSpins;

void resetStats() @nogc nothrow
{
    import core.atomic : atomicStore;

    atomicStore(gParks, 0);
    atomicStore(gSpins, 0);
}

PoolStats copyCounters() @nogc nothrow
{
    import core.atomic : atomicLoad;

    PoolStats s;
    s.parks = atomicLoad(gParks);
    s.spins = atomicLoad(gSpins);
    return s;
}
