module threadpool.stats;

struct PoolStats
{
    ulong parksP, parksE, spinsP, spinsE;
}

/// Process-wide counters mutated by workers (@nogc). Snapshot copies them out.
shared ulong gParksP;
shared ulong gParksE;
shared ulong gSpinsP;
shared ulong gSpinsE;

void resetStats() @nogc nothrow
{
    import core.atomic : atomicStore;

    atomicStore(gParksP, 0);
    atomicStore(gParksE, 0);
    atomicStore(gSpinsP, 0);
    atomicStore(gSpinsE, 0);
}

PoolStats copyCounters() @nogc nothrow
{
    import core.atomic : atomicLoad;

    PoolStats s;
    s.parksP = atomicLoad(gParksP);
    s.parksE = atomicLoad(gParksE);
    s.spinsP = atomicLoad(gSpinsP);
    s.spinsE = atomicLoad(gSpinsE);
    return s;
}
