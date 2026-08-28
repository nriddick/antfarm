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
