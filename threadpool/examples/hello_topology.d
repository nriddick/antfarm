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
    writeln("os scope ", t.osScopeSource, "  allowed ",
        t.osScopeSource == OsScopeSource.unrestricted ? "all" : formatCpuList(t.osScopeAllowed),
        "  grouping source ", t.groupingSource);

    foreach (ref d; t.llcDomains)
    {
        writefln("  LLC %s  source %s  size %s KiB  line %s  CPUs %s  eligible %s  P-cores %s  E-cores %s",
            d.llcIndex, d.source, d.l3SizeBytes / 1024, d.lineSize,
            formatCpuList(d.processors), formatCpuList(t.eligibleProcessorsInLlc(d.llcIndex)),
            d.pCoreIndices, d.eCoreIndices);
    }

    writeln();
    writeln("cpu    core  llc  numa  class  smtSib  elig  offline  osExcl  parked  l2KiB  l3KiB  module");
    foreach (ref p; t.processors)
    {
        writefln("%-5s  %4s  %3s  %4s  %5s  %6s  %4s  %7s  %6s  %6s  %5s  %5s  %6s",
            ProcessorId.of(p), p.coreIndex, p.llcIndex, p.numaIndex,
            p.efficiencyClass, p.smtSibling, t.isEligible(p), p.offlineAtDiscovery,
            p.affinityExcluded, p.parkedAtDiscovery,
            p.l2.sizeBytes / 1024, p.l3.sizeBytes / 1024, p.moduleIndex);
    }

    writeln("\ndefault pool resolution:");
    writeln(new CacheAwarePool(PoolOptions.init).resolve());
    PoolOptions noSmt;
    noSmt.skipSmtSiblings = true;
    writeln("skipSmtSiblings pool resolution:");
    writeln(new CacheAwarePool(noSmt).resolve());

    if (auto here = t.current())
        writefln("this thread is on CPU %s, LLC %s, NUMA %s, efficiencyClass %s",
            ProcessorId.of(*here), here.llcIndex, here.numaIndex, here.efficiencyClass);
    writeln("hello_topology: discovered cache topology and compared worker selections");
}
