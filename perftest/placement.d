module placement;

import threadpool.topology : LogicalProcessor, TopologySnapshot;
public import threadpool.pin : PinTarget, pinToLogicalProcessor;
import std.algorithm.sorting : sort;
import std.exception : enforce;
import core.stdc.stdio : printf;

struct Placement
{
    PinTarget[] consumers;
    PinTarget producer, mailbox;
    bool reused;
}

// One thread per physical core first, faster classes first within each pass.
// Only after every core has a representative do we use SMT siblings.
Placement choosePlacement(ref const TopologySnapshot topology, uint count)
{
    auto processors = topology.processors.dup;
    sort!((a, b) => a.efficiencyClass != b.efficiencyClass
        ? a.efficiencyClass > b.efficiencyClass
        : (a.group != b.group ? a.group < b.group : a.lpIndex < b.lpIndex))(processors);
    PinTarget[] order;
    foreach (siblings; [false, true])
        foreach (ref p; processors)
        {
            version (linux)
                if (p.parkedAtDiscovery) continue; // Outside this process's allowed CPUs.
            if (p.smtSibling == siblings)
                order ~= PinTarget(p.cpuSetId, p.group, p.lpIndex);
        }
    enforce(order.length != 0, "no logical processors discovered");
    Placement result;
    result.consumers = new PinTarget[count];
    foreach (i; 0 .. count) result.consumers[i] = order[i % order.length];
    result.producer = order[count % order.length];
    result.mailbox = order[(cast(size_t) count + 1) % order.length];
    result.reused = cast(size_t) count + 2 > order.length;
    return result;
}

void printPlacement(ref const Placement p)
{
    printf("placement nc=%u consumers(group:LP)=", cast(uint) p.consumers.length);
    foreach (i, cpu; p.consumers)
        printf("%s%u:%u", i ? ",".ptr : "".ptr, cast(uint) cpu.group, cast(uint) cpu.lpIndex);
    printf(" producer=%u:%u mailbox=%u:%u%s\n", cast(uint) p.producer.group,
        cast(uint) p.producer.lpIndex, cast(uint) p.mailbox.group,
        cast(uint) p.mailbox.lpIndex, p.reused ? " (LP reuse)".ptr : "".ptr);
}

unittest
{
    TopologySnapshot topology;
    // Two fast SMT cores and a slow core in another processor group.
    foreach (i; 0 .. 4)
    {
        LogicalProcessor p;
        p.lpIndex = cast(ushort) i;
        p.coreIndex = cast(ushort)(i / 2);
        p.efficiencyClass = 1;
        p.smtSibling = (i % 2) != 0;
        topology.processors ~= p;
    }
    LogicalProcessor slow;
    slow.group = 1;
    topology.processors ~= slow;
    auto p = choosePlacement(topology, 2);
    assert(p.consumers[0].lpIndex == 0 && p.consumers[1].lpIndex == 2);
    assert(p.producer.group == 1 && p.producer.lpIndex == 0);
    assert(p.mailbox.group == 0 && p.mailbox.lpIndex == 1);
    assert(!p.reused);
    assert(choosePlacement(topology, 5).reused);
}
