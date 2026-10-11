module placement;

import threadpool.topology : LogicalProcessor, ProcessorId, TopologySnapshot;
public import threadpool.pin : PinOutcome, pinProcessor;
import std.algorithm.sorting : sort;
import std.exception : enforce;
import core.stdc.stdio : printf;

struct Placement
{
    ProcessorId[] consumers;
    ProcessorId producer, mailbox;
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
    ProcessorId[] order;
    foreach (siblings; [false, true])
        foreach (ref p; processors)
        {
            if (!topology.isEligible(p)) continue;
            if (p.smtSibling == siblings)
                order ~= ProcessorId.of(p);
        }
    enforce(order.length != 0, "no logical processors discovered");
    Placement result;
    result.consumers = new ProcessorId[count];
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
        printf("%s%u:%u", i ? ",".ptr : "".ptr, cast(uint) cpu.group, cast(uint) cpu.lp);
    printf(" producer=%u:%u mailbox=%u:%u%s\n", cast(uint) p.producer.group,
        cast(uint) p.producer.lp, cast(uint) p.mailbox.group,
        cast(uint) p.mailbox.lp, p.reused ? " (LP reuse)".ptr : "".ptr);
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
    assert(p.consumers == [ProcessorId(0, 0), ProcessorId(0, 2)]);
    assert(p.producer == ProcessorId(1, 0));
    assert(p.mailbox == ProcessorId(0, 1));
    assert(!p.reused);
    assert(choosePlacement(topology, 5).reused);
}
