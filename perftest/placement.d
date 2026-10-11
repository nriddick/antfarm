module placement;

import threadpool.topology : LogicalProcessor, ProcessorId, TopologySnapshot;
public import threadpool.pin : PinOutcome, pinProcessor;
import std.algorithm.sorting : sort;
import std.exception : enforce;
import core.stdc.stdio : printf;

/// One placed thread: the CPU to pin plus its physical group and core.
struct PlacedCpu
{
    ProcessorId id;
    ushort llc;
    ushort core;
}

struct Placement
{
    PlacedCpu[] consumers;
    PlacedCpu producer, mailbox;
    bool reused;
    /// The consumers, producer and mailbox span more than one physical group.
    bool crossesLlc;
}

// One thread per physical core first, faster classes first within each pass.
// Only after every core has a representative do we use SMT siblings.
// `llc >= 0` restricts the candidates to that physical group; the default
// ordering is unchanged so historical results stay comparable.
Placement choosePlacement(ref const TopologySnapshot topology, uint count, int llc = -1)
{
    auto processors = topology.processors.dup;
    sort!((a, b) => a.efficiencyClass != b.efficiencyClass
        ? a.efficiencyClass > b.efficiencyClass
        : (a.group != b.group ? a.group < b.group : a.lpIndex < b.lpIndex))(processors);
    PlacedCpu[] order;
    foreach (siblings; [false, true])
        foreach (ref p; processors)
        {
            if (!topology.isEligible(p)) continue;
            if (llc >= 0 && p.llcIndex != llc) continue;
            if (p.smtSibling == siblings)
                order ~= PlacedCpu(ProcessorId.of(p), p.llcIndex, p.coreIndex);
        }
    enforce(order.length != 0, "no logical processors discovered");
    Placement result;
    result.consumers = new PlacedCpu[count];
    foreach (i; 0 .. count) result.consumers[i] = order[i % order.length];
    result.producer = order[count % order.length];
    result.mailbox = order[(cast(size_t) count + 1) % order.length];
    result.reused = cast(size_t) count + 2 > order.length;
    foreach (c; result.consumers ~ [result.mailbox])
        if (c.llc != result.producer.llc)
            result.crossesLlc = true;
    return result;
}

void printPlacement(ref const Placement p)
{
    printf("placement nc=%u consumers(group:LP@llc)=", cast(uint) p.consumers.length);
    foreach (i, cpu; p.consumers)
        printf("%s%u:%u@llc%u", i ? ",".ptr : "".ptr, cast(uint) cpu.id.group,
            cast(uint) cpu.id.lp, cast(uint) cpu.llc);
    printf(" producer=%u:%u@llc%u mailbox=%u:%u@llc%u%s crossesLlc=%s\n",
        cast(uint) p.producer.id.group, cast(uint) p.producer.id.lp, cast(uint) p.producer.llc,
        cast(uint) p.mailbox.id.group, cast(uint) p.mailbox.id.lp, cast(uint) p.mailbox.llc,
        p.reused ? " (LP reuse)".ptr : "".ptr, p.crossesLlc ? "yes".ptr : "no".ptr);
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
    assert(p.consumers[0].id == ProcessorId(0, 0) && p.consumers[1].id == ProcessorId(0, 2));
    assert(p.consumers[1].core == 1);
    assert(p.producer.id == ProcessorId(1, 0));
    assert(p.mailbox.id == ProcessorId(0, 1));
    assert(!p.reused);
    assert(!p.crossesLlc);
    assert(choosePlacement(topology, 5).reused);
}

unittest
{
    // Two LLCs (5900XT-like, scaled down): LPs 0-3 on LLC 0 and 4-7 on LLC 1,
    // SMT sibling N+8. The default order crosses LLCs once LLC 0's cores are
    // used; the llc filter keeps every role inside one group.
    TopologySnapshot topology;
    foreach (i; 0 .. 16)
    {
        LogicalProcessor p;
        p.lpIndex = cast(ushort) i;
        p.coreIndex = cast(ushort)(i % 8);
        p.llcIndex = cast(ushort)((i % 8) / 4);
        p.smtSibling = i >= 8;
        topology.processors ~= p;
    }
    auto wide = choosePlacement(topology, 4);
    assert(wide.consumers[3].id == ProcessorId(0, 3) && wide.consumers[3].llc == 0);
    assert(wide.producer.id == ProcessorId(0, 4) && wide.producer.llc == 1);
    assert(wide.crossesLlc);

    auto one = choosePlacement(topology, 4, 1);
    foreach (c; one.consumers ~ [one.producer, one.mailbox])
        assert(c.llc == 1);
    assert(one.consumers[0].id == ProcessorId(0, 4));
    assert(one.producer.id == ProcessorId(0, 12), "SMT siblings follow the group's cores");
    assert(!one.crossesLlc);
}
