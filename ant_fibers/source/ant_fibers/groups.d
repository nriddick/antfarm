/++
 + Fibers on a `GroupHost`: one ready lane per group Farm, served by every
 + pool worker through the host's managed lane.
 +
 + `FiberGroups` builds the lanes in the attachment's `install` hook (after
 + the host created its Farms, before the pool starts) and serves the
 + worker's home lane in its `serve` hook: cancellation requests, timers,
 + generation triggers, flush with the worker's group token, retirement when
 + the worker may, and the earliest deadline, which the host parks until.
 + Fiber activations are Farm payloads, so whichever view consumes the group
 + Farm (the host's, or a consuming attachment's such as the fork-join
 + dispatcher) resumes them.
 +
 + The constructing thread of the domain is its Director; under the default
 + `FiberRetirement.director` it calls `domain.retire()` itself, since no
 + worker may. Drain the domain (`beginShutdown`, wait for `drained`, retire)
 + before `host.close()`.
 +/
module ant_fibers.groups;

import antfarm;
import ant_fibers.scheduler;
import core.atomic;
import core.thread : Thread;
import threadpool.groups : GroupAttachment, GroupHost, GroupServiceResult, groupToken;
import threadpool.hybrid : WorkerSelf;

/// A ready lane on one of a `GroupHost`'s Farms whose wakes reach the
/// workers that consume that Farm.
final class GroupFiberLane : FiberReadyLane
{
    private GroupHost host_;
    private ushort llc_;
    private bool shared_; // the one Farm every group consumes

    package this(FiberDomain domain, GroupHost host, ushort llc, bool sharedFarm)
    {
        super(host.farmOf(llc));
        host_ = host;
        llc_ = llc;
        shared_ = sharedFarm;
        domain.attachReadyLane(this);
    }

    /// The group whose workers consume this lane's Farm (the home group when
    /// one Farm is shared).
    @property ushort llc() const nothrow @nogc { return llc_; }

    /// Wake the workers that consume this lane's Farm.
    void wakeWorkers() nothrow @nogc
    {
        if (shared_) host_.wakeAll();
        else host_.wakeLlc(llc_);
    }

    protected override void noteExternalReady() nothrow @nogc { wakeWorkers(); }
    protected override void noteCancellationRequested() nothrow @nogc { wakeWorkers(); }
    // Every worker polls the domain's timers; one of them must leave its park
    // to adopt the new earliest deadline.
    protected override void noteTimerArmed() nothrow @nogc { host_.wakeAll(); }
}

/// The Fiber attachment of a `GroupHost`. Construct it with the domain, put
/// `attachment` in `GroupHostOptions.attachments`, construct the host, then
/// spawn.
final class FiberGroups
{
    private FiberDomain domain_;
    private GroupHost host_;
    private GroupFiberLane[] lanes_;      // one per host Farm, in `host.farms` order
    private GroupFiberLane[] laneByLlc_;  // indexed by LLC group
    private size_t flushBatch_;
    private uint avgCost_;

    /// `domain` must have no lane yet (`new FiberDomain(options)`); the lanes
    /// are built when the host installs this attachment.
    this(FiberDomain domain, size_t flushBatch = 32, uint avgCost = avgCostForChunk(8))
    {
        if (domain is null)
            throw new Exception("ant_fibers: FiberGroups requires a domain");
        if (flushBatch == 0)
            throw new Exception("ant_fibers: flushBatch must be greater than zero");
        domain_ = domain;
        flushBatch_ = flushBatch;
        avgCost_ = avgCost;
    }

    /// The attachment to place in `GroupHostOptions.attachments`.
    GroupAttachment attachment()
    {
        GroupAttachment a;
        a.name = "fibers";
        a.install = &install;
        a.uninstall = &uninstall;
        a.serve = &serve;
        return a;
    }

    /// The domain this attachment serves.
    @property FiberDomain domain() nothrow @nogc { return domain_; }
    /// The host, once it installed this attachment.
    @property GroupHost host() nothrow @nogc { return host_; }
    /// The lanes, one per host Farm, in `host.farms` order.
    @property GroupFiberLane[] lanes() nothrow @nogc { return lanes_; }

    /// The lane of group `llc`'s Farm; null for a group the host did not select.
    GroupFiberLane laneOf(ushort llc) nothrow @nogc
    {
        return llc < laneByLlc_.length ? laneByLlc_[llc] : null;
    }

    /// Admit a task on the caller's group lane (the host's `callerGroup`) and
    /// wake its workers.
    FiberTask spawn(void delegate() body, size_t stackSize = 0)
    {
        return spawnOn(host_.callerGroup, body, stackSize);
    }

    /// ditto
    FiberTask spawn(void function() body, size_t stackSize = 0)
    {
        return spawnOn(host_.callerGroup, body, stackSize);
    }

    /// Admit a task whose activations prefer group `llc`'s Farm.
    FiberTask spawnOn(Body)(ushort llc, Body body, size_t stackSize = 0)
        if (is(Body == void delegate()) || is(Body == void function()))
    {
        auto lane = requireLane(llc);
        auto task = domain_.spawnOn(lane, body, stackSize);
        lane.wakeWorkers();
        return task;
    }

    /// `spawn` returning the generation handle, the safe form under
    /// `FiberRetirement.serviceThreads`.
    TaskHandle spawnHandle(void delegate() body, size_t stackSize = 0)
    {
        return spawnHandleOn(host_.callerGroup, body, stackSize);
    }

    /// ditto
    TaskHandle spawnHandle(void function() body, size_t stackSize = 0)
    {
        return spawnHandleOn(host_.callerGroup, body, stackSize);
    }

    /// ditto, on group `llc`'s lane.
    TaskHandle spawnHandleOn(Body)(ushort llc, Body body, size_t stackSize = 0)
        if (is(Body == void delegate()) || is(Body == void function()))
    {
        auto lane = requireLane(llc);
        auto handle = domain_.spawnHandleOn(lane, body, stackSize);
        lane.wakeWorkers();
        return handle;
    }

    private GroupFiberLane requireLane(ushort llc)
    {
        if (host_ is null)
            throw new Exception("ant_fibers: FiberGroups is not installed on a host");
        auto lane = laneOf(llc);
        if (lane is null)
            throw new Exception("ant_fibers: group has no Farm on this host");
        return lane;
    }

    private void install(GroupHost host)
    {
        if (host_ !is null)
            throw new Exception("ant_fibers: FiberGroups is already installed");
        host_ = host;
        auto farms = host.farms;
        immutable sharedFarm = farms.length == 1;
        ushort maxLlc;
        foreach (llc; host.groups) if (llc > maxLlc) maxLlc = llc;
        laneByLlc_ = new GroupFiberLane[maxLlc + 1];
        foreach (llc; host.groups)
        {
            auto farm = host.farmOf(llc);
            GroupFiberLane lane;
            foreach (existing; lanes_)
                if (existing.farm is farm) lane = existing;
            if (lane is null)
            {
                lane = new GroupFiberLane(domain_, host, llc, sharedFarm);
                lanes_ ~= lane;
            }
            laneByLlc_[llc] = lane;
        }
    }

    private void uninstall(GroupHost host)
    {
        // The Farms die after this; the domain must already be drained.
        host_ = null;
    }

    private GroupServiceResult serve(WorkerSelf* worker)
    {
        auto lane = laneOf(worker.llcIndex);
        if (lane is null) return GroupServiceResult.init;
        auto r = lane.serve(groupToken(worker.llcIndex),
            FiberServicePass(true, false, flushBatch_, avgCost_));
        return GroupServiceResult(r.progress, r.deadline);
    }
}
