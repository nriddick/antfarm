module ant_fibers.group_host_tests;

import antfarm;
import ant_fibers.groups;
import ant_fibers.scheduler;
import core.atomic;
import core.thread : Thread;
import core.time : Duration, MonoTime, msecs, seconds;
import std.exception : enforce;
import std.stdio : writeln;
import threadpool.bins : home;
import threadpool.groups : GroupAttachment, GroupHost, GroupHostOptions, GroupSlot;
import threadpool.host : HostGroups;
import threadpool.hybrid : WorkerSelf;

/// Fibers on a GroupHost through the managed lane: the host consumes the
/// group Farms, every worker serves its home lane, parked workers wake for
/// timer deadlines (the pool parks until woken, so only `until` can end a
/// park) and for signals from the Director, the Director retires under
/// `director`, and workers retire under `serviceThreads`. Then the same
/// domain beside a consuming `@nogc` attachment, whose view resumes the
/// Fibers.
void groupHostFiberSmoke()
{
    GroupHostOptions probe;
    probe.groups = HostGroups.all();
    if (GroupHost.resolve(probe).workers.length == 0)
    {
        writeln("SKIP group host fibers: no pool workers");
        return;
    }
    directorRetirement();
    serviceThreadRetirement();
    besideConsumingAttachment();
    smallRingBurst();
}

private GroupHostOptions hostOptions()
{
    GroupHostOptions o;
    o.groups = HostGroups.all();
    o.farmPerGroup = GroupHost.resolve(o).groups.length > 1;
    o.idleTimeout = Duration.max; // park until woken: deadlines must wake
    o.farm.ringMiB = 2;
    return o;
}

private void waitFor(scope bool delegate() done, string what)
{
    immutable deadline = MonoTime.currTime + 20.seconds;
    while (!done())
    {
        enforce(MonoTime.currTime < deadline, "group host fibers: " ~ what ~ " stalled");
        Thread.sleep(1.msecs);
    }
}

private void directorRetirement()
{
    shared int ran, lateRan;
    size_t completed;
    FiberDomainOptions options;
    options.onCompletion = (FiberTask task) {
        enforce(task.outcome == FiberOutcome.completed);
        ++completed;
    };
    auto domain = new FiberDomain(options);
    auto fibers = new FiberGroups(domain);
    auto o = hostOptions();
    o.attachments = [fibers.attachment];
    auto host = new GroupHost(o);
    scope (exit) host.close();
    enforce(host.managedLane, "a Fiber attachment selects the managed lane");
    enforce(fibers.lanes.length == host.farms.length, "one lane per host Farm");

    // Spread over every group; each parks on a timer, so completion needs
    // the deadline wake.
    enum count = 200;
    foreach (i; 0 .. count)
    {
        immutable llc = host.groups[i % host.groups.length];
        fibers.spawnOn(llc, { FiberDomain.sleepFor(5.msecs); atomicOp!"+="(ran, 1); });
    }
    // A long sleep after everything else is idle: no spawn or signal wake
    // remains, only the `until` deadline.
    fibers.spawn({ FiberDomain.sleepFor(80.msecs); atomicOp!"+="(lateRan, 1); });
    // A parked waiter woken by the Director through the lane's wake.
    auto event = new FiberEvent(domain);
    shared int eventRan;
    fibers.spawn({ event.wait(); atomicOp!"+="(eventRan, 1); });
    waitFor(() => atomicLoad(ran) == count, "timer sleepers");
    enforce(atomicLoad(eventRan) == 0 && !domain.drained);
    Thread.sleep(20.msecs);
    event.set();
    waitFor(() => domain.drained, "the late sleeper and the event waiter");
    enforce(atomicLoad(lateRan) == 1 && atomicLoad(eventRan) == 1);
    // Workers may not retire under `director`; the Director does.
    enforce(domain.retirementPending && completed == 0);
    enforce(domain.retire() == count + 2 && completed == count + 2);
    domain.beginShutdown();
    writeln("PASS group host fibers: ", host.workers, " workers, ", host.farms.length,
        " Farm(s), timer and signal wakes, Director retirement");
}

private void serviceThreadRetirement()
{
    shared size_t completed;
    FiberDomainOptions options;
    options.retirement = FiberRetirement.serviceThreads;
    options.onCompletion = (FiberTask task) { atomicOp!"+="(completed, 1); };
    auto domain = new FiberDomain(options);
    auto fibers = new FiberGroups(domain);
    auto o = hostOptions();
    o.attachments = [fibers.attachment];
    auto host = new GroupHost(o);
    scope (exit) host.close();

    enum count = 300;
    foreach (i; 0 .. count)
        fibers.spawnHandleOn(host.groups[i % host.groups.length], {
            FiberDomain.yieldReady();
            FiberDomain.sleepFor(1.msecs);
        });
    // The Director never retires: the workers' service passes do.
    waitFor(() => domain.drained && !domain.retirementPending, "worker retirement");
    enforce(atomicLoad(completed) == count && domain.reservedRecords == 0);
    domain.beginShutdown();
    writeln("PASS group host fibers: serviceThreads retirement on ", host.workers, " workers");
}

/// A burst larger than the ring on the smallest Farm, workers parked until
/// woken. A worker whose flush the full ring refuses wakes its peers (whose
/// idle views pin reclamation) and a written batch chain-wakes a sleeper;
/// without those wakes every worker parked with the ring full and the
/// burst never drained (found with a 1 MiB ring in the warm-burst
/// benchmark, 2.1.1).
private void smallRingBurst()
{
    enum count = 200_000; // about 6,250 tables of 32 one-word bodies, many laps of 128 Ki words
    FiberDomainOptions options;
    options.recordRetention = 4 * count + 4; // the whole burst is live before the Director retires
    auto domain = new FiberDomain(options);
    auto fibers = new FiberGroups(domain);
    auto o = hostOptions();
    o.farm.ringMiB = 1;
    o.farmPerGroup = false;
    o.farm.quotaSmall = 2048;
    o.pool.skipSmtSiblings = false; // every logical CPU: more flushers than the ring holds
    o.attachments = [fibers.attachment];
    auto host = new GroupHost(o);
    scope (exit) host.close();
    shared int ran;
    foreach (i; 0 .. count)
        domain.spawnOn(fibers.lanes[0], { atomicOp!"+="(ran, 1); });
    host.wakeAll();
    waitFor(() => domain.drained, "a burst larger than a 1 MiB ring");
    enforce(atomicLoad(ran) == count);
    domain.retire();
    domain.beginShutdown();
    writeln("PASS group host fibers: ", count, " tasks through a 1 MiB ring with parked workers");
}

private shared ulong consumedByAttachment;
private ConsumerView tView;
private bool tSubscribed;

private bool consumingBody(WorkerSelf*) nothrow @nogc @system
{
    if (!tSubscribed)
    {
        auto slot = home!GroupSlot();
        if (slot is null || slot.farm is null) return false;
        if (tView.subscribe(slot.farm) < 0) return false;
        tSubscribed = true;
    }
    if (!tView.consumeNext()) return false;
    atomicOp!"+="(consumedByAttachment, 1UL);
    return true;
}

private void consumingStop(WorkerSelf*) nothrow @nogc @system
{
    if (tSubscribed) { tView.unsubscribe(); tSubscribed = false; }
}

/// A consuming `@nogc` attachment (the fork-join dispatcher's shape) owns the
/// workers' views; Fiber activations are Farm payloads, so its view resumes
/// them and the host subscribes no view of its own.
private void besideConsumingAttachment()
{
    shared int ran;
    auto domain = new FiberDomain;
    auto fibers = new FiberGroups(domain);
    auto o = hostOptions();
    o.attachments = [GroupAttachment("consumer", &consumingBody, &consumingStop, true),
                     fibers.attachment];
    atomicStore(consumedByAttachment, 0UL);
    auto host = new GroupHost(o);
    scope (exit) host.close();
    enforce(host.managedLane);
    enum count = 100;
    foreach (i; 0 .. count)
        fibers.spawnOn(host.groups[i % host.groups.length], {
            FiberDomain.yieldReady();
            atomicOp!"+="(ran, 1);
        });
    waitFor(() => domain.drained, "fibers beside a consuming attachment");
    enforce(atomicLoad(ran) == count);
    enforce(atomicLoad(consumedByAttachment) != 0, "the attachment's view consumed the activations");
    domain.retire();
    domain.beginShutdown();
    writeln("PASS group host fibers: resumed by a consuming attachment's view (",
        atomicLoad(consumedByAttachment), " tables)");
}
