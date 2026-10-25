module host_attach_tests;

// A FiberDomain attached to a FarmHost drains through waitUntil, retires its
// records and terminal tasks during step(), and close() refuses while the
// domain is not drained. Attaching from a thread that is not the domain's
// Director is refused.

import ant_fibers;
import ant_fibers.host : attachFiberDomain;
import core.atomic : atomicLoad, atomicStore;
import core.thread : Thread;
import core.time : MonoTime, msecs, seconds;
import std.algorithm.searching : canFind;
import std.stdio : writeln;
import threadpool.host;

void hostAttachSmoke()
{
    FarmHostOptions options;
    options.workers = 0;
    options.waitTimeout = 10.seconds;
    auto host = new FarmHost(options);
    scope (exit) host.closeOrAbandon();
    FiberDomain foreign;
    auto other = new Thread({ foreign = new FiberDomain(host.farm); });
    other.start();
    other.join();
    bool foreignRefused;
    try
        attachFiberDomain(host, foreign);
    catch (Exception e)
        foreignRefused = e.msg.canFind("Director");
    assert(foreignRefused, "attach refuses a domain whose Director is another thread");

    size_t completed, recorded;
    FiberDomainOptions domainOptions;
    domainOptions.onCompletion = (FiberTask task) {
        assert(task.outcome == FiberOutcome.completed);
        ++completed;
    };
    domainOptions.onRecord = (FiberLifecycleEvent event) { ++recorded; };
    auto domain = new FiberDomain(host.farm, domainOptions);
    attachFiberDomain(host, domain);

    shared int woke;
    domain.spawn({
        FiberDomain.sleepUntil(MonoTime.currTime + 20.msecs);
        atomicStore(woke, 1);
    });
    foreach (_; 0 .. 8) domain.spawn({});
    bool refused;
    try
        host.close();
    catch (FarmHostException e)
        refused = e.msg.canFind("fiber domain not drained");
    assert(refused && host.state == HostState.open, "close refuses an undrained domain");

    // step() retires as tasks finish: no close() needed to bound them.
    host.waitUntil(() => domain.drained && !domain.retirementPending);
    assert(atomicLoad(woke) == 1);
    assert(completed == 9 && recorded == 18, "admitted + terminal per task");
    assert(domain.reservedRecords == 0);
    host.close();
    assert(host.state == HostState.closed);
    writeln("PASS FarmHost fiber domain attachment");
}
