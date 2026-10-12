module host_attach_tests;

// A FiberDomain attached to a FarmHost drains through waitUntil, and close()
// refuses while the domain is not drained.

import antfarm_fibers;
import antfarm_fibers.host : attachFiberDomain;
import core.atomic : atomicLoad, atomicStore;
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
    scope (exit) host.close();
    scope (failure) host.abandon();
    auto domain = new FiberDomain(host.farm);
    attachFiberDomain(host, domain);

    shared int woke;
    domain.spawn({
        FiberDomain.sleepUntil(MonoTime.currTime + 20.msecs);
        atomicStore(woke, 1);
    });
    bool refused;
    try
        host.close();
    catch (FarmHostException e)
        refused = e.msg.canFind("fiber domain not drained");
    assert(refused && host.state == HostState.open, "close refuses an undrained domain");

    host.waitUntil(() => domain.drained);
    assert(atomicLoad(woke) == 1);
    host.close();
    assert(host.state == HostState.closed);
    writeln("PASS FarmHost fiber domain attachment");
}
