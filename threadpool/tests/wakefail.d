// wake() racing pool shutdown caused by a worker failure.
// Per round: FarmHost(workers=2, park); a waker thread spins host.wake() while
// the control thread triggers a worker failure through an asserting actor and
// observes FarmHostFailed from waitUntil/step (which runs failed()->stopConsumers()
// -> pool.shutdown()). Then the waker is joined BEFORE close()/abandon() and
// the claim must be reusable.
module wakefail;
import core.atomic;
import core.thread : Thread;
import core.time : Duration, seconds;
import std.conv : to;
import std.stdio : stderr, writeln;
import actors, antfarm, threadpool.host;

struct Ledger { long balance; }
void failing(scope ref ActorBorrow!Ledger, scope ref ActorContext) nothrow @nogc @system
{
    assert(false, "wakefail: injected actor failure");
}

shared bool gStop;
shared ulong gWakes;

int main(string[] args)
{
    size_t rounds = args.length > 1 ? to!size_t(args[1]) : 500;
    bool infinitePark = args.length > 2 && args[2] == "wait";
    uint wakers = args.length > 3 ? to!uint(args[3]) : 1;
    size_t failedOk, other, nonOpen;
    foreach (r; 0 .. rounds)
    {
        FarmHostOptions o; o.workers = 2; o.waitTimeout = 10.seconds;
        if (infinitePark) o.idleTimeout = Duration.max;
        auto host = new FarmHost(o);
        auto runtime = host.createRuntime(1);
        auto owner = runtime.createActor!(Ledger, failing)(Ledger(0));
        if (!owner.valid) { stderr.writeln("actor"); return 2; }
        atomicStore(gStop, false);
        Thread[] ts;
        foreach (w; 0 .. wakers)
        {
            auto t = new Thread({
                ulong n;
                while (!atomicLoad(gStop)) { host.wake(); ++n; }
                atomicOp!"+="(gWakes, n);
            });
            t.start();
            ts ~= t;
        }
        bool sawFailed;
        try
        {
            auto res = owner.handle.wake();
            host.waitUntil(() => false);
        }
        catch (FarmHostFailed e) { sawFailed = true; }
        catch (Throwable t) { stderr.writeln("round ", r, " unexpected: ", t.toString()); ++other; }
        atomicStore(gStop, true);
        foreach (t; ts) t.join();
        if (sawFailed) ++failedOk;
        if (host.state != HostState.failed) { ++nonOpen; }
        try host.close(); catch (Throwable t) { stderr.writeln("close: ", t.msg); ++other; }
    }
    // claim must be reusable
    try { auto h = new FarmHost(FarmHostOptions.init); h.close(); }
    catch (Throwable t) { stderr.writeln("final host: ", t.msg); ++other; }
    writeln("rounds=", rounds, " failed-observed=", failedOk, " state-not-failed=", nonOpen,
        " other=", other, " wake-calls=", atomicLoad(gWakes));
    return failedOk == rounds && other == 0 && nonOpen == 0 ? 0 : 1;
}
