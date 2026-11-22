module expect_live;

// Live readiness test (`dub run :threadpool -c expect-test`): a publisher
// thread tells some of a pool's workers when each burst is due
// (`Selection.expect`) and publishes it without a wake.
//
// It asserts structure, not timing: the selected workers enter a window per
// instant and find the bursts, the others enter none, a publisher that stops
// leaves the workers parked, and the counts add up. The lateness each
// placement measured is printed, not judged: it depends on the machine and
// on what else it is doing.

import core.atomic;
import core.thread : Thread;
import core.time : Duration, MonoTime, msecs, seconds;
import std.algorithm : sum;
import std.exception : enforce;
import std.stdio : writefln, writeln;

import threadpool;

shared long gPending;
shared ulong gDone;

/// Take one unit of the burst, if any is left.
bool takeOne(WorkerSelf*) nothrow @nogc
{
    if (atomicLoad!(MemoryOrder.raw)(gPending) <= 0) return false;
    if (atomicFetchSub(gPending, 1) > 0)
    {
        atomicFetchAdd(gDone, 1);
        return true;
    }
    atomicFetchAdd(gPending, 1);
    return false;
}

/// Spin to an instant on `monoNanos`. The publisher holds a processor: a
/// sleep here would end on the system timer tick.
void spinUntil(long atNanos) nothrow @nogc
{
    while (monoNanos() < atNanos) pause();
}

void await(scope bool delegate() done, string what)
{
    immutable deadline = MonoTime.currTime + 10.seconds;
    while (!done())
    {
        enforce(MonoTime.currTime < deadline, "timed out: " ~ what);
        Thread.sleep(1.msecs);
    }
}

void report(string name, TimingStats t, ulong beats)
{
    writefln("  %s: %s windows over %s beats: ready %s, busy %s, parked late %s (%s over 1 ms, %s missed, "
        ~ "worst %s us), spinning late %s (%s over 1 ms, worst %s us), empty %s", name, t.windows, beats,
        t.ready, t.busy, t.parkedLate, t.parkedLate1ms, t.missedWindows, t.worstParkedLateMicros,
        t.spinningLate, t.spinningLate1ms, t.worstSpinningLateMicros, t.emptyWindows);
    writefln("    park overshoot, us, under %s and over: %s; worst %s us; %s deadline parks, %s plain",
        parkOvershootBoundsMicros, t.parkOvershoot, t.worstParkOvershootMicros, t.timedParks, t.parks);
    writefln("    first work of a window, us after the instant: before it, under %s and over: %s",
        burstArrivalBoundsMicros, t.burstArrival);
}

/// One placement: `seconds_` of bursts at 240 Hz for `readyCount` workers.
/// `repoll` is the base policy: `Duration.max` for `wait`, otherwise
/// `sleep(repoll)`, the bound hosts run with (`idleTimeout`, 1 ms by
/// default), which cuts each park before a window into shorter ones.
void run(WorkerPlacement placement, uint leadMicros, double seconds_, Duration repoll)
{
    enum long period = 4_166_667; // 240 Hz in nanoseconds
    atomicStore(gPending, 0L);
    atomicStore(gDone, 0UL);
    PoolOptions options;
    options.placement = placement;
    options.skipSmtSiblings = true;
    options.workerBody = &takeOne;
    auto pool = new CacheAwarePool(options);
    pool.start();
    scope (exit) pool.shutdown();
    auto director = pool.director();
    if (repoll != Duration.max) director.sleep(repoll);
    immutable workers = pool.workerCount;
    immutable readyCount = workers >= 5 ? 4 : workers > 1 ? workers - 1 : 1;
    auto ready = director.take(readyCount);
    auto rest = director.skip(readyCount);
    enforce(ready.count == readyCount && rest.count == workers - readyCount, "take and skip split the pool");
    ready.readiness(leadMicros, defaultHoldMicros);

    // Parked until told otherwise: no window, no deadline park.
    Thread.sleep(50.msecs);
    enforce(director.timing().windows == 0 && director.timing().timedParks == 0,
        "workers with nothing to expect entered a window");

    immutable beats = cast(ulong)(seconds_ * 240);
    long at = monoNanos() + 20_000_000;
    ready.expect(at);
    foreach (beat; 0 .. beats)
    {
        spinUntil(at);
        atomicStore(gPending, cast(long) readyCount); // the burst, with no wake
        at += period;
        ready.expect(at); // when the next is due
    }
    // The publisher stops. Each worker finishes its window and parks.
    spinUntil(at + 2_000_000);
    ready.expectNothing();
    await(() {
        if (atomicLoad(gPending) <= 0) return true;
        pool.wakeAll();
        return false;
    }, "the last burst");

    auto t = ready.timing();
    writefln("placement %s, lead %s us, %s: %s workers, %s ready", pool.resolution.placement, leadMicros,
        repoll == Duration.max ? "wait" : "sleep(1 ms)", workers, readyCount);
    report("ready workers", t, beats);

    // Structure.
    enforce(atomicLoad(gDone) == beats * readyCount, "every unit of every burst ran once");
    enforce(t.workers == readyCount, "timing sums the selection");
    enforce(t.windows >= beats * readyCount / 2 && t.windows <= (beats + 1) * readyCount,
        "the ready workers entered about one window per instant");
    enforce(t.ready + t.busy + t.parkedLate + t.spinningLate <= t.windows
        && t.ready + t.busy + t.parkedLate + t.spinningLate + readyCount >= t.windows,
        "each window's instant is accounted for once");
    enforce(t.burstArrival[].sum + t.emptyWindows + t.missedWindows <= t.windows
        && t.burstArrival[].sum + t.emptyWindows + t.missedWindows + readyCount >= t.windows,
        "each window found work, ended empty or was missed");
    enforce(t.missedWindows <= t.parkedLate && t.busy <= t.windows);
    enforce(t.burstArrival[].sum > 0, "the ready workers found bursts inside their windows");
    enforce(t.timedParks >= t.windows / 2, "the ready workers parked to the lead before their windows");
    // Under `wait` a park runs to its lead. Under `sleep` the re-poll bound
    // may end every one of them first, on the system timer tick.
    enforce(repoll != Duration.max || t.parkOvershoot[].sum > 0, "no park ran to its lead");
    enforce(t.parkOvershoot[].sum <= t.timedParks, "a deadline park is counted before its overshoot");
    enforce(t.parkedLate1ms <= t.parkedLate && t.spinningLate1ms <= t.spinningLate);
    if (workers > readyCount)
    {
        auto others = rest.timing();
        enforce(others.windows == 0 && others.timedParks == 0 && others.workers == workers - readyCount,
            "workers that were told nothing entered no window");
    }

    // With nothing expected the workers come to rest, and stay parked. A
    // park that the OS brings back very late is still in flight for a
    // while, so wait for the counts to stop before holding them still.
    TimingStats later;
    await(() {
        immutable settled = ready.timing();
        Thread.sleep(60.msecs);
        later = ready.timing();
        return later.windows == settled.windows && later.timedParks == settled.timedParks;
    }, "workers went on waking after the publisher stopped");

    // A lead and a hold of nothing: an instant opens no window.
    ready.readiness(0, 0);
    ready.expect(monoNanos() + 5_000_000);
    Thread.sleep(30.msecs);
    enforce(ready.timing().windows == later.windows, "an empty window was entered");
    ready.expectNothing();

    // reset: each worker clears its own record the next time it writes it.
    ready.readiness(leadMicros, defaultHoldMicros);
    ready.resetTiming();
    at = monoNanos() + 10_000_000;
    foreach (beat; 0 .. 12)
    {
        ready.expect(at);
        spinUntil(at);
        at += period;
    }
    spinUntil(at);
    ready.expectNothing();
    Thread.sleep(20.msecs);
    immutable fresh = ready.timing();
    enforce(fresh.windows > 0 && fresh.windows <= 14 * readyCount, "the records started again from zero");

    // A worker under `spin` polls all the time, ignores `expect`, and samples its stalls.
    director.take(1).spin();
    immutable parksBefore = director.take(1).timing().parks + director.take(1).timing().timedParks;
    director.take(1).expect(monoNanos() + 1_000_000);
    Thread.sleep(100.msecs);
    auto spinner = director.take(1).timing();
    enforce(spinner.parks + spinner.timedParks == parksBefore, "a spinning worker parked");
    writefln("  one worker under spin for 100 ms: %s stalls over 100 us, %s over 1 ms, worst %s us",
        spinner.stalls, spinner.stalls1ms, spinner.worstStallMicros);
    if (repoll == Duration.max) director.take(1).wait();
    else director.take(1).sleep(repoll);
}

int main(string[] args)
{
    import std.conv : to;

    immutable seconds_ = args.length > 1 ? args[1].to!double : 2.0;
    immutable lead = args.length > 2 ? args[2].to!uint : defaultLeadMicros;
    auto snap = CacheAwarePool.topology();
    size_t eligible;
    foreach (ref p; snap.processors)
        if (snap.isEligible(p)) eligible++;
    if (eligible < 3)
    {
        writeln("SKIP expect-test: needs three processors (a publisher and two workers)");
        return 0;
    }
    // `reverse` runs the placements in the other order, for a measurement
    // on a machine that slows down as it warms up.
    // Each placement by name: `automatic` is `group` on a machine with
    // several LLC groups, which would leave `free` unrun there.
    auto placements = [WorkerPlacement.free, WorkerPlacement.group, WorkerPlacement.pin];
    if (args.length > 3 && args[3] == "reverse")
        placements = [WorkerPlacement.pin, WorkerPlacement.group, WorkerPlacement.free];
    foreach (placement; placements)
        run(placement, lead, seconds_, Duration.max);
    // The hosts' default policy, once: a 1 ms re-poll bound under the lead.
    run(WorkerPlacement.automatic, lead, seconds_ < 1 ? seconds_ : 1, 1.msecs);
    writeln("PASS expect-test: windows, parks and counts hold under every placement, and under sleep");
    return 0;
}
