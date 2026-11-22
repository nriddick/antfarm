/++
 + What each worker records about its own timing, and the sum a Director
 + reads.
 +
 + A worker writes its own record and nothing else does, so there is no
 + shared counter on any worker's path. `Selection.timing` reads the records
 + of the workers it matches while they run; each field is read whole, and
 + the fields of one worker may be a few events apart.
 +/
module threadpool.timing;

import core.atomic : MemoryOrder, atomicLoad, atomicStore;
import core.time : MonoTime;

/// Buckets of `TimingStats.parkOvershoot`.
enum parkOvershootBuckets = 9;
/// Upper bounds, in microseconds, of the first eight park-overshoot buckets;
/// the ninth holds everything at or over the last bound.
static immutable uint[parkOvershootBuckets - 1] parkOvershootBoundsMicros =
    [100, 250, 500, 750, 1_000, 1_500, 2_000, 5_000];

/// Buckets of `TimingStats.burstArrival`.
enum burstArrivalBuckets = 8;
/// Bucket 0 is work found before the expected instant. Buckets 1 to 6 have
/// these upper bounds, in microseconds after the instant; bucket 7 holds
/// everything at or over the last.
static immutable uint[burstArrivalBuckets - 2] burstArrivalBoundsMicros =
    [50, 100, 250, 500, 1_000, 2_000];

/// What a selection's workers recorded, summed; each `worst` is the largest
/// over the workers. Durations are in microseconds. Counts run from the
/// pool's start or the last `Selection.resetTiming`.
struct TimingStats
{
    uint workers; /// workers summed

    // ---- readiness at the instants given to `expect` ----
    /// Windows a worker entered, or missed because its park ran past them. A
    /// worker that runs work from before a window's lead until after its
    /// hold records nothing for it.
    ulong windows;
    /// The instant passed with the worker polling, and it looked within
    /// 100 µs of it.
    ulong ready;
    /// The instant passed while the worker was running work it had found
    /// before it.
    ulong busy;
    /// The park before the window returned more than 100 µs after the
    /// instant: the timer fired late, or the thread then waited for a
    /// processor. The two are not told apart.
    ulong parkedLate;
    /// Of those, the park returned after the window had closed, so the
    /// worker was never ready for that instant.
    ulong missedWindows;
    ulong parkedLate1ms;           /// of those, more than 1 ms after
    ulong worstParkedLateMicros;   /// the latest such return
    /// The worker was polling before the instant and next looked more than
    /// 100 µs after it: it lost its processor.
    ulong spinningLate;
    ulong spinningLate1ms;         /// of those, more than 1 ms after
    ulong worstSpinningLateMicros; /// the latest such look
    /// Windows that ended without any work found: the hold ran out, or
    /// nothing was published.
    ulong emptyWindows;
    /// The first work found in a window, by how long after the expected
    /// instant (`burstArrivalBoundsMicros`). For choosing the hold.
    ulong[burstArrivalBuckets] burstArrival;

    // ---- parks ----
    ulong parks;      /// parks without a deadline
    ulong timedParks; /// parks to a deadline: the lead before a window, or a managed deadline
    /// Deadline parks that ran to their deadline, by how far past it they
    /// returned (`parkOvershootBoundsMicros`). For choosing the lead.
    ulong[parkOvershootBuckets] parkOvershoot;
    ulong worstParkOvershootMicros; /// the latest such return

    // ---- workers under the `spin` policy ----
    /// Gaps of more than 100 µs between two idle polls with no work between
    /// them. Sampled about every 256 PAUSEs, and not at all when
    /// `spinIters` is over 1,024.
    ulong stalls;
    ulong stalls1ms;        /// of those, more than 1 ms
    ulong worstStallMicros; /// the longest
}

/// One worker's record. Its worker is the only writer; every field is a whole
/// word, written and read with relaxed atomics, so a reader on another thread
/// sees each one intact.
package struct WorkerTiming
{
    // The records sit side by side in one array. A line of padding at each
    // end keeps one worker's writes off the next worker's line whatever the
    // array's alignment.
    private ubyte[64] padBefore;
    shared ulong windows, ready, busy;
    shared ulong parkedLate, parkedLate1ms, worstParkedLate;      // worst in ticks
    shared ulong missedWindows;
    shared ulong spinningLate, spinningLate1ms, worstSpinningLate;
    shared ulong emptyWindows;
    shared ulong[burstArrivalBuckets] burstArrival;
    shared ulong parks, timedParks;
    shared ulong[parkOvershootBuckets] parkOvershoot;
    shared ulong worstParkOvershoot;
    shared ulong stalls, stalls1ms, worstStall;
    private ubyte[64] padAfter;

    /// The owning worker: zero every count.
    void clear() @nogc nothrow
    {
        static foreach (name; ["windows", "ready", "busy", "parkedLate", "parkedLate1ms",
            "worstParkedLate", "missedWindows", "spinningLate", "spinningLate1ms", "worstSpinningLate",
            "emptyWindows", "parks", "timedParks", "worstParkOvershoot", "stalls", "stalls1ms",
            "worstStall"])
            atomicStore!(MemoryOrder.raw)(__traits(getMember, this, name), 0UL);
        foreach (ref b; burstArrival) atomicStore!(MemoryOrder.raw)(b, 0UL);
        foreach (ref b; parkOvershoot) atomicStore!(MemoryOrder.raw)(b, 0UL);
    }

    /// Any thread: add this record to `sum`.
    void addTo(ref TimingStats sum) @nogc nothrow
    {
        static ulong get(ref shared ulong f) @nogc nothrow { return atomicLoad!(MemoryOrder.raw)(f); }
        static void most(ref ulong worst, ulong ticks) @nogc nothrow
        {
            immutable us = ticksToMicros(ticks);
            if (us > worst) worst = us;
        }
        sum.workers++;
        sum.windows += get(windows);
        sum.ready += get(ready);
        sum.busy += get(busy);
        sum.parkedLate += get(parkedLate);
        sum.parkedLate1ms += get(parkedLate1ms);
        most(sum.worstParkedLateMicros, get(worstParkedLate));
        sum.missedWindows += get(missedWindows);
        sum.spinningLate += get(spinningLate);
        sum.spinningLate1ms += get(spinningLate1ms);
        most(sum.worstSpinningLateMicros, get(worstSpinningLate));
        sum.emptyWindows += get(emptyWindows);
        foreach (i, ref b; burstArrival) sum.burstArrival[i] += get(b);
        sum.parks += get(parks);
        sum.timedParks += get(timedParks);
        foreach (i, ref b; parkOvershoot) sum.parkOvershoot[i] += get(b);
        most(sum.worstParkOvershootMicros, get(worstParkOvershoot));
        sum.stalls += get(stalls);
        sum.stalls1ms += get(stalls1ms);
        most(sum.worstStallMicros, get(worstStall));
    }
}

/// One per worker, index == workerIndex. Sized by `CacheAwarePool.start`.
package __gshared WorkerTiming[] gTiming;

/// The owning worker: add one.
pragma(inline, true)
package void count(ref shared ulong field) @nogc nothrow
{
    atomicStore!(MemoryOrder.raw)(field, atomicLoad!(MemoryOrder.raw)(field) + 1);
}

/// The owning worker: count a lateness of `ticks` past 100 µs, and past 1 ms,
/// and keep the worst. Returns false, counting nothing, at 100 µs or under.
package bool countLate(ref shared ulong over100, ref shared ulong over1ms, ref shared ulong worst,
    long ticks) @nogc nothrow
{
    if (ticks <= microsToTicks(100)) return false;
    count(over100);
    if (ticks > microsToTicks(1_000)) count(over1ms);
    if (cast(ulong) ticks > atomicLoad!(MemoryOrder.raw)(worst))
        atomicStore!(MemoryOrder.raw)(worst, cast(ulong) ticks);
    return true;
}

/// The owning worker: a deadline park returned `ticks` past its deadline.
package void countOvershoot(ref WorkerTiming t, long ticks) @nogc nothrow
{
    count(t.parkOvershoot[overshootBucket(ticksToMicros(ticks))]);
    if (cast(ulong) ticks > atomicLoad!(MemoryOrder.raw)(t.worstParkOvershoot))
        atomicStore!(MemoryOrder.raw)(t.worstParkOvershoot, cast(ulong) ticks);
}

/// The owning worker: a window's first work was found `ticks` after its
/// instant (negative: before it).
package void countArrival(ref WorkerTiming t, long ticks) @nogc nothrow
{
    count(t.burstArrival[arrivalBucket(ticks < 0, ticksToMicros(ticks < 0 ? 0 : ticks))]);
}

/// The park-overshoot bucket of a lateness in microseconds.
size_t overshootBucket(ulong micros) @nogc nothrow pure @safe
{
    foreach (i, bound; parkOvershootBoundsMicros)
        if (micros < bound) return i;
    return parkOvershootBuckets - 1;
}

/// The burst-arrival bucket of work found `micros` after the instant, or
/// before it when `early`.
size_t arrivalBucket(bool early, ulong micros) @nogc nothrow pure @safe
{
    if (early) return 0;
    foreach (i, bound; burstArrivalBoundsMicros)
        if (micros < bound) return i + 1;
    return burstArrivalBuckets - 1;
}

/// Whole microseconds in `ticks` of `MonoTime`.
ulong ticksToMicros(ulong ticks) @nogc nothrow
{
    immutable tps = cast(ulong) MonoTime.ticksPerSecond;
    return ticks / tps * 1_000_000 + ticks % tps * 1_000_000 / tps;
}

/// `MonoTime` ticks in `micros` microseconds.
long microsToTicks(ulong micros) @nogc nothrow
{
    immutable tps = cast(ulong) MonoTime.ticksPerSecond;
    return cast(long)(micros / 1_000_000 * tps + micros % 1_000_000 * tps / 1_000_000);
}

unittest
{
    assert(overshootBucket(0) == 0 && overshootBucket(99) == 0 && overshootBucket(100) == 1);
    assert(overshootBucket(999) == 4 && overshootBucket(1_000) == 5 && overshootBucket(4_999) == 7);
    assert(overshootBucket(5_000) == 8 && overshootBucket(ulong.max) == 8);
    assert(arrivalBucket(true, 0) == 0 && arrivalBucket(false, 0) == 1 && arrivalBucket(false, 49) == 1);
    assert(arrivalBucket(false, 50) == 2 && arrivalBucket(false, 1_999) == 6);
    assert(arrivalBucket(false, 2_000) == 7 && arrivalBucket(false, ulong.max) == 7);
    assert(ticksToMicros(cast(ulong) microsToTicks(1_500)) == 1_500);
    assert(microsToTicks(0) == 0 && ticksToMicros(0) == 0);

    WorkerTiming t;
    count(t.windows);
    count(t.windows);
    assert(!countLate(t.parkedLate, t.parkedLate1ms, t.worstParkedLate, microsToTicks(100)));
    assert(countLate(t.parkedLate, t.parkedLate1ms, t.worstParkedLate, microsToTicks(300)));
    assert(countLate(t.parkedLate, t.parkedLate1ms, t.worstParkedLate, microsToTicks(2_500)));
    countOvershoot(t, microsToTicks(600));
    countOvershoot(t, microsToTicks(7_000));
    countArrival(t, -5);
    countArrival(t, microsToTicks(120));
    TimingStats sum;
    t.addTo(sum);
    t.addTo(sum);
    assert(sum.workers == 2 && sum.windows == 4 && sum.parkedLate == 4 && sum.parkedLate1ms == 2);
    assert(sum.worstParkedLateMicros == 2_500, "the worst is a maximum, not a sum");
    assert(sum.parkOvershoot[3] == 2 && sum.parkOvershoot[8] == 2 && sum.worstParkOvershootMicros == 7_000);
    assert(sum.burstArrival[0] == 2 && sum.burstArrival[3] == 2);
    t.clear();
    TimingStats empty;
    t.addTo(empty);
    assert(empty.windows == 0 && empty.parkOvershoot[3] == 0 && empty.worstParkedLateMicros == 0);
}
