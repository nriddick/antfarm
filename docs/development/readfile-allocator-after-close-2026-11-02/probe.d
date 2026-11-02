// When does an ant_uv readFile call its allocator, relative to UvLoop.close
// returning? Built and run by run.sh in this directory; see README.md.
//
//   probe stall <path> [blocking]
//       One readFile, closed 300 ms after it was submitted, with closeGrace
//       50 ms and abandonAfter 100 ms. With `blocking`, the allocator waits
//       until close has returned, as a budget waits for the frees that
//       follow close.
//   probe zero <path> <n> <usecs> [spans]
//       n readFiles, closed <usecs> after the last submit, with closeGrace and
//       abandonAfter zero. Prints one summary line of key=value pairs; with
//       `spans`, also each allocator call still running when close returned.
module probe;

import ant_uv.loop;
import core.atomic;
import core.stdc.stdio : printf;
import core.stdc.stdlib : free;
import core.sys.posix.stdlib : posix_memalign;
import core.sys.posix.time : clock_gettime, nanosleep, timespec, CLOCK_MONOTONIC;
import core.thread : Thread;
import core.time : dur, msecs, Duration;
import std.conv : to;
import std.string : toStringz;

enum maxCalls = 256;
shared long[maxCalls] enterNs, exitNs; // per allocator call, CLOCK_MONOTONIC
shared uint calls, deallocs;
shared int gate; // the blocking allocator waits for it
__gshared bool blocking;

long nowNs() nothrow @nogc
{
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1_000_000_000L + ts.tv_nsec;
}

// Runs on libuv's auxiliary thread: atomics and libc, nothing of druntime.
void* probeAllocate(void*, size_t bytes, size_t alignment) nothrow @nogc @system
{
    immutable i = atomicFetchAdd(calls, 1);
    if (i < maxCalls) atomicStore(enterNs[i], nowNs());
    if (blocking)
        while (atomicLoad(gate) == 0)
        {
            auto ts = timespec(0, 1_000_000);
            nanosleep(&ts, null);
        }
    void* p;
    if (posix_memalign(&p, alignment < size_t.sizeof ? size_t.sizeof : alignment, bytes) != 0)
        p = null;
    if (i < maxCalls) atomicStore(exitNs[i], nowNs());
    return p;
}

void probeDeallocate(void*, void* memory, size_t, size_t) nothrow @nogc @system
{
    atomicFetchAdd(deallocs, 1);
    free(memory);
}

double msSince(long t, long origin) { return (t - origin) / 1e6; }
double usSince(long t, long origin) { return (t - origin) / 1e3; }

int stall(const(char)* path, bool block)
{
    blocking = block;
    UvLoopOptions o;
    o.allocator = ActorAllocator(null, &probeAllocate, &probeDeallocate);
    o.closeGrace = 50.msecs;
    o.abandonAfter = 100.msecs;
    auto loop = UvLoop.create(o);
    auto r = UvRequest.readFile(path);
    immutable submitted = nowNs();
    if (loop.submit(&r) != UvSubmit.accepted) { printf("FAIL submit\n"); return 1; }
    Thread.sleep(300.msecs); // the job reaches its stall
    immutable closeCall = nowNs();
    auto report = loop.close();
    immutable closeReturn = nowNs();
    atomicStore(gate, 1); // the frees after close: a waiting budget opens
    Thread.sleep(2500.msecs);
    printf("submitted at %.3f ms (CLOCK_MONOTONIC); close called at +%.1f ms, returned at +%.1f ms\n",
        submitted / 1e6, msSince(closeCall, submitted), msSince(closeReturn, submitted));
    printf("report: completed %llu, cancelled %llu, abandoned %llu, loopClose %d; request state %u, result %lld\n",
        report.completed, report.cancelled, report.abandoned, report.loopClose,
        cast(uint) r.state, r.result);
    immutable n = atomicLoad(calls);
    printf("allocate calls: %u\n", n);
    foreach (i; 0 .. n < maxCalls ? n : maxCalls)
    {
        immutable e = atomicLoad(enterNs[i]), x = atomicLoad(exitNs[i]);
        if (x == 0)
            printf("  #%u entered %+.1f ms relative to close returning, still running\n",
                i, msSince(e, closeReturn));
        else
            printf("  #%u entered %+.1f ms, returned %+.1f ms, relative to close returning\n",
                i, msSince(e, closeReturn), msSince(x, closeReturn));
    }
    printf("deallocate calls: %u\n", atomicLoad(deallocs));
    return 0;
}

int zero(const(char)* path, uint n, uint delayUs, bool spans)
{
    UvLoopOptions o;
    o.allocator = ActorAllocator(null, &probeAllocate, &probeDeallocate);
    o.closeGrace = Duration.zero;
    o.abandonAfter = Duration.zero;
    auto loop = UvLoop.create(o);
    auto rs = new UvRequest[n];
    foreach (ref r; rs)
    {
        r = UvRequest.readFile(path);
        if (loop.submit(&r) != UvSubmit.accepted) { printf("FAIL submit\n"); return 1; }
    }
    if (delayUs) Thread.sleep(dur!"usecs"(delayUs));
    immutable closeCall = nowNs();
    auto report = loop.close();
    immutable closeReturn = nowNs();
    Thread.sleep(200.msecs); // abandoned jobs leave the allocator
    immutable c = atomicLoad(calls);
    uint after, running;
    foreach (i; 0 .. c < maxCalls ? c : maxCalls)
    {
        immutable e = atomicLoad(enterNs[i]), x = atomicLoad(exitNs[i]);
        if (e > closeReturn) ++after;
        else if (x == 0 || x > closeReturn) ++running;
        if (spans && (x == 0 || x > closeReturn))
            printf("  call %u: entered %+.1f us, returned %+.1f us, relative to close returning; took %.1f us\n",
                i, usSince(e, closeReturn), x == 0 ? double.nan : usSince(x, closeReturn),
                x == 0 ? double.nan : (x - e) / 1e3);
    }
    printf("zero delay_us=%u close_us=%.1f completed=%llu cancelled=%llu abandoned=%llu allocate=%u started_after_close=%u running_at_close=%u deallocate=%u\n",
        delayUs, usSince(closeReturn, closeCall), report.completed, report.cancelled,
        report.abandoned, c, after, running, atomicLoad(deallocs));
    return 0;
}

int main(string[] args)
{
    if (args.length >= 3 && args[1] == "stall")
        return stall(toStringz(args[2]), args.length > 3 && args[3] == "blocking");
    if (args.length >= 5 && args[1] == "zero")
        return zero(toStringz(args[2]), to!uint(args[3]), to!uint(args[4]),
            args.length > 5 && args[5] == "spans");
    printf("usage: probe stall <path> [blocking] | probe zero <path> <n> <usecs> [spans]\n");
    return 2;
}
