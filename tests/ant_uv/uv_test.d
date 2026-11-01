/++
 + ant_uv tests. The binding's layout and constants against the real uv.h
 + (layout.c); then UvLoop: lifecycle and auxiliary placement, completion
 + order and ring retention, timers, credits under a flood, a full request
 + ring, cancellation, close with work in flight, a failing step hook,
 + Fibers on the loop thread, files read by worker Fibers on a GroupHost,
 + readFile's allocators, and abandonment of an operation that cannot
 + finish. Built with `AntfarmMimallocV3` and the real mimalloc library, it
 + also reads into mimalloc v3. Linux and Windows.
 +/
module uv_test;

version (TSan)
    extern(C) __gshared string[] rt_options = ["gcopt=parallel:0"];

import ant_fibers;
import ant_uv;
import ant_uv.binding;
import antfarm.ring;
import core.atomic;
import core.stdc.config : c_long;
import core.stdc.errno : errno;
import core.stdc.stdio : fflush, printf, stdout;
import core.stdc.stdlib : abort, free;
import core.thread : Thread;
import core.time;
import std.conv : to;
import std.file : mkdirRecurse, rmdirRecurse, tempDir, write;
import std.path : buildPath;
import std.process : thisProcessID;
import std.string : toStringz;
import threadpool.groups : GroupHost, GroupHostOptions;
import threadpool.host : CallerPlacement, HostGroups;
import threadpool.topology : ProcessorId;

version (linux)
{
    import core.sys.linux.sched : cpu_set_t, CPU_ISSET, sched_getaffinity;
    import core.sys.posix.sys.resource : getpriority, PRIO_PROCESS;
}
else version (Windows)
{
    import core.sys.windows.windows : GetCurrentThread, THREAD_PRIORITY_BELOW_NORMAL;
    import threadpool.sys.win_bindings : GetThreadGroupAffinity, GROUP_AFFINITY;
}

extern (C) size_t antuv_c_sizeof_buf();
extern (C) size_t antuv_c_offsetof_buf_base();
extern (C) size_t antuv_c_offsetof_buf_len();
extern (C) size_t antuv_c_sizeof_buf_len();
extern (C) size_t antuv_c_offsetof_stat_size();
extern (C) size_t antuv_c_sizeof_stat();
extern (C) c_long antuv_c_constant(int which); // C long: 32-bit on Windows

alias Ring = SlotRing!RingElement;

void check(bool condition, string what, int line = __LINE__)
{
    if (!condition)
    {
        printf("FAIL line %d: %.*s\n", line, cast(int) what.length, what.ptr);
        fflush(stdout);
        abort();
    }
}

void pass(string name)
{
    printf("PASS %.*s\n", cast(int) name.length, name.ptr);
    fflush(stdout);
}

bool waitUntil(scope bool delegate() done, Duration timeout = 10.seconds)
{
    immutable deadline = MonoTime.currTime + timeout;
    while (!done())
    {
        if (MonoTime.currTime >= deadline) return false;
        Thread.sleep(200.usecs);
    }
    return true;
}

Thread spawn(void delegate() body)
{
    auto t = new Thread(body);
    t.start();
    return t;
}

ProcessorId[] threadCpus()
{
    ProcessorId[] cpus;
    version (linux)
    {
        cpu_set_t set;
        sched_getaffinity(0, cpu_set_t.sizeof, &set);
        foreach (cpu; 0 .. cpu_set_t.sizeof * 8)
            if (CPU_ISSET(cpu, &set)) cpus ~= ProcessorId(0, cast(ushort) cpu);
    }
    else version (Windows)
    {
        GROUP_AFFINITY affinity;
        check(GetThreadGroupAffinity(GetCurrentThread(), &affinity) != 0, "GetThreadGroupAffinity");
        foreach (lp; 0 .. 64)
            if ((affinity.Mask >> lp) & 1) cpus ~= ProcessorId(affinity.Group, cast(ushort) lp);
    }
    return cpus;
}

ubyte[] pattern(size_t seed, size_t length)
{
    auto bytes = new ubyte[length];
    ulong x = seed * 0x9E37_79B9_7F4A_7C15UL + 1;
    foreach (ref b; bytes)
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        b = cast(ubyte) x;
    }
    return bytes;
}

bool allFinished(const(UvRequest)[] requests)
{
    foreach (ref r; requests)
        if (!r.finished) return false;
    return true;
}

// ---------------------------------------------------------------------------

void layout()
{
    check(antuv_c_sizeof_buf() == uv_buf_t.sizeof, "uv_buf_t size");
    check(antuv_c_offsetof_buf_base() == uv_buf_t.base.offsetof, "uv_buf_t.base offset");
    check(antuv_c_offsetof_buf_len() == uv_buf_t.len.offsetof, "uv_buf_t.len offset");
    check(antuv_c_sizeof_buf_len() == uv_buf_t.len.sizeof, "uv_buf_t.len width");
    check(antuv_c_offsetof_stat_size() == uv_stat_t.st_size.offsetof, "uv_stat_t.st_size offset");
    check(antuv_c_sizeof_stat() == uv_stat_t.sizeof, "uv_stat_t size");
    immutable long[] expected = [
        uv_handle_type.UV_ASYNC, uv_handle_type.UV_TIMER, uv_req_type.UV_FS, uv_req_type.UV_WORK,
        uv_run_mode.UV_RUN_DEFAULT, uv_run_mode.UV_RUN_ONCE, uv_run_mode.UV_RUN_NOWAIT,
        UV_ECANCELED, UV_ENOMEM, UV_EINVAL, UV_EBUSY, UV_EBADF, UV_EOF,
        UV_FS_O_RDONLY, UV_FS_O_WRONLY, UV_FS_O_RDWR, UV_FS_O_APPEND, UV_FS_O_CREAT,
        UV_FS_O_TRUNC, UV_FS_O_EXCL];
    foreach (i, value; expected)
        check(antuv_c_constant(cast(int) i) == value, "constant " ~ i.to!string ~ " differs from uv.h");
    check(antuv_c_constant(21) == uv_handle_type.UV_CHECK, "UV_CHECK");
    check(antuv_c_constant(22) == uv_handle_type.UV_SIGNAL, "UV_SIGNAL");
    check(antuv_c_constant(23) == uv_req_type.UV_RANDOM, "UV_RANDOM");
    check(antuv_c_constant(20) >= UV_MINIMUM_VERSION && uv_version() >= UV_MINIMUM_VERSION,
        "libuv older than 1.19");
    printf("  uv.h %#lx, library %#x\n", antuv_c_constant(20), uv_version());
    pass("binding layout and constants match uv.h");
}

// First loop in the process: it creates libuv's auxiliary pool, so the
// pool size, CPUs and priority set here are exact.
void lifecycleAndPlacement(ProcessorId[] cpus)
{
    UvLoopOptions o;
    immutable pin = cpus.length >= 2;
    if (pin) o.cpu = cpus[1];
    o.auxThreads = 3;
    o.auxCpus = [cpus[$ - 1]];
    o.auxPriority = UvAuxPriority.belowNormal;
    ProcessorId[] loopCpus;
    shared bool started, stopped;
    o.start = () {
        loopCpus = threadCpus();
        check(UvLoop.current !is null && UvLoop.current.onLoopThread, "current loop on its thread");
        atomicStore(started, true);
    };
    o.stop = () { atomicStore(stopped, true); };
    // Linux raises the threads' nice value by five; Windows sets
    // THREAD_PRIORITY_BELOW_NORMAL.
    version (linux)
    {
        errno = 0;
        immutable baseNice = getpriority(PRIO_PROCESS, 0);
        immutable wantPriority = baseNice + 5 > 19 ? 19 : baseNice + 5;
    }
    else version (Windows)
        immutable wantPriority = THREAD_PRIORITY_BELOW_NORMAL;
    auto loop = UvLoop.create(o);
    check(UvLoop.current is null && !loop.onLoopThread, "current is null off the loop thread");
    check(waitUntil(() => atomicLoad(started)), "start hook did not run");
    auto summary = loop.summary();
    printf("  %.*s\n", cast(int) summary.length, summary.ptr);
    auto aux = loop.auxThreads;
    check(aux.length == 3, "three auxiliary threads");
    foreach (ref t; aux)
    {
        check(t.placed && t.error == 0, "auxiliary placement failed");
        check(t.cpus == [cpus[$ - 1]], "auxiliary thread not on the requested CPU");
        check(t.priority == wantPriority, "auxiliary priority not lowered");
    }
    check(loop.isOpen && !loop.failed, "loop not open");
    auto report = loop.close();
    check(report.completed == 0 && report.loopClose == 0 && report.failure is null, "close report");
    check(atomicLoad(stopped) && !loop.isOpen, "stop hook did not run");
    if (pin) check(loopCpus == [cpus[1]], "loop thread not pinned");
    auto again = loop.close();
    check(again.loopClose == 0, "second close");
    pass("lifecycle, loop pin, auxiliary pool size, CPUs and priority");
}

void delegate() nopProducer(UvLoop loop, UvRequest[] mine)
{
    return () {
        foreach (ref r; mine)
            for (;;)
            {
                immutable s = loop.submit(&r);
                if (s == UvSubmit.accepted) break;
                check(s == UvSubmit.full, "nop submit refused");
                Thread.yield();
            }
    };
}

// Nops from four threads through a small completion ring: every request is
// delivered once, after its publication, in its producer's order.
void nopOrdering()
{
    enum producers = 4, perProducer = 5000;
    auto completions = Ring.create(64);
    auto loop = UvLoop.create();
    auto records = new UvRequest[](producers * perProducer);
    foreach (i, ref r; records)
    {
        r.op = UvOp.nop;
        r.user = i;
        r.completions = completions;
    }
    Thread[producers] threads;
    foreach (p; 0 .. producers)
        threads[p] = spawn(nopProducer(loop, records[p * perProducer .. (p + 1) * perProducer]));
    auto seen = new ubyte[](records.length);
    size_t[producers] next;
    size_t received;
    RingElement e;
    immutable deadline = MonoTime.currTime + 20.seconds;
    while (received < records.length)
    {
        if (!completions.tryPop(e))
        {
            check(MonoTime.currTime < deadline, "nop completions stalled");
            Thread.yield();
            continue;
        }
        immutable index = cast(size_t) e.tag;
        check(index < records.length && e.pointer == cast(ulong) &records[index], "element names its request");
        check(seen[index] == 0, "duplicate completion");
        seen[index] = 1;
        immutable p = index / perProducer;
        check(index == p * perProducer + next[p], "producer order broken");
        ++next[p];
        check(e.payload[0] == 0 && e.payload[3] == UvRequestState.done && records[index].done,
            "delivered before publication");
        ++received;
    }
    foreach (t; threads) t.join();
    immutable deferred = loop.deferredDeliveries;
    auto report = loop.close();
    check(report.completed == records.length && report.undelivered == 0, "nop close report");
    completions.destroy();
    printf("  %zu nops, %llu deliveries waited for room\n", received, deferred);
    pass("nop completion order and ring delivery");
}

struct TimerState
{
    MonoTime[] submitted;
    shared(long)[] elapsed;
    shared int count;
}

void onTimerDone(UvRequest* r, void* context) nothrow @nogc
{
    auto state = cast(TimerState*) context;
    immutable i = cast(size_t) r.user;
    atomicStore(state.elapsed[i], (MonoTime.currTime - state.submitted[i]).total!"hnsecs");
    atomicOp!"+="(state.count, 1);
}

void timers()
{
    enum n = 40;
    auto loop = UvLoop.create();
    auto state = new TimerState;
    state.submitted = new MonoTime[n];
    state.elapsed = new shared(long)[n];
    auto records = new UvRequest[n];
    foreach (i, ref r; records)
    {
        r.op = UvOp.timer;
        r.timeoutMs = 1 + (i * 7) % 40;
        r.user = i;
        r.callback = &onTimerDone;
        r.context = state;
        state.submitted[i] = MonoTime.currTime;
        check(loop.submit(&r) == UvSubmit.accepted, "timer submit");
    }
    check(waitUntil(() => atomicLoad(state.count) == n), "timers did not fire");
    foreach (i, ref r; records)
    {
        check(r.done && r.result == 0, "timer result");
        check(atomicLoad(state.elapsed[i]) >= (cast(long) r.timeoutMs - 1) * 10_000, "timer fired early");
    }
    loop.close();
    pass("timers fire after their timeout");
}

void completionRetention()
{
    auto ring = Ring.create(2);
    auto loop = UvLoop.create();
    auto records = new UvRequest[100];
    foreach (i, ref r; records)
    {
        r.op = UvOp.nop;
        r.user = i;
        r.completions = ring;
        check(loop.submit(&r) == UvSubmit.accepted, "nop submit");
    }
    check(waitUntil(() => loop.completed == records.length), "nops not published");
    Thread.sleep(5.msecs);
    check(loop.deferredDeliveries >= records.length - 2, "deliveries were not retained");
    size_t received;
    RingElement e;
    immutable deadline = MonoTime.currTime + 10.seconds;
    while (received < records.length)
    {
        if (!ring.tryPop(e))
        {
            check(MonoTime.currTime < deadline, "retained deliveries stalled");
            Thread.sleep(100.usecs);
            continue;
        }
        check(e.tag == received, "retained deliveries out of order");
        ++received;
    }
    auto report = loop.close();
    check(report.undelivered == 0, "a retained delivery was dropped");
    ring.destroy();
    pass("a full completion ring holds deliveries until it has room");
}

void delegate() timerProducer(UvLoop loop, UvRequest[] mine)
{
    return () {
        foreach (ref r; mine)
        {
            r.op = UvOp.timer;
            r.timeoutMs = 1;
            while (loop.submit(&r) != UvSubmit.accepted) Thread.yield();
        }
    };
}

void creditFlood()
{
    enum producers = 4, perProducer = 250;
    UvLoopOptions o;
    o.credits = 8;
    o.ringCapacity = 32;
    auto loop = UvLoop.create(o);
    auto records = new UvRequest[producers * perProducer];
    Thread[producers] threads;
    foreach (p; 0 .. producers)
        threads[p] = spawn(timerProducer(loop, records[p * perProducer .. (p + 1) * perProducer]));
    foreach (t; threads) t.join();
    check(waitUntil(() => allFinished(records), 30.seconds), "flood did not finish");
    foreach (ref r; records) check(r.done && r.result == 0, "flood timer result");
    check(loop.maxInFlight <= 8 && loop.maxInFlight >= 2, "credit limit not respected");
    printf("  most in flight %llu of 8 credits\n", loop.maxInFlight);
    loop.close();
    pass("credits bound operations in libuv under a flood");
}

void noopCallback(UvRequest*, void*) nothrow @nogc {}

void fullRingAndValidation()
{
    UvLoopOptions o;
    o.ringCapacity = 4;
    o.credits = 1;
    o.closeGrace = 10.msecs;
    auto loop = UvLoop.create(o);
    auto records = new UvRequest[6];
    foreach (ref r; records)
    {
        r.op = UvOp.timer;
        r.timeoutMs = 10_000;
    }
    check(loop.submit(&records[0]) == UvSubmit.accepted, "first timer");
    check(waitUntil(() => loop.maxInFlight == 1), "first timer not started");
    foreach (i; 1 .. 5)
        check(loop.submit(&records[i]) == UvSubmit.accepted, "ring slot");
    check(loop.submit(&records[5]) == UvSubmit.full && records[5].state == UvRequestState.idle,
        "full ring did not refuse");
    check(loop.submit(&records[1]) == UvSubmit.invalid, "double submit accepted");
    auto ring = Ring.create(2);
    UvRequest bad;
    bad.op = UvOp.open;
    check(loop.submit(&bad) == UvSubmit.invalid, "open without a path");
    bad.op = UvOp.read;
    check(loop.submit(&bad) == UvSubmit.invalid, "read without a file");
    bad.op = UvOp.nop;
    bad.callback = &noopCallback;
    bad.completions = ring;
    check(loop.submit(&bad) == UvSubmit.invalid, "callback and completion ring together");
    check(loop.submit(null) == UvSubmit.invalid, "null request");
    auto report = loop.close();
    check(report.cancelled == 5, "close did not cancel the queued timers");
    foreach (i; 0 .. 5)
        check(records[i].done && records[i].result == UV_ECANCELED, "queued timer not cancelled");
    check(records[5].state == UvRequestState.idle, "refused request touched");
    check(loop.submit(&records[5]) == UvSubmit.closed, "submit after close");
    ring.destroy();
    pass("a full ring refuses and keeps the request; malformed requests are invalid");
}

void cancellation(string path)
{
    UvLoopOptions o;
    o.credits = 2;
    o.ringCapacity = 64;
    auto loop = UvLoop.create(o);
    // Two timers take the credits; twenty wait in the ring behind them.
    auto timers = new UvRequest[22];
    foreach (ref r; timers)
    {
        r.op = UvOp.timer;
        r.timeoutMs = 10_000;
        check(loop.submit(&r) == UvSubmit.accepted, "timer submit");
    }
    check(waitUntil(() => loop.maxInFlight == 2), "timers not started");
    immutable started = MonoTime.currTime;
    foreach (ref r; timers) check(loop.cancel(&r), "cancel refused");
    check(waitUntil(() => allFinished(timers)), "cancelled timers not published");
    check(MonoTime.currTime - started < 2.seconds, "cancellation waited for the timers");
    foreach (ref r; timers) check(r.done && r.result == UV_ECANCELED, "timer not cancelled");
    auto timerReport = loop.close();
    check(timerReport.cancelled == 22, "timer cancel count");
    // File work cancelled right after submission, on a ring that holds the
    // whole batch behind two credits: whatever libuv had not started is
    // cancelled, the rest completes, and every request publishes.
    o.ringCapacity = 256;
    loop = UvLoop.create(o);
    auto stats = new UvRequest[200];
    auto cpath = path.toStringz;
    foreach (ref r; stats)
    {
        r.op = UvOp.stat;
        r.path = cpath;
        check(loop.submit(&r) == UvSubmit.accepted, "stat submit");
    }
    foreach (ref r; stats) check(loop.cancel(&r), "stat cancel refused");
    check(waitUntil(() => allFinished(stats)), "cancelled stats not published");
    size_t cancelled, completed;
    foreach (ref r; stats)
    {
        check(r.done, "stat not done");
        if (r.result == UV_ECANCELED) ++cancelled;
        else
        {
            check(r.result == 0 && r.size == 4096, "stat result");
            ++completed;
        }
    }
    check(cancelled > 0, "no stat was cancelled");
    auto report = loop.close();
    check(report.cancelled == cancelled, "cancel count");
    printf("  stats: %zu cancelled, %zu completed\n", cancelled, completed);
    pass("cancellation before and during libuv work");
}

void closeInFlight()
{
    UvLoopOptions o;
    o.closeGrace = 20.msecs;
    auto loop = UvLoop.create(o);
    auto records = new UvRequest[10];
    foreach (ref r; records)
    {
        r.op = UvOp.timer;
        r.timeoutMs = 10_000;
        check(loop.submit(&r) == UvSubmit.accepted, "timer submit");
    }
    check(waitUntil(() => loop.maxInFlight == 10), "timers not started");
    immutable started = MonoTime.currTime;
    auto report = loop.close();
    check(MonoTime.currTime - started < 2.seconds, "close waited for the timers");
    check(report.cancelled == 10 && report.abandoned == 0 && report.loopClose == 0, "close report");
    foreach (ref r; records) check(r.done && r.result == UV_ECANCELED, "timer not cancelled by close");
    pass("close cancels work in flight after its grace period");
}

void stepFailure()
{
    UvLoopOptions o;
    shared int calls;
    o.step = () {
        if (atomicOp!"+="(calls, 1) == 2) throw new Exception("step failure probe");
        return UvStepResult.init;
    };
    auto loop = UvLoop.create(o);
    auto records = new UvRequest[3];
    foreach (ref r; records)
    {
        r.op = UvOp.timer;
        r.timeoutMs = 10_000;
        immutable s = loop.submit(&r);
        check(s == UvSubmit.accepted || s == UvSubmit.closed, "submit during failure");
        if (s == UvSubmit.closed) r.result = UV_ECANCELED; // refused: count it as cancelled
    }
    check(waitUntil(() => loop.failed), "step failure not recorded");
    check(waitUntil(() {
        foreach (ref r; records)
            if (r.state == UvRequestState.queued) return false;
        return true;
    }), "requests not settled after the failure");
    bool threw;
    try
        loop.close();
    catch (UvLoopException e)
    {
        threw = true;
        check(e.report.failure !is null && e.report.failure.msg == "step failure probe",
            "failure not reported");
        check(e.report.loopClose == 0, "failed loop not closed");
    }
    check(threw, "close did not throw after a failure");
    foreach (ref r; records)
        check(r.result == UV_ECANCELED, "request not cancelled by the failure");
    pass("a throwing step hook fails the loop, cancels its work and surfaces at close");
}

__gshared TaskHandle gTimerWaiter;
__gshared FiberDomain gLoopDomain;

void loopFibers(string path, const(ubyte)[] expected)
{
    UvFibers fibers;
    shared int outcome, cancelOutcome, waiterReady;
    auto cpath = path.toStringz;
    UvLoopOptions o;
    o.start = () {
        auto loop = UvLoop.current;
        fibers = new UvFibers(loop);
        gLoopDomain = fibers.domain;
        // Parks on a long timer, then is cancelled from the main thread: the
        // request must be published before the cancellation unwinds.
        gTimerWaiter = fibers.spawnHandle(() {
            UvRequest r;
            r.op = UvOp.timer;
            r.timeoutMs = 10_000;
            r.event = new FiberEvent(fibers.domain);
            atomicStore(waiterReady, 1);
            try
            {
                submitAndWait(loop, &r);
                atomicStore(cancelOutcome, 2);
            }
            catch (FiberCancelled cancelled)
            {
                atomicStore(cancelOutcome,
                    r.finished && r.result == UV_ECANCELED ? 1 : 3);
                throw cancelled;
            }
        });
        fibers.spawn(() {
            FiberDomain.sleepFor(5.msecs);
            auto event = new FiberEvent(fibers.domain);
            UvRequest r;
            r.op = UvOp.readFile;
            r.path = cpath;
            r.event = event;
            bool good = submitAndWait(loop, &r) == UvSubmit.accepted && r.done
                && r.result == expected.length && r.size == expected.length;
            auto data = r.takeBuffer();
            good = good && data.bytes == expected && loop.onLoopThread;
            data.free();
            atomicStore(outcome, good ? 1 : 2);
        });
    };
    o.step = () => fibers.step();
    o.stop = () { fibers.shutdown(); };
    auto loop = UvLoop.create(o);
    check(waitUntil(() => atomicLoad(outcome) != 0), "loop-thread Fiber did not finish");
    check(atomicLoad(outcome) == 1, "loop-thread Fiber read the wrong bytes");
    check(waitUntil(() => atomicLoad(waiterReady) != 0 && loop.maxInFlight >= 1),
        "timer waiter did not start");
    Thread.sleep(2.msecs);
    check(gLoopDomain.requestCancel(gTimerWaiter) == CancellationSubmission.queued,
        "cancellation not queued");
    check(waitUntil(() => atomicLoad(cancelOutcome) != 0), "cancelled waiter did not unwind");
    check(atomicLoad(cancelOutcome) == 1, "cancellation unwound before the request was published");
    auto report = loop.close();
    check(report.completed == 2 && report.cancelled == 1 && report.loopClose == 0,
        "loop fibers close report");
    gLoopDomain = null;
    pass("Fibers on the loop thread sleep, read a file, and outwait their request when cancelled");
}

shared long gPublished, gSubmitted;

void countPublication(UvRequest*, void*) nothrow @nogc
{
    atomicOp!"+="(gPublished, 1);
}

bool submitted(UvSubmit s)
{
    if (s == UvSubmit.accepted) atomicOp!"+="(gSubmitted, 1);
    return s == UvSubmit.accepted;
}

void delegate() readerBody(UvLoop loop, FiberDomain domain, size_t index, string path,
    const(ubyte)[] expected, shared(int)* ok, shared(int)* bad)
{
    return () {
        auto cpath = path.toStringz;
        bool good;
        if (index % 3 == 0)
        {
            // Whole file into the loop's allocation, woken by an event.
            UvRequest r;
            r.op = UvOp.readFile;
            r.path = cpath;
            r.event = new FiberEvent(domain);
            r.callback = &countPublication;
            good = submitted(submitAndWait(loop, &r)) && r.done
                && r.result == expected.length && r.size == expected.length;
            auto data = r.takeBuffer();
            good = good && data.bytes == expected;
            data.free();
        }
        else if (index % 3 == 1)
        {
            // open, fstat, read in pieces into a GC buffer, close: one
            // record reused, woken by a counted trigger.
            auto trigger = new FiberGenerationTrigger(domain);
            UvRequest r;
            r.trigger = trigger;
            r.callback = &countPublication;
            bool run(UvOp op)
            {
                r.op = op;
                for (;;)
                {
                    immutable s = loop.submit(&r);
                    if (s == UvSubmit.full)
                    {
                        FiberDomain.yieldReady();
                        continue;
                    }
                    if (!submitted(s)) return false;
                    break;
                }
                trigger.waitNext();
                return r.done;
            }
            r.path = cpath;
            r.flags = UV_FS_O_RDONLY;
            good = run(UvOp.open) && r.result >= 0;
            if (good)
            {
                immutable file = cast(int) r.result;
                r.file = file;
                good = run(UvOp.fstat) && r.result == 0 && r.size == expected.length;
                auto buffer = new ubyte[expected.length + 1];
                size_t got;
                while (good)
                {
                    r.file = file;
                    r.buffer = buffer.ptr + got;
                    r.length = 4096 < buffer.length - got ? 4096 : buffer.length - got;
                    r.offset = cast(long) got;
                    good = run(UvOp.read) && r.result >= 0;
                    if (!good || r.result == 0) break;
                    got += cast(size_t) r.result;
                }
                good = good && got == expected.length && buffer[0 .. got] == expected;
                r.file = file;
                r.buffer = null;
                r.length = 0;
                good = run(UvOp.close) && r.result == 0 && good;
            }
        }
        else
        {
            // Whole file into a caller buffer one byte short: truncated.
            auto buffer = new ubyte[expected.length == 0 ? 1 : expected.length - 1];
            UvRequest r;
            r.op = UvOp.readFile;
            r.path = cpath;
            r.buffer = buffer.ptr;
            r.length = expected.length == 0 ? 1 : buffer.length;
            r.event = new FiberEvent(domain);
            r.callback = &countPublication;
            good = submitted(submitAndWait(loop, &r)) && r.done && r.size == expected.length;
            immutable want = expected.length == 0 ? 0 : expected.length - 1;
            good = good && r.result == want && buffer[0 .. want] == expected[0 .. want]
                && !r.ownsBuffer;
        }
        atomicOp!"+="(*(good ? ok : bad), 1);
    };
}

void workerFiberReads(string[] paths, ubyte[][] contents)
{
    GroupHostOptions probe;
    probe.groups = HostGroups.all();
    if (GroupHost.resolve(probe).workers.length == 0)
    {
        printf("SKIP worker Fiber reads: no pool workers\n");
        return;
    }
    auto loop = UvLoop.create();
    auto domain = new FiberDomain;
    auto fibers = new FiberGroups(domain);
    auto o = probe;
    o.farmPerGroup = GroupHost.resolve(o).groups.length > 1;
    o.caller = CallerPlacement.unchanged;
    o.farm.ringMiB = 2;
    o.attachments = [fibers.attachment];
    auto host = new GroupHost(o);
    enum readers = 96;
    shared int ok, bad;
    atomicStore(gPublished, 0);
    atomicStore(gSubmitted, 0);
    foreach (i; 0 .. readers)
    {
        immutable llc = host.groups[i % host.groups.length];
        fibers.spawnOn(llc, readerBody(loop, domain, i, paths[i % paths.length],
            contents[i % paths.length], &ok, &bad));
    }
    check(waitUntil(() => atomicLoad(ok) + atomicLoad(bad) == readers, 60.seconds),
        "worker Fiber reads stalled");
    check(atomicLoad(bad) == 0, "a worker Fiber read the wrong bytes");
    check(waitUntil(() => domain.drained), "worker Fibers did not finish");
    check(atomicLoad(gPublished) == atomicLoad(gSubmitted), "publications differ from submissions");
    // A worker Fiber cancelled while parked on a long timer holds its worker
    // until the loop publishes the request, then unwinds.
    shared int waiterReady, workerCancel;
    auto waiter = fibers.spawnHandle(() {
        UvRequest r;
        r.op = UvOp.timer;
        r.timeoutMs = 10_000;
        r.event = new FiberEvent(domain);
        atomicStore(waiterReady, 1);
        try
        {
            submitAndWait(loop, &r);
            atomicStore(workerCancel, 2);
        }
        catch (FiberCancelled cancelled)
        {
            atomicStore(workerCancel, r.finished && r.result == UV_ECANCELED ? 1 : 3);
            throw cancelled;
        }
    });
    check(waitUntil(() => atomicLoad(waiterReady) != 0), "worker timer waiter did not start");
    Thread.sleep(5.msecs);
    check(domain.requestCancel(waiter) == CancellationSubmission.queued, "worker cancellation not queued");
    check(waitUntil(() => atomicLoad(workerCancel) != 0), "cancelled worker Fiber did not unwind");
    check(atomicLoad(workerCancel) == 1, "worker cancellation unwound before publication");
    check(waitUntil(() => domain.drained), "cancelled worker Fiber did not finish");
    cast(void) domain.retire();
    auto report = loop.close();
    host.close();
    domain.beginShutdown();
    check(report.completed == cast(ulong) atomicLoad(gSubmitted) + 1 && report.cancelled == 1
        && report.loopClose == 0, "worker reads close report");
    printf("  %d reads, %lld requests on %u workers in %zu group(s)\n", readers,
        atomicLoad(gSubmitted), host.workers, host.groups.length);
    pass("worker Fibers on a GroupHost read files through the loop; a cancelled one outwaits its request");
}

// An allocator that counts its calls, checks each free against the block it
// last allocated, and notes whether it ran on the loop thread. Allocations
// happen on the loop thread and are published before anyone frees them, so
// the `last*` fields need no atomics.
struct CountingHeap
{
    shared long allocations, deallocations, offLoopAllocations, loopFrees, mismatches;
    shared bool refuse;
    void* lastBlock;
    size_t lastBytes, lastAlignment;
}

// The Microsoft C runtime has no aligned_alloc: over-allocate and keep the
// malloc block just below the aligned address.
version (CRuntime_Microsoft)
{
    void* alignedAllocate(size_t alignment, size_t bytes) nothrow @nogc @system
    {
        import core.stdc.stdlib : malloc;
        auto raw = malloc(bytes + alignment + (void*).sizeof);
        if (raw is null) return null;
        auto aligned = (cast(size_t) raw + (void*).sizeof + alignment - 1) & ~(alignment - 1);
        (cast(void**) aligned)[-1] = raw;
        return cast(void*) aligned;
    }

    void alignedFree(void* memory) nothrow @nogc @system
    {
        if (memory !is null) free((cast(void**) memory)[-1]);
    }
}
else
{
    void* alignedAllocate(size_t alignment, size_t bytes) nothrow @nogc @system
    {
        import core.stdc.stdlib : aligned_alloc;
        return aligned_alloc(alignment, bytes);
    }

    alias alignedFree = free;
}

void* countingAllocate(void* context, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto heap = cast(CountingHeap*) context;
    atomicOp!"+="(heap.allocations, 1);
    if (UvLoop.current is null) atomicOp!"+="(heap.offLoopAllocations, 1);
    if (atomicLoad(heap.refuse)) return null;
    auto block = alignedAllocate(alignment, (bytes + alignment - 1) & ~(alignment - 1));
    heap.lastBlock = block;
    heap.lastBytes = bytes;
    heap.lastAlignment = alignment;
    return block;
}

void countingDeallocate(void* context, void* memory, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    auto heap = cast(CountingHeap*) context;
    atomicOp!"+="(heap.deallocations, 1);
    if (UvLoop.current !is null) atomicOp!"+="(heap.loopFrees, 1);
    if (memory !is heap.lastBlock || bytes != heap.lastBytes || alignment != heap.lastAlignment)
        atomicOp!"+="(heap.mismatches, 1);
    alignedFree(memory);
}

// Submit from this thread and spin until the loop publishes.
bool readNow(UvLoop loop, UvRequest* r)
{
    return loop.submit(r) == UvSubmit.accepted && waitUntil(() => r.finished);
}

void readFileAllocators(string dir, string path, const(ubyte)[] expected, string emptyPath)
{
    auto loop = UvLoop.create();
    CountingHeap heap;
    auto counting = ActorAllocator(&heap, &countingAllocate, &countingDeallocate);
    auto cpath = path.toStringz;
    long allocations() { return atomicLoad(heap.allocations); }
    long deallocations() { return atomicLoad(heap.deallocations); }

    // The request's allocator at page alignment: allocated on the loop
    // thread, freed here with the block, size and alignment it allocated,
    // even after the request's own field changed.
    UvRequest r;
    r.op = UvOp.readFile;
    r.path = cpath;
    r.allocator = counting;
    r.alignment = 4096;
    check(readNow(loop, &r) && r.succeeded && r.ownsBuffer, "read into the request's allocator");
    check(r.buffer[0 .. r.length] == expected, "the file in place");
    check(allocations == 1 && atomicLoad(heap.offLoopAllocations) == 0,
        "allocated once, on the loop thread");
    r.allocator = ActorAllocator.init;
    auto file = r.takeBuffer();
    check(!r.ownsBuffer && r.buffer is null && r.length == 0, "takeBuffer clears the request");
    check(file.bytes == expected && file.capacity == expected.length && file.alignment == 4096
        && (cast(size_t) file.bytes.ptr & 4095) == 0, "the handed-over block");
    check(r.takeBuffer().bytes is null, "a second take hands over nothing");
    file.free();
    check(file.bytes is null && file.capacity == 0, "free empties the buffer");
    file.free();
    check(deallocations == 1 && atomicLoad(heap.mismatches) == 0
        && atomicLoad(heap.loopFrees) == 0, "freed once, here, as allocated");

    // freeBuffer frees through the same allocator.
    r = UvRequest.init;
    r.op = UvOp.readFile;
    r.path = cpath;
    r.allocator = counting;
    r.alignment = 16;
    check(readNow(loop, &r) && r.succeeded, "second read");
    check(loop.submit(&r) == UvSubmit.invalid, "a request owning a block is refused");
    r.freeBuffer();
    check(allocations == 2 && deallocations == 2 && atomicLoad(heap.mismatches) == 0,
        "freeBuffer frees through the allocator");

    // Unset: the C runtime's 64-byte-aligned pair.
    UvRequest plain;
    plain.op = UvOp.readFile;
    plain.path = cpath;
    check(readNow(loop, &plain) && plain.succeeded, "default allocator read");
    auto plainFile = plain.takeBuffer();
    check(plainFile.bytes == expected && plainFile.alignment == 64
        && (cast(size_t) plainFile.bytes.ptr & 63) == 0, "default block");
    plainFile.free();

    // A read that fails after allocating: the loop frees the block. On
    // Linux a directory opens and reports a size, then its read fails with
    // EISDIR. libuv on Windows reports every directory's size as 0, so there
    // the file is one whose bytes another handle has locked: it opens (libuv
    // shares everything) and reports its size, then its read fails with
    // EBUSY.
    version (Windows)
    {
        import core.sys.windows.windows : CloseHandle, CreateFileW, FILE_ATTRIBUTE_NORMAL,
            FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, GENERIC_READ, GENERIC_WRITE,
            INVALID_HANDLE_VALUE, LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
            OPEN_EXISTING, OVERLAPPED;
        import std.utf : toUTF16z;
        auto failedPath = buildPath(dir, "locked");
        write(failedPath, pattern(7, 4096));
        auto locker = CreateFileW(failedPath.toUTF16z, GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, null, OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL, null);
        check(locker != INVALID_HANDLE_VALUE, "open the file to lock");
        scope (exit) CloseHandle(locker);
        OVERLAPPED whole;
        check(LockFileEx(locker, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0,
            uint.max, uint.max, &whole) != 0, "lock the file");
        enum failedError = "EBUSY";
        immutable failedReports = true;
    }
    else
    {
        import std.file : getSize;
        auto failedPath = dir;
        enum failedError = "EISDIR";
        immutable failedReports = getSize(dir) != 0;
    }
    if (!failedReports)
        printf("  SKIP failed-read free: the directory reports size 0\n");
    else
    {
        UvRequest failed;
        failed.op = UvOp.readFile;
        failed.path = failedPath.toStringz;
        failed.allocator = counting;
        check(readNow(loop, &failed) && failed.done && failed.errorName == failedError
            && !failed.ownsBuffer, "a read that fails after allocating");
        check(allocations == 3 && deallocations == 3 && atomicLoad(heap.loopFrees) == 1
            && atomicLoad(heap.mismatches) == 0, "the loop freed the failed read's block");
    }
    immutable before = allocations;
    immutable freed = deallocations;

    // An allocator that returns null fails the request with ENOMEM.
    atomicStore(heap.refuse, true);
    UvRequest refused;
    refused.op = UvOp.readFile;
    refused.path = cpath;
    refused.allocator = counting;
    check(readNow(loop, &refused) && refused.done && refused.result == UV_ENOMEM
        && !refused.ownsBuffer, "a refused allocation is ENOMEM");
    check(allocations == before + 1 && deallocations == freed, "nothing to free after a refusal");
    atomicStore(heap.refuse, false);

    // An empty file allocates nothing and hands over nothing.
    UvRequest empty;
    empty.op = UvOp.readFile;
    empty.path = emptyPath.toStringz;
    empty.allocator = counting;
    check(readNow(loop, &empty) && empty.succeeded && empty.result == 0 && !empty.ownsBuffer,
        "empty file read");
    auto none = empty.takeBuffer();
    check(none.bytes is null && none.capacity == 0, "an empty file hands over nothing");
    none.free();
    check(allocations == before + 1, "an empty file allocated");

    // Allocation fields are checked only when the loop allocates.
    UvRequest bad;
    bad.op = UvOp.readFile;
    bad.path = cpath;
    bad.alignment = 0;
    check(loop.submit(&bad) == UvSubmit.invalid, "alignment 0 accepted");
    bad.alignment = 48;
    check(loop.submit(&bad) == UvSubmit.invalid, "alignment 48 accepted");
    bad.alignment = 64;
    bad.allocator.allocate = counting.allocate;
    check(loop.submit(&bad) == UvSubmit.invalid, "half an allocator accepted");
    ubyte[16] small;
    bad.buffer = small.ptr;
    bad.length = small.length;
    bad.alignment = 0;
    check(readNow(loop, &bad) && bad.succeeded && bad.result == 16 && !bad.ownsBuffer
        && small[] == expected[0 .. 16], "a caller buffer ignores the allocation fields");

    auto report = loop.close();
    check(report.failure is null && report.loopClose == 0, "allocators close report");
    check(allocations == before + 1 && deallocations == freed, "unbalanced allocator calls");
    pass("readFile allocates from the request's allocator at its alignment and frees once");
}

version (AntfarmMimallocV3)
{
    import antfarm.actors.mimalloc : mimallocV3ActorAllocator;

    extern (C) int mi_version() nothrow @nogc;
    extern (C) bool mi_is_in_heap_region(const(void)* p) nothrow @nogc;
    extern (C) size_t mi_usable_size(const(void)* p) nothrow @nogc;

    // The real mimalloc v3 adapter: blocks allocated on the loop thread,
    // freed on this one.
    void mimallocReads(string path, const(ubyte)[] expected)
    {
        immutable v = mi_version();
        check(v >= 30000 && v < 40000, "mimalloc is not v3");
        auto loop = UvLoop.create();
        UvBuffer[16] files;
        foreach (ref file; files)
        {
            UvRequest r;
            r.op = UvOp.readFile;
            r.path = path.toStringz;
            r.allocator = mimallocV3ActorAllocator();
            r.alignment = 256;
            check(readNow(loop, &r) && r.succeeded, "mimalloc read");
            file = r.takeBuffer();
            check(file.bytes == expected && (cast(size_t) file.bytes.ptr & 255) == 0
                && mi_is_in_heap_region(file.bytes.ptr)
                && mi_usable_size(file.bytes.ptr) >= expected.length, "a mimalloc block");
        }
        foreach (ref file; files)
            file.free();
        auto report = loop.close();
        check(report.completed == files.length && report.loopClose == 0, "mimalloc close report");
        printf("  mimalloc %d.%d.%d\n", v / 10000, v / 100 % 100, v % 100);
        pass("readFile into mimalloc v3 on the loop thread, freed on another");
    }
}

// An operation that blocks in an auxiliary thread cannot be cancelled by
// libuv: close abandons it and leaves the loop allocated. The test then
// unblocks it so the auxiliary thread is free again. Linux opens a FIFO
// that has no writer; Windows opens a named pipe and reads it before its
// server writes, because opening a pipe there does not block.
void abandonBlocked(string dir)
{
    UvLoopOptions o;
    o.closeGrace = 20.msecs;
    o.abandonAfter = 50.msecs;
    version (linux)
    {
        import core.sys.posix.fcntl : open, O_WRONLY;
        import core.sys.posix.sys.stat : mkfifo;
        import core.sys.posix.unistd : close;
        auto fifo = buildPath(dir, "fifo");
        auto cfifo = fifo.toStringz;
        check(mkfifo(cfifo, 0x180) == 0, "mkfifo");
        auto loop = UvLoop.create(o);
        auto r = new UvRequest;
        r.op = UvOp.open;
        r.path = cfifo;
        r.flags = UV_FS_O_RDONLY;
        check(loop.submit(r) == UvSubmit.accepted, "fifo open submit");
    }
    else version (Windows)
    {
        import core.sys.windows.windows : CloseHandle, CreateNamedPipeA, DWORD, HANDLE,
            INVALID_HANDLE_VALUE, PIPE_ACCESS_OUTBOUND, PIPE_TYPE_BYTE, PIPE_WAIT, WriteFile;
        auto pipe = ("\\\\.\\pipe\\antuv-test-" ~ thisProcessID.to!string).toStringz;
        HANDLE server = CreateNamedPipeA(pipe, PIPE_ACCESS_OUTBOUND, PIPE_TYPE_BYTE | PIPE_WAIT,
            1, 4096, 4096, 0, null);
        check(server != INVALID_HANDLE_VALUE, "CreateNamedPipe");
        scope (exit) CloseHandle(server);
        auto loop = UvLoop.create(o);
        UvRequest client;
        client.op = UvOp.open;
        client.path = pipe;
        client.flags = UV_FS_O_RDONLY;
        check(readNow(loop, &client) && client.succeeded, "pipe open");
        auto r = new UvRequest;
        r.op = UvOp.read;
        r.file = cast(int) client.result;
        r.buffer = (new ubyte[16]).ptr;
        r.length = 16;
        check(loop.submit(r) == UvSubmit.accepted, "pipe read submit");
    }
    Thread.sleep(30.msecs);
    auto report = loop.close();
    check(report.abandoned == 1 && report.loopClose == UV_EBUSY, "close did not abandon the operation");
    check(r.state == UvRequestState.abandoned && r.result == UV_ECANCELED, "abandoned request state");
    version (linux)
    {
        immutable writer = open(cfifo, O_WRONLY);
        check(writer >= 0, "fifo writer");
        close(writer);
    }
    else version (Windows)
    {
        ubyte[16] data;
        DWORD written;
        check(WriteFile(server, data.ptr, data.length, &written, null) && written == data.length,
            "pipe writer");
    }
    pass("close abandons an operation libuv cannot cancel and leaves the loop allocated");
}

int main()
{
    immutable started = MonoTime.currTime;
    auto dir = buildPath(tempDir, "antuv-test-" ~ thisProcessID.to!string);
    mkdirRecurse(dir);
    scope (exit) rmdirRecurse(dir);
    auto cpus = threadCpus();
    check(cpus.length != 0, "no CPUs in the affinity mask");

    immutable size_t[] sizes = [0, 1, 7, 4095, 4096, 4097, 65_536, 65_537, (1 << 20) + 13];
    string[] paths;
    ubyte[][] contents;
    foreach (i; 0 .. 18)
    {
        auto path = buildPath(dir, "file" ~ i.to!string);
        auto bytes = pattern(i, sizes[i % sizes.length]);
        write(path, bytes);
        paths ~= path;
        contents ~= bytes;
    }
    auto statPath = buildPath(dir, "stat");
    write(statPath, pattern(99, 4096));

    layout();
    lifecycleAndPlacement(cpus);
    nopOrdering();
    timers();
    completionRetention();
    creditFlood();
    fullRingAndValidation();
    cancellation(statPath);
    closeInFlight();
    stepFailure();
    loopFibers(paths[7], contents[7]);
    workerFiberReads(paths, contents);
    readFileAllocators(dir, paths[8], contents[8], paths[0]);
    version (AntfarmMimallocV3) mimallocReads(paths[8], contents[8]);
    abandonBlocked(dir);
    printf("ALL UV TESTS PASSED (%lld ms)\n", (MonoTime.currTime - started).total!"msecs");
    return 0;
}
