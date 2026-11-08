/++
 + ant_uv tests. The binding's layout and constants against the real uv.h
 + (layout.c); then UvLoop: lifecycle and auxiliary placement, completion
 + order and ring retention, timers, credits under a flood, a full request
 + ring, cancellation, close with work in flight, a failing step hook,
 + Fibers on the loop thread, files read by worker Fibers on a GroupHost,
 + readFile's allocators, the per-operation constructors, whole-file reads
 + cancelled in flight, UDP sockets on the loopback and the `@safe` surface
 + over them, Fibers spawned from `@safe` code, a whole-file read abandoned
 + inside its allocator, and abandonment of an operation that cannot
 + finish. With ANTUV_TEST_IO_URING=1 every loop uses io_uring, and
 + create must have built the ring. Built with `AntfarmMimallocV3` and the
 + real mimalloc library, it also reads into mimalloc v3. Linux and Windows.
 +/
module uv_test;

// Under ThreadSanitizer the collector never runs. druntime stops the world
// with a signal, and TSan holds an asynchronous signal until its thread next
// calls an interceptor or an instrumented atomic. A thread spinning on the
// GC's own lock (uninstrumented, and it only pauses and yields) or parked in
// a raw futex never does, so a collection that begins while any thread is in
// such a state waits for it forever. The suite's heap is small; the plain
// lanes cover collection. Parallel marking is off for the same reason.
version (TSan)
    extern(C) __gshared string[] rt_options = ["gcopt=parallel:0 disable:1"];

import ant_fibers;
import ant_uv;
import ant_uv.binding;
import antfarm.ring;
import core.atomic;
import core.lifetime : move;
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

void check(bool condition, string what, int line = __LINE__) @trusted
{
    if (!condition)
    {
        printf("FAIL line %d: %.*s\n", line, cast(int) what.length, what.ptr);
        fflush(stdout);
        abort();
    }
}

void pass(string name) @trusted
{
    printf("PASS %.*s\n", cast(int) name.length, name.ptr);
    fflush(stdout);
}

bool waitUntil(scope bool delegate() done, Duration timeout = 10.seconds) @trusted
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

// ANTUV_TEST_IO_URING=1 runs every loop with UvLoopOptions.ioUring, so open,
// close, read, write, stat and fstat go through the ring (Linux only), and
// adds ioUringRingAtCreate.
__gshared bool gIoUring;

UvLoopOptions testOptions()
{
    UvLoopOptions o;
    o.ioUring = gIoUring;
    return o;
}

// libuv's io_uring polling threads in this process, by the name the kernel
// gives them (`iou-sqp-<pid>`).
version (linux) size_t pollingThreads()
{
    import std.algorithm.searching : startsWith;
    import std.file : dirEntries, readText, SpanMode;
    size_t n;
    foreach (entry; dirEntries("/proc/self/task", SpanMode.shallow))
    {
        try
        {
            if (readText(entry.name ~ "/comm").startsWith("iou-sqp")) ++n;
        }
        catch (Exception) {} // the thread exited meanwhile
    }
    return n;
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
    check(antuv_c_constant(24) == UV_EISDIR, "UV_EISDIR");
    check(antuv_c_constant(25) == UV_S_IFMT && antuv_c_constant(26) == UV_S_IFDIR,
        "st_mode's S_IFMT and S_IFDIR");
    immutable sqpoll = antuv_c_constant(27); // -1: a uv.h older than 1.49
    check(sqpoll == -1 || sqpoll == uv_loop_option.UV_LOOP_USE_IO_URING_SQPOLL,
        "UV_LOOP_USE_IO_URING_SQPOLL");
    immutable long[] udpExpected = [UV_EAGAIN, UV_ENOBUFS, UV_EADDRINUSE, UV_EMFILE,
        uv_handle_type.UV_UDP, uv_req_type.UV_UDP_SEND, uv_req_type.UV_GETADDRINFO,
        UV_UDP_IPV6ONLY, UV_UDP_PARTIAL, UV_UDP_REUSEADDR, uv_membership.UV_JOIN_GROUP,
        sockaddr_in.sizeof, sockaddr_in6.sizeof, sockaddr_in6.sin6_addr.offsetof,
        sockaddr_in6.sin6_scope_id.offsetof, addrinfo.ai_addr.offsetof, addrinfo.ai_next.offsetof,
        AF_INET, AF_INET6];
    foreach (i, value; udpExpected)
        check(antuv_c_constant(cast(int)(28 + i)) == value,
            "UDP constant " ~ (28 + i).to!string ~ " differs from the C headers");
    check(antuv_c_constant(20) >= UV_MINIMUM_VERSION && uv_version() >= UV_MINIMUM_VERSION,
        "libuv older than 1.19");
    printf("  uv.h %#lx, library %#x\n", antuv_c_constant(20), uv_version());
    pass("binding layout and constants match uv.h");
}

// First loop in the process: it creates libuv's auxiliary pool, so the
// pool size, CPUs and priority set here are exact.
void lifecycleAndPlacement(ProcessorId[] cpus)
{
    UvLoopOptions o = testOptions();
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
    version (linux)
        check(loop.ioUring == gIoUring, gIoUring ? "io_uring requested but not configured "
            ~ "(needs libuv 1.49 and Linux 5.10.186)" : "io_uring configured unasked");
    else
        check(!loop.ioUring, "io_uring outside Linux");
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

// With UvLoopOptions.ioUring, create builds libuv's ring before the loop
// takes requests, so the kernel's polling thread runs before the first
// request. Left to itself, libuv builds the ring inside that request.
version (linux) void ioUringRingAtCreate()
{
    // An earlier loop's polling thread exits shortly after its close.
    check(waitUntil(() => pollingThreads() == 0), "an earlier ring's polling thread lingers");
    auto loop = UvLoop.create(testOptions());
    check(loop.ioUring, "io_uring not configured");
    // The thread takes its name when it first runs: allow it a moment.
    if (waitUntil(() => pollingThreads() != 0, 1.seconds))
    {
        check(loop.close().loopClose == 0, "ring close report");
        pass("create builds the io_uring ring before the first request");
        return;
    }
    // No polling thread yet: a request tells a refused ring, where libuv
    // uses the pool, from one that nothing has built.
    auto r = UvRequest.stat("/");
    check(readNow(loop, &r) && r.succeeded, "stat through the loop");
    check(!waitUntil(() => pollingThreads() != 0, 200.msecs),
        "the first request built the ring, not create");
    check(loop.close().loopClose == 0, "ring close report");
    printf("SKIP io_uring ring at create: the kernel refused the ring\n");
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
    auto loop = UvLoop.create(testOptions());
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
    auto loop = UvLoop.create(testOptions());
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
    // libuv's loop time is whole milliseconds, and on Linux it reads
    // CLOCK_MONOTONIC_COARSE when that ticks every millisecond or faster.
    // Measured from a precise time taken at submit, a timer can therefore
    // fire up to a tick plus a millisecond early (1.39 ms in libuv alone on
    // a 1000 Hz kernel), so the bound is 2 ms.
    foreach (i, ref r; records)
    {
        check(r.done && r.result == 0, "timer result");
        check(atomicLoad(state.elapsed[i]) >= (cast(long) r.timeoutMs - 2) * 10_000, "timer fired early");
    }
    loop.close();
    pass("timers fire after their timeout");
}

void completionRetention()
{
    auto ring = Ring.create(2);
    auto loop = UvLoop.create(testOptions());
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
    UvLoopOptions o = testOptions();
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
    UvLoopOptions o = testOptions();
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
    UvLoopOptions o = testOptions();
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
    UvLoopOptions o = testOptions();
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
    UvLoopOptions o = testOptions();
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
    UvLoopOptions o = testOptions();
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
    auto loop = UvLoop.create(testOptions());
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
    // The loop runs a request's callback after its trigger or event has
    // woken the Fiber, so the count can trail until close joins the loop
    // thread; after that every callback has run.
    check(atomicLoad(gPublished) == atomicLoad(gSubmitted), "publications differ from submissions");
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
    auto loop = UvLoop.create(testOptions());
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
    check(allocations == 1 && atomicLoad(heap.offLoopAllocations) == 1,
        "allocated once, on libuv's thread");
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

    // A directory fails with EISDIR and size 0 on every platform, before the
    // loop allocates or reads anything, with or without a caller buffer.
    {
        immutable allocated = allocations;
        UvRequest directory;
        directory.op = UvOp.readFile;
        directory.path = dir.toStringz;
        directory.allocator = counting;
        check(readNow(loop, &directory) && directory.done && directory.result == UV_EISDIR
            && directory.errorName == "EISDIR" && directory.size == 0 && !directory.ownsBuffer
            && allocations == allocated, "a directory is refused before allocating");
        ubyte[64] into;
        directory = UvRequest.init;
        directory.op = UvOp.readFile;
        directory.path = dir.toStringz;
        directory.buffer = into.ptr;
        directory.length = into.length;
        check(readNow(loop, &directory) && directory.done && directory.result == UV_EISDIR
            && directory.size == 0, "a directory is refused with a caller buffer");
    }

    // A read that fails after allocating: the loop frees the block. On
    // Windows the file is one whose bytes another handle has locked: it
    // opens (libuv shares everything) and reports its size, then its read
    // fails with EBUSY. On Linux it is a sysfs attribute, which reports a
    // page as its size; the loopback device's link speed then fails to read
    // with EINVAL.
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
        immutable failedReports = true;
    }
    else
    {
        // Checked first, so another kernel skips rather than fails.
        import std.exception : collectException;
        import std.file : exists, getSize, read;
        auto failedPath = "/sys/class/net/lo/speed";
        immutable failedReports = exists(failedPath) && getSize(failedPath) != 0
            && collectException(read(failedPath)) !is null;
    }
    if (!failedReports)
        printf("  SKIP failed-read free: %s does not report a size and fail to read\n",
            failedPath.toStringz);
    else
    {
        immutable allocated = allocations, freedBefore = deallocations;
        UvRequest failed;
        failed.op = UvOp.readFile;
        failed.path = failedPath.toStringz;
        failed.allocator = counting;
        check(readNow(loop, &failed) && failed.done && failed.result < 0 && !failed.ownsBuffer,
            "a read that fails after allocating");
        version (Windows)
            check(failed.errorName == "EBUSY", "the locked read is EBUSY");
        check(allocations == allocated + 1 && deallocations == freedBefore + 1
            && atomicLoad(heap.loopFrees) == 1 && atomicLoad(heap.mismatches) == 0,
            "the loop freed the failed read's block");
        printf("  a read failing with %.*s after allocating: the loop freed the block\n",
            cast(int) failed.errorName.length, failed.errorName.ptr);
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
        auto loop = UvLoop.create(testOptions());
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
        pass("readFile into mimalloc v3 on libuv's thread, freed on another");
    }
}

// The per-operation constructors, and the loop's default allocator: a
// readFile that names no allocator takes the loop's, a request's own wins,
// and both run on libuv's thread.
void constructorsAndDefaultAllocator(string path, const(ubyte)[] expected)
{
    CountingHeap loopHeap, ownHeap;
    auto loopAllocator = ActorAllocator(&loopHeap, &countingAllocate, &countingDeallocate);
    auto ownAllocator = ActorAllocator(&ownHeap, &countingAllocate, &countingDeallocate);
    auto o = testOptions();
    o.allocator = loopAllocator;
    auto loop = UvLoop.create(o);
    auto cpath = path.toStringz;

    auto r = UvRequest.readFile(cpath);
    check(readNow(loop, &r) && r.succeeded && r.buffer[0 .. r.length] == expected
        && (cast(size_t) r.buffer & 63) == 0, "readFile(path) through the loop's allocator");
    check(atomicLoad(loopHeap.allocations) == 1 && atomicLoad(loopHeap.offLoopAllocations) == 1
        && atomicLoad(ownHeap.allocations) == 0, "the loop's allocator, on libuv's thread");
    r.freeBuffer();
    check(atomicLoad(loopHeap.deallocations) == 1 && atomicLoad(loopHeap.mismatches) == 0, "freed once");

    r = UvRequest.readFile(cpath, ownAllocator, 128);
    check(readNow(loop, &r) && r.succeeded && (cast(size_t) r.buffer & 127) == 0,
        "readFile(path, allocator, alignment)");
    check(atomicLoad(ownHeap.allocations) == 1 && atomicLoad(loopHeap.allocations) == 1,
        "the request's allocator wins");
    r.freeBuffer();

    auto into = new ubyte[expected.length];
    r = UvRequest.readFile(cpath, into);
    check(readNow(loop, &r) && r.succeeded && r.result == expected.length && !r.ownsBuffer
        && into == expected, "readFile(path, buffer)");

    r = UvRequest.stat(cpath);
    check(readNow(loop, &r) && r.succeeded && r.size == expected.length, "stat(path)");
    r = UvRequest.open(cpath, UV_FS_O_RDONLY);
    check(readNow(loop, &r) && r.succeeded, "open(path, flags)");
    immutable file = cast(int) r.result;
    r = UvRequest.fstat(file);
    check(readNow(loop, &r) && r.succeeded && r.size == expected.length, "fstat(file)");
    ubyte[16] tail;
    immutable offset = expected.length > 16 ? expected.length - 16 : 0;
    r = UvRequest.read(file, tail[], cast(long) offset);
    check(readNow(loop, &r) && r.succeeded && r.result == expected.length - offset
        && tail[0 .. expected.length - offset] == expected[offset .. $], "read(file, buffer, offset)");
    r = UvRequest.close(file);
    check(readNow(loop, &r) && r.succeeded, "close(file)");
    r = UvRequest.timer(1);
    check(readNow(loop, &r) && r.succeeded, "timer(ms)");
    r = UvRequest.nop();
    check(readNow(loop, &r) && r.succeeded, "nop()");

    auto report = loop.close();
    check(report.completed == 10 && report.loopClose == 0, "constructors close report");
    check(atomicLoad(loopHeap.allocations) == atomicLoad(loopHeap.deallocations)
        && atomicLoad(ownHeap.allocations) == atomicLoad(ownHeap.deallocations), "unbalanced allocators");
    pass("one fresh record per operation; the loop's default allocator and the request's own");
}

// An allocator that only counts, with atomics: several pool threads may
// allocate at once.
struct StormHeap
{
    shared long allocations, deallocations;
}

void* stormAllocate(void* context, size_t bytes, size_t alignment) nothrow @nogc @system
{
    atomicOp!"+="((cast(StormHeap*) context).allocations, 1);
    return alignedAllocate(alignment, (bytes + alignment - 1) & ~(alignment - 1));
}

void stormDeallocate(void* context, void* memory, size_t, size_t) nothrow @nogc @system
{
    atomicOp!"+="((cast(StormHeap*) context).deallocations, 1);
    alignedFree(memory);
}

// Whole-file reads cancelled in flight, through a counting allocator and the
// C runtime: every request publishes once, a cancelled one with UV_ECANCELED
// and no block, and every block allocated is freed exactly once.
void readFileCancelStorm(string path, const(ubyte)[] expected)
{
    auto loop = UvLoop.create(testOptions());
    StormHeap heap;
    auto storm = ActorAllocator(&heap, &stormAllocate, &stormDeallocate);
    enum perKind = 72;
    auto requests = new UvRequest[2 * perKind];
    auto cpath = path.toStringz;
    foreach (i, ref r; requests)
    {
        r = i < perKind ? UvRequest.readFile(cpath, storm) : UvRequest.readFile(cpath);
        check(loop.submit(&r) == UvSubmit.accepted, "storm submit");
    }
    // Let the pool get going, so the cancels below meet jobs that are queued,
    // running, or between their two halves, and blocks already allocated.
    size_t finishedCount()
    {
        size_t n;
        foreach (ref r; requests) n += r.finished;
        return n;
    }
    check(waitUntil(() => finishedCount() >= 6), "storm reads did not start");
    foreach_reverse (ref r; requests)
        check(loop.cancel(&r), "storm cancel refused");
    check(waitUntil(() => allFinished(requests)), "storm requests did not publish");
    size_t cancelled, completed;
    foreach (ref r; requests)
    {
        check(r.done, "a storm request was abandoned");
        if (r.result == UV_ECANCELED)
        {
            ++cancelled;
            check(!r.ownsBuffer, "a cancelled read owns a block");
        }
        else
        {
            ++completed;
            check(r.succeeded && r.length == expected.length && r.buffer[0 .. r.length] == expected,
                "a storm read that completed");
            r.freeBuffer();
        }
    }
    check(completed >= 6 && cancelled > 0, "the storm did not mix completed and cancelled reads");
    auto report = loop.close();
    check(report.loopClose == 0 && report.cancelled == cancelled, "storm close report");
    check(atomicLoad(heap.allocations) == atomicLoad(heap.deallocations),
        "a storm block leaked or was freed twice");
    printf("  %d cancelled, %d completed, %lld blocks\n", cast(int) cancelled, cast(int) completed,
        atomicLoad(heap.allocations));
    pass("readFile cancelled in flight publishes once and frees every block");
}

// A UDP socket pair on the loopback: stamped sends through a connected
// client to a server, replies to the datagram's sender, the window exhausted
// and counted, releases from another thread, truncation, a close with a
// datagram still held, the ring-capacity check, malformed sends, resolution
// into a slice, address text, and a socket left open at the loop's close.
void udpLoopback()
{
    auto loop = UvLoop.create(testOptions());
    auto ring = Ring.create(256);
    bool submitNow(UvRequest* r)
    {
        return loop.submit(r) == UvSubmit.accepted && waitUntil(() => r.finished);
    }
    UvDatagram* popped(ulong tag, string what)
    {
        RingElement e;
        check(ring.tryPop(e) && e.tag == tag, what);
        auto d = cast(UvDatagram*) cast(size_t) e.pointer;
        check((e.payload[0] & 0xFFFF_FFFF) == d.length && (e.payload[0] >> 32) == d.flags
            && e.payload[1] == cast(ulong) d.arrivalNanos && e.payload[2] == d.socket,
            "the element describes its datagram");
        return d;
    }
    long stampIn(const(ubyte)[] bytes)
    {
        long stamp;
        foreach (k; 0 .. 8) stamp |= cast(long) bytes[k] << (8 * k);
        return stamp;
    }

    UvUdpOptions serverOptions;
    serverOptions.bind = UvAddress.loopback4(0);
    serverOptions.completions = ring;
    serverOptions.tag = 1;
    serverOptions.window = 16;
    serverOptions.maxDatagram = 64;
    auto open = UvRequest.udpOpen(&serverOptions);
    check(submitNow(&open) && open.succeeded, "server open");
    immutable server = cast(int) open.result;
    immutable serverPort = cast(ushort) open.size;
    check(serverPort != 0, "the server's bound port");

    auto clientOptions = serverOptions;
    clientOptions.connect = UvAddress.loopback4(serverPort);
    clientOptions.tag = 2;
    open = UvRequest.udpOpen(&clientOptions);
    check(submitNow(&open) && open.succeeded, "client open");
    immutable client = cast(int) open.result;
    immutable clientPort = cast(ushort) open.size;
    check(client != server && clientPort != 0, "the client's id and port");

    // Eight stamped datagrams, delivered in order with the sender named.
    ubyte[32] packet;
    foreach (i; 0 .. 8)
    {
        packet[8] = cast(ubyte) i;
        packet[9 .. $] = cast(ubyte)(0x40 + i);
        auto send = UvRequest.udpSend(client, packet[]);
        send.stampAt = 0;
        immutable before = UvLoop.nanos();
        check(submitNow(&send) && send.succeeded && send.result == packet.length, "client send");
        immutable stamp = stampIn(packet[]);
        check(stamp >= before && stamp <= UvLoop.nanos(), "the transmit stamp");
    }
    check(waitUntil(() => loop.udpStats(server).received == 8), "the server received eight");
    UvAddress clientAddress;
    foreach (i; 0 .. 8)
    {
        auto d = popped(1, "an element per datagram");
        check(d.socket == server && d.length == packet.length && d.flags == 0, "the header");
        check(d.peer.family == UvFamily.ipv4 && d.peer.port == clientPort
            && d.peer.bytes[0 .. 4] == [cast(ubyte) 127, 0, 0, 1], "the sender");
        check(d.arrivalNanos >= stampIn(d.bytes) && d.arrivalNanos <= UvLoop.nanos(),
            "arrival after transmit");
        check(d.bytes[8] == i && d.bytes[9] == 0x40 + i && d.bytes.length == packet.length, "the data");
        clientAddress = d.peer;
        d.release();
    }
    {
        RingElement e;
        check(!ring.tryPop(e), "nothing more on the ring");
    }

    // A reply to the sender arrives on the client's tag.
    auto reply = UvRequest.udpSend(server, cast(const(ubyte)[]) "pong", clientAddress);
    check(submitNow(&reply) && reply.result == 4, "server reply");
    check(waitUntil(() => loop.udpStats(client).received == 1), "the client received the reply");
    {
        auto d = popped(2, "the reply's element");
        check(d.bytes == cast(const(ubyte)[]) "pong" && d.peer.port == serverPort, "the reply");
        d.release();
    }

    // Twenty-four sends into a window of sixteen that nobody releases: the
    // rest are read, dropped and counted, and the socket keeps working.
    foreach (i; 0 .. 24)
    {
        packet[8] = cast(ubyte) i;
        auto send = UvRequest.udpSend(client, packet[]);
        check(submitNow(&send) && send.succeeded, "burst send");
    }
    bool burstSettled()
    {
        auto s = loop.udpStats(server);
        return s.received + s.dropped == 32;
    }
    check(waitUntil(&burstSettled), "the burst delivered or dropped");
    auto stats = loop.udpStats(server);
    check(stats.received == 24 && stats.dropped == 8, "sixteen delivered, eight dropped");
    UvDatagram*[] held;
    {
        RingElement e;
        while (ring.tryPop(e)) held ~= cast(UvDatagram*) cast(size_t) e.pointer;
    }
    check(held.length == 16, "sixteen held");
    spawn({ foreach (d; held) d.release(); }).join();
    packet[8] = 99;
    {
        auto send = UvRequest.udpSend(client, packet[]);
        check(submitNow(&send) && send.succeeded, "send after the releases");
    }
    check(waitUntil(() => loop.udpStats(server).received == 25), "delivered after the releases");
    {
        auto d = popped(1, "the element after the releases");
        check(d.bytes[8] == 99, "the data after the releases");
        d.release();
    }

    // A datagram larger than maxDatagram is cut and flagged.
    auto big = pattern(5, 100);
    {
        auto send = UvRequest.udpSend(client, big);
        check(submitNow(&send) && send.result == 100, "a big send");
    }
    check(waitUntil(() => loop.udpStats(server).received == 26), "the big one delivered");
    {
        auto d = popped(1, "the big one's element");
        check(d.length == 64 && (d.flags & UvDatagramFlag.truncated) != 0 && d.bytes == big[0 .. 64],
            "truncated to maxDatagram");
        check(loop.udpStats(server).truncated == 1, "truncation counted");
        d.release();
    }

    // A datagram held across its socket's close stays readable; its release
    // frees the window. A second close is EBADF.
    packet[8] = 7;
    {
        auto send = UvRequest.udpSend(client, packet[]);
        check(submitNow(&send) && send.succeeded, "the last send to the server");
    }
    check(waitUntil(() => loop.udpStats(server).received == 27), "the last one delivered");
    {
        auto d = popped(1, "the last one's element");
        auto close = UvRequest.udpClose(server);
        check(submitNow(&close) && close.succeeded, "server close");
        check(d.bytes[8] == 7 && d.socket == server, "held across the close");
        d.release();
        close = UvRequest.udpClose(server);
        check(submitNow(&close) && close.result == UV_EBADF, "closing a closed socket");
    }

    // The ring's capacity is checked against the windows that share it: the
    // client holds 16 of 256.
    auto third = serverOptions;
    third.window = 241;
    open = UvRequest.udpOpen(&third);
    check(submitNow(&open) && open.result == UV_ENOBUFS, "a window the ring cannot hold");
    third.window = 240;
    open = UvRequest.udpOpen(&third);
    check(submitNow(&open) && open.succeeded, "a window that fills the ring exactly");
    {
        auto close = UvRequest.udpClose(cast(int) open.result);
        check(submitNow(&close) && close.succeeded, "third close");
    }

    // Malformed sends are refused at submit; an unopened socket at publication.
    {
        auto bad = UvRequest.udpSend(client, packet[]);
        bad.stampAt = packet.length - 4;
        check(loop.submit(&bad) == UvSubmit.invalid, "a stamp past the end");
        bad = UvRequest.udpSend(-1, packet[]);
        check(loop.submit(&bad) == UvSubmit.invalid, "no socket");
        bad = UvRequest.udpSend(200, packet[]);
        check(submitNow(&bad) && bad.result == UV_EBADF, "an unopened socket");
    }

    // Resolution on the pool, into a slice of one and of several.
    UvAddress[1] one;
    auto resolve = UvRequest.resolve("localhost", 7, one[]);
    check(submitNow(&resolve) && resolve.result == 1 && one[0].valid && one[0].port == 7,
        "localhost into one address");
    UvAddress[8] several;
    resolve = UvRequest.resolve("localhost", 7, several[]);
    check(submitNow(&resolve) && resolve.result >= 1 && resolve.result <= several.length,
        "localhost into several");
    char[64] text;
    check(UvAddress.loopback4(80).format(text[]) == "127.0.0.1:80", "IPv4 text");
    check(UvAddress.ipv6("::1", 80).format(text[]) == "[::1]:80", "IPv6 text");
    check(!UvAddress.ipv4("not an address", 1).valid, "a parse failure is unset");
    check(UvAddress.init.format(text[]) == "unset", "unset text");

    // The client is left open: the loop's close closes it.
    auto report = loop.close();
    check(report.loopClose == 0 && report.abandoned == 0 && report.failure is null, "UDP close report");
    ring.destroy();
    pass("UDP on the loopback: stamped sends, replies, a dropped burst, truncation, "
        ~ "holds across close, the ring check, and resolution");
}

// ---- the @safe surface --------------------------------------------------------
// The shapes @safe code uses over the loopback: an inbox and an outbox,
// datagrams as values released by destruction, by hand or by the next pop,
// the blocking submit, the slice address parsers and the string resolve.
// What stays @system here: the loop's creation and close, the inbox's
// destruction, and one forged element pushed to exercise the tag registry.

struct SafeSockets
{
    int server, client;
    ushort serverPort, clientPort;
}

SafeSockets safeOpen(UvLoop loop, UvInbox inbox) @safe
{
    SafeSockets s;
    auto serverOptions = inbox.options(1, 8, 64);
    auto clientOptions = inbox.options(2, 8, 64);
    serverOptions.bind = UvAddress.loopback4(0);
    auto open = UvRequest.udpOpen(serverOptions);
    check(loop.submitAndBlock(open) == UvSubmit.accepted && open.succeeded,
        "server open through the blocking submit");
    s.server = cast(int) open.result;
    s.serverPort = cast(ushort) open.size;

    clientOptions.bind = UvAddress.loopback4(0);
    clientOptions.connect = UvAddress.ipv4("127.0.0.1", s.serverPort);
    check(clientOptions.connect.valid && clientOptions.connect.port == s.serverPort,
        "an address parsed from a slice");
    check(!UvAddress.ipv4("300.1.1.1", 1).valid && !UvAddress.ipv6("not::an::address", 1).valid,
        "slice parse failures are unset");
    open = UvRequest.udpOpen(clientOptions);
    check(loop.submitAndBlock(open) == UvSubmit.accepted && open.succeeded,
        "client open through the blocking submit");
    s.client = cast(int) open.result;
    s.clientPort = cast(ushort) open.size;
    check(s.client != s.server && s.clientPort != 0, "the client's id and port");
    return s;
}

// A loopback send publishes within microseconds; a full outbox waits for
// its oldest slot.
UvSubmit sendWhenFree(UvOutbox outbox, int socket, scope const(ubyte)[] packet,
    UvAddress peer = UvAddress.init, uint stampAt = uint.max) @safe
{
    immutable deadline = MonoTime.currTime + 10.seconds;
    for (;;)
    {
        immutable r = outbox.send(socket, packet, peer, stampAt);
        if (r != UvSubmit.full || MonoTime.currTime >= deadline) return r;
        Thread.sleep(10.usecs);
    }
}

void safeExchange(UvLoop loop, UvInbox inbox, UvOutbox outbox, SafeSockets s) @safe
{
    import std.bitmanip : Endian, peek;

    // Six stamped packets through four slots: a slot is reused once its
    // send published, and the stamp lands in the outbox's copy.
    ubyte[32] packet;
    foreach (i; 0 .. 6)
    {
        packet[8] = cast(ubyte) i;
        check(sendWhenFree(outbox, s.client, packet[], UvAddress.init, 0) == UvSubmit.accepted,
            "a stamped send through the outbox");
    }
    check(waitUntil(() => loop.udpStats(s.server).received == 6), "six received through the outbox");
    check(waitUntil(() => outbox.settle() == 0), "every send published");
    check(outbox.accepted == 6 && outbox.sent == 6 && outbox.failed == 0 && outbox.lastError == 0,
        "the outbox's counts");
    UvAddress sender;
    {
        UvReceived d;
        foreach (i; 0 .. 6)
        {
            check(inbox.tryPop(d) && d.held && d.tag == 1 && d.socket == s.server && d.flags == 0,
                "a datagram per send, with its header");
            auto bytes = d.bytes();
            check(bytes.length == packet.length && bytes[8] == i, "its data");
            immutable stamp = bytes.peek!(long, Endian.littleEndian)();
            check(stamp > 0 && stamp <= d.arrivalNanos, "stamped before arrival");
            sender = d.peer;
        }
        check(!inbox.tryPop(d) && !d.held, "the ring is empty, and the pop released the last one");
    }
    check(inbox.popped == 6 && sender.port == s.clientPort && sender.family == UvFamily.ipv4,
        "the sender");

    // A reply reaches the client's tag. The value is moved out of its scope;
    // the source holds nothing, the destination reads the datagram and
    // releases it by hand, idempotently.
    UvReceived kept;
    {
        UvReceived d;
        check(sendWhenFree(outbox, s.server, cast(const(ubyte)[]) "pong", sender) == UvSubmit.accepted,
            "a reply through the outbox");
        check(waitUntil(() => loop.udpStats(s.client).received == 1), "the reply arrived");
        check(inbox.tryPop(d) && d.tag == 2 && d.bytes == cast(const(ubyte)[]) "pong", "the reply");
        kept = move(d);
        check(!d.held && kept.held && kept.bytes.length == 4, "moved out: the source holds nothing");
    }
    check(kept.bytes == cast(const(ubyte)[]) "pong" && kept.peer.port == s.serverPort,
        "the moved value reads its datagram");
    kept.release();
    check(!kept.held && kept.bytes.length == 0, "released by hand: empty");
    kept.release();

    // Eight datagrams on the ring exhaust a window of eight and the ninth is
    // dropped; values hold them past the pop; destroying the values frees
    // the window and the next one arrives.
    {
        UvReceived[8] held;
        foreach (i; 0 .. 9)
        {
            packet[8] = cast(ubyte)(10 + i);
            check(sendWhenFree(outbox, s.client, packet[]) == UvSubmit.accepted, "a burst send");
        }
        check(waitUntil(() {
            auto st = loop.udpStats(s.server);
            return st.received + st.dropped == 15;
        }), "the burst delivered or dropped");
        check(loop.udpStats(s.server).received == 14 && loop.udpStats(s.server).dropped == 1,
            "eight delivered, one dropped");
        foreach (i; 0 .. 8)
            check(inbox.tryPop(held[i]) && held[i].held && held[i].bytes[8] == 10 + i,
                "eight held in values, in order");
        UvReceived none;
        check(!inbox.tryPop(none), "nothing more on the ring");
        packet[8] = 99;
        check(sendWhenFree(outbox, s.client, packet[]) == UvSubmit.accepted,
            "a send while the values hold the window");
        check(waitUntil(() => loop.udpStats(s.server).dropped == 2), "dropped while held");
    }
    packet[8] = 100;
    check(sendWhenFree(outbox, s.client, packet[]) == UvSubmit.accepted,
        "a send after the values were destroyed");
    check(waitUntil(() => loop.udpStats(s.server).received == 15),
        "delivered once destruction released the window");
    {
        UvReceived d;
        check(inbox.tryPop(d) && d.bytes[8] == 100, "the datagram after the release");
    }

    // Resolution from a string; the blocking submit's refusal of a callback;
    // the clock and the formatter from @safe.
    UvAddress[4] found;
    auto resolve = UvRequest.resolve("localhost", 7, found[]);
    check(loop.submitAndBlock(resolve) == UvSubmit.accepted && resolve.result >= 1
        && found[0].valid && found[0].port == 7, "localhost resolved from a string");
    auto nop = UvRequest.nop();
    nop.callback = &noopCallback;
    check(loop.submitAndBlock(nop) == UvSubmit.invalid, "the blocking submit refuses a callback");
    nop = UvRequest.nop();
    check(loop.submitAndBlock(nop) == UvSubmit.accepted && nop.done, "a nop through the blocking submit");
    check(UvLoop.nanos() > 0, "the clock from @safe");
    char[64] text;
    check(UvAddress.ipv6("::1", 80).format(text[]) == "[::1]:80", "formatting from @safe");
}

void safeAfterForgery(UvLoop loop, UvInbox inbox, UvOutbox outbox, SafeSockets s) @safe
{
    ubyte[16] packet;
    packet[0] = 42;
    check(sendWhenFree(outbox, s.client, packet[]) == UvSubmit.accepted, "a send behind the forged element");
    check(waitUntil(() => loop.udpStats(s.server).received == 16), "it arrived");
    {
        UvReceived d;
        check(inbox.tryPop(d) && d.bytes.length == 16 && d.bytes[0] == 42,
            "the pop skipped the forged element to the datagram");
        check(inbox.unexpected == 1, "the forged element counted as unexpected");
    }
    auto close = UvRequest.udpClose(s.client);
    check(loop.submitAndBlock(close) == UvSubmit.accepted && close.succeeded, "client close from @safe");
    close = UvRequest.udpClose(s.server);
    check(loop.submitAndBlock(close) == UvSubmit.accepted && close.succeeded, "server close from @safe");
    outbox.close();
    check(outbox.send(s.client, packet[]) == UvSubmit.invalid, "a closed outbox refuses");
}

// DIP1000 is on when @safe code may take a local's address into a scope
// pointer; no version identifier says so.
enum dip1000Enabled = __traits(compiles, () @safe { int x; scope int* p = &x; });

static if (dip1000Enabled)
{
    // Under DIP1000 a datagram's bytes cannot leave the value holding them,
    // neither returned nor assigned to a longer-lived slice, while reading
    // them in place is fine. (A nested function with a `ref` parameter
    // would have `return` inferred for it, so the checks use locals.)
    static assert(!__traits(compiles, {
        static const(ubyte)[] leak() @safe { UvReceived d; return d.bytes(); }
    }));
    static assert(!__traits(compiles, {
        static const(ubyte)[] leak() @safe
        {
            const(ubyte)[] outer;
            {
                UvReceived d;
                outer = d.bytes();
            }
            return outer;
        }
    }));
    static assert(__traits(compiles, {
        static size_t fine() @safe
        {
            UvReceived d;
            auto bytes = d.bytes();
            return bytes.length;
        }
    }));
}

void safeUdpLoopback()
{
    auto loop = UvLoop.create(testOptions());
    auto inbox = new UvInbox(64);
    check(inbox.capacity == 64, "the inbox's ring");
    auto sockets = safeOpen(loop, inbox);
    auto outbox = new UvOutbox(loop, 4, 64);
    check(outbox.slots == 4 && outbox.capacity == 64, "the outbox's shape");
    safeExchange(loop, inbox, outbox, sockets);
    // An element from anywhere but the loop is discarded by tag, never read
    // as a datagram.
    {
        RingElement forged;
        forged.tag = 77;
        forged.pointer = 0xDEAD;
        check(inbox.ring.tryPush(forged) == RingPush.pushed, "a forged element pushed by @system code");
    }
    safeAfterForgery(loop, inbox, outbox, sockets);
    // The registry's refusals: windows beyond the ring, and a zero window.
    bool threw;
    try inbox.options(3, 64);
    catch (Exception) threw = true;
    check(threw, "windows beyond the ring's capacity are refused");
    threw = false;
    try inbox.options(1, 0);
    catch (Exception) threw = true;
    check(threw, "a zero window is refused");
    auto report = loop.close();
    check(report.loopClose == 0 && report.abandoned == 0 && report.failure is null,
        "close report after the @safe surface");
    inbox.destroy();
    pass("the @safe surface on the loopback: inbox and outbox, values released by destruction, "
        ~ "by hand and by the next pop, a moved value, the held window, the blocking submit, "
        ~ "parsers from slices, a forged element discarded");
}

// A Fiber on the loop thread spawned from @safe code: the wait by reference
// for a timer and a stat from a string, a sleep, a semaphore, and the
// refusal of a request that names a callback.
void safeFiberWait(string path)
{
    UvFibers fibers;
    shared int finished;
    UvLoopOptions options = testOptions();
    // `create` returns once the loop serves, before the start hook has
    // run, so the Fiber is spawned from the hook itself.
    options.start = () {
        auto loop = UvLoop.current;
        fibers = new UvFibers(loop);
        fibers.spawn(() @safe {
            auto domain = fibers.domain;
            auto r = UvRequest.timer(1);
            r.event = new FiberEvent(domain);
            check(submitAndWait(loop, r) == UvSubmit.accepted && r.done, "a timer awaited by reference");
            r = UvRequest.stat(path);
            r.event = new FiberEvent(domain);
            check(submitAndWait(loop, r) == UvSubmit.accepted && r.succeeded && r.size == 4096,
                "stat from a string, awaited by reference");
            r = UvRequest.nop();
            r.event = new FiberEvent(domain);
            r.callback = &noopCallback;
            check(submitAndWait(loop, r) == UvSubmit.invalid, "the wait refuses a callback");
            FiberDomain.sleepFor(1.msecs);
            auto gate = new FiberSemaphore(domain, 1);
            gate.wait();
            gate.post();
            check(gate.available == 1, "a semaphore from @safe");
            atomicStore(finished, 1);
        });
    };
    options.step = () => fibers.step();
    options.stop = () { fibers.shutdown(); };
    auto loop = UvLoop.create(options);
    check(waitUntil(() => atomicLoad(finished) == 1), "the @safe Fiber finished");
    auto report = loop.close();
    check(report.failure is null, "close after the @safe Fiber");
    pass("@safe Fiber bodies on the loop thread: spawn, the wait by reference for a timer and a "
        ~ "stat from a string, a sleep, a semaphore, a callback refused");
}

// A whole-file read abandoned inside its allocator, which waits for the
// thread that calls close: close abandons the job, the job frees the block
// itself once the allocator returns, and abandonedJobs says when it ended.
struct GateHeap
{
    shared long allocations, deallocations, wrongBlocks;
    shared int open;
    void* block; // written and read on the job's thread
}

// On libuv's thread: no druntime.
void napMs(uint ms) nothrow @nogc @system
{
    version (Windows)
    {
        import core.sys.windows.winbase : Sleep;
        Sleep(ms);
    }
    else
    {
        import core.sys.posix.time : nanosleep, timespec;
        auto ts = timespec(ms / 1000, (ms % 1000) * 1_000_000L);
        nanosleep(&ts, null);
    }
}

void* gateAllocate(void* context, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto heap = cast(GateHeap*) context;
    atomicOp!"+="(heap.allocations, 1);
    while (atomicLoad!(MemoryOrder.acq)(heap.open) == 0) napMs(1);
    heap.block = alignedAllocate(alignment, (bytes + alignment - 1) & ~(alignment - 1));
    return heap.block;
}

void gateDeallocate(void* context, void* memory, size_t, size_t) nothrow @nogc @system
{
    auto heap = cast(GateHeap*) context;
    atomicOp!"+="(heap.deallocations, 1);
    if (memory !is heap.block) atomicOp!"+="(heap.wrongBlocks, 1);
    alignedFree(memory);
}

void abandonReadFileInAllocator(string path)
{
    UvLoopOptions o = testOptions();
    o.closeGrace = 20.msecs;
    o.abandonAfter = 50.msecs;
    auto loop = UvLoop.create(o);
    GateHeap heap;
    auto r = new UvRequest;
    *r = UvRequest.readFile(path.toStringz, ActorAllocator(&heap, &gateAllocate, &gateDeallocate));
    check(loop.submit(r) == UvSubmit.accepted, "gated submit");
    check(waitUntil(() => atomicLoad(heap.allocations) == 1), "the job did not reach the allocator");
    check(loop.abandonedJobs == 0, "abandonedJobs before close");
    auto report = loop.close();
    check(report.abandoned == 1 && report.loopClose == UV_EBUSY, "close did not abandon the read");
    check(r.state == UvRequestState.abandoned && r.result == UV_ECANCELED && !r.ownsBuffer,
        "abandoned read state");
    // The job is still inside the allocator: nothing can have been freed.
    check(loop.abandonedJobs == 1 && atomicLoad(heap.deallocations) == 0,
        "abandonedJobs after close");
    atomicStore!(MemoryOrder.rel)(heap.open, 1);
    check(waitUntil(() => loop.abandonedJobs == 0), "the abandoned job did not end");
    check(atomicLoad(heap.deallocations) == 1 && atomicLoad(heap.wrongBlocks) == 0,
        "the abandoned job did not free its block once");
    pass("close abandons a readFile inside its allocator; the job frees the block and leaves abandonedJobs");
}

// An operation that blocks in an auxiliary thread cannot be cancelled by
// libuv: close abandons it and leaves the loop allocated. The test then
// unblocks it so the auxiliary thread is free again. Linux opens a FIFO
// that has no writer; Windows opens a named pipe and reads it before its
// server writes, because opening a pipe there does not block.
void abandonBlocked(string dir)
{
    UvLoopOptions o = testOptions();
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
    version (linux) if (loop.ioUring)
    {
        // Through io_uring no thread blocks here: the kernel's open of a
        // FIFO did not wait for a writer (Linux 6.6), and libuv could cancel
        // it in the kernel if it did. Close has nothing to abandon.
        check(report.abandoned == 0 && report.loopClose == 0 && r.done
            && (r.result >= 0 || r.result == UV_ECANCELED),
            "close through io_uring: abandoned " ~ report.abandoned.to!string ~ ", loopClose "
            ~ report.loopClose.to!string ~ "; the open " ~ r.state.to!string ~ " with "
            ~ r.result.to!string);
        if (r.result >= 0) close(cast(int) r.result);
        pass("close through io_uring: an open of a FIFO with no writer blocks no thread");
        return;
    }
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
    {
        import std.process : environment;
        gIoUring = environment.get("ANTUV_TEST_IO_URING", "") == "1";
        if (gIoUring) printf("readFile through io_uring (ANTUV_TEST_IO_URING=1)\n");
    }
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
    version (linux) if (gIoUring) ioUringRingAtCreate();
    nopOrdering();
    timers();
    completionRetention();
    creditFlood();
    fullRingAndValidation();
    cancellation(statPath);
    closeInFlight();
    stepFailure();
    loopFibers(paths[7], contents[7]);
    safeFiberWait(statPath);
    workerFiberReads(paths, contents);
    readFileAllocators(dir, paths[8], contents[8], paths[0]);
    constructorsAndDefaultAllocator(paths[8], contents[8]);
    readFileCancelStorm(paths[8], contents[8]);
    udpLoopback();
    safeUdpLoopback();
    version (AntfarmMimallocV3) mimallocReads(paths[8], contents[8]);
    abandonReadFileInAllocator(paths[8]);
    abandonBlocked(dir);
    printf("ALL UV TESTS PASSED (%lld ms)\n", (MonoTime.currTime - started).total!"msecs");
    return 0;
}
