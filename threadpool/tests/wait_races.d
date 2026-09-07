module wait_races;

import threadpool;
import core.atomic;
import core.thread;
import core.time;
import std.stdio;
import threadpool.worker : requestStop;
version (Windows)
    import threadpool.sys.win_wait : gWait;
else version (linux)
    import threadpool.sys.linux_wait : gWait;

__gshared shared(int) calls, arrived, releasePump, pending, completed, stops;
__gshared int pauseAt;
__gshared bool deadlineMode, throwArmed;

bool pump(WorkerSelf*) nothrow @nogc
{
    if (atomicExchange(&pending, 0))
    {
        atomicFetchAdd(completed, 1);
        return true;
    }
    auto visit = atomicFetchAdd(calls, 1) + 1;
    if (visit == pauseAt)
    {
        atomicStore(arrived, 1);
        while (!atomicLoad(releasePump)) Thread.yield();
    }
    return false;
}

ManagedPumpResult managedPump(WorkerSelf* worker)
{
    if (pump(worker)) return ManagedPumpResult.again;
    auto visit = atomicLoad(calls);
    if (throwArmed && visit == 2) throw new Exception("armed pump failure");
    if (deadlineMode)
    {
        if (visit >= 3) atomicStore(completed, 1);
        // The second, armed pump supersedes the first pump's long deadline.
        return ManagedPumpResult.until((MonoTime.currTime
            + (visit == 1 ? 60.seconds : 1.msecs)).ticks);
    }
    return ManagedPumpResult.idle;
}

void stopped(WorkerSelf*) { atomicFetchAdd(stops, 1); }

bool await(ref shared int flag)
{
    auto deadline = MonoTime.currTime + 2.seconds;
    while (!atomicLoad(flag) && MonoTime.currTime < deadline) Thread.yield();
    return atomicLoad(flag) != 0;
}

void runRace(bool managed, int boundary, bool stopRace = false,
             bool deadline = false, bool throwOnRecheck = false)
{
    foreach (p; [&calls, &arrived, &releasePump, &pending, &completed, &stops])
        atomicStore(*p, 0);
    pauseAt = boundary;
    deadlineMode = deadline;
    throwArmed = throwOnRecheck;
    auto topology = CacheAwarePool.topology();
    auto lp = topology.processors[0];
    PoolOptions options;
    options.onlyProcessors = [ProcessorId(lp.group, lp.lpIndex)];
    if (managed) options.managedWorker = ManagedWorkerHooks(null, &managedPump, &stopped);
    else options.workerBody = &pump;
    auto pool = new CacheAwarePool(options);
    pool.start();
    scope (exit) pool.shutdown();
    if (boundary != 0)
    {
        auto reached = await(arrived);
        if (!reached) atomicStore(releasePump, 1);
        assert(reached, "worker did not reach the forced empty boundary");
    }
    if (stopRace) requestStop();
    else if (!deadline && !throwOnRecheck)
    {
        if (boundary == 0) Thread.sleep(20.msecs);
        atomicStore(pending, 1);
        pool.wakeAll();
    }
    atomicStore(releasePump, 1);
    auto progressed = stopRace ? true : await(throwOnRecheck ? stops : completed);
    if (throwOnRecheck && progressed)
        assert(atomicLoad(gWait[0].wakeWord) == 0, "exception left worker armed");
    // Rescue even a broken implementation so regression failure cannot hang.
    pool.wakeAll();
    pool.shutdown();
    assert(progressed, "publication/deadline required a rescue wake");
    if (managed) assert(atomicLoad(stops) == 1);
    assert(pool.workerFailures().length == (throwOnRecheck ? 1 : 0));
}

void suite()
{
    foreach (managed; [false, true])
    {
        foreach (boundary; [0, 1, 2]) runRace(managed, boundary);
        foreach (boundary; [1, 2]) runRace(managed, boundary, true);
    }
    runRace(true, 0, false, true);
    runRace(true, 0, false, false, true);
}

void main()
{
    suite();
    version (Windows)
    {
        import threadpool.sys.win_bindings;
        auto wait = pWaitOnAddress;
        auto single = pWakeByAddressSingle;
        auto all = pWakeByAddressAll;
        scope (exit) { pWaitOnAddress = wait; pWakeByAddressSingle = single; pWakeByAddressAll = all; }
        pWaitOnAddress = null;
        pWakeByAddressSingle = null;
        pWakeByAddressAll = null;
        suite();
        writeln("armed wait races passed: address and event backends");
    }
    else writeln("armed wait races passed: futex backend");
}
