module threadpool.pin;

import threadpool.os_scope : ensureOsScopeCaptured, inOsScope;
import threadpool.topology : LogicalProcessor, ProcessorId, cachedSnapshot, discover;

version (Windows)
    version = ThreadpoolOs;
version (linux)
    version = ThreadpoolOs;

/// Result of pinning a thread to one processor.
enum PinOutcome : ubyte
{
    pinned,           /// the thread now runs only on the processor
    unknownProcessor, /// the id names no discovered processor
    outsideOsScope,   /// the processor is outside the frozen OS scope
    osRejected,       /// the OS refused the affinity change
}

/// Pin the calling thread to `p`. Validates against the frozen OS scope on
/// both platforms first, then pins (Windows: CPU-set preference plus a 1-LP
/// group mask; Linux: a `sched_setaffinity` singleton). No unchecked variant
/// exists.
PinOutcome pinProcessor(ref const LogicalProcessor p) @nogc nothrow
{
    ensureOsScopeCaptured();
    if (!inOsScope(ProcessorId.of(p)))
        return PinOutcome.outsideOsScope;
    version (ThreadpoolOs)
        return pinOs(p) ? PinOutcome.pinned : PinOutcome.osRejected;
    else
        return PinOutcome.osRejected;
}

/// Look `id` up in the discovered topology (discovering it on first use), then
/// pin as above. An id the snapshot does not know is `unknownProcessor`.
PinOutcome pinProcessor(ProcessorId id) nothrow
{
    auto snap = cachedSnapshot();
    if (snap is null)
    {
        try
            cast(void) discover();
        catch (Exception)
            return PinOutcome.unknownProcessor;
        snap = cachedSnapshot();
    }
    auto p = snap.byId(id);
    if (p is null)
        return PinOutcome.unknownProcessor;
    return pinProcessor(*p);
}

version (Windows)
{
    import core.sys.windows.basetsd : ULONG_PTR;
    import core.sys.windows.winbase : GetCurrentThread, SetThreadAffinityMask,
        GetCurrentProcess, GetProcessAffinityMask;
    import core.sys.windows.windef : BOOL, FALSE, HANDLE, TRUE;
    import threadpool.sys.win_bindings;

    private bool pinOs(ref const LogicalProcessor t) @nogc nothrow
    {
        if (t.lpIndex >= 64) return false;
        // Group affinity can widen a thread beyond the process mask. Check
        // before changing CPU sets, including when discovery has been cached.
        ushort[64] groups;
        ushort count = cast(ushort) groups.length;
        auto process = GetCurrentProcess();
        if (!GetProcessGroupAffinity(process, &count, groups.ptr)) return false;
        if (count == 1)
        {
            if (t.group != groups[0]) return false;
            ULONG_PTR processMask, systemMask;
            if (!GetProcessAffinityMask(process, &processMask, &systemMask)) return false;
            if (processMask != 0 && (processMask & (cast(ULONG_PTR) 1 << t.lpIndex)) == 0)
                return false;
        }

        resolveOptionalApis();
        auto self = GetCurrentThread();

        // CPU Sets cooperate with parking / Thread Director but are a preference.
        if (pSetThreadSelectedCpuSets !is null)
        {
            uint id = t.cpuSetId;
            pSetThreadSelectedCpuSets(self, &id, 1);
        }

        // Hard 1-LP mask so a parked E-core cannot be satisfied by running on a P-core.
        GROUP_AFFINITY ga;
        ga.Mask = cast(KAFFINITY)(1UL << t.lpIndex);
        ga.Group = t.group;
        if (SetThreadGroupAffinity(self, &ga, null))
            return true;

        if (t.group != 0)
            return false;
        auto prev = SetThreadAffinityMask(self, cast(size_t)(1UL << t.lpIndex));
        return prev != 0;
    }

    /// eco=true → ExecutionSpeed throttling on (Win11 EcoQoS).
    /// eco=false → throttling off (Win11 HighQoS).
    bool applyPowerThrottling(bool eco) @nogc nothrow
    {
        resolveOptionalApis();
        if (pSetThreadInformation is null)
            return false;

        THREAD_POWER_THROTTLING_STATE st;
        st.Version = THREAD_POWER_THROTTLING_CURRENT_VERSION;
        st.ControlMask = THREAD_POWER_THROTTLING_EXECUTION_SPEED;
        st.StateMask = eco ? THREAD_POWER_THROTTLING_EXECUTION_SPEED : 0;
        return pSetThreadInformation(
            GetCurrentThread(),
            ThreadPowerThrottling,
            &st,
            cast(uint) st.sizeof) != FALSE;
    }
}
else version (linux)
{
    import core.sys.linux.sched :
        CPU_ALLOC, CPU_ALLOC_SIZE, CPU_FREE, cpu_mask, cpu_set_t, sched_setaffinity;

    private bool pinOs(ref const LogicalProcessor t) @nogc nothrow
    {
        // Use a dynamically sized CPU set so machines with more than the
        // legacy 1024-CPU cpu_set_t can still pin by system CPU number.
        auto count = cast(size_t) t.lpIndex + 1;
        auto setsize = CPU_ALLOC_SIZE(count);
        auto set = CPU_ALLOC(count);
        if (set is null) return false;
        scope (exit) CPU_FREE(set);

        auto words = (cast(cpu_mask*) set)[0 .. setsize / cpu_mask.sizeof];
        words[] = 0;
        auto word = cast(size_t) t.lpIndex / (cpu_mask.sizeof * 8);
        auto bit  = cast(size_t) t.lpIndex % (cpu_mask.sizeof * 8);
        words[word] |= cast(cpu_mask)(1UL << bit);

        return sched_setaffinity(0, setsize, set) == 0;
    }

    /// No Linux analogue of ThreadPowerThrottling in v1.
    bool applyPowerThrottling(bool) @nogc nothrow
    {
        return true;
    }
}
