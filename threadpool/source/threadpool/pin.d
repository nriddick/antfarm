module threadpool.pin;

struct PinTarget
{
    uint   cpuSetId;
    ushort group;
    ushort lpIndex;
}

version (Windows)
{
    import core.sys.windows.winbase : GetCurrentThread, SetThreadAffinityMask;
    import core.sys.windows.windef : BOOL, FALSE, HANDLE, TRUE;
    import threadpool.sys.win_bindings;

    bool pinToLogicalProcessor(PinTarget t) @nogc nothrow
    {
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

    bool pinToLogicalProcessor(PinTarget t) @nogc nothrow
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
