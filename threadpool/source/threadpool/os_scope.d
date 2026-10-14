module threadpool.os_scope;

/**
 * Internal: the process-level set of CPUs the OS allows, frozen once per
 * process. The public view is `TopologySnapshot.osScopeSource` and
 * `.osScopeAllowed`; see `threadpool.topology`.
 *
 * Linux captures the launch mask in this module's constructor, before
 * `main` runs. Windows captures the single-group process mask at first use
 * (discovery or a pin); a multi-group process stays unrestricted.
 */

import threadpool.topology : OsScopeSource, ProcessorId, affinityKey;

private __gshared OsScopeSource gSource = OsScopeSource.unrestricted;
private __gshared const(ProcessorId)[] gAllowed; // sorted; empty iff unrestricted
private __gshared const(ulong)[] gKeys; // sorted affinityKey values of gAllowed
private __gshared bool gCaptured;

version (linux)
{
    shared static this()
    {
        if (!gCaptured)
            setScope(OsScopeSource.processLaunch, linuxThreadMaskKeys());
    }
}

/// Capture the scope if nothing has yet. The first call on Windows is a
/// setup operation; afterwards, and on Linux always, nothing happens.
private void capture()
{
    if (gCaptured) return;
    version (Windows)
        setScope(OsScopeSource.processAffinity, windowsProcessMaskKeys());
    else version (linux)
        setScope(OsScopeSource.processLaunch, linuxThreadMaskKeys());
    else
        setScope(OsScopeSource.unrestricted, null);
}

/// The frozen scope discovery uses: its provenance in `source` and its
/// allowed ids, sorted (empty when unrestricted). Captures on first use.
package(threadpool) const(ProcessorId)[] frozenOsScope(out OsScopeSource source)
{
    capture();
    source = gSource;
    return gAllowed;
}

/// Replace the scope with the calling thread's current mask.
/// `threadpool.topology.scopeOsToCallingThread` checks it runs before discovery.
package(threadpool) void scopeToCallingThread()
{
    version (Windows)
        setScope(OsScopeSource.callingThread, windowsThreadMaskKeys());
    else version (linux)
        setScope(OsScopeSource.callingThread, linuxThreadMaskKeys());
    else
        setScope(OsScopeSource.unrestricted, null);
}

/// Capture the scope now if nothing has yet: Windows captures lazily, so a
/// `pinProcessor` before any discovery must not skip validation. The one-time
/// capture allocates; every later call is a plain `@nogc` read.
package(threadpool) void ensureOsScopeCaptured() @nogc nothrow
{
    if (gCaptured) return;
    static void captureNothrow() nothrow
    {
        try capture();
        catch (Exception) {}
    }
    (cast(void function() @nogc nothrow) &captureNothrow)();
}

/// Membership test used by pin validation. An unrestricted (or not yet
/// captured) scope admits every id; `pinProcessor` captures first.
package(threadpool) bool inFrozenScope(ProcessorId id) @nogc nothrow
{
    if (!gCaptured || gSource == OsScopeSource.unrestricted)
        return true;
    immutable key = affinityKey(id.group, id.lp);
    size_t lo, hi = gKeys.length;
    while (lo < hi)
    {
        immutable mid = (lo + hi) / 2;
        if (gKeys[mid] < key) lo = mid + 1;
        else hi = mid;
    }
    return lo < gKeys.length && gKeys[lo] == key;
}

/// What the OS reports right now (Linux: the calling thread's mask; Windows:
/// the process mask). Backs `threadpool.pin.liveOsMask`.
package(threadpool) ProcessorId[] liveMaskIds()
{
    version (Windows)
        return idsOf(windowsProcessMaskKeys());
    else version (linux)
        return idsOf(linuxThreadMaskKeys());
    else
        return null;
}

/// Sorted allowed-processor keys for discovery; empty when unrestricted.
package(threadpool) const(ulong)[] osScopeKeys()
{
    capture();
    return gKeys;
}

private void setScope(OsScopeSource source, ulong[] keys)
{
    import std.algorithm.sorting : sort;

    if (keys.length == 0)
    {
        gSource = OsScopeSource.unrestricted;
        gAllowed = null;
        gKeys = null;
    }
    else
    {
        keys.sort();
        gSource = source;
        gAllowed = idsOf(keys);
        gKeys = keys;
    }
    gCaptured = true;
}

private ProcessorId[] idsOf(const(ulong)[] keys)
{
    auto r = new ProcessorId[](keys.length);
    foreach (i, k; keys)
        r[i] = ProcessorId(cast(ushort)(k >> 16), cast(ushort) k);
    return r;
}

version (linux)
{
    /// The calling thread's affinity, or null when the query fails.
    private ulong[] linuxThreadMaskKeys() nothrow
    {
        import core.sys.linux.sched :
            CPU_ALLOC, CPU_ALLOC_SIZE, CPU_FREE, cpu_mask, sched_getaffinity;
        import core.sys.posix.unistd : _SC_NPROCESSORS_CONF, sysconf;

        auto conf = sysconf(_SC_NPROCESSORS_CONF);
        auto count = conf > 0 ? cast(size_t) conf : size_t(1024);
        auto setsize = CPU_ALLOC_SIZE(count);
        auto set = CPU_ALLOC(count);
        if (set is null) return null;
        scope (exit) CPU_FREE(set);
        if (sched_getaffinity(0, setsize, set) != 0) return null;

        auto words = (cast(cpu_mask*) set)[0 .. setsize / cpu_mask.sizeof];
        ulong[] keys;
        foreach (w, word; words)
            foreach (bit; 0 .. cpu_mask.sizeof * 8)
                if ((word >> bit) & 1)
                {
                    auto cpu = cast(ulong) w * cpu_mask.sizeof * 8 + bit;
                    if (cpu > ushort.max) break;
                    keys ~= affinityKey(0, cast(ushort) cpu);
                }
        return keys;
    }
}

version (Windows)
{
    /// Process affinity keys, or null when the process spans more than one
    /// processor group or the query fails. The mask only applies within the
    /// one group the process lives in; multi-group processes stay
    /// unrestricted, matching GetProcessAffinityMask's own zero-mask behaviour.
    private ulong[] windowsProcessMaskKeys()
    {
        import core.sys.windows.basetsd : ULONG_PTR;
        import core.sys.windows.winbase : GetCurrentProcess, GetProcessAffinityMask;
        import threadpool.sys.win_bindings : GetProcessGroupAffinity;

        ushort[64] groups;
        ushort count = cast(ushort) groups.length;
        if (GetProcessGroupAffinity(GetCurrentProcess(), &count, groups.ptr) == 0)
            return null;
        if (count != 1)
            return null;

        ULONG_PTR processMask, systemMask;
        if (GetProcessAffinityMask(GetCurrentProcess(), &processMask, &systemMask) == 0)
            return null;
        if (processMask == 0)
            return null;
        ulong[] keys;
        foreach (bit; 0 .. 64)
            if (processMask & (cast(ULONG_PTR) 1 << bit))
                keys ~= affinityKey(groups[0], cast(ushort) bit);
        return keys;
    }

    /// The calling thread's group affinity, or null when the query fails.
    private ulong[] windowsThreadMaskKeys()
    {
        import core.sys.windows.winbase : GetCurrentThread;
        import threadpool.sys.win_bindings : GROUP_AFFINITY, GetThreadGroupAffinity;

        GROUP_AFFINITY ga;
        if (GetThreadGroupAffinity(GetCurrentThread(), &ga) == 0 || ga.Mask == 0)
            return null;
        ulong[] keys;
        foreach (bit; 0 .. 64)
            if (ga.Mask & (cast(ulong) 1 << bit))
                keys ~= affinityKey(ga.Group, cast(ushort) bit);
        return keys;
    }
}

unittest
{
    // The frozen scope agrees with its own membership test and is sorted.
    OsScopeSource source;
    auto allowed = frozenOsScope(source);
    if (source == OsScopeSource.unrestricted)
    {
        assert(allowed.length == 0);
        assert(inFrozenScope(ProcessorId(0, 0)));
        return;
    }
    assert(allowed.length > 0);
    foreach (i, id; allowed)
    {
        assert(inFrozenScope(id));
        if (i) assert(allowed[i - 1] < id);
    }
    assert(!inFrozenScope(ProcessorId(0, ushort.max)) || allowed[$ - 1] == ProcessorId(0, ushort.max));
}

unittest
{
    // A pin before any capture (Windows captures lazily) must capture and
    // validate, not admit everything. Simulate "not yet captured" here.
    import threadpool.pin : PinOutcome, pinProcessor;
    import threadpool.topology : LogicalProcessor;

    auto savedSource = gSource;
    auto savedAllowed = gAllowed;
    auto savedKeys = gKeys;
    scope (exit)
    {
        gSource = savedSource;
        gAllowed = savedAllowed;
        gKeys = savedKeys;
        gCaptured = true;
    }
    gCaptured = false;
    gSource = OsScopeSource.unrestricted;
    gAllowed = null;
    gKeys = null;

    LogicalProcessor outside;
    outside.group = 0;
    outside.lpIndex = ushort.max;
    immutable outcome = pinProcessor(outside);
    assert(gCaptured);
    if (gSource != OsScopeSource.unrestricted)
        assert(outcome == PinOutcome.outsideOsScope);
}
