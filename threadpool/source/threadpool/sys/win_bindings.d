module threadpool.sys.win_bindings;

version (Windows):

import core.sys.windows.windef;
import core.sys.windows.winnt;
import core.sys.windows.winbase;

alias KAFFINITY = ULONG_PTR;

enum LOGICAL_PROCESSOR_RELATIONSHIP : uint
{
    RelationProcessorCore    = 0,
    RelationNumaNode         = 1,
    RelationCache            = 2,
    RelationProcessorPackage = 3,
    RelationGroup            = 4,
    RelationProcessorDie     = 5,
    RelationNumaNodeEx       = 6,
    RelationProcessorModule  = 7,
    RelationAll              = 0xffff,
}

enum PROCESSOR_CACHE_TYPE : uint
{
    CacheUnified = 0,
    CacheInstruction,
    CacheData,
    CacheTrace,
}

enum LTP_PC_SMT = 0x1;

struct GROUP_AFFINITY
{
    KAFFINITY Mask;
    ushort    Group;
    ushort[3] Reserved;
}

struct PROCESSOR_GROUP_INFO
{
    ubyte     MaximumProcessorCount;
    ubyte     ActiveProcessorCount;
    ubyte[38] Reserved;
    KAFFINITY ActiveProcessorMask;
}

struct PROCESSOR_RELATIONSHIP
{
    ubyte          Flags;
    ubyte          EfficiencyClass;
    ubyte[20]      Reserved;
    ushort         GroupCount;
    GROUP_AFFINITY GroupMask;
}

struct NUMA_NODE_RELATIONSHIP
{
    uint           NodeNumber;
    ubyte[18]      Reserved;
    ushort         GroupCount;
    GROUP_AFFINITY GroupMask;
}

struct CACHE_RELATIONSHIP
{
    ubyte                Level;
    ubyte                Associativity;
    ushort               LineSize;
    uint                 CacheSize;
    PROCESSOR_CACHE_TYPE Type;
    ubyte[18]            Reserved;
    ushort               GroupCount;
    GROUP_AFFINITY       GroupMask;
}

struct GROUP_RELATIONSHIP
{
    ushort               MaximumGroupCount;
    ushort               ActiveGroupCount;
    ubyte[20]            Reserved;
    PROCESSOR_GROUP_INFO GroupInfo;
}

struct SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX
{
    LOGICAL_PROCESSOR_RELATIONSHIP Relationship;
    uint                           Size;
}

enum CPU_SET_INFORMATION_TYPE : uint
{
    CpuSet = 0,
}

enum SYSTEM_CPU_SET_INFORMATION_PARKED                   = 0x1;
enum SYSTEM_CPU_SET_INFORMATION_ALLOCATED                = 0x2;
enum SYSTEM_CPU_SET_INFORMATION_ALLOCATED_TO_TARGET_PROC = 0x4;
enum SYSTEM_CPU_SET_INFORMATION_REALTIME                 = 0x8;

struct SYSTEM_CPU_SET_INFORMATION
{
    uint   Size;
    uint   Type;
    uint   Id;
    ushort Group;
    ubyte  LogicalProcessorIndex;
    ubyte  CoreIndex;
    ubyte  LastLevelCacheIndex;
    ubyte  NumaNodeIndex;
    ubyte  EfficiencyClass;
    ubyte  AllFlags;
    uint   SchedulingClass;
    ulong  AllocationTag;
}

static assert(SYSTEM_CPU_SET_INFORMATION.sizeof == 32);

struct PROCESSOR_NUMBER
{
    ushort Group;
    ubyte  Number;
    ubyte  Reserved;
}

enum ThreadPowerThrottling = 3;
enum THREAD_POWER_THROTTLING_CURRENT_VERSION = 1;
enum THREAD_POWER_THROTTLING_EXECUTION_SPEED = 0x1;

struct THREAD_POWER_THROTTLING_STATE
{
    uint Version;
    uint ControlMask;
    uint StateMask;
}

extern (Windows) @nogc nothrow
{
    BOOL GetLogicalProcessorInformationEx(
        LOGICAL_PROCESSOR_RELATIONSHIP RelationshipType,
        void* Buffer,
        uint* ReturnedLength);

    BOOL GetSystemCpuSetInformation(
        void* Information,
        uint BufferLength,
        uint* ReturnedLength,
        HANDLE Process,
        uint Flags);

    BOOL SetThreadGroupAffinity(
        HANDLE thread,
        const(GROUP_AFFINITY)* groupAffinity,
        GROUP_AFFINITY* previousGroupAffinity);

    void GetCurrentProcessorNumberEx(PROCESSOR_NUMBER* ProcNumber);
}

alias FnSetThreadSelectedCpuSets = BOOL function(
    HANDLE thread, const(uint)* cpuSetIds, uint count) @nogc nothrow;

alias FnGetThreadSelectedCpuSets = BOOL function(
    HANDLE thread, uint* cpuSetIds, uint count, uint* required) @nogc nothrow;

alias FnSetThreadInformation = BOOL function(
    HANDLE thread, uint threadInformationClass, void* info, uint size) @nogc nothrow;

alias FnSetThreadSelectedCpuSetMasks = BOOL function(
    HANDLE thread, GROUP_AFFINITY* masks, ushort count) @nogc nothrow;

alias FnWaitOnAddress = BOOL function(
    void* address, void* compareAddress, size_t addressSize, uint dwMilliseconds) @nogc nothrow;

alias FnWakeByAddressSingle = void function(void* address) @nogc nothrow;
alias FnWakeByAddressAll = void function(void* address) @nogc nothrow;

__gshared FnSetThreadSelectedCpuSets pSetThreadSelectedCpuSets;
__gshared FnGetThreadSelectedCpuSets pGetThreadSelectedCpuSets;
__gshared FnSetThreadInformation pSetThreadInformation;
__gshared FnSetThreadSelectedCpuSetMasks pSetThreadSelectedCpuSetMasks; // unused in v1
__gshared FnWaitOnAddress pWaitOnAddress;
__gshared FnWakeByAddressSingle pWakeByAddressSingle;
__gshared FnWakeByAddressAll pWakeByAddressAll;
__gshared bool gBindingsResolved;

private void* proc(HMODULE mod, const(char)* name) @nogc nothrow
{
    if (mod is null) return null;
    return GetProcAddress(mod, name);
}

private HMODULE load(const(wchar)* name) @nogc nothrow
{
    auto m = GetModuleHandleW(name);
    if (m !is null) return m;
    return LoadLibraryW(name);
}

/// Resolve optional Win32 entry points. Safe to call more than once.
void resolveOptionalApis() @nogc nothrow
{
    if (gBindingsResolved) return;
    gBindingsResolved = true;

    auto k32 = load("kernel32.dll");
    pSetThreadSelectedCpuSets = cast(FnSetThreadSelectedCpuSets)
        proc(k32, "SetThreadSelectedCpuSets");
    pGetThreadSelectedCpuSets = cast(FnGetThreadSelectedCpuSets)
        proc(k32, "GetThreadSelectedCpuSets");
    pSetThreadInformation = cast(FnSetThreadInformation)
        proc(k32, "SetThreadInformation");
    pSetThreadSelectedCpuSetMasks = cast(FnSetThreadSelectedCpuSetMasks)
        proc(k32, "SetThreadSelectedCpuSetMasks");

    void resolveWait(HMODULE m)
    {
        if (pWaitOnAddress !is null) return;
        pWaitOnAddress = cast(FnWaitOnAddress) proc(m, "WaitOnAddress");
        pWakeByAddressSingle = cast(FnWakeByAddressSingle) proc(m, "WakeByAddressSingle");
        pWakeByAddressAll = cast(FnWakeByAddressAll) proc(m, "WakeByAddressAll");
        if (pWaitOnAddress is null)
        {
            pWakeByAddressSingle = null;
            pWakeByAddressAll = null;
        }
    }

    resolveWait(k32);
    if (pWaitOnAddress is null) resolveWait(load("KernelBase.dll"));
    if (pWaitOnAddress is null) resolveWait(load("api-ms-win-core-synch-l1-2-0.dll"));
    if (pWaitOnAddress is null) resolveWait(load("synchronization.dll"));
}
