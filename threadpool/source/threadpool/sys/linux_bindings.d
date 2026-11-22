module threadpool.sys.linux_bindings;

version (linux):

import core.sys.linux.unistd : syscall;
import core.sys.posix.time : timespec;

// Druntime does not declare futex(2). Numbers from asm/unistd.h.

version (X86_64)
    enum SYS_futex = 202;
else version (X86)
    enum SYS_futex = 240;
else version (AArch64)
    enum SYS_futex = 98;
else version (ARM)
    enum SYS_futex = 240;
else version (PPC64)
    enum SYS_futex = 221;
else version (RISCV64)
    enum SYS_futex = 98;
else
    static assert(0, "threadpool: SYS_futex is not defined for this architecture");

enum FUTEX_WAIT = 0;
enum FUTEX_WAKE = 1;
enum FUTEX_WAIT_BITSET = 9;
enum FUTEX_PRIVATE_FLAG = 128;
enum FUTEX_WAIT_PRIVATE = FUTEX_WAIT | FUTEX_PRIVATE_FLAG;
enum FUTEX_WAKE_PRIVATE = FUTEX_WAKE | FUTEX_PRIVATE_FLAG;
enum FUTEX_WAIT_BITSET_PRIVATE = FUTEX_WAIT_BITSET | FUTEX_PRIVATE_FLAG;
enum uint FUTEX_BITSET_MATCH_ANY = 0xFFFF_FFFF;

int futexWait(shared(uint)* word, uint observed) @nogc nothrow
{
    return futexWaitTimeout(word, observed, null);
}

int futexWaitTimeout(shared(uint)* word, uint observed, const(timespec)* to) @nogc nothrow
{
    return cast(int) syscall(SYS_futex, cast(int*) word, FUTEX_WAIT_PRIVATE,
        observed, to, null, 0);
}

/// Wait until woken or until the absolute `CLOCK_MONOTONIC` time `deadline`.
/// `FUTEX_WAIT_BITSET` takes its timeout as an absolute time on that clock
/// (it is `FUTEX_WAIT` that takes a relative one); `FUTEX_WAKE` wakes it, as
/// its bit set matches any.
int futexWaitUntil(shared(uint)* word, uint observed, const(timespec)* deadline) @nogc nothrow
{
    return cast(int) syscall(SYS_futex, cast(int*) word, FUTEX_WAIT_BITSET_PRIVATE,
        observed, deadline, null, FUTEX_BITSET_MATCH_ANY);
}

int futexWake(shared(uint)* word, int waiters) @nogc nothrow
{
    return cast(int) syscall(SYS_futex, cast(int*) word, FUTEX_WAKE_PRIVATE,
        waiters, null, null, 0);
}
