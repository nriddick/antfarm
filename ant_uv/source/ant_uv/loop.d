/++
 + One libuv event loop on a dedicated thread beside the Ant Farm worker
 + pool. docs/design/event-loop.md is the design; this module owns the
 + contracts below.
 +
 + `UvLoop.create` spawns the loop thread, which pins itself (optionally),
 + creates the loop, warms libuv's auxiliary thread pool and places its
 + threads, then serves. Any thread submits a caller-owned `UvRequest` with
 + `UvLoop.submit`: the request travels on a bounded `SlotRing` the loop
 + thread alone consumes, and the loop issues it to libuv when it holds an
 + in-flight credit. Every accepted request reaches exactly one terminal
 + publication (`UvRequestState.done`, or `abandoned` when `close` gave up),
 + after which the loop never touches the record again except through the
 + delivery targets it snapshotted first: a `FiberGenerationTrigger`, a
 + `FiberEvent`, a callback on the loop thread, or a completion ring.
 +
 + Ownership rules:
 +
 + - A request is the submitter's until `submit` returns `accepted`, the
 +   loop's until it publishes, and the submitter's again after. `path` and
 +   `buffer` must stay valid and unmoved until publication; GC buffers must
 +   be rooted by the caller and should be `NO_SCAN`. libuv's auxiliary
 +   threads write into `buffer`; they never run D code that touches GC
 +   memory and are not registered with druntime.
 + - Exactly one party reclaims a published request: the thread that observed
 +   `done`, the completion callback, or the consumer of the completion ring.
 +   A request with a callback must not also name a completion ring.
 + - A trigger or event named by a request must be kept alive by its owner.
 + - Cancellation is not a release fence: a cancelled operation still
 +   publishes, with `UV_ECANCELED`, and only after libuv's terminal callback.
 + - A block the loop allocates for a `readFile` is freed exactly once: by the
 +   loop when the read fails, or by its owner through `UvRequest.freeBuffer`
 +   or the `UvBuffer` that `UvRequest.takeBuffer` hands over. An abandoned
 +   request's block is never freed.
 +
 + One `UvLoop` may be open per process at a time: libuv's auxiliary pool is
 + process-global, and its size is fixed by the first use anywhere in the
 + process.
 +/
module ant_uv.loop;

import ant_fibers.scheduler : FiberEvent, FiberGenerationTrigger;
import ant_uv.binding;
// The allocator a readFile takes, shared with the actor runtimes; its
// mimalloc v3 adapter is antfarm.actors.mimalloc.mimallocV3ActorAllocator.
public import antfarm.actors.actor : ActorAllocator;
import antfarm.allocation : allocateAligned64, freeAligned64;
import antfarm.ring : RingElement, RingPush, SlotRing;
import core.atomic;
import core.stdc.stdlib : calloc, free, getenv, malloc, strtol;
import core.stdc.string : memset;
import core.thread : Thread;
import core.time : convClockFreq, Duration, MonoTime, msecs, seconds, usecs;
import threadpool.pin : PinOutcome, pinProcessor;
import threadpool.topology : ProcessorId;

version (linux)
{
    import core.stdc.errno : errno;
    import core.sys.linux.sched : cpu_set_t, CPU_ISSET, CPU_SET, sched_getaffinity,
        sched_setaffinity;
    import core.sys.posix.sys.resource : getpriority, PRIO_PROCESS, setpriority;

    private extern (C) int gettid() nothrow @nogc;
}
else version (Windows)
{
    import core.sys.windows.windows : GetCurrentThread, GetCurrentThreadId, GetLastError,
        GetThreadPriority, SetThreadPriority, THREAD_PRIORITY_BELOW_NORMAL,
        THREAD_PRIORITY_LOWEST;
    import threadpool.sys.win_bindings : GetThreadGroupAffinity, GROUP_AFFINITY,
        SetThreadGroupAffinity;
}

/// The "no processor" sentinel for `UvLoopOptions.cpu`.
enum ProcessorId noProcessor = ProcessorId(ushort.max, ushort.max);

/// What a `UvRequest` asks the loop thread to do.
enum UvOp : ubyte
{
    /// Nothing: published on the loop thread when it is dequeued, in order
    /// with the requests around it. Takes no credit.
    nop,
    /// Published `timeoutMs` milliseconds of loop time after it starts.
    timer,
    /// `uv_fs_open(path, flags, mode)`; `result` is the file.
    open,
    /// `uv_fs_close(file)`.
    close,
    /// `uv_fs_read` into `buffer[0 .. length]` at `offset` (negative: at and
    /// advancing the file position); `result` is the bytes read, 0 at end of
    /// file.
    read,
    /// `uv_fs_write` of `buffer[0 .. length]` at `offset`; `result` is the
    /// bytes written.
    write,
    /// `uv_fs_stat(path)`; `size` is the file's size.
    stat,
    /// `uv_fs_fstat(file)`; `size` is the file's size.
    fstat,
    /// Open `path`, read it from the start until end of file or a full
    /// buffer, and close it, as one job on libuv's auxiliary pool. `result`
    /// is the bytes read and `size` the file's size, so `size > result` means
    /// the buffer was too small. With a null `buffer` the job allocates the
    /// file's size from the request's `allocator` and hands the block over:
    /// see `UvRequest.takeBuffer`. An empty file allocates nothing. A
    /// directory fails with `UV_EISDIR` and `size` 0 on every platform,
    /// before anything is allocated or read.
    readFile,
}

/// Where a `UvRequest` is in its life. Read it with `UvRequest.state`.
enum UvRequestState : uint
{
    /// The submitter's; never submitted, or refused.
    idle,
    /// Accepted: the loop's until it publishes.
    queued,
    /// Published: `result` and `size` are final, and the request is the
    /// submitter's again.
    done,
    /// Published by `close` after it stopped waiting: the operation may
    /// still be running inside libuv, so its `buffer` must be leaked. The
    /// record itself is the submitter's again.
    abandoned,
}

/// Result of `UvLoop.submit`. Every value but `accepted` leaves the request
/// with the caller, unchanged apart from its state.
enum UvSubmit : ubyte
{
    /// The loop owns the request until it publishes.
    accepted,
    /// The request ring is full: retry later, without spinning a thread the
    /// loop or the pool needs.
    full,
    /// The loop is closing, closed or failed.
    closed,
    /// The request is malformed for its operation or already queued.
    invalid,
}

/// Priority applied to libuv's auxiliary threads by `UvLoop.create`.
enum UvAuxPriority : ubyte
{
    /// Leave them as created.
    unchanged,
    /// Linux: five more nice levels than they started with (capped at 19).
    /// Windows: `THREAD_PRIORITY_BELOW_NORMAL`.
    belowNormal,
    /// Linux: nice 19. Windows: `THREAD_PRIORITY_LOWEST`.
    lowest,
}

/// Called on the loop thread after a request is published. It owns the
/// request from then on (see the module's ownership rules), must not throw,
/// allocate GC memory or block.
alias UvCompletionCallback = void function(UvRequest* request, void* context)
    nothrow @nogc;

/// One request: inputs set by the submitter, outputs published by the loop.
/// Keep it at a stable address from `submit` until publication.
struct UvRequest
{
    /// The operation.
    UvOp op;
    /// `open`: `UV_FS_O_*` flags.
    int flags;
    /// `open`: permission bits for a created file (0644 by default).
    int mode = 0x1A4;
    /// `close`, `read`, `write`, `fstat`: the file.
    int file = -1;
    /// `open`, `stat`, `readFile`: a NUL-terminated path, valid until
    /// publication.
    const(char)* path;
    /// `read`, `write`, `readFile`: the bytes, valid and unmoved until
    /// publication. Null for `readFile` asks the loop to allocate; once that
    /// succeeds, `buffer[0 .. length]` is the file until `takeBuffer` or
    /// `freeBuffer`.
    ubyte* buffer;
    /// Bytes at `buffer`.
    size_t length;
    /// `readFile` with a null `buffer`: the allocator for the file's block;
    /// unset means the loop's `UvLoopOptions.allocator`. `allocate` runs on
    /// libuv's auxiliary thread, which druntime does not know: it must be
    /// thread-safe, may block, and must not touch the GC or druntime
    /// (`ActorAllocator.cRuntime()` and `mimallocV3ActorAllocator()`
    /// qualify; `ActorAllocator.gcRooted` does not). `deallocate` runs where
    /// the block is freed, on any thread, or on the loop thread when the
    /// read fails. A null result fails the request with `UV_ENOMEM`.
    ActorAllocator allocator;
    /// `readFile` with a null `buffer`: the block's alignment, a power of
    /// two. `ActorAllocator.cRuntime()` gives at most 64.
    size_t alignment = 64;
    /// `read`, `write`: the file offset. Negative uses the file position,
    /// which races when several reads of one file are in flight; the `read`
    /// and `write` constructors take an explicit offset.
    long offset = -1;
    /// `timer`: milliseconds of loop time.
    ulong timeoutMs;
    /// Application data the loop never reads, except that it becomes the
    /// `tag` of the completion ring element.
    ulong user;

    /// Advanced once after publication, if set.
    FiberGenerationTrigger trigger;
    /// Set once after publication, if set.
    FiberEvent event;
    /// Called on the loop thread after publication, if set.
    UvCompletionCallback callback;
    /// Passed to `callback`.
    void* context;
    /// Receives one element after publication, if set: `tag` = `user`,
    /// `generation` = the request's ticket, `pointer` = the request, and
    /// `payload` = [`result`, `size`, `op`, the terminal state]. A full ring
    /// is retried by the loop, never dropped while it can still accept.
    SlotRing!RingElement* completions;

    /// The libuv result: non-negative on success (a file, or bytes), else a
    /// negative libuv error code (`uv_strerror`, `errorName`).
    long result;
    /// `stat`, `fstat`, `readFile`: the file's size in bytes.
    ulong size;

    private shared uint state_;
    private shared uint cancel_;
    private ulong ticket_;
    // The loop's readFile block, with the allocator that made it, so that
    // freeing it never depends on fields the owner may have changed since.
    private UvBuffer owned_;

    /// The request's state, read with acquire: after `done` or `abandoned`
    /// the outputs are visible.
    @property UvRequestState state() const nothrow @nogc
    {
        return cast(UvRequestState) atomicLoad!(MemoryOrder.acq)(state_);
    }

    /// True once the loop published `done`.
    @property bool done() const nothrow @nogc
    {
        return state == UvRequestState.done;
    }

    /// True once the loop published, `done` or `abandoned`.
    @property bool finished() const nothrow @nogc
    {
        immutable s = state;
        return s == UvRequestState.done || s == UvRequestState.abandoned;
    }

    /// True when published `done` with a non-negative result.
    @property bool succeeded() const nothrow @nogc
    {
        return done && result >= 0;
    }

    /// The ticket `UvLoop.submit` assigned to the latest accepted submission.
    @property ulong ticket() const nothrow @nogc
    {
        return ticket_;
    }

    /// libuv's name for a negative `result` ("ECANCELED"), else "".
    const(char)[] errorName() const nothrow @nogc
    {
        import core.stdc.string : strlen;
        if (result >= 0) return "";
        auto name = uv_err_name(cast(int) result);
        return name is null ? "" : name[0 .. strlen(name)];
    }

    /// True when `buffer` holds the block the loop allocated for a
    /// `readFile`. The request owns it, and `submit` refuses the request,
    /// until `takeBuffer` or `freeBuffer`.
    @property bool ownsBuffer() const nothrow @nogc
    {
        return owned_.block_ !is null;
    }

    /// Hand over the block the loop allocated for a `readFile`, as a
    /// `UvBuffer` that frees it, and clear `buffer`. Empty when the request
    /// owns none, as after reading an empty file.
    UvBuffer takeBuffer() nothrow @nogc
    {
        if (owned_.block_ is null) return UvBuffer.init;
        auto taken = owned_;
        owned_ = UvBuffer.init;
        buffer = null;
        length = 0;
        return taken;
    }

    /// Free the block the loop allocated for a `readFile`, if the request
    /// owns one.
    void freeBuffer() nothrow @nogc
    {
        auto taken = takeBuffer();
        taken.free();
    }

    // ---- one fresh record per operation ---------------------------------------
    // Each sets only its operation's inputs. Set the delivery targets and
    // `user` on the result. A record is reusable once published, but a fresh
    // one carries nothing over from the last operation.

    /// Read all of `path` into a block allocated from `allocator`, or from
    /// the loop's default when `allocator` is unset, at `alignment`.
    static UvRequest readFile(const(char)* path, ActorAllocator allocator = ActorAllocator.init,
        size_t alignment = 64) nothrow @nogc
    {
        UvRequest r;
        r.op = UvOp.readFile;
        r.path = path;
        r.allocator = allocator;
        r.alignment = alignment;
        return r;
    }

    /// Read all of `path` into `buffer`, stopping when it is full.
    static UvRequest readFile(const(char)* path, ubyte[] buffer) nothrow @nogc
    {
        UvRequest r;
        r.op = UvOp.readFile;
        r.path = path;
        r.buffer = buffer.ptr;
        r.length = buffer.length;
        return r;
    }

    /// `uv_fs_open(path, flags, mode)`; `result` is the file.
    static UvRequest open(const(char)* path, int flags, int mode = 0x1A4) nothrow @nogc
    {
        UvRequest r;
        r.op = UvOp.open;
        r.path = path;
        r.flags = flags;
        r.mode = mode;
        return r;
    }

    /// `uv_fs_close(file)`.
    static UvRequest close(int file) nothrow @nogc
    {
        UvRequest r;
        r.op = UvOp.close;
        r.file = file;
        return r;
    }

    /// Read up to `buffer.length` bytes of `file` at `offset` into `buffer`;
    /// `result` is the bytes read, 0 at end of file.
    static UvRequest read(int file, ubyte[] buffer, long offset) nothrow @nogc
    {
        UvRequest r;
        r.op = UvOp.read;
        r.file = file;
        r.buffer = buffer.ptr;
        r.length = buffer.length;
        r.offset = offset;
        return r;
    }

    /// Write `bytes` to `file` at `offset`; `result` is the bytes written.
    static UvRequest write(int file, const(ubyte)[] bytes, long offset) nothrow @nogc
    {
        UvRequest r;
        r.op = UvOp.write;
        r.file = file;
        r.buffer = cast(ubyte*) bytes.ptr; // only read, for a write
        r.length = bytes.length;
        r.offset = offset;
        return r;
    }

    /// `uv_fs_stat(path)`; `size` is the file's size.
    static UvRequest stat(const(char)* path) nothrow @nogc
    {
        UvRequest r;
        r.op = UvOp.stat;
        r.path = path;
        return r;
    }

    /// `uv_fs_fstat(file)`; `size` is the file's size.
    static UvRequest fstat(int file) nothrow @nogc
    {
        UvRequest r;
        r.op = UvOp.fstat;
        r.file = file;
        return r;
    }

    /// Published `timeoutMs` milliseconds of loop time after it starts.
    static UvRequest timer(ulong timeoutMs) nothrow @nogc
    {
        UvRequest r;
        r.op = UvOp.timer;
        r.timeoutMs = timeoutMs;
        return r;
    }

    /// Published in order with the requests around it.
    static UvRequest nop() nothrow @nogc
    {
        return UvRequest.init;
    }
}

/// A file that `readFile` read into a block the loop allocated, handed over
/// by `UvRequest.takeBuffer`. It owns the block until `free`, which returns
/// the block to the allocator that made it, from any thread. Copies name the
/// same block: free one of them, once.
struct UvBuffer
{
    private ubyte* block_;
    private size_t length_;
    private size_t capacity_;
    private size_t alignment_;
    private ActorAllocator allocator_;

    /// The file's bytes, at the start of the block. Empty after `free`.
    @property inout(ubyte)[] bytes() inout nothrow @nogc
    {
        return block_[0 .. length_];
    }

    /// Bytes allocated: the file's size when the loop opened it, so at
    /// least `bytes.length`.
    @property size_t capacity() const nothrow @nogc
    {
        return capacity_;
    }

    /// The block's alignment, as the request asked.
    @property size_t alignment() const nothrow @nogc
    {
        return alignment_;
    }

    /// Return the block to its allocator and empty this copy. Does nothing
    /// when it is already empty.
    void free() nothrow @nogc
    {
        if (block_ is null) return;
        allocator_.deallocate(allocator_.context, block_, capacity_, alignment_);
        this = UvBuffer.init;
    }
}

/// What the application's step hook did in one loop iteration.
struct UvStepResult
{
    /// Something ran or was published: the loop polls without blocking.
    bool progress;
    /// Earliest `MonoTime` tick the hook needs to run again; `long.max` for
    /// none. The loop arms a timer for it before blocking.
    long deadlineTicks = long.max;
}

/// Everything `UvLoop.create` reads.
struct UvLoopOptions
{
    /// The CPU the loop thread pins itself to; `noProcessor` leaves its
    /// affinity as inherited from the creating thread.
    ProcessorId cpu = noProcessor;
    /// Request ring slots: a power of two, 2 to 2^20.
    uint ringCapacity = 1024;
    /// Operations in libuv at once, plus completions waiting for a full
    /// completion ring; 0 means `ringCapacity`. At most 65536.
    uint credits;
    /// The allocator for a `readFile` that names neither a `buffer` nor an
    /// `allocator`; unset means `ActorAllocator.cRuntime()`. Set it once to
    /// the application's switch between arenas and a general allocator such
    /// as mimalloc. The contract is `UvRequest.allocator`'s.
    ActorAllocator allocator;
    /// libuv's auxiliary pool size (`UV_THREADPOOL_SIZE`, 1 to 1024); 0
    /// keeps the environment's value or libuv's default of 4. Effective only
    /// if nothing in the process used the pool before.
    uint auxThreads;
    /// CPUs for libuv's auxiliary threads; empty means the creating
    /// thread's affinity, so the threads do not inherit the loop's pin.
    const(ProcessorId)[] auxCpus;
    /// Priority for libuv's auxiliary threads.
    UvAuxPriority auxPriority = UvAuxPriority.unchanged;
    /// Runs once on the loop thread before it serves, for example to build a
    /// `UvFibers`. If it throws, the loop fails and closes.
    void delegate() start;
    /// Runs on the loop thread once per iteration, between draining requests
    /// and polling libuv; must return promptly. If it throws, the loop fails,
    /// stops calling it and closes.
    UvStepResult delegate() step;
    /// Runs once on the loop thread after every request is settled and
    /// before the loop is closed, whenever `start` ran.
    void delegate() stop;
    /// How long `close` keeps serving before it cancels what is outstanding.
    Duration closeGrace = 5.seconds;
    /// How long `close` then waits for terminal callbacks before it
    /// abandons the rest.
    Duration abandonAfter = 5.seconds;
    /// Linux: let libuv issue `open`, `close`, `read`, `write`, `stat` and
    /// `fstat` through io_uring (`UV_LOOP_USE_IO_URING_SQPOLL`, libuv 1.49
    /// or later) instead of its auxiliary pool. `create` sets
    /// `UV_USE_IO_URING=1` unless the environment sets it already; a value
    /// of 0 there keeps the pool. `create` also builds the ring, before the
    /// loop takes requests, so no request pays its setup. The kernel runs
    /// the operations on a polling thread of its own, which spins while the
    /// ring is busy and copies in turn what the pool's threads copy side by
    /// side; `readFile` stays a pool job for that reason. libuv falls back
    /// to the pool for an operation the ring cannot take. Ignored on other
    /// systems; `UvLoop.ioUring` reports whether it was configured.
    bool ioUring;
}

/// One of libuv's auxiliary threads as placed and read back by `UvLoop.create`.
struct UvAuxThread
{
    /// OS thread id (Linux tid, Windows thread id).
    ulong threadId;
    /// Its affinity, read back after placement.
    const(ProcessorId)[] cpus;
    /// Its nice value (Linux) or thread priority (Windows), read back.
    int priority;
    /// The requested affinity was applied.
    bool placed;
    /// OS error of the placement calls; 0 when none failed.
    int error;
}

/// What `UvLoop.close` found.
struct UvCloseReport
{
    /// Requests published, cancelled ones included.
    ulong completed;
    /// Requests published with `UV_ECANCELED`.
    ulong cancelled;
    /// Requests published `abandoned`: libuv may still use their buffers.
    ulong abandoned;
    /// Completion ring elements never pushed: the ring was closed, or the
    /// loop gave up waiting for room.
    ulong undelivered;
    /// `uv_loop_close`'s result: 0 when the loop was closed and freed;
    /// `UV_EBUSY` when it was left open because operations were abandoned.
    int loopClose;
    /// The loop thread's first failure, or null.
    Throwable failure;
}

/// Thrown by `UvLoop.create` for invalid options or a failed start, and by
/// `UvLoop.close` when the loop thread failed.
class UvLoopException : Exception
{
    /// The close report, when thrown by `close`.
    UvCloseReport report;

    /// Message and optional cause.
    this(string message, Throwable cause = null, string file = __FILE__,
        size_t line = __LINE__) pure nothrow @safe
    {
        super(message, file, line, cause);
    }
}

private shared int gLoopClaimed;
private __gshared uint gAuxPoolSize; // set by the first warm-up in the process

private enum ulong tagSubmit = 1;
private enum ulong tagCancel = 2;
private enum ulong gateClosed = 1UL << 63;
private enum uint noSlot = uint.max;
private enum size_t readChunk = size_t(1) << 30; // a Windows uv_buf_t length is 32-bit
private enum uint submitBatch = 64; // requests dequeued per loop iteration
private enum uint controlCapacity = 256; // control ring slots (cancellations)

private enum State : int { starting, open, failed, closed }
private enum ClosePhase : ubyte { open, grace, cancelling }
// A readFile job's uv_fs_t lives on the auxiliary thread's stack.
private enum size_t jobFsBytes = 1024;

// Loop-thread bookkeeping for one in-flight operation. `uv` points at the
// operation's libuv storage, used as a uv_fs_t, a uv_timer_t or a uv_work_t.
// While a readFile job runs, the auxiliary thread owns the read fields
// (`file` through `allocator`); libuv's queue orders them both ways.
private struct OpSlot
{
    void* uv;
    void* owner; // the UvLoop
    uv_loop_t* uvLoop; // for the job's synchronous calls, which only store it
    UvRequest* request; // null once published
    ulong ticket;
    uint index;
    uint next = noSlot;
    bool inUse;
    bool handleOpen; // `uv` is an initialized timer handle: close before reuse
    bool cancelling;
    shared bool stop; // a cancel the running job polls between steps
    UvOp op;
    const(char)* path;
    int file = -1;
    long status;
    ulong fileSize;
    ulong filled;
    ubyte* buffer;
    size_t capacity;
    bool allocated; // `buffer` came from `allocator` at `alignment`
    size_t alignment;
    ActorAllocator allocator;
}

private struct PendingDelivery
{
    SlotRing!RingElement* ring;
    RingElement element;
}

// One placement record per auxiliary thread, written by that thread.
private struct AuxRecord
{
    ulong threadId;
    int priority;
    int error;
    bool placed;
    version (linux) cpu_set_t mask;
    else version (Windows) GROUP_AFFINITY affinity;
}

private struct AuxWarmup
{
    shared uint arrived;
    uint expected;
    long deadlineTicks;
    bool applyAffinity;
    UvAuxPriority priority;
    version (linux) cpu_set_t mask;
    else version (Windows) GROUP_AFFINITY affinity;
    AuxRecord* records;
}

/// One libuv loop on its own thread. Create it with `create`, submit from
/// any thread, and `close` it from any thread but the loop's.
final class UvLoop
{
    /// Validate `options`, claim the process's one loop, spawn and pin the
    /// loop thread, create the loop, warm and place libuv's auxiliary pool,
    /// and return once the loop serves. Throws `UvLoopException` on invalid
    /// options, a second open loop, or a failed start.
    static UvLoop create(UvLoopOptions options = UvLoopOptions.init)
    {
        validate(options);
        if (!cas(&gLoopClaimed, 0, 1))
            throw new UvLoopException("ant_uv: a UvLoop is already open in this process "
                ~ "(libuv's auxiliary pool is process-global)");
        UvLoop loop;
        try
            loop = new UvLoop(options);
        catch (Throwable t)
        {
            atomicStore(gLoopClaimed, 0);
            throw t;
        }
        try
        {
            loop.thread_ = new Thread(&loop.threadMain);
            loop.thread_.isDaemon = false;
            loop.thread_.start();
        }
        catch (Throwable t)
        {
            // No loop thread: release what the constructor took.
            loop.releaseRings();
            atomicStore(gLoopClaimed, 0);
            throw t;
        }
        while (atomicLoad!(MemoryOrder.acq)(loop.state_) == State.starting)
            Thread.sleep(50.usecs);
        if (atomicLoad!(MemoryOrder.acq)(loop.state_) == State.failed)
        {
            loop.thread_.join();
            loop.releaseRings();
            atomicStore(gLoopClaimed, 0);
            auto cause = loop.startFailure_;
            throw new UvLoopException("ant_uv: the loop thread failed to start: "
                ~ (cause is null ? "unknown" : cause.msg), cause);
        }
        return loop;
    }

    /// Hand `request` to the loop. Any thread, including the loop's own;
    /// never blocks. On `accepted` the loop owns the request until it
    /// publishes; otherwise the caller keeps it. A request still owning a
    /// `readFile` allocation is `invalid` until `takeBuffer` or `freeBuffer`.
    UvSubmit submit(UvRequest* request) nothrow @nogc
    {
        if (request is null || !wellFormed(request)) return UvSubmit.invalid;
        immutable prior = atomicLoad!(MemoryOrder.acq)(request.state_);
        if (prior == UvRequestState.queued) return UvSubmit.invalid;
        if (atomicLoad!(MemoryOrder.acq)(closeRequested_) != 0) return UvSubmit.closed;
        request.result = 0;
        request.size = 0;
        atomicStore!(MemoryOrder.raw)(request.cancel_, 0);
        immutable ticket = atomicFetchAdd!(MemoryOrder.raw)(tickets_, 1UL) + 1;
        immutable previousTicket = request.ticket_;
        request.ticket_ = ticket;
        atomicStore!(MemoryOrder.raw)(request.state_, cast(uint) UvRequestState.queued);
        RingElement element;
        element.tag = tagSubmit;
        element.generation = ticket;
        element.pointer = cast(ulong) cast(size_t) request;
        immutable pushed = requests_.tryPush(element);
        if (pushed == RingPush.pushed)
        {
            wake();
            return UvSubmit.accepted;
        }
        request.ticket_ = previousTicket;
        atomicStore!(MemoryOrder.rel)(request.state_, prior);
        return pushed == RingPush.full ? UvSubmit.full : UvSubmit.closed;
    }

    /// Ask the loop to cancel `request`'s latest submission. Any thread that
    /// owns the request's lifetime. A request not yet started is published
    /// with `UV_ECANCELED` without reaching libuv; a timer is stopped; file
    /// work libuv has not started is cancelled and file work it has started
    /// runs to completion (`readFile` stops at its next step). False when
    /// the control ring is full or closed: retry later. Idempotent.
    bool cancel(UvRequest* request) nothrow @nogc
    {
        if (request is null) return false;
        atomicStore!(MemoryOrder.rel)(request.cancel_, 1);
        RingElement element;
        element.tag = tagCancel;
        element.generation = request.ticket_;
        element.pointer = cast(ulong) cast(size_t) request;
        if (controls_.tryPush(element) != RingPush.pushed) return false;
        wake();
        return true;
    }

    /// Wake the loop thread out of `uv_run`. Any thread, including from a
    /// Farm callback; a no-op once the loop has closed.
    void wake() nothrow @nogc
    {
        immutable before = atomicFetchAdd!(MemoryOrder.acq_rel)(wakeGate_, 1UL);
        if ((before & gateClosed) == 0)
            uv_async_send(async_);
        atomicFetchSub!(MemoryOrder.acq_rel)(wakeGate_, 1UL);
    }

    /// Close: refuse new requests, keep serving for `closeGrace`, then
    /// cancel what is outstanding and wait `abandonAfter` for libuv's
    /// terminal callbacks, publish anything still running as `abandoned`,
    /// run the `stop` hook, close the loop and join its thread. Any thread
    /// but the loop's; concurrent and repeated calls return the same report.
    /// Throws `UvLoopException`, carrying the report, when the loop thread
    /// failed.
    UvCloseReport close()
    {
        if (onLoopThread)
            throw new UvLoopException("ant_uv: UvLoop.close called on the loop thread");
        if (cas(&closeRequested_, 0, 1))
        {
            requests_.close();
            wake();
            thread_.join();
            atomicStore(gLoopClaimed, 0);
            atomicStore!(MemoryOrder.rel)(joined_, 1);
        }
        else
            while (atomicLoad!(MemoryOrder.acq)(joined_) == 0)
                Thread.sleep(100.usecs);
        auto report = UvCloseReport(atomicLoad(completed_), atomicLoad(cancelled_),
            atomicLoad(abandoned_), atomicLoad(undelivered_), loopClose_, failure_);
        if (failure_ !is null)
        {
            auto e = new UvLoopException("ant_uv: the loop thread failed: " ~ failure_.msg,
                failure_);
            e.report = report;
            throw e;
        }
        return report;
    }

    // Loop thread only, outside libuv callbacks (Fibers run from the step
    // hook, never inside uv_run): serve until `request` is published, asking
    // for its cancellation first. A cancelled loop-thread Fiber cannot park
    // and must not unwind while the loop owns a request on its stack.
    package void serveUntilPublished(UvRequest* request)
    {
        bool asked;
        while (!request.finished)
        {
            if (!asked) asked = cancel(request);
            bool progress = drainControls();
            if (retryPending()) progress = true;
            if (closePhase_ == ClosePhase.cancelling)
            {
                if (cancelQueued()) progress = true;
            }
            else if (drainRequests())
                progress = true;
            if (request.finished) break;
            runOnce(progress, MonoTime.currTime.ticks + toTicks(1.msecs));
        }
    }

    /// The loop whose thread is calling, or null on any other thread: how
    /// the `start`, `step` and `stop` hooks and loop-thread Fibers reach it.
    static @property UvLoop current() nothrow @nogc
    {
        return tlsCurrent_;
    }

    /// True on the loop thread.
    @property bool onLoopThread() const nothrow @nogc
    {
        return thread_ !is null && Thread.getThis() is thread_;
    }

    /// True until `close` begins or the loop fails.
    @property bool isOpen() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(state_) == State.open
            && atomicLoad!(MemoryOrder.acq)(closeRequested_) == 0
            && atomicLoad!(MemoryOrder.acq)(failed_) == 0;
    }

    /// True once the loop thread recorded a failure.
    @property bool failed() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(failed_) != 0;
    }

    /// Requests accepted so far (tickets issued, including refused pushes'
    /// discarded tickets).
    @property ulong submitted() const nothrow @nogc { return atomicLoad(tickets_); }
    /// Requests published so far.
    @property ulong completed() const nothrow @nogc { return atomicLoad(completed_); }
    /// Requests published with `UV_ECANCELED` so far.
    @property ulong cancelled() const nothrow @nogc { return atomicLoad(cancelled_); }
    /// Most operations in libuv at once so far.
    @property ulong maxInFlight() const nothrow @nogc { return atomicLoad(maxInFlight_); }
    /// Completions that waited for room in a completion ring so far.
    @property ulong deferredDeliveries() const nothrow @nogc { return atomicLoad(deferred_); }
    /// The credit limit in effect.
    @property uint credits() const nothrow @nogc { return slotCount_; }
    /// libuv's auxiliary threads as placed by `create`.
    @property const(UvAuxThread)[] auxThreads() const nothrow @nogc { return auxThreads_; }
    /// True when `UvLoopOptions.ioUring` took effect: Linux 5.10.186 or
    /// later, libuv accepted the option, and `UV_USE_IO_URING` is positive.
    /// The kernel may still refuse the ring, and libuv then uses its pool.
    @property bool ioUring() const nothrow @nogc { return ioUring_; }

    /// One line: libuv version, loop CPU, ring, credits, file I/O path,
    /// auxiliary threads.
    string summary() const
    {
        import std.format : format;
        import core.stdc.string : strlen;
        auto v = uv_version_string();
        string aux;
        foreach (i, ref t; auxThreads_)
            aux ~= format("%s%s:%s%s", i == 0 ? "" : " ", t.threadId, formatCpus(t.cpus),
                t.error == 0 ? "" : format("(error %s)", t.error));
        return format("UvLoop: libuv %s, loop thread %s, ring %s, credits %s, file operations through %s, "
            ~ "%s auxiliary thread(s) priority %s [%s]",
            v[0 .. strlen(v)],
            options_.cpu == noProcessor ? "unpinned" : format("%s:%s", options_.cpu.group, options_.cpu.lp),
            requests_.capacity, slotCount_, ioUring_ ? "io_uring" : "the pool", auxThreads_.length,
            auxThreads_.length == 0 ? 0 : auxThreads_[0].priority, aux);
    }

private:
    static UvLoop tlsCurrent_; // thread-local: set on the loop thread only
    UvLoopOptions options_;
    Thread thread_;
    SlotRing!RingElement* requests_;
    SlotRing!RingElement* controls_;
    shared ulong tickets_;
    shared ulong wakeGate_ = gateClosed; // count of wakers in progress, plus gateClosed
    shared int state_;
    shared int closeRequested_;
    shared int joined_;
    shared int failed_;
    Throwable startFailure_;
    Throwable failure_;
    shared ulong completed_, cancelled_, abandoned_, undelivered_, maxInFlight_, deferred_;
    int loopClose_;
    UvAuxThread[] auxThreads_;

    // Loop thread only.
    uv_loop_t* loop_;
    uv_async_t* async_;
    uv_timer_t* stepTimer_;
    bool stepTimerOpen_, asyncOpen_;
    OpSlot* slots_;
    uint slotCount_;
    uint freeHead_ = noSlot;
    uint slotsInUse_;
    void* slotMemory_;
    size_t slotStride_;
    PendingDelivery* pending_;
    size_t pendingCap_, pendingHead_, pendingCount_;
    bool ioUring_; // set before the loop opens; read anywhere after
    ClosePhase closePhase_;
    long phaseDeadline_;
    bool loopAbandoned_;
    version (linux) cpu_set_t inheritedMask_;
    else version (Windows) GROUP_AFFINITY inheritedAffinity_;

    this(UvLoopOptions options)
    {
        options_ = options;
        options_.auxCpus = options.auxCpus.dup;
        slotCount_ = options.credits == 0 ? options.ringCapacity : options.credits;
        requests_ = SlotRing!RingElement.create(options.ringCapacity);
        controls_ = SlotRing!RingElement.create(controlCapacity);
        if (requests_ is null || controls_ is null)
        {
            releaseRings();
            throw new UvLoopException("ant_uv: ring allocation failed");
        }
    }

    ~this()
    {
        releaseRings();
    }

    void releaseRings() nothrow @nogc
    {
        if (requests_ !is null) requests_.destroy();
        if (controls_ !is null) controls_.destroy();
        requests_ = null;
        controls_ = null;
    }

    static void validate(ref const UvLoopOptions o)
    {
        import std.format : format;
        alias Ring = SlotRing!RingElement;
        if (!Ring.validCapacity(o.ringCapacity))
            throw new UvLoopException(format("ant_uv: ringCapacity %s is not a power of two in 2 .. 2^20",
                o.ringCapacity));
        if (o.credits > 65_536)
            throw new UvLoopException(format("ant_uv: credits %s exceeds 65536", o.credits));
        if (!o.allocator.valid && (o.allocator.allocate !is null || o.allocator.deallocate !is null))
            throw new UvLoopException("ant_uv: allocator has only one of its halves set");
        if (o.auxThreads > 1024)
            throw new UvLoopException(format("ant_uv: auxThreads %s exceeds libuv's 1024", o.auxThreads));
        if (o.closeGrace < Duration.zero || o.abandonAfter < Duration.zero)
            throw new UvLoopException("ant_uv: closeGrace and abandonAfter must not be negative");
        version (Windows)
        {
            foreach (id; o.auxCpus)
                if (id.group != o.auxCpus[0].group || id.lp >= 64)
                    throw new UvLoopException("ant_uv: auxCpus must lie in one processor group");
        }
        else version (linux)
        {
            foreach (id; o.auxCpus)
                if (id.group != 0 || id.lp >= cpu_set_t.sizeof * 8)
                    throw new UvLoopException(format("ant_uv: auxCpus entry %s:%s is not a Linux CPU",
                        id.group, id.lp));
        }
    }

    static bool wellFormed(const(UvRequest)* r) nothrow @nogc
    {
        if (r.callback !is null && r.completions !is null) return false;
        version (Windows)
            if (r.length > uint.max && (r.op == UvOp.read || r.op == UvOp.write)) return false;
        final switch (r.op)
        {
        case UvOp.nop, UvOp.timer:
            return true;
        case UvOp.open, UvOp.stat:
            return r.path !is null;
        case UvOp.close, UvOp.fstat:
            return r.file >= 0;
        case UvOp.read, UvOp.write:
            return r.file >= 0 && (r.buffer !is null || r.length == 0);
        case UvOp.readFile:
            if (r.path is null || r.ownsBuffer) return false;
            if (r.buffer !is null) return true;
            // The loop allocates: a power-of-two alignment, and an allocator
            // that is either unset or whole.
            return r.length == 0 && r.alignment != 0 && (r.alignment & (r.alignment - 1)) == 0
                && (r.allocator.valid
                    || (r.allocator.allocate is null && r.allocator.deallocate is null));
        }
    }

    // ---- the loop thread ----------------------------------------------------

    void threadMain()
    {
        tlsCurrent_ = this;
        scope (exit) tlsCurrent_ = null;
        try
            initialize();
        catch (Throwable t)
        {
            startFailure_ = t;
            atomicStore!(MemoryOrder.rel)(state_, State.failed);
            return;
        }
        atomicStore!(MemoryOrder.rel)(state_, State.open);
        bool stepEnabled = options_.step !is null;
        bool started;
        if (options_.start !is null)
        {
            started = true;
            try
                options_.start();
            catch (Throwable t)
            {
                recordFailure(t);
                stepEnabled = false;
                beginCancelling(MonoTime.currTime.ticks);
            }
        }
        for (;;)
        {
            try
            {
                serve(stepEnabled);
                break;
            }
            catch (Throwable t)
            {
                recordFailure(t);
                stepEnabled = false;
                if (closePhase_ != ClosePhase.cancelling)
                    beginCancelling(MonoTime.currTime.ticks);
            }
        }
        if (started && options_.stop !is null)
        {
            try
                options_.stop();
            catch (Throwable t)
                recordFailure(t);
        }
        teardown();
        atomicStore!(MemoryOrder.rel)(state_, State.closed);
    }

    void recordFailure(Throwable t) nothrow
    {
        if (failure_ is null) failure_ = t;
        atomicStore!(MemoryOrder.rel)(failed_, 1);
        requests_.close();
    }

    void serve(bool stepEnabled)
    {
        for (;;)
        {
            bool progress = drainControls();
            if (retryPending()) progress = true;
            immutable now = MonoTime.currTime.ticks;
            if (closePhase_ == ClosePhase.open && atomicLoad!(MemoryOrder.acq)(closeRequested_) != 0)
            {
                closePhase_ = ClosePhase.grace;
                phaseDeadline_ = now + toTicks(options_.closeGrace);
            }
            if (closePhase_ == ClosePhase.cancelling)
            {
                if (cancelQueued()) progress = true;
            }
            else if (drainRequests())
                progress = true;
            long deadline = long.max;
            if (stepEnabled)
            {
                auto r = options_.step();
                if (r.progress) progress = true;
                deadline = r.deadlineTicks;
            }
            if (closePhase_ != ClosePhase.open)
            {
                if (settled()) return;
                if (now >= phaseDeadline_)
                {
                    if (closePhase_ == ClosePhase.grace)
                        beginCancelling(now);
                    else
                    {
                        abandonOutstanding();
                        return;
                    }
                    progress = true;
                }
                if (phaseDeadline_ < deadline) deadline = phaseDeadline_;
            }
            if (pendingCount_ != 0)
            {
                // A completion ring's consumer frees room without telling the
                // loop: retry on a short timer rather than only on wakes.
                immutable retry = now + toTicks(1.msecs);
                if (retry < deadline) deadline = retry;
            }
            runOnce(progress, deadline);
        }
    }

    bool settled() nothrow @nogc
    {
        return requests_.drained && slotsInUse_ == 0 && pendingCount_ == 0;
    }

    void runOnce(bool progress, long deadline) nothrow @nogc
    {
        if (progress)
        {
            uv_run(loop_, uv_run_mode.UV_RUN_NOWAIT);
            return;
        }
        if (deadline != long.max)
        {
            immutable now = MonoTime.currTime.ticks;
            if (deadline <= now)
            {
                uv_run(loop_, uv_run_mode.UV_RUN_NOWAIT);
                return;
            }
            // UV_RUN_ONCE has no timeout: a timer bounds the poll.
            immutable ms = convClockFreq(deadline - now, MonoTime.ticksPerSecond, 1000) + 1;
            uv_update_time(loop_);
            uv_timer_start(stepTimer_, &onStepTimer, cast(ulong) ms, 0);
            uv_run(loop_, uv_run_mode.UV_RUN_ONCE);
            uv_timer_stop(stepTimer_);
            return;
        }
        uv_run(loop_, uv_run_mode.UV_RUN_ONCE);
    }

    static long toTicks(Duration d) nothrow @nogc
    {
        if (d == Duration.max) return long.max / 2;
        return convClockFreq(d.total!"hnsecs", 10_000_000, MonoTime.ticksPerSecond);
    }

    bool drainControls() nothrow @nogc
    {
        bool any;
        RingElement e;
        while (controls_.tryPop(e))
        {
            any = true;
            if (e.tag == tagCancel)
                applyCancel(cast(UvRequest*) cast(size_t) e.pointer, e.generation);
        }
        return any;
    }

    bool haveCredit() nothrow @nogc
    {
        return freeHead_ != noSlot && slotsInUse_ + pendingCount_ < slotCount_;
    }

    bool drainRequests() nothrow @nogc
    {
        uint taken;
        RingElement e;
        while (taken < submitBatch && haveCredit() && requests_.tryPop(e))
        {
            ++taken;
            start(cast(UvRequest*) cast(size_t) e.pointer, e.generation);
        }
        return taken != 0;
    }

    // Close or failure: everything still queued is published cancelled
    // without reaching libuv.
    bool cancelQueued() nothrow @nogc
    {
        bool any;
        RingElement e;
        while (requests_.tryPop(e))
        {
            any = true;
            publish(cast(UvRequest*) cast(size_t) e.pointer, UV_ECANCELED, 0);
        }
        return any;
    }

    void beginCancelling(long now) nothrow @nogc
    {
        closePhase_ = ClosePhase.cancelling;
        requests_.close();
        phaseDeadline_ = now + toTicks(options_.abandonAfter);
        cancelQueued();
        foreach (i; 0 .. slotCount_)
            if (slots_[i].inUse && slots_[i].request !is null)
                cancelSlot(&slots_[i]);
    }

    void abandonOutstanding() nothrow @nogc
    {
        foreach (i; 0 .. slotCount_)
        {
            auto slot = &slots_[i];
            if (!slot.inUse) continue;
            loopAbandoned_ = true; // libuv still holds this slot's memory
            if (slot.request is null) continue;
            auto request = slot.request;
            slot.request = null;
            publish(request, UV_ECANCELED, 0, UvRequestState.abandoned);
        }
        atomicStore(undelivered_, atomicLoad(undelivered_) + pendingCount_);
        pendingCount_ = 0;
    }

    // ---- starting operations --------------------------------------------------

    void start(UvRequest* request, ulong ticket) nothrow @nogc
    {
        if (atomicLoad!(MemoryOrder.acq)(request.cancel_) != 0
            || closePhase_ == ClosePhase.cancelling)
        {
            publish(request, UV_ECANCELED, 0);
            return;
        }
        if (request.op == UvOp.nop)
        {
            publish(request, 0, 0);
            return;
        }
        auto slot = takeSlot();
        slot.request = request;
        slot.ticket = ticket;
        slot.op = request.op;
        slot.cancelling = false;
        atomicStore!(MemoryOrder.raw)(slot.stop, false);
        slot.path = null;
        slot.file = -1;
        slot.status = 0;
        slot.fileSize = 0;
        slot.filled = 0;
        slot.buffer = null;
        slot.capacity = 0;
        slot.allocated = false;
        auto fs = cast(uv_fs_t*) slot.uv;
        int rc;
        final switch (request.op)
        {
        case UvOp.nop:
            assert(0);
        case UvOp.timer:
            rc = startTimer(slot, request.timeoutMs);
            break;
        case UvOp.open:
            rc = armed(slot, uv_fs_open(loop_, fs, request.path, request.flags, request.mode,
                &onFsDone));
            break;
        case UvOp.close:
            rc = armed(slot, uv_fs_close(loop_, fs, request.file, &onFsDone));
            break;
        case UvOp.read:
        {
            auto buf = makeBuf(request.buffer, request.length);
            rc = armed(slot, uv_fs_read(loop_, fs, request.file, &buf, 1, request.offset,
                &onFsDone));
            break;
        }
        case UvOp.write:
        {
            auto buf = makeBuf(request.buffer, request.length);
            rc = armed(slot, uv_fs_write(loop_, fs, request.file, &buf, 1, request.offset,
                &onFsDone));
            break;
        }
        case UvOp.stat:
            rc = armed(slot, uv_fs_stat(loop_, fs, request.path, &onFsDone));
            break;
        case UvOp.fstat:
            rc = armed(slot, uv_fs_fstat(loop_, fs, request.file, &onFsDone));
            break;
        case UvOp.readFile:
            rc = startReadJob(slot, request);
            break;
        }
        if (rc < 0)
        {
            // Refused synchronously: no callback will follow.
            slot.request = null;
            finishSlot(slot);
            publish(request, rc, 0);
        }
    }

    // A readFile: copy what the job reads into the slot and queue it.
    int startReadJob(OpSlot* slot, UvRequest* request) nothrow @nogc
    {
        slot.path = request.path;
        if (request.buffer !is null)
        {
            slot.buffer = request.buffer;
            slot.capacity = request.length;
        }
        else
        {
            slot.allocator = request.allocator.valid ? request.allocator
                : options_.allocator.valid ? options_.allocator : ActorAllocator.cRuntime();
            slot.alignment = request.alignment;
        }
        // The job may run before uv_queue_work returns: name the slot first.
        uv_req_set_data(cast(uv_req_t*) slot.uv, slot);
        return uv_queue_work(loop_, cast(uv_work_t*) slot.uv, &onReadJob, &onReadJobDone);
    }

    // A readFile on one of libuv's auxiliary threads, which druntime does
    // not know: only libuv's synchronous calls (they store the loop and never
    // touch it), the allocator, and atomics.
    static void runReadJob(OpSlot* slot) nothrow @nogc
    {
        align(16) ubyte[jobFsBytes] storage = void;
        auto fs = cast(uv_fs_t*) storage.ptr;
        auto loop = slot.uvLoop;
        bool stopped() { return atomicLoad!(MemoryOrder.acq)(slot.stop); }
        void closeFile(long status)
        {
            slot.status = status;
            uv_fs_close(loop, fs, slot.file, null);
            uv_fs_req_cleanup(fs);
            slot.file = -1;
        }

        immutable fd = uv_fs_open(loop, fs, slot.path, UV_FS_O_RDONLY, 0, null);
        uv_fs_req_cleanup(fs);
        if (fd < 0)
        {
            slot.status = fd;
            return;
        }
        slot.file = fd;
        if (stopped()) return closeFile(UV_ECANCELED);
        immutable rc = uv_fs_fstat(loop, fs, slot.file, null);
        immutable st = *uv_fs_get_statbuf(fs);
        uv_fs_req_cleanup(fs);
        if (rc < 0) return closeFile(rc);
        // A directory opens on both platforms, then reports a size Linux
        // would fail to read and Windows reports as 0: refuse it here,
        // before anything is allocated or read, with size 0 everywhere.
        if ((st.st_mode & UV_S_IFMT) == UV_S_IFDIR) return closeFile(UV_EISDIR);
        slot.fileSize = st.st_size;
        if (slot.buffer is null && st.st_size != 0) // an empty file allocates nothing
        {
            if (st.st_size > size_t.max) return closeFile(UV_ENOMEM);
            auto p = cast(ubyte*) slot.allocator.allocate(slot.allocator.context,
                cast(size_t) st.st_size, slot.alignment);
            if (p is null) return closeFile(UV_ENOMEM);
            slot.buffer = p;
            slot.capacity = cast(size_t) st.st_size;
            slot.allocated = true;
        }
        while (slot.filled < slot.capacity)
        {
            if (stopped()) return closeFile(UV_ECANCELED);
            auto remaining = slot.capacity - cast(size_t) slot.filled;
            if (remaining > readChunk) remaining = readChunk;
            auto buf = makeBuf(slot.buffer + cast(size_t) slot.filled, remaining);
            immutable n = uv_fs_read(loop, fs, slot.file, &buf, 1, cast(long) slot.filled, null);
            uv_fs_req_cleanup(fs);
            if (n < 0) return closeFile(n);
            if (n == 0) break;
            slot.filled += n;
        }
        closeFile(cast(long) slot.filled);
    }

    // libuv's after-work callback for a readFile job, on the loop thread.
    // UV_ECANCELED means the job never ran: nothing is open or allocated.
    void readJobDone(OpSlot* slot, int status) nothrow @nogc
    {
        finishReadFile(slot, status == UV_ECANCELED ? UV_ECANCELED : slot.status);
    }

    // libuv calls the request's callback from uv_run only, so naming the slot
    // once the call accepted the request is early enough.
    static int armed(OpSlot* slot, int rc) nothrow @nogc
    {
        if (rc >= 0) uv_req_set_data(cast(uv_req_t*) slot.uv, slot);
        return rc;
    }

    static uv_buf_t makeBuf(ubyte* base, size_t length) nothrow @nogc
    {
        uv_buf_t buf;
        buf.base = cast(char*) base;
        buf.len = cast(typeof(buf.len)) length;
        return buf;
    }

    int startTimer(OpSlot* slot, ulong timeoutMs) nothrow @nogc
    {
        auto timer = cast(uv_timer_t*) slot.uv;
        immutable rc = uv_timer_init(loop_, timer);
        if (rc < 0) return rc;
        slot.handleOpen = true;
        uv_handle_set_data(cast(uv_handle_t*) timer, slot);
        uv_update_time(loop_);
        return uv_timer_start(timer, &onOpTimer, timeoutMs, 0);
    }

    OpSlot* takeSlot() nothrow @nogc
    {
        assert(freeHead_ != noSlot);
        auto slot = &slots_[freeHead_];
        freeHead_ = slot.next;
        slot.next = noSlot;
        slot.inUse = true;
        ++slotsInUse_;
        if (slotsInUse_ > atomicLoad!(MemoryOrder.raw)(maxInFlight_))
            atomicStore!(MemoryOrder.raw)(maxInFlight_, cast(ulong) slotsInUse_);
        return slot;
    }

    // Release now, or after the timer handle's close callback.
    void finishSlot(OpSlot* slot) nothrow @nogc
    {
        if (slot.handleOpen)
            uv_close(cast(uv_handle_t*) slot.uv, &onOpClosed);
        else
            releaseSlot(slot);
    }

    void releaseSlot(OpSlot* slot) nothrow @nogc
    {
        assert(slot.inUse);
        slot.inUse = false;
        slot.handleOpen = false;
        slot.request = null;
        slot.next = freeHead_;
        freeHead_ = slot.index;
        --slotsInUse_;
    }

    // ---- cancellation ---------------------------------------------------------

    void applyCancel(UvRequest* request, ulong ticket) nothrow @nogc
    {
        // Match by address and ticket without touching the record: a
        // request that is not in flight was either not yet dequeued (its
        // flag is checked then) or already published and maybe reclaimed.
        foreach (i; 0 .. slotCount_)
        {
            auto slot = &slots_[i];
            if (slot.inUse && slot.request is request && slot.ticket == ticket)
            {
                cancelSlot(slot);
                return;
            }
        }
    }

    void cancelSlot(OpSlot* slot) nothrow @nogc
    {
        if (slot.cancelling) return;
        slot.cancelling = true;
        if (slot.op == UvOp.timer)
        {
            uv_timer_stop(cast(uv_timer_t*) slot.uv);
            timerDone(slot, UV_ECANCELED);
            return;
        }
        // Only work libuv has not started can be cancelled; started work
        // completes, and a readFile job stops at its next step, which it
        // learns from `stop`.
        if (slot.op == UvOp.readFile) atomicStore!(MemoryOrder.rel)(slot.stop, true);
        uv_cancel(cast(uv_req_t*) slot.uv);
    }

    // ---- completions --------------------------------------------------------

    void timerDone(OpSlot* slot, long result) nothrow @nogc
    {
        auto request = slot.request;
        slot.request = null;
        finishSlot(slot);
        if (request !is null) publish(request, result, 0);
    }

    void fsDone(OpSlot* slot, uv_fs_t* req) nothrow @nogc
    {
        immutable result = cast(long) uv_fs_get_result(req);
        ulong statSize;
        if (result >= 0 && (slot.op == UvOp.stat || slot.op == UvOp.fstat))
            statSize = uv_fs_get_statbuf(req).st_size;
        uv_fs_req_cleanup(req);
        auto request = slot.request;
        slot.request = null;
        releaseSlot(slot);
        if (request !is null) publish(request, result, statSize);
    }

    // A readFile's publication, on the loop thread: hand the block over, or
    // free it when the read failed after allocating.
    void finishReadFile(OpSlot* slot, long status) nothrow @nogc
    {
        auto request = slot.request;
        immutable size = slot.fileSize;
        if (status >= 0 && slot.allocated)
        {
            request.buffer = slot.buffer;
            request.length = cast(size_t) slot.filled;
            request.owned_ = UvBuffer(slot.buffer, cast(size_t) slot.filled, slot.capacity,
                slot.alignment, slot.allocator);
        }
        else if (slot.allocated)
            slot.allocator.deallocate(slot.allocator.context, slot.buffer, slot.capacity,
                slot.alignment);
        slot.allocated = false;
        slot.request = null;
        releaseSlot(slot);
        if (request !is null) publish(request, status, size);
    }

    // Publication: snapshot the delivery targets, write the outputs, release
    // the state (the last access to the record), then deliver.
    void publish(UvRequest* request, long result, ulong size,
        UvRequestState terminal = UvRequestState.done) nothrow @nogc
    {
        auto trigger = request.trigger;
        auto event = request.event;
        auto callback = request.callback;
        auto context = request.context;
        auto ring = request.completions;
        immutable user = request.user;
        immutable ticket = request.ticket_;
        immutable op = request.op;
        request.result = result;
        request.size = size;
        bump(completed_);
        if (result == UV_ECANCELED) bump(cancelled_);
        if (terminal == UvRequestState.abandoned) bump(abandoned_);
        atomicStore!(MemoryOrder.rel)(request.state_, cast(uint) terminal);
        if (trigger !is null) trigger.advance();
        if (event !is null) event.set();
        if (callback !is null) callback(request, context);
        if (ring !is null)
        {
            RingElement e;
            e.tag = user;
            e.generation = ticket;
            e.pointer = cast(ulong) cast(size_t) request;
            e.payload[0] = cast(ulong) result;
            e.payload[1] = size;
            e.payload[2] = op;
            e.payload[3] = terminal;
            deliver(ring, e);
        }
    }

    void deliver(SlotRing!RingElement* ring, ref RingElement e) nothrow @nogc
    {
        if (pendingCount_ == 0)
        {
            immutable r = ring.tryPush(e);
            if (r == RingPush.pushed) return;
            if (r == RingPush.closed)
            {
                bump(undelivered_);
                return;
            }
        }
        // Full, or older deliveries wait: keep order, hold credit, retry.
        if (pendingCount_ == pendingCap_)
        {
            bump(undelivered_);
            return;
        }
        pending_[(pendingHead_ + pendingCount_) % pendingCap_] = PendingDelivery(ring, e);
        ++pendingCount_;
        bump(deferred_);
    }

    bool retryPending() nothrow @nogc
    {
        bool progress;
        while (pendingCount_ != 0)
        {
            auto p = &pending_[pendingHead_];
            immutable r = p.ring.tryPush(p.element);
            if (r == RingPush.full) break;
            if (r == RingPush.closed) bump(undelivered_);
            pendingHead_ = (pendingHead_ + 1) % pendingCap_;
            --pendingCount_;
            progress = true;
        }
        return progress;
    }

    static void bump(ref shared ulong counter) nothrow @nogc
    {
        atomicStore!(MemoryOrder.raw)(counter, atomicLoad!(MemoryOrder.raw)(counter) + 1);
    }

    // ---- start and teardown ---------------------------------------------------

    void initialize()
    {
        import std.format : format;
        captureInheritedAffinity();
        if (options_.cpu != noProcessor)
        {
            immutable outcome = pinProcessor(options_.cpu);
            if (outcome != PinOutcome.pinned)
                throw new UvLoopException(format("ant_uv: pinning the loop thread to %s:%s failed: %s",
                    options_.cpu.group, options_.cpu.lp, outcome));
        }
        if (uv_version() < UV_MINIMUM_VERSION)
            throw new UvLoopException(format("ant_uv: libuv %#x is older than 1.19", uv_version()));
        if (options_.auxThreads != 0)
        {
            char[16] text;
            import core.stdc.stdio : snprintf;
            snprintf(text.ptr, text.length, "%u", options_.auxThreads);
            uv_os_setenv("UV_THREADPOOL_SIZE", text.ptr);
        }
        loop_ = cast(uv_loop_t*) malloc(uv_loop_size());
        if (loop_ is null) throw new UvLoopException("ant_uv: loop allocation failed");
        immutable rc = uv_loop_init(loop_);
        if (rc < 0)
        {
            free(loop_);
            loop_ = null;
            throw uvFailure("uv_loop_init", rc);
        }
        bool done;
        scope (exit) if (!done) abortInitialization();
        uv_loop_set_data(loop_, cast(void*) this);
        version (linux) if (options_.ioUring && ioUringKernel())
        {
            // libuv reads the variable once, when it first builds the ring,
            // and treats unset as off; an explicit value is the user's.
            if (getenv("UV_USE_IO_URING") is null) uv_os_setenv("UV_USE_IO_URING", "1");
            auto env = getenv("UV_USE_IO_URING");
            ioUring_ = env !is null && strtol(env, null, 10) > 0
                && uv_loop_configure(loop_, uv_loop_option.UV_LOOP_USE_IO_URING_SQPOLL) == 0;
        }
        allocateSlots();
        warmAuxPool();
        version (linux) if (ioUring_) buildIoUring();
        stepTimer_ = cast(uv_timer_t*) malloc(uv_handle_size(uv_handle_type.UV_TIMER));
        if (stepTimer_ is null) throw new UvLoopException("ant_uv: timer allocation failed");
        immutable trc = uv_timer_init(loop_, stepTimer_);
        if (trc < 0) throw uvFailure("uv_timer_init", trc);
        stepTimerOpen_ = true;
        async_ = cast(uv_async_t*) malloc(uv_handle_size(uv_handle_type.UV_ASYNC));
        if (async_ is null) throw new UvLoopException("ant_uv: async allocation failed");
        immutable arc = uv_async_init(loop_, async_, &onWake);
        if (arc < 0) throw uvFailure("uv_async_init", arc);
        asyncOpen_ = true;
        atomicStore!(MemoryOrder.rel)(wakeGate_, 0UL);
        done = true;
    }

    // libuv builds its ring inside the first file operation that could use
    // it. Build it here, before the loop takes requests, with an fstat of no
    // descriptor: EBADF through the ring, or through the pool where the
    // kernel refused the ring. No request then pays for the polling thread
    // and the ring's mappings. Under ThreadSanitizer, a ring built later
    // could also map a descriptor number that a readFile job had just closed,
    // which reads as a race: libuv closes descriptors and creates the ring
    // with raw system calls, which TSan does not see.
    version (linux) void buildIoUring()
    {
        auto fs = cast(uv_fs_t*) calloc(1, uv_req_size(uv_req_type.UV_FS));
        if (fs is null) throw new UvLoopException("ant_uv: io_uring warm-up allocation failed");
        scope (exit) free(fs);
        // Nothing else is active yet, so the run returns after the fstat.
        if (uv_fs_fstat(loop_, fs, -1, &onRingBuilt) == 0)
            uv_run(loop_, uv_run_mode.UV_RUN_DEFAULT);
        uv_fs_req_cleanup(fs);
    }

    // libuv builds its polling ring only on 5.10.186 or later; earlier
    // kernels' polling thread could spin at full CPU.
    version (linux) static bool ioUringKernel() nothrow @nogc
    {
        import core.stdc.stdio : sscanf;
        import core.sys.posix.sys.utsname : uname, utsname;
        utsname u;
        int major, minor, patch;
        if (uname(&u) != 0 || sscanf(u.release.ptr, "%d.%d.%d", &major, &minor, &patch) < 2)
            return false;
        return major > 5 || (major == 5 && (minor > 10 || (minor == 10 && patch >= 186)));
    }

    static UvLoopException uvFailure(string call, int rc)
    {
        import std.format : format;
        import core.stdc.string : strlen;
        auto name = uv_err_name(rc);
        return new UvLoopException(format("ant_uv: %s failed: %s", call,
            name is null ? "unknown" : name[0 .. strlen(name)].idup));
    }

    void allocateSlots()
    {
        import std.algorithm.comparison : max;
        immutable fsSize = uv_req_size(uv_req_type.UV_FS);
        immutable timerSize = uv_handle_size(uv_handle_type.UV_TIMER);
        immutable workSize = uv_req_size(uv_req_type.UV_WORK);
        if (fsSize > jobFsBytes)
            throw new UvLoopException("ant_uv: this libuv's uv_fs_t is larger than a readFile job reserves");
        slotStride_ = (max(fsSize, timerSize, workSize) + 63) & ~cast(size_t) 63;
        slotMemory_ = allocateAligned64(slotStride_ * slotCount_);
        slots_ = cast(OpSlot*) calloc(slotCount_, OpSlot.sizeof);
        pendingCap_ = slotCount_ + options_.ringCapacity;
        pending_ = cast(PendingDelivery*) calloc(pendingCap_, PendingDelivery.sizeof);
        if (slotMemory_ is null || slots_ is null || pending_ is null)
            throw new UvLoopException("ant_uv: slot allocation failed");
        foreach (i; 0 .. slotCount_)
        {
            auto slot = &slots_[i];
            *slot = OpSlot.init;
            slot.uv = cast(ubyte*) slotMemory_ + i * slotStride_;
            slot.owner = cast(void*) this;
            slot.uvLoop = loop_;
            slot.index = i;
            slot.next = i + 1 < slotCount_ ? i + 1 : noSlot;
        }
        freeHead_ = 0;
    }

    void abortInitialization() nothrow
    {
        if (stepTimerOpen_) uv_close(cast(uv_handle_t*) stepTimer_, null);
        if (asyncOpen_) uv_close(cast(uv_handle_t*) async_, null);
        uv_run(loop_, uv_run_mode.UV_RUN_DEFAULT);
        uv_loop_close(loop_);
        freeLoopMemory();
    }

    void freeLoopMemory() nothrow @nogc
    {
        free(loop_);
        loop_ = null;
        free(stepTimer_);
        stepTimer_ = null;
        free(async_);
        async_ = null;
        freeAligned64(slotMemory_);
        slotMemory_ = null;
        free(slots_);
        slots_ = null;
        free(pending_);
        pending_ = null;
    }

    void closeWakeGate() nothrow @nogc
    {
        atomicOp!"|="(wakeGate_, gateClosed);
        while ((atomicLoad!(MemoryOrder.acq)(wakeGate_) & ~gateClosed) != 0)
            core.atomic.pause();
    }

    void teardown() nothrow
    {
        closeWakeGate();
        controls_.close();
        RingElement e;
        while (controls_.tryPop(e)) {}
        if (loopAbandoned_)
        {
            // libuv may still write into slot memory and post to the loop:
            // leave the loop, its handles and the slots allocated.
            loopClose_ = UV_EBUSY;
            return;
        }
        uv_close(cast(uv_handle_t*) async_, null);
        uv_close(cast(uv_handle_t*) stepTimer_, null);
        uv_run(loop_, uv_run_mode.UV_RUN_DEFAULT);
        int rc = uv_loop_close(loop_);
        if (rc == UV_EBUSY)
        {
            uv_walk(loop_, &closeAnyHandle, null);
            uv_run(loop_, uv_run_mode.UV_RUN_DEFAULT);
            rc = uv_loop_close(loop_);
        }
        loopClose_ = rc;
        if (rc == 0) freeLoopMemory();
    }

    // ---- auxiliary pool ---------------------------------------------------------

    void captureInheritedAffinity() nothrow @nogc
    {
        version (linux)
            sched_getaffinity(0, cpu_set_t.sizeof, &inheritedMask_);
        else version (Windows)
            GetThreadGroupAffinity(GetCurrentThread(), &inheritedAffinity_);
    }

    void warmAuxPool()
    {
        uint expected = gAuxPoolSize;
        if (expected == 0)
        {
            expected = options_.auxThreads;
            if (expected == 0)
            {
                auto env = getenv("UV_THREADPOOL_SIZE");
                expected = env is null ? 4 : cast(uint) strtol(env, null, 10);
            }
            if (expected == 0) expected = 1;
            if (expected > 1024) expected = 1024;
        }
        auto warm = cast(AuxWarmup*) calloc(1, AuxWarmup.sizeof);
        auto records = cast(AuxRecord*) calloc(expected, AuxRecord.sizeof);
        auto works = cast(ubyte*) calloc(expected, uv_req_size(uv_req_type.UV_WORK));
        scope (exit)
        {
            free(warm);
            free(records);
            free(works);
        }
        if (warm is null || records is null || works is null)
            throw new UvLoopException("ant_uv: auxiliary warm-up allocation failed");
        warm.expected = expected;
        warm.records = records;
        warm.priority = options_.auxPriority;
        warm.deadlineTicks = MonoTime.currTime.ticks + toTicks(250.msecs);
        warm.applyAffinity = true;
        version (linux)
        {
            if (options_.auxCpus.length != 0)
                foreach (id; options_.auxCpus)
                    CPU_SET(id.lp, &warm.mask);
            else
                warm.mask = inheritedMask_;
        }
        else version (Windows)
        {
            if (options_.auxCpus.length != 0)
            {
                warm.affinity.Group = options_.auxCpus[0].group;
                foreach (id; options_.auxCpus)
                    warm.affinity.Mask |= cast(typeof(warm.affinity.Mask)) 1 << id.lp;
            }
            else
                warm.affinity = inheritedAffinity_;
        }
        else
            warm.applyAffinity = false;
        immutable workSize = uv_req_size(uv_req_type.UV_WORK);
        uint queued;
        foreach (i; 0 .. expected)
        {
            auto work = cast(uv_work_t*)(works + i * workSize);
            uv_req_set_data(cast(uv_req_t*) work, warm);
            if (uv_queue_work(loop_, work, &auxWarmupWork, &auxWarmupDone) == 0)
                ++queued;
        }
        // Only these requests are active: the loop returns when all are done.
        uv_run(loop_, uv_run_mode.UV_RUN_DEFAULT);
        immutable arrived = atomicLoad!(MemoryOrder.acq)(warm.arrived);
        auxThreads_ = null;
        foreach (i; 0 .. (arrived < expected ? arrived : expected))
        {
            auto r = &records[i];
            bool duplicate;
            foreach (ref t; auxThreads_)
                if (t.threadId == r.threadId) duplicate = true;
            if (duplicate) continue;
            UvAuxThread t;
            t.threadId = r.threadId;
            t.priority = r.priority;
            t.placed = r.placed;
            t.error = r.error;
            version (linux)
            {
                ProcessorId[] cpus;
                foreach (cpu; 0 .. cpu_set_t.sizeof * 8)
                    if (CPU_ISSET(cpu, &r.mask)) cpus ~= ProcessorId(0, cast(ushort) cpu);
                t.cpus = cpus;
            }
            else version (Windows)
            {
                ProcessorId[] cpus;
                foreach (lp; 0 .. 64)
                    if ((r.affinity.Mask >> lp) & 1) cpus ~= ProcessorId(r.affinity.Group, cast(ushort) lp);
                t.cpus = cpus;
            }
            auxThreads_ ~= t;
        }
        if (gAuxPoolSize == 0) gAuxPoolSize = cast(uint) auxThreads_.length;
        if (queued != expected)
            throw new UvLoopException("ant_uv: queueing the auxiliary warm-up failed");
    }
}

private string formatCpus(const(ProcessorId)[] ids)
{
    import threadpool.topology : formatCpuList;
    import std.algorithm.sorting : sort;
    auto sorted = ids.dup;
    sort(sorted);
    return formatCpuList(sorted);
}

// ---- libuv callbacks: C linkage, nothrow, @nogc ----------------------------

private UvLoop ownerOf(OpSlot* slot) nothrow @nogc
{
    return cast(UvLoop) slot.owner;
}

private extern (C) void onWake(uv_async_t*) nothrow @nogc
{
    // Returning from uv_run is the whole effect.
}

private extern (C) void onStepTimer(uv_timer_t*) nothrow @nogc
{
}

private extern (C) void onFsDone(uv_fs_t* req) nothrow @nogc
{
    auto slot = cast(OpSlot*) uv_req_get_data(cast(uv_req_t*) req);
    ownerOf(slot).fsDone(slot, req);
}

// The fstat that builds the io_uring ring at create; its EBADF is expected.
private extern (C) void onRingBuilt(uv_fs_t*) nothrow @nogc {}

// Runs on one of libuv's auxiliary threads.
private extern (C) void onReadJob(uv_work_t* work) nothrow @nogc
{
    UvLoop.runReadJob(cast(OpSlot*) uv_req_get_data(cast(uv_req_t*) work));
}

private extern (C) void onReadJobDone(uv_work_t* work, int status) nothrow @nogc
{
    auto slot = cast(OpSlot*) uv_req_get_data(cast(uv_req_t*) work);
    ownerOf(slot).readJobDone(slot, status);
}

private extern (C) void onOpTimer(uv_timer_t* timer) nothrow @nogc
{
    auto slot = cast(OpSlot*) uv_handle_get_data(cast(uv_handle_t*) timer);
    ownerOf(slot).timerDone(slot, 0);
}

private extern (C) void onOpClosed(uv_handle_t* handle) nothrow @nogc
{
    auto slot = cast(OpSlot*) uv_handle_get_data(handle);
    ownerOf(slot).releaseSlot(slot);
}

private extern (C) void closeAnyHandle(uv_handle_t* handle, void*) nothrow @nogc
{
    if (!uv_is_closing(handle)) uv_close(handle, null);
}

private extern (C) void auxWarmupDone(uv_work_t*, int) nothrow @nogc
{
}

// Runs on one of libuv's auxiliary threads: no druntime registration, so
// only atomics and OS calls. Every thread places and records itself, then
// holds until all expected threads arrived (or the deadline), so each work
// item lands on a distinct thread.
private extern (C) void auxWarmupWork(uv_work_t* work) nothrow @nogc
{
    auto warm = cast(AuxWarmup*) uv_req_get_data(cast(uv_req_t*) work);
    immutable index = atomicFetchAdd!(MemoryOrder.acq_rel)(warm.arrived, 1u);
    if (index < warm.expected)
        placeAuxThread(warm, &warm.records[index]);
    while (atomicLoad!(MemoryOrder.acq)(warm.arrived) < warm.expected
        && MonoTime.currTime.ticks < warm.deadlineTicks)
        Thread.sleep(20.usecs);
}

private void placeAuxThread(AuxWarmup* warm, AuxRecord* record) nothrow @nogc
{
    version (linux)
    {
        immutable tid = gettid();
        record.threadId = cast(ulong) tid;
        if (warm.applyAffinity)
        {
            if (sched_setaffinity(0, cpu_set_t.sizeof, &warm.mask) == 0)
                record.placed = true;
            else
                record.error = errno;
        }
        if (warm.priority != UvAuxPriority.unchanged)
        {
            errno = 0;
            immutable current = getpriority(PRIO_PROCESS, cast(uint) tid);
            int target = warm.priority == UvAuxPriority.lowest ? 19 : current + 5;
            if (target > 19) target = 19;
            if (setpriority(PRIO_PROCESS, cast(uint) tid, target) != 0 && record.error == 0)
                record.error = errno;
        }
        sched_getaffinity(0, cpu_set_t.sizeof, &record.mask);
        errno = 0;
        record.priority = getpriority(PRIO_PROCESS, cast(uint) tid);
    }
    else version (Windows)
    {
        auto self = GetCurrentThread();
        record.threadId = GetCurrentThreadId();
        if (warm.applyAffinity)
        {
            if (SetThreadGroupAffinity(self, &warm.affinity, null))
                record.placed = true;
            else
                record.error = cast(int) GetLastError();
        }
        if (warm.priority != UvAuxPriority.unchanged)
        {
            immutable target = warm.priority == UvAuxPriority.lowest
                ? THREAD_PRIORITY_LOWEST : THREAD_PRIORITY_BELOW_NORMAL;
            if (!SetThreadPriority(self, target) && record.error == 0)
                record.error = cast(int) GetLastError();
        }
        GetThreadGroupAffinity(self, &record.affinity);
        record.priority = GetThreadPriority(self);
    }
}
