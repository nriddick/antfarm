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
 +   loop when the read fails, by its owner through `UvRequest.freeBuffer` or
 +   the `UvBuffer` that `UvRequest.takeBuffer` hands over, or by the job
 +   itself when `close` abandoned it. An abandoned `readFile` job runs on
 +   until its file operation returns, then frees the block it allocated and
 +   counts itself out of `UvLoop.abandonedJobs`; until that count is zero
 +   the job may call its allocator and write into a caller-provided
 +   `buffer`, so both must stay valid. Another abandoned operation gives no
 +   such signal: its buffer must be leaked.
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
import core.stdc.string : memcpy, memset;
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
    /// Open a UDP socket as `udpOptions` says and start receiving into its
    /// ring; `result` is the socket id and `size` the port bound.
    udpOpen,
    /// Send `buffer[0 .. length]` from socket `file` to `address`, or to
    /// the connected peer; `result` is the bytes sent. With `stampAt` set,
    /// the loop writes its transmit time into the bytes first.
    udpSend,
    /// Close socket `file`: stop receiving, close the handle, publish once
    /// it is closed. Datagrams the application still holds stay valid.
    udpClose,
    /// Resolve the host at `path` on libuv's pool into the `UvAddress`es at
    /// `buffer`, `length` of them, each with `address.port`; `result` is how
    /// many were written.
    resolve,
    /// Listen for TCP connections as `tcpOptions` says: bind, listen, and
    /// push one element per accepted connection on the ring; `result` is the
    /// listener's socket id and `size` the port bound. An accepted stream
    /// reads nothing until `tcpStart` names its ring and window; `tcpClose`
    /// refuses it.
    tcpListen,
    /// Connect a TCP stream to `tcpOptions.connect` and start reading into
    /// its window; `result` is the socket id and `size` the local port.
    tcpConnect,
    /// Start reading accepted stream `file` as `tcpOptions` says; `result`
    /// is the socket id.
    tcpStart,
    /// Write `buffer[0 .. length]` to stream `file`; `result` is the bytes
    /// written, all of them unless the write failed.
    tcpWrite,
    /// Shut the write side of stream `file` once its pending writes are out;
    /// reading continues until the peer's end of stream.
    tcpShutdown,
    /// Close listener or stream `file`: stop reading, cancel pending writes
    /// (they publish `UV_ECANCELED`), close the handle, and publish once it
    /// is closed. Chunks the application still holds stay valid.
    tcpClose,
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
    /// still be running inside libuv. A `readFile`'s `buffer` and allocator
    /// stay in use until `UvLoop.abandonedJobs` is zero; another operation's
    /// `buffer` must be leaked. The record itself is the submitter's again.
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
    /// `close`, `read`, `write`, `fstat`: the file. `udpSend`, `udpClose`,
    /// `tcpStart`, `tcpWrite`, `tcpShutdown`, `tcpClose`: the socket.
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
    /// the block is freed, on any thread; on the loop thread when the read
    /// fails; or on the auxiliary thread when `close` abandoned the job,
    /// which may be after `close` returned. A null result fails the request
    /// with `UV_ENOMEM`.
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
    /// `udpSend`: the peer, ignored on a connected socket. `resolve`: its
    /// `port` goes into every address found.
    UvAddress address;
    /// `udpOpen`: how to open the socket; the caller's until publication.
    const(UvUdpOptions)* udpOptions;
    /// `tcpListen`, `tcpConnect`, `tcpStart`: how to set the socket up; the
    /// caller's until publication.
    const(UvTcpOptions)* tcpOptions;
    /// `udpSend`: unless `uint.max`, the loop writes its transmit time,
    /// `UvLoop.nanos` as a little-endian 64-bit integer, at
    /// `buffer[stampAt .. stampAt + 8]` just before the system call.
    uint stampAt = uint.max;

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
    @property UvRequestState state() const scope nothrow @nogc @safe
    {
        return cast(UvRequestState) atomicLoad!(MemoryOrder.acq)(state_);
    }

    /// True once the loop published `done`.
    @property bool done() const scope nothrow @nogc @safe
    {
        return state == UvRequestState.done;
    }

    /// True once the loop published, `done` or `abandoned`.
    @property bool finished() const scope nothrow @nogc @safe
    {
        immutable s = state;
        return s == UvRequestState.done || s == UvRequestState.abandoned;
    }

    /// True when published `done` with a non-negative result.
    @property bool succeeded() const scope nothrow @nogc @safe
    {
        return done && result >= 0;
    }

    /// The ticket `UvLoop.submit` assigned to the latest accepted submission.
    @property ulong ticket() const scope nothrow @nogc @safe
    {
        return ticket_;
    }

    /// libuv's name for a negative `result` ("ECANCELED"), else "".
    const(char)[] errorName() const scope nothrow @nogc @trusted
    {
        import core.stdc.string : strlen;
        if (result >= 0) return "";
        auto name = uv_err_name(cast(int) result);
        return name is null ? "" : name[0 .. strlen(name)];
    }

    /// True when `buffer` holds the block the loop allocated for a
    /// `readFile`. The request owns it, and `submit` refuses the request,
    /// until `takeBuffer` or `freeBuffer`.
    @property bool ownsBuffer() const scope nothrow @nogc @safe
    {
        return owned_.block_ !is null;
    }

    /// Hand over the block the loop allocated for a `readFile`, as a
    /// `UvBuffer` that frees it, and clear `buffer`. Empty when the request
    /// owns none, as after reading an empty file.
    UvBuffer takeBuffer() nothrow @nogc @safe
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
    void freeBuffer() nothrow @nogc @safe
    {
        auto taken = takeBuffer();
        taken.free();
    }

    // ---- one fresh record per operation ---------------------------------------
    // Each sets only its operation's inputs. Set the delivery targets and
    // `user` on the result. A record is reusable once published, but a fresh
    // one carries nothing over from the last operation.
    //
    // Attributes. A constructor that takes a slice is `@trusted`: it stores
    // the slice's bounds and dereferences nothing; the loop reads and writes
    // within them. One that takes a `const(char)*` is `@system`, since
    // nothing proves the pointer NUL-terminated; its `string` form copies
    // through `toStringz` and is `@trusted`. The pointer forms are templates
    // over the pointer's type so that a string literal, which converts to
    // both, picks the `string` form. Every pointer, slice and string
    // parameter is `return scope`: the record refers to it, so under DIP1000
    // the record cannot outlive it in `@safe` code.

    /// Read all of `path` into a block allocated from `allocator`, or from
    /// the loop's default when `allocator` is unset, at `alignment`. The
    /// pointer form takes a NUL-terminated path the caller keeps valid until
    /// publication; the `string` form copies it (`toStringz`).
    static UvRequest readFile(P : const(char)*)(return scope P path,
        ActorAllocator allocator = ActorAllocator.init, size_t alignment = 64)
        nothrow @nogc @system
    {
        UvRequest r;
        r.op = UvOp.readFile;
        r.path = path;
        r.allocator = allocator;
        r.alignment = alignment;
        return r;
    }

    /// ditto
    static UvRequest readFile(return scope string path,
        ActorAllocator allocator = ActorAllocator.init, size_t alignment = 64) nothrow @trusted
    {
        import std.string : toStringz;
        return readFile(toStringz(path), allocator, alignment);
    }

    /// Read all of `path` into `buffer`, stopping when it is full.
    static UvRequest readFile(P : const(char)*)(return scope P path, return scope ubyte[] buffer)
        nothrow @nogc @system
    {
        UvRequest r;
        r.op = UvOp.readFile;
        r.path = path;
        r.buffer = buffer.ptr;
        r.length = buffer.length;
        return r;
    }

    /// ditto
    static UvRequest readFile(return scope string path, return scope ubyte[] buffer)
        nothrow @trusted
    {
        import std.string : toStringz;
        return readFile(toStringz(path), buffer);
    }

    /// `uv_fs_open(path, flags, mode)`; `result` is the file.
    static UvRequest open(P : const(char)*)(return scope P path, int flags, int mode = 0x1A4)
        nothrow @nogc @system
    {
        UvRequest r;
        r.op = UvOp.open;
        r.path = path;
        r.flags = flags;
        r.mode = mode;
        return r;
    }

    /// ditto
    static UvRequest open(return scope string path, int flags, int mode = 0x1A4) nothrow @trusted
    {
        import std.string : toStringz;
        return open(toStringz(path), flags, mode);
    }

    /// `uv_fs_close(file)`.
    static UvRequest close(int file) nothrow @nogc @safe
    {
        UvRequest r;
        r.op = UvOp.close;
        r.file = file;
        return r;
    }

    /// Read up to `buffer.length` bytes of `file` at `offset` into `buffer`;
    /// `result` is the bytes read, 0 at end of file.
    static UvRequest read(int file, return scope ubyte[] buffer, long offset) nothrow @nogc @trusted
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
    static UvRequest write(int file, return scope const(ubyte)[] bytes, long offset)
        nothrow @nogc @trusted
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
    static UvRequest stat(P : const(char)*)(return scope P path) nothrow @nogc @system
    {
        UvRequest r;
        r.op = UvOp.stat;
        r.path = path;
        return r;
    }

    /// ditto
    static UvRequest stat(return scope string path) nothrow @trusted
    {
        import std.string : toStringz;
        return stat(toStringz(path));
    }

    /// `uv_fs_fstat(file)`; `size` is the file's size.
    static UvRequest fstat(int file) nothrow @nogc @safe
    {
        UvRequest r;
        r.op = UvOp.fstat;
        r.file = file;
        return r;
    }

    /// Published `timeoutMs` milliseconds of loop time after it starts.
    static UvRequest timer(ulong timeoutMs) nothrow @nogc @safe
    {
        UvRequest r;
        r.op = UvOp.timer;
        r.timeoutMs = timeoutMs;
        return r;
    }

    /// Published in order with the requests around it.
    static UvRequest nop() nothrow @nogc @safe
    {
        return UvRequest.init;
    }

    /// Open a UDP socket; `options` stays the caller's until publication.
    /// The `ref` form is `@trusted`: it stores the address and dereferences
    /// nothing, and a submit that holds the caller until publication
    /// (`submitAndBlock`, `submitAndWait`) is what makes a local `options`
    /// sound.
    static UvRequest udpOpen(return scope const(UvUdpOptions)* options) nothrow @nogc @safe
    {
        UvRequest r;
        r.op = UvOp.udpOpen;
        r.udpOptions = options;
        return r;
    }

    /// ditto
    static UvRequest udpOpen(return ref const UvUdpOptions options) nothrow @nogc @trusted
    {
        return udpOpen(&options);
    }

    /// Send `bytes` from `socket` to `peer`, or to the connected peer when
    /// `peer` is unset; `bytes` stays the caller's until publication.
    static UvRequest udpSend(int socket, return scope const(ubyte)[] bytes,
        UvAddress peer = UvAddress.init) nothrow @nogc @trusted
    {
        UvRequest r;
        r.op = UvOp.udpSend;
        r.file = socket;
        r.buffer = cast(ubyte*) bytes.ptr; // only read, unless `stampAt` is set
        r.length = bytes.length;
        r.address = peer;
        return r;
    }

    /// Close `socket`.
    static UvRequest udpClose(int socket) nothrow @nogc @safe
    {
        UvRequest r;
        r.op = UvOp.udpClose;
        r.file = socket;
        return r;
    }

    /// Resolve `host` on libuv's pool into `into`, each address with `port`;
    /// `result` is how many were written. The pointer form takes a
    /// NUL-terminated host the caller keeps valid until publication; the
    /// `string` form copies it (`toStringz`).
    static UvRequest resolve(P : const(char)*)(return scope P host, ushort port,
        return scope UvAddress[] into) nothrow @nogc @system
    {
        UvRequest r;
        r.op = UvOp.resolve;
        r.path = host;
        r.buffer = cast(ubyte*) into.ptr;
        r.length = into.length; // addresses, not bytes
        r.address.port = port;
        return r;
    }

    /// ditto
    static UvRequest resolve(return scope string host, ushort port, return scope UvAddress[] into)
        nothrow @trusted
    {
        import std.string : toStringz;
        return resolve(toStringz(host), port, into);
    }

    /// Listen for TCP connections; `options` stays the caller's until
    /// publication. The `ref` forms here are `@trusted` as `udpOpen`'s is.
    static UvRequest tcpListen(return scope const(UvTcpOptions)* options) nothrow @nogc @safe
    {
        UvRequest r;
        r.op = UvOp.tcpListen;
        r.tcpOptions = options;
        return r;
    }

    /// ditto
    static UvRequest tcpListen(return ref const UvTcpOptions options) nothrow @nogc @trusted
    {
        return tcpListen(&options);
    }

    /// Connect a TCP stream to `options.connect`; `options` stays the
    /// caller's until publication.
    static UvRequest tcpConnect(return scope const(UvTcpOptions)* options) nothrow @nogc @safe
    {
        UvRequest r;
        r.op = UvOp.tcpConnect;
        r.tcpOptions = options;
        return r;
    }

    /// ditto
    static UvRequest tcpConnect(return ref const UvTcpOptions options) nothrow @nogc @trusted
    {
        return tcpConnect(&options);
    }

    /// Start reading accepted stream `socket` as `options` says; `options`
    /// stays the caller's until publication.
    static UvRequest tcpStart(int socket, return scope const(UvTcpOptions)* options)
        nothrow @nogc @safe
    {
        UvRequest r;
        r.op = UvOp.tcpStart;
        r.file = socket;
        r.tcpOptions = options;
        return r;
    }

    /// ditto
    static UvRequest tcpStart(int socket, return ref const UvTcpOptions options)
        nothrow @nogc @trusted
    {
        return tcpStart(socket, &options);
    }

    /// Write `bytes` to stream `socket`; `bytes` stays the caller's until
    /// publication.
    static UvRequest tcpWrite(int socket, return scope const(ubyte)[] bytes) nothrow @nogc @trusted
    {
        UvRequest r;
        r.op = UvOp.tcpWrite;
        r.file = socket;
        r.buffer = cast(ubyte*) bytes.ptr; // only read
        r.length = bytes.length;
        return r;
    }

    /// Shut the write side of stream `socket`.
    static UvRequest tcpShutdown(int socket) nothrow @nogc @safe
    {
        UvRequest r;
        r.op = UvOp.tcpShutdown;
        r.file = socket;
        return r;
    }

    /// Close listener or stream `socket`.
    static UvRequest tcpClose(int socket) nothrow @nogc @safe
    {
        UvRequest r;
        r.op = UvOp.tcpClose;
        r.file = socket;
        return r;
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
    @property inout(ubyte)[] bytes() inout nothrow @nogc @trusted
    {
        return block_[0 .. length_];
    }

    /// Bytes allocated: the file's size when the loop opened it, so at
    /// least `bytes.length`.
    @property size_t capacity() const nothrow @nogc @safe
    {
        return capacity_;
    }

    /// The block's alignment, as the request asked.
    @property size_t alignment() const nothrow @nogc @safe
    {
        return alignment_;
    }

    /// Return the block to its allocator and empty this copy. Does nothing
    /// when it is already empty.
    void free() nothrow @nogc @trusted
    {
        if (block_ is null) return;
        allocator_.deallocate(allocator_.context, block_, capacity_, alignment_);
        this = UvBuffer.init;
    }
}

// ---- UDP ---------------------------------------------------------------------

/// Which protocol a `UvAddress` names.
enum UvFamily : ushort
{
    /// Unset.
    none,
    /// IPv4: `bytes[0 .. 4]`.
    ipv4,
    /// IPv6: `bytes[0 .. 16]` and `scope_`.
    ipv6,
}

/// An IPv4 or IPv6 socket address, 32 bytes, the one type `ant_uv` uses
/// wherever it names an address: a socket's `bind` and `connect`, a send's
/// peer, a resolved name, and a received datagram's sender.
struct UvAddress
{
    /// The protocol; `none` is unset.
    UvFamily family;
    /// The port, in host byte order.
    ushort port;
    /// The address in network order: 4 bytes for IPv4, 16 for IPv6.
    ubyte[16] bytes;
    /// IPv6 scope (zone) id; 0 for none.
    uint scope_;
    private ulong reserved_;

    /// True unless `family` is `none`.
    @property bool valid() const nothrow @nogc @safe { return family != UvFamily.none; }

    /// Parse dotted IPv4 text; `none` when it does not parse or is
    /// `addressTextMax` bytes or longer. `@trusted`: the text is copied into
    /// a NUL-terminated buffer of that size before libuv reads it.
    static UvAddress ipv4(scope const(char)[] text, ushort port) nothrow @nogc @trusted
    {
        char[addressTextMax] terminated = void;
        if (!terminate(text, terminated)) return UvAddress.init;
        sockaddr_in sa;
        if (uv_ip4_addr(terminated.ptr, port, &sa) != 0) return UvAddress.init;
        return fromSockaddr(cast(const(sockaddr)*) &sa);
    }

    /// Parse IPv6 text, with an optional `%zone`; `none` as for `ipv4`.
    static UvAddress ipv6(scope const(char)[] text, ushort port) nothrow @nogc @trusted
    {
        char[addressTextMax] terminated = void;
        if (!terminate(text, terminated)) return UvAddress.init;
        sockaddr_in6 sa;
        if (uv_ip6_addr(terminated.ptr, port, &sa) != 0) return UvAddress.init;
        return fromSockaddr(cast(const(sockaddr)*) &sa);
    }

    /// Longest address text the parsers take, NUL included: room for the
    /// longest IPv6 text with a zone name.
    enum size_t addressTextMax = 128;

    // `text` copied into `into` with a NUL after it; false when it does not fit.
    private static bool terminate(scope const(char)[] text, ref char[addressTextMax] into)
        nothrow @nogc @safe
    {
        if (text.length >= into.length) return false;
        into[0 .. text.length] = text[];
        into[text.length] = 0;
        return true;
    }

    /// 127.0.0.1 at `port`.
    static UvAddress loopback4(ushort port) nothrow @nogc @safe
    {
        auto a = any4(port);
        a.bytes[0] = 127;
        a.bytes[3] = 1;
        return a;
    }

    /// 0.0.0.0 at `port`.
    static UvAddress any4(ushort port) nothrow @nogc @safe
    {
        UvAddress a;
        a.family = UvFamily.ipv4;
        a.port = port;
        return a;
    }

    /// ::1 at `port`.
    static UvAddress loopback6(ushort port) nothrow @nogc @safe
    {
        auto a = any6(port);
        a.bytes[15] = 1;
        return a;
    }

    /// :: at `port`; dual-stack unless the socket is `ipv6Only`.
    static UvAddress any6(ushort port) nothrow @nogc @safe
    {
        UvAddress a;
        a.family = UvFamily.ipv6;
        a.port = port;
        return a;
    }

    /// "1.2.3.4:80" or "[::1]:80" written into `buffer` (64 bytes always
    /// suffice), returned as the slice written; "unset" for `none`.
    char[] format(return scope char[] buffer) const scope nothrow @nogc @trusted
    {
        import core.stdc.stdio : snprintf;
        if (buffer.length == 0) return buffer;
        char[46] text = 0;
        sockaddr_storage storage;
        int n;
        if (toSockaddr(this, &storage) == 0)
            n = snprintf(buffer.ptr, buffer.length, "unset");
        else if (family == UvFamily.ipv4)
        {
            uv_ip4_name(cast(const(sockaddr_in)*) &storage, text.ptr, text.length);
            n = snprintf(buffer.ptr, buffer.length, "%s:%u", text.ptr, cast(uint) port);
        }
        else
        {
            uv_ip6_name(cast(const(sockaddr_in6)*) &storage, text.ptr, text.length);
            n = snprintf(buffer.ptr, buffer.length, "[%s]:%u", text.ptr, cast(uint) port);
        }
        if (n < 0) return buffer[0 .. 0];
        return buffer[0 .. n < buffer.length ? n : buffer.length - 1];
    }
}

static assert(UvAddress.sizeof == 32);

// Network byte order on the little-endian targets this builds for.
private ushort toNetworkOrder(ushort v) pure nothrow @nogc @safe
{
    version (LittleEndian) return cast(ushort)((v << 8) | (v >> 8));
    else return v;
}

// A UvAddress as a sockaddr in `storage`: the bytes it occupies, 0 for `none`.
private uint toSockaddr(ref const UvAddress a, sockaddr_storage* storage) nothrow @nogc
{
    memset(storage, 0, sockaddr_storage.sizeof);
    final switch (a.family)
    {
    case UvFamily.none:
        return 0;
    case UvFamily.ipv4:
    {
        auto sa = cast(sockaddr_in*) storage;
        sa.sin_family = cast(typeof(sa.sin_family)) AF_INET;
        sa.sin_port = toNetworkOrder(a.port);
        memcpy(&sa.sin_addr, a.bytes.ptr, 4);
        return sockaddr_in.sizeof;
    }
    case UvFamily.ipv6:
    {
        auto sa = cast(sockaddr_in6*) storage;
        sa.sin6_family = cast(typeof(sa.sin6_family)) AF_INET6;
        sa.sin6_port = toNetworkOrder(a.port);
        memcpy(&sa.sin6_addr, a.bytes.ptr, 16);
        sa.sin6_scope_id = a.scope_;
        return sockaddr_in6.sizeof;
    }
    }
}

private UvAddress fromSockaddr(const(sockaddr)* sa) nothrow @nogc
{
    UvAddress a;
    if (sa is null) return a;
    if (sa.sa_family == AF_INET)
    {
        auto v4 = cast(const(sockaddr_in)*) sa;
        a.family = UvFamily.ipv4;
        a.port = toNetworkOrder(v4.sin_port);
        memcpy(a.bytes.ptr, &v4.sin_addr, 4);
    }
    else if (sa.sa_family == AF_INET6)
    {
        auto v6 = cast(const(sockaddr_in6)*) sa;
        a.family = UvFamily.ipv6;
        a.port = toNetworkOrder(v6.sin6_port);
        memcpy(a.bytes.ptr, &v6.sin6_addr, 16);
        a.scope_ = v6.sin6_scope_id;
    }
    return a;
}

/// How `udpOpen` opens a socket. The record is the caller's until the open
/// publishes; the ring and the strings must outlive the socket.
struct UvUdpOptions
{
    /// The local address. Port 0 takes any, and the open publishes the one
    /// bound as `size`.
    UvAddress bind;
    /// Optional peer: sends then omit the address, and errors the network
    /// reports about the peer reach the socket.
    UvAddress connect;
    /// The ring every datagram goes to, one element each. Its capacity must
    /// cover the windows of every socket that shares it.
    SlotRing!RingElement* completions;
    /// The element tag that names this socket on the ring.
    ulong tag;
    /// Receive buffers, fixed for the socket's life. A datagram arriving
    /// while every buffer is held is dropped and counted.
    uint window = 64;
    /// Data bytes per buffer. A larger datagram is cut to this and flagged.
    uint maxDatagram = 1472;
    /// `SO_REUSEADDR` at bind.
    bool reuseAddress;
    /// Allow broadcast sends.
    bool broadcast;
    /// An IPv6 bind takes no IPv4-mapped traffic.
    bool ipv6Only;
    /// Time to live; 0 leaves the OS default.
    int ttl;
    /// The kernel's receive buffer; 0 leaves the OS default.
    int receiveBufferSize;
    /// The kernel's send buffer; 0 leaves the OS default.
    int sendBufferSize;
    /// A multicast group to join, as text, or null.
    const(char)* multicastGroup;
    /// The interface address for the group, or null for the OS's choice.
    const(char)* multicastInterface;
    /// Advanced once per element the socket pushes on the ring, so a Fiber
    /// can `waitNext` and then pop; null for none. Kept alive by its owner.
    FiberGenerationTrigger trigger;
}

/// How `tcpListen`, `tcpConnect` and `tcpStart` set a socket up. The record
/// is the caller's until the request publishes; the ring must outlive the
/// socket.
struct UvTcpOptions
{
    /// `tcpListen`: the local address, port 0 for any. `tcpConnect`: an
    /// optional local address to bind first.
    UvAddress bind;
    /// `tcpConnect`: the peer.
    UvAddress connect;
    /// The ring the socket delivers to: one element per accepted connection
    /// for a listener; one per chunk or stream event for a stream.
    SlotRing!RingElement* completions;
    /// The element tag that names this socket on the ring.
    ulong tag;
    /// A stream's receive buffers, fixed for its life. When every buffer is
    /// held the loop stops reading, and resumes within about a millisecond
    /// of a release; nothing is dropped. A listener reserves `backlog` ring
    /// slots instead.
    uint window = 16;
    /// Data bytes per receive buffer: the most one chunk carries.
    uint chunkBytes = 4096;
    /// `tcpListen`: pending connections the kernel queues.
    int backlog = 128;
    /// Disable Nagle's algorithm so a write goes out at once. On by
    /// default: the streams carry control traffic that waits on nothing.
    bool noDelay = true;
    /// TCP keep-alive probes after this many idle seconds; 0 leaves them off.
    uint keepAliveSeconds;
    /// An IPv6 bind takes no IPv4-mapped traffic.
    bool ipv6Only;
    /// The kernel's receive buffer; 0 leaves the OS default.
    int receiveBufferSize;
    /// The kernel's send buffer; 0 leaves the OS default.
    int sendBufferSize;
    /// Advanced once per element the socket pushes on the ring, so a Fiber
    /// can `waitNext` and then pop; null for none. Kept alive by its owner.
    FiberGenerationTrigger trigger;
}

/// `UvDatagram.flags` bits, and the kinds of stream event. A stream event is
/// an element with `pointer` 0 and no buffer: `payload[0]` holds the socket
/// id in its low 32 bits and the kind above them, `payload[1]` the libuv
/// status of an error, and `payload[1 .. 4]` the peer of an accepted
/// connection (`unpackAddress`). `UvReceived` presents them by value.
enum UvDatagramFlag : uint
{
    /// The OS cut the datagram to the socket's `maxDatagram`.
    truncated = 1,
    /// A stream event: the peer ended its side. Reading stopped; the stream
    /// may still be written to until it is closed.
    endOfStream = 2,
    /// A stream event: reading failed with the libuv code in `status`, and
    /// stopped.
    streamError = 4,
    /// A listener's event: a connection was accepted as the stream named
    /// by `socket`, from `peer`. It reads nothing until `tcpStart`.
    accepted = 8,
}

/// The kind of a stream event, in bits 32 and up of its element's
/// `payload[0]`.
enum StreamEvent : uint
{
    /// A listener accepted a connection: the new stream's id and its peer.
    accepted = 1,
    /// The peer ended its side of the stream.
    endOfStream = 2,
    /// Reading failed with the libuv code in `payload[1]`.
    error = 3,
}

/// Pack an address into `payload[1 .. 4]` of a stream event's element.
void packAddress(ref const UvAddress a, ref ulong[4] payload) nothrow @nogc @safe
{
    payload[1] = cast(ulong) a.family | (cast(ulong) a.port << 16) | (cast(ulong) a.scope_ << 32);
    ulong lo, hi;
    foreach (i; 0 .. 8)
    {
        lo |= cast(ulong) a.bytes[i] << (8 * i);
        hi |= cast(ulong) a.bytes[8 + i] << (8 * i);
    }
    payload[2] = lo;
    payload[3] = hi;
}

/// The address `packAddress` put in `payload`.
UvAddress unpackAddress(ref const ulong[4] payload) nothrow @nogc @safe
{
    UvAddress a;
    a.family = cast(UvFamily)(payload[1] & 0xFFFF);
    a.port = cast(ushort)(payload[1] >> 16);
    a.scope_ = cast(uint)(payload[1] >> 32);
    foreach (i; 0 .. 8)
    {
        a.bytes[i] = cast(ubyte)(payload[2] >> (8 * i));
        a.bytes[8 + i] = cast(ubyte)(payload[3] >> (8 * i));
    }
    return a;
}

/// A received datagram: the first 64 bytes of one of its socket's receive
/// buffers, with the data at offset 64. The consumer's from the pop until
/// `release`. A stream's chunks use the same header, with `peer` the
/// stream's peer. `@safe` code never sees this type: it pops `UvReceived`
/// from a `UvInbox` (`ant_uv.safe`), which carries the same header by value
/// and releases the buffer once.
struct UvDatagram
{
    union
    {
        /// The loop's clock when libuv delivered it (`UvLoop.nanos`).
        long arrivalNanos;
        private UvDatagram* next_; // while on the window's free or return list
    }
    /// Data bytes, at most the socket's `maxDatagram`.
    uint length;
    /// `UvDatagramFlag` bits.
    uint flags;
    /// The sender.
    UvAddress peer;
    /// The socket id it arrived on.
    uint socket;
    private uint reserved_;
    private UdpWindow* window_;

    /// The data.
    inout(ubyte)[] bytes() inout nothrow @nogc
    {
        return (cast(inout(ubyte)*) &this)[UvDatagram.sizeof .. UvDatagram.sizeof + length];
    }

    /// Return the buffer to its socket's window, from any thread, once.
    void release() nothrow @nogc
    {
        if (window_ !is null) window_.release(&this);
    }
}

static assert(UvDatagram.sizeof == 64);

/// A socket's counters, read with `UvLoop.socketStats`.
struct UvSocketStats
{
    /// Elements delivered to the ring: datagrams, chunks and stream events,
    /// or a listener's accepted connections.
    ulong received;
    /// Datagrams lost to an exhausted window or a full ring; connections a
    /// listener refused because the table or its ring was full; stream
    /// elements a closed ring refused.
    ulong dropped;
    /// Delivered datagrams the OS cut to `maxDatagram`.
    ulong truncated;
    /// Receive and accept errors libuv reported.
    ulong errors;
    /// Sends and writes that succeeded.
    ulong sent;
    /// Sends and writes that failed.
    ulong sendErrors;
    /// Times a stream's reading paused on an exhausted window.
    ulong paused;
}

/// ditto
alias UvUdpStats = UvSocketStats;

// A socket's receive window, for datagrams and for a stream's chunks:
// `count` buffers of `stride` bytes, each a UvDatagram header and
// `maxDatagram` data bytes. The loop thread takes buffers from the free
// list; any thread returns them through the atomic list; the last reference
// frees the memory, so datagrams outlive a close.
private struct UdpWindow
{
    void* memory_;
    size_t stride_;
    uint count_;
    UvDatagram* free_;     // loop thread only
    UvDatagram* spare_;    // read into and discarded while the window is exhausted
    shared size_t remote_; // UvDatagram*, released from any thread
    shared uint refs_;     // datagrams held outside the window, plus one for the socket

    static UdpWindow* create(uint count, uint maxDatagram) nothrow @nogc
    {
        auto w = cast(UdpWindow*) calloc(1, UdpWindow.sizeof);
        if (w is null) return null;
        w.stride_ = (UvDatagram.sizeof + maxDatagram + 63) & ~cast(size_t) 63;
        w.count_ = count;
        w.memory_ = allocateAligned64(w.stride_ * (count + 1));
        if (w.memory_ is null)
        {
            free(w);
            return null;
        }
        foreach_reverse (i; 0 .. count + 1)
        {
            auto d = cast(UvDatagram*)(cast(ubyte*) w.memory_ + i * w.stride_);
            *d = UvDatagram.init;
            d.window_ = w;
            if (i == count)
            {
                w.spare_ = d;
                continue;
            }
            d.next_ = w.free_;
            w.free_ = d;
        }
        atomicStore(w.refs_, 1u);
        return w;
    }

    // Loop thread: a free buffer, or null when the window is exhausted.
    UvDatagram* take() nothrow @nogc
    {
        if (free_ is null) collectReturned();
        auto d = free_;
        if (d !is null) free_ = d.next_;
        return d;
    }

    // Loop thread: true when no buffer is free, after collecting the ones
    // other threads returned.
    bool exhausted() nothrow @nogc
    {
        if (free_ is null) collectReturned();
        return free_ is null;
    }

    void collectReturned() nothrow @nogc
    {
        auto returned = cast(UvDatagram*) atomicExchange!(MemoryOrder.acq_rel)(&remote_, size_t(0));
        while (returned !is null)
        {
            auto next = returned.next_;
            returned.next_ = free_;
            free_ = returned;
            returned = next;
        }
    }

    // Loop thread: a buffer back without a datagram.
    void giveBack(UvDatagram* d) nothrow @nogc
    {
        d.next_ = free_;
        free_ = d;
    }

    // Any thread: a held datagram comes back.
    void release(UvDatagram* d) nothrow @nogc
    {
        for (;;)
        {
            immutable head = atomicLoad!(MemoryOrder.acq)(remote_);
            d.next_ = cast(UvDatagram*) head;
            if (cas(&remote_, head, cast(size_t) d)) break;
        }
        drop();
    }

    // One reference fewer; the last frees the window.
    void drop() nothrow @nogc
    {
        if (atomicFetchSub!(MemoryOrder.acq_rel)(refs_, 1u) == 1)
        {
            freeAligned64(memory_);
            free(&this);
        }
    }
}

private enum SocketKind : ubyte { udp, tcpListener, tcpStream }

// One entry of the loop's socket table: a UDP socket, a TCP listener or a
// TCP stream. `handle` is a uv_udp_t or a uv_tcp_t.
private struct Socket
{
    uv_handle_t* handle;
    UdpWindow* window;          // UDP, and a stream once it reads
    SlotRing!RingElement* ring;
    FiberGenerationTrigger trigger;
    UvLoop owner;
    UvRequest* closeRequest;
    OpSlot* connectSlot;        // a tcpConnect in flight
    UvAddress peer;             // a stream's peer, copied into each chunk
    ulong tag;
    ulong sequence;
    uint id;
    uint maxDatagram;           // a stream's chunkBytes
    uint committed;             // ring slots reserved: the window, or a listener's backlog
    SocketKind kind;
    bool inUse;
    bool closing;
    bool connected;             // UDP: a peer was connected; stream: the connection is up
    bool reading;               // stream: uv_read_start done
    bool paused;                // stream: reading stopped on an exhausted window
    bool ended;                 // stream: end of stream or an error was delivered
    shared UvSocketStats stats;

    uv_udp_t* udp() nothrow @nogc { return cast(uv_udp_t*) handle; }
    uv_tcp_t* tcp() nothrow @nogc { return cast(uv_tcp_t*) handle; }
    uv_stream_t* stream() nothrow @nogc { return cast(uv_stream_t*) handle; }
}

// Windows reserved on one ring by the sockets that share it.
private struct RingCommit
{
    SlotRing!RingElement* ring;
    uint committed;
}

// On the loop thread, inside libuv's receive callback: deliver one datagram,
// hand back an empty buffer, or count an error.
private void deliverDatagram(Socket* s, ptrdiff_t nread, const(uv_buf_t)* buf,
    const(sockaddr)* addr, uint flags) nothrow @nogc
{
    auto d = buf !is null && buf.base !is null
        ? cast(UvDatagram*)(buf.base - UvDatagram.sizeof) : null;
    immutable spare = d !is null && d is s.window.spare_;
    if (nread == 0 && addr is null)
    {
        if (d !is null && !spare) s.window.giveBack(d);
        return;
    }
    if (nread < 0)
    {
        if (d !is null && !spare) s.window.giveBack(d);
        atomicOp!"+="(s.stats.errors, 1);
        return;
    }
    if (d is null) return;
    if (spare)
    {
        // The window was exhausted: the datagram was read so the socket moves
        // on, and is discarded.
        atomicOp!"+="(s.stats.dropped, 1);
        return;
    }
    d.arrivalNanos = UvLoop.nanos();
    d.length = cast(uint) nread;
    d.flags = (flags & UV_UDP_PARTIAL) != 0 ? UvDatagramFlag.truncated : 0;
    d.peer = fromSockaddr(addr);
    d.socket = s.id;
    RingElement e;
    e.tag = s.tag;
    e.generation = ++s.sequence;
    e.pointer = cast(ulong) cast(size_t) d;
    e.payload[0] = d.length | (cast(ulong) d.flags << 32);
    e.payload[1] = cast(ulong) d.arrivalNanos;
    e.payload[2] = s.id;
    atomicFetchAdd!(MemoryOrder.acq_rel)(s.window.refs_, 1u);
    if (s.ring.tryPush(e) == RingPush.pushed)
    {
        atomicOp!"+="(s.stats.received, 1);
        if (d.flags & UvDatagramFlag.truncated) atomicOp!"+="(s.stats.truncated, 1);
        if (s.trigger !is null) s.trigger.advance();
        return;
    }
    // Never the last reference: the socket holds one while open.
    atomicFetchSub!(MemoryOrder.acq_rel)(s.window.refs_, 1u);
    s.window.giveBack(d);
    atomicOp!"+="(s.stats.dropped, 1);
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
    /// abandons the rest. An abandoned `readFile` job frees its own block
    /// when it ends and counts itself out of `UvLoop.abandonedJobs`; zero
    /// here means accepting such jobs on every close that finds one running.
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
    /// Requests published `abandoned`: their operations may still be
    /// running. `UvLoop.abandonedJobs` counts the `readFile` jobs among them
    /// that have not ended.
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
private enum int alreadyPublished = 1; // a start that published synchronously
private enum size_t readChunk = size_t(1) << 30; // a Windows uv_buf_t length is 32-bit
private enum uint submitBatch = 64; // requests dequeued per loop iteration
private enum uint controlCapacity = 256; // control ring slots (cancellations)
private enum uint maxSockets = 256; // open sockets per loop, of every kind
private enum long pausedPollMs = 1; // how often a paused stream's window is rechecked
private enum size_t maxRingCommits = 64; // distinct rings UDP sockets may deliver to

private enum State : int { starting, open, failed, closed }
private enum ClosePhase : ubyte { open, grace, cancelling }
// A readFile job's handoff with the loop thread, in `OpSlot.phase`. The job
// moves `running` to `done` at its end; `abandonOutstanding` moves `running`
// to `abandoned`. Whoever moves it owns the block from then on.
private enum JobPhase : uint { running, done, abandoned }
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
    bool queueCancelled; // uv_cancel took the job before a thread did: its callback is posted
    shared bool stop; // a cancel the running job polls between steps
    shared uint phase; // JobPhase
    shared(uint)* abandonedJobs; // the loop's count, in memory that outlives the loop
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
    FiberGenerationTrigger trigger; // advanced once the element is on the ring
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
    /// `@system`: the loop keeps the pointer until publication. `@safe` code
    /// submits through `submitAndBlock`, a Fiber's `submitAndWait`, or a
    /// `UvOutbox`.
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

    /// Submit `request` and hold the calling thread in short sleeps until
    /// the loop publishes it, retrying while the request ring is full. The
    /// request is the caller's again when this returns, whatever it
    /// returns, so a request on the caller's stack is sound: this is how
    /// `@safe` code outside a Fiber submits, for example to open a socket
    /// or resolve a name during setup. The request may name an `event` or a
    /// `trigger` but not a `callback` or a `completions` ring, whose
    /// deliveries would follow the return (`invalid`). Any thread but the
    /// loop's, where it would wait for itself (`UvLoopException`).
    UvSubmit submitAndBlock(scope ref UvRequest request) @trusted
    {
        if (onLoopThread)
            throw new UvLoopException(
                "ant_uv: submitAndBlock on the loop thread would wait for itself");
        if (request.callback !is null || request.completions !is null) return UvSubmit.invalid;
        for (;;)
        {
            immutable r = submit(&request);
            if (r == UvSubmit.accepted) break;
            if (r != UvSubmit.full) return r;
            Thread.sleep(50.usecs);
        }
        while (!request.finished)
            Thread.sleep(50.usecs);
        return UvSubmit.accepted;
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
    void wake() nothrow @nogc @trusted
    {
        immutable before = atomicFetchAdd!(MemoryOrder.acq_rel)(wakeGate_, 1UL);
        if ((before & gateClosed) == 0)
            uv_async_send(async_);
        atomicFetchSub!(MemoryOrder.acq_rel)(wakeGate_, 1UL);
    }

    /// Close: refuse new requests, keep serving for `closeGrace`, then
    /// cancel what is outstanding and wait `abandonAfter` for libuv's
    /// terminal callbacks, publish anything still running as `abandoned`,
    /// run the `stop` hook, close the loop and join its thread. A `readFile`
    /// job that finished during the wait but whose callback the loop had
    /// not yet run is collected rather than abandoned; one still running is
    /// counted in `abandonedJobs`. Any thread but the loop's; concurrent
    /// and repeated calls return the same report.
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
            if (resumePaused()) progress = true;
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
    static @property UvLoop current() nothrow @nogc @safe
    {
        return tlsCurrent_;
    }

    /// True on the loop thread.
    @property bool onLoopThread() const nothrow @nogc @safe
    {
        return thread_ !is null && Thread.getThis() is thread_;
    }

    /// True until `close` begins or the loop fails.
    @property bool isOpen() const nothrow @nogc @safe
    {
        return atomicLoad!(MemoryOrder.acq)(state_) == State.open
            && atomicLoad!(MemoryOrder.acq)(closeRequested_) == 0
            && atomicLoad!(MemoryOrder.acq)(failed_) == 0;
    }

    /// True once the loop thread recorded a failure.
    @property bool failed() const nothrow @nogc @safe
    {
        return atomicLoad!(MemoryOrder.acq)(failed_) != 0;
    }

    /// Requests accepted so far (tickets issued, including refused pushes'
    /// discarded tickets).
    @property ulong submitted() const nothrow @nogc @safe { return atomicLoad(tickets_); }
    /// Requests published so far.
    @property ulong completed() const nothrow @nogc @safe { return atomicLoad(completed_); }
    /// Requests published with `UV_ECANCELED` so far.
    @property ulong cancelled() const nothrow @nogc @safe { return atomicLoad(cancelled_); }
    /// Most operations in libuv at once so far.
    @property ulong maxInFlight() const nothrow @nogc @safe { return atomicLoad(maxInFlight_); }
    /// Completions that waited for room in a completion ring so far.
    @property ulong deferredDeliveries() const nothrow @nogc @safe { return atomicLoad(deferred_); }
    /// The credit limit in effect.
    @property uint credits() const nothrow @nogc @safe { return slotCount_; }

    /// `readFile` jobs that `close` abandoned and that have not ended. Each
    /// may still call its allocator and write into its `buffer`; when it
    /// ends it frees the block it allocated and leaves this count. Zero
    /// after `close` means no abandoned job will touch an allocator again.
    /// Any thread, at any time; zero before `close`.
    @property uint abandonedJobs() const nothrow @nogc @trusted
    {
        auto cell = abandonedJobs_;
        return cell is null ? 0 : atomicLoad!(MemoryOrder.acq)(*cell);
    }
    /// libuv's auxiliary threads as placed by `create`.
    @property const(UvAuxThread)[] auxThreads() const nothrow @nogc @safe { return auxThreads_; }
    /// True when `UvLoopOptions.ioUring` took effect: Linux 5.10.186 or
    /// later, libuv accepted the option, and `UV_USE_IO_URING` is positive.
    /// The kernel may still refuse the ring, and libuv then uses its pool.
    @property bool ioUring() const nothrow @nogc @safe { return ioUring_; }

    /// The loop's clock: `MonoTime` in nanoseconds, the unit of every
    /// transport timestamp (`UvDatagram.arrivalNanos`, `UvRequest.stampAt`).
    /// Any thread; the caller's own `MonoTime` compares through it.
    static long nanos() nothrow @nogc @safe
    {
        return convClockFreq(MonoTime.currTime.ticks, MonoTime.ticksPerSecond, 1_000_000_000);
    }

    /// A socket's counters, meaningful between its open's and its close's
    /// publications; an id out of range reads as zeros.
    UvSocketStats socketStats(int socket) const nothrow @nogc @trusted
    {
        UvSocketStats r;
        if (socket < 0 || socket >= maxSockets || sockets_ is null) return r;
        auto s = &sockets_[socket];
        r.received = atomicLoad(s.stats.received);
        r.dropped = atomicLoad(s.stats.dropped);
        r.truncated = atomicLoad(s.stats.truncated);
        r.errors = atomicLoad(s.stats.errors);
        r.sent = atomicLoad(s.stats.sent);
        r.sendErrors = atomicLoad(s.stats.sendErrors);
        r.paused = atomicLoad(s.stats.paused);
        return r;
    }

    /// ditto
    alias udpStats = socketStats;

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
    Socket* sockets_; // maxSockets entries, allocated before the loop opens
    uint socketsClosing_;
    uint pausedStreams_; // streams whose reading stopped on an exhausted window
    RingCommit[maxRingCommits] ringCommits_;
    ClosePhase closePhase_;
    long phaseDeadline_;
    bool loopAbandoned_;
    shared(uint)* abandonedJobs_; // leaked with the slots when a job is abandoned
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
        // Abandoned jobs still subtract from the cell: leave it to them.
        if (abandonedJobs_ !is null && !loopAbandoned_) free(cast(void*) abandonedJobs_);
        abandonedJobs_ = null;
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
            if (r.length > uint.max && (r.op == UvOp.read || r.op == UvOp.write
                    || r.op == UvOp.udpSend || r.op == UvOp.tcpWrite)) return false;
        final switch (r.op)
        {
        case UvOp.nop, UvOp.timer:
            return true;
        case UvOp.udpOpen:
        {
            auto o = r.udpOptions;
            return o !is null && o.completions !is null && o.bind.valid && o.window != 0
                && o.maxDatagram != 0;
        }
        case UvOp.udpSend:
            return r.file >= 0 && (r.buffer !is null || r.length == 0)
                && (r.stampAt == uint.max || cast(ulong) r.stampAt + 8 <= r.length);
        case UvOp.udpClose:
            return r.file >= 0;
        case UvOp.tcpListen:
        {
            auto o = r.tcpOptions;
            return o !is null && o.completions !is null && o.bind.valid && o.backlog > 0;
        }
        case UvOp.tcpConnect:
        {
            auto o = r.tcpOptions;
            return o !is null && o.completions !is null && o.connect.valid && o.window != 0
                && o.chunkBytes != 0;
        }
        case UvOp.tcpStart:
        {
            auto o = r.tcpOptions;
            return r.file >= 0 && o !is null && o.completions !is null && o.window != 0
                && o.chunkBytes != 0;
        }
        case UvOp.tcpWrite:
            return r.file >= 0 && (r.buffer !is null || r.length == 0);
        case UvOp.tcpShutdown, UvOp.tcpClose:
            return r.file >= 0;
        case UvOp.resolve:
            return r.path !is null && (r.buffer !is null || r.length == 0);
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
            if (resumePaused()) progress = true;
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
            if (pendingCount_ != 0 || pausedStreams_ != 0)
            {
                // A completion ring's consumer frees room, and a chunk's
                // holder releases it, without telling the loop: retry on a
                // short timer rather than only on wakes.
                immutable retry = now + toTicks(pausedPollMs.msecs);
                if (retry < deadline) deadline = retry;
            }
            runOnce(progress, deadline);
        }
    }

    bool settled() nothrow @nogc
    {
        return requests_.drained && slotsInUse_ == 0 && pendingCount_ == 0 && socketsClosing_ == 0;
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

    // Close gave up waiting. A readFile job still running is abandoned: it
    // frees its own block when it ends and counts itself out of
    // `abandonedJobs`. One that already ended, or that uv_cancel took before
    // any thread did, has its callback posted: that one is collected here,
    // a completion rather than an abandonment.
    void abandonOutstanding() nothrow @nogc
    {
        foreach (i; 0 .. slotCount_)
        {
            auto slot = &slots_[i];
            if (!slot.inUse) continue;
            if (slot.op == UvOp.readFile && collectReadJob(slot)) continue;
            loopAbandoned_ = true; // libuv still holds this slot's memory
            if (slot.request is null) continue;
            auto request = slot.request;
            slot.request = null;
            publish(request, UV_ECANCELED, 0, UvRequestState.abandoned);
        }
        foreach (i; 0 .. maxSockets)
        {
            auto s = &sockets_[i];
            if (!s.inUse || s.closeRequest is null) continue;
            loopAbandoned_ = true; // libuv still holds the handle
            auto request = s.closeRequest;
            s.closeRequest = null;
            publish(request, UV_ECANCELED, 0, UvRequestState.abandoned);
        }
        atomicStore(undelivered_, atomicLoad(undelivered_) + pendingCount_);
        pendingCount_ = 0;
    }

    // True when the job's callback ran and released the slot; false when the
    // job is still running and is now abandoned.
    bool collectReadJob(OpSlot* slot) nothrow @nogc
    {
        if (!slot.queueCancelled)
        {
            // Count before the move: a job that finds itself abandoned
            // subtracts, and must never see zero.
            atomicFetchAdd!(MemoryOrder.acq_rel)(*abandonedJobs_, 1u);
            if (cas(&slot.phase, cast(uint) JobPhase.running, cast(uint) JobPhase.abandoned))
                return false;
            atomicFetchSub!(MemoryOrder.acq_rel)(*abandonedJobs_, 1u);
        }
        // The job returned, or never ran: libuv posts its callback within
        // microseconds of that. Run the loop until the callback has run.
        while (slot.inUse)
        {
            uv_run(loop_, uv_run_mode.UV_RUN_NOWAIT);
            if (slot.inUse) core.atomic.pause();
        }
        return true;
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
        // Socket setup and teardown publish from the loop thread without a
        // slot; connects, writes and shutdowns are operations like any other.
        switch (request.op)
        {
        case UvOp.udpOpen:
            openUdp(request);
            return;
        case UvOp.udpClose:
            closeSocket(request, false);
            return;
        case UvOp.tcpListen:
            listenTcp(request);
            return;
        case UvOp.tcpStart:
            startTcp(request);
            return;
        case UvOp.tcpClose:
            closeSocket(request, true);
            return;
        default:
            break;
        }
        auto slot = takeSlot();
        slot.request = request;
        slot.ticket = ticket;
        slot.op = request.op;
        slot.cancelling = false;
        slot.queueCancelled = false;
        atomicStore!(MemoryOrder.raw)(slot.stop, false);
        atomicStore!(MemoryOrder.raw)(slot.phase, cast(uint) JobPhase.running);
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
        case UvOp.udpOpen, UvOp.udpClose, UvOp.tcpListen, UvOp.tcpStart, UvOp.tcpClose:
            assert(0);
        case UvOp.udpSend:
            rc = sendUdp(slot, request);
            if (rc == alreadyPublished) return;
            break;
        case UvOp.resolve:
            rc = startResolve(slot, request);
            break;
        case UvOp.tcpConnect:
            rc = connectTcp(slot, request);
            break;
        case UvOp.tcpWrite:
            rc = writeTcp(slot, request);
            if (rc == alreadyPublished) return;
            break;
        case UvOp.tcpShutdown:
            rc = shutdownTcp(slot, request);
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
    // touch it), the allocator, and atomics. The read, then the handoff.
    static void runReadJob(OpSlot* slot) nothrow @nogc
    {
        readJobSteps(slot);
        // A job that ends first is the loop thread's: its after-work callback
        // hands the block over or frees it. A job that close abandoned has no
        // callback coming: it frees the block itself and counts itself out,
        // so the allocator's owner can learn when the job is over.
        if (cas(&slot.phase, cast(uint) JobPhase.running, cast(uint) JobPhase.done)) return;
        if (slot.allocated)
        {
            slot.allocator.deallocate(slot.allocator.context, slot.buffer, slot.capacity,
                slot.alignment);
            slot.allocated = false;
        }
        atomicFetchSub!(MemoryOrder.acq_rel)(*slot.abandonedJobs, 1u);
    }

    static void readJobSteps(OpSlot* slot) nothrow @nogc
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

        // A cancel can land before the job runs, so `path` is read only once
        // the job knows it was not cancelled.
        if (stopped())
        {
            slot.status = UV_ECANCELED;
            return;
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
        // The fstat may have waited past a cancel: do not allocate for it.
        if (stopped()) return closeFile(UV_ECANCELED);
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

    // ---- the socket table -----------------------------------------------------

    Socket* socketAt(int id) nothrow @nogc
    {
        if (id < 0 || id >= maxSockets) return null;
        auto s = &sockets_[id];
        return s.inUse ? s : null;
    }

    // A fresh table entry, not yet in use, or null when the table is full.
    Socket* allocateSocket(SocketKind kind) nothrow @nogc
    {
        foreach (i; 0 .. maxSockets)
        {
            auto s = &sockets_[i];
            if (s.inUse) continue;
            *s = Socket.init;
            s.owner = this;
            s.id = i;
            s.kind = kind;
            return s;
        }
        return null;
    }

    // Undo a setup that failed before the entry was in use: an initialized
    // handle is closed and freed by libuv; the window and the ring
    // reservation go now.
    void discardSocket(Socket* s) nothrow @nogc
    {
        if (s.handle !is null)
        {
            uv_handle_set_data(s.handle, null);
            uv_close(s.handle, &onHandleFreed);
            s.handle = null;
        }
        if (s.window !is null)
        {
            s.window.drop();
            s.window = null;
        }
        if (s.ring !is null && s.committed != 0) uncommitRing(s.ring, s.committed);
        *s = Socket.init;
    }

    // udpClose and tcpClose: the request publishes from the close callback.
    void closeSocket(UvRequest* request, bool stream) nothrow @nogc
    {
        auto s = socketAt(request.file);
        if (s is null || s.closing)
        {
            publish(request, UV_EBADF, 0);
            return;
        }
        if ((s.kind == SocketKind.udp) == stream)
        {
            publish(request, UV_EINVAL, 0);
            return;
        }
        s.closeRequest = request;
        beginSocketClose(s);
    }

    void beginSocketClose(Socket* s) nothrow @nogc
    {
        s.closing = true;
        ++socketsClosing_;
        if (s.kind == SocketKind.udp)
            uv_udp_recv_stop(s.udp);
        else if (s.reading)
        {
            uv_read_stop(s.stream);
            s.reading = false;
        }
        if (s.paused)
        {
            s.paused = false;
            --pausedStreams_;
        }
        uv_close(s.handle, &onSocketClosed);
    }

    // libuv's close callback: the handle is gone; a window goes with the
    // last buffer the application returns.
    void socketClosed(Socket* s) nothrow @nogc
    {
        free(s.handle);
        s.handle = null;
        if (s.ring !is null && s.committed != 0) uncommitRing(s.ring, s.committed);
        if (s.window !is null)
        {
            s.window.drop();
            s.window = null;
        }
        auto request = s.closeRequest;
        s.closeRequest = null;
        s.inUse = false;
        s.closing = false;
        --socketsClosing_;
        if (request !is null) publish(request, 0, 0);
    }

    void closeAllSockets() nothrow @nogc
    {
        if (sockets_ is null) return;
        foreach (i; 0 .. maxSockets)
        {
            auto s = &sockets_[i];
            if (s.inUse && !s.closing) beginSocketClose(s);
        }
    }

    // ---- UDP sockets ----------------------------------------------------------

    // Reserve `window` elements of `ring` for a socket: false when the ring
    // could not hold the windows that would share it, or the table is full.
    bool commitRing(SlotRing!RingElement* ring, uint window) nothrow @nogc
    {
        RingCommit* entry = null;
        foreach (ref c; ringCommits_)
            if (c.ring is ring)
            {
                entry = &c;
                break;
            }
        if (entry is null)
            foreach (ref c; ringCommits_)
                if (c.ring is null)
                {
                    entry = &c;
                    break;
                }
        if (entry is null) return false;
        immutable total = cast(ulong) entry.committed + window;
        if (total > ring.capacity) return false;
        entry.ring = ring;
        entry.committed = cast(uint) total;
        return true;
    }

    void uncommitRing(SlotRing!RingElement* ring, uint window) nothrow @nogc
    {
        foreach (ref c; ringCommits_)
            if (c.ring is ring)
            {
                c.committed -= window;
                if (c.committed == 0) c.ring = null;
                return;
            }
    }

    // udpOpen is synchronous on the loop thread: it publishes before it returns.
    void openUdp(UvRequest* request) nothrow @nogc
    {
        auto o = request.udpOptions;
        auto s = allocateSocket(SocketKind.udp);
        if (s is null)
        {
            publish(request, UV_EMFILE, 0);
            return;
        }
        // The record is read-only; the ring it names is the application's to write.
        auto ring = cast(SlotRing!RingElement*) o.completions;
        if (!commitRing(ring, o.window))
        {
            *s = Socket.init;
            publish(request, UV_ENOBUFS, 0);
            return;
        }
        s.tag = o.tag;
        s.ring = ring;
        s.trigger = cast(FiberGenerationTrigger) o.trigger; // the record is const, the trigger is the owner's
        s.committed = o.window;
        s.maxDatagram = o.maxDatagram;
        s.handle = cast(uv_handle_t*) malloc(uv_handle_size(uv_handle_type.UV_UDP));
        s.window = UdpWindow.create(o.window, o.maxDatagram);
        int rc = s.handle is null || s.window is null ? UV_ENOMEM : uv_udp_init(loop_, s.udp);
        if (rc < 0)
        {
            free(s.handle); // not initialized: nothing in libuv refers to it
            s.handle = null;
            discardSocket(s);
            publish(request, rc, 0);
            return;
        }
        uv_handle_set_data(s.handle, s);
        ushort port;
        rc = configureUdp(s, o, port);
        if (rc < 0)
        {
            discardSocket(s);
            publish(request, rc, 0);
            return;
        }
        s.inUse = true;
        publish(request, s.id, port);
    }

    // Bind, options, connect, read back the port, and start receiving.
    int configureUdp(Socket* s, const(UvUdpOptions)* o, out ushort port) nothrow @nogc
    {
        sockaddr_storage storage;
        if (toSockaddr(o.bind, &storage) == 0) return UV_EINVAL;
        immutable flags = (o.ipv6Only ? UV_UDP_IPV6ONLY : 0) | (o.reuseAddress ? UV_UDP_REUSEADDR : 0);
        int rc = uv_udp_bind(s.udp, cast(const(sockaddr)*) &storage, flags);
        if (rc == 0 && o.broadcast) rc = uv_udp_set_broadcast(s.udp, 1);
        if (rc == 0 && o.ttl != 0) rc = uv_udp_set_ttl(s.udp, o.ttl);
        if (rc == 0 && o.receiveBufferSize != 0)
        {
            int value = o.receiveBufferSize;
            rc = uv_recv_buffer_size(s.handle, &value);
        }
        if (rc == 0 && o.sendBufferSize != 0)
        {
            int value = o.sendBufferSize;
            rc = uv_send_buffer_size(s.handle, &value);
        }
        if (rc == 0 && o.multicastGroup !is null)
            rc = uv_udp_set_membership(s.udp, o.multicastGroup, o.multicastInterface,
                uv_membership.UV_JOIN_GROUP);
        if (rc == 0 && o.connect.valid)
        {
            sockaddr_storage peer;
            if (toSockaddr(o.connect, &peer) == 0) rc = UV_EINVAL;
            else rc = uv_udp_connect(s.udp, cast(const(sockaddr)*) &peer);
            s.connected = rc == 0;
        }
        if (rc == 0)
        {
            int length = sockaddr_storage.sizeof;
            rc = uv_udp_getsockname(s.udp, cast(sockaddr*) &storage, &length);
            if (rc == 0) port = fromSockaddr(cast(const(sockaddr)*) &storage).port;
        }
        if (rc == 0) rc = uv_udp_recv_start(s.udp, &onUdpAlloc, &onUdpRecv);
        return rc;
    }

    // A send goes out at once when the socket can take it, and publishes
    // here; otherwise it waits in libuv with the slot.
    int sendUdp(OpSlot* slot, UvRequest* request) nothrow @nogc
    {
        auto s = socketAt(request.file);
        if (s is null || s.closing || s.kind != SocketKind.udp) return UV_EBADF;
        sockaddr_storage storage;
        const(sockaddr)* addr = null;
        if (!s.connected)
        {
            if (toSockaddr(request.address, &storage) == 0) return UV_EINVAL;
            addr = cast(const(sockaddr)*) &storage;
        }
        if (request.stampAt != uint.max)
        {
            immutable now = nanos();
            foreach (i; 0 .. 8)
                request.buffer[request.stampAt + i] = cast(ubyte)(now >> (8 * i));
        }
        auto buf = makeBuf(request.buffer, request.length);
        immutable n = uv_udp_try_send(s.udp, &buf, 1, addr);
        if (n >= 0)
        {
            atomicOp!"+="(s.stats.sent, 1);
            slot.request = null;
            finishSlot(slot);
            publish(request, n, 0);
            return alreadyPublished;
        }
        if (n != UV_EAGAIN)
        {
            atomicOp!"+="(s.stats.sendErrors, 1);
            return n;
        }
        slot.file = request.file;
        immutable rc = armed(slot, uv_udp_send(cast(uv_udp_send_t*) slot.uv, s.udp, &buf, 1,
            addr, &onUdpSendDone));
        if (rc < 0) atomicOp!"+="(s.stats.sendErrors, 1);
        return rc;
    }

    void udpSendDone(OpSlot* slot, int status) nothrow @nogc
    {
        auto s = socketAt(slot.file);
        if (s !is null)
        {
            if (status < 0) atomicOp!"+="(s.stats.sendErrors, 1);
            else atomicOp!"+="(s.stats.sent, 1);
        }
        auto request = slot.request;
        slot.request = null;
        releaseSlot(slot);
        if (request !is null) publish(request, status < 0 ? status : cast(long) request.length, 0);
    }

    // ---- TCP streams ----------------------------------------------------------

    // A TCP handle initialized on the loop and named by `s`; the libuv code
    // when that fails.
    int initTcp(Socket* s) nothrow @nogc
    {
        s.handle = cast(uv_handle_t*) malloc(uv_handle_size(uv_handle_type.UV_TCP));
        if (s.handle is null) return UV_ENOMEM;
        immutable rc = uv_tcp_init(loop_, s.tcp);
        if (rc < 0)
        {
            free(s.handle);
            s.handle = null;
            return rc;
        }
        uv_handle_set_data(s.handle, s);
        return 0;
    }

    int bindTcp(Socket* s, ref const UvAddress address, bool ipv6Only, out ushort port)
        nothrow @nogc
    {
        sockaddr_storage storage;
        if (toSockaddr(address, &storage) == 0) return UV_EINVAL;
        int rc = uv_tcp_bind(s.tcp, cast(const(sockaddr)*) &storage, ipv6Only ? UV_TCP_IPV6ONLY : 0);
        if (rc < 0) return rc;
        return localPort(s, port);
    }

    int localPort(Socket* s, out ushort port) nothrow @nogc
    {
        sockaddr_storage storage;
        int length = sockaddr_storage.sizeof;
        immutable rc = uv_tcp_getsockname(s.tcp, cast(sockaddr*) &storage, &length);
        if (rc == 0) port = fromSockaddr(cast(const(sockaddr)*) &storage).port;
        return rc;
    }

    // Nagle, keep-alive and the kernel buffers, on a connected stream.
    int applyStreamOptions(Socket* s, const(UvTcpOptions)* o) nothrow @nogc
    {
        int rc = uv_tcp_nodelay(s.tcp, o.noDelay ? 1 : 0);
        if (rc == 0 && o.keepAliveSeconds != 0) rc = uv_tcp_keepalive(s.tcp, 1, o.keepAliveSeconds);
        if (rc == 0 && o.receiveBufferSize != 0)
        {
            int value = o.receiveBufferSize;
            rc = uv_recv_buffer_size(s.handle, &value);
        }
        if (rc == 0 && o.sendBufferSize != 0)
        {
            int value = o.sendBufferSize;
            rc = uv_send_buffer_size(s.handle, &value);
        }
        return rc;
    }

    // Give a stream its ring and window and start reading; undone on failure.
    int startReading(Socket* s, const(UvTcpOptions)* o) nothrow @nogc
    {
        auto ring = cast(SlotRing!RingElement*) o.completions;
        if (!commitRing(ring, o.window)) return UV_ENOBUFS;
        auto window = UdpWindow.create(o.window, o.chunkBytes);
        if (window is null)
        {
            uncommitRing(ring, o.window);
            return UV_ENOMEM;
        }
        s.ring = ring;
        s.tag = o.tag;
        s.trigger = cast(FiberGenerationTrigger) o.trigger; // the record is const, the trigger is the owner's
        s.committed = o.window;
        s.maxDatagram = o.chunkBytes;
        s.window = window;
        int rc = applyStreamOptions(s, o);
        if (rc == 0) rc = uv_read_start(s.stream, &onStreamAlloc, &onStreamRead);
        if (rc < 0)
        {
            window.drop();
            uncommitRing(ring, o.window);
            s.window = null;
            s.ring = null;
            s.trigger = null;
            s.committed = 0;
            return rc;
        }
        s.reading = true;
        return 0;
    }

    // tcpListen is synchronous on the loop thread.
    void listenTcp(UvRequest* request) nothrow @nogc
    {
        auto o = request.tcpOptions;
        auto s = allocateSocket(SocketKind.tcpListener);
        if (s is null)
        {
            publish(request, UV_EMFILE, 0);
            return;
        }
        auto ring = cast(SlotRing!RingElement*) o.completions;
        immutable reserve = cast(uint) o.backlog;
        if (!commitRing(ring, reserve))
        {
            *s = Socket.init;
            publish(request, UV_ENOBUFS, 0);
            return;
        }
        s.ring = ring;
        s.tag = o.tag;
        s.trigger = cast(FiberGenerationTrigger) o.trigger; // the record is const, the trigger is the owner's
        s.committed = reserve;
        ushort port;
        int rc = initTcp(s);
        if (rc == 0) rc = bindTcp(s, o.bind, o.ipv6Only, port);
        if (rc == 0) rc = uv_listen(s.stream, o.backlog, &onConnection);
        if (rc < 0)
        {
            discardSocket(s);
            publish(request, rc, 0);
            return;
        }
        s.inUse = true;
        publish(request, s.id, port);
    }

    // libuv's connection callback on a listener: accept into a new table
    // entry and tell the ring, or take the connection and close it when
    // there is no room, so the kernel's queue moves on.
    void connection(Socket* listener, int status) nothrow @nogc
    {
        if (status < 0)
        {
            atomicOp!"+="(listener.stats.errors, 1);
            return;
        }
        auto s = allocateSocket(SocketKind.tcpStream);
        if (s is null)
        {
            refuseConnection(listener);
            return;
        }
        int rc = initTcp(s);
        if (rc == 0) rc = uv_accept(listener.stream, s.stream);
        if (rc < 0)
        {
            discardSocket(s);
            atomicOp!"+="(listener.stats.errors, 1);
            return;
        }
        s.connected = true;
        s.inUse = true;
        sockaddr_storage storage;
        int length = sockaddr_storage.sizeof;
        if (uv_tcp_getpeername(s.tcp, cast(sockaddr*) &storage, &length) == 0)
            s.peer = fromSockaddr(cast(const(sockaddr)*) &storage);
        RingElement e;
        e.tag = listener.tag;
        e.generation = ++listener.sequence;
        e.payload[0] = s.id | (cast(ulong) StreamEvent.accepted << 32);
        packAddress(s.peer, e.payload);
        if (listener.ring.tryPush(e) == RingPush.pushed)
        {
            atomicOp!"+="(listener.stats.received, 1);
            if (listener.trigger !is null) listener.trigger.advance();
            return;
        }
        // The ring would not take it: the connection is closed, as a
        // dropped datagram is discarded.
        atomicOp!"+="(listener.stats.dropped, 1);
        beginSocketClose(s);
    }

    void refuseConnection(Socket* listener) nothrow @nogc
    {
        auto h = cast(uv_tcp_t*) malloc(uv_handle_size(uv_handle_type.UV_TCP));
        if (h is null || uv_tcp_init(loop_, h) < 0)
        {
            free(h);
            atomicOp!"+="(listener.stats.errors, 1);
            return;
        }
        cast(void) uv_accept(listener.stream, cast(uv_stream_t*) h);
        uv_close(cast(uv_handle_t*) h, &onHandleFreed);
        atomicOp!"+="(listener.stats.dropped, 1);
    }

    // tcpStart is synchronous on the loop thread.
    void startTcp(UvRequest* request) nothrow @nogc
    {
        auto s = socketAt(request.file);
        if (s is null || s.closing || s.kind != SocketKind.tcpStream)
        {
            publish(request, UV_EBADF, 0);
            return;
        }
        if (s.reading || s.ring !is null || !s.connected)
        {
            publish(request, UV_EINVAL, 0);
            return;
        }
        immutable rc = startReading(s, request.tcpOptions);
        publish(request, rc < 0 ? rc : s.id, 0);
    }

    // A connect takes a slot for its uv_connect_t; the socket is in the
    // table from here, so a close can reach it, and reads nothing yet.
    int connectTcp(OpSlot* slot, UvRequest* request) nothrow @nogc
    {
        auto o = request.tcpOptions;
        auto s = allocateSocket(SocketKind.tcpStream);
        if (s is null) return UV_EMFILE;
        ushort port;
        int rc = initTcp(s);
        if (rc == 0 && o.bind.valid) rc = bindTcp(s, o.bind, o.ipv6Only, port);
        sockaddr_storage peer;
        if (rc == 0 && toSockaddr(o.connect, &peer) == 0) rc = UV_EINVAL;
        if (rc < 0)
        {
            discardSocket(s);
            return rc;
        }
        s.peer = o.connect;
        s.connectSlot = slot;
        s.inUse = true;
        slot.file = s.id;
        rc = armed(slot, uv_tcp_connect(cast(uv_connect_t*) slot.uv, s.tcp,
            cast(const(sockaddr)*) &peer, &onConnected));
        if (rc < 0)
        {
            s.connectSlot = null;
            s.inUse = false;
            discardSocket(s);
        }
        return rc;
    }

    void tcpConnected(OpSlot* slot, int status) nothrow @nogc
    {
        auto s = socketAt(slot.file);
        auto request = slot.request;
        slot.request = null;
        releaseSlot(slot);
        if (s !is null) s.connectSlot = null;
        if (s is null || s.closing)
        {
            // Closed while connecting: the close publishes on its own.
            if (request !is null) publish(request, status < 0 ? status : UV_ECANCELED, 0);
            return;
        }
        if (status < 0)
        {
            beginSocketClose(s); // no close request: the entry frees itself
            if (request !is null) publish(request, status, 0);
            return;
        }
        s.connected = true;
        ushort port;
        int rc = request is null ? UV_ECANCELED : startReading(s, request.tcpOptions);
        if (rc == 0) rc = localPort(s, port);
        if (rc < 0)
        {
            beginSocketClose(s);
            if (request !is null) publish(request, rc, 0);
            return;
        }
        publish(request, s.id, port);
    }

    // libuv's read callback on a stream: deliver a chunk, deliver the end or
    // an error as an event, or pause on an exhausted window.
    void streamRead(Socket* s, ptrdiff_t nread, const(uv_buf_t)* buf) nothrow @nogc
    {
        auto d = buf !is null && buf.base !is null
            ? cast(UvDatagram*)(buf.base - UvDatagram.sizeof) : null;
        if (nread == 0)
        {
            if (d !is null) s.window.giveBack(d);
            return;
        }
        if (nread == UV_ENOBUFS)
        {
            // The alloc callback found no buffer; pause until one returns.
            pauseStream(s);
            return;
        }
        if (nread < 0)
        {
            if (d !is null) s.window.giveBack(d);
            stopStream(s);
            if (nread != UV_EOF) atomicOp!"+="(s.stats.errors, 1);
            pushStreamEvent(s, nread == UV_EOF ? StreamEvent.endOfStream : StreamEvent.error, nread);
            return;
        }
        if (d is null) return;
        d.arrivalNanos = nanos();
        d.length = cast(uint) nread;
        d.flags = 0;
        d.peer = s.peer;
        d.socket = s.id;
        RingElement e;
        e.tag = s.tag;
        e.generation = ++s.sequence;
        e.pointer = cast(ulong) cast(size_t) d;
        e.payload[0] = d.length;
        e.payload[1] = cast(ulong) d.arrivalNanos;
        e.payload[2] = s.id;
        atomicFetchAdd!(MemoryOrder.acq_rel)(s.window.refs_, 1u);
        // A full ring holds the chunk in the pending queue, in order, rather
        // than dropping it; a closed ring has no consumer, and the chunk is
        // discarded and counted.
        if (deliver(s.ring, e, s.trigger))
            atomicOp!"+="(s.stats.received, 1);
        else
        {
            atomicFetchSub!(MemoryOrder.acq_rel)(s.window.refs_, 1u);
            s.window.giveBack(d);
            atomicOp!"+="(s.stats.dropped, 1);
        }
        if (s.window.exhausted()) pauseStream(s);
    }

    void pauseStream(Socket* s) nothrow @nogc
    {
        if (s.paused || !s.reading) return;
        uv_read_stop(s.stream);
        s.paused = true;
        ++pausedStreams_;
        atomicOp!"+="(s.stats.paused, 1);
    }

    void stopStream(Socket* s) nothrow @nogc
    {
        if (s.reading)
        {
            uv_read_stop(s.stream);
            s.reading = false;
        }
        if (s.paused)
        {
            s.paused = false;
            --pausedStreams_;
        }
        s.ended = true;
    }

    // Resume every paused stream whose window has a buffer again. Called
    // each loop iteration; the iteration's deadline is capped while any
    // stream is paused, since a release says nothing to the loop.
    bool resumePaused() nothrow @nogc
    {
        if (pausedStreams_ == 0) return false;
        bool progress;
        foreach (i; 0 .. maxSockets)
        {
            auto s = &sockets_[i];
            if (!s.inUse || !s.paused || s.closing) continue;
            if (s.window.exhausted()) continue;
            s.paused = false;
            --pausedStreams_;
            progress = true;
            immutable rc = uv_read_start(s.stream, &onStreamAlloc, &onStreamRead);
            if (rc < 0)
            {
                stopStream(s);
                atomicOp!"+="(s.stats.errors, 1);
                pushStreamEvent(s, StreamEvent.error, rc);
            }
        }
        return progress;
    }

    void pushStreamEvent(Socket* s, StreamEvent kind, long status) nothrow @nogc
    {
        RingElement e;
        e.tag = s.tag;
        e.generation = ++s.sequence;
        e.payload[0] = s.id | (cast(ulong) kind << 32);
        e.payload[1] = cast(ulong) status;
        if (deliver(s.ring, e, s.trigger)) atomicOp!"+="(s.stats.received, 1);
        else atomicOp!"+="(s.stats.dropped, 1);
    }

    // A write goes out at once when the socket takes all of it, and
    // publishes here; otherwise the rest waits in libuv with the slot.
    int writeTcp(OpSlot* slot, UvRequest* request) nothrow @nogc
    {
        auto s = socketAt(request.file);
        if (s is null || s.closing || s.kind != SocketKind.tcpStream || !s.connected) return UV_EBADF;
        auto buf = makeBuf(request.buffer, request.length);
        immutable n = request.length == 0 ? 0 : uv_try_write(s.stream, &buf, 1);
        if (n >= 0 && cast(size_t) n == request.length)
        {
            atomicOp!"+="(s.stats.sent, 1);
            slot.request = null;
            finishSlot(slot);
            publish(request, n, 0);
            return alreadyPublished;
        }
        if (n < 0 && n != UV_EAGAIN)
        {
            atomicOp!"+="(s.stats.sendErrors, 1);
            return n;
        }
        immutable written = n > 0 ? cast(size_t) n : 0;
        slot.file = request.file;
        auto rest = makeBuf(request.buffer + written, request.length - written);
        immutable rc = armed(slot, uv_write(cast(uv_write_t*) slot.uv, s.stream, &rest, 1,
            &onWriteDone));
        if (rc < 0) atomicOp!"+="(s.stats.sendErrors, 1);
        return rc;
    }

    void tcpWriteDone(OpSlot* slot, int status) nothrow @nogc
    {
        auto s = socketAt(slot.file);
        if (s !is null)
        {
            if (status < 0) atomicOp!"+="(s.stats.sendErrors, 1);
            else atomicOp!"+="(s.stats.sent, 1);
        }
        auto request = slot.request;
        slot.request = null;
        releaseSlot(slot);
        if (request !is null) publish(request, status < 0 ? status : cast(long) request.length, 0);
    }

    int shutdownTcp(OpSlot* slot, UvRequest* request) nothrow @nogc
    {
        auto s = socketAt(request.file);
        if (s is null || s.closing || s.kind != SocketKind.tcpStream || !s.connected) return UV_EBADF;
        slot.file = request.file;
        return armed(slot, uv_shutdown(cast(uv_shutdown_t*) slot.uv, s.stream, &onShutdownDone));
    }

    void tcpShutdownDone(OpSlot* slot, int status) nothrow @nogc
    {
        auto request = slot.request;
        slot.request = null;
        releaseSlot(slot);
        if (request !is null) publish(request, status, 0);
    }

    int startResolve(OpSlot* slot, UvRequest* request) nothrow @nogc
    {
        addrinfo hints;
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_DGRAM;
        return armed(slot, uv_getaddrinfo(loop_, cast(uv_getaddrinfo_t*) slot.uv, &onResolved,
            request.path, null, &hints));
    }

    void resolved(OpSlot* slot, int status, addrinfo* found) nothrow @nogc
    {
        auto request = slot.request;
        long result = status;
        if (status == 0 && request !is null)
        {
            auto into = (cast(UvAddress*) request.buffer)[0 .. request.length];
            size_t n;
            for (auto ai = found; ai !is null && n < into.length; ai = ai.ai_next)
            {
                auto a = fromSockaddr(ai.ai_addr);
                if (!a.valid) continue;
                a.port = request.address.port;
                into[n++] = a;
            }
            result = cast(long) n;
        }
        if (found !is null) uv_freeaddrinfo(found);
        slot.request = null;
        releaseSlot(slot);
        if (request !is null) publish(request, result, 0);
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
        if (slot.op == UvOp.tcpConnect)
        {
            // Closing the handle ends the connect with UV_ECANCELED.
            auto s = socketAt(slot.file);
            if (s !is null && !s.closing) beginSocketClose(s);
            return;
        }
        // Only work libuv has not started can be cancelled; started work
        // completes, and a readFile job stops at its next step, which it
        // learns from `stop`.
        if (slot.op == UvOp.readFile) atomicStore!(MemoryOrder.rel)(slot.stop, true);
        // 0: the request was still queued and its callback is now posted
        // with UV_ECANCELED. UV_EBUSY: a thread has it, or the ring does.
        slot.queueCancelled = uv_cancel(cast(uv_req_t*) slot.uv) == 0;
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
        if (status >= 0 && slot.allocated && request !is null)
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

    // True when the element is on the ring or queued to reach it; false
    // when it never will (a closed ring, or no room to queue it). A trigger
    // is advanced once the element is on the ring.
    bool deliver(SlotRing!RingElement* ring, ref RingElement e,
        FiberGenerationTrigger trigger = null) nothrow @nogc
    {
        if (pendingCount_ == 0)
        {
            immutable r = ring.tryPush(e);
            if (r == RingPush.pushed)
            {
                if (trigger !is null) trigger.advance();
                return true;
            }
            if (r == RingPush.closed)
            {
                bump(undelivered_);
                return false;
            }
        }
        // Full, or older deliveries wait: keep order, hold credit, retry.
        if (pendingCount_ == pendingCap_)
        {
            bump(undelivered_);
            return false;
        }
        pending_[(pendingHead_ + pendingCount_) % pendingCap_] = PendingDelivery(ring, e, trigger);
        ++pendingCount_;
        bump(deferred_);
        return true;
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
            else if (p.trigger !is null) p.trigger.advance();
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
        immutable sendSize = uv_req_size(uv_req_type.UV_UDP_SEND);
        immutable resolveSize = uv_req_size(uv_req_type.UV_GETADDRINFO);
        immutable connectSize = uv_req_size(uv_req_type.UV_CONNECT);
        immutable writeSize = uv_req_size(uv_req_type.UV_WRITE);
        immutable shutdownSize = uv_req_size(uv_req_type.UV_SHUTDOWN);
        if (fsSize > jobFsBytes)
            throw new UvLoopException("ant_uv: this libuv's uv_fs_t is larger than a readFile job reserves");
        slotStride_ = (max(fsSize, timerSize, workSize, sendSize, resolveSize, connectSize, writeSize,
            shutdownSize) + 63) & ~cast(size_t) 63;
        slotMemory_ = allocateAligned64(slotStride_ * slotCount_);
        slots_ = cast(OpSlot*) calloc(slotCount_, OpSlot.sizeof);
        pendingCap_ = slotCount_ + options_.ringCapacity;
        pending_ = cast(PendingDelivery*) calloc(pendingCap_, PendingDelivery.sizeof);
        sockets_ = cast(Socket*) calloc(maxSockets, Socket.sizeof);
        if (abandonedJobs_ is null) abandonedJobs_ = cast(shared(uint)*) calloc(1, uint.sizeof);
        if (slotMemory_ is null || slots_ is null || pending_ is null || sockets_ is null
            || abandonedJobs_ is null)
            throw new UvLoopException("ant_uv: slot allocation failed");
        foreach (i; 0 .. slotCount_)
        {
            auto slot = &slots_[i];
            *slot = OpSlot.init;
            slot.uv = cast(ubyte*) slotMemory_ + i * slotStride_;
            slot.owner = cast(void*) this;
            slot.uvLoop = loop_;
            slot.abandonedJobs = abandonedJobs_;
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
        free(sockets_);
        sockets_ = null;
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
        closeAllSockets(); // sockets the application left open; their windows outlive them
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

private extern (C) void onUdpAlloc(uv_handle_t* handle, size_t, uv_buf_t* buf) nothrow @nogc
{
    auto s = cast(Socket*) uv_handle_get_data(handle);
    auto d = s.window.take();
    // Window exhausted: the spare takes the datagram, which is then dropped.
    // An empty buffer would leave it unread on Linux and stall the socket on
    // Windows.
    if (d is null) d = s.window.spare_;
    buf.base = cast(char*) d + UvDatagram.sizeof;
    buf.len = cast(typeof(buf.len)) s.maxDatagram;
}

private extern (C) void onUdpRecv(uv_udp_t* handle, ptrdiff_t nread, const(uv_buf_t)* buf,
    const(sockaddr)* addr, uint flags) nothrow @nogc
{
    deliverDatagram(cast(Socket*) uv_handle_get_data(cast(uv_handle_t*) handle), nread, buf,
        addr, flags);
}

private extern (C) void onUdpSendDone(uv_udp_send_t* req, int status) nothrow @nogc
{
    auto slot = cast(OpSlot*) uv_req_get_data(cast(uv_req_t*) req);
    ownerOf(slot).udpSendDone(slot, status);
}

private extern (C) void onSocketClosed(uv_handle_t* handle) nothrow @nogc
{
    auto s = cast(Socket*) uv_handle_get_data(handle);
    s.owner.socketClosed(s);
}

// A handle whose setup failed after its init, or a connection refused for
// want of a table entry: nothing else refers to it.
private extern (C) void onHandleFreed(uv_handle_t* handle) nothrow @nogc
{
    free(handle);
}

private extern (C) void onConnection(uv_stream_t* server, int status) nothrow @nogc
{
    auto s = cast(Socket*) uv_handle_get_data(cast(uv_handle_t*) server);
    s.owner.connection(s, status);
}

private extern (C) void onConnected(uv_connect_t* req, int status) nothrow @nogc
{
    auto slot = cast(OpSlot*) uv_req_get_data(cast(uv_req_t*) req);
    ownerOf(slot).tcpConnected(slot, status);
}

// A stream's window has no spare: with every buffer held, the read callback
// sees UV_ENOBUFS and reading pauses until a release.
private extern (C) void onStreamAlloc(uv_handle_t* handle, size_t, uv_buf_t* buf) nothrow @nogc
{
    auto s = cast(Socket*) uv_handle_get_data(handle);
    auto d = s.window.take();
    if (d is null)
    {
        buf.base = null;
        buf.len = 0;
        return;
    }
    buf.base = cast(char*) d + UvDatagram.sizeof;
    buf.len = cast(typeof(buf.len)) s.maxDatagram;
}

private extern (C) void onStreamRead(uv_stream_t* stream, ptrdiff_t nread, const(uv_buf_t)* buf)
    nothrow @nogc
{
    auto s = cast(Socket*) uv_handle_get_data(cast(uv_handle_t*) stream);
    s.owner.streamRead(s, nread, buf);
}

private extern (C) void onWriteDone(uv_write_t* req, int status) nothrow @nogc
{
    auto slot = cast(OpSlot*) uv_req_get_data(cast(uv_req_t*) req);
    ownerOf(slot).tcpWriteDone(slot, status);
}

private extern (C) void onShutdownDone(uv_shutdown_t* req, int status) nothrow @nogc
{
    auto slot = cast(OpSlot*) uv_req_get_data(cast(uv_req_t*) req);
    ownerOf(slot).tcpShutdownDone(slot, status);
}

private extern (C) void onResolved(uv_getaddrinfo_t* req, int status, addrinfo* found) nothrow @nogc
{
    auto slot = cast(OpSlot*) uv_req_get_data(cast(uv_req_t*) req);
    ownerOf(slot).resolved(slot, status, found);
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
