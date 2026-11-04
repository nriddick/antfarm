# ant_uv: one libuv loop beside the worker pool

`ant_uv` runs one [libuv](https://libuv.org) event loop on a dedicated
thread beside the Ant Farm worker pool. Any thread hands it a caller-owned
`UvRequest`: a file operation, a whole-file read, or a timer. The loop
thread is the only thread that calls libuv. It publishes every accepted
request exactly once, then wakes whoever waits through a Fiber event, a
counted trigger, a callback or a completion ring. The pool is unchanged;
the loop is a service thread on a CPU the application keeps out of the
worker set.

The design, including what the library owns and what the application owns,
is [docs/design/event-loop.md](../docs/design/event-loop.md). The contracts
are the comments in [loop.d](source/ant_uv/loop.d) and
[fibers.d](source/ant_uv/fibers.d); this page is how to use them.

Status: new in 2.2.0, on Linux x86-64 and Windows x64.

## Use it

```sdl
dependency "antfarm:ant_uv" path="path/to/antfarm"
```

It links the pinned libuv that `python3 scripts/build-libuv.py` builds
into `.build/libuv/pinned/` from one checksummed source archive: at
present libuv's `v1.x` branch at `716390f` (1.53.1-dev) plus the SQPOLL
wakeup fence of [libuv pull request #5409](https://github.com/libuv/libuv/pull/5409),
from [the project's fork](https://github.com/nriddick/libuv/tree/linux-sqpoll-wakeup-fence),
until a release carries it. The script needs CMake and a C compiler, and
downloads the archive once. On Linux the result is a
static `libuv.a`, which Dub builds when it is missing and links into the
program. On Windows it is `uv.dll` with its import library `uv.lib`: Dub
links the import library from that installation (not through `LIB`, which
DMD replaces with its own), and the program needs the installation's `bin`
on `PATH` when it runs; a direct compiler command names `uv.lib` by path,
as the Windows runner does. The system libuv is not used.

[read_files.d](examples/read_files.d) is the smallest complete program:
worker Fibers on a `GroupHost` read files through the loop.
[load_assets.d](examples/load_assets.d) is a resource manager that runs at
the start of every tick; [Loading files](../docs/guide/loading-files.md)
walks through it.

```sh
dub run --single ant_uv/examples/read_files.d -- /etc/hostname /etc/os-release
dub run --single ant_uv/examples/load_assets.d
```

On Windows, name two files of your own in place of the `/etc` paths.

## Create the loop

```d
UvLoopOptions options;
options.cpu = ProcessorId(0, 1);              // the loop thread's CPU
options.auxThreads = 2;                       // libuv's pool (UV_THREADPOOL_SIZE)
options.auxCpus = [ProcessorId(0, 17)];       // where libuv's threads run
options.auxPriority = UvAuxPriority.belowNormal;
options.allocator = mimallocV3ActorAllocator(); // where readFile puts files
auto loop = UvLoop.create(options);
writeln(loop.summary());
```

`create` returns once the loop thread is pinned, the loop exists, and
libuv's auxiliary threads are created and placed. Keep `options.cpu` out of
the worker pool, for example by passing the remaining CPUs as
`GroupHostOptions.pool.processors`.

libuv runs file and name-resolution work on its own process-global pool,
which blocks in `read` and `write`. The only D code it runs is `ant_uv`'s
whole-file job, which calls libuv and the OS, and the allocator that job
takes. `create` starts that pool from the loop thread. It then places each
auxiliary thread from inside the thread, so the threads do not inherit the
loop thread's pin:

- `auxCpus` sets the CPUs.
- Leaving `auxCpus` empty gives them the creating thread's affinity.
- `auxPriority` lowers their priority.

`auxThreads` sizes the pool only if nothing else in the process used libuv's
pool first. `loop.auxThreads` reads back what each thread ended up with.

On Linux, `options.ioUring = true` has libuv issue `open`, `close`, `read`,
`write`, `stat` and `fstat` through io_uring instead of the pool (libuv 1.49
and Linux 5.10.186 or later). `create` sets `UV_USE_IO_URING=1` unless the
environment already sets it, and an explicit `0` there keeps the pool.
`create` builds the ring, so the first request does not pay for it. The
kernel runs the operations on a polling thread of its own, which spins while
the ring is busy and which `auxCpus` does not place. Whether that pays
depends on the system, so measure on the target. Random 64 KiB reads from
an open pack file ran twice as fast as through the pool under WSL2, where
system calls cost more. On native Linux on a Ryzen 9 5900XT they ran at
three quarters of the pool's rate: the pool's four threads copy side by
side, and the ring's one thread copies in turn. `readFile` stays a pool job
for the same reason; through the ring, 8 MiB files loaded at a fifth to a
half of the job's rate
([measurements](../docs/performance/file-loading-sdl3-2026-11-02.md)).
`loop.ioUring` reports whether the option took effect; where the kernel
refuses the ring, libuv falls back to the pool. libuv up to 1.53.0 as
released can lose a ring request to a missing fence in its submit path,
about once in a hundred runs of the test suite on a Ryzen 9 5900XT; the
pinned libuv carries the fence of libuv pull request #5409
([record](../docs/development/libuv-sqpoll-wakeup-2026-11-04/README.md)),
and a program linking a released libuv does not.

One `UvLoop` may be open per process at a time.

## Submit requests

A `UvRequest` belongs to its submitter until `submit` returns `accepted`. It
then belongs to the loop until the loop publishes it, and to the submitter
again after that. Its address, `path` and `buffer` must stay valid and
unmoved until publication.

```d
auto request = UvRequest.readFile("/data/level1.pak"); // NUL-terminated path
request.event = new FiberEvent(domain);
submitAndWait(loop, &request);       // in a managed Fiber
if (request.succeeded)
{
    auto file = request.takeBuffer();    // the block the loop allocated
    scope (exit) file.free();
    // ... file.bytes
}
```

Each constructor returns a fresh record with only its operation's inputs
set; set the delivery target and `user` on it.

| Request | Outputs |
| --- | --- |
| `readFile(path)`, `readFile(path, allocator, alignment)`, `readFile(path, buffer)` | `result` = bytes read, `size` = the file's size; with no buffer, a block to take or free |
| `open(path, flags, mode)` | `result` = the file |
| `read(file, buffer, offset)`, `write(file, bytes, offset)` | `result` = bytes |
| `close(file)` | `result` |
| `stat(path)`, `fstat(file)` | `size` |
| `timer(milliseconds)` | `result` = 0 |
| `nop()` | published in order with the requests around it |

A negative `result` is a libuv error code; `errorName` names it. The fields
behind the constructors are public (`op`, `path`, `file`, `buffer`,
`length`, `offset`, `flags`, `mode`, `timeoutMs`), documented in
[loop.d](source/ant_uv/loop.d).

`submit` never blocks. Its other results leave the request with the caller:

- `full` means the request ring is full. Retry later, and never spin a
  thread the loop or the pool needs.
- `closed` means the loop is closing or has failed.
- `invalid` means a malformed request, one that is already queued, or one
  still owning a `readFile` block.

`submitAndWait` yields a Fiber while the ring is full.

The loop issues at most `credits` operations to libuv at once; the rest
wait in the ring. A completion that waits for room in a completion ring also
holds a credit.

### Where `readFile` puts a file

`readFile(path, buffer)` reads into the buffer, stopping when it is full.
The other two allocate the file's size, from the request's allocator or
else the loop's `options.allocator`, at the request's alignment. The whole
read is one job on libuv's pool: open, `fstat`, allocate, read and close on
one auxiliary thread, then one completion on the loop thread.

- Both unset is `ActorAllocator.cRuntime()`, the C runtime with alignments
  up to 64. The alignment is 64 unless you pass another power of two.
- `allocate` runs on libuv's thread, which druntime does not know: it must
  be thread-safe, may block, and must not touch the GC or druntime.
  `ActorAllocator.cRuntime()` and `mimallocV3ActorAllocator()` qualify;
  `ActorAllocator.gcRooted` does not.
- `deallocate` runs wherever the block is freed: any thread; the loop
  thread when the read fails after allocating; or libuv's thread when
  `close` abandoned the job, which can be after `close` returned (see
  [Close](#close)).
- An allocator that returns null fails the request with `UV_ENOMEM`. An
  empty file allocates nothing. A directory fails with `UV_EISDIR` and
  `size` 0 on every platform, before anything is read or allocated.
- `submit` returns `invalid` for an alignment that is not a power of two,
  or for an allocator with only one of its halves set.

After a successful read the request owns the block. Hand it over with
`takeBuffer`, which returns a `UvBuffer` carrying the allocator that made
it, or free it with `freeBuffer`; `submit` refuses the request until you do
one or the other. `UvBuffer.free` returns the block to its allocator from
any thread. If the read fails, the loop frees the block itself.

For files of a megabyte or more, prefer mimalloc v3, whose cost is low and
steady. On the test laptop the system allocators returned a large block to
the OS when it was freed and faulted its pages in again on the next
allocation. Freeing an 8 MiB file cost the freeing thread 100 to 350 µs on
Windows and under WSL2, against 1 to 2 µs with mimalloc. 8 MiB loads ran at
8.0 GiB/s against 12.8 on Windows, and 11.5 against 17.1 under WSL2. On
native Linux with glibc 2.44 the same free cost from 0.1 to 71 µs, varying
from round to round, against about 1 µs with mimalloc, which loaded 8 MiB
files 6 to 13% faster
([file loading against SDL3](../docs/performance/file-loading-sdl3-2026-11-02.md)).
`mimallocV3ActorAllocator()` is in `antfarm.actors.mimalloc`, compiled only
with `AntfarmMimallocV3` and needing mimalloc linked. With Dub, depend on
`antfarm` as well and add `subConfiguration "antfarm" "mimalloc-v3"`, as
[load_assets.d](examples/load_assets.d) does in its `mimalloc`
configuration. The adapter itself is described in
[the allocation guide](../docs/guide/sizing-and-allocation.md#the-mimalloc-v3-adapter).

## UDP sockets

A UDP socket is an object the loop owns, opened and closed by requests.
Sends are requests; received datagrams are not: they arrive on a ring the
application names at open, one element each, in buffers from a window the
socket owns, and the application returns the buffers from any thread.
The design and the netcode it was shaped by are
[section 9 of the event loop design](../docs/design/event-loop.md#9-221-udp-sockets).

```d
UvUdpOptions options;
options.bind = UvAddress.loopback4(0);        // port 0: any; the open publishes it as size
options.completions = ring;                   // a SlotRing!RingElement the application owns
options.tag = 1;                              // names this socket on the ring
options.window = 64;                          // receive buffers, fixed for the socket's life
options.maxDatagram = 1200;                   // data bytes per buffer
auto open = UvRequest.udpOpen(&options);      // options stay yours until publication
...                                           // open.result is the socket, open.size the port

auto send = UvRequest.udpSend(socket, bytes, peer);  // peer omitted on a connected socket
send.stampAt = 0;                             // the loop writes its transmit time at bytes[0 .. 8]

RingElement e;
while (ring.tryPop(e))                        // e.tag names the socket
{
    auto d = cast(UvDatagram*) cast(size_t) e.pointer;
    // d.bytes, d.peer, d.arrivalNanos, d.flags
    d.release();                              // back to the window, from any thread
}
```

`UvAddress` is the one address type: `ipv4`/`ipv6` parse text, `loopback4`,
`any4`, `loopback6` and `any6` build the usual ones, and `format` writes
`1.2.3.4:80` or `[::1]:80`. `UvRequest.resolve(host, port, slice)` fills a
caller-owned slice from `getaddrinfo` on libuv's pool; it answers with
addresses, not trust.

The window is the whole flow-control story. A datagram that arrives while
every buffer is held is read and discarded, and the socket counts it in
`loop.udpStats(socket).dropped`; nothing queues outside the window, so a
stalled consumer loses the newest packets rather than piling up stale
ones. Size the window for the backlog between your polls, not for your own
histories, which are your copies. The ring's capacity must cover the windows
of every socket that shares it, which `udpOpen` checks (`UV_ENOBUFS`). A
datagram larger than `maxDatagram` is cut and flagged `truncated`. A
datagram you still hold stays valid across its socket's close; the window
is freed by the last `release`.

Every transport timestamp is `UvLoop.nanos()`, the loop's monotonic clock
in nanoseconds: `arrivalNanos` is when libuv delivered the datagram, and
`stampAt` is written just before the send's system call. Those are the
inputs to clock synchronization; the game's own time is yours to put in the
packet.

Options set at open and fixed for the socket's life: `connect` (a peer, so
sends omit the address and the network's errors about it reach the socket),
`reuseAddress`, `broadcast`, `ipv6Only`, `ttl`, the kernel buffer sizes,
and a multicast group. Sessions, authentication, replay protection and
packet formats are the application's; the transport carries no
cryptography.

## Completion

After publication the loop delivers through the targets the request named,
in this order:

- `trigger`: `FiberGenerationTrigger.advance()`, atomics only. One trigger
  can serve a sequence of requests (`waitNext` once per request).
- `event`: `FiberEvent.set()`, which takes the waiter queue's mutex.
- `callback(request, context)`: on the loop thread, `nothrow @nogc`.
- `completions`: one element on a `SlotRing!RingElement`. `tag` is
  `user`, `pointer` is the request, and `payload` holds `result`, `size`,
  `op` and the state. A full ring is retried, never dropped while it can
  still accept.

Exactly one party reclaims a published request: the one that observed
`done`, the callback, or the completion ring's consumer. A request may not
name both a callback and a completion ring. Whoever owns a request keeps its
trigger or event alive.

## Fibers

**On worker threads.** Use `submitAndWait` with an event of the Fiber's
domain, or `submit` followed by `trigger.waitNext()`. If the Fiber is
cancelled while it waits, `submitAndWait` cancels the request, holds the
thread in short sleeps until the loop publishes it, then rethrows
`FiberCancelled`. A Fiber's cancellation is sticky, so it cannot park again,
and the loop must never write to a request on a dead stack.

**On the loop thread.** `UvFibers` gives the loop thread a private Farm and
a Fiber domain with the loop thread as Director. That is the place for logic
that belongs beside the loop:

```d
UvFibers fibers;
UvLoopOptions options;
options.start = () {
    fibers = new UvFibers(UvLoop.current);
    fibers.spawn(&loopSideLogic);
};
options.step = () => fibers.step();
options.stop = () { fibers.shutdown(); };
```

Loop-thread Fibers wait on events the loop sets on the same thread, so their
I/O work resumes where it was issued. Cancelled while waiting, they keep the
loop running until their request publishes.

## Cancel

`loop.cancel(&request)` cancels the latest submission:

- A request still in the ring is published with `UV_ECANCELED` without
  reaching libuv.
- A timer stops at once.
- File work libuv has not started is cancelled. Work it has started runs to
  completion; a `readFile` job stops at its next step, between its open,
  its `fstat` and each read. Through io_uring, libuv also asks the kernel to
  cancel work already submitted.

`cancel` returns false only when its control ring is full or closed. The
request is flagged either way, so a request not yet started is still
published cancelled; retry for one already inside libuv. Every cancelled
request is still published, and only after libuv's final callback, so
cancellation is never a reason to free a buffer early.

## Close

`loop.close()` refuses new requests and keeps serving for `closeGrace`. It
then cancels what is outstanding and waits up to `abandonAfter` for libuv's
final callbacks. Next it runs the `stop` hook, closes every handle and the
loop, and joins the thread.

An operation libuv cannot cancel is published `abandoned` if it outlasts the
wait, for example an `open` of a FIFO with no writer on the pool. (Through
io_uring that open did not wait for a writer on Linux 6.6.) The loop's
memory is left allocated (`loopClose` is `UV_EBUSY`). The returned
`UvCloseReport` counts completed, cancelled, abandoned and undelivered
requests.

An abandoned `readFile` job runs on until its file operation returns. Then
it frees the block it allocated, on libuv's thread, and leaves
`loop.abandonedJobs`; a job that ended during the wait is collected as a
completion instead. At most one job per auxiliary thread can be abandoned,
since queued jobs are cancelled. Until `abandonedJobs` is zero a job may
still call its allocator and write into a buffer you gave it, so keep both
valid: after a close that reports abandonment, do not reset an arena or
destroy a mimalloc heap until the count is zero, and do not let `allocate`
wait on the thread that calls `close`, which the loop cannot interrupt; a
bounded wait that returns null fails the read with `UV_ENOMEM`. A job
stuck in a file operation that never returns, such as a read from a dead
network mount, holds one of libuv's auxiliary threads for the rest of the
process. Any other abandoned operation gives no signal when it ends: its
buffer must be leaked.

Shut down in this order:

1. Finish or cancel the worker Fibers that use the loop, and retire them.
2. `loop.close()`.
3. `host.close()`.
4. `domain.beginShutdown()`.

The loop thread is not a daemon, so a loop left open keeps the process
alive at exit.

## Failures

If the `start` or `step` hook throws, the loop stops calling the step,
cancels everything and closes its libuv loop. `close()` then throws a
`UvLoopException` carrying the report and the failure. No D exception ever
unwinds through libuv: every callback `ant_uv` registers is
`nothrow @nogc`.

## Test

`tests/ant_uv` is the suite; what it covers and how `make check` and the
Windows matrix run it are in
[Testing](../docs/development/testing.md).
