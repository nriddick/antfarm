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

It links libuv 1.19 or later (`libs "uv"`), so install the library and its
header first. On Linux that is the system libuv. On Windows,
`python -B scripts/build-libuv-windows.py` builds the pinned libuv 1.53.0
into `.build/libuv/1.53.0/`; put its `lib` on the linker's `LIB` path and
its `bin`, which holds `uv.dll`, on `PATH` when the program runs.

[read_files.d](examples/read_files.d) is the smallest complete program:
worker Fibers on a `GroupHost` read files through the loop.
[load_assets.d](examples/load_assets.d) is a resource manager that runs at
the start of every tick; [Loading files](../docs/guide/loading-files.md)
walks through it.

```sh
dub run --single ant_uv/examples/read_files.d -- /etc/hostname /etc/os-release
dub run --single ant_uv/examples/load_assets.d
```

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
environment already sets it, and an explicit `0` there keeps the pool. The
kernel runs the operations on a polling thread of its own, which spins while
the ring is busy and which `auxCpus` does not place. That suits many reads
from files already open, such as a pack file: random 64 KiB reads ran twice
as fast as through the pool. `readFile` stays a pool job, because the one
polling thread would copy every whole file in turn; through the ring, 8 MiB
files loaded at a fifth to a half of the job's rate
([measurements](../docs/performance/file-loading-sdl3-2026-11-02.md)).
`loop.ioUring` reports whether the option took effect; where the kernel
refuses the ring, libuv falls back to the pool.

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
- `deallocate` runs wherever the block is freed: any thread, or the loop
  thread when the read fails after allocating.
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

For files of a megabyte or more, prefer mimalloc v3. The system allocators
return a large block to the OS when it is freed and fault its pages in again
on the next allocation. On the test laptop, freeing an 8 MiB file cost the
freeing thread 100 to 350 µs on Windows and Linux, against 1 to 2 µs with
mimalloc, and 8 MiB loads ran at 8.0 GiB/s against 12.8 on Windows, 11.5
against 17.1 on Linux
([file loading against SDL3](../docs/performance/file-loading-sdl3-2026-11-02.md)).
`mimallocV3ActorAllocator()` is in `antfarm.actors.mimalloc`, compiled only
with `AntfarmMimallocV3` and needing mimalloc linked. With Dub, depend on
`antfarm` as well and add `subConfiguration "antfarm" "mimalloc-v3"`, as
[load_assets.d](examples/load_assets.d) does in its `mimalloc`
configuration. The adapter itself is described in
[the allocation guide](../docs/guide/sizing-and-allocation.md#the-mimalloc-v3-adapter).

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
io_uring that open did not wait for a writer on Linux 6.6.) Its `buffer`
must then be leaked, as the loop leaks a block it allocated for it. The
loop's memory is left allocated (`loopClose` is `UV_EBUSY`). The returned
`UvCloseReport` counts completed, cancelled, abandoned and undelivered
requests.

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
