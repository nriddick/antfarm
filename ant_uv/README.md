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
[fibers.d](source/ant_uv/fibers.d).

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

[read_files.d](examples/read_files.d) is the smallest complete program: worker Fibers on a `GroupHost` read files through
the loop. [load_assets.d](examples/load_assets.d) loads files from the main
thread, uses them on later frames and unloads them;
[Loading files](../docs/guide/loading-files.md) walks through it.

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
auto loop = UvLoop.create(options);
writeln(loop.summary());
```

`create` returns once the loop thread is pinned, the loop exists, and
libuv's auxiliary threads are created and placed. Keep `options.cpu` out of
the worker pool, for example by passing the remaining CPUs as
`GroupHostOptions.pool.processors`.

libuv runs file and name-resolution work on its own process-global pool,
which blocks in `read` and `write` and never runs D code. `create` starts
that pool from the loop thread. It then places each auxiliary thread from
inside the thread, so the threads do not inherit the loop thread's pin:

- `auxCpus` sets the CPUs.
- Leaving `auxCpus` empty gives them the creating thread's affinity.
- `auxPriority` lowers their priority.

`auxThreads` sizes the pool only if nothing else in the process used libuv's
pool first. `loop.auxThreads` reads back what each thread ended up with.

One `UvLoop` may be open per process at a time.

## Submit requests

A `UvRequest` belongs to its submitter until `submit` returns `accepted`. It
then belongs to the loop until the loop publishes it, and to the submitter
again after that. Its address, `path` and `buffer` must stay valid and
unmoved until publication.

```d
UvRequest request;
request.op = UvOp.readFile;          // open, read to the end, close
request.path = "/data/level1.pak";   // NUL-terminated
request.event = new FiberEvent(domain);
submitAndWait(loop, &request);       // in a managed Fiber
if (request.succeeded)
{
    auto file = request.takeBuffer();    // the block the loop allocated
    scope (exit) file.free();
    // ... file.bytes
}
```

| `op` | Inputs | Outputs |
| --- | --- | --- |
| `readFile` | `path`; `buffer` and `length`, or null to have the loop allocate from `allocator` at `alignment` | `result` = bytes read, `size` = file size |
| `open` | `path`, `flags` (`UV_FS_O_*`), `mode` | `result` = the file |
| `read`, `write` | `file`, `buffer`, `length`, `offset` (negative uses the file position) | `result` = bytes |
| `close` | `file` | `result` |
| `stat`, `fstat` | `path` or `file` | `size` |
| `timer` | `timeoutMs` | `result` = 0 |
| `nop` | nothing | completes in order with the requests around it |

A negative `result` is a libuv error code; `errorName` names it.

`submit` never blocks. Its other results leave the request with the caller:

- `full` means the request ring is full. Retry later, and never spin a
  thread the loop or the pool needs.
- `closed` means the loop is closing or has failed.
- `invalid` means a malformed request or one that is already queued.

`submitAndWait` yields a Fiber while the ring is full.

The loop issues at most `credits` operations to libuv at once; the rest
wait in the ring. A completion that waits for room in a completion ring also
holds a credit.

### Where `readFile` puts a file

Give `readFile` a `buffer` and `length` and it reads into them, stopping
when the buffer is full. A directory fails with `UV_EISDIR` and `size` 0 on
every platform, before anything is read or allocated. With a null `buffer`
the loop opens the file, then
allocates its size from `request.allocator`, an `ActorAllocator`, at
`request.alignment`:

- Leaving `allocator` unset gives `ActorAllocator.cRuntime()`, the C
  runtime with alignments up to 64. `alignment` is 64 unless you set
  another power of two.
- `allocate` runs on the loop thread, so it must not block.
- `deallocate` runs wherever the block is freed. That is any thread, or the
  loop thread when the read fails after allocating.
- An allocator that returns null fails the request with `UV_ENOMEM`.
- An empty file allocates nothing.
- `submit` returns `invalid` for an alignment that is not a power of two,
  or for an allocator with only one of its halves set.

After a successful read the request owns the block, and `buffer[0 .. length]`
is the file. `submit` refuses the request until you free the block with
`freeBuffer` or hand it over with `takeBuffer`. `takeBuffer` returns a
`UvBuffer`, which keeps the allocator that made the block. Its `free`
returns the block to that allocator from any thread. If the read fails,
the loop frees the block itself.

Any `ActorAllocator` works, including the one the actor runtimes use. For
mimalloc v3, set `request.allocator = mimallocV3ActorAllocator()` from
`antfarm.actors.mimalloc`. It is compiled only with `AntfarmMimallocV3`
and needs mimalloc linked. With Dub, depend on `antfarm` as well and add
`subConfiguration "antfarm" "mimalloc-v3"`, as
[load_assets.d](examples/load_assets.d) does in its `mimalloc`
configuration. The adapter itself is described in
[the allocation guide](../docs/guide/sizing-and-allocation.md#the-mimalloc-v3-adapter).

## Completion

The loop publishes a request in three steps. It first copies the delivery
targets out of the record. It then writes `result` and `size` and stores the
state with release ordering, which is its last access to the record. Finally
it delivers through the copies, in this order:

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
that belongs beside the loop, such as a resource manager:

```d
UvFibers fibers;
UvLoopOptions options;
options.start = () {
    fibers = new UvFibers(UvLoop.current);
    fibers.spawn(&resourceManager);
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
  completion; a `readFile` stops at its next step.

Every cancelled request is still published, and only after libuv's final
callback, so cancellation is never a reason to free a buffer early.

## Close

`loop.close()` refuses new requests and keeps serving for `closeGrace`. It
then cancels what is outstanding and waits up to `abandonAfter` for libuv's
final callbacks. Next it runs the `stop` hook, closes every handle and the
loop, and joins the thread.

An operation libuv cannot cancel is published `abandoned` if it outlasts the
wait, for example an `open` of a FIFO with no writer. Its `buffer` must then
be leaked, as the loop leaks a block it allocated for it. The loop's memory
is left allocated (`loopClose` is `UV_EBUSY`). The returned `UvCloseReport` counts completed, cancelled,
abandoned and undelivered requests.

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

```sh
make -C tests/ant_uv run run-dmd run-tsan
```

The suite checks the binding's layout and constants against the real
`uv.h`, through a small C translation unit. It then covers lifecycle and
placement, ordering, timers, credits, a full ring, cancellation, close,
failure, Fibers on both sides, `readFile`'s allocators, and abandonment.
`make check` runs it when `pkg-config` finds libuv 1.19 or later. On
Windows, the `ant_uv` lane of `python -B scripts/check-windows.py` builds
and runs it under LDC and DMD against the pinned libuv, then both examples
([Testing](../docs/development/testing.md#windows-x64-integration)).
`make -C tests/ant_uv run-mimalloc`, which `make check-mimalloc` runs, adds
reads into the real mimalloc v3.
