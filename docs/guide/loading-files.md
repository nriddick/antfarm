# Load files through the loop thread

`ant_uv` runs one libuv event loop on its own thread beside the worker pool.
This page walks through a resource manager that runs at the start of every
tick, then says where the loaded bytes live and how a worker Fiber waits
for its own load. How to use the loop is
[ant_uv/README.md](../../ant_uv/README.md); the contracts are the comments
in [loop.d](../../ant_uv/source/ant_uv/loop.d); the design and its reasons
are in [design/event-loop.md](../design/event-loop.md).

## A resource manager at tick start

[load_assets.d](../../ant_uv/examples/load_assets.d) is a table of
file-backed assets owned by the main thread, which drives the ticks. At the
start of each tick the table's `step` settles the loads that finished and
applies the releases the previous tick asked for. Then the tick's work runs
on the fork-join workers, every leaf reading the assets it needs, and the
main thread waits for it. During the tick nothing in the table changes: an
`unload` only queues a release, and a load that finishes sits in the
completion ring. So the work never reads a freed block, and never sees a
block the loop is still writing. That is the whole rule: **only `step`
frees, and `step` runs between ticks.**

```sh
dub run --single ant_uv/examples/load_assets.d
```

```text
loading 4 files into the C runtime
settled after 2 tick(s)
load_assets.d: 9101 bytes, first line /+ dub.sdl:
/etc/os-release: 400 bytes, byte sum 32832
/no/such/file: failed (ENOENT)
load_assets: unloaded
load_assets.d after unload: releasing, 9101 bytes still readable
load_assets.d after the next step: unloaded
UvCloseReport(4, 1, 0, 0, 0, null)
```

Your sizes and sums will differ. The report counts four requests
published, one of them cancelled, none abandoned or undelivered, and
libuv's loop closed cleanly.

### The table

Each asset holds its own request, so the request keeps a stable address
while the loop owns it. Never resize the array while anything is loading.
`releasing` is `ready` with a release queued.

```d
// fragment: from load_assets.d
enum AssetState { unloaded, loading, ready, releasing, failed, unloading }

struct Asset
{
    const(char)* path;  // NUL-terminated; the table keeps it alive
    AssetState state;
    UvRequest request;  // the loop's while loading or unloading
    UvBuffer file;      // the file while ready or releasing
}
```

The table also owns a `SlotRing!RingElement` of 64 slots. The loop pushes
one element into it for each finished request, and only the main thread pops.

### Load

`load` makes a whole-file read and submits it. `user` comes back as the
completion element's `tag`, which is how the table finds the asset again.

```d
// fragment: AssetTable.load, for asset `id`
a.request = UvRequest.readFile(a.path); // open, read to the end, close
a.request.user = id;                    // comes back as the element's tag
a.request.completions = completions;    // deliver to this table
final switch (loop.submit(&a.request))
{
case UvSubmit.accepted:
    a.state = AssetState.loading;
    return true;
case UvSubmit.full:
    return false;                       // the request ring is full: try next tick
case UvSubmit.closed, UvSubmit.invalid:
    a.state = AssetState.failed;
    return true;
}
```

### The step

`step` pops what the loop delivered, then frees what was queued. Popping an
element is the hand-back point: the loop no longer touches that request,
and its results are visible to the main thread.

```d
// fragment: AssetTable.step and settle
void step()
{
    RingElement e;
    while (completions.tryPop(e))
        settle(&assets[cast(size_t) e.tag]);
    foreach (ref a; assets)
        if (a.state == AssetState.releasing)
        {
            a.file.free(); // back to the allocator that made it
            a.state = AssetState.unloaded;
        }
}

private void settle(Asset* a)
{
    if (a.state == AssetState.unloading)
    {
        a.request.freeBuffer(); // only if the read won the race
        a.state = AssetState.unloaded;
    }
    else if (a.request.succeeded)
    {
        a.file = a.request.takeBuffer();
        a.state = AssetState.ready;
    }
    else
        a.state = AssetState.failed;
}
```

### The tick

The step, then the work. `forkJoinEach` runs on the calling thread as a
participant and returns when every leaf has run, so the next `step` cannot
start while a leaf still reads. The leaves here sum each asset's bytes;
`table.bytes(id)` is null for an asset that is not loaded.

```d
// fragment: main
void tick()
{
    table.step();
    forkJoinEach(iota(paths.length), 1, (size_t id) nothrow @nogc @system {
        ulong sum;
        foreach (b; table.bytes(id)) sum += b;
        sums[id] = sum;
    });
}
```

The same shape holds for any barrier the main thread already has: a wave
it joins, a Farm drain it counts, or a Fiber domain it waits to drain. What
matters is that the work has returned before the next `step`.

### Unload

A ready asset is not freed on the spot: its state becomes `releasing`, its
bytes stay readable for the rest of the tick, and the next `step` frees
them. A loading asset cannot be freed at all yet, because the loop owns its
request and may be about to write into the block. So `unload` asks for a
cancel and marks the asset `unloading`; the step that collects it frees
whatever the read produced.

```d
// fragment: AssetTable.unload, for asset `id`
final switch (a.state)
{
case AssetState.ready:
    a.state = AssetState.releasing;    // freed by the next step
    break;
case AssetState.loading:
    loop.cancel(&a.request);           // a hint: if it misses, step frees the result
    a.state = AssetState.unloading;
    break;
case AssetState.unloaded, AssetState.failed, AssetState.releasing, AssetState.unloading:
    break;
}
```

The cancel can lose the race. In the run above it won, and the report
counts one cancelled request. When the unload comes after the read has
finished, `settle` frees the loaded block instead, and the report counts
none.

An open file works the same way: keep the descriptor in the table with a
count of its reads in flight, and close it at a `step` where the count is
zero.

### Shut down

`loop.close()` publishes every request before it returns. A completion the
ring could not take is still published. So `shutdown`, which runs after
`close`, steps once more, settles any finished request it did not see,
queues every asset for release, and steps again. A read that `close`
abandoned frees its own block later, on libuv's thread, so a table whose
allocator does not live for the process would wait for
`loop.abandonedJobs` to reach zero before tearing the allocator down; this
example's allocators do live for the process.

```d
// fragment: main calls loop.close(), then this
void shutdown()
{
    step();
    foreach (ref a; assets)
        if ((a.state == AssetState.loading || a.state == AssetState.unloading)
            && a.request.finished)
            settle(&a);
    foreach (id; 0 .. assets.length)
        unload(id);
    step();
    completions.destroy();
}
```

## Where the bytes live

By default a file is allocated from the C runtime, 64-byte aligned. To
choose otherwise, set `UvLoopOptions.allocator` once for the loop, or pass
an allocator to `UvRequest.readFile` for one request; both take any
`ActorAllocator`, the type the actor runtimes also take. The alignment can
be any power of two the allocator supports.

| Choice | Set | Note |
| --- | --- | --- |
| The C runtime | nothing | `ActorAllocator.cRuntime()`: alignments up to 64 |
| mimalloc v3 | `options.allocator = mimallocV3ActorAllocator()` | compile with `AntfarmMimallocV3` and link mimalloc |
| Your own | `ActorAllocator(context, &allocate, &deallocate)` | an arena, a budgeted pool, a switch between arenas and mimalloc, or a wrapper that counts |

For files of a megabyte or more, mimalloc is the better choice. On Windows
and under WSL2 the system allocators returned a large block to the OS on
`free` and faulted its pages in again on the next allocation. An 8 MiB free
then cost a tenth to a third of a millisecond on the freeing thread, which
is the main thread in this example, against a microsecond or two with
mimalloc. glibc on native Linux varied from round to round, from a tenth of
a microsecond to 71 µs
([measurements](../performance/file-loading-sdl3-2026-11-02.md)).

An allocator for `readFile` must work like this:

- `allocate` runs on libuv's auxiliary thread, which druntime does not
  know. It must be thread-safe, may block, and must not touch the GC or
  druntime.
- `deallocate` runs on whichever thread frees the block. That is the owner
  of the `UvBuffer`; the loop thread when a read fails after allocating; or
  libuv's auxiliary thread when `close` abandoned the read, possibly after
  `close` returned. After a close that reports abandonment, keep the
  allocator alive until `loop.abandonedJobs` is zero
  ([Close](../../ant_uv/README.md#close)).
- Returning null from `allocate` fails the request with `UV_ENOMEM`.

A `UvBuffer` carries its block, the block's size and alignment, and the
allocator that made it. So `free` needs nothing else, and it works from any
thread. `capacity` is the size allocated, which a resource manager can count
against a memory budget.

The example loads into mimalloc v3 when built with its `mimalloc`
configuration:

```sh
dub run --single ant_uv/examples/load_assets.d --config=mimalloc
```

Its first line then reads `loading 4 files into mimalloc v3`. The
configuration depends on `antfarm` and selects its `mimalloc-v3`
configuration. That builds the pinned library from the `third_party/mimalloc`
submodule, so it needs the submodule, CMake and a C compiler. The adapter is
described in [the allocation guide](sizing-and-allocation.md#the-mimalloc-v3-adapter).
The full allocation rules are in
[Where readFile puts a file](../../ant_uv/README.md#where-readfile-puts-a-file).

## From a worker Fiber

A Fiber on a worker can wait for its own load. Set `event` to a `FiberEvent`
of its domain and call `submitAndWait`, which parks the Fiber until the loop
publishes the request:

```d
// fragment: inside a managed Fiber; loop, domain and path are in scope
UvRequest request;
request.op = UvOp.readFile;
request.path = path;                 // NUL-terminated
request.event = new FiberEvent(domain);
if (submitAndWait(loop, &request) == UvSubmit.accepted && request.succeeded)
{
    auto file = request.takeBuffer();
    scope (exit) file.free();
    // use file.bytes
}
```

[read_files.d](../../ant_uv/examples/read_files.d) runs this on a
`GroupHost`. If the Fiber is cancelled while it waits, `submitAndWait`
cancels the request and outwaits it before rethrowing. The loop therefore
never writes into a stack that has unwound. See
[Fibers](../../ant_uv/README.md#fibers).
