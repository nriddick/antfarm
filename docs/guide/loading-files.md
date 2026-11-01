# Load files through the loop thread

`ant_uv` runs one libuv event loop on its own thread beside the worker pool.
This page gives its API at a glance. It then walks through a table of assets
that the main thread loads, uses on later frames and unloads, and ends with
where the loaded bytes live. The contracts belong to
[ant_uv/README.md](../../ant_uv/README.md) and the comments in
[loop.d](../../ant_uv/source/ant_uv/loop.d). The design and its reasons are in
[design/event-loop.md](../design/event-loop.md).

## The API at a glance

`ant_uv` has three parts: the loop, a request record you own, and four ways
to be told that a request finished.

**The loop.** There is one per process, on its own thread. Only that thread
calls libuv.

| Member | What it does |
| --- | --- |
| `UvLoop.create(options)` | Starts the loop thread, pinned when `cpu` is set, then starts and places libuv's auxiliary threads. Returns once the loop serves |
| `submit(&request)` | Hands a request to the loop from any thread without blocking. Returns `accepted`, `full`, `closed` or `invalid` |
| `cancel(&request)` | Asks the loop to cancel the request's latest submission. Returns false only when its control ring is full or closed |
| `wake()` | Wakes the loop thread out of libuv, for its `step` hook |
| `close()` | Serves for a grace period, cancels what is left, closes libuv and joins the thread. Returns a `UvCloseReport` |
| `UvLoop.current` | The loop, on the loop thread. Null elsewhere |

These are the options `create` reads most often:

| Option | Default | Sets |
| --- | --- | --- |
| `cpu` | unpinned | the loop thread's CPU, which the worker set should exclude |
| `auxThreads`, `auxCpus`, `auxPriority` | libuv's 4 threads, the creating thread's CPUs, unchanged | libuv's auxiliary pool |
| `ringCapacity`, `credits` | 1024 slots, credits equal to the ring size | how many requests wait, and how many run in libuv at once |
| `start`, `step`, `stop` | none | hooks that run on the loop thread |
| `closeGrace`, `abandonAfter` | 5 seconds each | how long `close` serves, then how long it waits after cancelling |

**The request.** A `UvRequest` is a plain struct you own. Its address,
`path` and `buffer` must stay put from `submit` until the loop publishes it.

| Group | Fields |
| --- | --- |
| What to do | `op`, `path`, `file`, `flags`, `mode`, `buffer`, `length`, `offset`, `timeoutMs` |
| Where `readFile` allocates | `allocator`, `alignment` |
| Your data | `user`, `userPointer` |
| How to be told | `trigger`, `event`, `callback` with `context`, `completions` |
| Results | `result`, `size`, `state`, `done`, `finished`, `succeeded`, `errorName` |
| The loaded file | `ownsBuffer`, `takeBuffer`, `freeBuffer`, and the `UvBuffer` that `takeBuffer` returns |

The operations are:

- `readFile` opens a file, reads it to the end and closes it. Given a null
  `buffer`, the loop allocates the file's size.
- `open`, `read`, `write`, `close`, `stat` and `fstat` map one to one onto
  libuv's calls.
- `timer` completes after `timeoutMs`. `nop` completes in order with the
  requests around it.

**Who owns a request.** It is yours until `submit` returns `accepted`, the
loop's until it publishes, then yours again. Exactly one party reclaims a
published request: whoever saw it finish, the callback, or the consumer of
the completion ring. Cancelling never hands a request back early: a
cancelled request is still published, with `UV_ECANCELED`. The full rules
are in [Submit requests](../../ant_uv/README.md#submit-requests),
[Completion](../../ant_uv/README.md#completion) and
[Cancel](../../ant_uv/README.md#cancel).

## Load, use later, unload

[load_assets.d](../../ant_uv/examples/load_assets.d) keeps a table of
file-backed assets on the main thread. It loads four files, unloads one of
them before it can finish, collects the rest over the next frames, uses
them, and unloads them.

```sh
dub run --single ant_uv/examples/load_assets.d
```

```text
loading 4 files into the C runtime
settled after 2 frame(s)
load_assets.d: 7100 bytes, first line /+ dub.sdl:
/etc/os-release: 361 bytes
/no/such/file: failed (ENOENT)
load_assets: unloaded
UvCloseReport(4, 1, 0, 0, 0, null)
```

Your sizes will differ. The report counts four requests published, one of
them cancelled, none abandoned or undelivered, and libuv's loop closed
cleanly.

### The table

Each asset holds its own request, so the request keeps a stable address
while the loop owns it. Never resize the array while anything is loading.

```d
// fragment: from load_assets.d
struct Asset
{
    const(char)* path;  // NUL-terminated; the table keeps it alive
    AssetState state;
    UvRequest request;  // the loop's while loading or unloading
    UvBuffer file;      // the file while ready
}
```

The table also owns a `SlotRing!RingElement` of 64 slots. The loop pushes
one element into it for each finished request, and only the main thread pops.

### Load

`load` fills in a whole-file read and submits it. `user` comes back as the
completion element's `tag`, which is how the table finds the asset again.

```d
// fragment: AssetTable.load, for asset `id`
a.request = UvRequest.init;
a.request.op = UvOp.readFile;          // open, read to the end, close
a.request.path = a.path;
a.request.user = id;                   // comes back as the element's tag
a.request.completions = completions;   // deliver to this table
final switch (loop.submit(&a.request))
{
case UvSubmit.accepted:
    a.state = AssetState.loading;
    return true;
case UvSubmit.full:
    return false;                      // the request ring is full: try next tick
case UvSubmit.closed, UvSubmit.invalid:
    a.state = AssetState.failed;
    return true;
}
```

### Collect once per frame

`tick` pops what the loop delivered. Popping an element is the hand-back
point: the loop no longer touches that request, and its results are
visible to the main thread.

```d
// fragment: AssetTable.tick and settle
void tick()
{
    RingElement e;
    while (completions.tryPop(e))
        settle(&assets[cast(size_t) e.tag]);
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

### Use

A ready asset's bytes stay valid until it is unloaded, any number of frames
later. `table.bytes(id)` returns `file.bytes` while the asset is ready, and
null otherwise.

The bytes are ordinary memory. To use them on a worker, hand them over
through something that publishes, such as a Farm payload or a Fiber event.
The table does not track those users, so the application must stop them
before it calls `unload`.

### Unload

A ready asset frees its block at once. A loading asset cannot, because the
loop owns its request and may be about to write into the block. So `unload`
asks for a cancel and marks the asset `unloading`. The tick that collects
it frees whatever the read produced.

```d
// fragment: AssetTable.unload, for asset `id`
final switch (a.state)
{
case AssetState.ready:
    a.file.free(); // back to the allocator that made it
    a.state = AssetState.unloaded;
    break;
case AssetState.loading:
    loop.cancel(&a.request); // a hint: if it misses, tick frees the result
    a.state = AssetState.unloading;
    break;
case AssetState.unloaded, AssetState.failed, AssetState.unloading:
    break;
}
```

The cancel can lose the race. In the run above it won, and the report
counts one cancelled request. When the unload comes after the read has
finished, `settle` frees the loaded block instead, and the report counts
none.

### Shut down

`loop.close()` publishes every request before it returns. A completion the
ring could not take is still published. So `shutdown`, which runs after
`close`, collects the ring, settles any finished request it did not see,
and frees every block.

```d
// fragment: main calls loop.close(), then this
void shutdown()
{
    tick();
    foreach (ref a; assets)
        if ((a.state == AssetState.loading || a.state == AssetState.unloading)
            && a.request.finished)
            settle(&a);
    foreach (id; 0 .. assets.length)
        unload(id);
    completions.destroy();
}
```

## Where the bytes live

By default the loop allocates each file from the C runtime, 64-byte aligned.
To choose otherwise, set `request.allocator` to any `ActorAllocator`, the
type the actor runtimes also take. `request.alignment` can be any power of
two the allocator supports.

| Choice | Set | Note |
| --- | --- | --- |
| The C runtime | nothing | `ActorAllocator.cRuntime()`: alignments up to 64 |
| mimalloc v3 | `allocator = mimallocV3ActorAllocator()` | compile with `AntfarmMimallocV3` and link mimalloc |
| Your own | `allocator = ActorAllocator(context, &allocate, &deallocate)` | an arena, a budgeted pool, or a wrapper that counts |

An allocator for `readFile` must work like this:

- `allocate` runs only on the loop thread, so it must not block.
- `deallocate` runs on whichever thread frees the block. That is the owner
  of the `UvBuffer`, or the loop thread when a read fails after allocating.
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
