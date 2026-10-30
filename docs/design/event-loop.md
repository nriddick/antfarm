# Event loop on worker hosts: libuv integration design

Status: design for the 2.2.x series, opened 2026-10-30 on `2.2.0-progress`.
Nothing on this page is implemented yet. It coalesces the exploratory work of
2026-10-30 (the singleton-write study, the Vyukov slot-ring and partitioned
ring measurements, the Element Farm prototype, and the OMMe game-engine
requirements) into one plan. The measurements it cites are on the
`exp/slot-ring` branch and land here together with the ring.

The line this design draws: **Ant Farm supplies the seams and an optional,
separately linked subpackage that binds libuv; the application owns what a
request means.** The core packages (`antfarm`, `threadpool`, `ant_fibers`,
`forkjoin`) stay dependency-free. The roadmap's position that async I/O stays
out of the runtime still holds for the runtime; the loop runs in a worker
through the `GroupHost` attachment hooks that already exist for Fibers and
the fork-join dispatcher.

## 1. Requirements from the driving application

OMMe (the One Man Machine Engine, the first consumer) has this shape:

- A **main thread** that owns SDL: input polling, the window and the render
  context. It runs a timing loop, never parks in the pool, and should do as
  little else as possible so it can structure game-tick and render-tick
  cadence.
- A **permanent resident host**: the main thread, the Ant Farm worker pool
  and the resident services. Bulk engine code is a **hot-swappable module**
  behind a versioned C ABI; nothing resident may hold a pointer into it
  across a reload.
- The workers must be the **sole dispatch** for application work. The pool
  decides what file resources it needs and fetches them itself; the main
  thread learns about results once per tick.
- Linux and Windows, LDC and DMD.

Translated into library requirements: an I/O multiplexer that a pool worker
can drive without leaving the pool's park and wake contract; a channel from
any thread (workers, Fibers, the main thread) to that worker that nobody else
consumes; a completion path that costs no allocation and can wake a parked
Fiber; and a binding that holds no pointer into reloadable code.

## 2. Why a library loop, and why libuv

A bespoke reactor over io_uring, epoll and IOCP is the
[coroutine proposal's Tier 2](coroutine-replacement.md#tier-2-native-async-io-continuations):
stackless continuations, one owner per reactor, batched submissions. That
remains the right long-term shape for a symmetric design where several
workers wait on one completion queue. It is also a large project with its own
acceptance runs, and nothing in it is needed to load files for a game.

libuv covers files, sockets, pipes, DNS, processes, TTY and timers on both
platforms behind one C surface, and its model matches "one designated worker
runs the loop". Two libuv facts shape everything below:

- **A loop belongs to one thread, and so do its handles and requests.**
  Every `uv_*` call on a loop happens on the worker that drives it. Other
  threads reach the loop only through `uv_async_send`, which is the one
  thread-safe entry, and through data they post for that worker to pick up.
- **File operations are blocking calls on libuv's own thread pool** (four
  threads by default, `UV_THREADPOOL_SIZE`), with the completion callback
  delivered on the loop thread. Sockets, pipes and timers are asynchronous
  through epoll and IOCP. Recent libuv submits some file operations through
  io_uring on Linux (`UV_USE_IO_URING` disables it), which reduces but does
  not remove the pool; Windows uses it throughout. The pool's threads only
  block in `read` and `write` and never run Ant Farm or application code.
  The contract states this rather than claiming the workers are the only
  threads in the process: they are the only threads that *dispatch*.

What a library loop cannot do is let several workers wait on it at once.
That is why the loop is pinned to one worker, and why a symmetric design
stays with Tier 2.

## 3. Architecture

```text
 main thread (SDL)        workers (GroupHost pool)              libuv pool
   |  outbound ring ->   [jobs body][fibers serve][uv serve]      (blocking
   |  <- inbound ring         ^            ^           |            read/write)
   |                          |            |      designated worker
   |                          |            |      uv_run(NOWAIT|ONCE)
 requests ----------------> request ring --|---------> submit uv_fs_* ...
 completions <------------- fence / FiberEvent.set <-- shim sink on loop worker
```

### 3.1 The designated worker and the loop attachment

The loop joins a `GroupHost` as a `GroupAttachment` (`threadpool.groups`),
after the host's consuming body and the Fiber attachment:

- `install` (control thread, after the Farms exist, before the pool
  starts): create the loop, its wake handle (`uv_async_t`) and the request
  ring.
- `start` (per worker): the lowest-indexed worker of the home group becomes
  the **designated worker**. The attachment records its `WorkerSelf`; no
  other worker touches the loop.
- `serve` (per pump, managed lane): on the designated worker, drain the
  request ring into libuv submissions, then `uv_run(loop, UV_RUN_NOWAIT)`.
  Return `progress` when anything was submitted, completed or delivered,
  and the loop's next timer deadline (`uv_backend_timeout`, converted to
  `MonoTime` ticks) as `deadlineTicks`, which the host parks until. Every
  other worker returns no progress.
- `finish`/`uninstall`: stop accepting requests, cancel what can be
  cancelled, run the loop until no handle is live, close it. `uv_loop_close`
  returning 0 is the drained check.

With the polling form the floor for noticing a completion is one park
interval (`idleTimeout`, 1 ms by default). That is adequate for resource
loading and lets the whole feature ship without changing `threadpool`.
Section 3.4 removes the floor.

One loop per host is the starting point. A loop per group is a later knob the
attachment already has the information for (`host.groups`); file completions
are not latency-critical at the CCD scale and socket traffic is light.

### 3.2 Elements: a ring the designated worker owns

Requests to the loop cannot be Farm payloads: any consumer may claim a
payload, and only the designated worker may call into libuv. They travel on
a bounded ring of 64-byte elements that the designated worker is the sole
consumer of, and completions that need a second hop travel the same way.

The 2026-10-30 study settles the ring's shape for this use:

- A **Vyukov bounded MPMC ring** (a sequence word per slot, check-then-CAS
  cursors, FIFO by reservation, no membership) is the baseline. Single-ended
  it costs 7 ns per operation and delivers in about 50 ns on the 5900XT,
  against 77 ns for a one-element Farm table write (every view visits every
  table header, so singleton tables pay the bulk design's fixed cost). The
  ring loses only when four or more threads share one end, where the Farm's
  dispersion wins; one ring per CCD restores the single-ended regime (19 ns
  at 16 threads) provided consumers stay local. Capacity is insensitive
  between 512 and 8192 slots; 1024 is the default.
- The **Element Farm** (`docs/design/element-farm.md` on `exp/slot-ring`)
  is a fixed-element Ant Farm: 64-byte elements, a state word carrying lap,
  publication and claim, peek-then-fetch-add claims with the exactly-once
  guarantee in the word, sharded completions gating a per-segment `Live`
  discharge, and subscription. It measured 16 to 23 ns in one-CCD shapes and
  40 ns delivery, ties the ring at fan-out, and is twice as fast in dual-role
  shapes at three rings per CCD. It is the swap behind the same interface if
  dual-role traffic comes to dominate; the single ring is the test
  configuration.

The ring is dependency-free and useful without libuv (any
designated-consumer channel, the main thread's two rings in section 5), so it
lives in the core as `antfarm.ring`, with the ring's exactly-once and drained
tests, a ThreadSanitizer lane and the `tools/perf/slot_ring` harness from the
study. Backpressure is the Farm's rule restated: a full ring returns false and
the caller keeps ownership of the element. A worker whose pool must drain the
ring never spins on it; a Fiber yields, a job hands the request back to its
owner, the main thread tries again next tick.

Elements are opaque to the library: a tag word, a generation word, a pointer
word and inline payload. What a tag means, and how a generation rejects a
stale element after a reload, is the application's table.

### 3.3 Completion delivery

The shim's completion sink runs on the designated worker, inside `serve`,
and does three things without allocating or waiting:

1. Fills the completion record the submitter supplied and publishes it
   through a fence the submitter owns (the application keeps fences
   reachable; Farm and ring bodies are not GC roots).
2. If a `FiberEvent` is attached, calls `set()`. `ant_fibers` documents
   `set` as callable from any thread; the Fiber resumes on whichever worker
   next consumes its lane's Farm. A deferred-edge object in the style of
   `ActorWaveTrigger` serves callers that must not touch the scheduler from
   the sink.
3. If the submitter asked for a second hop (the main thread, another
   worker), pushes the completion on that consumer's ring.

### 3.4 The external park (`threadpool` change)

Polling leaves a 1 ms floor. Removing it means the designated worker parks
*inside* the multiplexer: `uv_run(loop, UV_RUN_ONCE)` bounded by the
Director's deadline, with the pool's wake for that worker becoming
`uv_async_send`. This is the only part of the design that touches the pool,
and it is specified here so it can be reviewed against the pool's contracts
in [threadpool.md](threadpool.md):

- A `GroupAttachment` may declare a **park owner** for one worker: a pair of
  `@nogc` hooks, `park(WorkerSelf*, long deadlineTicks)` and
  `wake(WorkerSelf*)`. At most one attachment per worker may own the park;
  the host rejects a second at construction.
- The pool's arm-and-recheck sequence is unchanged: the worker arms its wait
  slot, runs the bodies and `serve` hooks once more, and only then parks. The
  park owner's `park` replaces the wait-word sleep for that worker and must
  honor the deadline it is given, which already folds in every attachment's
  `deadlineTicks`.
- `wakeOne`, `wakeOneIn`, `wakeLlc`, `wakeAll`, the Director's policy
  changes and shutdown route a wake for that worker through the park
  owner's `wake`. `uv_async_send` coalesces, which is what a wake needs;
  the wait-word generation still increments so the arm-and-recheck proof
  holds. A worker failure during `park` is captured like any other.
- The owner's `park` runs on the worker only; `wake` must be callable from
  any thread, including from inside a Farm callback (it is one atomic and
  one write to a pipe or an IOCP post).

The same hook serves any multiplexer, not only libuv, which keeps the pool
change generic. Its validation reuses the wait-race shapes in
`tests/threadpool/wait_races.d`, with the park owner as a test double before
libuv is involved.

### 3.5 Binding without ImportC

ImportC over the system `uv.h` fails with LDC 1.43.0 and DMD 2.113.0 on
glibc: first `pthread_rwlock_t` is undefined unless `_GNU_SOURCE` precedes the
include, then `__int128` in `linux/types.h` is unsupported. The binding
therefore declares libuv's surface by hand, restricted to the **opaque-handle
API libuv provides for exactly this purpose**:

- Handles, requests and loops are allocated by size: `uv_loop_size`,
  `uv_handle_size(type)`, `uv_req_size(type)`. No libuv struct layout is
  declared in D except `uv_buf_t`, whose two-field order differs between Unix
  and Windows and is declared per platform.
- User data and results go through accessors: `uv_handle_set_data`,
  `uv_req_get_data`, `uv_fs_get_result`, `uv_fs_get_ptr`, `uv_fs_get_path`,
  `uv_loop_get_data`, `uv_handle_get_loop`.
- Callback signatures are declared as `extern(C)` function types; the
  binding's sink functions are the only callbacks ever registered.
- `uv_version()` is checked at loop creation (the accessor set needs 1.19;
  io_uring submission appears in 1.45).

Linking: the system `libuv` on Linux; a pinned libuv source archive built by
CMake on Windows, checksummed, in the style of OMMe's Jolt bridge. Neither is
pulled in by the core packages.

### 3.6 Package layout

| Piece | Where | Dependencies |
| --- | --- | --- |
| Bounded 64-byte element ring, tests, TSan lane, perf harness | `antfarm.ring` (core) | none |
| Designated-worker selection and the park-owner hook | `threadpool.groups`, `threadpool.pool` | none |
| libuv binding, loop attachment, completion sink, fs/timer/async wrappers | subpackage `ant_uv` (name open) | `antfarm`, `threadpool`, `ant_fibers` for the event wake, libuv |
| Request kinds, tag table, generations, main-thread rings, quiesce | the application | |

`ant_uv` follows `ant_fibers` in shape: a `GroupAttachment` factory, a
`dub.sdl` configuration for its tests, and a C-free D API over the binding.
It is optional in every sense: not built by `make check` unless libuv is
present, not referenced by any core module, and absent from the C ABI until
it has production use.

## 4. What stays with the application

The first consumer's plan is `notes/EVENT_LOOP.md` in the OMMe repository.
The library does not know:

- what a request kind is, which buffers it owns, or how it maps to a
  resource record;
- the tag table, the module generation carried in each element, and the
  rule that a stale element is rejected rather than executed;
- the main thread's rings and its timed root wait;
- the quiesce protocol before a module unload: close producer sides, drain
  the rings, `uv_cancel` what is pending, retire the module's Fibers.

Those rules are what make the binding safe under hot swapping, and they are
all expressible with the elements and hooks above: the binding never holds a
pointer into application code, because every callback it registers is its
own.

## 5. Validation

- **Ring:** exactly-once under M producers and one consumer, drained-state
  checks, the loser and foreign counters from the study, a TSan lane, and the
  `slot_ring` harness re-run as a regression table (single-ended, shared-end,
  per-CCD partition, latency).
- **Loop attachment on a `GroupHost`:** timers only (no libuv pool
  involved); file reads from several worker Fibers with exactly-once
  completion; cancellation of a pending batch; a full request ring returning
  ownership while the pool keeps draining; shutdown with requests in flight,
  checked by `uv_loop_close` returning 0 and the fabric's outstanding count.
- **Park owner:** the wait-race suite with a test-double owner, then the
  same suite with the libuv owner; wake from a Farm callback; wake during
  shutdown; a worker failure inside `park`.
- **Measurement:** completion latency from `uv_async_send` to Fiber resume,
  polling against the park owner; ring numbers unchanged from the study.
- **Platforms:** Linux under both compilers in `make check` when libuv is
  present (skipped with a printed reason otherwise); the native Windows
  matrix with the pinned libuv build.

## 6. Release plan

- **2.2.0:** `antfarm.ring` from the `exp/slot-ring` work, the
  designated-worker attachment helper, and `ant_uv` with the polling `serve`.
  Everything an application needs to load files from worker Fibers today.
- **2.2.1:** the park owner hook in `threadpool` and the libuv owner, with
  the wait-race validation and the latency comparison.
- **Later:** the Element Farm behind the ring interface if dual-role traffic
  warrants it; `ant_uv` in the C ABI after production use; Tier 2 of the
  coroutine proposal for symmetric multi-waiter I/O.

## 7. Open questions

- The subpackage name (`ant_uv` matches `ant_fibers`; `uv_host` names the
  role).
- Whether `antfarm.ring` is public API in 2.2.0 or an internal of `ant_uv`
  until its interface has a second user.
- One loop per host against one per group: decide after measuring
  completion latency across CCDs with the park owner in place.
- Windows processor groups: `HostGroups.span/all` across groups is not
  supported by the planner yet; the designated worker is chosen within the
  home group, so the loop is unaffected, but a per-group loop would be.
