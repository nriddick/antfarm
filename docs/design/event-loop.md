# Event loop on worker hosts: libuv integration design

Status: design for the 2.2.x series, opened 2026-10-30 on `2.2.0-progress`
and revised the same day after an independent review (Codex, gpt-6-astra at
xhigh effort; twenty findings, all folded in below, chiefly the libuv
lifecycle details, the wake proof for the park owner, completion ownership,
backpressure, shutdown ordering and the measurement citations). Nothing on
this page is implemented yet. It coalesces the exploratory work of
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
native, batched, reactor-owned stackless continuations with one owner per
reactor. That remains the right long-term shape for high-rate network and
storage work. It is also a large project with its own acceptance runs, and
nothing in it is needed to load files for a game. A symmetric design in
which several workers wait on one completion queue is a third thing, not
proposed anywhere yet.

libuv covers files, sockets, pipes, DNS, processes, TTY and timers on both
platforms behind one C surface, and its model matches "one designated worker
runs the loop". The facts that shape the design:

- **A loop belongs to one thread, and so do its handles and requests.**
  After initialization, every `uv_*` call on a loop happens on the worker
  that drives it. The one thread-safe entry is `uv_async_send`, which sets
  a pending flag that persists until the loop processes it and coalesces
  repeated sends into one callback.
- **Filesystem and name-resolution requests run on libuv's process-global
  thread pool** (four threads by default), with the completion callback
  delivered on the loop thread. Sockets, pipes and timers are asynchronous
  through epoll and IOCP. `UV_THREADPOOL_SIZE` must be set before the pool
  is first used anywhere in the process, by any loop. libuv can also submit
  some filesystem operations through io_uring on Linux; since 1.49 that is
  off by default and `UV_USE_IO_URING=1` opts in, so the pool is the path
  to plan for. The pool's threads execute only libuv's own C work; the
  binding never offers `uv_queue_work`, so no Ant Farm or application code
  runs there. The contract states this rather than claiming the workers are
  the only threads in the process: they are the only threads that
  *dispatch*.
- **Pool threads inherit the creating thread's affinity** (`pthread_create`
  on Linux). libuv creates the pool lazily on the first request. If that
  request is issued from a pinned worker, every pool thread is pinned to
  that worker's CPU. The attachment therefore warms the pool from the
  control thread in `install`, before any worker exists, under whatever
  affinity the host chose for the caller (`CallerPlacement`), and documents
  that placement as a measured choice.
- **Cancellation is not a release fence.** `uv_cancel` succeeds only for
  pool work that has not started; a cancelled request still delivers its
  callback later with `UV_ECANCELED`, and a read already executing runs to
  completion. Every accepted operation reaches a terminal callback, and
  only then may its request storage, buffers and completion targets be
  released (`uv_fs_req_cleanup` after the callback; immediate submission
  failures are a separate path with no callback).
- **A loop closes only when every handle is closed.** Running the loop does
  not close handles; the async wake handle in particular stays active and
  would keep `uv_run` from returning. Teardown is: stop admission, settle or
  cancel requests and wait for their terminal callbacks, `uv_close` every
  initialized handle (tracked ownership, or `uv_walk` with `uv_is_closing`),
  run the loop until the close callbacks have run, then `uv_loop_close`,
  which returns 0 only then. Handle storage lives until its close callback.
  Files opened with `uv_fs_open` are closed separately.

What a library loop cannot do is let several workers wait on it at once.
That is why the loop is pinned to one worker.

## 3. Architecture

```text
 main thread (SDL)        workers (GroupHost pool)              libuv pool
   |  outbound ring ->   [jobs body][fibers serve][uv serve]      (blocking
   |  <- inbound ring         ^            ^           |            fs / dns)
   |                          |            |      designated worker
   |                          |            |      uv_run(NOWAIT | ONCE)
 requests ----------------> request ring --|---------> submit uv_fs_* ...
 completions <------------- record + fence / trigger <-- sink on loop worker
```

### 3.1 The designated worker and the loop attachment

The loop joins a `GroupHost` as a `GroupAttachment` (`threadpool.groups`),
after the host's consuming body and the Fiber attachment. The managed lane's
pump (`groupManagedPump`) already runs every attachment's `serve` and parks
until the earliest deadline they report, which is the whole service seam
this needs.

**Initialization and ownership handoff.** `install` runs on the control
thread after the Farms exist and before the pool starts. It creates the
loop, the wake handle (`uv_async_t`, referenced, with a no-op callback whose
only job is to return `uv_run`), the bounding timer (section 3.4), the
request ring, warms libuv's pool, and publishes the attachment's owner
state as *initialized, unowned*. Every later `uv_*` call happens on the
designated worker; the control thread touches the loop again only in
`uninstall`, after every worker has joined.

**Designation** is computed in `install`, not elected at startup: the
smallest `workerIndex` among `host.resolution.workers` whose `llcIndex` is
the loop's group, which defaults to the home group `host.groups[0]`. A
group with no workers is a construction error. The attachment stores that
index and its own context, never a `WorkerSelf*`, because wakers outlive a
worker's stack frame. In `start`, the worker with that index takes
ownership (*owned*); every other worker records that it is not the owner.
If the owner never reaches `start` (a start failure elsewhere stops the
pool), `uninstall` finds the state still *unowned* and tears down from the
control thread.

**Serving.** On the designated worker, `serve`:

1. Drains the request ring in a bounded batch (at most `submitBatch`
   elements, default 64) into libuv submissions, subject to the in-flight
   credit (section 3.2), so a continuously replenished ring cannot keep the
   worker from reaching the loop.
2. Calls `uv_update_time` then `uv_run(loop, UV_RUN_NOWAIT)`.
3. Returns `progress` when anything was submitted, completed or delivered,
   and a **finite** deadline: the smaller of the loop's next timer
   (`uv_backend_timeout`, milliseconds; `-1` means none and `0` means now)
   and the attachment's `pollInterval` (default 1 ms), converted to
   `MonoTime` ticks. The deadline is always finite because `uv_async_send`
   cannot wake a worker parked on the pool's wait word: with
   `idleTimeout = Duration.max` or a Director `wait`, an unbounded deadline
   would leave pool-thread completions unnoticed forever.

Every other worker returns no progress for this attachment.

**Waking the owner.** A request producer publishes its element and then
wakes the designated worker specifically. `wakeOne` may pick another worker,
so the helper exposes `wakeWorker(index)` on the host (the pool's existing
per-slot wake, made reachable through `GroupHost`). Until the park owner of
section 3.4 exists, that wake ends a wait-word park and the next pump's
`serve` picks the request up; the nominal polling interval, not a hard
floor, is what bounds completion latency, and a completion may be noticed
sooner or later than it.

**Shutdown** has a phase the attachment hooks alone do not provide. `finish`
runs after a worker leaves its pump and `uninstall` after every worker has
stopped; completions delivered there would wake Fibers that nobody will
flush or resume. The host therefore gets an explicit pre-shutdown step
before `stopWorkers`, driven by the control thread: close request admission
(new pushes return a closed status), keep the loop and the Fiber attachment
serving, wait until the in-flight credit returns to zero or a deadline
passes, cancel what is still pending and wait for those terminal callbacks,
then drain and retire the Fiber domain as `FiberGroups` already requires.
Only then does the pool stop. Work that cannot reach a terminal callback in
time is reported as an abort outcome, never dropped silently. `finish` on
the owner releases ownership; `uninstall` performs the close sequence of
section 2 on the control thread. `GroupHost` does not wire
`ManagedWorkerHooks.poolStopped`, so `uninstall` is the post-join cleanup
for partial initialization as well.

**Wake-target lifetime.** Producers on other threads may call the owner
wake while the owner is leaving. The async handle is closed only in
`uninstall`, after every worker has joined and after admission has been
closed long enough that no producer holds a reference; `wakeWorker` on a
stopped pool is already harmless. Submissions after a pool failure are
rejected by the owner state.

One loop per host is the starting point. A loop per group is a later knob
the attachment already has the information for (`host.groups`); file
completions are not latency-critical at the CCD scale and socket traffic is
light.

### 3.2 Elements: a ring the designated worker owns

Requests to the loop cannot be Farm payloads: any consumer may claim a
payload, and only the designated worker may call into libuv. They travel on
a bounded ring of 64-byte slots that the designated worker is the sole
consumer of, and completions that need a second hop travel the same way.

The 2026-10-30 study (`docs/performance/slot-ring-2026-10-30.md` on
`exp/slot-ring`) settles the ring's shape for this use. Its elements were
three words (24 bytes) in 32- or 64-byte slots; the numbers below are for
that layout and are re-run with the real request layout before 2.2.0:

- A **Vyukov bounded MPMC ring** (a sequence word per slot, check-then-CAS
  cursors, FIFO by reservation, no membership) is the baseline. With one
  thread enqueuing and dequeuing, or one producer and one consumer, it costs
  7 to 10 ns per element against 42 to 46 ns for a one-element Farm table
  (every view visits every table header, so singleton tables pay the bulk
  design's fixed cost; the 77 ns figure elsewhere includes a failed
  `consumeNext` after each write). The ring loses as soon as an end is
  shared: the Farm is ahead from two producers on one consumer (37 against
  48 ns) and by three to five times from four, and ahead at seven consumers
  on one producer. Partitioning restores the single-ended regime: sixteen
  dual-role threads across both CCDs measured 19 to 20 ns with four rings
  per CCD, provided consumers stay on their CCD's rings; capacity between
  512 and 8192 slots made no difference in the one partitioned shape it
  was varied in. For this design the shape is many producers and one
  consumer per loop, which is the ring's weakest shape, so the request ring
  is sized for occupancy, not throughput, and 1024 slots is the default.
- The **Element Farm** (`docs/design/element-farm.md` on `exp/slot-ring`)
  is a fixed-element Ant Farm: 64-byte slots holding an 8-byte state word
  (lap, publication, claim) and a 56-byte payload, peek-then-fetch-add
  claims with the exactly-once guarantee in the word, sharded completions
  gating a per-segment `Live` discharge, and subscription. It measured 16 to
  23 ns in one-CCD shapes and 40 ns delivery, ties the ring at fan-out, and
  is twice as fast in dual-role shapes at three rings per CCD. Its shard
  traversal changes ordering and it has membership, so it is **not**
  interchangeable with the ring behind one interface today; it is the
  candidate if dual-role traffic comes to dominate, after its contract is
  written.

**Ring contract.** The ring is dependency-free and useful without libuv
(any designated-consumer channel, the main thread's rings in section 4),
so it lives in the core as `antfarm.ring`. Its public status in 2.2.0 is
**provisional**: the module ships for `ant_uv` and the harness, and its
interface is marked experimental until these points are settled and tested:

- Ordering is FIFO by reservation. A dequeue may return false while a
  later reservation is already published, because an earlier producer has
  reserved but not yet written; false therefore does not mean drained, and
  a stalled producer blocks the consumer behind it. The drained test is
  "every producer closed and the ring empty", not "dequeue returned false".
- A full ring returns false and the caller keeps ownership of the element.
  A worker whose pool must drain the ring never spins on it; a Fiber
  yields, a job hands the request back to its owner, the main thread tries
  again next tick.
- Ownership transfers with publication: once enqueued, the element's
  pointer words belong to the consumer. A dequeued slot is scratch; nothing
  retains storage borrowed from it.
- A push is followed by a wake of the ring's consumer; the ring itself
  never wakes anyone.
- Destruction requires quiescence: every producer closed, the consumer
  stopped, the ring empty.

Elements are opaque to the library: a tag word, a generation word, a pointer
word and inline payload. What a tag means, and how a generation rejects a
stale element after a reload, is the application's table.

**Credits.** A bounded ring bounds ingress, not outstanding work: draining
it into libuv frees slots while native requests and buffers accumulate. The
attachment holds an in-flight credit (default equal to the ring's capacity)
that covers queued, executing and undelivered operations; `serve` stops
submitting when it is spent and the ring keeps its backpressure. The
completion path never spins and never drops: a second-hop ring reserves
capacity for completions (the attachment keeps completions it could not
push and retries them in the next `serve`), and an undeliverable completion
holds its credit until it is delivered.

### 3.3 Completion delivery

Every accepted request carries a caller-owned completion record with stable
storage (native memory, or GC memory the caller keeps rooted and does not
let move) and a result slot. The sink runs on the designated worker, inside
`serve`, after libuv's own callback. Nothing on this path allocates.

1. **Snapshot the delivery targets** (the event or trigger, the second-hop
   ring, the fence) from the record *before* publishing, because after
   publication the receiver may free or reuse the record.
2. **Publish with a release store** into the record's result slot, paired
   with the receiver's acquire load. This is the ordering rule the C ABI
   already states for native message completion
   ([c-abi.md](c-abi.md)); a fence object alone does not supply it. The
   publication is the sink's final access to the record unless the record
   distinguishes *result ready* from *safe to reclaim*, in which case the
   sink's last access is the reclaim edge.
3. **Deliver** through the snapshotted targets: `FiberGenerationTrigger
   .advance()` for a waiting Fiber (atomics and an enqueue, no mutex; the
   scheduler's next pump turns the edge into a `FiberEvent` wake, and the
   Fiber resumes when some worker consumes its lane's Farm), or
   `FiberEvent.set()` where the caller accepts that `set` takes the waiter
   queue's mutex; then the second-hop push, retried under the credit rule
   above. A trigger or event referenced only from the record must be kept
   alive by whoever owns the record.

**What roots what.** Native ring slots and `uv_req_t.data` are not GC roots.
The library requires stable request, record and buffer storage through the
terminal callback, independently of Fiber cancellation or reload: native
byte buffers by default, or explicitly rooted, non-moving `NO_SCAN` GC
buffers. libuv's pool threads need no druntime registration under that
contract, because they never touch D object graphs; their stacks are not GC
roots, which is why nothing reachable only from them may be GC memory.

### 3.4 The park owner (`threadpool` change, 2.2.1)

Polling leaves the nominal interval as the completion-latency bound.
Removing it means the designated worker parks *inside* the multiplexer and
its wake becomes `uv_async_send`. This is the only part of the design that
touches the pool, and it is specified against the pool's implementation so
it can be reviewed with [threadpool.md](threadpool.md).

**What the pool does today.** A managed worker's loop is: pump; if idle,
arm its wait slot (`prepareWorkerWait`, an acquire/release exchange of the
slot word from ACTIVE to ARMED) and set its bit in the armed bitmap; pump
once more; if still idle, `applyIdlePolicy`, which spins, sleeps for the
Director's period, or waits on the slot word with the managed deadline as
its bound. A waker exchanges the word to ACTIVE and, if it was ARMED, calls
the OS wake; `wakeOne`/`wakeOneIn` first clear a bit in the armed bitmap
with a CAS, and `wakeAllForStop` sweeps every slot. The proof that no wake
is lost is the exchange on the word: a wake between arming and waiting
leaves ACTIVE in the word, and the OS wait compares the word before
sleeping.

**The hook.** A `GroupAttachment` may declare a park owner for one worker:
two `@nogc nothrow` function pointers, `park(context, deadlineTicks)` and
`wake(context)`, plus the worker index. At most one owner per worker; the
host rejects a second at construction. The hook replaces only the blocking
primitive inside `applyIdlePolicy` for that worker, so `spin`, the
Director's `sleep` period and the cadence versions keep their behavior; a
`park` owner is consulted where the wait-word sleep would have been, with
the same deadline the pump computed.

**The wake proof, restated for the owner.** The hazard is specific: a wake
that arrives between arming and the final pump sends the async
notification, the final pump's `uv_run(NOWAIT)` consumes it, and the owner
then blocks in `uv_run(ONCE)` with newly published work waiting. The
protocol that closes it:

- `wake(context)`: exchange the slot word to ACTIVE; if the previous value
  was ARMED, `uv_async_send`. Every existing wake path (`wakeWorker`, the
  bitmap-selected `wakeOne` family, `wakeLlc`, the Director's policy
  changes, `wakeAllForStop`) reaches the owner through this one
  backend-aware function for the owned index.
- `park(context, deadline)`: load the slot word; if it is ACTIVE, return at
  once (a wake landed after arming, whether or not its notification
  survived the final pump). Otherwise arm the bounding timer for the
  remaining deadline, `uv_update_time`, `uv_run(ONCE)`, stop the timer,
  return. A `wake` that lands after the load and before `uv_run` sets the
  async pending flag, which persists until the loop processes it, so
  `uv_run` returns promptly; a `wake` during `uv_run` is the ordinary case.
  The word check stands in for the futex's compare, and the pending flag
  bridges the gap the compare would have covered atomically.
- The loop's own completions returning `uv_run` are the normal exit; the
  owner then re-pumps as any worker does after a wake.

`UV_RUN_ONCE` has no timeout argument, which is why the bound is a timer the
owner arms and stops around each park; `NOWAIT` is used when the deadline
has already passed. The bound limits the blocking poll, not callback
execution. `uv_backend_fd` is not an alternative: it is a Unix embedding
aid and returns -1 on Windows.

**Failures.** Hooks are `@nogc nothrow`. libuv callbacks capture any failure
into the attachment's state and the owner reports it after `uv_run`
returns, through the managed lane's ordinary failure capture; no D exception
unwinds through C. `wakeAllForStop` and shutdown reach the owner like any
wake, so a stop request ends an owner park.

The hook serves any multiplexer, not only libuv, which keeps the pool
change generic. Its validation reuses the wait-race shapes in
`tests/threadpool/wait_races.d` with a test-double owner before libuv is
involved, including the arm, wake, consume-notification, park sequence
above.

### 3.5 Binding without ImportC

ImportC over the system `uv.h` fails with LDC 1.43.0 and DMD 2.113.0 on
glibc: first `pthread_rwlock_t` is undefined unless `_GNU_SOURCE` precedes the
include, then `__int128` in `linux/types.h` is unsupported. The binding
therefore declares libuv's surface by hand, restricted to the **opaque-handle
API libuv provides for exactly this purpose**:

- Handles, requests and loops are allocated by size: `uv_loop_size`,
  `uv_handle_size(type)`, `uv_req_size(type)` (present since 1.0). No libuv
  struct layout is declared in D except `uv_buf_t`, which differs in both
  field order and width: `{ char* base; size_t len; }` on Unix,
  `{ ULONG len; char* base; }` on Windows, a 32-bit length even on Win64.
  The binding declares it per platform and checks sizes, offsets and the
  calling convention against a C translation unit in its test build.
- User data and results go through accessors (1.19 and later):
  `uv_handle_set_data`, `uv_handle_get_data`, `uv_req_set_data`,
  `uv_req_get_data`, `uv_fs_get_result`, `uv_fs_get_ptr`,
  `uv_fs_get_path`, `uv_loop_get_data`, `uv_handle_get_loop`.
- Callback signatures are declared as `extern(C)` function types; the
  binding's sink functions are the only callbacks ever registered.
- Compatibility is enforced twice: at link time, by referencing the
  accessor symbols (a libuv older than 1.19 fails to link), and at loop
  creation, by `uv_version()`, with optional newer entry points gated
  separately.

Linking: the system `libuv` on Linux; a pinned libuv source archive built by
CMake on Windows, checksummed, in the style of OMMe's Jolt bridge. Neither is
pulled in by the core packages.

### 3.6 Package layout

| Piece | Where | Dependencies |
| --- | --- | --- |
| Bounded 64-byte slot ring (provisional interface), tests, TSan lane, perf harness | `antfarm.ring` (core) | none |
| Designated-worker helper, `wakeWorker(index)` on the host, the pre-shutdown step | `threadpool.groups`, `threadpool.pool` | none |
| Park owner hook (2.2.1) | `threadpool.hybrid`, `threadpool.pool` | none |
| libuv binding, loop attachment, credits, completion sink, fs/timer/async wrappers | subpackage `ant_uv` (name open) | `antfarm`, `threadpool`, `ant_fibers` for the trigger wake, libuv |
| Request kinds, tag table, generations, main-thread rings, quiesce | the application | |

`ant_uv` follows `ant_fibers` in shape: a `GroupAttachment` factory, a
`dub.sdl` configuration for its tests, and a C-free D API over the binding.
It is optional in every sense: not built by `make check` unless libuv is
present, not referenced by any core module, and absent from the C ABI until
it has production use. The D API is called behind an application-owned
resident bridge, not across a module's C ABI, and the C ABI's rules (one
core instance, no D exception crossing C) apply unchanged.

## 4. What stays with the application

The first consumer's plan is `notes/EVENT_LOOP.md` in the OMMe repository.
The library does not know:

- what a request kind is, which buffers it owns, or how it maps to a
  resource record;
- the tag table, the module generation carried in each element, and the
  rule that a stale element is rejected rather than executed;
- the main thread's rings and its timed root wait;
- the quiesce protocol before a module unload.

Resident callback addresses are necessary for reload safety, not sufficient:
pointers into reloadable code can also reach the loop through request data,
completion records, Fiber delegates, destructors and buffers. The
application's resident bridge therefore retains nothing from a module
generation past that generation's quiescence, which includes every
accepted request's terminal callback and every completion's delivery
acknowledgement, using the pre-shutdown step of section 3.1 scoped to one
generation: close that generation's admission, settle or cancel its
requests, wait for their terminal callbacks, retire its Fibers, then unload.

## 5. Validation

- **Ring:** exactly-once under M producers and one consumer; the drained
  check as "all producers closed and empty"; the stalled-producer and
  false-not-drained cases; the loser and foreign counters from the study;
  a TSan lane; the `slot_ring` harness re-run with the request layout as a
  regression table (single-ended, shared-end, per-CCD partition, latency).
- **Loop attachment on a `GroupHost`:** timers only (no libuv pool
  involved); the pool warmed from `install` and its threads' affinity
  checked; file reads from several worker Fibers with exactly-once
  completion; cancellation of a pending batch with every terminal callback
  observed before buffers are released; a full request ring returning
  ownership while the pool keeps draining; the credit limit under a flood;
  a full second-hop ring with completions retained and retried; the
  pre-shutdown step with requests in flight, checked by the credit
  returning to zero and `uv_loop_close` returning 0; an owner that never
  reaches `start`.
- **Park owner (2.2.1):** the wait-race suite with a test-double owner,
  then the same suite with the libuv owner; the arm, wake, consume,
  park sequence; wake from a Farm callback; wake during shutdown; a failure
  captured inside `park`.
- **Measurement:** completion latency from the producer's push to Fiber
  resume, polling against the park owner; the ring numbers re-taken.
- **Platforms:** Linux under both compilers in `make check` when libuv is
  present (skipped with a printed reason otherwise); the native Windows
  matrix with the pinned libuv build, including the `uv_buf_t` layout check.

## 6. Release plan

- **2.2.0:** `antfarm.ring` from the `exp/slot-ring` work with the contract
  of section 3.2 and a provisional interface; the designated-worker helper,
  `wakeWorker(index)` and the pre-shutdown step; `ant_uv` with the polling
  `serve`, credits and the completion sink. Everything an application needs
  to load files from worker Fibers.
- **2.2.1:** the park owner hook in `threadpool` and the libuv owner, as a
  separate change with its own review of the wake proof, the wait-race
  validation and the latency comparison.
- **Later:** the Element Farm, once its ordering and membership contract is
  written, as a second channel kind rather than a drop-in; `ant_uv` in the
  C ABI after production use; Tier 2 of the coroutine proposal for native,
  batched, reactor-owned continuations.

## 7. Open questions

- The subpackage name (`ant_uv` matches `ant_fibers`; `uv_host` names the
  role).
- When `antfarm.ring` leaves provisional status: after a second user, or
  after the Element Farm contract shows what the common interface is.
- One loop per host against one per group: decide after measuring
  completion latency across CCDs with the park owner in place.
- Windows processor groups: `HostGroups.span/all` across groups is not
  supported by the planner yet; the designated worker is chosen within one
  group, so the loop is unaffected, but a per-group loop would be.
- Whether the credit default should follow the ring's capacity or libuv's
  pool size; the flood test decides.
