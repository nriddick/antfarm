# Event loop beside the worker pool: libuv integration design

Status: design for the 2.2.x series, opened 2026-10-30 on `2.2.0-progress`,
revised the same day after an independent review (Codex, gpt-6-astra at
xhigh effort; twenty findings on libuv lifecycle details, completion
ownership, backpressure, shutdown ordering and the measurement citations,
all folded in), then narrowed: the loop runs on **one dedicated thread per
application**, not inside a pool worker. The in-worker variant with its
park-owner hook is recorded in this page's history (`ca9d7ba`) and is not
planned. Nothing on this page is implemented yet. It coalesces the exploratory work of
2026-10-30 (the singleton-write study, the Vyukov slot-ring and partitioned
ring measurements, the Element Farm prototype, and the OMMe game-engine
requirements) into one plan. The measurements it cites are on the
`exp/slot-ring` branch and land here together with the ring.

The line this design draws: **Ant Farm supplies a ring and an optional,
separately linked subpackage that owns one libuv loop thread; the
application owns what a request means and where the thread sits.** The
core packages (`antfarm`, `threadpool`, `ant_fibers`, `forkjoin`) stay
dependency-free and the pool is not changed. The roadmap's position that
async I/O stays out of the runtime holds in full: the loop is a service
thread beside the pool, placed on a CPU the application keeps out of the
worker set.

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
- The workers are the **sole dispatch for bulk work**. The pool decides
  what file resources it needs and fetches them itself; the main thread
  learns about results once per tick.
- **One I/O service thread** for the whole application, on the second core
  of the main thread's CCD, owning the one libuv loop. The application's
  resource manager runs with affinity to that thread, so most I/O decisions
  are made where the loop is driven. The application tunes worker count,
  priorities and yield schedules so libuv's auxiliary pool and the OS's
  background threads do not contend with the bulk of the worker pool.
- Linux and Windows, LDC and DMD.

Translated into library requirements: a loop thread the application can
place and stop; a channel from any thread (workers, Fibers, the main
thread) to that thread that nobody else consumes; a completion path that
costs no allocation and can wake a parked Fiber on a worker; deliberate
placement of libuv's auxiliary threads; and a binding that holds no pointer
into reloadable code.

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
platforms behind one C surface, and its model matches "one dedicated thread
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
- **Pool threads inherit the creating thread's affinity**, and on Linux its
  nice value too (`pthread_create`); Windows threads start at normal
  priority within the process class whatever their creator's. libuv creates
  the pool lazily on the first request. This is the placement mechanism:
  the loop thread warms the pool while it temporarily carries the mask and
  priority the application wants the auxiliary threads to have, then
  restores its own. Left to chance, a pool first used from a pinned thread
  is pinned to that one CPU.
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

What a library loop cannot do is let several threads wait on it at once.
One thread owns it, and since nothing else that thread does needs the
pool's park and wake contract, that thread is not a pool worker.

## 3. Architecture

```text
 main thread (SDL)      loop thread (reserved core)       workers (GroupHost pool)
   |  outbound ring ->   [drain ring][manager step]           [jobs][fibers]
   |  <- inbound ring    [uv_run ONCE] <- uv_async_send <---- push + wake
   |                          |     \                              ^
   |                          |      +-- libuv pool (fs, dns) ----- |
 requests ------------------> request ring (single consumer)       |
 completions <--------------- record + fence / trigger.advance ----+
```

### 3.1 The loop thread

`ant_uv` owns one thread per application, `UvLoop` (name open), created by
the application's control thread and joined by it:

- **Creation.** The control thread calls `UvLoop.create(options)`:
  the CPU to pin to, the ring capacity, the credit limit, the auxiliary pool
  size and placement (section 3.4), and an optional application **step**
  hook (section 3.6). `create` spawns the thread; the thread pins itself,
  initializes the loop, the wake handle (`uv_async_t`, referenced, with a
  callback that only returns `uv_run`), the request ring and the auxiliary
  pool, then publishes *open*. Every later `uv_*` call happens on this
  thread.
- **Serving.** The thread's loop is: drain the request ring in a bounded
  batch (`submitBatch`, default 64) into libuv submissions, subject to the
  in-flight credit; run the application step if one is installed;
  `uv_run(loop, UV_RUN_ONCE)`, which blocks until at least one event (a
  completion, a timer, or the wake handle). No bounding timer is needed:
  the thread has no other duties, and anything that wants periodic service
  arms a `uv_timer_t`.
- **Waking.** A producer publishes its element on the ring and calls
  `UvLoop.wake()`, which is `uv_async_send`: callable from any thread and
  from inside a Farm callback, coalescing, and effective whether the loop
  thread is blocked in `uv_run` or between iterations, because the pending
  flag persists until the loop processes it.
- **Shutdown**, driven by the control thread through `UvLoop.close()`:
  close admission (pushes return a closed status); the thread keeps serving
  until the in-flight credit returns to zero or the close deadline passes;
  cancel what is still pending and wait for those terminal callbacks; run
  the sequence of section 2 (`uv_close` every handle, run until the close
  callbacks have run, `uv_loop_close` returning 0); exit the thread; the
  control thread joins it. Work that cannot reach a terminal callback in
  time is reported as an abort outcome, never dropped. The wake handle is
  closed last, after admission has been closed long enough that no producer
  holds a reference; `wake()` on a closed loop is a no-op. Fibers on the
  pool that wait on an aborted operation are woken with the abort outcome
  before the host stops, so the order is: `UvLoop.close()`, then the Fiber
  domain's drain and retirement, then `host.close()`.
- **Failure.** libuv callbacks capture failures into the loop's state; the
  thread reports them through `UvLoop.failure` and stops serving, and no D
  exception unwinds through C. `close()` then runs the same teardown.

### 3.2 Elements: a ring the loop thread owns

Requests to the loop cannot be Farm payloads: any consumer may claim a
payload, and only the loop thread may call into libuv. They travel on a
bounded ring of 64-byte slots that the loop thread is the sole consumer of,
and completions that need a second hop travel the same way. With the
resource manager on the loop thread, most I/O decisions never touch the
ring; it carries the requests of workers, Fibers and the main thread.

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
(any single-consumer channel, the main thread's rings in section 4),
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
loop holds an in-flight credit (default equal to the ring's capacity) that
covers queued, executing and undelivered operations; the drain stops
submitting when it is spent and the ring keeps its backpressure. The
completion path never spins and never drops: a second-hop ring reserves
capacity for completions (the loop keeps completions it could not push and
retries them on its next iteration), and an undeliverable completion holds
its credit until it is delivered.

### 3.3 Completion delivery

Every accepted request carries a caller-owned completion record with stable
storage (native memory, or GC memory the caller keeps rooted and does not
let move) and a result slot. The sink runs on the loop thread, inside
libuv's own callback. Nothing on this path allocates.

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

### 3.4 Placement: the reserved core and the auxiliary pool

The loop thread sits on a CPU the application keeps out of the worker set,
in the first consumer's case the second core of the main thread's CCD so
the main thread, the loop and the resource manager share one L3. The pool
already supports this: `GroupHostOptions.pool.processors` lists the CPUs
the planner may use, and the application passes the machine minus the main
thread's CPU and the loop's. A `reservedProcessors` convenience on
`GroupHostOptions` (excluded from the worker set, printed in `summary()`)
is a small optional addition; nothing in the planner has to change.

libuv's auxiliary pool is sized with `UV_THREADPOOL_SIZE` before any loop in
the process first uses it, and placed by inheritance (section 2): the loop
thread sets the mask and, on Linux, the nice value intended for the
auxiliary threads, issues one trivial request to create them, waits for its
callback, and restores its own pin and priority. The intended placement is
an application choice; the obvious ones are the reserved core's SMT sibling
(blocking reads beside the loop, off the workers), or the confined set the
host left to the caller. On Windows only the mask is inherited; priority
comes from the process class. `UvLoop.create` takes both as options and
`UvLoop.summary()` reports what the auxiliary threads ended up with, so the
placement is measured rather than assumed. The pool is process-global, so a
second `UvLoop` in one process would share it; the library allows one loop
per process and says so.

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

### 3.6 The resource manager on the loop thread

The optional step hook runs once per loop iteration, between the ring drain
and `uv_run`, on the loop thread. That is where an application places logic
that should sit next to the loop: OMMe's resource manager, which turns
demand into file requests and completions into decode jobs for the pool.
Two shapes are available without any new mechanism:

- **Plain code.** The step is a function; the manager keeps its state in
  the loop thread's TLS and issues `ant_uv` requests directly, with no ring
  hop, since it already runs on the loop thread.
- **Fibers on the loop thread.** The manager constructs a private
  `AntFarm` (one consumer, a few producers) and a `FiberDomain` with the
  loop thread as Director; the step serves the domain and consumes that
  Farm on the loop thread's own view, as a `FarmHost` with `workers == 0`
  does. Manager logic is then Fibers that wait on `FiberEvent`s which the
  completion sink sets on the same thread (the waiter-queue mutex is
  uncontended), and suspended I/O work resumes where it was issued. This
  does not touch the pool's `GroupHost`, which stays process-unique for
  the workers.

Workers' Fibers still receive completions across threads through
`FiberGenerationTrigger.advance` (section 3.3); decode and transform work
goes to the pool as Farm payloads through the loop thread's token on the
group Farm (`groupToken` works on any thread; `releaseThreadTokens` before
the host closes).

### 3.7 Package layout

| Piece | Where | Dependencies |
| --- | --- | --- |
| Bounded 64-byte slot ring (provisional interface), tests, TSan lane, perf harness | `antfarm.ring` (core) | none |
| Optional `reservedProcessors` on `GroupHostOptions` | `threadpool.groups` | none |
| libuv binding, `UvLoop` thread, ring ownership, credits, completion sink, auxiliary pool placement, fs/timer/async wrappers | subpackage `ant_uv` (name open) | `antfarm`, `threadpool` for pinning and tokens, `ant_fibers` for the trigger wake, libuv |
| Request kinds, tag table, generations, the resource manager, main-thread rings, quiesce | the application | |

`ant_uv` follows `ant_fibers` in shape: one owning class, a `dub.sdl`
configuration for its tests, and a C-free D API over the binding. It is
optional in every sense: not built by `make check` unless libuv is present,
not referenced by any core module, and absent from the C ABI until it has
production use. The D API is called behind an application-owned resident
bridge, not across a module's C ABI, and the C ABI's rules (one core
instance, no D exception crossing C) apply unchanged.

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
acknowledgement, using the close sequence of section 3.1 scoped to one
generation: close that generation's admission, settle or cancel its
requests, wait for their terminal callbacks, retire its Fibers, then unload.

## 5. Validation

- **Ring:** exactly-once under M producers and one consumer; the drained
  check as "all producers closed and empty"; the stalled-producer and
  false-not-drained cases; the loser and foreign counters from the study;
  a TSan lane; the `slot_ring` harness re-run with the request layout as a
  regression table (single-ended, shared-end, per-CCD partition, latency).
- **Loop thread:** timers only (no auxiliary pool involved); the auxiliary
  pool created under the requested mask and priority and its threads'
  placement read back; file reads requested from several worker Fibers
  and from the main thread with exactly-once completion; cancellation of a
  pending batch with every terminal callback observed before buffers are
  released; a full request ring returning ownership while the pool keeps
  draining; the credit limit under a flood; a full second-hop ring with
  completions retained and retried; `close()` with requests in flight,
  checked by the credit returning to zero and `uv_loop_close` returning 0;
  a failure captured inside a callback; the step hook with a private
  Fiber domain waiting on completions.
- **Measurement:** completion latency from a worker's push to Fiber
  resume, and from the main thread's push to its inbound ring; the ring
  numbers re-taken.
- **Platforms:** Linux under both compilers in `make check` when libuv is
  present (skipped with a printed reason otherwise); the native Windows
  matrix with the pinned libuv build, including the `uv_buf_t` layout check
  and the priority-not-inherited difference.

## 6. Release plan

- **2.2.0:** `antfarm.ring` from the `exp/slot-ring` work with the contract
  of section 3.2 and a provisional interface; `ant_uv` with `UvLoop`, the
  ring, credits, the completion sink, auxiliary pool placement and the
  file, timer and async wrappers; the optional `reservedProcessors`.
  Everything an application needs to load files from worker Fibers and a
  loop-affine resource manager.
- **2.2.1:** sockets, pipes and process spawn in the binding as the first
  consumer needs them; the latency measurements; the Windows pinned build.
- **Later:** the Element Farm, once its ordering and membership contract is
  written, as a second channel kind rather than a drop-in; `ant_uv` in the
  C ABI after production use; a loop inside a pool worker (the park-owner
  design in this page's history) only if a consumer needs the loop's CPU
  back for workers; Tier 2 of the coroutine proposal for native, batched,
  reactor-owned continuations.

## 7. Open questions

- The subpackage and class names (`ant_uv` matches `ant_fibers`; `UvLoop`).
- When `antfarm.ring` leaves provisional status: after a second user, or
  after the Element Farm contract shows what the common interface is.
- Where the auxiliary threads go by default when the application gives no
  placement: the loop's SMT sibling, or unpinned at lower priority. The
  flood test on the 5900XT decides.
- Whether the credit default should follow the ring's capacity or the
  auxiliary pool size; the flood test decides that too.
- Whether `reservedProcessors` is worth adding over `pool.processors`.
