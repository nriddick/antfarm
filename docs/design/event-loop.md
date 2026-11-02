# Event loop beside the worker pool: libuv integration design

Status: design for the 2.2.x series, opened 2026-10-30 on `2.2.0-progress`,
revised the same day after an independent review (Codex, gpt-6-astra at
xhigh effort; twenty findings on libuv lifecycle details, completion
ownership, backpressure, shutdown ordering and the measurement citations,
all folded in), then narrowed: the loop runs on **one dedicated thread per
application**, not inside a pool worker. The in-worker variant with its
park-owner hook is recorded in this page's history (`ca9d7ba`) and is not
planned. It coalesces the exploratory work of 2026-10-30 (the
singleton-write study, the Vyukov slot-ring and partitioned ring
measurements, the Element Farm prototype, and the OMMe game-engine
requirements) into one plan. The study's records stay on the `exp/slot-ring`
branch.

**Implemented on `2.2.0-progress`**: the 2.2.0 content of section 6, in
`antfarm/ring.d`, `ant_uv/` (guide: [ant_uv/README.md](../../ant_uv/README.md)),
`tests/ring/`, `tests/ant_uv/` and `tools/perf/slot_ring.d`. Where the
implementation settled a point the plan left open, or departed from it, this
page now says what was built; the departures are listed in section 8.

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
- **Pool threads inherit the creating thread's affinity** on Linux, and
  its nice value too (`pthread_create`). libuv creates every pool thread on
  the first request, so a pool first used from a pinned thread is pinned to
  that one CPU. The loop therefore warms the pool itself and places each
  auxiliary thread from inside it (section 3.4) rather than relying on
  inheritance, which Windows does not provide for thread affinity at all.
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
  the CPU to pin to, the ring and control-ring capacities, the credit limit,
  the auxiliary pool size and placement (section 3.4), and optional
  application `start`, `step` and `stop` hooks (section 3.6). `create`
  claims the process's one loop, spawns the thread and waits; the thread
  captures its inherited affinity, pins itself, checks `uv_version()`,
  initializes the loop, warms and places the auxiliary pool, creates the
  bounding timer and the wake handle (`uv_async_t`, referenced, with a
  callback that only returns `uv_run`), then opens the wake gate and
  publishes *open*. A failure anywhere unwinds what was created and
  `create` throws. Every later `uv_*` call happens on this thread, and
  `UvLoop.current` names the loop there.
- **Serving.** The thread's loop is: drain the control ring (cancels);
  retry deliveries that waited for room; drain the request ring in a
  bounded batch (`submitBatch`, default 64) into libuv submissions, subject
  to the in-flight credit; run the step hook; then poll libuv. When
  something made progress it polls with `UV_RUN_NOWAIT`. Otherwise it
  blocks in `UV_RUN_ONCE` until a completion, a timer or the wake handle.
  The step hook can report a deadline (a loop-thread Fiber's timer), and a
  delivery waiting for room needs a retry in about a millisecond. Since
  `UV_RUN_ONCE` has no timeout argument, a reusable timer bounds that one
  poll and is stopped afterwards.
- **Waking.** `submit` publishes its element on the ring and calls
  `UvLoop.wake()`, which is `uv_async_send`: callable from any thread and
  from inside a Farm callback, coalescing, and effective whether the loop
  thread is blocked in `uv_run` or between iterations, because the pending
  flag persists until the loop processes it. A wake gate (a count of wakers
  in progress plus a closed bit) keeps `uv_async_send` off the handle once
  teardown has begun: teardown sets the bit and waits for the count to
  drain before it closes the handle.
- **Shutdown**, driven by any thread but the loop's through
  `UvLoop.close()`, in three phases:
  1. Close admission: the ring's close bit makes every later push fail.
  2. Grace: keep serving for `closeGrace` until the ring is drained, no
     operation is in libuv and no delivery waits.
  3. Cancelling: publish everything still queued as `UV_ECANCELED`, cancel
     every operation in libuv (timers stop at once; file work libuv has not
     started is cancelled), and wait `abandonAfter` for the final callbacks.
  An operation that is still running after that wait is published
  `abandoned`: its buffer stays libuv's, so the application must leak it.
  Then the `stop` hook runs, every handle is closed, and the loop is closed
  (`uv_loop_close` returning 0); when something was abandoned, the loop and
  its memory are instead left allocated, since libuv may still write to
  them. The thread exits and `close` joins it. Fibers waiting on the loop
  are woken by those publications, so the order is: retire the Fibers that
  use the loop, `UvLoop.close()`, then `host.close()`.
- **Failure.** Every callback the loop registers with libuv is
  `nothrow @nogc`, so no D exception unwinds through C. The only throwing
  code on the loop thread is the application's hooks. If `start` or `step`
  throws, the loop records the failure, stops calling the step, closes
  admission and goes straight to the cancelling phase, then tears down as
  above. `close()` throws a `UvLoopException` that carries the failure and
  the close report.

### 3.2 Elements: a ring the loop thread owns

Requests to the loop cannot be Farm payloads: any consumer may claim a
payload, and only the loop thread may call into libuv. They travel on a
bounded ring of 64-byte slots that the loop thread is the sole consumer of,
and completions that need a second hop travel the same way. With the
resource manager on the loop thread, most I/O decisions never touch the
ring; it carries the requests of workers, Fibers and the main thread.

The 2026-10-30 study (`docs/performance/slot-ring-2026-10-30.md` on
`exp/slot-ring`) settled the ring's shape for this use. Its elements were
three words (24 bytes) in 32- or 64-byte slots. The shipped ring re-run with
the real request element follows the study's findings:

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
- **The shipped ring with the request layout**, measured with the ported
  harness ([slot-ring-shipped-2026-10-30.md](../performance/slot-ring-shipped-2026-10-30.md)):
  `SlotRing!RingElement` (a 56-byte element in a 64-byte slot) costs 10 to
  11 ns per element single-ended and delivers in 40 ns (p50) on one CCD,
  190 ns across CCDs. The shape a loop's request ring sees, many producers
  and one consumer, costs 44, 63 and 81 ns at two, four and seven
  producers. That is better than the 32-byte reference (54, 81, 110 ns),
  because a line per slot removes neighbour-slot sharing, and worse than the
  Farm from four producers on (22 ns), as the study predicted. At file-I/O
  rates none of these numbers matters; the ring is sized for occupancy.
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
so it lives in the core as `antfarm.ring` (`SlotRing!T`, `RingElement`,
`RingPush`), compiles as BetterC, and is exercised from the native C probe.
Its public status in 2.2.0 is **provisional**: the module ships for `ant_uv`
and the harness, and its interface may change. The contract, as implemented
and tested by `tests/ring` (including a producer stalled between reservation
and publication through a test hook):

- Ordering is FIFO by reservation. A dequeue may return false while a
  later reservation is already published, because an earlier producer has
  reserved but not yet written; false therefore does not mean drained, and
  a stalled producer blocks the consumer behind it. The drained test is
  exact: `close` sets a bit in the enqueue cursor, so a push's existing
  load and CAS refuse every later reservation at no extra cost, and
  `drained` is true once the dequeue cursor has reached the cursor's value
  at the close.
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
loop holds an in-flight credit (default equal to the ring's capacity, at
most 65536). An operation in libuv, including a timer handle until its
close callback, holds one credit. A completion waiting for room in a
completion ring also holds one. The drain stops dequeuing when the credit
is spent, so the ring keeps its backpressure. A `nop`, and a request
cancelled before it starts, publish at once and hold none. Each credit is
an operation slot preallocated at creation, sized for both a `uv_fs_t` and
a `uv_timer_t`.

The completion path never spins and never drops. Deliveries that find a
completion ring full wait in a FIFO and are retried on every iteration,
which a one-millisecond poll bound guarantees. Only a closed completion
ring, or a close that gives up waiting, counts a delivery as undelivered.

**Cancellation** travels on a second, small control ring, so it never waits
behind requests the credit is holding back. `UvLoop.cancel` first sets a
flag in the request, which the loop checks when it dequeues the request.
It then pushes the request's address and ticket to the control ring, and
the loop matches those against its operation slots without touching the
record. So a cancel never races a reclaimed record, and it is never lost:
a request not yet dequeued sees the flag, and one in libuv is found by
address and ticket.

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

As built, the four targets are `trigger`, `event`, `callback` and
`completions`, delivered in that order. A request may not name both a
`callback` and a `completions` ring, because each hands the record to a
party that may reclaim it. The completion element carries `user` as its tag
and `result`, `size`, `op` and the terminal state as payload, so a consumer
can dispatch without reading the record.

`submitAndWait`, the helper a managed Fiber uses, handles one more case: the
Fiber is cancelled while it waits. A Fiber's cancellation is sticky, so it
cannot park again, and the loop must never write into a request on a dead
stack. The helper cancels the request and waits for its publication before
rethrowing `FiberCancelled`. On a worker the wait is short sleeps of the
worker's thread. On the loop thread it serves the loop, which is safe
because Fibers run from the step hook and never inside `uv_run`.

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

libuv's auxiliary pool is sized with `UV_THREADPOOL_SIZE` (`auxThreads`),
set before any loop in the process first uses the pool, and placed directly.
During creation the loop queues one warm-up work item per expected pool
thread. Each item, running on an auxiliary thread, applies the requested
mask and priority to its own thread and records what it reads back. It then
waits, up to 250 ms, until every item has arrived, so each lands on a
distinct thread.

The mask is `auxCpus`, or the creating thread's affinity when that is empty,
so the threads never inherit the loop's pin. The priority is
`UvAuxPriority`: on Linux five more nice levels, or nice 19; on Windows
`THREAD_PRIORITY_BELOW_NORMAL` or `THREAD_PRIORITY_LOWEST`. The items call
only OS functions and atomics, because the auxiliary threads are not
registered with druntime.

Direct placement replaces the plan's inheritance trick, which Windows cannot
support (thread affinity is not inherited there) and which needed the loop
thread to change its own pin and priority back and forth.
`UvLoop.auxThreads` and `summary()` report each thread's id, CPUs and
priority, so the placement is measured rather than assumed.

The intended placement is an application choice. The obvious ones are the
reserved core's SMT sibling, for blocking reads beside the loop and off the
workers, or the confined set the host left to the caller. The pool is
process-global, so a second `UvLoop` would share it. `create` therefore
claims the process's one loop, and the claim is released at close.

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
pulled in by the core packages. As built
(`scripts/build-libuv-windows.py`), the Windows library is libuv 1.53.0 as
`uv.dll` with its import library `uv.lib`: the DLL carries its own C
runtime, so D programs keep their static one.

### 3.6 The resource manager on the loop thread

The optional step hook runs once per loop iteration, between the ring drain
and `uv_run`, on the loop thread. That is where an application places logic
that should sit next to the loop: OMMe's resource manager, which turns
demand into file requests and completions into decode jobs for the pool.
Two shapes are available without any new mechanism:

- **Plain code.** The step is a function; the manager keeps its state in
  the loop thread's TLS and issues `ant_uv` requests like any other thread.
  As built there is one submission path: loop-thread submissions also go
  through the ring and are dequeued on the next iteration, which keeps the
  credit and cancellation rules identical for every submitter.
- **Fibers on the loop thread.** `ant_uv.fibers.UvFibers` packages this:
  a private `AntFarm` with the loop thread as its only consumer and
  producer, a `FiberDomain` with the loop thread as Director, and a ready
  lane whose wakes call `UvLoop.wake`. The application builds it in the
  `start` hook, returns its `step` from the `step` hook, and calls its
  `shutdown` from the `stop` hook. The step serves the domain and consumes
  that Farm on the loop thread's own view, as a `FarmHost` with
  `workers == 0` does. Manager logic is then Fibers that wait on `FiberEvent`s which the
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
| libuv binding (`ant_uv.binding`), `UvLoop` thread, rings, credits, cancellation, completion sink, auxiliary pool placement, file, whole-file and timer requests (`ant_uv.loop`), Fibers on the loop thread and `submitAndWait` (`ant_uv.fibers`) | subpackage `ant_uv` | `antfarm`, `threadpool` for pinning, `ant_fibers` for the trigger and event wakes, libuv |
| Request kinds, tag table, generations, the resource manager, main-thread rings, quiesce | the application | |

`ant_uv` follows `ant_fibers` in shape: one owning class and a C-free D
API over the binding; its tests build through `tests/ant_uv/Makefile`
because the layout check links a C translation unit compiled against the
real `uv.h`. `reservedProcessors` was not added: `pool.processors` already
keeps the loop's CPU out of the worker set. It is
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

What runs, and where:

- **Ring** (`tests/ring`, LDC, DMD and ThreadSanitizer; the module's
  unittests run in the root `dub test`; BetterC through the native C
  probe):
  - per-producer FIFO with four producers and one consumer;
  - exactly-once delivery with four producers and four consumers;
  - close under contention, five rounds, ending only on the exact drained
    test;
  - a producer stalled between reservation and publication, which hides
    the published element behind it and keeps the ring from draining;
  - backpressure on a two-slot ring.
- **Loop** (`tests/ant_uv`, LDC, DMD and ThreadSanitizer):
  - the binding's `uv_buf_t` and `uv_stat_t` layouts and every constant it
    declares, against a C translation unit compiled with the real `uv.h`;
  - lifecycle and placement: the loop thread's pin, and the auxiliary pool's
    size, CPUs and nice value read back;
  - 20,000 `nop`s from four threads through a 64-slot completion ring, each
    delivered once, after publication, in its producer's order;
  - timers firing no earlier than their timeouts;
  - a two-slot completion ring holding 98 deliveries until it has room;
  - eight credits under a flood of 1,000 timers from four threads;
  - a full four-slot ring refusing and leaving the request untouched;
    malformed and doubly submitted requests refused;
  - cancellation of timers both in libuv and queued behind the credit, and
    of a batch of 200 `stat`s;
  - close cancelling ten long timers after its grace period;
  - a throwing step hook failing the loop, which cancels its work and
    reports the failure at close;
  - Fibers on the loop thread reading a file and, when cancelled, waiting
    for their request;
  - 96 worker Fibers on a two-group `GroupHost` reading files three ways
    (loop allocation, caller buffer, and `open`/`fstat`/`read`/`close` on a
    counted trigger), with publications equal to submissions once close
    has joined the loop thread, and a cancelled worker Fiber waiting for
    its request;
  - `readFile` into the request's allocator, at its alignment, freed
    exactly once; a directory refused with `EISDIR` before any allocation;
    a read that fails after allocating, freed by the loop (a locked file
    on Windows, a sysfs attribute on Linux); a refused allocation and an
    empty file;
  - an `open` of a FIFO with no writer abandoned by close, with the loop
    left allocated (on Windows, where opening a pipe does not block, a read
    of a named pipe before its server writes).
- **Stability:** 30 consecutive LDC runs, 10 DMD and 10 ThreadSanitizer
  runs of the loop suite, and 10 ThreadSanitizer runs of the ring suite,
  without a failure. ThreadSanitizer needs two suppressions, both explained
  in `tests/ant_uv/tsan.supp`: druntime's GC suspend handler and races
  internal to druntime's uninstrumented GC allocator. libuv itself is
  deliberately not suppressed, because suppressing calls from it would also
  hide the mutexes that order its auxiliary threads' writes.
- **`make check`** runs the ring lanes always and the loop lanes when
  `pkg-config` finds libuv 1.19 or later, printing the reason when it skips
  them.
- **Latency**, measured
  ([uv-latency-2026-10-30.md](../performance/uv-latency-2026-10-30.md)):
  a request's round trip from `submit` to the waiter running again. It is
  0.4 to 0.5 µs for a thread on the loop's CCD spinning on `done` (1.0 µs
  across CCDs), 1.2 µs for a Fiber on the loop thread, and 7 µs at p50
  (10 µs at p99) for a worker Fiber parked on an event. The worker figure
  is mostly the parked worker's wake and the Fiber's resume, not the loop.
- **The full gate:** `make check` passed on `2.2.0-progress`, with the ring
  and `ant_uv` lanes in it.
- **Windows x64:** the native matrix passed with the pinned libuv build,
  the `ring` and `ant_uv` lanes in it, under LDC and DMD. The layout oracle,
  compiled by MSVC against the real `uv.h`, confirms the 16-byte `uv_buf_t`
  with its 32-bit length first; the loop pin, the auxiliary threads' CPUs
  and `THREAD_PRIORITY_BELOW_NORMAL` read back as asked. libuv on Windows
  reports every directory's size as 0, which is why `readFile` refuses
  directories by their mode (section 8).
- **Linux under WSL2** on the Windows host, after the directory refusal:
  the ring and `ant_uv` suites under LDC, DMD and ThreadSanitizer, with
  30, 10 and 10 repeated `ant_uv` runs, both examples and the root
  `dub test`. This supplements `make check` on the 5900XT and does not
  replace it.

## 6. Release plan

- **2.2.0** (implemented on `2.2.0-progress`, unreleased):
  - `antfarm.ring` with the contract of section 3.2 and a provisional
    interface;
  - `ant_uv` with `UvLoop`, its rings and credits, cancellation, the
    completion sink, auxiliary pool placement, file, whole-file and timer
    requests, `UvFibers` and `submitAndWait`;
  - the slot-ring harness ported to the shipped ring.

  That is everything an application needs to load files from worker Fibers
  and from a loop-affine resource manager. The latency measurement, a full
  `make check` and the native Windows matrix with the pinned libuv build
  are done.
- **2.2.1:** sockets, pipes and process spawn in the binding as the first
  consumer needs them.
- **Later:** the Element Farm, once its ordering and membership contract is
  written, as a second channel kind rather than a drop-in; `ant_uv` in the
  C ABI after production use; a loop inside a pool worker (the park-owner
  design in this page's history) only if a consumer needs the loop's CPU
  back for workers; Tier 2 of the coroutine proposal for native, batched,
  reactor-owned continuations.

## 7. Open questions

- When `antfarm.ring` leaves provisional status: after a second user, or
  after the Element Farm contract shows what the common interface is.
- Whether the auxiliary threads' default should stay the creating thread's
  affinity, or become the loop core's SMT sibling at lower priority. A
  flood of file reads on the 5900XT beside a busy pool decides.
- Whether the credit default should follow the ring's capacity or the
  auxiliary pool size; the same flood decides that too.

## 8. Where the implementation departed from the plan

- **Placement is direct, not inherited** (section 3.4). Windows does not
  inherit thread affinity, and the inheritance trick needed the loop thread
  to change its own pin and priority back and forth.
- **One submission path** (section 3.6). The loop thread submits through
  the ring like every other thread, rather than through a separate local
  path.
- **Cancellation has its own ring** (section 3.2). The plan was silent on
  how a cancel reaches a request held back by the credit. The control ring
  and the request flag make cancellation independent of the credit.
- **An abandoned close is observable** (section 3.1). It publishes
  `abandoned`, distinct from `done`, and reports `UV_EBUSY` as `loopClose`.
  The plan said only "an abort outcome".
- **`reservedProcessors` was not added** (section 3.7).
- **`submitAndWait` survives Fiber cancellation** (section 3.3). The plan
  did not address a cancelled waiter. Without this, a cancelled Fiber would
  unwind while the loop still owned a request on its stack.
- **Whole-file reads take an allocator** (section 3.6). The plan left the
  allocation unspecified, and the first build used C `malloc`. A `readFile`
  now allocates from the request's `ActorAllocator`, whose `allocate` half
  runs only on the loop thread. A resource manager can load into an
  allocator it owns, mimalloc included. The block is handed over as a
  `UvBuffer`, which frees it through the same allocator.
- **`readFile` refuses directories** (section 5). The plan did not
  consider them. A directory opens on both platforms; Linux then reported
  its size, and the read failed with `EISDIR` after the allocation, while
  Windows reported size 0 and succeeded with no bytes. `readFile` now
  checks the file type in the `fstat` it already makes and fails with
  `UV_EISDIR` and size 0 everywhere, before allocating or reading.
