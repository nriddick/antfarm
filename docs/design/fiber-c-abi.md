# Fibers across the C ABI

Status: sketch, 2026-11-04, on `2.2.1-progress`, before any code. ABI 1
has no Fiber exports ([c-abi.md](c-abi.md), [c-dispatcher.md](c-dispatcher.md));
this page says which `ant_fibers` functionality should cross the C ABI,
what shape it takes there, and what stays in D. The decisions in sections
2 to 6 are proposed; section 9 lists what is open. Nothing here changes an
existing ABI 1 row.

## 1. What the first consumer asks

OMMe's shape, from [the event loop design](event-loop.md#1-requirements-from-the-driving-application):
a permanent resident host (the main thread, the Ant Farm pool, the loop
thread and the resident services, all D) and a hot-swappable engine module
behind a versioned C ABI. Nothing resident may hold a pointer into the
module across a reload. The workers are the sole dispatch for bulk work,
and some of that work has Fiber shape: fetch a file through the loop and
continue, wait for a datagram or a wave, sleep until a tick, join a
sibling. Today that shape is reachable only from D.

So the module needs, through C calls:

- to admit Fibers whose bodies are module code, on the resident's domain
  and pool, and to get back an identity that survives recycling;
- inside such a body, the scheduler boundaries: yield, sleep, wait on an
  event, a semaphore or a signal, join another Fiber, and learn that it
  has been cancelled;
- from any thread, the other side of each wait: set, post, signal, cancel,
  poll, join;
- outcomes, as values the module can read: completed, failed with a code,
  cancelled.

And the resident needs one thing the D API does not have: a way to find
and finish every Fiber that runs module code before the module is
unloaded, without stopping its own.

What the consumer does not ask: creating or hosting a domain from C,
driving a service loop from C, lifecycle records in the module, or GC
rooting of module memory. The resident does the hosting in D. A pure C
program should still be able to host a domain, since the C ABI's existing
tests and the first consumer's reload story both need it; section 6 keeps
that surface as small as the existing hosts allow.

Whether the module calls exported symbols or a table of function pointers
the resident hands it at load is the host's business. On Windows a DLL
cannot import from its executable without exports, so OMMe will use a
table; every entry point below is an ordinary C-linkage function either
way.

## 2. Decisions

**No D exception crosses a C frame, so no wait throws.** Every scheduler
boundary is an export that returns a status. `FiberCancelled` becomes
`AF_FIBER_CANCELLED` and is sticky: once a wait has reported it, every
later wait in that body reports it at once without parking, exactly as
the D scheduler refuses to park a cancelled Fiber again. Cancellation is
its own channel, never a failure: it has its own wait status, its own
outcome and its own disposition, and a C body cannot swallow it with a
catch-all the way a D `catch (Exception)` swallows `FiberCancelled`. It
can only ignore the status, and stickiness covers that. A body that
ignores it and keeps calling waits spins its worker; that is the same bug
as a D body that catches `FiberCancelled` in a loop.

**After cancellation the return value keeps its meaning and the
disposition records the request**, as it does for a D body. The
trampoline keeps the caught `FiberCancelled` on the task's C record. A
body that then returns zero has that same object rethrown after it
returns: outcome `cancelled`, disposition `acknowledged`. A body that
returns a failure code has the failure object rethrown instead: outcome
`failed`, disposition `cleanupFailed`, the code retained. A body that
never reached a boundary and returned normally is `cancelled` with
`resultAbrogated`, which the scheduler assigns on its own. Nothing is
allocated, no C frame is unwound (the rethrow happens in the trampoline's
D frame after the C frame has returned), and the scheduler's cancellation
path, its records and its cleanup rules run unchanged.

**A body is `uint32_t body(void *context)`.** Zero is `completed`; any
other value is `failed`, and the value is the failure code, 24 bits as in
`af_actor_message_fail`. The failure is retained on the task the way an
uncaught exception is, so records, `af_fiber_poll` and joins report it the
same way. The retained object is one preallocated exception per domain
carrying the code, not an allocation per failure.

**An `Error` aborts at the edge; an `Exception` becomes a status.** Every
Fiber export catches `Error` and calls `fatal`, which runs the handler
installed by `af_set_fatal_handler`, prints and aborts. An `Error` never
becomes a status a body could ignore, nor a sticky flag, so no C reader
ever sees an Error-class failure. This is deliberately stricter than the
runtime, fork-join and group wrappers, which fold every `Throwable` into
`AF_RUNTIME_ERROR`: the scheduler's state after an `Error` is not one to
hand back to a body. The D path, where an `Error` from a body fails the
task, marks the domain fatal and makes the next `serve` throw it at the
host, stays as it is for a D host's own domain, since that domain is the
host's to decide about. Every `Exception` a wait or spawn export can throw
is a misuse or an admission refusal, decided before the task parks: a
reserved high-bit signal, a negative semaphore count, a domain that is
shutting down, record backpressure. The body gets `AF_INVALID`, `AF_BUSY`
or `AF_FULL` with a borrowed message and is still a live, consistent
Fiber that may wait again; the misuse section 8 lists as death cases
aborts instead. No object crosses in either direction: downward an
exception becomes a status, upward a failure becomes a code, and the only
objects ever rethrown are ones the trampoline caught in its own D frame.
One rule follows for the implementation: the diagnostic is thread-local
and bodies migrate, so an export writes it on the thread that returns to
the body, after the last possible migration.

**The body runs on a D Fiber stack, on any eligible worker, and may
migrate at every boundary.** The TLS rule is the D rule restated for C: no
`thread_local` address, `errno`, locale, floating-point environment or
thread-affine lock may be carried across a wait. A body must not block its
worker in an OS wait, must not `longjmp` across a boundary, and must not
unwind across one. It may call back into the module freely.

**Identity is a 16-byte value handle.** `af_fiber_handle` is the slot
pointer and the generation, the same two words as `TaskHandle` without its
`diagnosticId`, which is a pure function of those two and comes back from
a query. It is copyable, generation-checked, canonical empty `{0}`, and a
stale one reports `AF_FIBER_STALE` rather than crashing, like an actor
owner. A fabricated slot pointer is an invalid memory access, as it is for
actor submission handles. The module never sees a `FiberTask`.

**Bodies and contexts live in a C record on the task, cleared at
retirement.** The delegate the scheduler runs is resident code that reads
the body pointer and context from the current task's record. A recycled
task therefore holds no pointer into the module: `Fiber.reset` keeps the
resident delegate, and the record was cleared when `retire` handed the
task back. This is what makes the reload rule of section 5 checkable.

**Domain-level handlers belong to the resident.** `onCompletion` and
`onRecord` are fixed at construction and run on the Director or a service
thread, so the module cannot own them without pinning itself across a
reload. The module observes its own Fibers through handles, or through an
optional per-spawn completion callback stored in the same cleared record.

**Events and semaphores are allocated handles, GC-rooted while C owns
them.** `FiberEvent` and `FiberSemaphore` are D classes with a mutex each,
so they cannot be 32 opaque bytes in caller memory the way an actor
message can. Signals are the existing `uint64_t` keys with the high bit
clear, and need no handle.

**Time is nanoseconds for relative waits and the private header's ticks
for absolute ones.** Timeouts take `uint64_t timeout_ns`, with
`AF_WAIT_FOREVER` as in the runtime header. `af_fiber_sleep_until` takes
the ticks of `af_private_clock_frequency`, which in the runtime archive
are `MonoTime` ticks, so a tick loop that already uses those ticks for
private-root deadlines can sleep to the same grid.

**The surface lives in the runtime archive.** A new header
`antfarm_fibers.h` with the status range 128 to 143, its D wrapper
`cabi/source/antfarm/c/fibers.d`, and `ant_fibers/source` added to the
runtime archive's sources. The core stays BetterC and unchanged. Every
new row is an addition to the ABI 1 lock; nothing existing moves.

## 3. The surface

The names follow the `af_` prefix and the existing headers' conventions:
exact-size versioned option structures, `af_status` returns, a borrowed
diagnostic from `af_last_error_message`, and outputs cleared on failure.

```c
/* antfarm_fibers.h: optional druntime-inclusive Fibers. Link with the
   runtime archive. Statuses 128-143. */
typedef struct af_fiber_domain af_fiber_domain;
typedef struct af_fiber_event af_fiber_event;
typedef struct af_fiber_semaphore af_fiber_semaphore;
typedef struct af_fiber_handle { void *slot; uint64_t generation; } af_fiber_handle;
enum { AF_FIBER_CANCELLED = 128, AF_FIBER_TIMED_OUT = 129, AF_FIBER_STALE = 130 };
/* No status stands for a D Error: one raised inside any export aborts
   through the fatal handler. */
/* FiberOutcome, FiberJoinStatus and CancellationDisposition, same values. */
enum { AF_FIBER_PENDING = 0, AF_FIBER_COMPLETED = 1, AF_FIBER_FAILED = 2,
       AF_FIBER_OUTCOME_CANCELLED = 3 };
enum { AF_FIBER_JOIN_PENDING = 0, AF_FIBER_JOIN_COMPLETED, AF_FIBER_JOIN_FAILED,
       AF_FIBER_JOIN_CANCELLED, AF_FIBER_JOIN_TIMED_OUT, AF_FIBER_JOIN_WOULD_BLOCK,
       AF_FIBER_JOIN_WRONG_DOMAIN, AF_FIBER_JOIN_STALE };
typedef struct af_fiber_join_result {
    af_fiber_handle task;
    uint32_t status, outcome, disposition, failure_code;
} af_fiber_join_result;

/* Bodies: zero completes, any other value fails with that 24-bit code. */
typedef uint32_t (*af_fiber_body)(void *context);
/* At retirement, on the Director or a service thread, before recycling. */
typedef void (*af_fiber_on_complete)(void *context, af_fiber_handle,
                                     uint32_t outcome, uint32_t failure_code);

typedef struct af_fiber_spawn_options {
    uint32_t struct_size, abi_version;
    uint32_t llc;          /* AF_FIBER_DEFAULT_LANE, or a group's lane */
    uint32_t tag;          /* reload epoch (section 5); 0 is untagged */
    uint64_t stack_bytes;  /* 0: the runtime default; steers pool reuse */
    af_fiber_on_complete on_complete;  /* optional; cleared at retirement */
    void *complete_context;
    uint64_t reserved;
} af_fiber_spawn_options;
void af_fiber_spawn_options_default(af_fiber_spawn_options *out);

/* Attached threads, including a running Fiber. AF_FULL is lifecycle
   backpressure (retention / 4 live or unretired tasks). */
af_status af_fiber_spawn(af_fiber_domain *, af_fiber_body, void *context,
                         const af_fiber_spawn_options *, af_fiber_handle *out);

/* Inside a managed Fiber of the domain only; AF_INVALID elsewhere. Each
   returns AF_OK, AF_FIBER_TIMED_OUT (timed forms), or AF_FIBER_CANCELLED,
   which is sticky for the rest of the body. */
af_status af_fiber_yield(void);
af_status af_fiber_sleep_for(uint64_t ns);
af_status af_fiber_sleep_until(int64_t ticks);
af_status af_fiber_await(uint64_t signal);            /* high bit clear */
af_status af_fiber_event_wait(af_fiber_event *, uint64_t timeout_ns);
af_status af_fiber_semaphore_wait(af_fiber_semaphore *, uint64_t timeout_ns);
af_status af_fiber_join_fiber(af_fiber_handle, uint64_t timeout_ns, af_fiber_join_result *out);
af_status af_fiber_current(af_fiber_handle *out);
uint32_t  af_fiber_cancellation_requested(void);      /* 0 or 1 */
af_status af_fiber_cancel_current(void);              /* then unwinds at the next wait */

/* Any native thread, no attachment: these take no GC path (section 4). */
af_status af_fiber_event_create(af_fiber_domain *, uint32_t manual_reset, af_fiber_event **out);
af_status af_fiber_event_set(af_fiber_event *);       /* one waiter per set, or all */
af_status af_fiber_event_reset(af_fiber_event *);
af_status af_fiber_event_destroy(af_fiber_event *);   /* no waiters; attached */
af_status af_fiber_semaphore_create(af_fiber_domain *, int64_t initial, af_fiber_semaphore **out);
af_status af_fiber_semaphore_post(af_fiber_semaphore *, int64_t count);
af_status af_fiber_semaphore_destroy(af_fiber_semaphore *);
af_status af_fiber_signal(af_fiber_domain *, uint64_t signal, size_t *woken);
af_status af_fiber_poll(af_fiber_handle, af_fiber_join_result *out);
/* Blocks the calling OS thread; never from a Fiber (AF_INVALID). */
af_status af_fiber_join(af_fiber_handle, uint64_t timeout_ns, af_fiber_join_result *out);
uint64_t  af_fiber_diagnostic_id(af_fiber_handle);

/* Attached threads. Submission is generation-paired and applied by the
   next service pass or an explicit drain; it is not a cancellation fence. */
af_status af_fiber_request_cancel(af_fiber_domain *, af_fiber_handle);
af_status af_fiber_drain_cancellations(af_fiber_domain *, size_t *applied);

/* Reload support (section 5). Control thread of the domain. */
af_status af_fiber_cancel_tagged(af_fiber_domain *, uint32_t tag, size_t *requested);
af_status af_fiber_count_tagged(af_fiber_domain *, uint32_t tag, size_t *live_or_unretired);

/* Hosting from C (section 6); the first consumer exports its D domain. */
typedef void (*af_fiber_on_record)(void *context, const struct af_fiber_record *);
typedef struct af_fiber_domain_options {
    uint32_t struct_size, abi_version;
    uint64_t record_retention;        /* >= 4; default 16384 */
    uint32_t retirement;              /* AF_FIBER_RETIRE_DIRECTOR or _SERVICE_THREADS */
    uint32_t reserved;
    af_fiber_on_complete on_completion; void *completion_context;
    af_fiber_on_record on_record;       void *record_context;
} af_fiber_domain_options;
void af_fiber_domain_options_default(af_fiber_domain_options *out);
af_status af_fiber_domain_create(const af_fiber_domain_options *, af_farm *, af_fiber_domain **out);
af_status af_group_start_fibers(const af_group_options *, const af_fiber_domain_options *,
                                af_group_body, af_group_stop, void *context,
                                af_group_host **host, af_fiber_domain **domain);
af_status af_fiber_domain_serve(af_fiber_domain *, af_token *, const struct af_fiber_service_pass *,
                                struct af_fiber_service_result *out);
af_status af_fiber_domain_retire(af_fiber_domain *, size_t maximum, size_t *retired);
af_status af_fiber_domain_begin_shutdown(af_fiber_domain *, uint32_t cancel_active);
af_status af_fiber_domain_state(const af_fiber_domain *, struct af_fiber_domain_state *out);
af_status af_fiber_domain_destroy(af_fiber_domain *);  /* drained and retired; AF_BUSY otherwise */
```

`af_fiber_record` carries the lifecycle record by value: sequence, kind,
handle, outcome, cancellation result and disposition, and the failure code
(zero for a D body's exception, whose message is borrowed for the call).
`af_fiber_domain_state` reports `active`, `waiting`, `ready`, pending
records and completions, `retirement_pending`, `drained` and `fatal`, the
reads the D hosts poll.

On the D side one function, not a C export, serves the first consumer:
`antfarm.c.fibers.exportFiberDomain(FiberDomain)` returns a borrowed
`af_fiber_domain*` for a domain the resident built, rooted while the
wrapper exists, and `releaseFiberDomain` drops it. `FiberGroups` and the
pool stay exactly where they are; the module's lane choice `llc` maps to
`spawnOn`.

## 4. Which thread may call what

| Caller | Entry points |
| --- | --- |
| A managed Fiber of the domain | yield, sleep, await, event and semaphore waits, join_fiber, current, cancellation_requested, cancel_current; also everything an attached thread may call |
| An attached thread | spawn, request_cancel, drain_cancellations, event and semaphore create/destroy, the hosting calls |
| Any native thread, no attachment | event_set and reset, semaphore_post, signal, poll, join, diagnostic_id |
| The domain's control thread | cancel_tagged, count_tagged, retire under `director`, begin_shutdown, destroy |

The third row is the `af_host_wake` class. `FiberEvent.set`,
`FiberSemaphore.post`, `FiberDomain.signal`, `TaskHandle.poll` and the
external `join` are `nothrow @nogc`, take only their own mutex and the
pool's wake path, and touch objects the wrapper has rooted, so an
unattached thread such as one of libuv's pool threads may use them. That
claim is an audit obligation of the implementation, not a property to
assume: the ABI check's declaration audit should list these functions and
the test suite should call them from native threads, as the core tests do.

Workers are druntime threads and attached; the resident's main thread is a
D thread. A foreign thread that spawns or cancels attaches for the call's
lifetime as the runtime header already requires.

## 5. The reload rule

The resident may unload the module only when no task, live or awaiting
retirement, refers to it. With section 2's cleared record, that is exactly
"no task with the module's tag is live or unretired":

1. The resident closes its own gate on spawns for the departing module.
   That gate is the resident's, in the module ABI, not a domain state.
2. `af_fiber_cancel_tagged(domain, tag)` queues a cancellation request for
   every live task with the tag. A request claims a parked wait as a
   cancellation does today, so a body parked on an event, a semaphore, a
   signal, a timer or a join wakes with `AF_FIBER_CANCELLED`; one that is
   ready or running sees it at its next boundary.
3. The resident keeps serving and retiring until
   `af_fiber_count_tagged(domain, tag)` is zero. `retire` runs each
   per-spawn `on_complete` and then clears the record, so a zero count
   means no body, context or callback pointer into the module remains in
   the domain, including in the recycle pool.
4. The module unloads.

What can hold step 3 open is what holds any cancellation open: a body that
never reaches a boundary. The resident bounds that as it bounds any
runaway work; the ABI cannot. A body parked on an application signal
nobody will deliver does not hold it open, because the request itself is
the wake.

The tag scan is a walk of the domain's roots under the admission lock,
linear in live tasks, on the control thread, at reload cadence. The
alternative, a per-module handle set kept by the resident's spawn wrapper,
needs its own lock because bodies spawn from workers, and would have to
forget handles as they retire; the tag puts that bookkeeping where the
scheduler already keeps every task.

## 6. Hosting from C

The first consumer does not host from C, but the C test suite must, and
a C program with no D main should be able to. Three hosts exist in D;
two are worth exporting now.

- **Caller-driven.** `af_fiber_domain_create` over an `af_farm` the caller
  owns, `af_fiber_domain_serve` with the caller's token as responder, and
  `af_consume_next` on the caller's view: the `drainUntilEmpty` loop
  written out. The calling thread is the Director. This is the simplest
  host for deterministic tests and for a single-threaded C program.
- **GroupHost.** `af_group_start_fibers` constructs the domain, builds the
  `FiberGroups` attachment, puts it beside an optional raw C body and
  starts the host in one call, because the attachment must be in the
  host's options before the host exists and `af_group_options` cannot grow
  under the exact-size rule. The managed pump runs raw bodies and `serve`
  hooks together, so a C worker body keeps running beside the Fibers; the
  cost is the managed lane's GC-enabled pump in place of the raw one. The
  control thread is the Director; under `director` retirement it calls
  `af_fiber_domain_retire` when `retirement_pending` says so, under
  `service_threads` the workers retire and the handlers run there.
  Teardown is the domain's `begin_shutdown`, `drained`, `retire`, then
  `af_group_stop_workers` and `af_group_close` as today; `af_group_close`
  returns `AF_BUSY` while the domain exists.
- **FarmHost** (`attachFiberDomain`) stays D-only. The C `af_host` has no
  attachment surface, and nothing asks for one.

Runtime shutdown counts domains, events and semaphores as it counts hosts
and completions: `af_druntime_shutdown` returns `AF_BUSY` while any
remains.

## 7. What stays out

- Raw `FiberTask` references, D delegates, `spawnHandle` as a separate
  call (every C spawn returns the handle), and `FiberReadyLane` objects:
  lanes are chosen by group index.
- Domain-level handlers owned by the module, and lifecycle records in the
  module. The resident reads records; the module reads handles.
- GC rooting of module memory. Context pointers are opaque to the
  runtime, which neither scans nor frees them.
- `ActorWaveTrigger` and `FiberGenerationTrigger`. A C wave's completion
  can set an `af_fiber_event` through the wave's completion hook; the
  allocation-free trigger is an optimisation to add when a C consumer
  measures the mutex.
- The loop. `ant_uv` has no C surface either; when it gets one, its
  delivery to a `FiberEvent` is the natural meeting point, and
  `submitAndWait` becomes `af_uv_submit_and_wait` on an `af_fiber_event`.
  Section 9 lists it.
- Shared-library packaging of the runtime archive, which the first
  consumer sidesteps with its function table.

## 8. Costs and validation

A C body costs one resident trampoline frame per activation, a status
check in each wait export, one byte of sticky state and a rethrown
exception reference on the task's C record, and two pointers for the
per-spawn callback. The warm path (recycled task, no allocation) is
unchanged; `af_fiber_spawn` on a warm pool allocates nothing beyond what
`spawn` does. The tag scan is paid at reload.

Validation, in `tests/cabi/fibers.c` under the runtime C lane:

- The caller-driven host: spawn, every boundary, timed forms timing out,
  failure codes, per-spawn completion, records on the domain, handle
  staleness after retirement, `AF_FULL` at retention.
- Sticky cancellation: a body cancelled in each wait kind reports
  `AF_FIBER_CANCELLED` and its later waits return at once; returning zero
  gives `cancelled` and `acknowledged`, returning a code gives `failed`
  and `cleanupFailed` with the code retained, a body that never reached a
  boundary gives `cancelled` and `resultAbrogated`; cancellation before
  first entry suppresses the body.
- GroupHost: migration between workers (resume identity changes across a
  sleep), a raw C body beside the Fibers, both retirement modes, two
  groups where the host has them (SKIP on one LLC as the D smoke does).
- Native threads: set, post, signal, poll and join from unattached C
  threads, as the core tests do.
- Reload: a tag drained by `cancel_tagged` and `count_tagged` with bodies
  parked on each wait kind, after which the recycle pool holds no body
  pointer (an audit seam reads the cleared records).
- Death cases in release builds: a wait outside a Fiber, a handle of
  another domain, `retire` from inside a body on the Director, destroy
  with live tasks, and a D `Error` raised inside an export through a test
  seam, which reaches the fatal handler and never a status.
- The declaration audit covers the header, the lock gains only rows, and
  the lane runs on Linux and Windows under both compilers. The Fiber TSan
  smoke stays D-only; the C bodies are uninstrumented.

## 9. Open

1. Whether OMMe's module receives a function table or links exports, and
   whether that table is versioned per header or as one block. Either
   works with the surface above; it decides nothing in `ant_fibers`.
2. Whether per-spawn `on_complete` earns its place next to handles. It
   saves the module a poll loop for fire-and-forget work; it also adds a
   module pointer the reload rule must clear. Handles alone would do.
3. Whether `af_group_start_fibers` is the right shape, or a two-step
   `af_fiber_groups_create` the host's options take by handle. The
   one-call form keeps the exact-size rule; the two-step form needs a
   second options structure or ABI 2.
4. Whether events and semaphores should be exported at all before the
   loop's C surface exists, since the first consumer's waits are mostly
   on loop completions and waves. Signals and sleeps cover the rest.
5. The failure object. One preallocated exception per domain carrying the
   latest code is cheap but loses codes under concurrent failures;
   one per task costs an object per slot. The record and the handle carry
   the code either way, so the retained object matters only to a D
   `onCompletion` that reads `exception`.
6. `af_fiber_cancel_tagged` as a scan versus a per-tag list on the task
   record. The scan is simpler and reload is rare.
7. The loop from C: `af_uv_*` with delivery to an `af_fiber_event`, and
   `af_uv_submit_and_wait` with the cancellation hold `submitAndWait` has.
   That is a 2.2.x item of its own once the sockets settle.
8. Whether DMD's runtime archive needs the same GC metadata
   materialisation for `ant_fibers`' arrays as it did for `GroupHost`'s.
