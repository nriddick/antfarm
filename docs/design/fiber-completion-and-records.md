# Fiber completions, lifecycle records and hosting

**Implemented on `2.1.0-progress`.** The resulting behaviour is folded into
[the scheduler design](ant_fibers.md); this records the reasoning and the
choices made. Names below match the code: the pass is `serve`, its options
are a `FiberServicePass`, and record handlers take the record by value.
A `FiberDomain` is the opt-in to Fibers, and every domain carries lifecycle
records. Terminal tasks and records are retired together by one call, and
neither stream can hold a task back from recycling. Each host drives a domain
through one shared service pass instead of its own copy of the loop.

## Why

Today a terminal task stays rooted and unrecyclable until two independent
things happen: the Director takes its completion, and every lifecycle record
it emitted has been drained. The coupling exists because a record's storage
is a `LifecycleEventNode` owned by the task (`lifecycleNodes[4]`), linked into
the lifecycle MPSC. Recycling the task while a node is queued would let the
next generation overwrite a linked node. Consequences:

- `takeCompletions` allocates a result array per drain (the only remaining
  steady-state GC allocation in a spawn/complete/release cycle).
- `release` is fatal when records are pending, so callers must drain records
  first; a recycling completion handler would have to hold completions back
  behind undrained records (head-of-line).
- Records keep the task rooted, and with them its Fiber stack.
- Four GC node objects per task when records are enabled.
- `pendingEvents`, read by every pump, includes a Director-owned plain field
  (`lifecycleBacklogCount`), which is a data race.

Records are a monitoring and audit stream: an ordered, bounded, structured
history of admission, accepted cancellation, terminal outcome and failure.
They are the only per-request view of cancellations a pool's pump applied.
Nothing in a record needs the task object after emission; every field is
copied by value except the failure `Throwable`, which the record can own.

Separately, the service logic exists three times: the managed pump
(`fiberWorkerPump`), the FarmHost attachment (`attachFiberDomain`) and
`drainUntilEmpty`. Each polls timers, cancellation and triggers and flushes
in its own order. Adding retirement to each would make a fourth variation.

## Configuration

```d
struct FiberDomainOptions
{
    /// Record ring capacity; spawn reserves 4 records per task against it.
    size_t recordRetention = 16_384;
    /// Each terminal task, before it is recycled. Null recycles silently.
    void delegate(FiberTask) onCompletion;
    /// Each lifecycle record, in sequence order. Null discards them.
    FiberLifecycleHandler onRecord;
    /// Who may retire: the Director, or any thread servicing the domain.
    FiberRetirement retirement = FiberRetirement.director;
}

this(AntFarm* farm, FiberDomainOptions options = FiberDomainOptions.init);
this(FiberDomainOptions options = FiberDomainOptions.init);  // lanes attach later
```

`enableLifecycleEvents` and `lifecycleEnabled` are removed: records are part
of every domain. Handlers are configuration rather than call arguments, so
any host can retire a domain without knowing what the application does with
its tasks and records.

## Retirement

```d
/// Retire up to `maximum` records, then up to `maximum` terminal tasks.
size_t retire(size_t maximum = size_t.max);
```

- **Records first,** in sequence order, each passed to `onRecord` or
  discarded. A record is consumed only after the handler returns. If the
  handler throws, that record and every later one stay queued for retry with
  the same sequence numbers.
- **Then completions,** in FIFO order. Each task goes to `onCompletion` while
  it is still rooted and valid. The handler may read outcome, exception and
  cancellation disposition, and may spawn. It runs outside the admission lock
  and must not call `retire`.
- **After the handler returns, the task is recycled.** That task, and the
  `FiberTask` reference `spawn` returned for it, are then invalid, and any
  `TaskHandle` to that generation becomes stale (`current == false`). If the
  handler throws, the tasks already handled are recycled, and the throwing
  task and the rest stay queued.
- **Queued cancellation requests are drained first,** with the counted drain
  (no allocation). The current `release` precondition, which throws while any
  request is queued, is dropped. A request appended concurrently is safe,
  because application revalidates the generation under admission.
- **Locks are taken once per batch:** root removal and pool insertion take
  `rootsMutex` and `poolMutex` once, not once in take and again in release.
- **External thread joins keep their gate:** recycling still waits for
  `TaskHandle.join` readers of that generation to unregister.
- **One retirer at a time:** a gate admits one retirer. A loser returns 0
  immediately; the winner's pass covers everything published before it began.

`FiberRetirement.director`, the default, keeps today's ownership rule: only
the constructing thread retires, so a `FiberTask` stays valid until the
Director's own `retire` returns. Handlers may be arbitrary GC code.

`FiberRetirement.serviceThreads` lets every service pass retire. That suits
fire-and-forget work on a managed pool, where no thread would otherwise drain
and admission would hit backpressure. A task can then be recycled at any time
after it terminates, so applications must identify tasks by `TaskHandle`.
Handlers run on whichever service thread wins the gate, one at a time. They
must not block, and a throwing handler fails that worker's pump.

`takeCompletions`, `release`, `releaseAll`, `takeLifecycleEvents` and
`handleLifecycleEvents` are removed. Nothing else hands out terminal tasks, so
`release` has no legal input left, and its misuse fatals (null, foreign,
live, pooled, rooted) disappear with it. `releaseAll(takeCompletions())`
becomes `retire()`.

## Lifecycle records

### Storage

The domain constructor allocates a ring of
`capacity = nextPow2(recordRetention)` slots:

```text
records  : RecordSlot[capacity]  NO_SCAN   { shared ulong stamp; record fields by value }
failures : Throwable[capacity]   scanned   one reference per slot, usually null
head     : consume index, owned by the retirement gate's holder
tail     : shared, fetch_add by emitters
budget   : shared, reserved-or-occupied slot count (the backpressure counter)
```

Record data lives in an unscanned block, so the ring does not add to every
collection's mark work. Only the parallel `failures` array is scanned, at 8
bytes per slot. A record's `TaskHandle` holds a `FiberTask` pointer in that
unscanned block. That is safe because the domain retains every task object
in its roots or recycle pool for its whole lifetime and never frees one. The
handle is generation-checked, so a record naming a recycled task simply
reports `current == false`.

### Admission reservation

Backpressure stays at admission, where it can throw:

- `spawn` adds `lifecycleRecordsPerTask` (4) to `budget` under the admission
  mutex, throwing `FiberLifecycleBackpressure` if that would exceed
  `recordRetention`.
- Emission never fails or blocks: the reservation guarantees its slot.
- `finishTerminal` returns the unused part of the task's reservation
  (`4 - emitted`, computed from the outcome and whether a cancellation record
  was emitted). After that the task holds no reservation, so it is
  recyclable regardless of record state.
- Retirement subtracts each consumed record from `budget`.

This removes the per-task `lifecycleReservations`, `pendingLifecycleEvents`
and `completionTaken` fields and the four node objects.

### Emission (any thread, `nothrow @nogc`)

```text
t    = atomicFetchAdd(tail, 1)
slot = records[t & mask]
while (atomicLoad!acquire(slot.stamp) != t) pause();   // slot free for lap t
write the record fields; failures[t & mask] = failure
atomicStore!release(slot.stamp, t + 1)                  // published
```

This is the per-slot stamp scheme of a bounded MPMC queue, used with a single
consumer. Slot `i` starts with stamp `i`. Retirement consumes ticket `h` when
its stamp is `h + 1`. After clearing the slot it stores stamp `h + capacity`
(release), marking the slot free for the next lap, and only then releases the
record's `budget`. The emitter's acquire of the stamp orders the clearing
before the emitter's writes; a counter alone would only bound occupancy, not
order the memory.

Reservations make the wait effectively vacuous. `budget` never exceeds
`capacity`, so at most `capacity` tickets are outstanding, and the slot's
previous lap has been consumed by the time a ticket reaches it. The loop
covers only the instant between that consumption and its stamp store. There
is no full check, no failure path and no CAS loop. A debug build asserts
that the wait stays short.

`sequence` is `t + 1`, assigned at emission, so it is stable across handler
retries without a drain-time assignment step.

Per-task order is preserved by construction:
- admission emits before the task is published;
- an accepted cancellation emits before `finishTerminal` can proceed, which
  already waits for `cancellationEventEmitted == 2`;
- terminal precedes failure on the same thread.

### Consumption

Retirement stops at the first slot whose stamp is not `head + 1`. That slot
belongs to an emitter between its `fetch_add` and its publish, and retirement
does not spin on it. After the handler returns, the slot's fields and its
`failures` entry are cleared, so the ring does not retain exceptions. The
stamp is then advanced a lap and `budget` is decremented.

`pendingEvents` becomes `tail - head`, read with atomics, which removes the
data race on the backlog count. A handler that wants to keep records copies
them; storage that keeps `failure` must be GC-visible.

### Ordering between the two streams

A task's terminal record is published before its completion is pushed, both
in `finishTerminal`, on the same thread. `retire` drains records before
completions, so it usually delivers a task's terminal record before its
completion. The exception is a record published after its record phase
began; that record arrives in the next `retire`. A record may outlive its
task. The handle inside it goes stale when the task is recycled, but the
outcome, disposition and failure it carries are its own.

## Hosting a domain

### One service pass

```d
struct FiberServicePass
{
    bool responder = true;     // polls timers, reports the deadline
    bool consumedWork;         // the caller just consumed: flush only full batches
    size_t flushBatch = 32;
    uint avgCost;              // Farm cost class of a flushed table
}

struct FiberServiceResult
{
    bool progress;             // something applied, woke, flushed or retired
    size_t flushed;            // ready tasks published
    long deadline = long.max;  // earliest timer (responders)
}

/// One pass by a thread holding `token` on `lane`'s Farm.
FiberServiceResult FiberReadyLane.serve(ref Token token,
                                        FiberServicePass pass);
```

The name `serve` avoided a clash with the `service` property of the
since-removed `FiberLane`. `FiberDomain.serve` forwards to a one-lane
domain's lane.

In order, one pass:
1. drains queued cancellation requests (counted);
2. if `responder`, polls timers;
3. delivers generation triggers;
4. flushes the lane's ready accumulator with `token`;
5. retires, if `retirement == serviceThreads`, or if this thread is the
   Director and anything is pending;
6. reports the earliest deadline when `responder`.

It never consumes Farm payloads; consuming is the host's job, because hosts
differ in which views they own. A host's loop is then: `serve`, consume,
and park until `deadline` when neither made progress. The managed pump keeps
its remote-edge visits around this pass, but no longer has its own copy of
the steps.

### Per host

| Host | Who consumes | Who calls `serve` | Director retirement | Parking |
| --- | --- | --- | --- | --- |
| Caller-driven (`drainUntilEmpty`, custom loops) | the caller's view | the caller, as responder | the caller is the Director | the caller's own |
| `FarmHost` (`attachFiberDomain`; with workers, itself a `GroupHost` layer) | pool workers on the host Farm | the control thread in each `step()` (emit), as responder | the control thread, which must have constructed the domain; `attach` checks | control thread steps; workers park as the host configures |
| `GroupHost` (`FiberGroups`) | the host's view, or a consuming attachment's (the dispatcher) | every worker's managed pump, as responder | the Director calls `retire`, or the domain uses `serviceThreads` | `until(deadline)` from the pump |

Today `FarmHost` holds completions until `close()`, and its `finish` releases
them. Under this design `step()` retires every pass, so the ring and roots
stay bounded while the host runs. `finish` retires whatever remains after the
consumer join. The `drained` precondition is unchanged.

`GroupHost` hosts Fibers through `ant_fibers.groups.FiberGroups`. The pool
makes the raw `WorkerBody` lane (`@nogc nothrow`) and the managed lane
mutually exclusive, and `serve` is GC-enabled and throws, so a Fiber
attachment selects the host's managed lane, which already carries the
park-until-deadline channel (`ManagedPumpResult.until`). The host picks the
lane at start, so a host with only the dispatcher attached keeps the raw
body and its measured hot path. `FiberGroups` builds one lane per Farm in
its `install` hook and serves the worker's home lane in `serve`; see
[the threadpool design](threadpool.md#group-host).

## Costs and limits

- Ring memory is paid when the domain is constructed: about 64 bytes per slot
  unscanned, plus 8 bytes scanned. The default of 16,384 records is about
  1.2 MiB, of which 128 KiB is scanned. Today's cost is four GC objects per
  task, and only when records are enabled.
- A task holds its 4-record reservation from admission until it terminates,
  so retention bounds **live plus unretired** tasks at retention / 4: 4,096 by
  default. This cannot be removed without making records lossy, because a
  live task's terminal record must never fail to emit. The default was set
  at 16,384 for that reason (an earlier draft's 4,096 capped a domain at
  1,024 concurrent fibers). Reserving 2 per task is the follow-up if that
  bound matters: fold the failure record into the terminal record, and
  reserve a cancellation record when a request is accepted, since every
  cancellation entry point already returns a result that could report
  backpressure.
- An application that never retires meets backpressure after 4,096
  unretired tasks, instead of unbounded root growth. Every host in the table
  retires, except a managed pool in `director` mode, whose Director must call
  `retire`. Its documentation and the wake event say so.
- `tail` is one contended line shared by all emitters, with one emission per
  spawn and per terminal event. This cost is now paid by every domain;
  measure it on the cold spawn path.
- The 4-record worst-case reservation per task is unchanged; see above for
  the 2-record follow-up. Claiming optional records at emission instead
  would need a fallible emission path, which emission must never have.

## Validation

- Port the lifecycle smoke cases to the handler API. These cover:
  ordering, failure retention, the fatal-`Error` path, retention and
  backpressure accounting, backlog retry, and stable sequences.
- New schedules (unittest seams, as in `wait_races.d`):
  - retirement that meets an unpublished slot stops and resumes on the next
    call;
  - a record whose task was recycled reports a stale handle and keeps its
    own failure;
  - a handler that throws mid-batch, in each stream;
  - two threads racing for the retirement gate under `serviceThreads`.
- Multi-thread stress: workers terminating tasks while the Director spawns,
  cancels and retires, at a small retention near backpressure, in both
  retirement modes. Check that sequences are contiguous with no gaps or
  duplicates, that per-task order holds, that every reservation is returned,
  and that `budget == 0` at quiescence.
- Hosts: `FarmHost` step retirement and close; the managed pool in both
  modes; the caller-driven drain.
- A GC-on run that holds failure records across collections, plus the fiber
  TSan lane.
- Measure a spawn/complete/retire cycle against the current code (records
  off), plus the `tools/perf/ant_fibers` benchmarks and the warm Fiber
  experiment's FiberDomain mode.
