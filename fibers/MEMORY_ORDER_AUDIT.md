# Fiber scheduler ownership and memory-order audit

This audit covers the scheduler-to-Farm hand-offs, cross-thread fiber
resumption, waits, timers, cancellation, terminal publication, GC rooting, and
recycling. It describes the current implementation, not a general proof of
druntime or Ant Farm.

## Required invariant

At most one thread may execute a `FiberTask`, and every write performed before
the fiber suspends must happen-before the next thread calls that fiber. A task
may be in exactly one scheduler disposition at a time: running, publishable,
published in the Farm, waiting, terminal/completing, or explicitly recycled.

## Handoff edges

| Edge | Publishing operation | Acquiring operation | Status |
|---|---|---|---|
| spawn/reset → publishable | initialization, then MPSC predecessor-link release | `stealChain` acquire-load of link | Holds |
| selected lane → activation | lane pointer initialized before the lane-local MPSC release | lane drain/Farm acquire chain before resume or external requeue | Holds; fixed for generation |
| Farm write → published count | `notePublished` release-add after `farm.write` | pump acquire-load of `published`; `noteEntered` acq/rel before `Fiber.call` | Holds |
| last grain → coordinator | grain `outstanding` fetch_sub; completed-list CAS release | pump exchange-acquire of the list, then WaitSet wake | Holds |
| event/semaphore post → waiter | posted/permit release store or fetch_add, then WaitSet take | waiter insert-then-recheck, or PARK/WAKE fetch_add / MPSC ready chain | Holds |
| yielded stack → publishable | `fetch_add` phase delta to ready, then MPSC release link | MPSC acquire link; later ENTER/phase fetch_add | Holds |
| publishable → Farm | MPSC acquire, plain snapshot copy, Farm sentinel release-store | consumer sentinel acquire-load | Holds |
| Farm → executing fiber | sentinel acquire, then ENTER add and ready→running phase add | successful ENTER/phase adds precede `Fiber.call` | Holds |
| running waiter → early signal | wait-state release under stripe mutex; waker `fetch_add` WAKE | yielding worker `fetch_add` PARK, sees WAKE, publishes ready | Holds |
| parked waiter → signal | yielding worker `fetch_add` PARK | signal `fetch_add` WAKE, sees PARK, publishes ready | Holds |
| remote cancel submit → Director | `(slot,generation)` append under cancellation-request mutex | Director swaps the GC-visible request array under the same mutex | Holds; submitter never touches task control |
| local/Director cancel → terminal cutoff | current Fiber or sole Director `fetch_add`s counted CANCEL | exclusive owner `fetch_add`s running→terminating and classifies from that operation's returned old word | Holds; additive total order, no rollback |
| accepted cancel metadata → terminal | winning request writes generation tag/pending/lifecycle, then release-stores publication state 2 | terminal cutoff containing CANCEL waits with acquire-load for state 2 before final disposition | Holds; duplicate request cannot overwrite final state |
| timer/cancel → ready | timer mutex or wait stripe chooses one remover; Director CANCEL/WAKE add precedes removal | MPSC/Farm acquisition chain | Holds |
| terminal fields → completion | failure/disposition/outcome/phase-terminated fetch_add, completion MPSC release | completion MPSC acquire; terminal snapshot acquire loads | Holds |
| terminal fields → external join | terminated phase add, then manual-reset event set | event wake, then generation-stable acquire snapshot | Holds |
| managed-join registration → target completion | target-list gate acquire; waiting phase add and intrusive link before gate release | target gate acquire, terminal recheck/list detach, waiter WAKE add | Holds |
| timer/cancel → managed joiner | target gate chooses list remover; membership clear release | exactly one remover performs WAKE add / MPSC publication | Holds |
| lifecycle producer → control drain | record initialization, pending-count release, raw-head CAS release | Director raw-head exchange acquire, then ordered copy | Holds |
| lifecycle reservation → admission/event | admission mutex and reservation-count release add before admitted publication | event acknowledgement or terminal unused-slot acq/rel subtraction | Holds and bounded |
| completion → `drained` | completion push finishes before `activeCount--` | acquire-load of `activeCount==0` | Holds |
| remote cancel → release/reuse | Director drains queued generation pairs; synchronous apply returns before release; release rejects a nonempty queue | Director owns pool insertion; later request for an old generation is stale before task mutation | Holds without CANCEL rollback |
| release → reuse | Director ownership plus pool mutex protect freelist and reuse initialization | same Director, pool mutex, then ordinary ready publication | Holds under the documented no-use-after-release contract |
| join waiter → release/reuse | waiter-count CAS; release atomically closes admission and waits for zero | event reset only after closed/zero, then new generation opens admission | Holds |

The Farm edge depends on Ant Farm writing table contents before its release
sentinel and consumers acquire-loading that sentinel before reading the body.
The current Ant Farm implementation does both.

Park and pre-entry no longer CAS a combined run-state enum. One `ulong`
`controlWord` on the task's control line holds PHASE in the high half and
CANCEL/ENTER/WAKE/PARK addends in 8-bit fields. WAKE remains an externally
composed addend after WaitSet ownership arbitration. CANCEL has exactly two
possible direct sources: the currently executing Fiber and the domain's sole
Director. Arbitrary threads append a generation-paired request to the Director
queue instead of touching task control. Repeated remote requests coalesce by
accepted generation, so at most a simultaneously racing local/Director pair
can contribute two CANCEL addends. The unique Farm consumer `fetch_add`s ENTER,
PARK, and phase deltas. A whole-word store is legal only while the slot is
unpublished (construct/recycle).

PARK-then-WAKE and WAKE-then-PARK each produce exactly one ready publisher;
there is no intermediate signalled phase for a later callback to reject. ENTER
vs CANCEL on first resume is the same composition for won-before-entry. For
normal return, the old word returned by running→terminating is the immutable
cancellation cutoff: CANCEL-before-cutoff abrogates the result;
CANCEL-after-cutoff is counted but ineffective. The latter is not rolled back.
The Director operation which made the late add completes before Director-owned
release, and the current Fiber cannot add after its own terminal transition.

## Container ownership

- `queueNext` is shared only while a task belongs to a `PublishAccumulator`.
  A stolen chain is fully copied into a stack snapshot and detached before
  `farm.write`; this prevents a consumed prefix from reusing `queueNext` while
  flush still traverses an unpublished suffix.
- `waitNext` is used by `WaitSet` or the recycle freelist, never both for a live
  generation. `waitPrev` and the membership generation are WaitSet-only. Wait
  stripes and the recycle pool use distinct mutexes.
- `ConsumerView` and producer `Token` are persistent and worker-owned. They are
  registered and destroyed on that worker. Responders may hold additional
  remote views/tokens for covered lanes; those are registered and destroyed
  on the same worker.
- Remote cancellation requests live in a GC-visible `TaskHandle[]` protected
  by the domain cancellation-request mutex. Only the claimed Director swaps
  and applies that array. Handles are exact slot/generation pairs, so a request
  queued across recycle is rejected before any new-generation control access.
- `directorThread` is bound to the thread constructing `FiberDomain`. Applying
  queued cancellation, synchronous shutdown cancellation, and pool release
  from a different thread fail explicitly. Release also refuses to recycle
  while the request array is nonempty.
- Managed joins use raw intrusive task words in aligned manual controls. A
  target-list spin gate owns inbound links; a distinct cache line stores each
  waiter's outbound membership. Completion marks the whole detached list before
  releasing the gate, so cancellation/expiry cannot edit a chain being woken.
- `currentTask` is D TLS. It identifies the task executing on the current OS
  thread; it is deliberately rewritten for each resume. It is an internal,
  resume-local association, not a promise that TLS follows a Fiber. User or
  dependency TLS state carried across a suspension remains undefined behavior,
  and LLVM may cache TLS addresses across calls.

## GC visibility

Farm bodies and lifecycle queue heads contain raw integer pointers and are not
relied upon as GC roots. Before first publication, the domain registers itself
with `GC.addRoot`; `FiberDomain.roots` then holds every task across every
attached ready lane until its completion and all enabled lifecycle events have
been taken. Removing the final task removes the domain root. The task keeps its
druntime `Fiber` alive, and druntime registers the suspended fiber stack for GC
scanning. The forced migration test preserves an object referenced only from
the suspended stack across collections and thread changes.

The manually allocated 64-byte control records contain shared integer state,
padding, and (for external joins) DRuntime's OS event storage. They contain no
GC references and are deliberately not registered with `GC.addRange`. GC owner
objects retain all Fibers, delegates, exceptions, tasks, and queue references.
Raw managed-join words do not provide liveness: both target and waiter remain
in the backend roots array as active tasks until the membership is detached.

Lifecycle-enabled tasks reserve four record slots before their admitted event
is published. The aligned backend reservation counter bounds emitted records
plus every event kind which can still occur. Terminal classification returns
impossible future kinds; a copied record or successful Director handler return
acknowledges one emitted slot. A throwing handler moves the untouched suffix to
a GC-visible backend backlog without changing pending counts, reservations, or
assigned sequence, so its task root remains installed for retry.

Completion queue publication occurs before `activeCount` reaches zero, so a
controller observing `drained` can take every completion. Root swap-removal is
deferred until completion has been taken and the per-task lifecycle pending
count reaches zero. Both drain APIs allocate their returned copies and perform
final root retirement on the GC-enabled control lane.

## Findings and design wrinkles

### A. Raw task identity is recyclable; stable handles are generation-safe

`release` makes the same `FiberTask` object available to a later `spawn`.
Holding or using the old class reference after release can therefore observe or
submit control for an unrelated new generation. Assertions catch double-release
only while assertions are enabled and do not make a raw task reference stable
after reuse.

`TaskHandle` now contains exact slot/generation identity. Remote cancellation
submission and terminal inspection validate that generation; inspection also
validates again after copying atomic terminal data. A handle submission is a
control-lane request, not permission for its caller to mutate the recyclable
slot. `FiberDomain.cancelCurrent` is the only direct non-Director path and can
name only the task executing on that worker slice.

The Director serializes request application with completion release. A request
already queued must be drained before release; a submitter paused before its
mutex append may enqueue later, but its captured old generation is stale when
the Director next drains it and therefore cannot touch the reused control word.
External join registration holds a per-slot waiter count. `release` first
closes that registration gate, then waits for already-signalled joiners to
leave before pooling; a later spawn can therefore reset the reusable event
without swallowing an old generation's wakeup.

### B. Cancellation/terminal arbitration and exposure — fixed

Cancellation is a bounded counted signal, not an unrestricted public control
mutation. The current Fiber may directly request its own cancellation; all
handle-based requests enter a generation-paired queue and the sole Director
applies at most one remote request for that generation. Consequently CANCEL is
bounded to zero, one, or a simultaneously racing local-plus-Director count of
two without a contended task-word CAS.

The old word returned by the exclusive running→terminating phase add is the
linearization cutoff. If it contains CANCEL, the request won and terminal waits
for `cancellationEventEmitted == 2` before publishing immutable disposition and
outcome. If it does not, any later Director CANCEL add returns an old
terminating/terminated phase, reports `alreadyTerminal`, publishes no metadata,
and cannot change the captured result. No rollback is required.

Only the request which changes cancellation publication state from 0 to 1 may
write `pending` and emit the lifecycle record. A duplicate accepted request can
increment the bounded count but cannot overwrite a terminal disposition.
`cancellationRequested` uses the accepted generation tag after terminal entry,
so a counted-but-late Director add is not reported as an accepted request.

### C. Cancellation before first entry ran user code — fixed

A CANCEL add before ENTER on a never-entered ready task makes the Farm
callback complete the task without calling its Fiber. Because
druntime cannot reset the resulting HOLD Fiber, pool reuse replaces that rare
Fiber/stack rather than risking entry into the rejected delegate.

### D. Cancellation outcome masked another failure — fixed

Terminal classification now chooses failed for any uncaught `Throwable` other
than `FiberCancelled`, even when cancellation was requested. The request tag
remains independently observable and the cleanup failure is retained.

### E. Spawn racing shutdown could remove a recycled task from the pool — fixed

Spawn and shutdown now serialize through `admissionMutex`. An admitted spawn
performs pool dequeue, generation commit, root/accounting insertion, and first
publication before shutdown can close admission; a rejected spawn touches no
pool slot.

### F. Director-owned publication originally leaked through `wakeAll` — fixed

External events honored `directorOwned`, but a successful worker flush still
called `pool.wakeAll` directly. Publication now records a `published` wake event
through the same lane policy. Compatibility mode still broadcasts.

### G. Responder coverage was not validated — fixed

The MT hang fix retried while `active > waiting`, because `consumeNext=false`
can mean a Farm hole rather than emptiness. That heuristic is now a per-lane
published-but-not-entered count, so a responder parks when its home and covered
lanes have no ready or published work even if another uncovered lane is busy.
Production pumps use `consumeNext` so a visit finishes a table (or 5e-m yields);
`consumeQuantum` is only a one-resume test probe.

Managed start counts workers and responders per lane, and covering responders
count as sweepers. Pumps remain behind a coverage gate until the last selected
worker registers. A lane with no native or remote sweeper fails explicitly;
installed lanes with no sweeper reject later spawns.

### H. Director mailbox lacked a wake edge — fixed

`noteWake` now uses a CAS OR loop. The thread which changes the mailbox from
zero to nonzero invokes an optional `nothrow @nogc` function pointer and opaque
context; arbitrary GC-enabled handlers remain Director-side. A racing exchange
can make a call spurious, but any producer which subsequently observes zero
creates a fresh edge and notification, so dormant control cannot miss the next
nonempty epoch.

### I. Timer registry allocated and scanned — fixed

The wait stripe still decides exactly-once wake ownership between timer
expiration and cancellation. The registry is now an indexed binary min-heap:
nearest-deadline lookup is O(1), insertion/removal are O(log N), and expiration
pops one task at a time without allocating an output array. A repeated race
forces both ownership orders and free concurrency; the losing remover never
publishes a second activation or leaves a heap registration behind.

### J. Sanitizer evidence remains unavailable

DMD/LDC forced-migration and contention tests are the current evidence. The
standalone TSan-enabled druntime crashes inside libtsan on sequential fiber
migration, so TSan cannot presently validate these handoffs. This raises the
importance of deterministic race tests and the explicit state-transition
model; it is not evidence of a scheduler race by itself.

## Next implementation order

1. Add repeated forced-migration contention tests around the existing state
   machine and assert exactly one entry/completion per generation.
2. Define result-abrogation aggregation policy for groups.
3. Add performance coverage for large sleeper populations and shutdown races.
