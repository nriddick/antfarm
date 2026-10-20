# Private actor ordering

This page describes the private path's publication, nested-credit, outcome,
cancellation and final-access reclamation protocols in
`antfarm/actors/private_actor.d`. The definitional symbols and the shared Farm and
public-actor notation are in
[public-actor-ordering.md](public-actor-ordering.md#atomic-objects-and-notation);
the cohort layout invariants and the slab-reuse proof are in
[private-actor-layout.md](private-actor-layout.md). The user-facing rules are in
[../guide/private-actors.md](../guide/private-actors.md) and
[../guide/outcomes-and-cancellation.md](../guide/outcomes-and-cancellation.md).

Private roots use a 32-bit root `pending` counter that counts physical-table
custody plus direct-wave notification tails. The common Entry overlays this root
metadata with the child's enclosing pointer; size and offset asserts preserve its
40-byte layout.

## Root child-join ordering

```text
child plain result writes
  -> Shc completion RMW (acq_rel) -> Tprogress RMW (acq_rel)
  -> wave table/descendant credit releases (acq_rel)
  -> root.pending final decrement (acq_rel)
  -> rootReturns successful push CAS (release)
  -> emitter detach exchange (acq_rel)
  -> next Farm publication (release) -> consumer acquire
  -> resumed root reads joined external results
```

Root state/disposition writes independently join through the original physical
table and the Batch-return release CAS / emitter acquire exchange before custody
is released. The unique final root decrement therefore acquires both the root
callback and all child notifications, whichever arrives last. `again` keeps
custody; waits release it on collection. The final publisher's successful CAS is
its last Entry and Service access. Nonfinal decrements perform no subsequent
Entry access. The acquiring emitter can reset `pending = 1` with a raw store: all
foreign credits are gone, and ownership has transferred through the queue.

A wave finisher snapshots parent/upstream pointers before release-publishing idle
as its final cohort access. The saved root credit keeps Entry and Service alive
through the post-idle tail. `finished(context)` acquires child results and
permits cohort reuse, but cannot by itself authorize root Entry reuse or park
destruction. Collection drains `live` only after the notification tails join.

Root retention uses checked CAS because the packed 32-bit counter can exhaust;
reject `uint.max` before incrementing. An existing custody hold prevents a
concurrent transition to zero during a legal retain. Existing 64-bit wave
fetch-adds remain acquire/release: their outstanding table/wave obligations are
bounded by simultaneously resident storage and outstanding notifications, not
lifetime-total starts. All releases remain acquire/release.

The raw empty-list hint supplies no publication edge; observing empty only
defers a concurrent wake to a later collection. The acquire exchange, not the
hint, makes the detached links and joined results visible. Reversing each list
preserves its successful-push FIFO order; appending at collection gives no global
completion-order guarantee. The state machine and lost-wake argument are below.

## Private state destructor visibility

Stored-state destruction runs inside the final consumer dispatch, sequenced
after the user callback and any cohort-owned result write. Disposal is
post-order: `disposeChildren` acquire-loads every cohort's idle publication
before reading or destroying its members. The existing tracked-table and
wave-credit acquire/release joins therefore carry each member's last state writes
to its destructor, including members last run on another consumer.
`finishAfterChildren` uses the existing joined wake publication and consumer
cleanup activation; it does not execute a destructor in emitter collection.
Under emitter reclamation, the consumer destroys the stored instance before the
table/wake handoff, and collection frees only its raw allocation. GC range
registration remains present through destruction and is removed only at byte
free. This rooting is separate from the visibility argument.

## Private child results

Typed cohorts store each operation's return value in `results_[i]`, where i is
its participant position (spawn order for a whole wave, selection position for a
subset). Continuations write only on their final `done` activation. The store
precedes callback return and member retirement. No new atomics or fences are
needed: the plain slot write joins through Shc and Tprogress acquire/release
RMWs, table and descendant wave credits, and the final wave credit's
acquire/release decrement. The finisher release-stores `active = 0` as its final
cohort access.

Polling roots and child-level parents acquire that idle store through
`finished(context)` or `results(context)` before reading the slots. A root
resumed through `waitChildren` additionally acquires the root pending decrement,
rootReturns publication, emitter detach, Farm publication and consumer acquire
chain above. `results` still acquire-loads active to diagnose an early read.

Parent reads precede the next `start` enqueue release CAS. The emitter's
acquire/release detach, Farm release publication and consumer acquire order those
reads before the next wave's slot writes. The idle acquire in `start` also orders
successive writing generations. `resultCount_` is parent-only and set before
enqueue; children read neither that count nor any slot. The slot pointer is
published through enqueue. The finisher never updates result bookkeeping after
publishing idle, so a polling parent can read and dispose the cohort while the
saved root notification credit is still in flight.

## Private wave failure aggregation

`context.fail` on a member elects one plain `(code, Entry)` writer with a raw
checked CAS on `Wave.failures`. The CAS needs atomicity only. Reaching `uint.max`
is fatal before the store, so a wrap cannot observe zero and elect a second
writer. The winner's plain code and Entry stores are sequenced before the
callback returns. They join through the same chain that carries result slots:

```text
member fail RMW + winner's plain code/Entry stores (sequenced before callback return)
  -> Shc completion (acq_rel) -> Tprogress (acq_rel)
  -> table/descendant releases (acq_rel)
  -> final wave decrement (acq_rel) -> finish: active.store(0, release)
  -> parent outcome(): active.load(acquire) -> plain reads
```

Every cohort callback, including a continuation activation, runs under a wave
table credit, so every report precedes the join. `outcome` then maps the winning
Entry to a participant position: the spawn index for a whole wave, or
`sel_[capacity + index]` for a subset. The pair is consistent. Which concurrent
report wins is unspecified. `failures` counts reports, not distinct members.

The next nonempty `start` or `startSubset` resets the count, the cancel flag, the
winning Entry, and the participant counters only after rejecting an active,
retiring, or empty start, and before its enqueue release. That start's idle
acquire happens-before the reset, so no writer from the previous generation
remains. `firstFailureCode` is valid only when `failures != 0`. The emitter may
lower `dispatched` on truncation.

## Private in-tree cancellation

Cancellation adds no operation on `root.pending`, `childPending`, `W.pending` or
any return list, and no wake source. The root/child credit proofs quantify over
the tables a wave publishes and the activations they run. Cancellation changes
only *which* tables exist: a prefix, or cleanup tables that take the same
per-table credit and rollback. The lost-wake and double-wake arguments therefore
carry over. The new obligations are below. R7, R8, R10 and R11 concern root
records and are in the next section.

Writers set a flag with two idempotent release stores: `W.cancel = true`, then
the root Entry's `subtreeCancel = true` (byte 35, formerly padding).
`scopeCancelled(W)` acquire-loads the summary byte and, only if it is set, walks
up: acquire-load `w.cancel`; stop when the cohort's parent is the root; otherwise
continue at `parent.enclosing`. The summary is a sticky fast-path hint, not an
aggregate join. Acquiring one writer's summary store orders that writer's flag
only; the per-wave acquire loads plus advisory semantics make that sufficient.

| # | Race | Argument |
| --- | --- | --- |
| R1 | In-tree cancel vs the emitter emitting W | Flags are atomic and advisory. The emitter sees the flag at a table boundary and truncates, or it does not and publishes the table, whose members may observe the flag later. Both are valid executions of the cancel protocol; no credit depends on the observation. |
| R2 | Flag reset at `start` vs a late writer | Writers are the parent (in its own activation, program-ordered before its next `start`) and members of W (`cancelPeers`, under a W table or resume credit). W's join precedes `start`'s idle acquire, so every member write happens-before the reset. A parent store onto a wave that went idle after its `active` check is also before the reset; it only makes `outcome.cancelled` true for a fully dispatched wave. |
| R3 | Truncated seal vs P lifetime | The emitter holds P when it checks. It stores `dispatched_` **before** `releaseWave` (the seal), which stays its last park access, including through the `Service` traversal that snapshotted `next`. A retiring wave keeps P until its cleanup tables cover `cursor_ == count_`. |
| R4 | `dispatched_` visibility | Emitter plain store -> seal RMW (acq_rel) on `W.pending` -> later RMWs in its modification order -> final decrement (acq_rel) -> `finish`, which reads it for the debug prefix check -> `active.store(release)` -> parent acquire in `outcome`/`results`. The same chain already carries consumer writes; the seal's release gains a new use, not a new edge. |
| R5 | Truncation with a scheduled resume descriptor | The resume descriptor has its own P', independent of the initial seal; an early seal with suspended members behaves like a normal seal with suspended members. The cleanup callback is selected locally, because `emitResume` publishes the wave's regular callback; Farm copies the function pointer into the table during `write`. Cleanup covers `[cursor_, count_)`; resumable members come only from the written prefix. |
| R6 | Cancel vs a suspended continuation or waiting root | No early wake exists. Resumption remains the unique final decrement of `childPending`/`pending`, and the flag is read only inside the resumed activation. |
| R9 | `subtreeCancel` reset at spawn vs in-tree writers | Every in-tree writer runs under a credit that precedes the root's final `pending` decrement or its table return. Collection acquires both before `reclaim`; the raw reset in `spawn` follows on the same emitter. |
| R13 | Ancestor reads during a walk | Each ancestor wave and parent Entry is pinned by the walker's credit chain: the emitter's P or a consumer's table credit on W, W's credit on its upstream or on the parent member's counter (whose H is taken before custody release), and so on up to `root.pending`. `ChildBase.parent`, `ChildBase.root` and child `Entry.enclosing` are immutable while pinned. The `parent is root` test precedes any read of the root union. |

An undispatched member of a truncated `startFinal` wave has idle children: the
previous wave's join included all descendants, every exit path cleared its
continuation flags, and it holds no H. Its cleanup table therefore runs
`retireMember` (post-order disposal, `~T`, free) on a consumer without the
operation. It does not touch `childPending`; a dormant one from an earlier
immediate `done` is not an obligation.

`subtreeCancel` is a one-byte atomic beside plain root bytes at offsets 32–34.
Those are distinct memory locations; TSan shadows by offset and size.

## Private root tracking

A tracked root park has one 32-byte record per slot. Its `control` word,
`generation:48 | cause:8 | flags:8` (live, cancelled, armed), is the only
location another thread touches, and only through `requestCancel`. The other
record fields are plain: `deadline`, `failed`/`failureCode`, `status` and the
immutable `index`. Tracking adds no operation on any join counter and no wake
source, so the credit proofs above are unchanged.

```text
VACANT(g)   --spawn (emitter: deadline/failed/code, then control.store(g|live[|armed], release))--> LIVE(g)
LIVE(g)     --requestCancel / cancelAll / observed deadline (CAS acq_rel from a live word)--> LIVE(g)|cancelled|cause
LIVE(g)*    --terminal collection (emitter CAS loop clears live; status from the replaced word)--> FINISHED(g)
FINISHED(g) --takeOutcome (emitter: control.store(g+1, release); slot to the free list)--> VACANT(g+1)
```

| # | Race | Argument |
| --- | --- | --- |
| R7 | Handle cancel vs terminal collection | Both are CASes on `control`; modification order linearizes them. Cancel first: the emitter's CAS fails, re-reads the cancelled word and records `cancelled` (unless `failed`). Collection first: the canceller's CAS fails, re-reads a non-live word and returns `finished`. `requested` never pairs with `completed`. |
| R8 | Handle cancel vs take and respawn (stale handle) | Cancellers CAS only from a live word of their own generation. The emitter's plain stores happen only from non-live words (FINISHED to VACANT(g+1), VACANT to LIVE), which no canceller writes, so neither store can overwrite a successful cancel. Every emitter transition runs on the one emitter after its own checks. A canceller holding a stale LIVE(g) fails its CAS and then sees another generation. `takeOutcome` is fatal at generation 2^48−1, before the store would wrap, so a stale handle never matches a later generation. |
| R10 | Observed deadline vs handle vs `cancelAll` | All set the same bit by CAS from a live word whose cancelled bit is clear, writing the cause with it. The first fixes the cause; the others see cancelled. Terminal collection keeps the bits and only the generation-advancing take clears them. In-tree observers know the root is live: a root context runs under custody, a member under a table credit that chains to the root's join, and the emitter's `truncated` check under the wave's producer hold. Debug builds assert it. |
| R11 | Root `fail` fields vs the emitter read | Plain writes in a root activation; activations are serialized by custody. The leaf-finish path orders them by the Batch return (release CAS) and the emitter's acquire exchange; the joined path adds the root's release RMW and the `rootReturns` push and exchange. Spawn resets them before publication. |

The canceller's CAS is acq_rel and every in-tree check starts with an acquire
load, so writes made before a successful `requestCancel` are visible to any
activation that observes `cancelled`. The release stores at spawn and take
publish nothing a canceller reads; consumers read the plain fields through table
publication. They are kept for clarity and cost nothing on x86.

**X1a.** A terminal root's activations happen-before `takeOutcome` returns it on
the emitter: the same R11 chain reaches collection, and `takeOutcome` follows
collection in emitter program order. Application storage written in the final
activation (typed outputs outside the library) is therefore readable on the
emitter after the take, and on another thread after ordinary synchronization with
the emitter. This replaces "read after `live` falls" with a per-root edge.

`deadline` is written only by the emitter at spawn, before publication; consumers
read it while the root is live, and `cancelAll` reads it on the emitter. The
clock (`MonoTime.currTime`) is read only for an armed word without the cancelled
bit.

## Private child continuation ordering

A continuation member owns 32-bit `childPending` custody during its activation.
Every wave it starts chooses that counter as its immutable notification target. A
lazy hold H pins its enclosing wave W from the first suspension/draining
activation until completion. Initial and resume tables both retain W credits; the
emitter holds P while a park's resume descriptor is scheduled.

Let `c` count activation custody plus waves started by this member; `H` is its
activation hold on enclosing wave `W`, and `P` is the resume descriptor's
producer hold. Every initial or resumed callback also has a tracked W table
credit. Completion allocates nothing.

```text
IDLE -> RUNNING (c := 1)
RUNNING -- done, c == 1 --> COMPLETE (retire inline if terminal)
RUNNING -- wait / done with tails --> take H if absent; release custody
    -> SUSPENDED(wait) / DRAINING(done or doneRetire)
unique final c decrement:
    wait       -> RETURN_QUEUED
    doneRetire -> RETURN_QUEUED(cleanup)
    done       -> clear flags; release H -> COMPLETE
RETURN_QUEUED -> emitter FIFO (take P) -> W-credited resume table
    -> RUNNING / CLEANUP (no user call)
final resumed done / cleanup -> clear flags; release H -> COMPLETE
FIFO fully published -> clear scheduled flag; release P (last park access)
```

```text
descendant writes / result slots
  -> Shc completion (acq_rel) -> Tprogress (acq_rel)
  -> descendant wave table/descendant releases (acq_rel)
  -> descendant finish: active.store(0, release)
  -> member childPending.fetchSub (acq_rel)
  -> childReturns push CAS (release; last Entry and Service access)
  -> emitter detach exchange (acq_rel)
  -> resume table publication (release) -> consumer acquire
  -> next activation reads joined descendants and prior member state
```

The activation's own state, step and flags precede its custody decrement. The
unique final decrement acquires both that release and all descendant releases,
whichever side arrives last. Children cannot take the count to zero while custody
is held. No new retain occurs without running custody. Nonfinal releasers never
access the Entry again; final owners either push a return or clear flags and
release H as their last access. This is the lost-wake and exactly-one-resumption
argument.

The exclusive reset `childPending := 1` is raw. A resumed activation follows the
previous final owner's queue handoff. A new wave follows W's prior join and its
parent's idle acquire. The immediate-done path can leave dormant one: its acquire
load of one, after every retain, proves all notification decrements are finished.
Neither path has another writer at reset.

`ResumeBodies` keeps its own cursor. `prepareTableFrontN` detaches only the
accepted prefix and clears the tail on full drain before publication. Farm's copy
advances the independent cursor before visibility; the emitter never reads an
accepted Entry's `next` afterward. Zero admission leaves the FIFO unchanged and
rolls back only the table credit. P covers all post-write park access and is
released last. H keeps suspended members, ancestors and Service alive.

Debug builds enforce empty FIFO head/count/tail equivalence and one result write
per participating slot. Result write flags share the existing allocation (one
extra byte per capacity slot in debug, none in release). Counter overflow and
underflow, wrong contexts, early result reads, and strict root finish with
suspended children are fatal; no detection is promised for a member that never
returns `done`.

Final activation writes, including result slots, reach the parent through its
tracked table's Shc/Tprogress/W releases. For done with outstanding tails, the
member's final owner also carries the child join into `releaseWave(W)`. The last W
decrement acquires all releases, publishes idle, then releases the snapshotted
root/member/ancestor target. After idle, the finisher touches only those
snapshots; a parent may reuse or dispose the child cohort immediately.

Parent-written header, retirement flag, subset positions and result count precede
start's incoming release CAS. The emitter acquires them before initial emission;
resume emission is later on the same emitter. H and P prevent W from joining
while a return is queued or a descriptor is still scheduled. Taking H inside the
callback is safe because its table credit cannot drop before return. P is cleared
and released only after the FIFO is empty, as the final park access. Service
traversal snapshots descriptor `next` before calling emit and does not
dereference a sealed descriptor afterward.

Return-link ownership transfers through the release CAS and acquire exchange. The
emitter reverses returns, groups them by park, and maintains an unpublished FIFO.
`ResumeBodies.save` copies its independent cursor; prepare detaches the accepted
prefix without moving that cursor, and clears tail on full drain. Farm sizes,
prepares and copies the range before publication. **Reading an accepted Entry's
`next` after publication is not an ordering edge and is forbidden:** its next
activation can already be writing a new return link. Zero admission transfers
nothing and rolls back its table credit under P.

A terminal done with tails becomes `doneRetire`. Even if the emitter's seal is
the final owner, it queues a cleanup activation instead of running a destructor.
The consumer cleanup acquires the chain above, disposes descendant cohorts
post-order, destroys state, then drops H while its table still fences return. The
result write precedes inline or deferred retirement. Both root reclamation
policies preserve this child destructor/free placement.

Debug result-written bytes follow the same publication and W-join edges as their
slots; each participant owns a distinct byte. They add no release-build work. The
child-return hint is a raw load that may delay discovery until the next pump; the
exchange supplies the acquire. Emitter wake remains outside this protocol.

## Publication and reclamation

Root entries have no admission/exit CAS. An untracked park has no public
generation or handle; a tracked park's handle names a generation in a separate
per-slot record, which only cancellation and outcome delivery use. The sole
emitter consumes entries from a FIFO, snapshots at most its configured batch
limit of entry addresses into a detached batch, and publishes one-word
single-shot Farm payloads. The Farm's primary claims provide exclusive dispatch.

Only the accepted prefix changes custody. The emitter restores the unaccepted
suffix at the front of the ready FIFO, ahead of entries it has not tried to
publish. The suffix retains its original links; only its last entry's link and
the FIFO head/tail need updating. These entries remain emitter-owned. No
accepted entry is inspected during this restoration, including when the whole
batch was accepted. Preserving this order prevents partial quota admission from
progressively permuting the ready/free lists across reuse.

The emitter does not dereference accepted actor entries until their completion
returns custody. With consumer reclamation, callback completion can free the
actor before `write` returns. Root state and entry metadata are separate allocations,
so accepted-pointer source storage stays valid. A terminal callback destroys
stored state after disposing its descendants. By default, the table notification
returns metadata to the emitter, which frees the raw bytes without inspecting
state. Allocation and reclamation therefore occur on the same emitter. The
optional third template argument `PrivateRootReclamation.consumer` frees state
inside the terminal callback instead. This is a measured policy choice:
simultaneous remote frees substantially hurt the small-allocation workload on
this host. Child terminal waves still free members on their executing consumers.

This FIFO rule governs admission, not callback execution order across consumers.
The emitter acquires completed batch metadata and appends `again` roots to the
ready FIFO. For a wait or deferred retirement it releases table custody and
recycles the Batch immediately, even while the root sleeps. Other roots can use
that Batch. A slow callback still delays peers in its original physical table.
Joined roots go to the back of the ready FIFO when their wake is collected. Each
detached wake list preserves FIFO order of successful publication; there is no
global completion-order guarantee across wake lists, table returns and newly
spawned roots.

A child cohort has one active wave at a time. The parent's callback sets it active
before enqueueing its metadata. The emitter holds a producer credit until
publication is sealed; every table receives a credit before it is published, with
rollback on zero admission. A descendant wave takes one credit before becoming
visible: on its enclosing wave for a plain/result member, or on a continuation
member's own join counter. Consequently callback return alone cannot release an
ancestor's execution authority.

Wave completion joins all table and descendant credits with acquire/release RMWs.
The last finisher snapshots the ancestor and parent pointers, completes metadata
updates, and release-publishes idle as its final cohort access. It then releases
the saved ancestor-wave credit or the parent's root/member join credit. The saved
credit pins the upstream metadata while the cohort can already be reused or
disposed. Notifications never borrow actor state or a producer token.

The Farm snapshots a completion hook's function and context before invoking it
and never dereferences either afterward. A hook may therefore transfer or free
its storage as its final access. Root return publication and final wave release
rely on this exact rule; observing a flag earlier in a callback would not justify
freeing storage that callback still uses.

## Root join state machine and lost-wake argument

Each root owns one scheduling/table-custody credit while ready, running or
awaiting physical-table collection. Each nonempty direct wave retains one
additional credit before enqueueing; descendants aggregate through their existing
enclosing-wave credits:

```text
pending = custody (0 or 1) + outstanding direct-wave notification credits

FREE -> READY (pending = 1) -> RUNNING -> TABLE_HELD
TABLE_HELD -- again --> READY (keep custody)
TABLE_HELD -- waitChildren / finishAfterChildren --> release custody
    remaining child credits -> WAITING
    zero credits            -> RETURN_QUEUED
WAITING -- last child credit --> RETURN_QUEUED
RETURN_QUEUED -- emitter collection --> READY or CLEANUP_READY (pending = 1)
CLEANUP_READY -- internal consumer activation --> TABLE_HELD (finish)
TABLE_HELD (finish) -- collection and any notification tails joined --> FREE
```

The collector releases custody only after acquiring the physical table, as its
last Entry access. Every decrement is acquire/release. Only the decrement from one
to zero publishes the root to the service's MPSC wake list; its successful
release CAS is its final Entry and Service access. The emitter acquire-detaches
and reverses the list, restores custody, and publishes through its normal Farm
path. Strict leaf finish keeps its direct reclamation path without a new RMW.

If children finish first, custody keeps `pending` at one until table collection;
the collector then owns the wake. If collection happens first, the last child
notification owns it. Concurrent releases have exactly one old-value-one owner.
Only a running root with custody can start a direct wave, so no new credit can
appear after zero. Idle flags alone cannot replace these credits: a wave may
publish idle and pause before its notification, including across earlier `again`
generations. The counter pins the root Entry and Service through that tail even
if the root state has already been freed under consumer reclamation.

The common child Entry remains 40 bytes. Root-only disposition, flags and a
checked 32-bit counter occupy the child enclosing-pointer union; static asserts
verify size and offsets under LDC and DMD. Existing 64-bit wave increments retain
their acquire/release fetch-add operations. No waiting-root scan is required.

## Farm completion prerequisite

Plain callback writes must join within each shard before the shard finisher joins
`Tprogress`. The completion RMW on `Shc` is acquire/release; claim increments
remain relaxed. Acquire/release on `Tprogress` alone does not acquire other
consumers' writes within the same shard. The two-consumer test forces separate
runs in a single shard and has the final table hook read both consumers' plain
writes, with no intervening test-side release. Reverting only that RMW produces a
ThreadSanitizer race; the corrected implementation passes. The detailed proof is
in [public-actor-ordering.md](public-actor-ordering.md#within-shard-completion-join).

## Non-edges

- Observing `context.cancelled` false is not a guarantee that no cancel was
  requested. `outcome.cancelled` true does not mean any work was skipped.
- `dispatched` is not readable before the join; the emitter may still lower it.
- `requestCancel` returning `requested` does not mean the root has observed it;
  it means the outcome will be `cancelled` or `failed`. `finished` does not mean
  the outcome was taken.
- `PrivateRootHandle` copies are plain values; publishing one to another thread
  needs ordinary synchronization. `requestCancel` after the park's `destroy`
  uses freed memory: join every canceller first.
- `takeOutcome`'s visibility edge reaches the emitter thread only. `live`
  falling on a tracked park is not a per-root edge for another thread.
- A result slice used after `start` or `startFinal` is a race with the next
  wave; it must not escape the authorized callback.
- A rejected start preserves the last nonempty wave; an empty or void start does
  not clear the previous outcome or results.

## Validation

Run `make check` (or the individual private `run`, `run-dmd`, `run-tsan`,
`doc-example`, and `death` targets). Coverage includes, but is not limited to,
FIFO progress, immediate completion/free before publication returns, allocation
failure cleanup, partial-prefix and zero-admission recovery, repeat dispatch,
three-level joins, authority rejection, terminal waves, allocation balance, and
four independent creators sharing a Farm. The `root-admission-order` selector
checks FIFO preservation across partial and repeated zero admission, an
otherwise-empty ready queue, and slot reuse under both reclamation policies.
Its immediate-completion cases consume the accepted prefix before publication
returns; the checks also run in release builds.

The root-wait selectors add
exactly-once waits, empty/already-joined waves, both custody orders, late
strict-finish tails, root-ready CAS retry, emitter-owned final sealing, and
counter-exhaustion death tests. The continuation selectors add continuation
trees, both final-owner orders, deferred terminal cleanup on consumers, resume
admission/FIFO reuse, subsets, repeated generations, and the C-M mutation checks.
The cancellation selectors add the C1–C13 schedules, each with its mutation, and
a random fail/cancel tree torture. The root-tracking selectors add the T1–T9
schedules (stale handles across recycling, cancel against the terminal CAS in
both orders, emitter-observed deadlines, clock-read counts, held slots, the
outcome visibility edge under TSan, a 1000-root `cancelAll` shutdown, handle
identity, generation wrap), each with its mutation, and a tracked torture with
deadlines, a foreign canceller and `cancelAll`. The selectors are listed in
[../../tests/private/README.md](../../tests/private/README.md).

## Proof to symbol and test map

| Proof | Primary symbols | Test selector |
| --- | --- | --- |
| Root child-join credit chain | `root.pending`, `rootReturns`, `Emittable`, `Service.childReturns` | `e1-basic`, `e1-before-custody`, `e1-after-custody`, `e1-late-strict`, `e1-joined-mixed`, `e1-root-ready-cas-retry`, `e1-seal-final-owner` |
| Destructor visibility and placement | `disposeChildren`, `retireMember`, `destroyState`, `active` | `dtor-root`, `dtor-child`, `dtor-order`, `dtor-moved-from`, `dtor-recursive`, `dtor-results`, `dtor-gc` |
| Child result slot publication | `results_`, `resultCount_`, `active`, `results(context)` | `ports-basic`, `ports-poll-visibility`, `ports-wait-visibility`, `ports-late-tail`, `ports-nested` |
| Wave failure aggregation | `Wave.failures`, `firstFailureCode`, `outcome(context)` | `cancel-c12`, `cancel-c13`, `tracked-torture` |
| In-tree cancellation | `W.cancel`, `subtreeCancel`, `scopeCancelled`, `dispatched_`, `cursor_`, `count_` | `cancel` / `cancel-c1`…`cancel-c13`, `cancel-torture` |
| Root tracking control word | `control`, `generation`, `cause`, `flags`, `deadline`, `status`, `PrivateRootHandle.requestCancel` | `root-tracking` / `root-t1`…`root-t8`, `tracked-torture`, `root-tracking-death` |
| Child continuation custody | `childPending`, `ChildStep`, `childFlags`, `ownJoin_`, `childReturns` | `phase-e1`, `slab-continuation`, `ports-nested`, continuation C-M mutations |
| Root join state machine | `root.pending`, `rootReturns`, `Service.childReturns`, `collectCompleted`, `pump` (states READY/WAITING/RETURN_QUEUED are descriptive) | `e1-basic`, `e1-root-ready-cas-retry`, `return-cas-retry`, `delayed-final-credit` |
| Within-shard completion join | completion `atomicFetchAdd!(MemoryOrder.acq_rel)(*shc, runlen)` | `private_tests_tsan join-only` |

## C callback instantiations

The erased C instantiations retain the typed protocol's atomic orders and final
accesses. Root/child state initialization and copied descriptors precede the
existing ready-list/table or cohort enqueue publication. Operation pointers and
user data are replaced only after the idle acquire and after rejecting busy,
retiring and empty starts. They remain immutable through the wave join.

A C result callback writes its exclusively assigned aligned slot before done
returns. The existing physical-table and descendant credits carry those writes
to idle, then to the parent's results acquire. For a continuation, interim slot
writes are inaccessible to the parent while active; done completes the result
before inline or deferred retirement. Slab clearing and destructors precede the
same release chain. No new per-member RMW or fence is needed for erasure.

The C native TLS context guard grants authority only around the current user
callback; construction initializers temporarily lose it. This diagnoses misuse
and is not a synchronization edge. The C root-handle representation copies the
existing record/generation capability; cancellation still uses the same CAS.
Farm/host bindings count allocated private parks, including empty ones, to
prevent ordinary teardown while the C park exists. That count is not a join;
park collection and external user quiescence remain required.
