# Actor memory ordering

## Scope

This document describes the implemented autonomous activation and intrusive-
inbox protocols in `actors/actor.d`, plus the phase-oriented aggregate protocol
in `actors/wave.d`. The separate private path's publication, nested-credit,
and final-access reclamation protocol is described in
[PRIVATE_ACTORS.md](PRIVATE_ACTORS.md). It supplements
[SPEC.md](../SPEC.md), whose Farm sentinel and segment-lifetime rules remain
unchanged.

The central safety claim is narrow: plain mutable actor state may migrate
between OS threads because, for autonomous activation, only one successful
`scheduled -> running` transition creates a borrow, and every handoff to a
later activation crosses a release/acquire chain. Wave dispatch is the
exception: it keeps the actor `scheduled`, verifies the unique wave
reservation, and borrows without that transition. `PayloadBody` remains
read-only ring storage and carries only a stable slot address and generation.

The application handler receives `scope ref ActorBorrow!T` and
`scope ref ActorContext`. Those qualifiers make the dynamic-extent intent part
of the handler function type, and actor creation rejects a handler that omits
either one. They do not add a memory-order edge or a reclamation fence.

This is still `@system` ownership. In particular, the compiler contract is not
a proof that raw pointers, casts, or nested references cannot escape. Returning
a reference from `ActorBorrow.value`, retaining it beyond the callback,
accessing actor state through another alias, freeing allocator storage before
retirement, or destroying the runtime while operations can still reach it
violates the contract. Safety comes from the runtime ownership transitions and
caller-observed quiescence described below.

## Atomic objects and notation

| Name | Field | Role |
| --- | --- | --- |
| `L` | `ActorSlot.lifecycle` | Generation, phase, `RETIRE`, `PENDING`, and `CANCEL` in one atomic word. |
| `H` | `ActorRuntime.readyHeadWord` | Intrusive MPSC ready-stack head. |
| `D` | `ActorRuntime.readyDrainGate` | Nonblocking exclusive ownership of ready-list snapshot extraction. |
| `Rcarry` | `ActorRuntime.readyCarryWord` | Plain detached backlog, accessed only while holding `D`. |
| `Fgate` | `ActorRuntime.freeSlotGate` | Acquire/release gate protecting the plain free-slot head and links. |
| `Fhead` / `Fnext` | `ActorRuntime.freeSlots` / `ActorSlot.freeNext` | Vacant slots available for construction; no allocated state is on this list. |
| `Qnext` | `ActorSlot.queueNextWord` | Link owned by the ready queue while that slot is queued. |
| `Rc` | `ActorRuntime.readyCount` | Diagnostic/shutdown accounting; not a publication edge. |
| `Lc` | `ActorRuntime.liveCount` | Allocated-generation accounting; not sufficient by itself for runtime destruction. |
| `G` | `ActorSlot.submissionGate` | Inbox/wave admission: one `CLOSED` bit and a count of in-flight reservations in one atomic word. |
| `I` | `ActorSlot.inboxHeadWord` | Intrusive MPSC inbox stack, detached and reversed by the exclusive actor. |
| `Inext` | `ActorInboxNode.nextWord_` | Link owned first by the inbox, then by the actor-private detached batch. |
| `Ncompletion` | `ActorInboxNode.completion_` | Plain 32-bit node outcome (low 8 bits) and failure code (high 24 bits), read only after acquiring availability. |
| `Nstate` | `ActorInboxNode.state_` | `FREE -> QUEUED -> PROCESSING -> FREE` node-ownership cycle. |
| `C` | `ActorSlot.inboxCarryWord` | Plain actor-private remainder of a bounded drain; handed to the next activation through `L`. |
| `Wowner` | `ActorSlot.waveOwnerWord` | Stable owning wave while aggregate membership and its lifetime pin remain active. |
| `Tsent` | Farm table sentinel | Release-published last by `AntFarm.write`, acquire-validated by a consumer. |
| `Pbody` | Two or three const ring words | Stable slot address and generation; wave payloads add the stable wave address. Raw-stored before `Tsent` and raw-loaded after its acquire. |
| `Tprogress` | Farm table completion counter | Shard completion RMWs on `Shc` are acquire/release; the finisher's acquire/release increment reaching `Tlen` invokes the table hook. Primary-run writes join; all callbacks only for tracked ST single-shot tables. |
| `Wlen` | `ActorWave.tablesPublished_` | Number of physical tables admitted into the open wave. |
| `Wprogress` | `ActorWave.tablesCompleted_` | Number of table hooks which have recorded table completion; diagnostic, not a callback-lifetime fence. |
| `Wstatus` | `ActorWave.status_` | `SEALED`, `FINISHED`, and `FAILED`; acquire observation of `FINISHED` joins the wave's descriptor accesses, not the optional user callback. |
| `Wcredits` | `ActorWave.pendingCredits_` | Producer hold plus one credit per admitted physical table; final decrement owns completion. |

`raw` means an atomic operation with no ordering semantics. It does not
publish preceding writes. It is not a license to race a plain access against
the atomic object: both sides of a reused ring word are atomic, and a
release/acquire edge is still required where one publishes data. See the
memory-order contract in [SPEC.md](../SPEC.md). `rel`, `acq`, and `acq_rel`
have their usual release, acquire, and combined meanings. D's `cas` is
strong; every retry reloads the changed word.

## End-to-end chart

```text
creator / waker          ready flusher          Farm                 consumer / actor
----------------        -------------          ----                 ----------------

plain state + slot init
L.store(rel, IDLE)

L.CAS(acq_rel,
      IDLE -> SCHEDULED)
Qnext.store(raw, old H)
H.CAS(acq_rel, slot)
                         H.exchange(acq_rel, 0)
                         Qnext.load(raw)
                         snapshot slot identity
                                                Pbody.store(raw)
                                                Tsent.store(rel)
                                                                     Tsent.load(acq)
                                                                     Pbody.load(raw)
                                                                     L.CAS(acq_rel,
                                                                       SCHEDULED -> RUNNING)
                                                                     create scope-ref
                                                                       ActorBorrow/context
                                                                     plain state mutation
                                                                     callback returns;
                                                                       borrows end
                                                                     L.CAS(acq_rel,
                                                                       RUNNING -> IDLE,
                                                                       SCHEDULED, or RETIRED)
                                                                          |
                                      republish: Qnext/H release <---------+
                                                                          |
                                      next activation acquires L/H/Tsent --+
```

There are three complementary chains in this chart:

1. `L` serializes actor ownership and carries actor-state writes between
   activations.
2. `Qnext`/`H` transfers a scheduled slot to a flusher without allocation.
3. `Tsent` transfers the copied identity words through the Farm ring to a
   consumer.

The queue and Farm chains deliver an activation. The lifecycle chain is the
authority to touch mutable state. Possessing a ring handle without winning
`scheduled -> running` does not grant an autonomous borrow. Wave dispatch,
described below, borrows without that transition.

## Wave completion chart

```text
orchestrator                 Farm table consumers                 observer
------------                 --------------------                 --------
begin: Wcredits = 1
Wlen.fetchAdd(acq_rel)
Wcredits.fetchAdd(acq_rel)
L.CAS(acq_rel,
  IDLE -> SCHEDULED)
link member; Wowner.store(rel)
writeTracked(...)
Tsent.store(rel)       ---> Tsent.load(acq)
                            L.load(acq, SCHEDULED)
                            phase-specific ActorBorrow mutation
                            L.CAS(acq_rel, SCHEDULED -> IDLE)
                            Shc completion.fetchAdd(acq_rel)
                            Tprogress.fetchAdd(acq_rel)
                              last shard: table hook
                            Wprogress.fetchAdd(acq_rel)
                            Wcredits.fetchSub(acq_rel) [final access]

seal: Wstatus.CAS(acq_rel,
  OPEN -> SEALED)
cache observation handle
Wcredits.fetchSub(acq_rel) [final access]

sole final-credit owner:
cache completion callback/context
clear intrusive members
Wowner.store(rel, 0) for each
Wstatus.store(rel, FINISHED) [last descriptor access] -----------> load(acq)
invoke cached optional completion
```

The producer hold prevents completion while more tables can be published or
while the writer still uses the descriptor after `writeTracked` returns.
A zero write rolls back both Wlen and its reserved table credit. Each hook
records diagnostic progress, completes its remaining descriptor accesses,
then drops its credit. The final acquire/release decrement joins all prior
participants and uniquely owns membership release and the `FINISHED` store.
`seal` caches its returned handle before dropping the producer hold.

`Wprogress == Wlen` is not a sufficient finish condition, even after sealing.
A hook can increment progress and still be accessing the descriptor. Also,
separately reading equality and then SEALED can use equality from an earlier
open publication state. The former equality/election implementation allowed
both races. The deterministic torture tests pause a hook after its progress
increment and verify that neither the sealer nor another completing table can
finish until that hook releases its credit; they then immediately reuse the
descriptor before joining the paused consumer's thread.

`FINISHED` is the runtime's last descriptor access, not a join of the optional
user completion callback. That callback runs after the release store. An owner
may observe `finished` and `begin` a new generation before the old callback
returns. The completion context must outlive the callback. An old callback
must not touch a reused descriptor, or storage the owner reclaimed after
seeing `finished`, without separate synchronization. `actor_torture`
characterizes this contract; it is not a bug reproduction.

The release/acquire chain joins consumers within each shard, shards within a
table, and table hooks into the final wave credit. Acquiring `FINISHED`
carries all predecessor actor writes into a dependent wave's Farm publication.
It does not carry writes the user callback has not yet performed.

Membership release reads the next intrusive link and clears the old link
before release-storing `Wowner = 0`. That store is the wave's final access to
the slot: another orchestrator may immediately reuse an idle actor in a new
wave, or its owner may reclaim a retired actor. Clearing the link after
unpinning could overwrite the new wave's list or access reclaimed storage.

While `waveOwnerWord` is nonzero, ordinary `wake`, `send`, and retirement
requests return `waveOwned`; a rejected send never claims its inbox node. A
wake/send that
linearized in the narrow reservation race before membership publication sets
`PENDING` on the already-scheduled actor. The wave operation retains exclusive
entry, reports failure, and materializes that pending autonomous activation
after its borrow ends.

## Private root child-join ordering

Private roots use a different counter from public `Wcredits`: a 32-bit root
`pending` counts physical-table custody plus direct-wave notification tails.
The common Entry overlays this root metadata with the child's enclosing pointer;
size and offset asserts preserve its 40-byte layout.

The E2-3 plumbing (child Service pointer in the cohort, the generic wave credit
target, `Emittable`, `childReturns` and `ownJoin_`) changed no memory order and
added no atomic operation. E2-4 uses the reserved paths as described below. Child
`spawn` loses a store, and wave emission and enqueue lose one load each.
`finish` selects the join target with one extra plain (non-atomic) byte load
of `parentIsRoot_`; that load is not an atomic and not an ordering
change.

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
custody; waits release it on collection. The final publisher's successful CAS
is its last Entry and Service access. Nonfinal decrements perform no subsequent
Entry access. The acquiring emitter can reset `pending = 1` with a raw store:
all foreign credits are gone, and ownership has transferred through the queue.

A wave finisher snapshots parent/upstream pointers before release-publishing
idle as its final cohort access. The saved root credit keeps Entry and Service
alive through the post-idle tail. `finished(context)` acquires child results and
permits cohort reuse, but cannot by itself authorize root Entry reuse or park
destruction. Collection drains `live` only after the notification tails join.

Root retention uses checked CAS because the packed 32-bit counter can exhaust;
reject `uint.max` before incrementing. An existing custody hold prevents a
concurrent transition to zero during a legal retain. Existing 64-bit wave
fetch-adds remain acquire/release as on the base: their outstanding table/wave
obligations are bounded by simultaneously resident storage and outstanding
notifications, not lifetime-total starts. E1 does not weaken their ordering or
replace them with checked CAS. All releases remain acquire/release.

The raw empty-list hint supplies no publication edge; observing empty only
defers a concurrent wake to a later collection. The acquire exchange, not the
hint, makes the detached links and joined results visible. Reversing each list
preserves its successful-push FIFO order; appending at collection gives no global
completion-order guarantee. See [the state machine and lost-wake proof](PRIVATE_ACTORS.md#root-join-state-machine-and-lost-wake-argument).

### Private state destructor visibility

Stored-state destruction runs inside the final consumer dispatch, sequenced
after the user callback and any cohort-owned result write. Disposal is
post-order: `disposeChildren` acquire-loads every cohort's idle publication
before reading or destroying its members. The existing tracked-table and
wave-credit acquire/release joins therefore carry each member's last state
writes to its destructor, including members last run on another consumer.
`finishAfterChildren` uses the existing joined wake publication and consumer
cleanup activation; it does not execute a destructor in emitter collection.
Under emitter reclamation, the consumer destroys the stored instance before
the table/wake handoff, and collection frees only its raw allocation.
GC range registration remains present through destruction and is removed
only at byte free. This rooting is separate from the visibility argument.

### Private child results

Typed cohorts store each operation's return value in `results_[i]`, where i is
its participant position (spawn order for a whole wave, selection position for
a subset). Continuations write only on their final `done` activation. The store
precedes callback return and member retirement. No new atomics or fences are needed: the plain slot write joins
through Shc and Tprogress acquire/release RMWs, table and descendant wave
credits, and the final wave credit's acquire/release decrement. The finisher
release-stores `active = 0` as its final cohort access.

Polling roots and child-level parents acquire that idle store through
`finished(context)` or `results(context)` before reading the slots. A root
resumed through `waitChildren` additionally acquires the root pending decrement,
rootReturns publication, emitter detach, Farm publication and consumer acquire
chain above. `results` still acquire-loads active to diagnose an early read.

Parent reads precede the next `start` enqueue release CAS. The emitter's
acquire/release detach, Farm release publication and consumer acquire order those
reads before the next wave's slot writes. The idle acquire in `start` also
orders successive writing generations. `resultCount_` is parent-only and set
before enqueue; children read neither that count nor any slot. The slot pointer
is published through enqueue. The finisher never updates result bookkeeping
after publishing idle, so a polling parent can read and dispose the cohort
while the saved root notification credit is still in flight.

## Private wave failure aggregation

`context.fail` on a member elects one plain `(code, Entry)` writer with a raw
checked CAS on `Wave.failures`. The CAS needs atomicity only. Reaching
`uint.max` is fatal before the store, so a wrap cannot observe zero and elect
a second writer. The winner's plain code and Entry stores are sequenced before
the callback returns. They join through the same chain that carries result slots:

```text
member fail RMW + winner's plain code/Entry stores (sequenced before callback return)
  -> Shc completion (acq_rel) -> Tprogress (acq_rel)
  -> table/descendant releases (acq_rel)
  -> final wave decrement (acq_rel) -> finish: active.store(0, release)
  -> parent outcome(): active.load(acquire) -> plain reads
```

Every cohort callback, including a continuation activation, runs under a wave
table credit, so every report precedes the join. `outcome` then maps the
winning Entry to a participant position: the spawn index for a whole wave, or
`sel_[capacity + index]` for a subset. The pair is consistent. Which concurrent
report wins is unspecified. `failures` counts reports, not distinct members.

The next nonempty `start` or `startSubset` resets the count, the cancel flag,
the winning Entry, and the participant counters only after rejecting an active,
retiring, or empty start, and before its enqueue release. That start's idle
acquire happens-before the reset, so no writer from the previous generation
remains. `firstFailureCode` is valid only when `failures != 0`. The emitter
may lower `dispatched` on truncation; see the next section.

## Private in-tree cancellation

Cancellation adds no operation on `root.pending`, `childPending`, `W.pending`
or any return list, and no wake source. The E1/E2 credit proofs quantify over
the tables a wave publishes and the activations they run. Cancellation changes
only *which* tables exist: a prefix, or cleanup tables that take the same
per-table credit and rollback. The lost-wake and double-wake arguments
therefore carry over. The new obligations are below. R7, R8, R10 and R11
concern root records; see [private root tracking](#private-root-tracking).

Writers set a flag with two idempotent release stores: `W.cancel = true`,
then the root Entry's `subtreeCancel = true` (byte 35, formerly padding).
`scopeCancelled(W)` acquire-loads the summary byte and, only if it is set,
walks up: acquire-load `w.cancel`; stop when the cohort's parent is the root;
otherwise continue at `parent.enclosing`. The summary is a sticky fast-path
hint, not an aggregate join. Acquiring one writer's summary store orders that
writer's flag only; the per-wave acquire loads plus advisory semantics make
that sufficient.

| # | Race | Argument |
| --- | --- | --- |
| R1 | In-tree cancel vs the emitter emitting W | Flags are atomic and advisory. The emitter sees the flag at a table boundary and truncates, or it does not and publishes the table, whose members may observe the flag later. Both are valid E2 executions; no credit depends on the observation. |
| R2 | Flag reset at `start` vs a late writer | Writers are the parent (in its own activation, program-ordered before its next `start`) and members of W (`cancelPeers`, under a W table or resume credit). W's join precedes `start`'s idle acquire, so every member write happens-before the reset. A parent store onto a wave that went idle after its `active` check is also before the reset; it only makes `outcome.cancelled` true for a fully dispatched wave. |
| R3 | Truncated seal vs P lifetime | The emitter holds P when it checks. It stores `dispatched_` **before** `releaseWave` (the seal), which stays its last park access, including through the `Service` traversal that snapshotted `next`. A retiring wave keeps P until its cleanup tables cover `cursor_ == count_`. |
| R4 | `dispatched_` visibility | Emitter plain store -> seal RMW (acq_rel) on `W.pending` -> later RMWs in its modification order -> final decrement (acq_rel) -> `finish`, which reads it for the debug prefix check -> `active.store(release)` -> parent acquire in `outcome`/`results`. The same chain already carries consumer writes; the seal's release gains a new use, not a new edge. |
| R5 | Truncation with a scheduled resume descriptor | The resume descriptor has its own P', independent of the initial seal; an early seal with suspended members behaves like a normal seal with suspended members. The cleanup header is a local copy, because `emitResume` publishes `header_`; Farm copies the header into the table during `writeTracked`. Cleanup covers `[cursor_, count_)`; resumable members come only from the written prefix. |
| R6 | Cancel vs a suspended continuation or waiting root | No early wake exists. Resumption remains the unique final decrement of `childPending`/`pending`, and the flag is read only inside the resumed activation. |
| R9 | `subtreeCancel` reset at spawn vs in-tree writers | Every in-tree writer runs under a credit that precedes the root's final `pending` decrement or its table return. Collection acquires both before `reclaim`; the raw reset in `spawn` follows on the same emitter. |
| R13 | Ancestor reads during a walk | Each ancestor wave and parent Entry is pinned by the walker's credit chain: the emitter's P or a consumer's table credit on W, W's credit on its upstream or on the parent member's counter (whose H is taken before custody release), and so on up to `root.pending`. `ChildBase.parent`, `ChildBase.root` and child `Entry.enclosing` are immutable while pinned. The `parent is root` test precedes any read of the root union. |

An undispatched member of a truncated `startFinal` wave has idle children:
the previous wave's join included all descendants, every exit path cleared
its continuation flags, and it holds no H. Its cleanup table therefore runs
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
publish nothing a canceller reads; consumers read the plain fields through
table publication. They are kept for clarity and cost nothing on x86.

**X1a.** A terminal root's activations happen-before `takeOutcome` returns it on
the emitter: the same R11 chain reaches collection, and `takeOutcome` follows
collection in emitter program order. Application storage written in the final
activation (typed outputs outside the library) is therefore readable on the
emitter after the take, and on another thread after ordinary synchronization
with the emitter. This replaces "read after `live` falls" with a per-root edge.

`deadline` is written only by the emitter at spawn, before publication; consumers
read it while the root is live, and `cancelAll` reads it on the emitter. The
clock (`MonoTime.currTime`) is read only for an armed word without the
cancelled bit.

## Private child continuation ordering

A continuation member owns 32-bit `childPending` custody during its activation.
Every wave it starts chooses that counter as its immutable notification target.
A lazy hold H pins its enclosing wave W from the first suspension/draining
activation until completion. Initial and resume tables both retain W credits;
the emitter holds P while a park's resume descriptor is scheduled.

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
whichever side arrives last. Children cannot take the count to zero while
custody is held. No new retain occurs without running custody. Nonfinal
releasers never access the Entry again; final owners either push a return or
clear flags and release H as their last access. This is the lost-wake and
exactly-one-resumption argument.

The exclusive reset `childPending := 1` is raw. A resumed activation follows
the previous final owner's queue handoff. A new wave follows W's prior join
and its parent's idle acquire. The immediate-done path can leave dormant one:
its acquire load of one, after every retain, proves all notification decrements
are finished. Neither path has another writer at reset.

Final activation writes, including result slots, reach the parent through its
tracked table's Shc/Tprogress/W releases. For done with outstanding tails,
the member's final owner also carries the child join into `releaseWave(W)`.
The last W decrement acquires all releases, publishes idle, then releases the
snapshotted root/member/ancestor target. After idle, the finisher touches only
those snapshots; a parent may reuse or dispose the child cohort immediately.

Parent-written header, retirement flag, subset positions and result count
precede start's incoming release CAS. The emitter acquires them before initial
emission; resume emission is later on the same emitter. H and P prevent W from
joining while a return is queued or a descriptor is still scheduled. Taking H
inside the callback is safe because its table credit cannot drop before return.
P is cleared and released only after the FIFO is empty, as the final park access.
Service traversal snapshots descriptor `next` before calling emit and does not
dereference a sealed descriptor afterward.

Return-link ownership transfers through the release CAS and acquire exchange.
The emitter reverses returns, groups them by park, and maintains an unpublished
FIFO. `ResumeBodies.save` copies its independent cursor; prepare detaches the
accepted prefix without moving that cursor, and clears tail on full drain.
Farm sizes, prepares and copies the range before publication. **Reading an
accepted Entry's `next` after publication is not an ordering edge and is
forbidden:** its next activation can already be writing a new return link.
Zero admission transfers nothing and rolls back its table credit under P.

A terminal done with tails becomes `doneRetire`. Even if the emitter's seal
is the final owner, it queues a cleanup activation instead of running a
destructor. The consumer cleanup acquires the chain above, disposes descendant
cohorts post-order, destroys state, then drops H while its table still fences
return. The result write precedes inline or deferred retirement. Both root
reclamation policies preserve this child destructor/free placement.

Debug result-written bytes follow the same publication and W-join edges as
their slots; each participant owns a distinct byte. They add no release-build
work. The child-return hint is a raw load that may delay discovery until the
next pump; the exchange supplies the acquire. Emitter wake remains outside
this protocol.

## Inbox and close chart

```text
producer                     inbox / lifecycle          exclusive actor              owner
--------                     -----------------          ---------------              -----

plain node payload init
G.CAS(acq_rel, count++)
Nstate.CAS(acq_rel,
  FREE -> QUEUED)
Inext.store(raw, old I)
I.CAS(acq_rel, node)       -- release publication -->
L.CAS/RMW(acq_rel, signal) -- coalesced notice ----->
G.fetchSub(acq_rel)                                      L.CAS(acq_rel,
                                                         SCHEDULED -> RUNNING)
                                                        I.exchange(acq_rel, 0)
                                                        reverse detached list
                                                        Nstate.store(raw, PROCESSING)
                                                        read/process payload
                                                        Ncompletion = outcome | (code << 8)
                                                        Nstate.store(rel, FREE)
                                                        bounded remainder -> C
                                                        L.CAS(acq_rel,
                                                          RUNNING -> SCHEDULED)

                                                                                 L.CAS(acq_rel,
                                                                                   set RETIRE)
                                                                                 G.CAS(acq_rel,
                                                                                   set CLOSED)
post-close G load/acquire -> CLOSED; reject

accepted-before-close send finishes as above
G count reaches zero ------------------------- acquire-observed by actor/owner ----->
I empty + C empty + CLOSED/count=0
                                                        L.CAS(acq_rel, -> RETIRED)
                                                                                 L.load(acq, RETIRED)
                                                                                 reclaim state
```

`G` is the admission linearization point. A reservation CAS ordered before the
CAS which sets `CLOSED` is accepted even if `RETIRE` becomes visible during its
second lifecycle check. A reservation ordered after `CLOSED` is rejected. This
single atomic gate avoids a close-versus-counter check split across independent
objects.

The inbox is a Treiber MPSC stack only on the producer side. The actor acquire-
exchanges the entire stack and reverses it, yielding FIFO order by successful
head-CAS linearization within each detached batch. New concurrent sends form a
later batch. The actor is the sole consumer and sole owner of `C`.

`complete`, `fail(code)` and `cancel` all require `PROCESSING` and write the
whole completion word before the existing release of `FREE`. `complete` and
`cancel` clear the code; `fail` accepts at most 24 bits. The sender reads
`outcome` and `failureCode` only after `available` acquire-observed completion
for that interaction, before reuse or transfer. `send` still claims only
`Nstate`; the completion word is not reset by a send. Initialization sets zero
but is not an interaction reply. An outcome read while queued or processing,
or after another producer has acquired the node, has no ordering guarantee.

## Lifecycle states

```text
VACANT --exclusive free-list pop, generation++--> CONSTRUCTING
                                          |
                           plain initialization,
                           then store-release IDLE
                                          |
                                  wake CAS(acq_rel)
                                          v
                                     SCHEDULED
                                    /         \
                 closed + empty  /           \ enter CAS(acq_rel),
                                  v             \ clear PENDING
                              RETIRED          RUNNING
                                  ^           /   |   \
                                  |          /    |    \
                       retire wins|   no work   work/pending/inbox
                                  |      /          \
                                  +-----+            +--> SCHEDULED
                                        \
                                         +------------> IDLE

RETIRED --clear/deallocate, store-release--> VACANT
```

`RETIRE`, `PENDING`, and `CANCEL` are bits in the same word as the phase.
Therefore an exit decision cannot be made from a phase snapshot that is
independent of a concurrent wake or retirement request.

`CANCEL` (0x20) is set only together with `RETIRE`, by a cancel-mode
`requestRetire` or by a drain-to-cancel escalation CAS against the full
current word. `RETIRE | CANCEL` is the sticky retirement mode: every
nonterminal rebuild of `L` copies `observed & (RETIRE | CANCEL)` rather than
reconstructing `RETIRE` alone. The seven rebuild sites are:

- consumer entry when it reschedules a closing actor with an undrained gate;
- consumer entry to `RUNNING`;
- the callback exit CAS;
- the `IDLE -> SCHEDULED` signal;
- the idle retirement driver's `IDLE -> SCHEDULED`;
- wave admission rollback;
- wave operation exit.

The other writes either OR bits into the observed word (retirement mark,
`PENDING`), start a fresh generation, or publish `RETIRED`/`VACANT`, which
clear both bits. A wave reservation CASes from the exact bare `IDLE` word, so
it never succeeds over a mode bit. However, a cancel accepted between a wave's
`IDLE -> SCHEDULED` reservation and its `Wowner` publication must survive that
wave's exit or rollback, because a pre-reserved sender can still queue work
afterwards. Losing `CANCEL` at any site silently turns cancel into drain.

`RETIRE` closes ordinary wakes immediately, but it does not skip interactions
whose `G` reservation won before `CLOSED`. A closing actor may continue through
`SCHEDULED -> RUNNING` solely to drain those accepted nodes. It reaches
`RETIRED` only when `G` is closed with count zero and both `I` and `C` are
empty.

## Handoff edges

| Handoff | Publication side | Acquisition side | What becomes visible |
| --- | --- | --- | --- |
| Slot reuse to construction | Reclaimer clears/deallocates the old state, release-stores `VACANT`, links the slot into `Fhead`, then release-stores `Fgate`. | Creator acquire-locks `Fgate` and exclusively removes the slot, then acquire-loads `L` and release-stores `CONSTRUCTING` with the next generation. | Completion of old-generation teardown and unique construction ownership before the slot is initialized again. |
| Construction to first wake | Typed creation or the erased adapter initializes state bytes and plain slot fields, including recorded size/alignment, then `L.store(rel, IDLE)`. | The first successful wake CAS/RMW on `L` is `acq_rel`. | State bytes, state metadata, `state`, `dispatch`, runtime pointer, and payload identity. |
| Idle actor to ready queue | `L.CAS(acq_rel, IDLE -> SCHEDULED)`, then raw `Qnext`, then `H.CAS(acq_rel, slot)`. | Flusher takes `H` with `atomicExchange(acq_rel)`. | The scheduled phase and initialized queue link/slot fields. |
| Republished actor to ready queue | Callback first release-publishes `RUNNING -> SCHEDULED`; `pushReady` then release-publishes the link through `H`. | Flusher's acquire exchange of `H`. | Completed actor mutations and the new queue membership. |
| Ready queue to Farm producer | `H` release CAS publishes `Qnext`; each selected node is detached before `write`. | Flusher's acquire exchange, or acquire CAS on `D` after a prior flusher release-stored `D`. | A private snapshot of distinct scheduled slots, including a saved remainder from another flusher. |
| Farm producer to ring consumer | `write` raw-stores header/body words and release-stores `Tsent` last. | Consumer acquire-loads and validates `Tsent`; callback raw-loads `Pbody` afterward. | Stable slot address and generation copied into the ring. |
| Ring activation to exclusive borrow | Consumer already acquired `Tsent`; trampoline then CASes `L` from the matching generation's `SCHEDULED` to `RUNNING` with `acq_rel`. | Successful CAS is the acquisition; borrow construction follows it. | Prior initialization or prior activation's actor-state writes. |
| One activation to the next | User mutation is sequenced before trampoline's `L.CAS(acq_rel, RUNNING -> SCHEDULED/IDLE)`. | A later wake and/or `SCHEDULED -> RUNNING` CAS acquires the lifecycle modification chain. | All plain writes made through the previous `ActorBorrow`. |
| Additional wake before entry | A producer waking an already `SCHEDULED` actor performs an `acq_rel` RMW on `L`, setting `PENDING` even if it was already set. | Entry CAS reads that lifecycle position, clears `PENDING`, and acquires it before calling user code. | Producer writes sequenced before `wake`; inbox publication also has its own `I` edge. |
| Wake during execution | Wake's `acq_rel` RMW sets `PENDING` on `RUNNING`. | Exit CAS reloads/acquires the changed word, chooses `SCHEDULED`, and release-publishes the next activation; its entry CAS acquires again. | The current callback need not observe the wake, but a subsequent activation is guaranteed. |
| Generation admission to sender | Send validates the generation through an acquire load of `L`, then increments the open `G` reservation count with an `acq_rel` CAS. | A successful CAS owns one live-generation submission reservation until its release decrement. | State/mailbox lifetime remains valid while that sender publishes and signals. |
| Sender to inbox | Caller payload writes and `Nstate: FREE -> QUEUED` precede raw `Inext` updates and the successful release CAS on `I`. | The actor detaches `I` with an acquire exchange before following links or reading payload fields. | Node identity, link, tag, payload pointer, and caller-initialized payload data. |
| Inbox to activation | The successful `I` publication is sequenced before an `acq_rel` signal RMW on `L`. | Entry acquires a pre-entry signal; an exit racing a signal reloads `L` and schedules another activation. The actor independently acquire-detaches `I`. | The notice cannot be lost even when several sends coalesce into one activation. |
| Detached batch to later activation | Actor stores an unpopped FIFO remainder in plain `C`, then release-CASes `RUNNING -> SCHEDULED`. | The later activation acquire-loads/CASes `L` before reading and taking `C`. | Exclusive ownership of the detached links and their payloads migrates between actor threads. |
| Actor completion to node reuse | Actor finishes reading/processing a node and plain-writes `Ncompletion`, then release-stores `Nstate = FREE`. | `available` acquire-loads `Nstate`, or the next send claims it with an `acq_rel` CAS. | Outcome, failure code and payload writes, plus completion of actor access before a producer rewrites or reuses node/payload storage. |
| Close against new send | Retirement first marks `RETIRE` on `L`, then sets `CLOSED` in `G` with an `acq_rel` CAS. | A sender's reservation CAS is totally ordered before or after the close CAS on `G`; post-close senders acquire-observe `CLOSED`. | Deterministic admission: earlier reservations finish, later attempts return `closed`. |
| Submission release to closing actor | An accepted sender enqueues and signals before release-decrementing the reservation count in `G`. | Closing entry/exit acquire-loads `G`; it may publish `RETIRED` only after observing `CLOSED`, count zero, `I` empty, and `C` empty. | All accepted send publication and signalling happens before final retirement. |
| External retirement to callback | `requestRetire` uses an `acq_rel` CAS to set `RETIRE` on `L` and closes `G`. | Entry or exit reloads/acquires `L`; `ActorContext.closing` also acquire-loads it. | Ordinary admission is closed; callbacks may still run to drain previously accepted inbox nodes. |
| Cancel request to callback | `requestRetire(cancel)` or escalation sets `RETIRE \| CANCEL` with an `acq_rel` CAS on `L`, then closes `G` and drives retirement. | Every nonterminal rebuild copies the mode bits. An entry CAS ordered after the request carries `CANCEL` into `RUNNING`; otherwise the exit CAS reloads it and schedules the next activation. `ActorContext.cancelling` acquire-loads `L`. | A flag only, no data. A handler activated after the request observes `cancelling` and may `cancelInbox`. Retirement with an empty inbox and a drained gate needs no activation. |
| Retirement publication to owner | Callback retirement uses `L.CAS(acq_rel, ... -> RETIRED)`; an idle external close may release-store `RETIRED` after joining the closed submission gate and rechecking `L`. | `ActorOwner.retired` and `reclaim` acquire-load `L`. | Completion of the user dispatch and all actor-state writes. |
| Reclamation to slot reuse | Reclaimer clears slot fields, deallocates state, then publishes `VACANT` and returns the slot through `Fgate`. | Creator acquires the free-slot gate and the `VACANT` lifecycle before incrementing the generation. | Completion of old-state teardown before new slot initialization. |
| Stale handle to reused slot | Every handle RMW compares the generation as part of the same `L` value it would modify. | A mismatch returns `staleHandle`; there is no state acquisition. | Nothing: an old generation cannot schedule or borrow the new state. |

## Actor-state handoff proof

Consider two successful callbacks, `A` and `B`, for the same slot generation.
`A`'s plain writes occur before its release CAS out of `RUNNING`. If it chooses
`SCHEDULED`, `B`'s entry acquire CAS reads that state directly or through
intervening lifecycle RMWs. If it chooses `IDLE`, a wake RMW first acquires the
idle value and release-publishes `SCHEDULED`; `B` then acquires that value.

Thus:

```text
A actor writes
  happens-before A exit release on L
  synchronizes-with later acquire/RMW chain on L
  happens-before B's successful entry acquire
  happens-before B actor reads/writes
```

Only the successful entry CAS calls `slot.dispatch`. A simultaneous second
claimant finds `RUNNING` and returns without constructing `ActorBorrow`. The
Farm's `MaxCs = 1`, `Done = 1` payload gate and the actor ready-state invariant
prevent two payloads for one scheduled activation; generation checking rejects
old identities after reuse. `L` remains the final per-actor serial-ownership
authority across publications.

## Wake versus callback exit

The lost-wakeup boundary is resolved on `L`, not by separately testing a queue
or mailbox.

If wake linearizes first:

```text
waker:  RUNNING -> RUNNING|PENDING       (acq_rel CAS)
actor:  observes PENDING and chooses
        RUNNING|PENDING -> SCHEDULED      (acq_rel CAS)
```

If exit linearizes first:

```text
actor:  RUNNING -> IDLE                   (acq_rel CAS)
waker:  IDLE -> SCHEDULED                 (acq_rel CAS; queues actor)
```

A wake against `SCHEDULED|PENDING` still executes a same-value atomic RMW
instead of returning after a load. This gives a future mailbox producer a
release point ordered against the entry CAS even when another wake had already
set the bit.

`ActorContext.republish()` is callback-local plain state. It is inspected only
by the same trampoline after the callback returns, so it needs no atomic
operation. A concurrent external retirement wins over both republish and
pending wake.

## Ready-queue ownership

`Qnext` is raw because only these owners may touch it:

- Before publication, the one thread pushing that scheduled slot writes it.
- After an acquire exchange detaches the list into `Rcarry`, the holder of `D`
  owns those links. A release-store of `D` hands the untouched remainder to
  the next successful acquire CAS on `D`.
- Before `AntFarm.write`, selected snapshot nodes have their links cleared.
- Written nodes are no longer queue members. An immediately consumed node may
  therefore republish and reuse its own `Qnext` without racing the flusher.
- Only the distinct unwritten suffix is returned to the ready queue.

Each flush tries `D` once and returns zero on contention. It removes at most
`min(maximum, 256)` nodes, fetching a new `H` only when `Rcarry` is empty.
The remaining chain stays detached and counted; it is neither traversed nor
republished. `Rc` is decremented once for the extracted snapshot. The gate is
released before Farm publication, allowing independent snapshots to publish
concurrently and callbacks to republish while another flusher extracts work.
`maximum == 0` leaves both lists untouched. As before, ordering is unspecified.

`Rc` is incremented before a node becomes visible so a fast flusher cannot
detach it and decrement an unincremented count. This permits a transient
overcount while a producer's head CAS is in flight. Consequently `ready > 0`
is a work hint, not proof that `H` is momentarily nonzero, and `Rc` does not
publish slot contents.

## Farm boundary

The actor layer does not introduce a new ring-memory rule. `ActorBodyRange`
reads the stable slot's two identity words after the ready-queue acquire.
`AntFarm.write` copies them with raw atomic stores and commits the table with a
release `Tsent`. A consumer validates `Tsent` with acquire before entering the
payload and the actor trampoline raw-loads those same ring words.

The ring identity is immutable for the live generation. Reclamation cannot
clear or reuse the slot until the activation has transitioned it to
`RETIRED`; generation comparison rejects an old ring identity after reuse.
The ring body itself is never cast to mutable actor state.

## Retirement and reclamation

`RETIRED` means the application dispatch has returned, no `ActorBorrow` exists,
and no later activation for that generation will be admitted. It does not mean
that the surrounding Farm consumer has finished all table accounting.

External retirement still marks `RETIRE` before closing the submission gate.
For an idle actor, the owner acquire-loads the closed gate with zero accepted
senders, then acquire-loads `L` again. That second lifecycle load is essential:
an accepted sender may have queued an activation after the initial idle
observation but before dropping the last reservation. If the joined load
still observes the same `IDLE | RETIRE` (or `IDLE | RETIRE | CANCEL`) word, no
lifecycle writer remains: accepted senders have completed their signals, no
callback is running, and ordinary wakes and wave reservations reject `RETIRE`.
The unique owner can therefore finish with a release store of `RETIRED`,
avoiding a redundant CAS. If `L` changed, retirement retries from the joined
value. Outstanding senders and scheduled/running actors retain the existing
RMW and drain protocol.

That argument also requires retirement requests on one owner to be serialized.
Sequential drain-then-cancel escalation is supported. Concurrent
`requestRetire` calls through aliases of one move-only owner are not: a
cancel CASed after the second load would be overwritten by the blind store.
The inbox is empty at that point, so no node escapes cancellation, but the
overlapping caller would see `requested` for a cancel no handler observed.
Supporting concurrent callers would require turning that store into a
retrying CAS.

After an acquire observation of `RETIRED`, the unique typed or erased owner may
deallocate the POD state using the size and alignment recorded at creation. It
clears the external-state pointer, dispatch pointer, and metadata before
publishing `VACANT`. Slot allocation for a new generation must acquire that
`VACANT` publication. Encoding generation and phase in the same atomic word
prevents a stale check-then-CAS from scheduling a reused slot.

Vacant slots form an intrusive free list initialized with the slab. Taking or
returning a slot performs constant-size pointer work under `Fgate`; there is
no scan of occupied slots. The gate uses an acquire CAS and release store,
with `pause` while contended, and avoids pointer ABA across concurrent reuse.
The creator owns a popped slot exclusively, so it can publish the next
`CONSTRUCTING` generation with a store. Allocation failure returns that slot
with the incremented generation intact. Allocator callbacks and actor code
execute outside the gate. This is constant-time slot bookkeeping, not a
lock-free allocation guarantee; allocator cost and contention remain separate.

The per-generation gate now protects built-in inbox submission, but
`ActorRuntime.destroy` still requires external quiescence: the engine must
retire and reclaim every owner, drain/join Farm consumers which could still be
inside the permanent trampoline, stop concurrent stale-handle calls, and only
then destroy the stable-slot slab. `Lc == 0`, `Rc == 0`, and `H == 0` are useful
assertions but are not a standalone runtime-lifetime fence.

`ActorOwner.reclaim` acquire-observes `RETIRED` and additionally asserts closed
`G`/zero reservations, empty `I`/`C`, and no `Wowner`. An engine module-
generation fence must
aggregate this actor result with non-actor work and with any message payload
destructors before unloading code or resetting an arena.

## Engine aggregate unload fence

The actor torture suite makes the higher-level contract executable with a
test-side engine generation owner. This is not an `actors` object. It
combines resident type-erased actor owners with an admission gate for module
code frames and ownership transferred out of callbacks:

```text
old-generation caller                    engine generation owner
---------------------                    -----------------------

erased actor owner in registry --------> callback lifetime claimant
gate.CAS(acq_rel, count++)  -----------> admitted sender / destructor /
                                             detached-node claimant
                                             gate.CAS(acq_rel, set CLOSED)
                                             reject later admissions
                                             request actor retirement
                                             poll RETIRED; reclaim owners

finish old-generation access
kind diagnostic.fetchSub(acq_rel)
gate.fetchSub(acq_rel, count--) --------> acquire-load CLOSED | count == 0
                                             actor registry empty
                                             simulated unload may cross
```

Close and non-actor claim acquisition modify the same gate, so each such
claimant is ordered before close and included, or ordered after close and
rejected. Per-kind destructor/sender/node counts are diagnostics; the closed
gate's aggregate count is their authority. Actor callbacks are covered by the
separately aggregated erased-owner registry. Unload is tested only from the
resident coordinator after close, so both zero observations are stable.

The sender test pauses at `submissionReleased`, after `G` no longer protects
the sender frame. The actor consumes the interaction, retires, and is reclaimed
while the engine sender claim remains live. The detached-node test transfers a
`PROCESSING` node out of the callback without calling `complete`; the actor
also retires and is reclaimed first. These cases demonstrate why an engine
cannot substitute actor retirement for its aggregate module fence.

| Engine handoff | Publication side | Acquisition side | What becomes visible |
| --- | --- | --- | --- |
| Module admission to close | An admitted old-generation operation increments the open gate with an `acq_rel` CAS. | Close sets `CLOSED` with an `acq_rel` CAS on the same gate. | The operation is either included in the aggregate count or rejected; it cannot slip across close. |
| Module claimant to unload | The callback, sender, destructor, or detached-node owner completes old-generation access, updates its diagnostic, then release-decrements the gate. | The resident unload probe acquire-loads the closed gate and requires aggregate count zero. | Completion of all admitted old-generation module-code and transferred-ownership access. |
| Actor registry to unload | The registry requests retirement, acquire-observes `RETIRED`, and reclaims each erased owner. | The unload probe requires the resident registry to be empty in addition to the closed gate being zero. | No actor handler for the generation can run again, and its state/dispatch registration has been cleared. |

The test's destructor claimant models module-owned resource or message cleanup,
not a non-POD actor-state destructor. General actor construction/destruction
remains deliberately undefined.

## Non-edges and caller obligations

- A result slice used after `start` or `startFinal` is a race with the next
  wave; it must not escape the authorized callback.
- Observing `context.cancelled` false is not a guarantee that no cancel was
  requested. `outcome.cancelled` true does not mean any work was skipped.
- `dispatched` is not readable before the join; the emitter may still lower it.
- `requestCancel` returning `requested` does not mean the root has observed
  it; it means the outcome will be `cancelled` or `failed`. `finished` does
  not mean the outcome was taken.
- `PrivateRootHandle` copies are plain values; publishing one to another
  thread needs ordinary synchronization. `requestCancel` after the park's
  `destroy` uses freed memory: join every canceller first.
- `takeOutcome`'s visibility edge reaches the emitter thread only. `live`
  falling on a tracked park is not a per-root edge for another thread.
- `ActorHandle` copies are plain values. Publishing a newly created handle to
  another thread requires ordinary application synchronization.
- `scope ref` on `ActorBorrow` and `ActorContext` is a callback type contract,
  not an atomic operation. It neither repairs an escaped state pointer nor
  proves that allocator-derived pointers cannot outlive an arena.
- `ActorErasedAdapter` is a non-owning resident ABI boundary, not a module
  lifetime fence. An erased handler pointer may still name reloadable code;
  the engine must keep that generation loaded through retirement and
  reclamation.
- `valid`, `ready`, `live`, and stale-activation counters are observations, not
  ownership grants or shutdown fences.
- Allocator callbacks must be thread-safe for the creation/reclamation pattern
  in which they are used. Arena reset requires higher-level quiescence.
- Non-GC actor and inbox memory is not scanned. Lifecycle ordering does not
  retain GC references stored inside it.
- A successful `send` transfers the intrusive node until `complete`, `fail`,
  or `cancel`; freeing, rewriting, or submitting it elsewhere in that interval violates ownership.
- A closing handler must eventually drain or deliberately assume ownership of
  every accepted node. A handler which permanently ignores a nonempty inbox
  deliberately prevents retirement rather than dropping work. Cancel mode is
  cooperative in the same way: the library never discards nodes, so a
  `cancelling` handler calls `cancelInbox` (or `cancel`s what it pops).
- Retirement guarantees no final handler call in either mode. An idle actor
  with an empty inbox retires directly, and a scheduled one can retire at
  entry before dispatch. Resources reachable from POD state are released by
  the application after `retired`.
- Plain access through an escaped state pointer is not repaired by lifecycle
  atomics; it is a data race and an ownership violation.
- Module unload is engine policy. Its aggregate fence must cover actor
  retirement plus non-actor jobs, callbacks, registrations, resources, and
  message destructors that can still execute module code.

## Validation

The A1 test exercises repeated migration through four consumers, concurrent
wakes, self and external retirement, slot-generation reuse, counted non-GC
allocation, and a GC-disabled warm loop. The A2 test adds four concurrent
inbox producers, bounded seven-node actor drains, exact-once counts and sums,
node ownership return, retirement after accepted work, deterministic
post-close rejection, stale-handle rejection, and flat warm-path allocator
counts. Compile-time assertions also cover the scoped handler type and reject
conversion from the otherwise matching unscoped function type. The erased
adapter test additionally covers non-template creation, checked erased borrow
metadata, inbox processing, warm-path allocator stability, stale handles, and
transfer of a typed owner into resident erased ownership.

The separate actor torture suite compiles production-elided hooks immediately
after submission reservation, node claim, inbox-head publication, activation
signal, and reservation release. It forces close and activation across those
seams. In particular, it verifies that `Nstate = QUEUED` without the release
CAS on `I` is not visible to the consumer, while an `I`-published node is
visible even if the producer has not yet signalled `L`; `G` still prevents
retirement until that accepted producer releases its reservation. A sustained
multi-actor arm checks the same edges statistically over generation churn. A
separate forced race lets eight senders reserve before close and then contend
for one node, requiring exactly one accepted claim and seven `nodeBusy`
results while retirement waits for every reservation to leave `G`.

The same suite exercises the engine aggregate fence with four independently
stalled old-generation claimants: callback, destructor, sender, and detached
inbox node. Every premature unload probe is rejected. The sender and detached-
node arms additionally reach actor reclamation before releasing their engine
claims, so the test would fail if unload were based on actor retirement alone.

Allocator lifetime has a separate pinned mimalloc v3.5.3 lane. Actor state,
runtime, and stable-slot storage are allocated on one thread only after normal
construction publication. Idle retirement establishes `RETIRED`; eight other
threads then call `reclaim` on disjoint owners, and a ninth calls
`ActorRuntime.destroy` after `live == ready == 0`. The release and
`MI_DEBUG=FULL` libraries both accept these cross-thread sized/aligned frees,
and a Clang-instrumented mimalloc plus LDC-instrumented actor binary passes one
ThreadSanitizer runtime. This tests allocator thread migration; it does not
weaken or replace any actor lifecycle handoff edge.

## Within-shard completion join

The Farm's chunk completion RMW on `Shc` uses acquire/release ordering, as
does the shard finisher's RMW on `Tprogress`. Both are needed when a tracked
ST table's hook observes plain writes from every callback: the first joins
primary-run consumers within a shard, and the second joins shards. That is a
join of all callbacks only for tracked ST single-shot tables. Work-claim
increments on `Shc` remain relaxed. `antfarm_test` `join-only` is the
two-consumer, one-shard ThreadSanitizer regression. It fails with the former
relaxed completion RMW and passes with acquire/release.
