# Public actor and wave ordering

This page describes the implemented autonomous-activation, intrusive-inbox and
phase-wave protocols in `antfarm/actors/actor.d` and `antfarm/actors/wave.d`. The private path's
publication, nested-credit and final-access protocol is in
[private-actor-ordering.md](private-actor-ordering.md); its layout invariants
are in [private-actor-layout.md](private-actor-layout.md). Both supplement
[SPEC.md](../../SPEC.md), whose Farm sentinel and segment-lifetime rules remain
unchanged.

The central safety claim is narrow: plain mutable actor state may migrate
between OS threads because, for autonomous activation, only one successful
`scheduled -> running` transition creates a borrow, and every handoff to a later
activation crosses a release/acquire chain. Wave dispatch is the exception: it
keeps the actor `scheduled`, verifies the unique wave reservation, and borrows
without that transition. `PayloadBody` remains read-only ring storage and
carries only a stable slot address and generation.

The application handler receives `scope ref ActorBorrow!T` and
`scope ref ActorContext`. Those qualifiers make the dynamic-extent intent part
of the handler function type, and actor creation rejects a handler that omits
either one. They do not add a memory-order edge or a reclamation fence.

This is still `@system` ownership. In particular, the compiler contract is not a
proof that raw pointers, casts, or nested references cannot escape. Returning a
reference from `ActorBorrow.value`, retaining it beyond the callback, accessing
actor state through another alias, freeing allocator storage before retirement,
or destroying the runtime while operations can still reach it violates the
contract. Safety comes from the runtime ownership transitions and
caller-observed quiescence described below.

## Public runtime invariants

### Ring and shim boundary

- The ring body remains `const(ulong)[]` and contains identity words only.
- A permanent host-owned trampoline is the only callback stored in actor payload
  headers. Reloadable module callbacks never appear directly in the ring.
- Every actor payload is single-shot and serial: `MaxCs = 1`, `Done = 1`.
- `antfarm.templates` keeps its current no-unshared-aliasing rule. The actor API
  is a separate, explicit escape hatch rather than a new ordinary packed
  parameter category.

### Identity and exclusive borrow

- An `ActorHandle!T` names a stable control slot and a generation encoded with
  the lifecycle state in one atomic word; it never points directly at
  reclaimable actor state and cannot dereference `T`.
- Only a successful `scheduled -> running` transition can construct an
  `ActorBorrow!T`. The borrow is valid only for the dynamic extent of the actor
  callback.
- The application handler must declare both `ActorBorrow!T` and `ActorContext`
  as `scope ref`. This makes callback-local capability intent part of the handler
  function type, so an unqualified handler is rejected at actor creation.
- This remains an `@system` API. The qualifiers do not prevent a handler from
  using casts or raw pointers to escape `ActorBorrow.value`, and they are not a
  substitute for the lifecycle state machine or a retirement fence.
- At most one activation for a generation is queued, published, or running.
  Wakes coalesce through a pending bit rather than publishing duplicates.
- Different actors remain independently runnable and may execute in parallel.
- A wave reserves each actor with an unqueued `IDLE -> SCHEDULED` transition
  before publishing its table. Intrusive membership and a submission-gate pin
  remain until aggregate wave completion; duplicate actors and overlapping
  unfinished waves are therefore rejected before publication.
- Wave operations receive only `scope ref ActorBorrow!T`. They do not receive
  `ActorContext`; republish, inbox draining and self-retirement remain the
  autonomous actor mode.

### Lifetime and allocation

- Stable control slots are retained until their `ActorRuntime` is destroyed.
  Actor state is a separate allocation with a shorter, explicit lifetime.
- Allocation and deallocation are supplied through an `@nogc` policy. The
  baseline adapter uses the C runtime; pools, slabs, high-performance malloc
  libraries and arenas are first-class alternatives.
- Actor state is not conservatively scanned. References to GC allocations
  require an explicit higher-level retention/scanning policy.
- An `ActorOwner!T` is the move-only authority for retirement and reclamation.
  Copied handles are non-owning capabilities and become stale after reuse.
- State cannot be destroyed until admission is closed and every queued,
  published, running and submitting use of that generation has retired.
- Warm wake, queue, flush, callback and republish paths allocate nothing.

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

`raw` means an atomic operation with no ordering semantics. It does not publish
preceding writes. It is not a license to race a plain access against the atomic
object: both sides of a reused ring word are atomic, and a release/acquire edge
is still required where one publishes data. See the memory-order contract in
[SPEC.md](../../SPEC.md). `rel`, `acq`, and `acq_rel` have their usual release,
acquire, and combined meanings. D's `cas` is strong; every retry reloads the
changed word.

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
while the writer still uses the descriptor after `writeTracked` returns. A zero
write rolls back both Wlen and its reserved table credit. Each hook records
diagnostic progress, completes its remaining descriptor accesses, then drops its
credit. The final acquire/release decrement joins all prior participants and
uniquely owns membership release and the `FINISHED` store. `seal` caches its
returned handle before dropping the producer hold.

`Wprogress == Wlen` is not a sufficient finish condition, even after sealing. A
hook can increment progress and still be accessing the descriptor. Also,
separately reading equality and then SEALED can use equality from an earlier open
publication state. The former equality/election implementation allowed both
races. The deterministic torture tests pause a hook after its progress increment
and verify that neither the sealer nor another completing table can finish until
that hook releases its credit; they then immediately reuse the descriptor before
joining the paused consumer's thread.

`FINISHED` is the runtime's last descriptor access, not a join of the optional
user completion callback. That callback runs after the release store. An owner
may observe `finished` and `begin` a new generation before the old callback
returns. The completion context must outlive the callback. An old callback must
not touch a reused descriptor, or storage the owner reclaimed after seeing
`finished`, without separate synchronization. `actors_torture` characterizes this
contract; it is not a bug reproduction.

The release/acquire chain joins consumers within each shard, shards within a
table, and table hooks into the final wave credit. Acquiring `FINISHED` carries
all predecessor actor writes into a dependent wave's Farm publication. It does
not carry writes the user callback has not yet performed.

Membership release reads the next intrusive link and clears the old link before
release-storing `Wowner = 0`. That store is the wave's final access to the slot:
another orchestrator may immediately reuse an idle actor in a new wave, or its
owner may reclaim a retired actor. Clearing the link after unpinning could
overwrite the new wave's list or access reclaimed storage.

While `waveOwnerWord` is nonzero, ordinary `wake`, `send`, and retirement
requests return `waveOwned`; a rejected send never claims its inbox node. A
wake/send that linearized in the narrow reservation race before membership
publication sets `PENDING` on the already-scheduled actor. The wave operation
retains exclusive entry, sets the sticky `INTERRUPTED` status bit, and
materializes that pending autonomous activation after its borrow ends. The same
outcome applies when the operation observes a racing retirement request;
retirement/cancel mode is preserved. `ActorWaveHandle.interrupted` reports this
observed handoff, while `failed` is reserved for structural admission rejection
(stale, non-idle, duplicate, or foreign actors). The bits are independent: a
later table's rejected admission can leave both set. Neither bit rolls back
operations. `FINISHED` joins wave operations, not the autonomous work handed off
by them. Signals arriving after the exit CAS need not set `INTERRUPTED`.

Moving the owner store before the lifecycle CAS cannot close admission: a sender
or waker may already have observed zero owner and signal after both. In
particular, an accepted sender must still publish and signal its inbox node.
The separate outcome avoids changing that inbox contract or the membership pin.

The payload callback sets `INTERRUPTED` through the existing acquire/release
status CAS loop before its physical table releases its credit. Admission sets
`FAILED` while the producer credit remains held. These RMWs preserve concurrent
sticky bits; the final credit's acquire joins every status writer before the
final release store of `FINISHED`. Thus no status update can be overwritten by
that store. The handle's acquire status load and generation double-check match
`failed`; `begin` clears both outcomes for the next generation.

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

The inbox is a Treiber MPSC stack only on the producer side. The actor
acquire-exchanges the entire stack and reverses it, yielding FIFO order by
successful head-CAS linearization within each detached batch. New concurrent
sends form a later batch. The actor is the sole consumer and sole owner of `C`.

`complete`, `fail(code)` and `cancel` all require `PROCESSING` and write the
whole completion word before the existing release of `FREE`. `complete` and
`cancel` clear the code; `fail` accepts at most 24 bits. The sender reads
`outcome` and `failureCode` only after `available` acquire-observed completion
for that interaction, before reuse or transfer. `send` still claims only
`Nstate`; the completion word is not reset by a send. Initialization sets zero
but is not an interaction reply. An outcome read while queued or processing, or
after another producer has acquired the node, has no ordering guarantee.

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
`requestRetire` or by a drain-to-cancel escalation CAS against the full current
word. `RETIRE | CANCEL` is the sticky retirement mode: every nonterminal rebuild
of `L` copies `observed & (RETIRE | CANCEL)` rather than reconstructing `RETIRE`
alone. The seven rebuild sites are:

- consumer entry when it reschedules a closing actor with an undrained gate;
- consumer entry to `RUNNING`;
- the callback exit CAS;
- the `IDLE -> SCHEDULED` signal;
- the idle retirement driver's `IDLE -> SCHEDULED`;
- wave admission rollback;
- wave operation exit.

The other writes either OR bits into the observed word (retirement mark,
`PENDING`), start a fresh generation, or publish `RETIRED`/`VACANT`, which clear
both bits. A wave reservation CASes from the exact bare `IDLE` word, so it never
succeeds over a mode bit. However, a cancel accepted between a wave's
`IDLE -> SCHEDULED` reservation and its `Wowner` publication must survive that
wave's exit or rollback, because a pre-reserved sender can still queue work
afterwards. Losing `CANCEL` at any site silently turns cancel into drain.

`RETIRE` closes ordinary wakes immediately, but it does not skip interactions
whose `G` reservation won before `CLOSED`. A closing actor may continue through
`SCHEDULED -> RUNNING` solely to drain those accepted nodes. It reaches
`RETIRED` only when `G` is closed with count zero and both `I` and `C` are empty.

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
operation. A concurrent external retirement wins over both republish and pending
wake.

## Ready-queue ownership

`Qnext` is raw because only these owners may touch it:

- Before publication, the one thread pushing that scheduled slot writes it.
- After an acquire exchange detaches the list into `Rcarry`, the holder of `D`
  owns those links. A release-store of `D` hands the untouched remainder to the
  next successful acquire CAS on `D`.
- Before `AntFarm.write`, selected snapshot nodes have their links cleared.
- Written nodes are no longer queue members. An immediately consumed node may
  therefore republish and reuse its own `Qnext` without racing the flusher.
- Only the distinct unwritten suffix is returned to the ready queue.

Each flush tries `D` once and returns zero on contention. It removes at most
`min(maximum, 256)` nodes, fetching a new `H` only when `Rcarry` is empty. The
remaining chain stays detached and counted; it is neither traversed nor
republished. `Rc` is decremented once for the extracted snapshot. The gate is
released before Farm publication, allowing independent snapshots to publish
concurrently and callbacks to republish while another flusher extracts work.
`maximum == 0` leaves both lists untouched. As before, ordering is unspecified.

`Rc` is incremented before a node becomes visible so a fast flusher cannot
detach it and decrement an unincremented count. This permits a transient
overcount while a producer's head CAS is in flight. Consequently `ready > 0` is
a work hint, not proof that `H` is momentarily nonzero, and `Rc` does not publish
slot contents.

## Farm boundary

The actor layer does not introduce a new ring-memory rule. `ActorBodyRange` reads
the stable slot's two identity words after the ready-queue acquire.
`AntFarm.write` copies them with raw atomic stores and commits the table with a
release `Tsent`. A consumer validates `Tsent` with acquire before entering the
payload and the actor trampoline raw-loads those same ring words.

A Farm subscription with no live root/pulse candidate validates that its
fallback epoch still equals the second acquired tail's epoch. A retained
producer grant can already cover a reclaimed old frontier before its next lap is
reserved; the reservation bound alone cannot authorize that attach. A rejected
attempt unwinds provisional pins and registration before retry. Unsubscribe
protects incomplete epochs in the atomic last-releaser drop; it does not scan
and plant after releasing ownership. The only independent plant is the
producer's, before publishing its own table, which prevents that epoch from
being completed and recycled during the plant.

Ring reuse is ordered by the Farm, not by the actor layer. A consumer's ring
reads precede its tally release. A producer's zero-`Rt` sweep acquires that
release, and every `Wt` reservation is `acq_rel`, so a producer spending a
retained quota balance acquires the sweeps other producers performed before it
overwrites those words. See the write-tail reservation and segment-reuse rows of
the memory-order contract in [SPEC.md](../../SPEC.md#9-memory-order-contract).

The ring identity is immutable for the live generation. Reclamation cannot clear
or reuse the slot until the activation has transitioned it to `RETIRED`;
generation comparison rejects an old ring identity after reuse. The ring body
itself is never cast to mutable actor state.

## Retirement and reclamation

`RETIRED` means the application dispatch has returned, no `ActorBorrow` exists,
and no later activation for that generation will be admitted. It does not mean
that the surrounding Farm consumer has finished all table accounting.

External retirement still marks `RETIRE` before closing the submission gate. For
an idle actor, the owner acquire-loads the closed gate with zero accepted
senders, then acquire-loads `L` again. That second lifecycle load is essential:
an accepted sender may have queued an activation after the initial idle
observation but before dropping the last reservation. If the joined load still
observes the same `IDLE | RETIRE` (or `IDLE | RETIRE | CANCEL`) word, no
lifecycle writer remains: accepted senders have completed their signals, no
callback is running, and ordinary wakes and wave reservations reject `RETIRE`.
The unique owner can therefore finish with a release store of `RETIRED`,
avoiding a redundant CAS. If `L` changed, retirement retries from the joined
value. Outstanding senders and scheduled/running actors retain the existing RMW
and drain protocol.

That argument also requires retirement requests on one owner to be serialized.
Sequential drain-then-cancel escalation is supported. Concurrent
`requestRetire` calls through aliases of one move-only owner are not: a cancel
CASed after the second load would be overwritten by the blind store. The inbox
is empty at that point, so no node escapes cancellation, but the overlapping
caller would see `requested` for a cancel no handler observed. Supporting
concurrent callers would require turning that store into a retrying CAS.

After an acquire observation of `RETIRED`, the unique typed or erased owner may
deallocate the POD state using the size and alignment recorded at creation. It
clears the external-state pointer, dispatch pointer, and metadata before
publishing `VACANT`. Slot allocation for a new generation must acquire that
`VACANT` publication. Encoding generation and phase in the same atomic word
prevents a stale check-then-CAS from scheduling a reused slot.

Vacant slots form an intrusive free list initialized with the slab. Taking or
returning a slot performs constant-size pointer work under `Fgate`; there is no
scan of occupied slots. The gate uses an acquire CAS and release store, with
`pause` while contended, and avoids pointer ABA across concurrent reuse. The
creator owns a popped slot exclusively, so it can publish the next
`CONSTRUCTING` generation with a store. Allocation failure returns that slot with
the incremented generation intact. Allocator callbacks and actor code execute
outside the gate. This is constant-time slot bookkeeping, not a lock-free
allocation guarantee; allocator cost and contention remain separate.

The per-generation gate protects built-in inbox submission, but
`ActorRuntime.destroy` still requires external quiescence: the engine must retire
and reclaim every owner, drain/join Farm consumers which could still be inside
the permanent trampoline, stop concurrent stale-handle calls, and only then
destroy the stable-slot slab. `Lc == 0`, `Rc == 0`, and `H == 0` are useful
assertions but are not a standalone runtime-lifetime fence.
`ActorOwner.reclaim` acquire-observes `RETIRED` and additionally asserts closed
`G`/zero reservations, empty `I`/`C`, and no `Wowner`. An engine
module-generation fence must aggregate this actor result with non-actor work and
with any message payload destructors before unloading code or resetting an arena.

## Engine aggregate unload fence

The actor torture suite makes the higher-level contract executable with a
test-side engine generation owner. This is not an `antfarm.actors` object. It combines
resident type-erased actor owners with an admission gate for module code frames
and ownership transferred out of callbacks:

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

The sender test pauses at `submissionReleased`, after `G` no longer protects the
sender frame. The actor consumes the interaction, retires, and is reclaimed while
the engine sender claim remains live. The detached-node test transfers a
`PROCESSING` node out of the callback without calling `complete`; the actor also
retires and is reclaimed first. These cases demonstrate why an engine cannot
substitute actor retirement for its aggregate module fence.

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
- `ActorHandle` copies are plain values. Publishing a newly created handle to
  another thread requires ordinary application synchronization.
- `scope ref` on `ActorBorrow` and `ActorContext` is a callback type contract,
  not an atomic operation. It neither repairs an escaped state pointer nor
  proves that allocator-derived pointers cannot outlive an arena.
- `ActorErasedAdapter` is a non-owning resident ABI boundary, not a module
  lifetime fence. An erased handler pointer may still name reloadable code; the
  engine must keep that generation loaded through retirement and reclamation.
- `valid`, `ready`, `live`, and stale-activation counters are observations, not
  ownership grants or shutdown fences.
- Allocator callbacks must be thread-safe for the creation/reclamation pattern in
  which they are used. Arena reset requires higher-level quiescence.
- Non-GC actor and inbox memory is not scanned. Lifecycle ordering does not
  retain GC references stored inside it.
- A successful `send` transfers the intrusive node until `complete`, `fail`, or
  `cancel`; freeing, rewriting, or submitting it elsewhere in that interval
  violates ownership.
- A closing handler must eventually drain or deliberately assume ownership of
  every accepted node. A handler which permanently ignores a nonempty inbox
  deliberately prevents retirement rather than dropping work. Cancel mode is
  cooperative in the same way: the library never discards nodes, so a
  `cancelling` handler calls `cancelInbox` (or `cancel`s what it pops).
- Retirement guarantees no final handler call in either mode. An idle actor with
  an empty inbox retires directly, and a scheduled one can retire at entry before
  dispatch. Resources reachable from POD state are released by the application
  after `retired`.
- Plain access through an escaped state pointer is not repaired by lifecycle
  atomics; it is a data race and an ownership violation.
- Module unload is engine policy. Its aggregate fence must cover actor retirement
  plus non-actor jobs, callbacks, registrations, resources, and message
  destructors that can still execute module code.

## Within-shard completion join

The Farm's chunk completion RMW on `Shc` uses acquire/release ordering, as does
the shard finisher's RMW on `Tprogress`. Both are needed when a tracked ST
table's hook observes plain writes from every callback: the first joins
primary-run consumers within a shard, and the second joins shards. That is a
join of all callbacks only for tracked ST single-shot tables. Work-claim
increments on `Shc` remain relaxed. `antfarm_test` `join-only` is the
two-consumer, one-shard ThreadSanitizer regression. It fails with the former
relaxed completion RMW and passes with acquire/release.

## Wave usage and published actor state

An actor wave is the actor-specific bulk-synchronous layer over Farm dispatch.
It is intended for workloads such as game simulation, where many entities can
run the same phase in parallel, but later phases must observe a coherent result
from the earlier phase. A tick can therefore be expressed as a sequence such as
intent, movement, collision, and combat, with one wave boundary wherever
intermediate state must become visible. Actors in a wave never observe one
another's timing-dependent, partially updated state.

The handle remains the stable, shareable identity and the borrow remains the
scoped exclusive capability. Wave admission and Farm payload gating acquire that
capability on behalf of the operation: each dispatched operation receives
mutable access to its own actor, and the orchestrator structurally prevents
another unfinished wave over the same actors.

### Aggregate completion

A wave may span several Farm tables. Every table contributes once when its
`Tprogress` reaches `Tlen`; the hook records diagnostic `Wprogress`, then drops
its table credit as its final descriptor access. A producer credit holds the wave
open until sealing, including all post-publication bookkeeping. Counts and
credits are reserved before Farm publication and rolled back on zero admission.
Only the final credit owner releases membership and publishes `FINISHED`;
equality between separately sampled progress/status fields does not elect a
finisher.

The completion barrier is:

```text
actor writes -> shard joins -> table hook -> final wave credit -> FINISHED -> waiter
```

Release/acquire ordering across that chain makes actor writes visible to a waiter
that observes the finished wave. The final credit belongs either to the sealer or
to a table hook. Structural admission rejection marks the wave failed and must
prevent publication of its prospective shared state. An interrupted operation
still completed; its observed control request is handed to autonomous dispatch.

`ActorWaveHandle` is the authoritative, pollable completion object. A thread or
Fiber may spin briefly, park, or arrange a notification, then acquire and
recheck the handle before publishing a dependent wave. Notifications are only
hints. A future direct Fiber adapter must use a lost-wake-safe
register-then-recheck protocol and preserve the actor path's no-allocation
contract.

The intended orchestration loop is:

1. Begin a wave over a non-overlapping actor set.
2. Publish table-sized prefixes, retrying later when Farm backpressure returns
   an unconsumed suffix.
3. Seal the wave after the final table is admitted.
4. On final success, commit its public generation before exposing `FINISHED`; on
   failure, leave the prior generation committed.
5. Acquire completion, reject a failed result, then publish any dependent wave.

An actor belongs to at most one unfinished wave. Persistent wave membership and
submission pins preserve both exclusivity and lifetime until aggregate
completion; autonomous wake, send, and retirement paths must not bypass that
ownership. Typed wave operation code must likewise remain resident until the wave
finishes.

### Private canonical state and public projections

The preferred state model is not two freely mutable copies of the whole actor.
Each actor has private canonical state, mutated only through its exclusive
borrow, plus a deliberately shaped public projection that other actors may read.
The projection is double-buffered:

```text
wave G reads public[G & 1]
       mutates private state
       writes complete projection into public[(G + 1) & 1]
wave completion publishes generation G + 1
```

All actors in a wave therefore read the same frozen public generation. They may
freely change private state and fill their own staging projection, but they may
not inspect another actor's staging slot. On successful aggregate completion,
the finisher release-publishes the next public generation before exposing
`FINISHED`. The old read buffer then becomes the staging buffer for the next
generation. On failure, the generation does not advance and partial staging
writes remain invisible.

Wave operations should receive an interface conceptually like
`ActorAccess!(Private, Public)`: a mutable `privateState`, a frozen
`publicState` view for the current generation, and `publish(Public)` or a
projector that writes a complete next value. Publishing a complete value is
preferred to exposing a mutable public reference, because a slot not fully
overwritten could otherwise leak data from two generations ago. Keeping the
projection separate also permits compact or structure-of-arrays layouts without
exposing actor internals.

The current `ActorWave` implementation provides scoped private borrowing,
exclusive membership, aggregate completion, and the pollable handle. The
double-buffered public projection is the proposed next layer, not an existing
API guarantee.

### Generation and membership rules

A monotonically increasing public generation is the commit authority; its parity
selects the physical buffer. The full generation value must be checked, not only
the buffer index, so delayed readers cannot mistake a later reuse for the
generation they requested. Per-entry actor generations still validate handles
and slot reuse independently.

If every live actor publishes a complete projection in every generation, one
global public generation is sufficient. Sparse waves require an explicit policy.
They can copy forward untouched projections, or store per-entry public generation
metadata and let readers select the newest committed version for that actor.
Such per-entry tags validate data but do not replace the global commit barrier.
Membership additions, removals, and slot reuse must also become visible only at a
defined wave boundary.

Several public tables that form one logical snapshot should share one commit
epoch, even if their work is spread across several Farm tables. State that is
intentionally visible at different phase boundaries should use separate wave
domains instead.

Public views are naturally callback- or wave-scoped. Ordinary actor-to-actor
reads finish before the next flip, so two buffers suffice. Readers that may
outlive a wave boundary require an additional lifetime mechanism such as epoch
pinning, a third buffer, or RCU-style retirement; double buffering alone cannot
protect them.

Open design work is therefore concentrated in the public projection schema,
sparse-publication semantics, boundary-safe membership changes, long-lived
reader pinning, and the direct Fiber completion adapter. None of those should
weaken the core rule: actors mutate only private state during a wave, observe
only a frozen committed public generation, and expose the next generation only
through successful aggregate wave completion.

## Validation

The actor unit suite exercises repeated migration through four consumers,
concurrent wakes, self and external retirement, slot-generation reuse, counted
non-GC
allocation, and a GC-disabled warm loop. The inbox coverage adds four concurrent inbox
producers, bounded seven-node actor drains, exact-once counts and sums, node
ownership return, retirement after accepted work, deterministic post-close
rejection, stale-handle rejection, and flat warm-path allocator counts.
The actor torture suite also verifies that progress equality alone cannot release
an active table hook: neither the sealer nor another completing table can finish
until the paused hook drops its credit, and descriptor reuse begins only after
`finished`.
Compile-time assertions also cover the scoped handler type and reject conversion
from the otherwise matching unscoped function type. The erased adapter test
additionally covers non-template creation, checked erased borrow metadata, inbox
processing, warm-path allocator stability, stale handles, and transfer of a
typed owner into resident erased ownership.

The separate actor torture suite compiles production-elided hooks immediately
after submission reservation, node claim, inbox-head publication, activation
signal, and reservation release. It forces close and activation across those
seams. In particular, it verifies that `Nstate = QUEUED` without the release CAS
on `I` is not visible to the consumer, while an `I`-published node is visible
even if the producer has not yet signalled `L`; `G` still prevents retirement
until that accepted producer releases its reservation. A sustained multi-actor
arm checks the same edges statistically over generation churn. A separate forced
race lets eight senders reserve before close and then contend for one node,
requiring exactly one accepted claim and seven `nodeBusy` results while
retirement waits for every reservation to leave `G`.

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
`MI_DEBUG=FULL` libraries both accept these cross-thread sized/aligned frees, and
a Clang-instrumented mimalloc plus LDC-instrumented actor binary passes one
ThreadSanitizer runtime. This tests allocator thread migration; it does not
weaken or replace any actor lifecycle handoff edge.

## Proof to symbol and test map

| Proof | Primary symbols | Test selector |
| --- | --- | --- |
| Actor-state serial ownership and migration | `ActorSlot.lifecycle`, `slot.dispatch` trampoline | `antfarm_test` (programmatic migration), `actors_torture` (serial entry, migration) |
| Ready-queue ownership and snapshot extraction | `ActorRuntime.readyHeadWord`, `readyDrainGate`, `readyCarryWord`, `ActorSlot.queueNextWord` | `actors_torture` (1024-actor backlog, four flushers, paused detach) |
| Farm publication and ring reuse | `AntFarm.write`, `ActorBodyRange`, `Tsent` | `antfarm_test_tsan join-only`, `tests/farm` `T16`/`T25`, `leaf_reuse.d` |
| Wave completion credits | `ActorWave.pendingCredits_`, `tablesPublished_`, `tablesCompleted_`, `status_`, `ActorSlot.waveOwnerWord` | `actors_torture` (`progress equality must not release active table notification`) |
| Inbox admission and close | `ActorSlot.submissionGate`, `inboxHeadWord`, `inboxCarryWord`, `ActorInboxNode.state_`, `nextWord_`, `completion_` | `actors_torture` (close after reservation/claim/publication/signal, eight-sender race) |
| Cancel-mode retirement | `ActorRetireMode.cancel`, the `cancelBit` in `ActorSlot.lifecycle` | `actors_torture` (F4b cancel mode, drain-to-cancel escalation, erased owner) |
| Engine aggregate unload fence | test-side gate and registry in `actors_torture` | `actors_torture` (four stalled claimants) |
| Within-shard completion join | `atomicFetchAdd!(MemoryOrder.acq_rel)(*shc, runlen)` in `antfarm/package.d` | `antfarm_test_tsan join-only` |
