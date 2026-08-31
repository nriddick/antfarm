# Actor memory ordering

## Scope

This document describes the implemented A1 activation and A2 intrusive-inbox
protocols in `antfarm_actor.d`. It supplements
[SPEC.md](SPEC.md), whose Farm sentinel and segment-lifetime rules remain
unchanged.

The central safety claim is narrow: plain mutable actor state may migrate
between OS threads because only one successful `scheduled -> running`
transition creates a borrow, and every handoff to a later activation crosses a
release/acquire chain. `PayloadBody` remains read-only ring storage and carries
only a stable slot address and generation.

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
| `L` | `ActorSlot.lifecycle` | Generation, phase, `RETIRE`, and `PENDING` in one atomic word. |
| `H` | `ActorRuntime.readyHeadWord` | Intrusive MPSC ready-stack head. |
| `Qnext` | `ActorSlot.queueNextWord` | Link owned by the ready queue while that slot is queued. |
| `Rc` | `ActorRuntime.readyCount` | Diagnostic/shutdown accounting; not a publication edge. |
| `Lc` | `ActorRuntime.liveCount` | Allocated-generation accounting; not sufficient by itself for runtime destruction. |
| `G` | `ActorSlot.submissionGate` | Inbox admission: one `CLOSED` bit and a count of in-flight send reservations in one atomic word. |
| `I` | `ActorSlot.inboxHeadWord` | Intrusive MPSC inbox stack, detached and reversed by the exclusive actor. |
| `Inext` | `ActorInboxNode.nextWord_` | Link owned first by the inbox, then by the actor-private detached batch. |
| `Nstate` | `ActorInboxNode.state_` | `FREE -> QUEUED -> PROCESSING -> FREE` node-ownership cycle. |
| `C` | `ActorSlot.inboxCarryWord` | Plain actor-private remainder of a bounded drain; handed to the next activation through `L`. |
| `Tsent` | Farm table sentinel | Release-published last by `AntFarm.write`, acquire-validated by a consumer. |
| `Pbody` | Two const ring words | Stable slot address and generation, raw-stored before `Tsent` and raw-loaded after its acquire. |

`raw` means an atomic operation with no ordering semantics. It prevents an
atomic/plain data race on reused storage but does not itself publish preceding
writes. `rel`, `acq`, and `acq_rel` have their usual release, acquire, and
combined meanings. D's `cas` is strong; every retry reloads the changed word.

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
`scheduled -> running` does not grant a borrow.

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

## Lifecycle states

```text
VACANT --CAS(acq_rel), generation++--> CONSTRUCTING
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

`RETIRE` and `PENDING` are bits in the same word as the phase. Therefore an
exit decision cannot be made from a phase snapshot that is independent of a
concurrent wake or retirement request.

`RETIRE` closes ordinary wakes immediately, but it does not skip interactions
whose `G` reservation won before `CLOSED`. A closing actor may continue through
`SCHEDULED -> RUNNING` solely to drain those accepted nodes. It reaches
`RETIRED` only when `G` is closed with count zero and both `I` and `C` are
empty.

## Handoff edges

| Handoff | Publication side | Acquisition side | What becomes visible |
| --- | --- | --- | --- |
| Slot reuse to construction | Reclaimer clears/deallocates the old state and release-stores `VACANT`. | Creator uses an `acq_rel` CAS for `VACANT -> CONSTRUCTING` while incrementing the generation. | Completion of old-generation teardown before the slot is initialized again. |
| Construction to first wake | Typed creation or the erased adapter initializes state bytes and plain slot fields, including recorded size/alignment, then `L.store(rel, IDLE)`. | The first successful wake CAS/RMW on `L` is `acq_rel`. | State bytes, state metadata, `state`, `dispatch`, runtime pointer, and payload identity. |
| Idle actor to ready queue | `L.CAS(acq_rel, IDLE -> SCHEDULED)`, then raw `Qnext`, then `H.CAS(acq_rel, slot)`. | Flusher takes `H` with `atomicExchange(acq_rel)`. | The scheduled phase and initialized queue link/slot fields. |
| Republished actor to ready queue | Callback first release-publishes `RUNNING -> SCHEDULED`; `pushReady` then release-publishes the link through `H`. | Flusher's acquire exchange of `H`. | Completed actor mutations and the new queue membership. |
| Ready queue to Farm producer | `H` release CAS publishes `Qnext`; each selected node is detached before `write`. | Flusher's acquire exchange, followed by raw link loads. | A private snapshot of distinct scheduled slots. |
| Farm producer to ring consumer | `write` raw-stores header/body words and release-stores `Tsent` last. | Consumer acquire-loads and validates `Tsent`; callback raw-loads `Pbody` afterward. | Stable slot address and generation copied into the ring. |
| Ring activation to exclusive borrow | Consumer already acquired `Tsent`; trampoline then CASes `L` from the matching generation's `SCHEDULED` to `RUNNING` with `acq_rel`. | Successful CAS is the acquisition; borrow construction follows it. | Prior initialization or prior activation's actor-state writes. |
| One activation to the next | User mutation is sequenced before trampoline's `L.CAS(acq_rel, RUNNING -> SCHEDULED/IDLE)`. | A later wake and/or `SCHEDULED -> RUNNING` CAS acquires the lifecycle modification chain. | All plain writes made through the previous `ActorBorrow`. |
| Additional wake before entry | A producer waking an already `SCHEDULED` actor performs an `acq_rel` RMW on `L`, setting `PENDING` even if it was already set. | Entry CAS reads that lifecycle position, clears `PENDING`, and acquires it before calling user code. | Producer writes sequenced before `wake`; inbox publication also has its own `I` edge. |
| Wake during execution | Wake's `acq_rel` RMW sets `PENDING` on `RUNNING`. | Exit CAS reloads/acquires the changed word, chooses `SCHEDULED`, and release-publishes the next activation; its entry CAS acquires again. | The current callback need not observe the wake, but a subsequent activation is guaranteed. |
| Generation admission to sender | Send validates the generation through an acquire load of `L`, then increments the open `G` reservation count with an `acq_rel` CAS. | A successful CAS owns one live-generation submission reservation until its release decrement. | State/mailbox lifetime remains valid while that sender publishes and signals. |
| Sender to inbox | Caller payload writes and `Nstate: FREE -> QUEUED` precede raw `Inext` updates and the successful release CAS on `I`. | The actor detaches `I` with an acquire exchange before following links or reading payload fields. | Node identity, link, tag, payload pointer, and caller-initialized payload data. |
| Inbox to activation | The successful `I` publication is sequenced before an `acq_rel` signal RMW on `L`. | Entry acquires a pre-entry signal; an exit racing a signal reloads `L` and schedules another activation. The actor independently acquire-detaches `I`. | The notice cannot be lost even when several sends coalesce into one activation. |
| Detached batch to later activation | Actor stores an unpopped FIFO remainder in plain `C`, then release-CASes `RUNNING -> SCHEDULED`. | The later activation acquire-loads/CASes `L` before reading and taking `C`. | Exclusive ownership of the detached links and their payloads migrates between actor threads. |
| Actor completion to node reuse | Actor finishes reading/processing a node, then release-stores `Nstate = FREE`. | `available` acquire-loads `Nstate`, or the next send claims it with an `acq_rel` CAS. | Completion of actor access before a producer rewrites or reuses node/payload storage. |
| Close against new send | Retirement first marks `RETIRE` on `L`, then sets `CLOSED` in `G` with an `acq_rel` CAS. | A sender's reservation CAS is totally ordered before or after the close CAS on `G`; post-close senders acquire-observe `CLOSED`. | Deterministic admission: earlier reservations finish, later attempts return `closed`. |
| Submission release to closing actor | An accepted sender enqueues and signals before release-decrementing the reservation count in `G`. | Closing entry/exit acquire-loads `G`; it may publish `RETIRED` only after observing `CLOSED`, count zero, `I` empty, and `C` empty. | All accepted send publication and signalling happens before final retirement. |
| External retirement to callback | `requestRetire` uses an `acq_rel` CAS to set `RETIRE` on `L` and closes `G`. | Entry or exit reloads/acquires `L`; `ActorContext.closing` also acquire-loads it. | Ordinary admission is closed; callbacks may still run to drain previously accepted inbox nodes. |
| Callback retirement to owner | Self-retirement or an observed external close ends with `L.CAS(acq_rel, ... -> RETIRED)`. | `ActorOwner.retired` and `reclaim` acquire-load `L`. | Completion of the user dispatch and all actor-state writes. |
| Reclamation to slot reuse | Reclaimer clears slot fields, deallocates state, then release-stores `VACANT` for the old generation. | A creator CAS-acquires `VACANT -> CONSTRUCTING` while incrementing the generation in the same word. | Completion of old-state teardown before new slot initialization. |
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
- After an acquire exchange detaches the list, the flusher owns those links.
- Before `AntFarm.write`, selected snapshot nodes have their links cleared.
- Written nodes are no longer queue members. An immediately consumed node may
  therefore republish and reuse its own `Qnext` without racing the flusher.
- Only the distinct unwritten suffix is returned to the ready queue.

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

After an acquire observation of `RETIRED`, the unique typed or erased owner may
deallocate the POD state using the size and alignment recorded at creation. It
clears the external-state pointer, dispatch pointer, and metadata before
publishing `VACANT`. Slot allocation for a new generation must acquire that
`VACANT` publication. Encoding generation and phase in the same atomic word
prevents a stale check-then-CAS from scheduling a reused slot.

The per-generation gate now protects built-in inbox submission, but
`ActorRuntime.destroy` still requires external quiescence: the engine must
retire and reclaim every owner, drain/join Farm consumers which could still be
inside the permanent trampoline, stop concurrent stale-handle calls, and only
then destroy the stable-slot slab. `Lc == 0`, `Rc == 0`, and `H == 0` are useful
assertions but are not a standalone runtime-lifetime fence.

`ActorOwner.reclaim` acquire-observes `RETIRED` and additionally asserts closed
`G`/zero reservations and empty `I`/`C`. An engine module-generation fence must
aggregate this actor result with non-actor work and with any message payload
destructors before unloading code or resetting an arena.

## Non-edges and caller obligations

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
- A successful `send` transfers the intrusive node until `complete`; freeing,
  rewriting, or submitting it elsewhere in that interval violates ownership.
- A closing handler must eventually drain or deliberately assume ownership of
  every accepted node. A handler which permanently ignores a nonempty inbox
  deliberately prevents retirement rather than dropping work.
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

Allocator lifetime has a separate pinned mimalloc v3.5.0 lane. Actor state,
runtime, and stable-slot storage are allocated on one thread only after normal
construction publication. Idle retirement establishes `RETIRED`; eight other
threads then call `reclaim` on disjoint owners, and a ninth calls
`ActorRuntime.destroy` after `live == ready == 0`. The release and
`MI_DEBUG=FULL` libraries both accept these cross-thread sized/aligned frees,
and a Clang-instrumented mimalloc plus LDC-instrumented actor binary passes one
ThreadSanitizer runtime. This tests allocator thread migration; it does not
weaken or replace any actor lifecycle handoff edge.
