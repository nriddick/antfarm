# Private actor layout

This page describes the private path's Entry/Service/cohort layout invariants and
the cohort-slab reuse proof in `antfarm/actors/private_actor.d`. The ordering proof that
uses these fields is in
[private-actor-ordering.md](private-actor-ordering.md); the user-facing rules are
in [../guide/private-actors.md](../guide/private-actors.md),
[../guide/private-results-and-waits.md](../guide/private-results-and-waits.md),
and [../guide/sizing-and-allocation.md](../guide/sizing-and-allocation.md).

## Entry layout and Service access

An Entry's word at offset 16 is a union. A root holds its `Service*` there. A
child member holds continuation metadata there (`childStep`, `childFlags`,
`childReserved`, `childPending`). Its Service pointer lives in its cohort's
`ChildBase.service`, set at creation from the creating context. The cohort's
`parentIsRoot_` records whether that context had no enclosing wave.

Access rule: code with only an Entry must never read `entry.service` directly.
Roots call `rootService(entry, enclosing)` with identity proved by their calling
context; members use `ChildBase.service`. Debug builds reject non-null
`enclosing` and null service words. The guard does not infer identity from the
word's bits: `releaseRoot` and root dispatch supply null because those paths
structurally own roots. A mistaken caller supplying null for a child would
violate that proof. Audited reads are the root branch of
`PrivateContext.service`, `releaseRoot`, root dispatch, and the test accessor;
root creation alone writes the Service word.

A wave selects one immutable credit target at start: enclosing wave, root
`pending`, or the continuation member's `childPending` when `ownJoin_` is set.
The finisher snapshots that target and parent kind before publishing idle.
`Service.childReturns` carries completed child waits. Each cohort embeds two
`Emittable` descriptors: initial emission and resumption. Its resume FIFO,
counter and scheduled flag are emitter-owned and reserved at cohort creation.

## Cohort and Entry sizes

- The common child Entry is **40 bytes**. Root-only disposition, flags and the
  checked 32-bit counter occupy the child enclosing-pointer union; static
  asserts verify size and offsets. `PrivateContext` stays four words.
- On 64-bit targets a signal-only per-object cohort is **264 bytes**, including
  both emission descriptors, resume FIFO, wave outcomes, cancellation root
  pointer and subset metadata. The ST-only 16-byte payload header removes
  112 bytes from the previous 376-byte cohort.
- Result cohorts add two words, for **280 bytes**. The cohort-slab policy
  appends one pointer to either layout. Plain/result dispatch adds no RMW;
  an empty child-return list costs one raw load per pump.
- Opt-in subset storage uses one extra eager allocation of **8 bytes per member
  of capacity**, containing the selection and its inverse positions.
- A tracked root record is **32 bytes per root slot**; an untracked park is
  32 bytes smaller. The root Entry keeps `subtreeCancel` at byte 35.

Root tracking's capacity is limited by a 32-bit `index`: at most `uint.max`.

## Cohort slab reuse

A `cohortSlab` cohort adds no atomic operation and no fence. Slot i belongs to
`entries_[i]`. Spawn writes the slot for Entry `count_`; `count_` rewinds to zero
in exactly one place, `finish` of a retiring wave, which is sequenced before the
finisher's `active = 0` release store.

```text
retiring member (any consumer): ~T -> zero slot -> entry.state = null
  -> callback return -> Shc completion RMW (acq_rel) -> Tprogress RMW (acq_rel)
  -> wave table/descendant credit releases (acq_rel)
  -> final wave decrement (acq_rel) -> finish: count_ = 0
  -> active = 0 (release)
parent spawn: active acquire-load sees 0 -> writes reused slot i
```

1. Every slot write of a retiring member runs in the consumer callback that
   retires it, before that callback returns: the terminal `runChild`, the
   continuation cleanup activation (before its `releaseWave`), or
   `runSkippedRetire` for members a cancelled wave never dispatched. The
   destructor precedes the zeroing, which precedes the state pointer clear.
2. Those writes precede the consumer's table-completion RMWs. Every wave credit
   release is an acquire/release RMW on `pending`, so the final decrement
   acquires all of them. The thread that runs `finish` therefore resets `count_`
   after every member's destroy and clear.
3. `spawn`, like `start`, acquire-loads `active` and returns `waveActive` unless
   it observes the idle store. A spawn that reuses slot i therefore happens after
   the previous occupant's destructor and clear. No new per-member
   synchronization is needed.
4. Coverage: a truncated retiring wave never seals early (`truncated` returns
   false when `retire_` is set), so cleanup tables still retire its undispatched
   members. When `count_` rewinds, each slot in `[0, old count)` has been
   destroyed exactly once and is zero. Nonretiring and subset waves never reset
   `count_`.
5. `finish` never reads the slab pointer. Its post-idle tail uses only the saved
   parent and upstream pointers, so the idle store remains its last cohort
   access.
6. Disposal runs once per cohort, after `disposeChildren` acquire-loads every
   descendant cohort's idle publication. It destroys retained members, then frees
   the slab once. A retired slot is never passed to the allocator. A retained POD
   slot is not zeroed; it needs no clearing, because a rooted slab's range is
   removed before its free.

Zeroing serves two purposes. Under `gcRooted` the slab stays rooted until
disposal, so a cleared dead slot stops retaining its references. (`gcRooted` adds
the range just before creation zeroes it, as for every zeroed metadata
allocation; a collection in that window over-retains transiently.) A refill also
starts from a zero slot, which debug builds check before construction. Results,
Entry and selection storage do not depend on slot contents.

## State destruction and results

The stored instance is destroyed exactly once on a consumer. Caller-side
parameters, temporaries and moved-from sources are excluded from that promise.
Every successful spawn destroys its moved-from `.init` parameter on the caller;
destructors must tolerate `.init`, including observable actions on that value.

| Retirement path | Consumer sequence | Byte free |
| --- | --- | --- |
| Root `finish` | user callback → child cohorts recursively → `~T` | emitter or consumer policy |
| Root `finishAfterChildren` | joined internal cleanup activation → child cohorts → `~T` | emitter or consumer policy |
| Child `startFinal`, plain or result op | op → result slot write if any → child cohorts → `~T` | consumer (perObject); slab slot zeroed on the consumer (cohortSlab) |
| Child `startFinal`, continuation op | final `done` result write → descendant join → child cohorts → `~T`; deferred retirement uses a consumer cleanup activation | as above |
| Parent disposes a cohort | each live member's child cohorts → `~T`, including never-dispatched members | consumer; a cohortSlab slab is freed once, after every member |
| Root park `destroy()` | requires no live state; no implicit retirement | emitter metadata free |

Order across siblings is unspecified. Destructors must not read other actors'
state, block, or call park APIs. Root-park reentry from an owning failed-spawn
argument destructor is fatal in debug; release builds leave reentry unchecked.
The guard does not cover moved-from parameter destruction after return.

Result slots belong to the cohort and outlive members. A result must not refer to
a resource its member's destructor releases. To transfer ownership, clear the
owning field in `T` before returning the pointer as `R`.

`stateAllowed!T` rejects classes, interfaces, context-carrying nested structs and
structs containing them, and destructors or moves that cannot be called from
`nothrow @nogc`. Postblits and copy constructors are allowed, but the library
never copies non-POD state. Pass non-copyable values as rvalues or with
`move(value)`. On DMD, non-copyable state with an alignment of 32 bytes or more
cannot be `move`d into a by-value `spawn` argument: because of a
compiler/druntime limit, `core.lifetime.move` tries to copy such structs and runs
the destructor during the move. Public actors retain their existing POD
restriction; `R` also remains POD.

## Proof to symbol and test map

| Layout invariant | Primary symbols | Test selector |
| --- | --- | --- |
| Entry union and Service-access rule | `Entry`, `rootService`, `ChildBase.service`, `parentIsRoot_`, `PrivateContext.service` | `phase-e1`, `phase-ports`, `slab` group |
| Credit target selection | `ownJoin_`, `childPending`, `root.pending`, `Emittable` | `ports-nested`, continuation C-M mutations |
| Subset storage layout | `sel_`, `resultCount_`, `subsetMembers(context)` | `subset-*`, subset C12/C13 mutations |
| Signal-only cohort size and layout | `ChildBase`, `Wave` outcome fields, `ChildBase.root` | static asserts under LDC and DMD (`make -C tests/private run run-dmd`) |
| Slab reuse and zeroing | `retireMember`, `finish`, `count_`, `active`, `slabByteLimit` | `slab`, `slab-retire`, `slab-retain`, `slab-refill`, `slab-idle`, `slab-continuation`, `slab-cancel` |
| State destruction order | `disposeChildren`, `retireMember`, `destroyState` | `dtor-order`, `dtor-child`, `dtor-root`, `dtor-results` |
