# F2 independent lifetime review (spike runtime, base 2b1eac7)

Reviewer: independent Opus consultant (CyDo task 199), read-only, 2026-10-06. Scope: R1–R5 and R13 of design 170 §6, plus byte 35 and general credit/lifetime checks, against the uncommitted spike diff of `actors/private_actor.d` and `actors/actor.d`. The reviewer read code only; it ran no tests. Its report follows verbatim. Follow-up: the two doc-comment items (`cancelWave` stale-idle note, `park.cancel` return semantics) were applied to the runtime before commit, and the ACTOR_MEMORYORDER section was written; no code change was needed.

---


**Verdict: PASS.** I found no lifetime, credit or memory-ordering defect. There are two minor semantic/documentation items and one missing deliverable (the ACTOR_MEMORYORDER section).

Scope: worktree `.cydo/tasks/37/worktree`, uncommitted diff vs 2b1eac7 (`actors/private_actor.d`, `actors/actor.d`). I read the whole of `private_actor.d`, plus `antfarm.d` `PayloadBroadcastRange`/`writeTracked`, and design §4/§6.

## Answer (per question)

**1. R1 — PASS.** `truncated()` (private_actor.d:1475-1484) changes only `dispatched_` and, for non-retire, does the one P release that the normal seal would otherwise do. No credit depends on what the flag shows.
- If the flag flips after `scopeCancelled` returned false, the table is written as a normal E2 table, and its members can see `cancelled` later.
- No state can tear. `cursor_`, `dispatched_` and `retire_` are written only by the emitter (or by the parent before enqueue).
- Monotonicity holds. Once a walk sees a `w.cancel` store, coherence keeps it visible. A reset needs W's join, and every walker is pinned inside W. The summary byte is sticky.

**2. R2 — PASS.** These are the writers of `W.cancel`:
- the parent: `park.cancel` (:1257) or `cancelChildren` (:185-191);
- members of W: `cancelPeers` (:195-200), run under a W table credit or a resume credit.

Member writes happen-before the final W decrement, then `finish` `active.store(rel)`, then the parent's acquire in `start`, then `resetOutcome` (:1385).

Parent writes are in the parent's own program order. Activations of one parent are serialized:
- roots, by Batch return;
- continuation members, by `releaseMember` acq_rel and then resume.

A stale store therefore always happens-before any later reset.

Stale-store schedule: `cancelChildren` loads `active != 0`, then the last W table completes and `finish` stores 0, then the parent stores `cancel = true` on the idle wave. This is safe for lifetime. The parent owns the park, and the root Entry is pinned by the parent's own credit chain. No walker can be reading W, because all its descendants have joined. A later `start` can only be by this parent, after the store, so the reset wins.

The visible effect is semantic only (see Risk 2): `outcome().cancelled == true` for a wave that ran fully, and `park.cancel` returning true with no effect.

**3. R3 — PASS.**
- **Non-retire:** `dispatched_ = cursor_` (:1479) comes before `releaseWave` (:1483). `releaseWave` is the last statement, and `return true` reads nothing through `this`. A D member function has no implicit epilogue access.
- **Subset:** `emitSubset` returns at once (:1453).
- **`Service.pump`** reads `next` before `emit` and does not touch `current` on true (:723-731).
- **Retire:** `truncated` returns false (:1480). Cleanup tables then run until `cursor_ == count_`, and the existing seal at :1446 is the single P release.
- **Zero admission (`written == 0`):** the rollback releases only the per-table credit while P still holds. `cursor_` is unchanged. On the next pass, either `cursor_ >= dispatched_` skips the check (already truncated, so the cleanup header is rebuilt), or the check runs again. Either way it is correct, and P is released exactly once.
- `cursor_ < dispatched_ ≤ count_/k_`, so truncation can never happen at `cursor_ == count_`.

**4. R4 — PASS.** Readers of `dispatched_`:
- `finish`/`checkResultsWritten` (:1500). On the emitter this follows program order. On a consumer it follows from the final acq_rel RMW in W.pending's modification order, which reads after the seal RMW.
- `outcome()` (:1244) and `results()` (:1278). Both load `active` with acquire *before* any `dispatched_`/`resultPrefix` read (:1239-1241, :1275-1278), and fatal if it is nonzero.
- `start`/`startSubset` write it only after rejecting `active != 0`.

Consumers (`runChild`, `writeResult`) never read it. While P is held, no reader runs at the same time as the emitter's write.

**5. R5 — PASS.**
- `PayloadBroadcastRange` copies the header into `hdr` at construction (antfarm.d:697-702), and the table serialises from that copy. The stack `cleanupHeader` therefore only has to live through `writeTracked`, and it does.
- `emitResume` (:1489-1494) still publishes `park.header_`, so resumed and `childCleanup` members reach `runChild`.
- Cleanup covers `[cursor_, count_)` after truncation. Resumable members come only from the written prefix `[0, cursor_)`, so the two ranges are disjoint.
- An undispatched member has `childFlags == 0` and no H. Every previous-wave exit path clears the flags: the `releaseMember` done path, `runChild`'s holding path, and the cleanup activation.
- Its child cohorts are idle. A plain member's waves are upstream-joined to the previous W. A continuation member's waves are held by H until `childPending` drains.
- That previous join happens-before `start`, then enqueue, then table, then consumer. `childrenIdle`'s acquire load therefore sees `active == 0`, and the cohorts' post-idle tails have already finished.
- Each member is retired exactly once: the prefix through `runChild`/the cleanup activation, the suffix through `runSkippedRetire`. `finish`'s `count_ = 0` is then correct.

**6. R13 — PASS.**
- **Emitter on cohort D:** P pins D. D's credit is on `upstream = W`, or on `X.childPending`. In the second case X holds H on W from before its custody release (:1574-1578). Before that, X's own table or resume credit pins W. So no gap exists in which W can finish.
- **Consumers:** the same chain, starting from their table credit.
- Ancestor Entries live in pinned parks' `entries_`. Retirement frees `state` but not `enclosing`, and a park is disposed only after its wave is idle.
- **Root identity:** `parent is root` is checked before `parent.enclosing` (:333-335). `create` reads `parent.enclosing` only when `!parentIsRoot` (:1205). The creating member is running, so its `enclosing` was set at its spawn (:1310) and published by the dispatch.
- The root Entry is pinned by the tree's root credit (`retainJoinCredit(root.pending)`, or a nested chain ending there).

**7. Byte 35 / TSan — PASS.** In the C11/D model, offsets 32/33/34/35 are separate memory locations, and compilers may not widen stores onto neighbouring locations. TSan shadows by offset and size, so 1-byte atomics next to plain bytes are not reported as a race.
- Child-Entry writes to `enclosing` (offset 32, 8 bytes) go only to child Entries.
- The raw reset at spawn (:843) follows `reclaim` on the same emitter. Every in-tree writer is ordered before it, through either:
  - root `pending` final acq_rel, then `rootReturns` rel CAS, then acq_rel exchange; or
  - Batch return (rel CAS, then acq_rel exchange) for a `finish` leaf whose activation called `cancelChildren`.

**8. Other — PASS.**
- **Wakes:** no new wake source and no credit change, so a lost or double wake is impossible.
- **Seal count:** the seal happens exactly once, because a descriptor that returned true is never re-queued.
- **Cascade from a truncation seal on the emitter:** `finish`, then `releaseWave(upstream)`/`releaseMember`/`releaseRoot`. The same cascade exists today for a normal final seal. It disposes nothing on the emitter, and any disposal it enables needs a later pump.
- **Subsets:** prefix in selection-position space, `resultCount_ = k_`, `firstFailurePosition` unchanged. Correct.
- **Results:** `resultPrefix` does not depend on `count_`, so retiring results survive.
- **Debug unwritten-slot check:** covers exactly the slots that ran the operation (including continuation members resumed later, whose results are written before the final W decrement).

## Risks (ranked)

1. **Missing deliverable (doc, not code).** `actors/ACTOR_MEMORYORDER.md` is not modified. It still says (around :320) that cancellation "is not part of this slice", and that `dispatched` is not a prefix. Design §6 requires a "Private cancellation and outcomes" section (R1–R13) and the two non-edges. Add it before landing.
2. **Minor semantic: a stale cancel onto an idle wave** (schedule in Q2) makes `outcome().cancelled` true while `succeeded()` stays true, makes `park.cancel` return true with no effect, and sets the root summary byte (the tree then takes the slow path). There is no lifetime impact. Suggestion: document `cancel`'s return value as "false if observed idle". Document `cancelled` as "a cancel was requested during this generation", which does not imply truncation. A C-test is optional.
3. **Comment accuracy (minor).** The `scopeCancelled` header comment says "w is active and pinned". That is true for its callers, but `cancelWave` can legitimately target a just-idled wave. A one-line note on `cancelWave` would stop a future maintainer from adding a walk there.

## Suggested next step

Write the ACTOR_MEMORYORDER section (R1–R13, both non-edges, plus the stale-idle-store case from Q2), adjust the two doc comments, and land. No code change is required for correctness.
