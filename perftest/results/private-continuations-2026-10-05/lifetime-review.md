# E2-4 independent lifetime review (uncommitted runtime, base 632c7f6)

## Answer

**PASS: I found no lifetime, ordering or exclusivity defect.** All four hardest points pass. The other items checked also pass: the reset of `c := 1`, absence of overlapping activations, credit-target snapshots, subset results, result-before-destructor ordering, Root/Service lifetime, no allocation on completion, and no new RMW on plain/result paths. The orchestrator-approved additions (structural `rootService`, the C-M13 FIFO invariant and the C-M10 written flags) are sound. Two of them are weaker than they look; see Risks 1 and 2. No fix is required before commit. There are three low-priority hardening and documentation items.

Scope: I traced `actors/private_actor.d` (working tree) and the Farm `writeImpl` path (`antfarm.d:1479-1717`, hook at `2494`) by hand. I did not run tests, TSan or mutations.

## Reasoning

### Four hardest points

**H1: H is taken before custody release and before the table is dropped. PASS.**
- In `runChild`, `entry.childStep = next` and then H is taken (`W.pending` fetchAdd, `private_actor.d:1289`). Both happen strictly before `releaseMember(entry)` at `:1292`.
- The activation's own table credit (initial or resume table, both on `&wave.hook`) is dropped only by `waveCompleted`. Farm calls `waveCompleted` after every callback in the table returns (`antfarm.d:2494-2501`). So W.pending ≥ 1 at the moment H is taken, and H exists before the table credit can drop.
- The resumed activation skips the take (`flags & childHolding`) and keeps the existing H.
- The C-M5b schedule needs a final custody decrement with nothing outstanding (`waitChildren` yield) plus a late H. That schedule is reachable here, because the yield path goes through `releaseMember`, which pushes.

**H2: the P release is the last park access, including the Service `waiting` traversal. PASS.**
- In `emitResume` (`:1197-1218`), the table credit is taken before `writeTracked`. A zero-admission rollback happens while P still pins W.
- `resumeCount` is read after publication. This is safe: P pins W, and only the emitter writes `resumeCount`.
- The order is `resumeScheduled = false` (`:1212`), then `releaseWave(wave)` (`:1213`), then a null seam. Nothing touches `base`, `park` or `emittable` after that.
- `Service.pump` snapshots `current.next` before `emit`. It writes `current.next` only on the `false` path, where P is still held (`:428-437`).
- The drain takes P (`:419-423`) before `&base.resume` is linked into `waiting`.
- The new emitter-side final owners do not invalidate the snapshotted `next`. These are a P release that finishes W, which then runs `releaseMember`/`releaseWave` up the tree, and a D seal that pushes a member. Any descriptor still on `waiting` holds its own P or producer hold, so its wave cannot finish under the traversal.
- An emitter-side push lands after this pump's `childReturns` exchange. It stays queued for the next pump; it is never lost.

**H3: independent ResumeBodies cursor, tail cleared before visibility, no accepted `Entry.next` read after publication. PASS.**
- `ResumeBodies` (`:871-900`) captures `{park, cursor, remaining}`. `save` is a value copy, and `front`/`popFront` use only `cursor`. `park.resumeHead` is never read through the range.
- `prepareTableFrontN` walks n links from its own `cursor`, then sets `resumeHead`/`resumeCount`, and clears `resumeTail` on a full drain (`:892`). It always returns true.
- In Farm, prepare runs after the sizing loop and before the `Wt` reservation (`antfarm.d:1619-1620`). No `return 0` follows it.
- Farm's copy loop takes `payloads.save` *after* prepare (`:1680`). The `ResumeBodies` copy therefore still starts at the original cursor, so accepting A from [A, B] publishes A, and B stays at the head.
- The last `emit.popFront()` reads `next` of the last accepted Entry before the sentinel store-release (`:1704` vs. `:1709`). Zero admission never calls prepare, so the FIFO is untouched.
- After `writeTracked` returns, `emitResume` reads only `base.resumeCount`.
- Later appends write `resumeTail.next` only on an unaccepted Entry, which the invariant guarantees.

**H4: deferred retirement when the emitter seal is the unique final owner; destructor and free placement. PASS.**
- Schedule: a member in a `startFinal` wave returns `done` while D is outstanding. `childStep = doneRetire` is taken, H is taken, and the custody decrement is nonfinal. Later the emitter's `emit` seal of D drops the last D credit, and `D.finish` → `releaseMember(m)` → `old == 1`.
- On that path, `doneRetire` only sets `childCleanup` and pushes (`:335-338`). Nothing is retired or freed on the emitter.
- The next pump enqueues m, and a consumer runs the cleanup activation (`:1247-1253`). That activation calls `retireMember`, which runs `disposeChildren` (post-order), then `~T`, then the free. It then clears the flags and drops H while the resume table credit still pins W.
- D's idle store precedes its `releaseMember` in `finish`, and the order chain continues through acq_rel `c` → release push → acq_rel exchange → table publication. So `childrenIdle` in `disposeChildren` cannot fire spuriously.
- The result is written by the `done` activation (`:1264-1271`), before both the inline retire (`:1275`) and any later cleanup.

### Other required checks

- **The `c := 1` reset (`:1254`) is safe.**
  - For a resumed or cleanup activation, the previous final owner's push CAS was its last access, and every other decrement observed `old > 1` and was its holder's last access.
  - For the first activation of a new wave, the previous participation ended in one of two ways. Either a final owner left `c` at 0, which is ordered through W's join → parent idle acquire → the new `start` → publication. Or the fast path acquire-loaded `c == 1` after all of that activation's retains, leaving a dormant 1.
  - Plain/result waves never touch `c`, because `ownJoin_` is false and `publishWave` routes to `upstream`.
  - No late D tail can be pending at the reset. For the member to resume, or for W to finish, that tail's decrement must already have happened (`releaseMember` is the tail's final action).
- **No overlapping user activations.** Activation k+1 is published only after k's custody release and the final-owner push. After `releaseMember`, the old `runChild` touches only a null test seam (`:1293-1294`). Yield with nothing active always pushes and is never re-called inline.
- **Credit-target snapshots.** `finish` snapshots `upstream`, `parent` and `parentIsRoot_` before the idle store, and touches only those locals afterwards (`:1220-1235`). `parentIsRoot_` agrees with the starting context: `authorized` forces the parent's context, and a root context has `enclosing_` null, a child context non-null. So exactly one of the three releases is chosen.
- **Same-wave subset results.** The slot is `sel_[capacity_ + idx]` (`:1268`). `sel_` and `header_` are parent-written before the `incoming` CAS, and a scheduled P or H keeps W active, so `startSubset` returns `waveActive`. Resume tables reuse the subset `header_`.
- **Root/Service lifetime.** `pushChildReturn` and the drain reach Service and ChildBase through `member.enclosing` while H is held. W being active implies a credit chain up through ancestors' `c`/H to `root.pending ≥ 1`, which implies `live_ ≠ 0`, so `PrivateRootPark.destroy` fails loudly and does not free.
- **Allocation and RMW cost.**
  - Completion paths do no allocation.
  - The plain/result `runChild` paths add no RMW: `writeResult` is a plain store in release.
  - `publishWave` adds RMW only on the new `ownJoin` branch.
  - `pump` adds one raw hint load.
  - Continuation `done` without descendants costs one raw store and one acquire load.
- **Sizes.** The `Entry == 40` static asserts are intact (`:174-183`). ChildBase grows by Emittable 16 + head/tail 16 + count 8 + bool padded to 8, which is 48 B.

### Orchestrator-approved additions

- **`rootService(entry, enclosing)`** is correct, and it does not infer anything from pointer bits.
- **C-M13 invariant** (`:893-896`): correct. Tail omission really is behaviorally masked, because the drain appends based on `resumeHead`. So the debug assertion is the only detector, and it fires on the first full drain under the mutation.
- **C-M10 written flags** (`:930-950`, resets at `:1091`/`:1127`, checks at `:1038`/`:1225`):
  - The trailing bytes start at `results_ + capacity_`. The allocation and free sizes match (`resultBytesPerMember` in create/fail/dispose).
  - The reset happens parent-side while W is idle, and the flags are covered by the same W credit chain as the slots.
  - Each slot's flag byte is a distinct location, so there is no TSan race.
  - An `empty` start leaves both the results and the flags unchanged (ports semantics).
  - Correctly, a `waitChildren` slot write followed by `done` is otherwise invisible: the slot is overwritten before the parent can read it. The design's "tag mismatch" catch for C-M10 was therefore wrong, and the flag check is what catches it.

## Risks (ranked)

1. **Low: the `rootService` guard is tautological at production call sites.** `PrivateContext.service()` calls it only when `enclosing_ is null`, and `releaseRoot` and `runRoot` pass a literal `null` (`:112`, `:295`, `:825`). The `enclosing` check therefore guards only the test accessor (`:278`). Root identity actually rests on the `parentIsRoot_` snapshot in `finish`. If a future bug routed a child Entry into `releaseRoot`, the nonzero continuation bytes (step, flags and `c`) would be misread as a `Service*`. The `null` check catches only the dormant all-zero word. This is acceptable as approved, but the review notes should say the guarantee is structural (`parentIsRoot_`) and not an assertion.
2. **Low, a test-process gotcha: C-M10 and C-M13 are detectable only in assert builds.** Mutation runs for those two must use debug LDC/DMD, and the review notes should record this. This is a separate issue from the design-wide `version(assert)` template-layout caveat: debug allocations use capacity×(R.sizeof+1). That caveat already exists for `PrivateRootPark.entered_`, so it is not new.
3. **Low, test robustness: `privateRootAfterIdleBeforeCredit` now fires on all three `finish` paths** (`:1230-1231`), not only for root parents. Existing tests arm it once (`private_actor_tests/main.d:1979, 3029, 5757`). In a nested tree, the first firing could be a non-root-parent wave. The core tests pass, but the full suite and soak should confirm this, or the hook should be renamed or split.
4. **Hardening, optional:** add a debug assertion in `dispose` (`:1313`) that `!resumeScheduled && resumeHead is null`. Idle already implies this through P, so it is defense in depth only.
5. **Doc nits:**
   - The `start` doc comment (`:1073-1074`) still says retire "forbids launching descendants". That is false for continuation ops.
   - The `retireMember` comment (`:468`) still says "future child cleanup activations".

## Suggested next step

Commit the runtime unchanged. In the review notes, record PASS on H1–H4 with the evidence above, plus Risks 1–2 as explicit caveats. When the test author resumes:
- run C-M10 and C-M13 in debug builds;
- check that the relocated idle hook does not mis-target the existing tests;
- the C7 emitter-seal variant should assert that the cleanup `~T`/free thread is not the emitter, with the push observed on the emitter thread.

## Implementation follow-through

The review's runtime caveats are documented in PRIVATE_ACTORS.md. Stale
terminal-operation and retirement comments were corrected. After this review,
a test-only null seam was added immediately after finish's child-parent
releaseMember call; it performs no Entry, park or Service access and does not
change the credit protocol. The new C4 test initially allowed emitter ownership
at a consumer-only barrier; its deterministically reproduced self-deadlock and
corrected forcing are recorded in test-evidence/late-owner-investigation/.
The runtime itself required no lifetime correction.
