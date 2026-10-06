# Consultant review of design §5.3 (F3 root control word), implementation at worktree task 37, base c8b4bf2

## Answer

**Verdict: PASS with changes.** I found no linearizability, overwrite, stale-match or data-race defect in the protocol or in the diff. The required changes are about hardening and accuracy: one debug assertion, the test seam, and the §5.6 cost wording. None of them changes the protocol.

## Reasoning

**Q1, R7/R8: linearizable as claimed.**
- Every write to `control` is one of four kinds:
  - canceller, `cancelAll` and lazy-deadline CASes (`private_actor.d:121`, `:453`, `:1307`). Each requires `live` in the expected word, and the canceller also requires a matching generation.
  - the terminal CAS (`:1174`), which clears `live`.
  - the spawn plain store (`:1066`), which writes over VACANT.
  - the take plain store (`:1274`), which writes over FINISHED.
- A slot reaches spawn only through `free_`. `free_` is fed only by `create` (records start zeroed, so the word is VACANT(0)) and by `takeOutcome` (`:1275`). So both plain stores replace non-live words, and no CAS can write a non-live word.
- A canceller CAS that succeeds is therefore always on LIVE(g) of its own generation. It is ordered in the modification order before the terminal CAS. The terminal CAS uses a strong `cas` that updates `w` on failure, so it reads the cancelled bit, and the status is computed from that same `w` (`:1177-1179`).
- `requested` therefore implies the outcome is `cancelled` or `failed`, never `completed`.
- A canceller holding an old `w` (LIVE(g)) fails its CAS after the word moves to FINISHED(g), VACANT(g+1) or LIVE(g+1). Within one generation the word never returns to a live value once FINISHED, so there is no ABA.
- Every emitter transition runs inside `enter()` on the single emitter, so no two emitter transitions race.

**Q2, R10: the first cause wins and is never overwritten.** Cause bits are written only together with `cancelled`, and only by a CAS whose expected word lacks `cancelled`. The terminal CAS keeps them (`w & ~live`), and take resets them only when the generation advances. The claim "in-tree callers know the root is live" holds for every caller:
- **Root context** (`:256`): the root is running under custody, so it is not collected.
- **Member** (`scopeCancelled:470`): it runs under a W table credit, which chains to `root.pending` or to the root's `hasJoinCredits` (R13).
- **Emitter `truncated()`** (`:1817`): it holds producer hold P on W, so W cannot finish, its parent credit is not released, and the root cannot be collected.
- **Members in a `finishAfterChildren` drain:** they run before the join that sends the root to `rootReturns`. The cleanup activation runs with `entry.cleanup` and makes no user call, so it never checks. A child wave's `finish` releases the root after its `active` store and does no check (`:1855-1861`).
- **Service pointer:** `ChildBase.service` is the root park's `Service` (propagated through `context.service`, `:562` → `:1542`), so `base.service.records` and `rootRecord(base.service, base.root)` index the right array.

The post-loop `return true` in `rootCancelledNow` (`:458`) when `!live` is unreachable for these callers. That is acceptable, but see Risk 1.

**Q3, R11/X1a: ordered on all three paths.**
- **Leaf finish:** activation writes, then the Batch-return release CAS, then the `returned_` exchange (acq_rel, `:1093`), then `reclaim`/`retainOutcome`.
- **Joined finish:** activation writes, then the Batch return, then the emitter `releaseRoot` RMW (acq_rel, `:643`). The final decrementer's acq_rel RMW reads within the release sequence, then comes the push CAS (rel, `:653`), then the `rootReturns` exchange (acq_rel, `:1120`). Members' and cleanup activations' writes follow the same chain.
- **Consumer reclamation:** the only difference is `freeMemory` in `runRoot` (`:1366`) before the return. It does not affect record fields.

`takeOutcome` runs on the emitter after collection (program order), so the X1a edge holds. I found no TSan-visible race between the fields:
- `RootRecord` fields are distinct memory locations.
- The foreign thread touches only `control` (atomic) and `index`, which is written once in `create` (`:978-980`) before any handle exists.
- `deadline`, `failed` and `failureCode` are written by spawn before publication (`:1062-1064`, then `appendReady` and the `writeTracked` release).
- Consumers read `deadline` only while the root is live.
- `status` is emitter-only.

**Q4, memory orders: sufficient and slightly generous.**
- The canceller's and the in-tree CASes are acq_rel, and the in-tree check is an acquire load. Together they give a useful guarantee that §5.3 should state: writes made before `requestCancel` are visible to any in-tree activation that observes `cancelled`, and to the emitter after `takeOutcome` returns `cancelled`.
- The two release stores (spawn `:1066` and take `:1274`) publish nothing that a foreign thread reads, because cancellers read no plain field. They are not load-bearing. That costs nothing on x86 and an `stlr` on ARM. Either keep them and comment that they are not load-bearing, or relax them to raw.
- The initial acquire loads in `retainOutcome` (`:1169`) and `requestCancel` could be raw. That is minor.
- The raw load in `takeOutcome` (`:1260`) is correct. Read-read/write-read coherence on the emitter's own terminal CAS guarantees it sees that value or a later one, and no thread writes a non-live word. Spawn's raw load of VACANT is correct for the same reason: its last writer is the emitter itself (take) or zero-initialisation.

**Q5, live/recycling: no double recycle or leak.**
- `reclaim` has two mutually exclusive callers: the leaf path `:1108`, and the `rootReturns` path `:1134` for roots with `hasJoinCredits` or a non-finish disposition. Each terminal root reaches it exactly once.
- `releaseRoot`'s write to `entry.next` (`:652`) happens before the push CAS, and the collector reads `next` before calling `reclaim` (`:1133`). So after `retainOutcome` sets `entry.next = null` (`:1180`), nothing else writes `next` until `takeOutcome` pops the entry.
- `live_` is decremented only in `takeOutcome` on tracked parks.
- spawn's failure returns (`:1037-1039`) happen before the slot is popped.
- The `destroy` live check covers untaken outcomes.

**Q6, generation wrap: the bound is correct.** There are 48 bits at shift 16 and `maxRootGeneration = 2^48-1`. The fatal at `:1263` fires before `(g+1) << 16` would overflow to VACANT(0). Default handles have `record_ is null`, so they are stale. I found no other path that matches a stale handle. This closes 173's "generation exhaustion" item.

**Q7, `cancelAll`: sound.**
- Recording `deadline` is an observation. Because the clock is read lazily at most once per call, a deadline that expires during the scan is reported as `requested`, which is acceptable.
- Counting deadline conversions in the return value matches the documented contract ("this call cancelled").
- Reading `deadline` plainly is safe: the emitter is its only writer, and `cancelAll` itself runs on the emitter.

## Risks (ranked)

1. **Hardening, worth a debug assertion.** `retainOutcome` (`:1171-1176`) CASes unconditionally. If a future change ever routed one entry to `reclaim` twice, it would silently re-append the entry to the FIFO and create a cycle. Add `version (assert) if (!(w & controlLive)) fatal("private root terminal: not live");` before the CAS. Add a similar check in `takeOutcome` that the popped word is non-live. A matching assertion in `rootCancelledNow` would turn the "in-tree callers know the root is live" claim from documentation into a checked invariant.
2. **Test seam race (test-only).** `privateTestSetRootGeneration` (`:599`) raw-stores over a LIVE word. Interleaving: a test thread calls `requestCancel` and its CAS succeeds; the root activation then raw-stores the new generation with the old flag bits. The cancel is lost and the outcome ends `completed` after `requested` was returned. Fix: make the seam a CAS loop that keeps flags, or document "no concurrent cancellers/deadline" and keep those tests single-actor.
3. **§5.6 cost wording is inaccurate.** On untracked parks:
   - `spawnRoot` gains a `records !is null` branch (`:1056`).
   - the root context's `cancelled` now loads `entry.service` and `records` (it was a constant before; harmless).
   - park size grows by 32 B (`Service.records`/`rootEntries` plus the outcome head and tail; the test changes 168→200).

   Fix: amend §5.6 to "Untracked: spawn, reclaim and the member check each gain one `records !is null` branch, and the park grows by 32 B." Alternatively, move `outcomeHead_`/`Tail_` behind the records allocation. I do not think that is worth it.
4. **Documentation.** Two points:
   - State the canceller-release guarantee (Q4) in §5.3 and in the `requestCancel` doc.
   - State explicitly that the application must join every foreign thread that might call `requestCancel` before calling `destroy`. Records are freed at `:1330`, so a canceller still inside its loop would use freed memory. "While the park exists" implies this, but it is the only cross-thread lifetime rule in the API.
5. **Minor performance note, already accepted under Q9.** An armed, unexpired root reads the clock on every member `cancelled` check and before every initial child table in its tree (`scopeCancelled` checks the record before the `subtreeCancel` fast path). This matches §5.6 ("clock reads only for armed roots"), but the benchmark should include an armed, not-expired case.

## Suggested next step

Add the two debug assertions from Risk 1 and fix or document the seam from Risk 2. Amend §5.6 and §5.3 (Risks 3 and 4), then land. Then add the following tests:
- a TSan/stress test of `requestCancel` against the terminal CAS, using hooks `privateRootCancelBeforeCas` and `privateRootTerminalBeforeCas`. It should assert that `requested` implies `cancelled` or `failed`.
- a stale-handle test across `privateTakeBeforeVacantStore`.
- an armed-deadline test where the conversion is first observed by the emitter's `truncated()`.
