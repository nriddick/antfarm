# F4b cancelBit propagation: consultant review

## Answer
The propagation is correct. S1–S7 covers every nonterminal lifecycle write that can observe `cancelBit`, and no site is missing. The other writes either OR into the observed word, clear the bit at a terminal state, start a fresh generation, or can only fire on a word that has no mode bits. I found no reachable schedule where an accepted cancel degrades to drain before a later handler activation. Nothing is a must-fix in code. The proof document is now stale and should be updated.

## Write table (current actor.d line numbers)

| # | Site | Kind | Nonterminal? | Can observe cancelBit? | Verdict |
|---|---|---|---|---|---|
| W1 | createActorErased → CONSTRUCTING, :834 | store | — (new gen) | No. The old word is VACANT, already cleared. | Fresh generation, no bits. Correct. |
| W2 | createActorErased → IDLE, :857 | store | yes | No. Its own CONSTRUCTING word is unpublished, and markActorRetiring rejects CONSTRUCTING. | Correct |
| W3 | returnFreeSlot → VACANT, :940 (reclaim and alloc-fail) | store | terminal | Reclaim: the word is RETIRED (already cleared). Alloc-fail: CONSTRUCTING. | Clears legitimately |
| W4 | enter S1 retire → RETIRED, :1017 | CAS | terminal | yes | Clears legitimately. Inbox and carry are empty and the gate is closed with zero count, so nothing remains to cancel. |
| W5 | enter S1 reschedule → SCHEDULED, :1025 | CAS | yes | yes | Preserved |
| W6 | enter S2 → RUNNING, :1038 | CAS | yes | yes | Preserved |
| W7 | self-retire markActorRetiring(retireBit), :1062 | CAS (OR) | yes | yes | Preserved: it ORs. If cancel is already present, `(obs & retireBit)==retireBit` returns `requested` with no write. The fatal fires only on `staleOwner`, as before. |
| W8 | enter exit S3, :1095 | CAS | RETIRED terminal, else nonterminal | yes | Preserved, or cleared at RETIRED |
| W9 | markActorRetiring, :1201 | CAS (OR) | yes | yes | Preserved: `observed | modeBits` |
| W10 | signalActor IDLE→SCHEDULED (S4), :1232 | CAS | yes | yes (allowClosing path) | Preserved |
| W11 | signalActor pendingBit, :1240 | CAS (OR) | yes | yes | Preserved |
| W12 | driveActorRetirement idle → RETIRED, :1309 | blind store | terminal | yes | Clears legitimately under the serialization contract (see Risk 2) |
| W13 | driveActorRetirement IDLE→SCHEDULED (S5), :1313 | CAS | yes | yes | Preserved |
| W14 | reserveActorWavePayload IDLE→SCHEDULED, :1523 | CAS (exact expected) | yes | Expected is a bare `IDLE` word with no bits, so it fails if retire or cancel is set | Not applicable. It never overwrites a mode bit. |
| W15 | cancelActorWavePayload (S6), :1567 | CAS | yes | yes (reservation window) | Preserved |
| W16 | dispatchActorWavePayload exit (S7), :1629 | CAS | yes | yes | Preserved |

The list is complete. A grep finds no lifecycle writer outside actor.d.

## Counterexample schedules
None found. The schedules I checked:

- **Wave reservation window.** W14 succeeds, then cancel is accepted because `waveOwnerWord` is still 0. markActorRetiring CASes `SCHEDULED|R|C`, and drive → signalActor sets `P`. After that, both rollback (S6) and payload exit (S7) see `interrupted`. They rebuild `SCHEDULED|R|C` and push once. enter then either retires (empty inbox) or reaches RUNNING with C intact. A pre-reserved sender paused before its push keeps the gate count above zero, so S1 reschedules with C rather than retiring.
- **S1 reschedule.** C is preserved on every loop iteration.
- **Escalation racing the exit CAS.** If the escalation CAS lands first, the exit CAS fails and reloads with C. The exit then goes to RETIRED (nothing to cancel) or to `SCHEDULED|R|C`. If the exit CAS lands first: on RETIRED, escalation returns `alreadyRetired`. On `SCHEDULED|R`, escalation CASes C in before the next enter, or races S2. A failed S2 CAS reloads, so either S2 carries C or the escalation lands on RUNNING and the following exit preserves it.
- **Carry.** Nodes left in `inboxLocal_` count as `hasInbox` and force a reschedule. `cancelInbox` pops the carry first.

The only way a `requested` cancel produces no `cancelling` activation is retirement with an empty inbox and a drained gate (W4/W8/W12). That loses no node, and the design's "no final handler call guaranteed" covers it.

**Q3.** cancelBit without retireBit is impossible. C is only ever set together with R (`retireModeMask`), every rebuild copies the mask as a whole, and every clear drops both. Nothing can leak into a later generation: W1 builds the new word from `lifecycleWord(gen+1, …)` with no flags, and W3 builds VACANT fresh.

**Q4.** Drain behaviour is unchanged. Before the change, every `true` outcome (RETIRED, already retiring, or CAS won) went on to closeSubmissionGate and drive. Now all three return `requested` and take the same path. Every `false` maps to `staleOwner`, as before. The self-retire fatal is equivalent. Cancel on RETIRED or an already-cancelling word returns `alreadyRetired` and skips close and drive. That is safe, because C or RETIRED implies the requester that set it already ran close and drive.

**Q5.** The memory ordering is sufficient. `cancelling` publishes no data; it is a flag. Coherence on L does the work: if the cancel RMW precedes S2 in L's modification order, S2 reads it and copies it, and the handler's later load in the same thread cannot read an older value. If the cancel comes after S2, the exit CAS carries it to the next activation. Acquire is more than needed; relaxed would also be correct. Keep acquire for consistency with `closing`.

**Q6.** `cancelInbox` terminates. Only senders whose reservation preceded the close can still push, and each reservation pushes exactly one node. Re-sends of a cancelled node by any other party are rejected because R is set. Pushes that land after `cancelInbox` returns are caught as `hasInbox` or `P` at exit, which yields another `cancelling` activation. One subtlety: the handler can see `cancelling` before the requester has closed the gate (mark comes before close). That window only admits senders whose opening load came before the mark, so it is still bounded.

## Risks (ranked)
1. **Should: ACTOR_MEMORYORDER.md is stale.**
   - The `L` row (:38) lists only RETIRE and PENDING.
   - The idle-fast-path text (:697–705) says "same `IDLE | RETIRE` word" and "unique owner". It needs a CANCEL bit, the S1–S7 preservation invariant, terminal clearing, and the owner-serialization contract, including that reference aliasing of a move-only owner is the unsupported case (finding #5).
   - Add a row to the ordering table (~:591) for "cancel request → `cancelling`".
2. **Nit: the comment overstates the alias hazard.** It says a concurrent alias's cancel is overwritten. W12 only fires when the inbox and carry are empty (it asserts so) and the gate is drained. If it overwrites an alias's C, the only observable difference is that the alias gets `requested` instead of `alreadyRetired`. No node escapes cancellation. Keep the serialization contract, but describe the consequence as a result-code inaccuracy rather than a lost cancel. Making W12 a CAS would cost one RMW at retirement, which is cheap if you would rather drop the caveat.
3. **Nit: `alreadyRetired` is returned for a live, still-cancelling actor.** A caller that then calls reclaim gets `busy`. Document this on `ActorRetireResult` or `requestRetire`.
4. **Nit: escalation returns `waveOwned` mid-wave.** A drain can be accepted in the reservation window and the wave then publishes ownership. A following cancel escalation returns `waveOwned`, and the caller must retry after the wave. Document this.
5. **Test (should).** Use the existing `waveLifecycleReserved` hook. Run cancel-in-window under both admission and rollback, with a paused pre-reserved sender so that the inbox becomes non-empty afterwards. Assert that the next handler sees `cancelling` and that the node's outcome is `cancelled`. Also run escalation while RUNNING with a carry left in `inboxLocal_`, and a drain→cancel sequence where cancel lands between W6 and the exit.

## Suggested next step
The code change is complete; land it as is. Update ACTOR_MEMORYORDER.md (Risk 1), soften the alias comment (Risk 2), and add the wave-window and carry tests from Risk 5.
