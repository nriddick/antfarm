Follow-up: C4 late finish forcing was corrected after a loaded soak exposed a test self-deadlock. See [diagnosis, exact test fix, mutation rechecks and final validation](late-owner-investigation/README.md).

# E2-4 private continuation test evidence

Tests are in `private_actor_tests/main.d`; no tests or runtime changes were committed by the test author. Runtime changes in this tree belong to the parent implementer.

## Reproduction

From the worktree root:

```sh
make -C private_actor_tests private_actor_tests private_actor_tests_dmd private_actor_tests_tsan private_actor_tests_release private_actor_tests_dmd_release -j3
ANTFARM_HUGE_PAGES=0 private_actor_tests/private_actor_tests cont
ANTFARM_HUGE_PAGES=0 private_actor_tests/private_actor_tests_dmd cont
ANTFARM_HUGE_PAGES=0 TSAN_OPTIONS=history_size=7:halt_on_error=1 private_actor_tests/private_actor_tests_tsan cont
make -C private_actor_tests run run-dmd run-tsan run-death run-death-dmd
python3 perftest/results/private-continuations-2026-10-05/test-evidence/run_mutations.py
```

The runner saves an exact unified patch, complete compiler/run command, exit status, elapsed time, and output for each mutation in `mutations/`. It uses separate `/tmp` binaries, runs one mutation at a time, and restores runtime and tests in `finally`. Canonical tests never run while a mutation is installed. Individual mutations can be selected by name.

## Coverage

| Spec | Selector / schedule |
|---|---|
| C1 | `cont-basic`: three child levels with typed results, exact calls, stored-instance destructor order/count, allocator balance |
| C2 | `cont-yield`: empty join, state from prior activations, exactly one dispatch per later pump, TLS nesting guard |
| C3 | `cont-descendants-first`: activation held before custody decrement; descendant notification has completed at the no-access finish seam |
| C4 | `cont-activation-first`: activation custody released before descendant is held after idle / before parent-credit release |
| C5 | `cont-hold-before-table`: parent sees suspended cohort active and start rejects it; empty yield held after final custody publication for late-H mutation |
| C6 | `cont-done-tails`: final value with outstanding leaf; one user activation, parent held until join |
| C7 | `cont-final-retire`: inline done retirement, deferred consumer final owner, and forced zero-admission then emitter-seal final owner; stored-state frees and destructors on consumer, exactly once, and count reset verified |
| C8 | `cont-resume-admission`: 513/1025 members, limits 1/64/256, full Farm rejects resumes, accepted partial prefix at limits 64 and 256, twice-yielded members run exactly three times |
| C9 | `cont-unschedule-race`: callback completion while P is retained; separate after-P seam lets consumer parent dispose cohort before emitter returns |
| C10 | `cont-results`: immediate/waiting members; final result tags; participant order [7,0,5,2]; continuation void clears slots |
| C11 | `cont-multi-gen`: six waves alternating result continuation, plain result, void continuation, plain void; multiple suspensions per continuation wave |
| C12 | `cont-nested`: depth-three result tree and depth-64 chain, five workers |
| C13 | `cont-return-cas-retry`: first member blocked with stale expected head, second member publishes on another table, first resumes and retries |
| C14 | `cont-fifo-drain-reuse`: full [A] drain with A held at its return CAS while B returns and is appended; full [A,B] drain and partial [A,B]/[A,B,C] drains; independent cursor and FIFO reuse |
| CT | both continuation forms accepted appropriately; mismatched steps, void steps, ambiguous result types rejected at compile time |
| CD | `cont-death`: counter exhaustion, strict root finish while suspended, and debug root-service misuse with a nonzero continuation child word |

`cont` runs all positive continuation cases under both reclamation policies. The regular default suite includes `cont`; deathHarness includes continuation deaths. Existing port/subset allocation and destructor slot-location probes were updated mechanically for the approved debug result-write tag bytes. The cohort layout comment now accounts for the 48-byte resume descriptor storage.

## Barrier and mutation notes

All interleaving flags use relaxed atomics. They do not publish result or actor-state writes. Callback completion is accounted after every consumeNext call, including false returns: idle sweeps can run callbacks. Accumulated TLS observations handle multiple callbacks in one consumeNext call. Audit hooks are installed before workers subscribe, remain immutable while workers run, and are cleared after join.

The parent added a no-access custody-after-release hook both in the activation epilogue and immediately after a descendant finish releases its child parent. C3 uses the latter to prove notification completion rather than merely callback completion.

C-M7 uses the explicitly approved mutant-only erased retire callback in ChildBase, initialized by typed cohort creation and invoked by the final owner. The mutation alone increases the cohort layout assertion by one pointer; no callback or layout change remains in canonical runtime.

C-M10 adds an actual extra wait-time result write through writeResult. Public results cannot expose an intermediate value in an active wave; the parent/orchestrator approved a production debug write-once tag to enforce the slot invariant. The extra store is caught by `private child result: slot written twice`. Release builds have no tag bytes.

C-M13 removes only the mandatory tail clear. The current append path tests head and therefore does not dereference a stale tail when empty. The parent/orchestrator approved a production debug empty FIFO invariant, which catches this exact mutation with `private resume FIFO invariant`, protecting the required invariant against future tail-based append changes. This is an invariant detection, not a claimed observed release-build corruption.

Batch limit 1 admits zero or one payload; positive partial admission is impossible there. All limits cover zero/retry and FIFO traversal. Publication audit explicitly proves the rejected pump published no table. A 1024-word quota forces a partial 54-member prefix for limit 64; a 4096-word quota forces a partial 225-member prefix for limit 256. Both cases assert a nonempty prefix smaller than the configured limit.

Full make check, check-soak, the ten-minute TSan soak, docs/performance gates, and commits remain owned by the parent task.

C-M12 initially survived a weaker barrier that allowed the emitter to exchange the head after observing a retry attempt. That could make the stale expected-null value valid again. The corrected test waits for the first producer to return **before** draining the competing head; the exact no-reload mutation then stalls and fails the bounded retry-completion check. The initial surviving result is preserved separately as `mutations/C-M12-before-barrier-fix.log`; it is not counted as the final result.

C-M5b also exposed an early test release on repeat: counting entry into the resumed user operation does not prove H was dropped. The corrected barrier waits until the resumed callback returns, then the polling parent must still observe waveActive while the old activation's table is held. The exact late-H mutation fails `cont old activation table still pins cohort after resume`. Its initial surviving repeat is archived in `mutations/C-M5b-before-barrier-fix.log`.

## Final results

All 18 required mutations caught; canonical runtime restored. Final builds, complete private suites (LDC/DMD/TSan), continuation tests in debug and release, all four death harnesses and negative controls, and 20 continuation repetitions per debug compiler/TSan passed. See [validation.md](validation.md) and [mutation-summary.md](mutation-summary.md).

## Verifier revision

Task 148 exposed a test-only C13 publication gap: A could reach its held return CAS before B's table was emitted. The fixture now keeps pumping until B starts, then preserves the existing wait for A's return before draining B's competing head. [The forcing reproducer](force-cas-stall.md) failed three times before the change and passed three normal plus 100 TSan invocations afterward. C-M12 remains caught at the retry-completion barrier.

The requested C3/C4/C5/C7/C9/C14 [publication/barrier audit](barrier-audit.md) found no additional stalled-publication pattern. Bare nested/disposition steps now have negative compile controls; both compilers also reject direct instantiation probes. Long non-mutation logs retain their first/last lines and original-output hashes; exact mutation logs and outcomes remain intact.
