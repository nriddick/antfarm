# C4 late finish owner investigation

Parent failure: `parent-ten-minute-failure.log`. This is C4 (`CiMode.late`, numeric1), using the generalized `privateRootAfterIdleBeforeCredit` hook. C7 itself uses `CiMode.finalDone` and does not hold that hook.

## Root cause and stalled leg

Old C4 opened `leafGo` as soon as activation custody was released. This can happen before the emitter publishes the leaf table. A fast consumer can then finish and return that table while `writeTracked` is still in progress. The producer seal credit remains outstanding, so the emitter's seal release owns descendant wave finish. This is expected runtime credit ownership, explicitly exercised by C7.

The generalized hook runs on that emitter after descendant idle publication and before the parent member notification credit is released. It waits for `hookGo`. Only the same emitter, after `park.pump` returns, sets `hookGo`. The emitter cannot return from pump while waiting in its hook: a test-created self-deadlock. `leafArrived=leafGo=leafDestroyed=leafReturned=1`, `midCalls=1`, `emitter=1` identify the leg. No missing runtime wake is required; the test blocks the credit release that would produce it.

`force-emitter-owner.patch` is a **temporary diagnostic test-only** patch, compiled from `/tmp/private_late_owner_diag.d`; it was never applied to canonical tests or runtime. It runs the existing C7 zero-admission/publication barrier with `CiMode.late`, thus holding exactly the problematic generalized hook. `force-emitter-owner-build.log` and `force-emitter-owner.log` contain exact commands. With 16 CPU stress processes it reproduces the exact parent stall and abort (exit-6) at10.102s. Stress was concurrently supplied by the old full-suite baseline runner. Natural pre-fix exploration did not reproduce: 200 C4 targeted runs and21 full runs passed; the latter was manually interrupted during run22, not reported as60 passes.

## Test correction

`fix.patch` records the test change. C4 keeps `leafGo` closed until `leafArrived` is observed **after `park.pump` returns**. The table was necessarily published before the leaf entered, and returning from pump completes its producer seal while the callback is still held. Releasing the leaf afterwards establishes consumer ownership of descendant finish. Custody is already released before opening the gate. The held hook now asserts consumer ownership, released activation custody, and leaf entry/gate state. The completed test asserts that the hook was actually held/released. This does not skip emitter hooks or weaken the late final-owner assertion. C7 continues to force and verify emitter ownership separately.

Interleaving flags remain raw atomics; the fix adds no release/acquire or thread-join handoff across the result visibility assertion. Publication and result visibility still rely on runtime ordering. Diagnostics include custodyReleased, hookGo, returnCalls and midReturned. Targeted C4 and newly available `cont-seal-final-owner` selectors exercise both reclamation policies.

Runtime is unchanged. Parent confirmed canonical builds/runtime edits frozen for serial affected mutants C-M3/C-M4/C-M7/C-M7b; runner restores exact files in finally. Post-fix verification is recorded below after completion.

## Canonical verification

`validate.py` rebuilds all five binaries then runs all18 commands serially. `validation.json` and per-binary logs record exact command, exit, source/runtime hashes and external `time.monotonic()` elapsed time. All passed; the full TSan run took2.108s, continuation-only took0.389s. Both LDC/DMD debug and release full/continuation suites, full death harnesses and death negative controls passed. The concurrent extended diagnostic build is separate from the canonical binaries.

| Binary | Selector | Exit | External elapsed |
|---|---|---:|---:|
| private_actor_tests | full | 0 | 0.953s |
| private_actor_tests | cont | 0 | 0.072s |
| private_actor_tests | death | 0 | 6.984s |
| private_actor_tests | death-negative-control | 0 | 0.135s |
| private_actor_tests_dmd | full | 0 | 1.660s |
| private_actor_tests_dmd | cont | 0 | 0.156s |
| private_actor_tests_dmd | death | 0 | 6.194s |
| private_actor_tests_dmd | death-negative-control | 0 | 0.121s |
| private_actor_tests_release | full | 0 | 0.940s |
| private_actor_tests_release | cont | 0 | 0.080s |
| private_actor_tests_release | death | 0 | 7.276s |
| private_actor_tests_release | death-negative-control | 0 | 0.184s |
| private_actor_tests_dmd_release | full | 0 | 1.601s |
| private_actor_tests_dmd_release | cont | 0 | 0.121s |
| private_actor_tests_dmd_release | death | 0 | 5.776s |
| private_actor_tests_dmd_release | death-negative-control | 0 | 0.104s |
| private_actor_tests_tsan | full | 0 | 2.108s |
| private_actor_tests_tsan | cont | 0 | 0.389s |

## Affected mutation recheck

After the final gate predicate (which retains the outer completion deadline while waiting for leaf entry), C-M3, C-M4, C-M7 and C-M7b were rebuilt and run serially. All were caught (exit-6). `mutations/` preserves exact patches, compile/run commands and outcomes for this follow-up. C-M3/C-M4 hit the completion deadline; C-M7 detects emitter destruction; C-M7b detects the erroneous activation count. Canonical runtime/tests were restored exactly.

## Extended stalled state

A second TSan reproduction, concurrent with the corrected C4 loaded repeats and their16 CPU stress processes, produced exit-6 after10.119s:

```text
CI stalled mode=1 emitter=1 leafArrived=1 leafGo=1 leafDestroyed=1 leafReturned=1 midCalls=1 custodyReleased=1 hookGo=0 returnCalls=0 midReturned=1
FAIL cont final owner seam:0
```

`force-emitter-owner-extended.patch` contains the diagnostic-only change from the original tests; its build and run logs record exact commands. `custodyReleased=1`, `midReturned=1` confirm the initial activation returned. `hookGo=0` confirms the blocked release gate. `returnCalls=0` confirms the descendant finish has not reached the member return CAS because the test is holding its notification epilogue. This isolates the stopped leg without reading any potentially released runtime state.

## Reproduction commands

```sh
python3 perftest/results/private-continuations-2026-10-05/test-evidence/late-owner-investigation/validate.py
python3 perftest/results/private-continuations-2026-10-05/test-evidence/late-owner-investigation/stress_validation.py
python3 perftest/results/private-continuations-2026-10-05/test-evidence/run_mutations.py C-M3 C-M4 C-M7 C-M7b
```

Run mutations only with canonical builds/runtime edits frozen, as parent confirmed for these runs. The diagnostic patches are applied to the **pre-fix test source**, obtainable by reversing `fix.patch` in an isolated copy. Compile and run commands are in each diagnostic build/run log; diagnostic binaries live under `/tmp` and were not substituted for canonical binaries.

## Confirmed stalled emitter stack

`stalled-emitter-backtrace.log` captures the TSan diagnostic abort under GDB while the corrected full suite is under CPU load. The main thread is stopped in this exact chain:

```text
ciHook
actorTestPoint(privateRootAfterIdleBeforeCredit)
PrivateChildPark!(CiLeaf, ulong).finish                private_actor.d:1232
releaseWave                                          private_actor.d:353
PrivateChildPark!(CiLeaf, ulong).emit                  private_actor.d:1180
Service.pump                                         private_actor.d:432
PrivateRootPark!(CiRoot, ciRoot).pump                 private_actor.d:749
contSealFinalOwner
```

This confirms **leaf initial emission producer seal**, rather than a root/middle finish or a missing queue wake, owns the held notification epilogue. The test blocks its emitter before it can reach the outer loop which opens `hookGo`. The first GDB attempt could not execute its startup shell; that failure is preserved as `gdb-shell-startup-failed.log`. `set startup-with-shell off` produced the successful trace. This debugger run is diagnostic evidence, not counted among canonical passing tests.

## Loaded TSan results

`stress_validation.py` ran the three groups serially. Each group starts16 concurrent `python3 -c "while True: pass"` processes, runs immutable canonical TSan binaries, then terminates/waits for every stress child in `finally`. CPU affinity and source/runtime hashes are in each log. External elapsed uses `time.monotonic()`, independent of in-test clocks.

| Group | Process runs | Policies per run | Exit | External group elapsed |
|---|---:|---|---:|---:|
| cont-activation-first | 1000 | emitter + consumer | 0 | 65.197s |
| cont-seal-final-owner | 200 | emitter + consumer | 0 | 41.964s |
| full | 20 | emitter + consumer | 0 | 110.084s |

All three groups passed, with no TSan report. Total external driver elapsed217.368s. These counts are process invocations: C4 executes2000 forced consumer-owned finishes; C7 executes400 forced emitter-owned seals. The20 full-suite runs took110.084s; this is **not** the parent's requested ten-minute serial soak or three-minute loaded full soak. Parent owns those longer gates. Full commands, individual run exits/times and output are in `after-fix-c4.log`, `after-fix-c7.log`, `after-fix-full.log` and corresponding progress logs.

## Direct emitter-seal mutation checks and final restoration

To prove the forced C7 path itself catches the retirement mutations, `run_seal_mutations.py C-M7 C-M7b` ran **only** `cont-seal-final-owner`, rather than stopping first in an earlier general C7 case. Both mutants were caught (exit-6): M7 at `cont final stored destructor on consumer`, M7b at the expected activation-count check in `ciRoot`. `seal-mutations/` contains exact patches/commands/results. Runtime and tests were restored exactly afterwards. No canonical test runs occurred while any mutation was live.

After the final restore, TSan `cont`, `cont-activation-first` and `cont-seal-final-owner` passed again; `git diff --check` passes. `final-restoration-check.log` records commands, source hashes, exits and external elapsed times. Runtime SHA256 remains34abc0bc615910d574308c91c1d633f995f59d8aa45bed20d08f01059145929d. Final test SHA256 is6b411bb2b1a6e2576e0fb48ace6e27e72487d797eed454421266906000b5e33d. No runtime fix and no commits. Parent can release its build/edit freeze and run the longer serial/loaded full TSan gates.
