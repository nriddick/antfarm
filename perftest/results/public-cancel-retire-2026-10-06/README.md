# F4b public cancel retirement — 2026-10-06

Base: local main `2e47beb` (F1, F4a, F2, F3). Branch: `feature/public-cancel-retire`.
Worktree: `/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree`.
No push; no main-checkout modification. Runtime/tests/docs commit: `8892357`.
This evidence commit changes no compiled sources.

## Scope and cost

`ActorRetireMode { drain, cancel }`, `requestRetire(mode = drain)` on both owner
forms, a sticky lifecycle `CANCEL` bit (0x20) set only together with `RETIRE`,
drain-to-cancel escalation by CAS against the full word, `context.cancelling`
and `context.cancelInbox()`. Cancel on an already cancelling or retired actor
returns `alreadyRetired`; drain behaviour and result codes are unchanged.
Retirement stays cooperative (Q12): the library never discards nodes. No final
handler call is guaranteed (Q15), as documented in LIFECYCLE.md and
ACTOR_MEMORYORDER.md. Concurrent `requestRetire` on aliases of one owner is
unsupported; sequential escalation is supported.

The slot stays 128 bytes and the node stays 32 bytes. Private Entry is
untouched (40 bytes). A static assertion checks that `CANCEL` is disjoint from
phase/RETIRE/PENDING and below the generation shift. Unused cost: the seven
rebuild sites copy `observed & (RETIRE|CANCEL)` instead of materializing
`RETIRE` conditionally. No new atomic operation, branch on the dispatch path,
or field.

## Propagation sites (actors/actor.d at `8892357`)

| Site | Location | Transition |
| --- | --- | --- |
| S1 | `ActorRuntime.enter`, line 1032 | closing, empty inbox, undrained gate: `SCHEDULED -> SCHEDULED` reschedule |
| S2 | `ActorRuntime.enter`, line 1045 | `SCHEDULED -> RUNNING` |
| S3 | `ActorRuntime.enter` exit CAS, line 1101 | `RUNNING -> SCHEDULED/IDLE` (RETIRED clears) |
| S4 | `signalActor`, line 1239 | `IDLE -> SCHEDULED` |
| S5 | `driveActorRetirement`, line 1319 | idle with undrained gate: `IDLE -> SCHEDULED` |
| S6 | `cancelActorWavePayload`, line 1575 | wave admission rollback |
| S7 | `dispatchActorWavePayload`, line 1637 | wave operation exit |

## Consultant review

The independent review (CyDo task 222, `propagation-review.md`) audited all 16
writes to the lifecycle word. S1–S7 is complete. Every other write ORs into the
observed word, clears at RETIRED/VACANT, starts a fresh generation, or is the
wave reservation from an exact bare `IDLE` word. No schedule loses an accepted
cancel before a later activation. The only `requested` cancel without a
`cancelling` activation is retirement with an empty inbox and drained gate,
which has nothing to cancel. Acted on:

- The memory-order doc was stale. It now covers the bit, the seven sites, the
  terminal clears, owner serialization including aliasing, and a "cancel
  request to callback" handoff row.
- The alias comment overstated the hazard. It now says that an overwritten
  alias cancel only misreports the result code, because the inbox is empty.
- `alreadyRetired` while still cancelling, and `waveOwned` during a wave,
  are now documented on `requestRetire`.
- The suggested wave-window and running-carry tests were already present.

## Deterministic tests and hand-run mutations

The test author (CyDo task 221) added 13 tests to `actor_torture`. All 11
mutations were rerun from scratch by `mutate.py` on an unpacked `git archive`
of `8892357`, under LDC and DMD: 22 of 22 were caught (`mutations.json`).

| Mutation | Catching test | Failure |
| --- | --- | --- |
| `cancelling` reads RETIRE | drain control (P2) | drain retirement completes the whole backlog |
| `cancelling` returns false | backlog (P2) | cancelInbox cancels exactly the whole backlog |
| escalation early-returns on RETIRE | queued escalation (P3) | cancel escalation returns requested |
| S1 keeps only RETIRE | idle reschedule | late message cancelled after the reschedules |
| S2 keeps only RETIRE | backlog (P2), also P5 | cancelInbox cancels exactly the whole backlog |
| S3 keeps only RETIRE | escalation while running (P6) | exit after escalation cancels the carried remainder |
| S4 keeps only RETIRE | signal during idle retirement | signalled cancellation reaches the handler |
| S5 keeps only RETIRE | idle reschedule | late message cancelled after the reschedules |
| S6 keeps only RETIRE | wave rollback (P7) | rollback exits with the cancel mode preserved and no wave call |
| S7 keeps only RETIRE | wave admission (P7) | wave exit preserves the cancel mode into the handler |
| `cancelInbox` drops the carry | inbox carry (P2) | cancelInbox includes the carried remainder |

Escalation is also exercised against consumer entry (sender paused at
`activationSignalled`) and across the idle join (`retirementIdleObserved`).
The existing late-sender regression (`testRetirementJoinsLateSignal`) is
unchanged. P8 shows that an idle, empty actor retires on cancel with zero
handler calls. No death tests are new in F4b.

## Correctness

`make check` passed at the tip (`check-tip.txt`). That run covered:

- dub LDC/DMD and the Farm TSan suite;
- the full actor_torture suite on LDC, DMD and TSan, including the new
  "actor cancel-mode retirement OK" lane;
- review torture;
- the private suites, including the doc example and the debug/release
  death lanes;
- the compiled examples.

`make check-soak` passed with 48 private TSan loops in 122 seconds. An
additional 10-minute actor_torture TSan loop ran 688 times with no failure
(`check-soak.txt`).

## Performance

Five alternating runs per revision per mode and profile. Each run used:

- 3 measured seconds, 5 warmups, 16384 actors, batch 256;
- small quota 16384, ordinary pages, pinned controller CPU 0;
- six-consumer profile workers on CPUs 1–6;
- real mimalloc 3.5.3, LDC `-O2 -release -d-version=AntfarmMimallocV3 -i`.

The baseline is `2e47beb` from `git archive`; the candidate is `8892357`.
Binary SHA256s are in `metadata.json`.

Pass 1 ran while other sessions held a load average of 3–8. All 60 runs were contaminated:
outside-affinity busy was 19–44%, mean 31%. Its −9.9%/−7.0% six-consumer
deltas are noise. They are kept as `runs-pass1-contaminated.jsonl`,
`summary-pass1-contaminated.json` and `perf-pass1.txt`, and are not used for
the gate.

Pass 2 (`runs.jsonl`, `summary.json`, `perf-pass2.txt`) ran on a quiet host.
No run was contaminated; maximum outside-affinity busy was 4.2%.

| Mode | Consumers | Base Mactor_cycles/s | Candidate | Mean delta | Paired range |
| --- | ---: | ---: | ---: | ---: | --- |
| inbox | 0 | 14.913 | 15.102 | +1.27% | +0.88% to +1.51% |
| actor | 0 | 19.056 | 19.266 | +1.10% | +0.34% to +1.83% |
| wave | 0 | 23.751 | 24.136 | +1.62% | +1.07% to +2.45% |
| inbox | 6 | 12.025 | 11.966 | −0.49% | −0.72% to −0.08% |
| actor | 6 | 14.233 | 14.163 | −0.49% | −0.81% to −0.01% |
| wave | 6 | 15.045 | 14.927 | −0.78% | −1.27% to −0.27% |

Every quiet delta is inside the original ±3% gate. No regression worse than
−5% reproduced under quiet conditions.
