# F4a public inbox-node outcomes — 2026-10-06

Base: local main `b782ce4` (includes F1). Branch: `feature/public-node-outcomes`.
Worktree: `/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree`.
No push; no main-checkout modification.
Benchmark commit: `8cc8203`. Runtime/tests/docs commit: `119debb`.
The evidence commit changes no compiled sources, so the same passing final
matrix and soak apply to its tip.

## Scope and cost

`ActorNodeOutcome`, `node.fail(code)`, `node.cancel`, and outcome/code accessors.
The reserved word becomes completion bits (8-bit outcome, 24-bit code). Node
stays 32 bytes, with the word at offset 12; private Entry stays 40 bytes.
LDC and DMD compile these static assertions. `send` is untouched by
construction. `complete` gains one plain 32-bit store before the existing
release store; no new production RMW, allocation, or dispatch branch.

Outcomes are read after `available` acquires completion for that interaction,
before reuse or transfer. Initialization does not imply a completed reply.
F4b cancel-mode retirement is not implemented. The public guide, memory-order
chart, both roadmaps, and compiled public-service example are updated.

## Plan adjustment

No existing public benchmark exercised send/pop/complete. The orchestrator
approved a minimal `inbox` mode in its own first commit (`8cc8203`), before the
runtime change. Baseline = archived `b782ce4` plus that benchmark-only commit;
candidate = the F4a runtime diff. `actor` is the existing wake-mode control;
`wave` is the other unchanged-path control. The mimalloc archive in the main
checkout was linked read-only, as authorized; its checksum is in metadata.

## Deterministic tests and hand-run mutations

The delegated test author (CyDo task 192) ran mutations in `/tmp/f4a_mut`,
leaving the worktree runtime untouched. All listed deterministic mutations
were caught under LDC; M1 and D1/D4 were also caught under DMD. The results
below were reported by that task; native M1' was explicitly not counted as a
successful kill. Parent self-review reused `StressConsumer` instead of a new
identical consumer loop; the final matrix covers that test-only adjustment.

| Test | Mutation | Result |
| --- | --- | --- |
| P1 completion ordering | M1: FREE release; hook; completion write, in all three terminal methods | LDC and DMD fail: `node published FREE before its completion word (M1)` |
| P1 handoff stress | M1': hook; FREE release; completion write | Native LDC/DMD runs miss it; TSan reports a data race in `outcome()` |
| P1 reuse transitions | Remove complete's word write | stale outcome caught |
| P1 reuse transitions | complete/cancel update only the outcome byte | stale failure code caught |
| P1 reuse transitions | fail/cancel use `\|=` | stale bits caught |
| P1 fail(0) | Omit failed outcome byte for zero code | completed outcome caught |
| P1 24-bit maximum | Truncate code to 20 bits | code mismatch caught |
| P1 initialize | Remove reset | dirty-storage reset fails first; reinitialize-after-failure check not separately mutation-verified |
| P4 oversize failure | D1: remove range check | death harness rejects survivor; maximum legal code is accepted first |
| P4 ownership | D2/D3/D4: remove fail/cancel/complete processing check | death harness rejects survivor |

P1 covers every ordered pair of complete, fail(7), cancel, fail(0), and
fail(0xFFFFFF) on one reused node; payload/tag preservation; deterministic
pauses over stale outcomes; and 20,000 ping-pong handoffs. Sender outcome reads
precede consumer joins. P4 covers oversize code, free/queued misuse, and all
nine repeated-terminal-call pairs, requiring nonzero exit, the exact fatal
message, and no survivor marker. The existing death harness runs these under
LDC/DMD debug and release. Linux-only re-exec harness; runtime tests portable.

## Correctness

Benchmark-only commit: `make check` passed; LDC and DMD release benchmark
builds and inbox runs with 0 and 6 consumers passed. Final `make check` passed, including full actor_torture LDC/DMD/TSan, Farm,
review torture, private suites, compiled doc examples, four death lanes and
compiled application examples. `make check-soak` passed: 73 TSan loops in
122 seconds. Results are recorded in `verification.txt`.

## Performance

Five alternating runs per revision, per mode and profile: 3 measured seconds,
5 warmups, 16384 actors, batch 256, small quota 16384, ordinary pages, pinned
controller CPU 0; six-consumer profile pins workers to CPUs 1–6. Real mimalloc
3.5.3, LDC `-O2 -release -d-version=AntfarmMimallocV3 -i`. See `measure.py`,
`metadata.json`, `runs.jsonl`, and `summary.json` for reproduction and raw data.
Candidate was measured before its runtime commit; binary SHA256s identify the
actual measurements. Only a layout static assertion was added after that
binary was compiled; executable behavior is unchanged.

A run is contaminated when mean busy fraction outside its affinity exceeds
10%. All six-consumer runs were contaminated, so they establish no quiet
multicore gate. Quiet single-thread means stay above the relaxed −5% threshold;
no quiet reproducible regression worse than −5% was observed.

| Mode | Consumers | All-run mean delta | Quiet mean delta | Contaminated runs |
| --- | ---: | ---: | ---: | ---: |
| inbox | 0 | -2.61% | -2.77% | 1/10 |
| actor | 0 | +0.17% | -0.42% | 3/10 |
| wave | 0 | -0.58% | +0.33% | 3/10 |
| inbox | 6 | +0.20% | unavailable | 10/10 |
| actor | 6 | +0.10% | unavailable | 10/10 |
| wave | 6 | -2.18% | unavailable | 10/10 |
