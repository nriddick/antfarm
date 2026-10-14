# Contributing to Ant Farm

Ant Farm is concurrent, `@nogc` code whose correctness rests on stated
memory-order arguments. Work proceeds through local iteration and pre-push
hardening. Local iteration uses focused evidence to make useful progress;
hardening establishes acceptance with the local gate, meaningful regression tests
and applicable performance comparisons. There is no CI; the gate is a local command.
The active CyDo session controls routing and delegation; project guidance specifies
correctness obligations, not an additional planning, review or verification pipeline.

## Local iteration (default)

Batch related edits, then run the smallest useful check of the changed behavior:
a build, focused test selector, reproducer, or inspection for a prose-only edit.
Add a focused behavioral test when it helps establish the increment. Local commits,
hand-offs and cherry-picks do not require the full gate, a compiler matrix, mutation
campaigns, extended soaks or performance comparisons. Report those as deferred.

Keep invariants and memory-order reasoning with the implementation. Fix defects
that prevent the requested behavior from working; record unrelated bugs and
optional improvements without chasing them. After a correction, rerun affected
checks and reuse applicable evidence from other agents. Never claim an unrun check.

## Hardening boundary

In `crew`, hardening runs before an authorized remote push or on explicit request.
Its Astra assessor reviews, runs checks, fixes material defects and validates the
result directly; do not add a separate reviewer, verifier or implementation handoff.
The standalone `harden` session provides the same complete assessment when selected
explicitly. `iterate` keeps its Sonnet/Opus/Sol workflow and does not automatically
start hardening; Sol satisfies any applicable pre-push project checks directly.

For pre-push scope, assess the accumulated change using an explicit base and final
candidate, not just the last commit. Apply the checklist below once, reusing
applicable evidence. An explicit narrow review stays within its requested scope
and does not establish pre-push clearance. If corrections invalidate a gate, rerun
it on the final candidate; a new commit ID alone does not invalidate evidence for
an identical tree and equivalent environment.

Full matrices and adversarial work belong here. Select additional probes for the
changed risks; do not repeat every historical mutation or soak by default. Finish
when required checks pass and material blockers are resolved. Keep unrelated or
optional findings separate. Hardening does not itself authorize a push.

General TSan soaks share a **600-second collective budget** for the requested change
or hardening pass, including all commands, agents, retries and correction rounds.
Record consumed time and use only the remainder; a handoff or new candidate does
not reset the budget. Hardening/concurrency work may use narrowly tailored TSan
checks for concrete hypotheses, each capped at **300 seconds**. Enforce deadlines
for the whole run and child processes; timeout is not PASS. These are ceilings,
not required durations, and no soak is mandatory merely because hardening runs.

## The correctness gate: `make check`

```text
make check
```

It stops at the first failure, prints `ALL CHECKS PASSED` on success, and must be
run from the repository root on Linux x86-64. Bare `make` runs it too. The full
lane matrix, prerequisites, focused entry points, and optional lanes are in
[docs/development/testing.md](docs/development/testing.md).

For a portable local iteration check, run `make check-quick`. It runs the root
unit tests, plain review-torture suite, threadpool unit tests, fiber smoke suite,
and LDC debug/release examples. It needs the LDC toolchain (including Dub), Python
3, GNU make and Bash; it uses neither DMD nor TSan. `make check` retains the full
matrix and remains the correctness gate.

The full gate accepts `CHECK_SMALL_CPUS` and `CHECK_ONE_LLC_CPUS` as `taskset -c`
CPU lists. By default the small-mask confine lane uses the first four CPUs in
the process's allowed affinity, and host tests use the full allowed mask. With
fewer than four or sixteen allowed CPUs respectively, the lane prints a notice
and runs unpinned. Explicit overrides are passed verbatim to `taskset`; choose
one LLC for `CHECK_ONE_LLC_CPUS` when testing that topology. Invalid overrides
fail the lane. Inspect selection with `scripts/check.sh --print-cpus`.

## Performance: `make perf-gate`

`make perf-gate BASE=<sha> CANDIDATE=<sha>` compares two committed refs on the
same benchmark source and decides whether the candidate regresses beyond the
project boundary. It is separate from `make check`. The method, the modes, and
the decision rules are in [perftest/README.md](perftest/README.md); the measured
numbers and the tuning guide are in [PERFORMANCE.md](PERFORMANCE.md).

Changes to executable code or its layout can move performance even when the
algorithm is unchanged, so treat a clean diff as no evidence of identical
performance.

## Change checklist

During implementation of changes to the Farm, actors, or shared lifecycle:

- Read the normative text for the area first (see the ownership map in
  [docs/README.md](docs/README.md)), and keep the text and the code in step. A
  change to behavior changes the owning document in the same commit.
- If a change alters a memory ordering, state the happens-before edge it relies
  on: in [SPEC.md](SPEC.md#9-memory-order-contract) for the Farm and in
  [docs/design/public-actor-ordering.md](docs/design/public-actor-ordering.md)
  or [docs/design/private-actor-ordering.md](docs/design/private-actor-ordering.md)
  for actors. Do not weaken an ordering without the proof that replaces it.
- Keep the TSan lanes meaningful: do not add annotations that hide a weakened
  edge (see [review_torture/FARM_ORDERING.md](review_torture/FARM_ORDERING.md)).
- New test code stays on druntime threads and atomics, and any new example is a
  compile target in `examples/run_examples.py`.
- If you change a documentation program, the README First payload, or an
  example, `make check` extracts and runs them. Mark partial code blocks as
  fragments.

For the pre-push scope, satisfy these obligations within the same assessment:

- Use meaningful deterministic regression tests for changed properties where
  feasible. Select a focused mutation only when needed to demonstrate detection
  of a concrete changed risk, such as a weakened ordering; do not rerun historical
  mutation catalogs or require mutation testing for every change. A passing stress
  run alone is not evidence of a memory-order argument.
- Contract violations that must be fatal need a death test in
  `private_actor_tests` or an equivalent that survives `-release`.
- Establish `make check` evidence for the final candidate when runtime, tests,
  build inputs or extracted example programs change. Pure prose/configuration
  changes use their relevant validation; do not run unrelated runtime matrices.
  If executable code on a measured path changed, also establish `make perf-gate`
  evidence. Reuse still-applicable results instead of repeating these gates.
- Do not claim a gate that was not run. Say which lanes passed, on which
  compilers and host, and which you did not run.

Commit message and trailer conventions are the maintainer's; follow the style of
recent commits (`git log`).

## Documentation map and ownership

Each rule has one document that states it in full; other documents summarize and
link. Update the owner first. The map and the full ownership index are in
[docs/README.md](docs/README.md).

## Reading before you change code

- **Farm:** the layout, invariants, producer, subscription and reclamation
  sections of [SPEC.md](SPEC.md), then
  [review_torture/FARM_ORDERING.md](review_torture/FARM_ORDERING.md).
- **Public actors:** [docs/reference/actor-lifecycle.md](docs/reference/actor-lifecycle.md),
  [docs/guide/public-actors.md](docs/guide/public-actors.md), then the relevant
  proofs in [docs/design/public-actor-ordering.md](docs/design/public-actor-ordering.md).
- **Private actors:** [docs/guide/private-actors.md](docs/guide/private-actors.md),
  then root custody and final-access in
  [docs/design/private-actor-ordering.md](docs/design/private-actor-ordering.md#publication-and-reclamation),
  and the private proofs in
  [docs/design/private-actor-ordering.md](docs/design/private-actor-ordering.md)
  and [docs/design/private-actor-layout.md](docs/design/private-actor-layout.md).
