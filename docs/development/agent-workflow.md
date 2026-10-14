# Agent workflow

This page defines the CyDo agent process. Human contributors should start with
[CONTRIBUTING.md](../../CONTRIBUTING.md).

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
candidate, not just the last commit. Apply the [change checklist](../../CONTRIBUTING.md#change-checklist) once, reusing
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

## Mutation selection

For the pre-push scope, satisfy this obligation within the same assessment:

- Use meaningful deterministic regression tests for changed properties where
  feasible. Select a focused mutation only when needed to demonstrate detection
  of a concrete changed risk, such as a weakened ordering; do not rerun historical
  mutation catalogs or require mutation testing for every change. A passing stress
  run alone is not evidence of a memory-order argument.
