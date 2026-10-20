# Agent workflow

This page defines the CyDo agent process. Human contributors should start with
[CONTRIBUTING.md](../../CONTRIBUTING.md).

Ant Farm is concurrent, `@nogc` code whose correctness rests on stated
memory-order arguments. Work proceeds through local iteration and a single
pre-push gate. There is no CI; the gate is a local command. The active CyDo
session controls routing and delegation; project guidance specifies
correctness obligations, not an additional planning, review or verification
pipeline.

## Local iteration (default)

Batch related edits, then run the smallest useful check of the changed behavior:
a build, focused test selector, reproducer, or inspection for a prose-only edit.
Add a focused behavioral test when it helps establish the increment. Local commits,
hand-offs and cherry-picks do not require the full gate, a compiler matrix,
soaks or performance comparisons. Report those as deferred.

Keep invariants and memory-order reasoning with the implementation. Fix defects
that prevent the requested behavior from working; record unrelated bugs and
optional improvements without chasing them. After a correction, rerun affected
checks and reuse applicable evidence from other agents. Never claim an unrun check.

## The push gate

Before an authorized remote push, or on explicit request, apply the
[push gate](../../CONTRIBUTING.md#the-push-gate) once to the accumulated change,
using an explicit base and final candidate rather than the last commit:
`make check` on the final tree, the performance report when a measured path
changed, and the Windows matrix before a release. In `crew`, the Astra assessor
reviews, runs the gate, fixes material defects and validates the result
directly; do not add a separate reviewer, verifier or implementation handoff.
The standalone `harden` session provides the same assessment when selected
explicitly. `iterate` does not start it automatically.

Select additional probes for the changed risks; do not repeat historical
mutation catalogs or soaks by default, and no soak is mandatory merely because
the gate runs. Any extra TSan run is optional and bounded with `timeout`;
there is no time budget to account for. If a correction invalidates a check,
rerun it on the final candidate; a new commit ID alone does not invalidate
evidence for an identical tree. Finish when the gate passes and material
blockers are resolved; keep unrelated or optional findings separate. The gate
does not itself authorize a push.
