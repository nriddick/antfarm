# E2-4 execution checklist

- [x] Intake and approved plan review (tasks 86, 89, 70, 74; e2-4.md).
- [x] Create feature/private-continuations from 632c7f6 in task 37 worktree.
- [x] Adopt reproducer: none supplied; task 86 probe is a design reference.
- [x] Implement core handoff spike; obtain TSan-clean evidence.
- [x] Implement API, holds, return FIFO, resume tables and deferred retirement.
- [x] Test sub-task: deterministic C cases, death tests and compile-time coverage.
- [x] Independent consultant lifetime review of all four hardest points.
- [x] Docs, compiled child-fold example and roadmap updates.
- [x] tree-cont performance mode and pinned alternating mimalloc measurements.
- [x] Run and record every C-M mutation.
- [x] Self-review: code reuse, quality, efficiency and diff.
- [x] Verify make check, make check-soak and at least 600 seconds private TSan soak.
- [x] Commit atomic changes, with each commit built/tested under LDC and DMD.

## Clarifications

Orchestrator response before coding: rootService takes (Entry*, Wave* enclosing),
rejects a non-null enclosing wave in debug and retains its null-service check.
Root call sites supply structural context, never infer identity from Entry bits.
Keep parentIsRoot_ in PrivateChildPark; add resume/FIFO fields to ChildBase.

C-M13 clarification: debug production FIFO invariant catches the exact omitted-tail-clear mutation; correct head-based append otherwise hides the stale tail.
C-M10 clarification: debug write-once result tags catch the exact extra wait-time write; tags share the result allocation, adding one byte/member only in debug.
C-M7 clarification: mutant-only erased retire callback is approved; canonical runtime must not retain it.
Independent lifetime review: task 139, lifetime-review.md; four hardest points pass by source trace, no required fixes. Runtime tests remain separate evidence.

## Review and verification revision

- [x] Adopt verifier CAS-stall forcing reproducer and fix publication barrier.
- [x] Reject nested/disposition PrivateChildStep result types with compile controls.
- [x] Explain releaseRoot root-only proof.
- [x] Audit C3, C4, C5, C7, C9 and C14 publication/barrier ordering.
- [x] Confirm forced CAS schedule passes and C-M12 remains caught.
- [x] Trim evidence to summaries and prioritize subset/tree quiet measurements.
- [x] Self-review the focused revision.
- [x] Rerun make check and make check-soak twice.
- [x] Run at least five minutes private TSan soak with CPU load.
- [x] Fold changes into three atomic commits and verify each with LDC and DMD.
- [x] Prepare final SHA/evidence execution report for parent (qid 1791129190).

Revision gates: make check passed; check-soak passed twice (79 and 78 loops, each reporting 122 seconds); loaded private TSan passed 65 loops in 303.236 seconds with 16 stress processes. Rewritten runtime/docs commits and the exact evidence source tree each passed LDC/DMD private suites. LDC/mimalloc and DMD/CRT benchmark builds and eight smoke cases passed. The orchestrator approved recording exact runtime/docs SHAs and identifying the tracked evidence report's own commit as "this commit"; all three final SHAs are returned in the execution report.
