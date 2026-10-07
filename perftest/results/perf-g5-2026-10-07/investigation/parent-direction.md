# Inert-size control direction

Parent clarification during the full five-pair gate, 2026-10-07:

> First resolve the /0 cells with 20 pairs + byte-identical control. Only if a
> /0 cell then shows a ≤−3% (or still inconclusive near −3%) result do you need
> the inert control. Exact offset matching isn't required. Use base plus
> never-executed inert text placed in the same module/near the changed
> functions (so downstream code shifts by a comparable amount), sized to the
> candidate's displacement magnitude (~112–193 B; pick one, e.g. ~128 B, and
> say which). Run it as base vs base+inert, 20 pairs on the affected cells. If
> that still doesn't separate layout from the real change, report the data
> and ask me — don't waive.

The candidate's whole-binary text delta is -112 B; its public loop delta is
+193 B. This clarification permits a positive inert-size control of comparable
magnitude rather than an exact negative-size or hot-function-offset twin.

## Correctness scope clarification

Parent confirmed that `make check` (default actor_torture under LDC/DMD/TSan)
plus `make check-soak` meets the planned correctness scope. Also run the
`run-mimalloc` actor_torture target after timing, using the existing release
archive. Skip optional debug/TSan mimalloc targets requiring new archives,
and report those omissions. The parent also confirmed 20 pairs plus identical
controls for actor/wave/inbox/6 and subset100/6.

The `run-mimalloc` direction above was subsequently superseded: the existing
release archive is present, but `third_party/mimalloc/CMakeLists.txt` is missing
in this worktree. The Make target requires that source prerequisite. Parent
instructed: skip `run-mimalloc`, do not hand-roll its recipe or populate
submodules, and report the skip. `make check` plus `check-soak` is sufficient.
The real-mimalloc benchmark builds and timings already used the existing
release archive successfully; optional allocator-specific torture targets
were not run.
