# Contributing to Ant Farm

Ant Farm is concurrent, `@nogc` code whose correctness rests on stated
memory-order arguments. Read the owning document for the area you are changing,
keep its rules and the implementation in step, and use the checklist below to
prepare your contribution. There is no CI; the correctness gate is a local
command.

During development, batch related edits and run the smallest useful check of
the changed behavior. Local commits do not require the full gate; report checks
that remain deferred. Before pushing, establish the applicable gate evidence
for the accumulated change. Agent session routing, local iteration, hardening,
TSan budgets and mutation selection are defined in
[docs/development/agent-workflow.md](docs/development/agent-workflow.md).

## The correctness gate: `make check`

Run these commands from the repository root:

```sh
make check-quick SHELL="$(command -v bash)"
make check SHELL="$(command -v bash)"
```

GNU make and the check scripts need Bash. Pass `SHELL` explicitly on hosts
without `/bin/sh`, including when invoking a sub-suite's makefile directly.

`make check` stops at the first failure, prints `ALL CHECKS PASSED` on success, and must be
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

Before pushing, satisfy these obligations for the accumulated change:

- Use meaningful deterministic regression tests for changed properties where
  feasible. A passing stress run alone is not evidence of a memory-order argument.
  The agent process's focused mutation policy is in
  [docs/development/agent-workflow.md](docs/development/agent-workflow.md#mutation-selection).
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

## Adding a test lane

Add the lane's target to the root [Makefile](Makefile) and register it in
[scripts/check.sh](scripts/check.sh) when it belongs in the full correctness
gate. Update `make help` so contributors can find it. Decide whether it belongs
in `make check-quick` too: that target dispatches to `scripts/check.sh --quick`,
whose quick branch runs the portable LDC-only checks. Keep its toolchain
requirements consistent with the description above.

Document the command, coverage, prerequisites and limitations in
[docs/development/testing.md](docs/development/testing.md), including whether
it runs in the full gate, the quick check, or only on explicit selection.

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
