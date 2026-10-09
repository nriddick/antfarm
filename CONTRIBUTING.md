# Contributing to Ant Farm

Ant Farm is concurrent, `@nogc` code whose correctness rests on stated
memory-order arguments. Changes are accepted on evidence: the local gate, tests
that fail when the property they protect is broken, and, for executable changes,
a performance comparison. There is no CI; the gate is a local command.

## The correctness gate: `make check`

```text
make check
```

It stops at the first failure, prints `ALL CHECKS PASSED` on success, and must be
run from the repository root on Linux x86-64. Bare `make` runs it too. The full
lane matrix, prerequisites, focused entry points, and optional lanes are in
[docs/development/testing.md](docs/development/testing.md).

## Performance: `make perf-gate`

`make perf-gate BASE=<sha> CANDIDATE=<sha>` compares two committed refs on the
same benchmark source and decides whether the candidate regresses beyond the
project boundary. It is separate from `make check`. The method, the modes, and
the decision rules are in [perftest/README.md](perftest/README.md).

Changes to executable code or its layout can move performance even when the
algorithm is unchanged, so treat a clean diff as no evidence of identical
performance.

## Change checklist

Before proposing a change that touches the Farm, actors, or the shared
lifecycle:

- Read the normative text for the area first (see the ownership map in
  [docs/README.md](docs/README.md)), and keep the text and the code in step. A
  change to behavior changes the owning document in the same commit.
- If a change alters a memory ordering, state the happens-before edge it relies
  on: in [SPEC.md](SPEC.md#9-memory-order-contract) for the Farm and in
  [docs/design/public-actor-ordering.md](docs/design/public-actor-ordering.md)
  or [docs/design/private-actor-ordering.md](docs/design/private-actor-ordering.md)
  for actors. Do not weaken an ordering without the proof that replaces it.
- Add a deterministic test for the property, and check it by mutation: break the
  code (for example weaken the ordering, or remove the check) and confirm the
  test fails; the suites' READMEs record the existing controls. A passing stress
  run alone is not evidence of a memory-order argument.
- Contract violations that must be fatal need a death test in
  `private_actor_tests` or an equivalent that survives `-release`.
- Keep the TSan lanes meaningful: do not add annotations that hide a weakened
  edge (see [review_torture/FARM_ORDERING.md](review_torture/FARM_ORDERING.md)).
- New test code stays on druntime threads and atomics, and any new example is a
  compile target in `examples/run_examples.py`.
- If you change a documentation program, the README First payload, or an
  example, `make check` extracts and runs them. Mark partial code blocks as
  fragments.
- Run `make check` with the final revision. If you changed executable code on a
  measured path, also run `make perf-gate`.
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
