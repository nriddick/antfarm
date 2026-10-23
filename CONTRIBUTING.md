# Contributing to Ant Farm

Start with `make check-quick SHELL=/usr/bin/bash` for local iteration.
A local Linux run on 2026-10-14 took **97.209 seconds** (about 1 min 37 s) on
an AMD Ryzen 9 5900XT with 32 allowed logical CPUs and LDC 1.43.0. Ant Farm is
concurrent, `@nogc` code whose correctness rests on stated memory-order
arguments. Read the owning document for the area you change, keep its rules
and implementation in step, and use the checklist below. There is no CI.

During development, batch related edits and run the smallest useful check of
the changed behavior. Local commits do not require the full gate; report checks
that remain deferred. The push gate below is the whole pre-push obligation.
The CyDo agent process is in
[docs/development/agent-workflow.md](docs/development/agent-workflow.md).

## The push gate

Stated once, here. Before any push:

1. **`make check` passes on the final tree.** That is the whole correctness
   obligation, including the TSan lanes it contains. A fix after the run means
   running it again on the fixed tree; an identical tree needs no second run.
   Targeted extra TSan runs for a concrete hypothesis are optional; wrap them
   in `timeout`. There is no budget to account for.
2. **Performance is a report, not a verdict.** When executable code on a
   measured path changed, run `make perf-gate` (five pairs per cell), rerun
   the flagged cells with `--boundary` (20 pairs), and record the medians and
   spreads in the CHANGELOG entry. The maintainer decides; the tool's
   classification and exit status block nothing. Prose and configuration
   changes need no measurement.
3. **Before a release**, the maintainer runs the native Windows matrix on the
   Windows host (`python -B scripts/check-windows.py`).

Say which of these ran, on which host and compilers, and which did not. Never
claim a check that was not run.

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

For a portable local iteration check, run `make check-quick`. It runs the BetterC/native-C link check, root
unit tests, plain review-torture suite, threadpool unit tests, fiber smoke suite,
and LDC debug/release examples. It includes the core C/C++ ABI check and needs the LDC toolchain (including Dub), Python
3, GNU make, Bash and C11/C++11 compilers; it uses neither DMD nor TSan. `make check` retains the full
matrix and remains the correctness gate.

On Windows x64, use `python -B scripts/check-windows.py` for the native
integration matrix, or add `--quick` for LDC packages, review and examples.
It needs neither Bash nor GNU make. Coverage, focused selectors and exclusions
are in [docs/development/testing.md](docs/development/testing.md#windows-x64-integration).

The full gate accepts `CHECK_SMALL_CPUS` and `CHECK_ONE_LLC_CPUS` as `taskset -c`
CPU lists. By default the small-mask confine lane uses the first four CPUs in
the process's allowed affinity, and host tests use the full allowed mask. With
fewer than four or sixteen allowed CPUs respectively, the lane prints a notice
and runs unpinned. Empty values count as unset. Explicit overrides are passed
verbatim to `taskset`; choose one LLC for `CHECK_ONE_LLC_CPUS` when testing that
topology. Invalid overrides fail the lane. Inspect selection with
`scripts/check.sh --print-cpus`.

## Performance: `make perf-gate`

Run major performance comparisons on the Ryzen 9 5900XT. The i7-12700H
Windows laptop remains a correctness-validation host; stable clocks over long
measurement runs are not expected, and no performance-table refresh is planned.

`make perf-gate BASE=<sha> CANDIDATE=<sha>` compares two committed refs on the
same benchmark source and reports each cell's paired change and spread. The
method, the modes, the flagging rule and the layout caveat are in
[tools/perf/README.md](tools/perf/README.md#performance-report); the measured
numbers and the tuning guide are in [PERFORMANCE.md](PERFORMANCE.md).

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
  edge (see [docs/design/farm-ordering.md](docs/design/farm-ordering.md)).
- A C ABI change keeps the headers, the D `extern(C)` definitions and
  [cabi/abi1.lock](cabi/abi1.lock) in step. `make check-c-abi` checks
  prototypes, struct layouts and numeric constants between headers and D, and
  the headers against the lock, in every lane except `--tsan`. Within ABI 1
  only additions are compatible: regenerate the lock with
  `python3 scripts/check-c-abi.py --write-abi-lock` and commit it with the
  header change.
- D test code stays on druntime threads and atomics. Native C ABI tests use
  native threads to verify runtime independence and foreign-thread attachment.
  Any new example is a compile target in `examples/run_examples.py`.
- If you change a documentation program, the quickstart First payload, or an
  example, `make check` extracts and runs them. Mark partial code blocks as
  fragments.

Tests for changed behavior:

- Prefer a deterministic regression test for a changed property. A passing
  stress run alone is not evidence of a memory-order argument. Add a focused
  mutation only to show that a test detects a concrete changed risk, such as
  a weakened ordering; do not rerun historical mutation catalogs.
- Contract violations that must be fatal need a death test in
  `private_tests` or an equivalent that survives `-release`.

Then the [push gate](#the-push-gate) applies to the accumulated change.

Commit message and trailer conventions are the maintainer's; follow the style of
recent commits (`git log`).

## Adding a test lane

Add the lane's target to its suite's Makefile under `tests/` (or to the root
[Makefile](Makefile) for a repository-wide check) and register it in
[scripts/check.sh](scripts/check.sh) when it belongs in the full correctness
gate. Update `make help` so contributors can find it. Decide whether it belongs
in `make check-quick` too: that target dispatches to `scripts/check.sh --quick`,
whose quick branch runs the portable LDC-only checks. Keep its toolchain
requirements consistent with the description above.

Windows-compatible lanes also belong in `scripts/check-windows.py` when they
contribute to native integration coverage. Keep its exclusions explicit; a
Windows pass does not replace the Linux correctness gate.

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
  [docs/design/farm-ordering.md](docs/design/farm-ordering.md).
- **Public actors:** [docs/reference/actor-lifecycle.md](docs/reference/actor-lifecycle.md),
  [docs/guide/public-actors.md](docs/guide/public-actors.md), then the relevant
  proofs in [docs/design/public-actor-ordering.md](docs/design/public-actor-ordering.md).
- **Private actors:** [docs/guide/private-actors.md](docs/guide/private-actors.md),
  then root custody and final-access in
  [docs/design/private-actor-ordering.md](docs/design/private-actor-ordering.md#publication-and-reclamation),
  and the private proofs in
  [docs/design/private-actor-ordering.md](docs/design/private-actor-ordering.md)
  and [docs/design/private-actor-layout.md](docs/design/private-actor-layout.md).
