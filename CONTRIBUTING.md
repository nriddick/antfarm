# Contributing to Ant Farm

Ant Farm is concurrent, `@nogc` code whose correctness rests on stated
memory-order arguments. Changes are accepted on evidence: the local gate below,
tests that fail when the property they protect is broken, and, for executable
changes, a performance comparison. There is no CI; the gate is a local command.

## The correctness gate: `make check`

```text
make check
```

It stops at the first failure and prints `ALL CHECKS PASSED` on success. Run it
from the repository root on Linux x86-64. Bare `make` runs it too. `make help`
lists every target.

| Lane | What runs | Compilers |
| --- | --- | --- |
| Root suite | `dub test` (module `unittest` blocks plus `antfarm_test.d`) | LDC, DMD |
| Farm ordering | `make check-farm-ordering`: controlled subscription/reclamation schedules and the leaf-reuse test, with write-audit hooks | LDC, DMD |
| Farm TSan | `make run-farm-tsan`: the root suite and `leaf_reuse.d` under ThreadSanitizer | LDC |
| Actor torture | `actor_torture`: deterministic interleavings and sustained contention for the public actors | LDC, DMD, LDC TSan |
| Review torture | `review_torture`: adversarial Farm tests | LDC, LDC TSan |
| Private actors | `private_actor_tests`: unit suite, the extracted [PRIVATE_ACTORS.md](actors/PRIVATE_ACTORS.md) documentation programs, and the fatal-death harness | LDC, DMD, LDC TSan |
| Examples | `examples/`: the First payload program extracted from [README.md](README.md), run in debug and release only; and the six actor examples, debug and release with 0 and 2 workers, plus an LDC TSan build (`fiber_cancel_at` with 0 workers only) | LDC, DMD; TSan on the actor examples only |

`ANTFARM_HUGE_PAGES=0` is exported for every lane, so the matrix does not depend
on how the calling shell is configured.

Not covered by `make check` (run these yourself when you change those
packages): the `threadpool` and `fibers` suites (`dub test` in each package;
see [threadpool/README.md](threadpool/README.md) and
[fibers/README.md](fibers/README.md)), the review-torture write-protection
(`allwrite`) targets, and `examples/iota_sum.d`. Performance is not part of it
either (see `make perf-gate`). Windows is validated by hand after a release
candidate and is not part of this gate.

### Prerequisites

- DMD 2.112+ and LDC 1.42+, with the LDC ThreadSanitizer runtime installed
  (the TSan lanes fail to link without it).
- Dub, GNU make, Python 3 and Bash. The matrix itself uses no other tools.
- `/bin/sh` is not required. GNU make execs recipes without shell
  metacharacters directly, and `scripts/check.sh` carries its own Bash shebang.
  When `/bin/sh` is missing it passes `SHELL=/usr/bin/bash` to child makes,
  because their TSan recipes quote `TSAN_OPTIONS` and GNU make never takes
  `SHELL` from the environment. If you invoke a sub-suite's makefile directly on
  such a host, pass `SHELL=/usr/bin/bash` yourself.

### Focused entry points

| Command | Use |
| --- | --- |
| `make check-farm-ordering` | the Farm ordering and leaf-reuse tests (LDC and DMD) |
| `make run-farm-tsan` | the Farm suite under ThreadSanitizer |
| `make -C actor_torture run run-dmd run-tsan` | public actor suite |
| `make -C review_torture run run-tsan` | adversarial Farm suite |
| `make -C private_actor_tests run run-dmd run-tsan doc-example death` | private actor suite, documentation programs and death tests |
| `make -C examples check` | example programs and the README program |

Each suite has its own README with selectors and the seams it uses:
[actor_torture](actor_torture/README.md),
[review_torture](review_torture/README.md) and
[private_actor_tests](private_actor_tests/README.md).

### Optional and longer lanes

- `make check-soak` runs the private-actor TSan suite in a loop for at least 121
  seconds. It builds only that target and does not run the matrix first, so run
  `make check` separately.
- `make check-mimalloc` runs the normal matrix, then the real allocator lanes
  (`actor_torture` with the pinned mimalloc v3.5.3, release and TSan). It needs
  the submodule (`git submodule update --init --recursive`), CMake and a C
  toolchain. See [third_party/README.md](third_party/README.md). Other allocator
  targets (`run-mimalloc-debug`, `run-mimalloc-dmd`) are in
  [actor_torture/Makefile](actor_torture/Makefile).

## Performance: `make perf-gate`

`make perf-gate BASE=<sha> CANDIDATE=<sha>` compares two committed refs on the
same benchmark source and decides whether the candidate regresses beyond the
project boundary. It is separate from `make check`, builds the refs in
temporary worktrees, needs a quiet Linux host with LDC, Python 3 and the pinned
mimalloc archive, and its output directory must be new. An inconclusive result
is not a pass. Run the correctness gate first; run timing serially with nothing
else heavy on the host. Everything else (methodology, conventions, how to read
the output) is in [perftest/README.md](perftest/README.md#optimization-pr-performance-gate)
for now.

Changes to executable code or its layout can move performance even when the
algorithm is unchanged, so treat a clean diff as no evidence of identical
performance.

## Change checklist

Before proposing a change that touches the Farm, actors or the shared
lifecycle:

- Read the normative text for the area first (see the map below), and keep the
  text and the code in step. A change to behavior changes the owning document
  in the same commit.
- If a change alters a memory ordering, state the happens-before edge it relies
  on: in [SPEC.md](SPEC.md#9-memory-order-contract) for the Farm and in
  [ACTOR_MEMORYORDER.md](actors/ACTOR_MEMORYORDER.md) for actors. Do not
  weaken an ordering without the proof that replaces it.
- Add a deterministic test for the property, and check it by mutation: break
  the code (for example weaken the ordering, or remove the check) and confirm
  the test fails; the suites' READMEs record the existing controls. A passing
  stress run alone is not evidence of a memory-order argument.
- Contract violations that must be fatal need a death test in
  `private_actor_tests` or an equivalent that survives `-release`.
- Keep the TSan lanes meaningful: do not add annotations that hide a weakened
  edge (see [review_torture/FARM_ORDERING.md](review_torture/FARM_ORDERING.md)).
- New test code stays on druntime threads and atomics, and any new example is a
  compile target in `examples/run_examples.py`.
- If you change a documentation program, the README First payload, or an
  example, `make check` extracts and runs them. Mark partial code blocks as
  fragments.
- Run `make check` with the final revision. If you changed executable code on
  a measured path, also run `make perf-gate`.
- Do not claim a gate that was not run. Say which lanes passed, on which
  compilers and host, and which you did not run.

Commit message and trailer conventions are the maintainer's; follow the style
of recent commits (`git log`).

## Documentation map and ownership

Each rule has one document that states it in full; other documents summarize
and link. Update the owner first.

| Topic | Owner |
| --- | --- |
| Farm algorithm, table layout, compact ST tables, callback header, memory-order contract | [SPEC.md](SPEC.md) |
| Layers, choosing a work representation, sizing one Farm, entries per quota, shutdown order | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Public actor API direction, inbox, retirement, waves, allocator and mimalloc adapter | [actors/ACTOR_ROADMAP.md](actors/ACTOR_ROADMAP.md) |
| Private actor ownership contract, API, sizing, batches | [actors/PRIVATE_ACTORS.md](actors/PRIVATE_ACTORS.md) |
| Actor lifecycle contracts shared by both models: completion, drain, tracked roots, cancellation, shutdown | [actors/LIFECYCLE.md](actors/LIFECYCLE.md) |
| Actor memory-order proofs | [actors/ACTOR_MEMORYORDER.md](actors/ACTOR_MEMORYORDER.md) |
| Farm ordering audit and real-atomic TSan lanes | [review_torture/FARM_ORDERING.md](review_torture/FARM_ORDERING.md) |
| What changed in each candidate and its compatibility notes | [CHANGELOG.md](CHANGELOG.md) |
| Remaining and deferred work | [ROADMAP.md](ROADMAP.md) |
| Runnable programs and their commands | [examples/README.md](examples/README.md) |
| Threadpool and Fibers | [threadpool/README.md](threadpool/README.md), [fibers/README.md](fibers/README.md) |
| Benchmarks and the performance gate | [perftest/README.md](perftest/README.md) |
| The local gate | this file and `scripts/check.sh` |

Paths in this table may move in a later documentation pass; the topic stays
with one owner.

## Reading before you change code

- **Farm:** the layout, invariants, producer, subscription and reclamation
  sections of [SPEC.md](SPEC.md), then
  [review_torture/FARM_ORDERING.md](review_torture/FARM_ORDERING.md).
- **Public actors:** [actors/LIFECYCLE.md](actors/LIFECYCLE.md),
  [actors/ACTOR_ROADMAP.md](actors/ACTOR_ROADMAP.md), then the relevant
  proofs in [actors/ACTOR_MEMORYORDER.md](actors/ACTOR_MEMORYORDER.md).
- **Private actors:** [actors/PRIVATE_ACTORS.md](actors/PRIVATE_ACTORS.md),
  including root custody and final-access, then the private proofs in
  [actors/ACTOR_MEMORYORDER.md](actors/ACTOR_MEMORYORDER.md).
