# Testing

The correctness gate is local. There is no CI. [CONTRIBUTING.md](../../CONTRIBUTING.md)
has the change checklist; this page is the lane matrix, prerequisites, and
limitations.

## The correctness gate: `make check`

```text
make check
```

It stops at the first failure and prints `ALL CHECKS PASSED` on success. Run it
from the repository root on Linux x86-64. Bare `make` runs it too. `make help`
lists every target.

| Lane | What runs | Compilers |
| --- | --- | --- |
| Docs links | `make check-docs`: tracked Markdown relative links and heading anchors (no network), plus `scripts/check-api-docs.py`, the API symbol/coverage check over the Ddoc of the public runtime modules | Python 3, DMD |
| Root suite | `dub test` (module `unittest` blocks plus `antfarm_test.d`) | LDC, DMD |
| Farm ordering | `make check-farm-ordering`: controlled subscription/reclamation schedules and the leaf-reuse test, with write-audit hooks | LDC, DMD |
| Farm TSan | `make run-farm-tsan`: the root suite and `leaf_reuse.d` under ThreadSanitizer | LDC |
| Actor torture | `actor_torture`: deterministic interleavings and sustained contention for the public actors | LDC, DMD, LDC TSan |
| Review torture | `review_torture`: adversarial Farm tests | LDC, LDC TSan |
| Private actors | `private_actor_tests`: unit suite, the extracted private-actor documentation programs, and the fatal-death harness | LDC, DMD, LDC TSan |
| Examples | `examples/`: the First payload program extracted from [README.md](../../README.md), run in debug and release only; and the six actor examples, debug and release with 0 and 2 workers, plus an LDC TSan build (`fiber_cancel_at` with 0 workers only) | LDC, DMD; TSan on the actor examples only |

`ANTFARM_HUGE_PAGES=0` is exported for every lane, so the matrix does not depend
on how the calling shell is configured.

Not covered by `make check` (run these yourself when you change those packages):
the `threadpool` and `fibers` suites (`dub test` in each package; see
[../../threadpool/README.md](../../threadpool/README.md) and
[../../fibers/README.md](../../fibers/README.md)), the review-torture
write-protection (`allwrite`) targets, and `examples/iota_sum.d`. Performance is
not part of it either (see `make perf-gate`). Windows is validated by hand after
a release candidate and is not part of this gate.

### Prerequisites

- DMD 2.112+ and LDC 1.42+, with the LDC ThreadSanitizer runtime installed (the
  TSan lanes fail to link without it).
- Dub, GNU make, Python 3 and Bash. The matrix itself uses no other tools.
- `/bin/sh` is not required. GNU make execs recipes without shell metacharacters
  directly, and `scripts/check.sh` carries its own Bash shebang. When `/bin/sh`
  is missing it passes `SHELL=/usr/bin/bash` to child makes, because their TSan
  recipes quote `TSAN_OPTIONS` and GNU make never takes `SHELL` from the
  environment. If you invoke a sub-suite's makefile directly on such a host, pass
  `SHELL=/usr/bin/bash` yourself.

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
[actor_torture](../../actor_torture/README.md),
[review_torture](../../review_torture/README.md),
[private_actor_tests](../../private_actor_tests/README.md), and
[examples](../../examples/README.md).

### Optional and longer lanes

- `make check-soak` runs the private-actor TSan suite in a loop for at least 121
  seconds. It builds only that target and does not run the matrix first, so run
  `make check` separately.
- `make check-mimalloc` runs the normal matrix, then the real allocator lanes
  (`actor_torture` with the pinned mimalloc v3.5.3, release and TSan). It needs
  the submodule (`git submodule update --init --recursive`), CMake and a C
  toolchain. See [../../third_party/README.md](../../third_party/README.md).
  Other allocator targets (`run-mimalloc-debug`, `run-mimalloc-dmd`) are in
  [../../actor_torture/Makefile](../../actor_torture/Makefile).

## Performance is separate

`make perf-gate` compares two committed refs and decides whether the candidate
regresses beyond the project boundary. It is separate from `make check`, builds
the refs in temporary worktrees, needs a quiet Linux host with LDC, Python 3 and
the pinned mimalloc archive, and its output directory must be new. An
inconclusive result is not a pass. Run the correctness gate first; run timing
serially with nothing else heavy on the host. The method, the modes and the
decision rules are in [../../perftest/README.md](../../perftest/README.md).

Changes to executable code or its layout can move performance even when the
algorithm is unchanged, so treat a clean diff as no evidence of identical
performance.

## Documentation and example extraction

`make check` extracts and runs two kinds of Markdown program:

- The complete First payload program in [README.md](../../README.md), by
  `examples/run_examples.py`.
- The six complete private-actor programs in the
  [private-actor guides](../guide/private-actors.md), by
  `private_actor_tests/extract_doc_examples.py`.

Mark partial code blocks as fragments; the extractors reject a block that is
neither a complete `void main` program nor marked as a fragment. Moving a
documentation program means updating the extractor path in the same change.
