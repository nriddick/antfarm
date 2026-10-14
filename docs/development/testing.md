# Testing

The correctness gate is local. There is no CI. [CONTRIBUTING.md](../../CONTRIBUTING.md)
explains how to contribute; this page is the lane matrix, prerequisites and
limitations. During development, select a focused check for the changed behavior.
Bare `make` runs the full matrix, so use explicit focused targets during iteration.
Record deferred checks rather than implying they passed. The CyDo agent process
for local iteration, evidence reuse and pre-push hardening is in
[agent-workflow.md](agent-workflow.md).

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
| Threadpool suite | `dub test :threadpool` | LDC, DMD |
| Fiber suite | `dub test :antfarm-fibers` (smoke suite and module unittests) | LDC, DMD |
| Threadpool affinity | `confine-test` with full and small affinity masks; `host-test` with the configured host mask | LDC for confine; LDC debug/release and DMD for host |
| Farm ordering | `make check-farm-ordering`: controlled subscription/reclamation schedules and the leaf-reuse test, with write-audit hooks, plus `scripts/check-ring-stores.py`, which rejects a plain (non-atomic) ring-word store in `writeImpl` | LDC, DMD, Python 3 |
| Farm TSan | `make run-farm-tsan`: the root suite and `leaf_reuse.d` under ThreadSanitizer | LDC |
| Actor torture | `actor_torture`: deterministic interleavings and sustained contention for the public actors | LDC, DMD, LDC TSan |
| Review torture | `review_torture`: adversarial Farm tests | LDC, LDC TSan |
| Private actors | `private_actor_tests`: unit suite, the extracted private-actor documentation programs, and the fatal-death harness | LDC, DMD, LDC TSan |
| Examples | `examples/`: the First payload program extracted from [quickstart.md](../guide/quickstart.md#first-payload), run in debug and release only; and the six actor examples, debug and release with 0 and 2 workers, plus an LDC TSan build (`fiber_cancel_at` with 0 workers only) | LDC, DMD; TSan on the actor examples only |

`make check-quick` runs the root unit suite, `make -C review_torture run`
(including `sweep_cursor`), threadpool unit suite, fiber smoke suite, and
`python3 examples/run_examples.py --ldc-only`. The examples include the quickstart
payload and actor/headless programs in LDC debug and release with 0 and 2
workers; the terminal front end is omitted. This target needs the LDC toolchain
(including Dub), Python 3, GNU make and Bash, without DMD, a TSan runtime or a C
compiler. It prints `ALL QUICK CHECKS PASSED` on success; the full `make check`
matrix remains the correctness gate.

For the full gate's affinity lanes, set `CHECK_SMALL_CPUS` or
`CHECK_ONE_LLC_CPUS` to a `taskset -c` CPU list. Unset or empty values select
the first four allowed CPUs for the confine-test small mask and the entire
process affinity for host tests. If fewer than four or sixteen CPUs
respectively are allowed, the lane prints a notice and runs unpinned. Explicit values are honoured
verbatim, and a rejected `taskset` selection fails the lane. To constrain host
tests to one LLC, supply that LLC's allowed CPUs in `CHECK_ONE_LLC_CPUS`.
`scripts/check.sh --print-cpus` shows both selections without running tests.

`ANTFARM_HUGE_PAGES=0` is exported for every lane, so the matrix does not depend
on how the calling shell is configured.

For focused package checks, see [../../threadpool/README.md](../../threadpool/README.md)
and [../../fibers/README.md](../../fibers/README.md).

Not covered by `make check` (select relevant focused checks during iteration and
include affected package gates in pre-push scope; these are not a second pipeline):
the dedicated fiber TSan smoke suite, the review-torture write-protection
(`allwrite`) targets, and `examples/iota_sum.d`. Performance is not part of it
either (see `make perf-gate`). Windows has a separate native integration runner
described below and is not part of this gate.

To run the fiber smoke suite under ThreadSanitizer, build it with
`DFLAGS='-fsanitize=thread -d-version=TSan -g -O1' dub build :antfarm-fibers -c unittest --compiler=ldc2`.
In that build `fibers/tests/smoke.d` sets `gcopt=parallel:0`, because druntime's
parallel GC marking intermittently reports a race in its own mark stack.
When also enabling profiling explicitly, pass `"--DRT-gcopt=parallel:0 profile:1"`
to preserve that setting.

LDC 1.43.0's druntime has ASan fiber hooks but does not notify TSan about fiber
switches. Without separate TSan contexts, migrating a suspended fiber between
threads can underflow TSan's shadow call stack and crash in `__tsan_func_entry`.
This is unrelated to the application's fiber stack size or GC profiling.
Under `version(TSan)`, the scheduler therefore keeps a TSan context per active
task and restores the caller's context after every call. The small switch bridge
is uninlined and uninstrumented and uses druntime's non-template `Fiber.call`
overload; scheduler code and fiber bodies remain instrumented. This assumes the
standard, uninstrumented druntime, not a separately TSan-instrumented druntime.
Contexts are destroyed at terminal completion, including a rejected raw yield,
so recycling starts with an empty shadow stack. The switch API carries the
sequential caller/task ordering; actual task publication still uses the Farm's
normal synchronization.

The smoke suite checks TSan context identity, restoration, reuse, and migration.
Run `fibers/antfarm_antfarm-fibers --tsan-context-only` for the context regression
or `--migration-only` for migration across fresh native threads (including
cancellation and exception unwinding). The full smoke run includes both checks.

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
| `make check-farm-ordering` | the Farm ordering and leaf-reuse tests (LDC and DMD) and the raw ring-store check |
| `make run-farm-tsan` | the Farm suite under ThreadSanitizer |
| `make -C actor_torture run run-dmd run-tsan` | public actor suite |
| `make -C review_torture run run-tsan` | adversarial Farm suite |
| `make -C private_actor_tests run run-dmd run-tsan doc-example death` | private actor suite, documentation programs and death tests |
| `make -C examples check` | example programs and the quickstart program |

Each suite has its own README with selectors and the seams it uses:
[actor_torture](../../actor_torture/README.md),
[review_torture](../../review_torture/README.md),
[private_actor_tests](../../private_actor_tests/README.md), and
[examples](../../examples/README.md).

## Windows x64 integration

Run from the repository root in PowerShell or cmd.exe with Python 3, Dub and
the selected D compilers on PATH (DMD 2.112+ / LDC 1.42+). No Bash, GNU make,
C compiler or administrator privileges are needed:

```text
python -B scripts/check-windows.py
python -B scripts/check-windows.py --quick
python -B scripts/check-windows.py --compiler dmd --lanes threadpool examples
```

The default runs LDC and DMD. `--quick` uses only LDC and runs the packages,
plain review torture (including sweep cursor), and debug/release examples.
`--lanes` selects one or more rows below; `--compiler ldc2` or `--compiler dmd`
restricts the compiler. With GNU make available, `make check-windows` invokes
the same runner; use `WINDOWS_PYTHON` and `WINDOWS_CHECK_ARGS` to override its
interpreter and arguments.

| Lane | Native coverage |
| --- | --- |
| `packages` | Root, threadpool and Fiber unit/smoke suites via Dub |
| `threadpool` | Process affinity, caller confinement/restoration, armed wait races and missing wait API rejection in debug/release, FarmHost lifecycle; also optimized LDC host tests |
| `ordering` | Raw ring-store audit, controlled Farm ordering and leaf reuse |
| `review` | Plain torture suite, sweep cursor, T19 flood/lap and concurrent first mapping creation; quick omits the last two |
| `actors` | Public actor deterministic/contention suite with the mimalloc stub |
| `private` | Private actor suite and six extracted documentation programs |
| `examples` | quickstart payload and actor/headless examples in debug/release with 0 and 2 workers |

The runner forces `ANTFARM_HUGE_PAGES=0`, keeps generated binaries and Dub's
home under `.build/windows-check/`, and uses local dependencies without registry
resolution. Dub package outputs and example binaries retain their usual ignored
locations. Each build or run has a 300 s timeout (`--timeout` overrides it).
A timeout kills that command's Windows process tree, including Dub's child;
nonzero exits stop the matrix. Live affinity/host coverage skips fail by default.
`--allow-hardware-skips` explicitly accepts missing CPUs/groups and labels the
final pass accordingly. Examples still require two eligible worker CPUs in one
LLC for their threaded runs.

Success prints `WINDOWS CHECKS PASSED`. This is separate from `make check`:
TSan, POSIX fork/death probes (including T01/T17), the Life terminal front end,
real mimalloc and timing gates are excluded. Existing POSIX-only tests may print
their own skips within a native suite; Fiber multi-LLC probes also skip on
single-LLC hosts. Use the separate
[Windows mimalloc commands](../../actor_torture/README.md) for
real allocator pairing.

The runner's failure/selection tests include a real Windows child-tree timeout
probe. The example tests also protect Linux's existing TSan and terminal matrix:

```text
python -B -m unittest scripts/test_check_windows.py examples/test_run_examples.py -v
```

### Portable Python checks

These focused checks run on Windows as well as Linux, without a D compiler or
benchmark build. Run from the repository root with Python 3 and Git on PATH:

```text
python -B scripts/check-doc-links.py
python -B -m unittest scripts/test_check_doc_links.py perftest/test_perf_gate.py perftest/test_run_performance_md.py perftest/test_private_lifecycle_matrix.py perftest/test_offccd_sweep.py -v
```

Use `python3` instead of `python` if that is your interpreter's command name.
Documentation fixtures live in a temporary repository; only the three symlink
probes skip when the host cannot create file symlinks. Performance decision,
parser, grouping and topology-fixture tests use synthetic inputs. The real
process-group timeout and gate error-accounting integration probes require
Linux and report skips elsewhere. The non-Linux gate-rejection probes skip on
Linux; the Windows child-timeout probe skips elsewhere. These checks do not
establish timing results or replace `make check`.

### Optional and longer lanes

- `make check-soak` runs the private-actor TSan suite in a loop for at least 121
  seconds. It builds only that target and does not run the matrix first. It is
  optional and counts against the 600-second collective general TSan soak budget
  in [agent-workflow.md](agent-workflow.md#hardening-boundary); wrap it in a
  watchdog using the remaining budget. Targeted hardening/concurrency TSan runs
  are capped at 300 seconds each. Reuse applicable correctness-gate evidence;
  invoking a soak does not itself require another `make check` run.
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
inconclusive result is not a pass. Establish applicable correctness evidence first,
reusing an existing valid result; run timing serially with nothing else heavy on
the host. The method, the modes and the
decision rules are in [../../perftest/README.md](../../perftest/README.md).

Changes to executable code or its layout can move performance even when the
algorithm is unchanged, so treat a clean diff as no evidence of identical
performance.

## Documentation and example extraction

`make check` extracts and runs two kinds of Markdown program:

- The complete First payload program in [quickstart.md](../guide/quickstart.md#first-payload), by
  `examples/run_examples.py`.
- The six complete private-actor programs in the
  [private-actor guides](../guide/private-actors.md), by
  `private_actor_tests/extract_doc_examples.py`.

Mark partial code blocks as fragments; the extractors reject a block that is
neither a complete `void main` program nor marked as a fragment. Moving a
documentation program means updating the extractor path in the same change.
