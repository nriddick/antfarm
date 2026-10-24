# Test suite review, October 24, 2026

A survey of `make check` for redundancy, written after adding the fork-join
suite and running the full gate on the 5900XT. It lists what each lane
compiles and runs, where the same code runs again under another compiler,
build type or sanitizer, how long each lane takes, and what could be
dropped, merged or moved out of the gate without losing a property the
suite proves. It is a list for the maintainer; nothing here is applied.

## Shape of the gate

The gate is 37,000 lines of test source in seven suites, driven by
`scripts/check.sh` in 31 steps. Every suite runs under two compilers; most
run again under ThreadSanitizer; the death harnesses run in debug and
release under both compilers; the examples run in four build configurations
with 0 and 2 workers, plus TSan.

| Suite | Source | Runs in the full gate |
| --- | --- | --- |
| Root unit suite (`antfarm_test.d`, 46 tests, plus module unittests) | 4,125 lines | ldc2, dmd, ldc2 TSan (`run-farm-tsan`) |
| Review torture (`torture_tests.d`, T01 to T19) and `sweep_cursor` | 2,300 lines | ldc2 (`run`), ldc2 TSan (`run-tsan`); `t19_flood_lap` is built by `all` but not run by the gate |
| Parallel loops (`parallel_tests.d`, 7 death probes) | 409 lines | ldc2 debug and release with probes, dmd debug and release with probes, ldc2 TSan |
| Farm ordering (`farm_ordering.d`, `leaf_reuse.d`) | 300 lines | ldc2 and dmd each, plus `leaf_reuse` under TSan via `run-farm-tsan` |
| Actor torture (`actors_torture.d`) | 4,863 lines | ldc2, dmd, ldc2 TSan, death in ldc2 debug and release and dmd debug and release |
| Private actors (`tests/private/main.d`) | 12,579 lines | ldc2, dmd, ldc2 TSan, death in ldc2 debug and release and dmd debug and release, documentation programs in ldc2 and dmd each in debug and release |
| Threadpool (`dub test :threadpool`, confine, host) | 2,900 lines | unit suite ldc2 and dmd; confine-test full and small mask; host-test ldc2 debug, dmd, ldc2 release |
| Fibers (`dub test :ant_fibers`: smoke plus unittests) | 2,780 lines smoke, 1,229 stress (not in the gate) | ldc2, dmd |
| Fork-join (`dub test :forkjoin`) | 390 lines | ldc2, dmd; deque TSan is a documented manual lane |
| C ABI (`check-c-abi.py`) | 1,900 lines of C and C++ | ldc2, dmd, ldc2 TSan; each builds the core twice (release and debug audit) and the C, C++, no-exceptions C++ and audit variants of every test |
| Examples (`run_examples.py`) | 7 programs | ldc2 debug, ldc2 release, dmd debug, dmd release, each with 0 and 2 workers, plus ldc2 TSan with 0 workers; quickstart program extracted and run |
| BetterC probe, docs links, API docs, ring-store scan | | once |

## Timings

Measured from the timestamped log of this run (`.build/check-full.log`),
wall time per `check.sh` step on 32 logical CPUs, `ALL CHECKS PASSED`.

| Step | Wall time |
| --- | ---: |
| `make -C examples check` (7 programs, 4 builds, 0 and 2 workers, TSan) | 156 s |
| `make -C tests/private run run-dmd run-tsan doc-example death` | 48 s |
| `check-c-abi.py --compiler ldc2` | 16 s |
| `make -C tests/farm run run-tsan` (torture, sweep_cursor, parallel probes, TSan) | 14 s |
| `check-c-abi.py --compiler dmd` | 12 s |
| host-test ldc2 release | 8 s |
| `make -C tests/farm run-farm-tsan` | 6 s |
| `make -C tests/actors run run-dmd run-tsan death` | 6 s |
| `dub test :ant_fibers` ldc2 | 6 s |
| `dub test :ant_fibers` dmd | 5 s |
| host-test ldc2 debug | 3 s |
| `check-c-abi.py --tsan` | 3 s |
| `dub test :threadpool` ldc2 | 3 s |
| every other step | 0 to 2 s each |
| **total** | **301 s** |

Two lanes are 68% of the gate. The examples lane is 156 s because it
compiles each of seven programs four times from source with `-i` (the
whole library per program per build) and runs each twice; its runtime
content is a few seconds. The private lane's 48 s is mostly the
documentation programs: each extracted program is compiled four times
with the full actor library. In both, compile time dominates, so the cuts
below (fewer compiler and build-type repeats) remove most of the wall
time rather than a little of it.

## Where the same thing runs again

1. **DMD as a second compiler on every suite.** DMD runs the root suite,
   threadpool, fibers, fork-join, C ABI, actors, private, parallel loops,
   farm ordering, host-test and the examples. What DMD catches that LDC does
   not: frontend disagreements (the two share the frontend since 2.1xx, so
   few), codegen differences in `-release` (the gate's own comment says the
   LDC `-O1+` host-test is the one that catches `closeOrAbandon` skipping
   `abandon`, which is the opposite direction), and `-checkaction=context`
   messages. The DMD lanes do not add a memory model or an optimizer that
   reorders differently from LLVM's; TSan is LDC-only. Suggestion: keep DMD
   on the root suite and the examples (the compile-compatibility promise)
   and the two death harnesses (release behaviour of `fatal`), and drop it
   from torture, private TSan-adjacent lanes, host-test and the C ABI run,
   where it repeats an LDC run on a frontend-identical compiler.
2. **Death harnesses in four builds.** Actors and private each re-exec
   themselves under ldc2 debug, ldc2 release, dmd debug, dmd release, each
   with a negative control. The property is "the contract violation aborts
   with this message even with asserts removed". Two builds prove it: one
   debug (asserts on) and one `-release`. The compiler choice does not
   change whether `fatal()` runs. Suggestion: ldc2 debug and ldc2 release
   only.
3. **Documentation programs for private actors in four builds.** Each
   extracted program compiles and runs under two compilers times two build
   types. They are documentation, not torture: one build (ldc2 debug) shows
   they are complete and correct; the release build adds nothing the private
   suite's own release run does not.
4. **Examples in nine configurations.** Seven programs times four
   compiler/build pairs times two worker counts, plus TSan. The 0-worker
   runs exercise the caller-consumes path and the 2-worker runs the pool;
   both are worth one configuration each. Release builds of examples protect
   against nothing the release death and host lanes do not. Suggestion:
   ldc2 debug with 0 and 2 workers, dmd debug with 2 workers, TSan as now.
5. **Parallel-loop probes in four builds.** The seven death probes run in
   ldc2 debug and release and dmd debug and release. One debug and one
   release suffice, as for the other death harnesses.
6. **C ABI built twice per compiler.** `check-c-abi.py` builds the core as
   release and again as a debug audit library, then compiles each C test
   with and without `AF_TEST_AUDIT`, and the C++ tests with and without
   exceptions, under ldc2, dmd and TSan. The audit variants catch
   contract violations the release build hides; the no-exceptions C++ build
   tests the factory path. All four variants are distinct properties. What
   repeats is running the whole set under dmd: the ABI is a layout and
   symbol contract, checked against `cabi/abi1.lock`, and the lock check is
   compiler-independent. Suggestion: ldc2 full, dmd `--core-only`
   (declarations and layout audit), TSan as now.
7. **Host-test three times.** ldc2 debug, dmd, ldc2 release. The comment in
   `check.sh` names the ldc2 release run as the one that matters. ldc2 debug
   and release are two properties (asserts on, optimizer on); dmd is a
   repeat.
8. **The root suite under TSan and `leaf_reuse` twice.** `run-farm-tsan`
   runs `antfarm_test_tsan` and `antfarm_test_leaf_tsan`; `check-farm-ordering`
   runs `leaf_reuse` under ldc2 and dmd without TSan. The non-TSan
   `leaf_reuse` runs are subsumed by the TSan run, which executes the same
   program with more checking.
9. **`sweep_cursor` and T19.** `sweep_cursor` runs in `run`; `t19_flood_lap`
   is compiled by `all` but no gate target runs it (`run-t19`, `run-t19-tsan`
   and `run-t19-dmd` exist and are unreferenced). Either it belongs in the
   gate or its build should leave `all`.
10. **Fiber stress.** `tests/ant_fibers/stress.d` (1,229 lines) is not in
    the gate or the quick check; it exists as `dub run -c stress`. Decide
    whether it is a soak (document it beside `check-soak`) or a test (run
    it).

## What is not redundant

- **TSan on every concurrent suite.** It is the only lane that checks the
  memory-order arguments rather than outcomes. Keep every TSan run,
  including the fork-join deque lane, which should move from a documented
  command into `check.sh` (one build, two seconds).
- **The two write-audit builds** (`AntfarmWriteAuditHooks`) in farm ordering
  and the private suite: they are the only tests that execute another
  actor's steps synchronously at a chosen interleaving.
- **The negative controls** of the death harnesses: they prove the harness
  can fail.
- **The confine-test small mask and host-test pinning**: they are the only
  checks of the OS-scope and placement contracts on real affinity.
- **The quickstart extraction**: the only check that the first program a
  reader sees compiles.

## Fork-join coverage after this review

`dub test :forkjoin --build=cov` on the admission and deque tests alone:
`deque.d` 87%, `dispatcher.d` 84%, `node.d` 91%. Uncovered: the
single-participant path, spill to the Farm and the inline fallback, sibling
and cross-group steals (the first eight eligible processors are one CCD's
cores), uniform victim selection, the spin idle policy, and a join that
forks a second phase. `tests/forkjoin/paths_test.d` now exercises each of
those; the deque's unused `empty` and `size` were removed instead of
tested. Coverage after the addition is in the section appended below.

With `paths_test.d` (LDC, `--build=cov`): `deque.d` 95%, `dispatcher.d`
97%, `node.d` 100%. The six lines still uncovered in the dispatcher are the
CAS-retry branch of `wakeOne`, the spin inside the submit lock, the recheck
that finds work between arming and parking, and two `fatal`/`enforce`
messages. The first three need a lost race to execute and are the kind of
line a deterministic test cannot reach without a hook; the fatal paths are
contract violations. Running the paths test also exposed that
`discardCounters` reset each participant's counters but not their global
sum, so consecutive dispatchers in one process reported cumulative totals;
fixed in the same commit.

## Suggested gate after the cuts

Keep: BetterC probe; C ABI ldc2 full, dmd core-only, TSan; root suite
ldc2, dmd, TSan; threadpool unit ldc2 and dmd; fibers ldc2 and dmd;
fork-join ldc2 and dmd plus deque TSan; confine-test full and small;
host-test ldc2 debug and release; docs; farm ordering ldc2 (write-audit)
and `leaf_reuse` under TSan; parallel loops ldc2 debug and release with
probes, TSan; actors ldc2, TSan, death in ldc2 debug and release; private
ldc2, TSan, death in ldc2 debug and release, documentation programs ldc2
debug; examples ldc2 debug (0 and 2 workers), dmd debug (2 workers), TSan.
Add: `t19_flood_lap` or remove its build from `all`; fiber stress as a
documented soak.

The expected saving is the DMD repeats of the torture, private, host, C ABI
run and release-example lanes and half of each death matrix; the timings
table says what that is worth on this host. The properties the gate proves
are unchanged.
