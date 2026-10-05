# E2-2 validation

- Runtime, tests and docs snapshots f01bb83, 8313b72 and ff7e74a build with
  LDC 1.43.0 and DMD 2.113.0, in isolated archives under /tmp/e2-2-commit-builds.
- `make check` passed after adding the runtime and tests and again at the
  complete perf tip (including the compiler, sanitizer, death and doc-example
  matrix). [Final check output](check-tip.txt).
- The final perf snapshot also builds both private tests and actor_churn
  with LDC and DMD. [All commit build output](commit-builds.txt).
  The perf commit was then amended only to record these validation results.
- `make check-soak`: 104 complete private-actor TSan loops in 121 seconds;
  no failures or sanitizer reports.
- `make -C private_actor_tests SHELL=/usr/bin/bash doc-example` passes all
  three programs with both compilers, in debug and release.
- LDC/mimalloc and DMD/CRT builds of actor_churn pass manual subset and
  whole-comparator smoke runs at 16384 members, 0 and 6 consumers.
- [Mutation evidence](mutations.md): 35 distinct mutations caught on isolated
  copies. The shared worktree runtime was never mutated.

## Approved clarifications and deviations

1. An active `startSubset` returns `waveActive`, preserving all metadata;
   it is not fatal. This follows design section 4.2 and the orchestrator's
   clarification of the conflicting death-test instruction in e2-2.md.
2. The selection and inverse positions share one eager allocation. One
   pointer plus a count adds 16 fixed bytes per cohort on 64-bit (264 to
   280 bytes); the subset flag fits existing padding. No per-member subset
   storage is allocated without reservation. The layout assertion changes
   in the runtime commit so it builds independently. This storage change
   replaces the design's separate sel_/pos_ allocation-failure cases with
   one combined-allocation failure test.
3. The docs extractor now expects three complete programs (approved).
4. The approved benchmark uses persistent state and no-op waves, excluding
   setup/teardown, and adds a whole-wave comparator without subset storage.
5. No subset retirement variant: startFinal retires the whole cohort;
   continuations and per-member retirement remain out of scope.
6. The prebuilt mimalloc archive is read from the main checkout. `make -o
   ARCHIVE` bypasses its absent submodule build prerequisite without changing
   the checkout. CPU metadata comes from /proc/cpuinfo because lscpu cannot
   access this environment's /sys CPU topology files.
7. ACTOR_MEMORYORDER.md is unchanged: selection publication, results and
   generation reuse use the same Wave join; PRIVATE_ACTORS.md states the
   existing ordering argument. Entry remains 40 bytes.

The main checkout was not modified and nothing was pushed. Windows was not
run locally; the runtime and portable tests use the existing D/thread/atomic
patterns, with Linux-only death harnesses skipped on Windows.

## Performance result

All ten unused-feature medians pass the −3% gate (−1.10% to +1.51%).
100% subset wave throughput is 21.71% lower with no workers and 13.98% lower
with six workers than a reserved whole wave. This misses design §8's 5%
expectation; the orchestrator approved recording it as a known limitation
without tuning the runtime. Select every member through `start`.
The equivalent time-per-wave increase is 27.74% / 16.25%.
[Raw runs](runs.jsonl), [summary](summary.json), and [metadata](metadata.json)
record all 150 runs, five alternating repeats, pinning and allocator details.
