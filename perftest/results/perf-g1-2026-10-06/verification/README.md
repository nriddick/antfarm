# Correctness evidence

G0 (`ea2c402`) and G1 (`4682799`) each passed `make check` before
commit; complete output is in `g0-check.log` and `g1-check.log`. The matrix
includes LDC and DMD builds/tests, Farm/public/private TSan, private
debug/release death checks, documentation examples, and application examples.

The gate tests passed: `python3 -m unittest perftest/test_perf_gate.py`
(58 tests; `gate-tests.log`).

After the G1 matrix, both release private binaries also ran the normal
lifecycle suite successfully, with ordinary pages:

```sh
ANTFARM_HUGE_PAGES=0 ./private_actor_tests/private_actor_tests_release
ANTFARM_HUGE_PAGES=0 ./private_actor_tests/private_actor_tests_dmd_release
```

Their output is retained as `g1-private-ldc-release.log` and
`g1-private-dmd-release.log`. All correctness work completed before timing.

The complete results tree also passed `make check` after all timing and
limitation documentation were finished (`results-tip-check.log`). Gate
tests passed again with `python3 -B -m unittest perftest/test_perf_gate.py`
(`gate-tests-tip.log`). No verification failure required a fix.

Trailing whitespace in the three `make check` logs was normalized for
commit review. Timing logs and `runs.jsonl` were preserved unchanged.
