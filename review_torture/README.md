# review_torture

Adversarial tests + code review for `antfarm.d` (`SPEC.md`).

## Build & run

From `construction/`:

```
make -C review_torture
make -C review_torture run
```

Or:

```
ldc2 -g -O1 review_torture/torture_common.d review_torture/torture_tests.d antfarm.d \
  -of=review_torture/torture_tests
./review_torture/torture_tests          # all
./review_torture/torture_tests T16 T01  # subset
```

Optional:

```
make -C review_torture run-tsan
make -C review_torture run-dmd
make -C review_torture baseline   # existing antfarm_test.d
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | All tests green, no defects |
| 2 | Correctness paths green; defect probes confirmed bugs |
| abort/1 | Correctness failure |

## Layout

- `torture_common.d` — counters, batch builder, producer/consumer helpers
- `torture_tests.d` — T01–T18, T20–T24. T20 exercises the D2
  confirmed-segment pulse invariant under write crossings and churn, with
  small bodies so the arm stays an invariant scan rather than a memcpy
  bench. T21 laps the ring several times and verifies payload body contents
  word-for-word (a run of numbers summed into a global counter, plus
  per-payload call counts).
- `t19_flood_lap.d` — T19: interleaved bulk dump + mid-tick small writes +
  subscription churn (four arms: no mid-tick, pure churn, consuming churn,
  steady consumer) plus a forged-token quota test
- `Makefile`

Run T19 on its own: `make -C review_torture run-t19` (LDC),
`run-t19-dmd`, or `run-t19-tsan`.

## Notes

Shared test counters must be `__gshared shared(T)` (see comments). Plain
`shared T` module globals are TLS and will silently break multi-threaded
accounting under LDC.

Stormer threads (T06/T18) are one `StormJob` class instance per thread.
A loop-local delegate capturing `&hctx[i]` made every thread alias the
last slot — that was a real harness race, not a sanitizer artifact.

## ThreadSanitizer

1.0.1. `make -C review_torture run-tsan` and `run-t19-tsan` build with LDC
`-fsanitize=thread -d-version=TSan`. The TSan build defaults to
`history_size=7 halt_on_error=1` (`__tsan_default_options`; override with
`TSAN_OPTIONS`). A TSan report is a defect.
