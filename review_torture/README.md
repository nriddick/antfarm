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
- `torture_tests.d` — T01–T18, T20, and T21. T20 exercises the D2
  confirmed-segment pulse invariant under write crossings and churn, while
  retaining small bodies so ThreadSanitizer sees the intended sentinel
  publication protocol without a giant-memcpy false positive. T21 laps the
  ring several times and verifies payload body contents word-for-word (a
  run of numbers summed into a global counter, plus per-payload call counts).
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

## Known TSAN noise

`make -C review_torture run-tsan` (`-fsanitize=thread`, halt_on_error) can false positive on T20 with large history size and as part of the greater test suit. It does a lot of memcpys and wraps which often alias to exactly shared values and the earlier context seems to pollute the history. T21 can trigger the same class of warning from its body memcpys and ring wrap. The stormer spawns now use explicit per-thread class instances, so a previous delegate capture race is gone.
