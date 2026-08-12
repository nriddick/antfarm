# review_torture

Adversarial tests + code review for `antfarm.d` (gen6 / spec2).

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
- `torture_tests.d` — T01–T18
- `CODE_REVIEW.md` — findings
- `last_run.log` — latest full run capture (gitignored)
- `Makefile`

## Notes

Shared test counters must be `__gshared shared(T)` (see comments). Plain
`shared T` module globals are TLS and will silently break multi-threaded
accounting under LDC.

## Design decisions (author, 2026-08-11)

These resolve the open questions from `CODE_REVIEW.md` and set policy for the
next revision of the farm.

1. **Always at least one pin.** There is always at least one consumer pin
   blocking producer reclamation. The mechanism is consumers holding the last
   segment: an idle consumer that has caught Wt keeps its position reference on
   the frontier segment (migrated forward as the frontier advances), rather
   than dropping to zero pins. The T16 fix must therefore *keep* the pin but
   make it track the frontier — release or migrate a position reference once
   its segment is confirmed complete, instead of parking it on a stale
   completed segment.
2. **Done and claims cap at 512.** `Done` (payload iterations) and `MaxCs`
   (payload claim slots) have a much lower practical maximum than the packed
   field widths suggest: **512**. `write()` must fatal on `Done > 512` or
   `MaxCs > 512`. For overflow protection across `Tcount` and `Pcount`,
   **fatal asserts are acceptable**: if a packed counter would exceed its field
   capacity, abort rather than wrap. (With the 512 caps the 16-bit
   calls/comps and 32-bit claim fields can only overflow under pathological
   visitor accumulation, and the assert turns that into a diagnosable abort.)
3. **Producers may consume.** Producers sharing the consumer role is supported
   and is the documented escape hatch on stall (spec 4a): a stalled producer
   may subscribe a `ConsumerView` and drain before retrying `write`.
4. **Producer registration via tickets.** `registerProducer` allocates a slot
   and returns a sentinel value unique to that slot; `write()` gains a required
   ticket parameter that must match one of the registered producer slots, so
   unregistered writers cannot bypass the Exmax invariant (H0). Tier slot
   pools (bulk/small) are the natural place to hold the sentinels.
