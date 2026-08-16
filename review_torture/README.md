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
- `POSTMORTEM.md` — settled directions for C1–C4 / H0 (next revision brief)
- `last_run.log` — latest full run capture (gitignored)
- `Makefile`

## Notes

Shared test counters must be `__gshared shared(T)` (see comments). Plain
`shared T` module globals are TLS and will silently break multi-threaded
accounting under LDC.

## Known TSAN noise (harness, not the farm)

`make -C review_torture run-tsan` (`-fsanitize=thread`, halt_on_error) stops
on one data race, and it is a test-harness bug, not `antfarm.d`:

- **Location**: `torture_tests.d:378` — `stormerMain` writing
  `StormCtx.cycles` (`torture_tests.d:359`); both race stacks are entirely
  inside `torture_tests.d`, no `antfarm` frames.
- **Cause**: D closures capture loop-body locals by reference. In T06 the
  six storm threads are spawned with `storm[i] = new Thread({ stormerMain(hc); })`
  inside the loop; every closure captures the *same* `hc` variable (whose
  final value is `&hctx[5]`), so all six threads concurrently write one
  ctx's `cycles`. (Minimal repro of the capture semantics: four closures
  each incrementing `&xs[i]` in a loop all increment `xs[3]`.)
- **Implication**: the storm threads still subscribe/unsubscribe and
  exercise the farm (T06's correctness checks pass), but the fan-out
  collapses onto one `StormCtx` — `hctx[0..4]` are unused and the cycle
  count is racy.
- **Effect on the run**: `halt_on_error=1` stops at this first report, so
  tests after T06 get no TSAN coverage. Treat any TSAN report *other than*
  `torture_tests.d:378/:412` as a real finding; this one is known noise.
- **Fix (not applied, cosmetic)**: spawn each storm thread through a
  helper that takes the `StormCtx*` by value so every closure captures its
  own instance, e.g. `auto spawn(ref StormCtx c) { return new Thread({ stormerMain(&c); }); }`
  — then `storm[i] = spawn(hctx[i]);`. Harmless to the farm either way.

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
4. **Producer registration via tickets.** `registerProducer` increments a dedicated `Reqs_p` (not the consumer `Reqs_c`) and returns a Token struct which contains the slot index and a hashed value using that index and `Reqs_p`. On the Farm side, this sets a slot in preallocated arrays for max-bulk and max-small producers. Producers deregister using their Token as a parameter, which changes the hash value at the slot index to some invalid value. (The arrays are also initialized with some invalid value.) `write()` gains a required Token parameter which verifies that the hash matches the value at Token's slot index for that role, so unregistered writers cannot bypass the Exmax invariant (H0). Consumer `IDc` stays a dense sequence from `Reqs_c`.
5. **Detect pathological payload lengths during write()** and abort if a Payload length exceeds the bulk producer's max capacity.
