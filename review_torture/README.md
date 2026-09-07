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

`concurrent_create.d` is a separate process test so eight threads race the
first mapping initialization. It creates 800 Farms and verifies both aliases
of each ring. Build from the repository root with
`ldc2 -g -O1 review_torture/concurrent_create.d antfarm.d antfarm_allocation.d -of=concurrent_create`.
Use `ANTFARM_HUGE_PAGES=0` for ordinary-page coverage; on a configured Windows
host, `=1` also exercises concurrent large-page creation.

## Notes

### Consumer protection audit: allWriteCheck

Every build checks each new segment entered by a write reservation,
**including an exact boundary
landing**, for `(Rt & LOWMASK) == 0`. It runs after the `Wt` fetch-add and before this
reservation changes any segment metadata or table body, and remains active
with `-release`. A failure prints the reserved interval, target epoch, full
root tally, decoded roots/Sub0/Sub fields, and a best-effort metadata/leaf
snapshot before `fatal`. Epoch zero and writes staying in the current segment
are not fresh-segment transitions.

This is diagnostic instrumentation, not an atomic check-and-reserve protocol.
A subscriber can provisionally pin a free slot after a producer's sweep, so
the check ignores the high `SUB` half. Both active root counts and `Sub0`
pulses in the low half remain fatal, with distinct diagnostic labels.
The quota sweep itself still checks the full `Rt == 0` word.

From the repository root:

```
ldc2 -g -O2 -d-version=AntfarmWriteAuditHooks review_torture/write_protection.d antfarm.d antfarm_allocation.d -of=write_protection_allwrite
ldc2 -g -O2 review_torture/boundary_runoff.d antfarm.d antfarm_allocation.d -of=boundary_runoff_allwrite
./boundary_runoff_allwrite
./write_protection_allwrite
python3 review_torture/quota_model.py
```

Or use `make -C review_torture run-allwrite` with LDC. The DUB configuration
`-c all-write-check` remains a compatibility alias for the normal library.
No version flag is needed, including in clients instantiating templated writes.
The helper is called only when a reservation crosses a segment boundary.

The default was selected after paired Windows x64/LDC 1.42 release benchmarks
with huge pages enabled. In the final four-pair comparison, ten raw/dual
throughput cases had median changes from -1.57% to +2.08%. Six focused latency
pairs showed no repeated busy-workload p99 regression. Extreme tails and
saturated near-full behavior remained noisy, including in an identical-binary
control. These measurements do not establish a hard latency bound.

- `boundary_runoff.d`: one producer with quota 512, one consumer parked at
  epoch zero, and 512-word tables. The pre-`d6b2eff` automatic-renewal code
  admits an unchecked exact-boundary reservation and laps the live pin.
  Current code stops at `Wt=229376` and rejects 10,000 further attempts.
  The `pulse` argument keeps epoch zero's initial `Sub0` with no consumers;
  it tests the same protection for unexecuted work.
- `write_protection.d`: exact boundaries and multi-segment tables at
  `K=2,4,8,16`; a callback repeatedly checks its spanning body under write
  pressure; forced delays after probing, sweeping, and reserving. Delayed
  probe/sweep cases allow another producer and consumer to make two laps,
  then retain a real lagging consumer pin before resuming the writer.
- `inject-root`, `inject-sub0`, `inject-both`, and `inject-second` arguments deliberately
  plant nonzero tallies after a successful sweep. Each must abort through
  the allWriteCheck diagnostic, including the second fresh segment of a
  spanning reservation. These are instrumentation negative controls.
- `subscription-race` forces a real provisional-SUB race with an already
  granted producer. The masked check must permit this controlled schedule
  and deliver all ten intact payloads. It also runs in the default suite.
- `quota_model.py`: exhaustive exploration within a two-lap bound of a
  small sequentially consistent quota model. Probe, each scan load, grant,
  reservation, and consumer pin movement can interleave. It omits the
  implementation's publication, subscription, pulse, leaf propagation,
  and weak-memory details; a pass is not a proof of the full implementation.
  Its automatic-refill negative control is intentionally simplified.

The audit scheduling hooks are compiled only with `AntfarmWriteAuditHooks`.
Ordinary builds contain no scheduling hook calls.

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
