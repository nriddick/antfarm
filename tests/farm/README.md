# Farm review torture suite

Adversarial tests + code review for `antfarm/package.d` (`SPEC.md`).

## Build & run

From the repository root:

```
make -C tests/farm
make -C tests/farm run
```

Or:

```
ldc2 -g -O1 tests/farm/torture_common.d tests/farm/torture_tests.d antfarm/package.d \
  antfarm/allocation.d antfarm/templates.d -of=tests/farm/torture_tests
./tests/farm/torture_tests          # all
./tests/farm/torture_tests T16 T01  # subset
```

Optional:

```
make -C tests/farm run-tsan
make -C tests/farm run-dmd
make -C tests/farm baseline   # antfarm_test.d unit suite built without dub
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | All tests green, no defects |
| 2 | Correctness paths green; defect probes confirmed bugs |
| abort/1 | Correctness failure |

## Layout

- `torture_common.d` — counters, batch builder, producer/consumer helpers
- `torture_tests.d` — T01–T18, T20–T26 and T28. T28 runs complete 16/32-member
  subscription cohorts at K=2 and K=16, including backlog-refused wakeups and
  actual ring reuse; only the cohort that joined at the round's start may drain
  it, and every departure leaves exactly the Live frontier. T20 checks Live discharge under
  write crossings and churn: at quiescence no confirmed segment is still
  Live or referenced, and only the frontier is Live. Small bodies keep the
  arm an invariant scan rather than a memcpy bench. T21 laps the ring several times and verifies payload body contents
  word-for-word (a run of numbers summed into a global counter, plus
  per-payload call counts). T25 checks borrowed callback metadata
  and exact execution in ST tables with two producers, eight
  consumers, ring reuse, and chunk sizes 32 and one.
- `sweep_cursor.d` — T27: the oldest-epoch idle re-walk resumes at its
  cursor. One consumer parks in the primary call of the last table of an
  epoch while another idles with that epoch held as oldest; idle polls
  after the first re-walk only the parked table. Built with
  `-d-version=AntfarmSweepStats` and run by `make -C tests/farm run`.
- `t19_flood_lap.d` — T19: interleaved bulk dump + mid-tick small writes +
  subscription churn (four arms: no mid-tick, pure churn, consuming churn,
  steady consumer) plus a forged-token quota test
- `subscription_live.d` — deterministic subscription schedules for the Live
  lifecycle (SPEC 3, 6b, 6c): a non-last departure during a producer's
  unpublished crossing (A, B, C); a consumer moving during the scan (D1, D2);
  out-of-order initialization racing the scan (E); a stale tail probe (F,
  P); a new lap's `Live` beside the old `Es` (G); a discharged epoch between
  two Live ones (H); an initialization landing between a read's two loads
  (I); and adversarial subscription rounds that drive the tail until retained
  frontier pins refuse renewal (R, for every supported K). D, E, F and G fail
  if they miss their requested interleaving; I alone intentionally permits its
  documented after-attachment path. Every payload carries an id that must execute exactly once, and
  progress is judged with the views that already exist, before any new
  subscription; a rescue subscriber afterwards is diagnostic only. Run all
  with no argument, or one schedule by name with an optional trigger. Run the
  same schedules under TSan with `make -C tests/farm run-subscription-live-tsan`.
- `Makefile`

Run T19 on its own: `make -C tests/farm run-t19` (LDC),
`run-t19-dmd`, or `run-t19-tsan`.

`concurrent_create.d` is a separate process test so eight threads race the
first mapping initialization. It creates 800 Farms and verifies both aliases
of each ring. Build from the repository root with
`ldc2 -g -O1 tests/farm/concurrent_create.d antfarm/package.d antfarm/allocation.d -of=concurrent_create`.
Use `ANTFARM_HUGE_PAGES=0` for ordinary-page coverage; on a configured Windows
host, `=1` also exercises concurrent large-page creation.

## Notes

### Consumer protection audit: allWriteCheck

Every build checks each new segment entered by a write reservation,
**including an exact boundary
landing**, for `(Rt & LOWMASK) == 0`. It runs after the `Wt` fetch-add and before this
reservation changes any segment metadata or table body, and remains active
with `-release`. A failure prints the reserved interval, target epoch, full
root tally, decoded roots/Live/Sub fields, and a best-effort metadata/leaf
snapshot before `fatal`. Epoch zero and writes staying in the current segment
are not fresh-segment transitions.

This is diagnostic instrumentation, not an atomic check-and-reserve protocol.
A subscriber can provisionally pin a free slot after a producer's sweep, so
the check ignores the high `SUB` half. Active root counts, and a `Live` bit
the slot's previous epoch never discharged, remain fatal in the low half,
with distinct diagnostic labels. The check runs before the reservation raises
its own `Live`, and the raise itself is fatal if it finds the low half
nonzero. The quota sweep itself still checks the full `Rt == 0` word.

From the repository root:

```
ldc2 -g -O2 -d-version=AntfarmWriteAuditHooks tests/farm/write_protection.d antfarm/package.d antfarm/allocation.d -of=write_protection_allwrite
ldc2 -g -O2 tests/farm/boundary_runoff.d antfarm/package.d antfarm/allocation.d -of=boundary_runoff_allwrite
./boundary_runoff_allwrite
./write_protection_allwrite
python3 tests/farm/quota_model.py
```

Or use `make -C tests/farm run-allwrite` with LDC. The DUB configuration
`-c all-write-check` remains a compatibility alias for the normal library.
No version flag is needed, including in clients instantiating templated writes.
The helper is called only when a reservation crosses a segment boundary.

This tripwire does not establish a hard latency bound. Performance is assessed
in dedicated sessions; the methodology and measurements are in
[PERFORMANCE.md](../../PERFORMANCE.md).

- `boundary_runoff.d`: one producer with quota 512, one consumer parked at
  epoch zero, and 512-word tables. The pre-`d6b2eff` automatic-renewal code
  admits an unchecked exact-boundary reservation and laps the live pin.
  Current code stops at `Wt=229376` and rejects 10,000 further attempts.
  The `live` argument subscribes no consumer, so only epoch zero's
  construction-time `Live` protects it; it tests the same protection for
  unexecuted work.
- `write_protection.d`: exact boundaries and multi-segment tables at
  `K=2,4,8,16`; a callback repeatedly checks its spanning body under write
  pressure; forced delays after probing, sweeping, and reserving. Delayed
  probe/sweep cases allow another producer and consumer to make two laps,
  then retain a real lagging consumer pin before resuming the writer.
- `inject-root`, `inject-live`, `inject-both`, and `inject-second` arguments deliberately
  store nonzero tallies after a successful sweep. Each must abort through
  the allWriteCheck diagnostic, including the second fresh segment of a
  spanning reservation. These are instrumentation negative controls.
- `subscription-race` forces a real provisional-SUB race with an already
  granted producer: a subscriber's stale tail probe pins slot 0 after the
  producer's sweep granted it, and the producer then initializes epoch 4
  there under that `Sub`. The masked check must permit this schedule, the
  subscriber must attach at the earliest Live epoch, and both payloads must
  arrive intact. It also runs in the default suite.
- `quota_model.py`: exhaustive exploration within a two-lap bound of a
  small sequentially consistent quota model. Probe, each scan load, grant,
  reservation, and consumer pin movement can interleave. It omits the
  implementation's publication, subscription, Live, leaf propagation,
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

1.0.1. `make -C tests/farm run-tsan` and `run-t19-tsan` build with LDC
`-fsanitize=thread -d-version=TSan`. The TSan build defaults to
`history_size=7 halt_on_error=1` (`__tsan_default_options`; override with
`TSAN_OPTIONS`). A TSan report is a defect.


The Farm, actor, private-actor, review, and example lanes all use these
flags. Farm has no manual acquire/release TSan annotations: the sanitizer
must observe the real leaf/root/sweep/reservation chain. The root-level
`make -C tests/farm check-farm-ordering` target adds deterministic S1/S2 schedules, the
subscription schedules of `subscription_live.d`, and the leaf-reuse test under LDC and DMD; `make -C tests/farm run-farm-tsan` includes leaf reuse
under TSan. See [the ordering audit](../../docs/design/farm-ordering.md) for proofs and mutation
evidence, including a leaf-acquire defect hidden by the former annotations.

## Parallel-loop adapters

`python3 run_parallel.py --compiler ldc2` (or `dmd`) builds and runs the
ST grain range and completion checks in debug and release. Optional write
hooks cover mixed tables, empty input, full-ring refusal and invalid-hook death probes. It covers empty and
short ranges, large/uneven grains, tuple and zero-argument grains, partial submission,
forward checkpoints, stepped ranges, stack-borrowing foreach, four worker
threads, early handle destruction after publication, and a deliberately held
ST grain callback that must delay the join. Debug death probes reject nested waits
from payload callbacks and table hooks; debug/release reject escaping foreach bodies.
Each compiler/run has a 300-second timeout.

`make check-parallel` runs LDC by default; `make check-parallel-dmd` runs
DMD. `make run` includes the LDC check; `make run-tsan` also invokes
`run_parallel.py --tsan`. The Windows review lane runs the selected compiler.
