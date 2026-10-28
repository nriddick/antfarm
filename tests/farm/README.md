# Farm sanity and torture tests

Checks for the contracts and invariants of [the Farm](../../SPEC.md), with
separate entry points for deterministic sanity checks, concurrent torture,
and controlled interleavings. The complete unit suite retains its table-format,
completion-ordering, and actor checks; `sanity` is a focused subset, not a
replacement for the correctness gate.

## Build & run

From the repository root:

```
make -C tests/farm sanity               # geometry, membership, Live, capacity
make -C tests/farm torture              # T00–T02, T04, T06, T09, T10, T13, T15–T18, T20–T26, T28, T29
make -C tests/farm check-farm-ordering  # controlled schedules, LDC and DMD
make -C tests/farm baseline             # complete antfarm_test.d, without dub
make -C tests/farm run                  # torture + sweep cursor + parallel adapters
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
make -C tests/farm run-farm-tsan
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Every selected check passed |
| 64 | Invalid selector (including a name that would run no tests) |
| other nonzero / abort | Contract, correctness, or watchdog failure |

T01/T17/T26 expect a child to abort with `SIGABRT` for invalid calls; a clean
return, another signal, failed fork/wait, or ten-second timeout fails the
parent. These POSIX death probes explicitly skip on Windows. There is no
"known defect but green" mode.

The unit binary accepts `sanity`, `join-only`, `stale-quota-only`, or
`compact-only`. The torture binary accepts one or more of its T-names; T19
and T27 have separate executables. `subscription_live` accepts one of the
schedule names below, with an optional `0..3` trigger for D1, D2, or E.

## Invariants and their checks

| Obligation | Sanity / controlled check | Torture / concurrency check |
| --- | --- | --- |
| Construction leaves only epoch 0 Live; membership never clears an open epoch | `testLiveMembership`, K=2/4/8/16, shared leaves, cached leaf identity, repeated detach | `testBacklog` departure mid-backlog; T18 abandoned backlog; T28 sleeping cohorts |
| Stable root reference count equals the number of occupied leaves; no negative leaves, reserved bits, or leftover `Sub` | `checkStableTallies`; exact quiescent roots and leaves | Full-word root/leaf cleanup checks after departures; all subscription schedules |
| FULL/INVALID do not leak membership or pins; one released capacity slot is reusable | `testSubscriptionCap` at 128; null/already-attached sanity cases | T10 capacity; T28 simultaneous arrival/departure |
| Initialized unfinished epochs retain Live without any consumer | `testSingleThreaded`, `testBacklog`; schedules A–C | T18 abandoned backlog; T23/T24 quiescent Live checks; T28 prefill with zero members |
| Confirmed epochs can discharge; Live need not be contiguous | H leaves a confirmed interior between two Live epochs | T20 checks confirmed roots; fully drained T28 leaves exactly the frontier Live |
| Discovery survives a moving consumer, out-of-order initialization, stale tail, and reused slots | D1/D2, E/I, F/P, G; J1 rejects a candidate discharged before its pinned re-read and releases its pin; J2 and J3 attach either side of a discharge; K interleaves two subscribers on one candidate; X attaches on an exact boundary; L and LN attach inside a three-boundary table, with the successor `Es` acquire and its negative control | T23 late attachment after wrap; T28 wake/drained cohorts |
| Increment-ahead references and spanning bodies survive reuse | `farm_ordering.d`, `leaf_reuse.d`, `write_protection.d` | `testSpannedTables`; T16 position-reference progress; T21 body integrity over multiple laps |
| Every published payload executes exactly once, with the right callback and contents | T00 harness negative controls; per-ID ledger in `subscription_live.d` | Per-ID checks alongside totals, including T16/T18/T20/T28; T21 word-for-word bodies; T25 borrowed callback metadata |
| Attached consumers make progress without another subscription rescuing them | `subscription_live.d` decides failure before its diagnostic rescue | T28 checks the existing cohort before departure, with no steady helper or final rescue; T29 lets 24 herd arrivals and departures overlap production over eight laps |

Root/leaf equality and quiescent Live scans run only after operations have
stopped or reached a barrier. A leaf zero-edge propagation is not an atomic
multi-counter snapshot: asserting that equality during propagation would reject
legal executions. A drained Farm must have exactly one Live, on the initialized
frontier; an abandoned Farm may have several, including holes between them.

The per-ID ledger is required even when totals balance. T00 demonstrates that
`[0, 2]` calls is a failure when `[1, 1]` was expected, and checks that the batch
builder preserves the callback supplied by a test. All callback IDs are checked
before indexing the ledger.

T18 deliberately permits a later subscriber to recover an abandoned backlog.
T06/T07/T23 and other steady-consumer arms remain useful contention checks, but
cannot alone establish that a churner's attachment makes progress. T28 tests
progress without those helpers, using four complete cohorts per configuration: 16 and 32
threads at K=2 and K=16 on a 1 MiB ring, alternating a full-ring wake and an
immediate resubscribe into the drained ring. Each configuration reuses the ring
for multiple laps. A watchdog bounds the test; barriers make arrivals and
departures contend and separate observations from activity. There is no CPU
affinity requirement or assertion of hardware-wide simultaneous execution, and
this is not a performance benchmark.

### Evidence limits

Controlled schedules check specific interleavings. Stress and TSan provide
execution evidence on the tested compiler and hardware; hook/barrier ordering
can strengthen those executions. They do not prove the I3 no-lap induction or
oldest-first discovery under the D/C++ weak-memory model. The existing
plain-write completion/leaf-reuse TSan checks remain separate from the new
barrier-controlled lifetime tests.

The `K + 2` subscribe-round budget remains unproved. `SUBSCRIBE_RETRY` stays
recoverable, and the herd test retries it. Tests do not assume a hard two-pin
limit: an attempt can retain up to K `Sub` pins. See [SPEC 11](../../SPEC.md#11-rationale-and-history-appendix)
for those open obligations.

## Layout

- `torture_common.d` — counters, batch builder, producer/consumer helpers
- `antfarm_test.d` — deterministic `sanity` entry point and the complete
  Farm/actor unit suite. The normal no-argument run includes sanity.
- `torture_tests.d` — T00–T02, T04, T06, T09, T10, T13, T15–T18, T20–T26,
  T28 and T29 (the unit suite covers the same-geometry runs that used to
  sit between them). T04 runs twelve consumers at K = 4, 8 and 16. T28 runs
  barrier-gated 16/32-member cohorts at K=2 and K=16; T29 is a 24-thread
  subscribe herd at the 1 MiB ring minimum whose arrivals and departures
  overlap production over about eight laps, counting `RETRY` and checking
  exactly-once and a drained Farm afterwards. T20 checks Live discharge under
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
  (I); a discharge landing at the subscriber's re-read, at its validation,
  or around its attach while the discharger is between its load and CAS
  (J1, J2, J3); a `Sub` landing in that same CAS window (Q); two subscribers
  interleaved on one candidate (K); an exact boundary landing with an empty
  frontier (X); and a subscriber attaching at the interior of a table that
  spans three boundaries, with the successor `Es` visible (L) or hidden
  during its first validation as an instrumentation negative control (LN);
  and adversarial rounds that drive the tail until retained frontier pins
  refuse renewal (R, for every supported K). D, E, F and G fail if they miss
  their requested interleaving; I alone permits its after-attach path.
  Every payload carries an id that must execute exactly once, and progress
  is judged with the views that already exist, before any new subscription;
  a rescue subscriber afterwards is diagnostic only. Every schedule ends by
  requiring that no member, reference, pin or leaf remains. Run all with no
  argument, or one schedule by name with an optional trigger; the schedules
  alone under TSan are `make -C tests/farm run-subscription-live-tsan`. The
  design record, with the ablations that show each schedule fails on the
  defect it targets, is [docs/design/farm-live-lifecycle.md](../../docs/design/farm-live-lifecycle.md).
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
  epoch zero, and 512-word tables. A reservation that ends exactly on a
  boundary initializes the next segment and must come out of a checked
  grant (SPEC 5a, 5c): the producer stops at `Wt=229376` and rejects 10,000
  further attempts instead of lapping the pin.
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
- `rc11_spanned_attach.py`: exhaustive RC11 enumeration of the
  spanned-interior attach chain (SPEC 6b-c, 6d) with and without the
  successor `Es` acquire; the stale-metadata executions it finds without
  the acquire are all RC11-only, which is why no schedule can reach them.
  Its header states what the last variant over-approximates.
- `quota_model.py`: exhaustive exploration within a two-lap bound of a
  small sequentially consistent quota model. Probe, each scan load, grant,
  reservation, and consumer pin movement can interleave. It omits the
  implementation's publication, subscription, Live, leaf propagation,
  and weak-memory details; a pass is not a proof of the full implementation.
  Its automatic-refill negative control is intentionally simplified.

The audit scheduling hooks are compiled only with `AntfarmWriteAuditHooks`.
Ordinary builds contain no scheduling hook calls. The producer phases are
`anchorProbed`, `quotaSwept`, `tailReserved`, `liveRaised` and
`tablePublished`; the subscriber phases are `subscriberCounted`,
`subscriberProbed`, `subscriberPinned`, `subscriberRead`,
`subscriberReadMid` and `subscriberValidating`; `liveClearing` pauses a
consumer's discharge between its load and its CAS. Each is listed with its
arguments beside the enum in `antfarm/package.d`.

The ledger is process-wide and its entries are accessed atomically. Allocate
it before starting worker threads and free it after they join; a missing or
out-of-range callback ID fails the test.

Stormer threads (T06/T18) are one `StormJob` class instance per thread: a
loop-local delegate capturing a slot address would alias every thread to
the last slot.

## ThreadSanitizer

`make -C tests/farm run-tsan` and `run-t19-tsan` build with LDC
`-fsanitize=thread -d-version=TSan`. The TSan build defaults to
`history_size=7 halt_on_error=1` (`__tsan_default_options`; override with
`TSAN_OPTIONS`). A TSan report is a defect.


The Farm, actor, private-actor, review, and example lanes all use these
flags. Farm has no manual acquire/release TSan annotations: the sanitizer
must observe the real leaf/root/sweep/reservation chain. The root-level
`make -C tests/farm check-farm-ordering` target adds deterministic S1/S2/span schedules, the
subscription schedules of `subscription_live.d`, and the leaf-reuse test under LDC and DMD; `make -C tests/farm run-farm-tsan` includes leaf reuse
and the subscription schedules under TSan. See [the ordering audit](../../docs/design/farm-ordering.md) for proofs and mutation
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
