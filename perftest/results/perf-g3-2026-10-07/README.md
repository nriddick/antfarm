# G3 cache-line placement results — 2026-10-07

**Negative result: no runtime change lands.** Base is `daad3d9`. The tip of
`perf/g3-placement` is `daad3d9` plus these records. Worktree:
`/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree`. Nothing was pushed
or written to the main checkout.

- Under the gate's default mimalloc, tip's root park already happens to sit
  in the favourable layout. The remaining six-consumer drift is not false
  sharing in the actor/private metadata. It is remote free in mimalloc
  (tree/6, private-remote/6), Entry/state/completion handoffs (private/6),
  and per-slot lifecycle CAS plus the Farm writer (actor/6, wave/6).
- That layout depends on where the 200-byte root park is allocated. With the
  library's default C-runtime allocator, or mimalloc at 64-byte alignment,
  the park lands at 0 mod 64. E1's hazard then returns: the consumer-read
  allocator shares a line with the queues the emitter exchanges on every
  pump. Removing that hazard (P1d) gained up to +18.5% at tree/6 under CRT
  in six-pair screens. The CRT baseline there is only about 10.6 M/s,
  against about 54 M/s under mimalloc.
- P1d failed the gate at default mimalloc: up to −15.6% at subset/0, −4.1%
  at tree/0 and −3.0% at actor/0, against byte-identical controls of −1.5%,
  −0.5% and +0.6%. Instruction counts are unchanged. The IPC drop coincides
  with the park's hot fields moving past offset 127 (1-byte displacement
  range) and with the resulting code-layout shifts; this is correlation, not
  an isolated cause, and the subset mechanism was not isolated.
- P1a, a targeted version of the audit's broad padding, cost tree-cont/6 and
  tree-pipelined/6 about 7%. The mechanism is documented below. P2 and P3 had
  no robust effect.

## P1d gate (base daad3d9 vs candidate 1091472, not landed)

P1d is one `align(64)` on `Service.batchLimit`, plus `align(64)` on the
park's `service_` field, because DMD does not propagate member alignment.
Service keeps tip's line grouping, but independent of the allocation
address: farm and allocator alone on line 0, and batchLimit, the queues,
records and rootEntries on line 1. The park's emitter fields follow from +128.
The commit is kept as branch `proto/p1d-gated` and as
[investigation/prototypes/p1d-gated-1091472.patch](investigation/prototypes/p1d-gated-1091472.patch).
It passed `make check` ([verification/check-1091472.log](verification/check-1091472.log)).

| Run | Pairs/cell | Cells | Pass | Inconclusive | Regression | Exit | Raw runs | Contaminated runs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| [full-5](full-5/gate.json) | 5 | 34 | 24 | 10 | 0 | 2 | 340 | 0 |
| [boundary-0](boundary-0/gate.json) | 20 | 11 | 3 | 3 | 5 | 1 | 440 | 0 |
| [boundary-6](boundary-6/gate.json) | 20 | 2 | 2 | 0 | 0 | 0 | 80 | 0 |
| [control-boundary-0](control-boundary-0/gate.json) | 20 | 11 | 10 | 1 | 0 | 2 | 440 | 0 |
| [control-boundary-6](control-boundary-6/gate.json) | 20 | 2 | 2 | 0 | 0 | 0 | 80 | 0 |

Every full-5 cell that was inconclusive, or whose paired minimum was within
one point of −3%, ran 20 pairs against the candidate and 20 in a
byte-identical control. tree/6 ran at 20 pairs regardless of its result.

| Cell | Candidate 20-pair median | Interval | Decision | Control median | Control interval |
| --- | ---: | --- | --- | ---: | --- |
| actor/0 | −3.04% | −3.37…−2.42% | inconclusive | +0.58% | +0.30…+0.70% |
| private/0 | −1.06% | −1.38…−0.64% | pass | −0.51% | −0.73…−0.13% |
| tree/0 | −4.09% | −5.37…−2.95% | inconclusive | −0.50% | −1.43…+0.55% |
| tree-wait/0 | −3.60% | −6.62…−2.41% | inconclusive | +0.95% | −1.76…+1.63% |
| subset1/0 | −13.34% | −15.21…−12.67% | **regression** | −3.06% (control inconclusive) | −3.54…−2.30% |
| subset10/0 | −15.64% | −16.28…−15.22% | **regression** | −1.52% | −2.02…−0.62% |
| subset100/0 | −13.91% | −14.74…−13.23% | **regression** | −0.12% | −0.45…+0.28% |
| subset-whole/0 | −10.38% | −10.76…−10.17% | **regression** | −0.06% | −0.26…+0.31% |
| unreserved whole/0 | −10.08% | −10.77…−9.64% | **regression** | +0.03% | −0.56…+0.44% |
| tree-cancel/0 | −1.37% | −1.65…−1.11% | pass | +0.74% | +0.12…+1.24% |
| private-tracked/0 | −0.54% | −0.86…−0.35% | pass | −0.20% | −0.56…−0.05% |
| tree/6 | −1.87% | −2.11…−1.09% | pass | +0.52% | +0.33…+0.77% |
| subset100/6 | −0.57% | −1.37…−0.19% | pass | +0.57% | −0.48…+2.06% |

All other six-consumer full-5 cells passed. Their medians range from −1.56%
(tree-wait/6) to +0.51% (private-remote/6). [per-cell-deltas.md](per-cell-deltas.md)
has every table and [commands.json](commands.json) every command. Each run
waited for one-minute load ≤ 1 before admission
(`investigation/scripts/gate.sh`). Binaries were deleted after the runs;
[binary-sha256.txt](binary-sha256.txt) records them, and both control builds
are `cd26f136…`, identical to base.

**Why P1d loses at zero consumers.** Instruction counts per unit are
identical (`investigation/profiles/zero-consumer-counters.txt`): tree/0 is
212.79 and actor/0 685.0 in both binaries. Only IPC changes: tree/0 from
3.31–3.41 to 3.14–3.16, actor/0 from 3.13–3.18 to 3.05. The drop coincides
with two placement changes; neither was isolated as the cause:

- The park's emitter fields moved from +0x68…+0x80 to +0x90…+0xa8, which
  needs 4-byte instead of 1-byte displacements. Root-park functions grow, for
  example from 590 to 625 bytes. Later text shifts by 0x4c0–0x770 bytes,
  so 556 of 3,316 text symbols change size or 64-byte alignment
  ([code-placement.txt](investigation/profiles/code-placement.txt)). The
  untouched `ActorRuntime.enter` and `reclaimActor` move from 48 to 0 mod 64,
  and `mi_free_size_aligned` and `mi_malloc_aligned` move from 0 to 48.
- The candidate's subset1/10/100 throughput at 0 consumers is bimodal, while
  base's is stable in those cells. Base subset-whole/0 is itself bimodal: 184
  vs 209 M/s in `zero-consumer-counters.txt`, and its byte-identical control
  has pairs of about ±13% (−12.56…+14.50%). Candidate subset10/0 ran 128.9–130.5 M/s in full-5 and 107.0–115.9 M/s in
  boundary-0, while base medians were 133.85 and 134.45 M/s. In a later
  fast-mode check it ran −3% with the same instruction count and 20% more
  int-cache fills per unit (84–86 vs 70–71). The exact mechanism was not
  isolated.

**What P1d would have bought.** Six-consumer screens against base, with six
pairs each (`investigation/screens/p1d-*.txt`):

| Allocator | Base park mod 64 | tree | tree-wait | tree-cont | private-remote |
| --- | ---: | ---: | ---: | ---: | ---: |
| crt (library default) | 0 | +18.54% (+16.46…+19.28) | +15.81% | +6.02% | +4.40% |
| mimalloc64 | 0 | +13.59% (+12.10…+14.00) | +14.56% | +12.08% | +7.74% |
| mimalloc (gate) | 32 | −1.59% | −1.91% | +0.39% | +1.45% |

**Footprint.** Nothing changed on the tip. The gated P1d grew the root park
from 200 bytes (208 with asserts) to 256, 64-byte aligned: Service went from
88 to 128 bytes. P1a had the same footprint. P3 grew cohorts from 376 to 384
bytes, which stays in the same mimalloc 384-byte class. P2 was size-neutral.
Service is only ever embedded in a root park, so no Service is allocated
separately. Entry (40 B), ActorSlot (128 B) and inbox nodes (32 B) were
never touched.

**Next step, not tried.** A fix for the CRT hazard must not push the park's
hot emitter fields past +127, or must be paid for elsewhere. One option is to
place an aligned Service after the emitter fields, or to move only the
allocator words to the park's end. Any candidate needs the CRT and mimalloc64
screens above, plus this gate at default mimalloc.

## How contention was measured

`perf c2c` and `perf mem` cannot run on this host. The kernel exposes no IBS
PMU ("no PMU supports the memory events"), sysfs is absent even outside the
sandbox, and `perf_event_paranoid` is 2. Two other sources were used.

- **Address probe** (`investigation/scripts/probe.py`, never timed). It copies
  a ref with `git archive`, adds `printf` calls that log field addresses in
  `PrivateRootPark.create`, `PrivateChildPark.create` (first 512 cohorts) and
  `ActorRuntime.create`, builds the copy with the gate's LDC command, and runs
  each mode for 0.05 s over several starts. `probe_analyze.py` then groups
  fields by 64-byte line. `alloc_placement.sh` repeats the root-park check for
  the benchmark's `mimalloc`, `mimalloc64` and `crt` allocators.
- **Zen 3 raw coherence events**, sampled user-only with `perf record -c`:
  - `r0243`: demand data-cache fills from L3 or another core's L2 in the CCX
    (`ls_dmnd_fills_from_sys.int_cache`). This event also counts L3 capacity
    hits, so it is not a pure HITM count.
  - `r0860`: L2 change-to-writable requests (`l2_request_g1.change_to_x`),
    i.e. stores to lines held shared.

  `hotip.py` maps samples to instructions using symbol offsets. AMD sampling
  without IBS has a few instructions of skid, so each attribution names the
  load or RMW just before the sampled instruction, with the field offset
  decoded from the operand. `threads.py` and `threadev.py` split samples
  between the controller and the workers.

The 6-consumer captures are at 3 s, 16,384 actors, batch 256 and quota 16,384,
using the gate's CPU pinning. Text reports are in `investigation/profiles/`;
the `perf.data` files were not kept.

## Placement at base daad3d9 (default mimalloc)

From `investigation/placement/probe-summary-base.txt` (10 starts per mode):

| Object | Size / address mod 64 | 64-byte lines |
| --- | --- | --- |
| Root park | 200 B, always at 32 | L0 farm, allocator · L1 batchLimit, incoming, waiting, rootReturns, childReturns, records, rootEntries, entries_ · L2 capacity…batchCount (emitter-local) · L3 storageLimit, spare_, returned_, outcome FIFO |
| Cohort (`PrivateChildPark`) | 376 B (392 with results), always at 0 (mimalloc 384/448 classes) | L0 next, parent, service, root, wave.emittable, pending, active, failures · L1 upstream, hook, finish, cancel, dispose, resume.next · L2 resume FIFO, tracked, entries_, capacity_, count_ · L3 cursor_, batchLimit_, participants_, dispatched_, flags, sel_, k_, header_ |
| `ActorRuntime` | 256 B, at 0 | L0 allocator, slots, readyHead, readyCount · L1 liveCount, stale, readyCarry, gates, freeSlots, header |

Neighbouring cohorts never shared a line: 830–9,437 adjacent pairs were
checked per mode. Entry, Batch and batch-entry arrays start 64-byte aligned
in every mode except subset, where the root park's arrays for a single root
start at 16 and 32.

Root-park placement depends on the allocator (`alloc_placement-base.txt`):

| Allocator | Park mod 64 | Fields sharing the allocator's line |
| --- | ---: | --- |
| mimalloc (gate) | 32 | farm |
| mimalloc64 | 0 | farm, batchLimit, incoming, waiting, rootReturns |
| crt (`ActorAllocator.cRuntime`, the library default) | 0 | farm, batchLimit, incoming, waiting, rootReturns |

At 0 mod 64 the park reproduces E1's layout. Consumers read the allocator on
every child free, while the emitter exchanges `incoming` and rewrites
`waiting` on every pump. Base under default mimalloc avoids this only because
of where the park happens to be allocated.

## Where the six-consumer coherence traffic goes

Shares of all `r0243` fill samples unless marked (`profiles/*.symbols.txt`;
instruction maps in `hot-instructions.txt`):

| Cell | Dominant sites | Attribution |
| --- | --- | --- |
| tree/6 | `mi_free_size_aligned` 54%; child `runChild` 10% (payload word, `Entry.state`/`enclosing`, the benchmark's Completion counter); `runCycle` verification loop 8% | Remote free and true-sharing handoffs. The cohort `service` and root-park allocator reads are about 5% of `runChild`. |
| private-remote/6 | `mi_free_size_aligned` 54%, `_mi_page_free_collect` 6%; collect 9%, pump 8% (Entry `disposition`/`next`) | Remote free and Entry handoffs |
| private/6 | `runRoot` 22% (payload word, `Entry.cleanup`, state, Completion); collect 15% (`Entry.disposition`); pump 15% (`Entry.next`) | Entry/state/completion handoffs. Root-park field accesses appear only as skid, at most a few percent of a symbol. |
| tree-cont/6 | `mi_free` 14%; **`publishWave` 7.7% (CAS on `Service.incoming`, +0x28) and `releaseMember` 4.3% (CAS on `Service.childReturns`, +0x40)**; cohort `emit` `lock incq wave.pending` (41% of emit's `r0860`) | The only contended metadata line: root park L1, plus intentional `wave.pending` sharing |
| subset100/6 | selected `runChild` 22% (`entry.enclosing` from a 640 KB Entry array that exceeds L2; `retire_` +0xe0 beside the emitter's `cursor_`); `r0860` 70% in the Farm subset writer | Mostly capacity misses and the Farm writer |
| actor/6, wave/6 | `r0860`: `ActorRuntime.enter` 37% (`lock cmpxchg` on slot `lifecycle`), Farm writer 20–27%, wave `dispatchActorWavePayload` 20% (slot `lifecycle`), `ActorWave.releaseCredit` 13% | Per-slot true sharing in 128-byte slots plus the Farm. No `ActorRuntime` field is hot. |

So the drift in tree/6 and private-remote/6 is in mimalloc's cross-thread
free. In private/6 it is in Entry and state handoffs. In actor/6 and wave/6 it
is in per-slot lifecycle CAS and the Farm. None of these is metadata false
sharing that field placement could remove. Per the plan, the public runtime
and the Farm are reported only.

## Prototypes, one at a time against base

Informal alternating A/B (`investigation/scripts/ab.py`, `screen2.sh`): six
process pairs per cell, gate arguments, admitted at load ≤ 1. Patches are in
`investigation/prototypes/`, logs in `investigation/screens/`. These are
screens, not gate results.

| Prototype | Change | Footprint | Six-consumer screen (paired median, min…max) |
| --- | --- | --- | --- |
| P1a | `align(64)` on `Service.incoming`, with farm, allocator, batchLimit, records and rootEntries on their own line | root park 200 → 256 B | **tree-cont −6.70% (−8.84…−4.00), tree-pipelined −6.85% (−7.66…−6.27)**, remote −1.28%, tree +0.15%, tree-wait −0.07%, private −0.30%, subset100 −0.45%; tree-cont/0 +1.89% |
| **P1d** (gated; failed) | `align(64)` on `Service.batchLimit` only: tip's grouping, made independent of allocation address | root park 200 → 256 B | mimalloc: tree −1.59%/−1.76%, tree-wait −1.91%, tree-cont +0.39%/+0.54%, tree-pipelined +0.93%, remote +1.07%/+1.45%, private −0.85%, subset100 +0.48%. **crt: tree +18.54% (+16.46…+19.28), tree-wait +15.81%, tree-cont +6.02%, remote +4.40%. mimalloc64: tree +13.59%, tree-wait +14.56%, tree-cont +12.08%, remote +7.74%, private −1.26%** |
| P2 | `PrivateChildPark` field order: emitter `cursor_`/`count_`/`k_` beside the resume FIFO; `entries_`, `capacity_`, `batchLimit_`, `sel_`, flags and participants on a member-read line | none (376/392 B) | Every cell within ±1.3% by median: tree-cont −0.20%, subset100 −1.28%, subset10 +0.54%, remote +0.39%, tree-cancel +0.38% |
| P3 | `Wave` first in `ChildBase`; next, parent, service, root, dispose and tracked on their own line; resume link and FIFO on the next | cohort 376 → 384 B (same mimalloc class) | tree-wait +0.68%, remote +1.28%, subset100 +1.24%, tree-cancel +1.50%, tree +0.54%, **private −1.99% (−2.28…−1.83)** |

P1a reproduces the audit's broad-padding loss with a much smaller change, and
the per-thread profiles explain it (`profiles/tree-cont-6-{base,p1a}.cycles-threads.txt`):

- On the workers, P1a does what was intended. `publishWave` falls from 7.27%
  to 2.29% of worker cycles and `releaseMember` from 2.96% to 1.95%.
- The freed worker time goes into polling the ring: `consumeNext` rises from
  16.45% to 22.32% and `sweepCurrentPosition` from 6.66% to 9.44%.
- The controller is the bottleneck, and its Farm writer share rises from
  53.83% to 56.55%. Controller fills per leaf rise 3% (0.901 → 0.928) and
  throughput falls 7%.

In controller-bound fold trees (and subset100/6, as in G2), making consumers
cheaper only increases polling pressure on the lines the writer is storing to.
Separating consumer-side queue heads cannot win there. Broad padding of the
root park or Service should not be retried.

P2 and P3 have no robust effect. P3's private/6 −1.99% comes from a binary in
which private mode never touches `ChildBase`, so it measures code placement.
That puts P3's +0.7…+1.5% gains inside layout noise. Neither was combined
with anything or gated.

## Verification

- The gated P1d commit `1091472` passed `make check` (LDC and DMD, debug and
  release, TSan, torture, private suites, examples, death tests). The first
  DMD build failed the new size assert, because DMD gave the park alignof 8.
  Adding `align(64)` to the `service_` field fixed it, and LDC output was
  byte-identical (`ac598420…`, the same as the screened `proto/p1d`).
- The tip of `perf/g3-placement` adds only data and documentation on top of
  `daad3d9`. At the tip, `make check` passed ("ALL CHECKS PASSED",
  `verification/check-tip-daad3d9.log`), and so did `make check-soak` (78
  TSan loops in 121 s, `verification/check-soak-tip-daad3d9.log`).

## Artifacts

- `investigation/scripts/`
  - `probe.py`, `probe_analyze.py` and `alloc_placement.sh`: placement.
  - `profile.sh`, `hotip.py`, `threads.py`, `threadev.py` and `statcmp.py`:
    counters.
  - `ab.py` and `screen2.sh`: screens.
  - `gate.sh` and `table.py`: the gate runs and the per-cell tables.
- `investigation/placement/`: probe summaries for base, P1a, P1d and P2, and
  root-park placement per allocator.
- `investigation/profiles/`: per-symbol `r0243`/`r0860` reports for the
  target cells, the hot-instruction maps, the tree-cont/6 per-thread split
  for base and P1a, the zero-consumer counters, and the code-placement diff.
- `investigation/prototypes/`: the P1a, P1d, P2 and P3 diffs against base,
  plus the gated P1d commit.
- `investigation/screens/`: the informal A/B logs.

The scripts expect the scratch layout they ran in: binaries in
`/tmp/g3/bin` (`churn-base`, `churn-p1a`, …, built with the gate's LDC
command) and profiles in `/tmp/g3/prof`.

The prototype screens used `ab.py`. They waited for load ≤ 1 at the start of
each prototype's screen, not before every cell. Their conclusions rest on
consistent pair signs and mechanisms, not on the gate.
