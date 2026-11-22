# Worker timekeeping: spinning, timed parks and the grid (design note, 2.2.2)

Status: implemented in the working tree of `2.2.2-progress` on 2026-11-22
(fifth revision). Sections 1 to 6 are the record of what was measured and
decided; section 7 says what was built and what the pool then measured. The
numbers of sections 1 to 4 come from scratch probes that are not in the
tree, and from OMMe's wait probe.

**Added in the fifth revision.** Section 7, and a note under 5.9.

**Changed in the fourth revision.** Sections 5 and 6 are rewritten:
workers belong to a set of processors and not to one, placement is an
option, and `cadence` gives way to `expect`, with the grid left to the
caller. Sections 1 to 4 are as they were.

**Added in the third revision.** OMMe's runs from a process with a window
(section 2 and the appendix), threads confined to a subset of processors
and a sweep of the park lead (section 2), how the pool keeps a worker in
its cache group and what could replace the pin (section 4), and the
owner's lean on placement.

**Correction to the first revision.** Its 2-minute Windows and WSL2 tables
said the probe workers were pinned through CPU Sets as the pool pins. They
were not pinned at all: the pin call sat inside an `assert`, which a
release build drops. Its claim that CPU Sets pinning removed the late wakes
was therefore wrong, and the opposite is true (section 2). The tables below
replace it.

## 1. Conditions

- Hardware: i7-12700H (6 P-cores with SMT on LP 0 to 11, 8 E-cores on LP 12
  to 19), Windows 11 build 26200, Balanced plan, on AC.
- The desktop was in ordinary use: a browser, Steam and two Electron apps
  running, about 10% total CPU.
- Workers: 4, at 240 Hz (period 4,166,667 ns), thread high QoS as
  `highQosOnP` sets it. "Pinned" means what the pool's `pinProcessor`
  does on Windows: a CPU Set preference for one logical processor plus a
  hard affinity mask of that one processor.
- The test process had no window and no console, so focus does not apply.
  OMMe's runs with a window are under "With a window" in section 2.
- Runs are 2 minutes (about 115,200 beats) unless a row says otherwise.
- Every figure is one run. Repeats of the same configuration differed by up
  to 4×, so read the tables for their order of magnitude.

## 2. What was measured

### The shipped cadence park

Through the real pool, pinned by the pool, 10-second runs:

| Windows | beats reached | late p50 |
|---|---|---|
| lead 0 | 2,726 of 9,596 (28%) | 2.1 ms |
| lead 1 ms | 2,718 (28%) | 1.8 ms |

- The park is `WaitOnAddress` with a millisecond timeout, which ends on
  the process's 15.6 ms timer tick. Workers sleep through 3 beats in 4.
- On WSL2 the futex park truncates to whole milliseconds: lead 0 gave
  p99 1.26 ms, and a 1 ms lead gave p99 19 µs.

### PAUSE and a worker that never yields

Cost of the primitives, pinned:

| | P-core (LP 2) | E-core (LP 14, 19) |
|---|---|---|
| one PAUSE | 36 to 40 ns | 18 ns |
| one clock read (`MonoTime.currTime`) | 14 ns | 27 ns |

The shipped pool under `Director.spin()`, pinned by the pool:

| workers | poll interval | first visit after a beat: p50 / p99 / p99.9 | > 100 µs | > 1 ms | beats skipped | stalls > 1 ms |
|---|---|---|---|---|---|---|
| P-cores 2,4,6,8 (128 PAUSEs) | about 5.5 µs | 2.6 µs / 303 µs / 2.75 ms | 1,772 | 447 | 176 | 2,911 |
| E-cores 12,14,16,18 (16 PAUSEs) | about 1.2 µs | 0.3 µs / 2.8 µs / 358 µs | 196 | 49 | 79 | 226 |

- A stall is a gap of more than 1 ms between two consecutive body visits
  of an idle worker. The worst was 35 ms on the P-cores and 41 ms on the
  E-cores.

A standalone spin that waits for each beat, reading the clock every k
PAUSEs:

| spin | pinned | > 100 µs | > 1 ms | late p99 | p99.9 | max | package power |
|---|---|---|---|---|---|---|---|
| PAUSE, k = 1 | P 2,4,6,8 | 983 | 525 | 51 µs | 6.3 ms | 158 ms | 36.8 W |
| PAUSE, k = 128 | P 2,4,6,8 | 1,790 | 837 | 473 µs | 7.5 ms | 721 ms | 35.4 W |
| no PAUSE, clock reads only (60 s) | P 2,4,6,8 | 201 | 110 | 1.4 µs | 3.2 ms | 36 ms | 50.3 W |
| TPAUSE, C0.2 | P 2,4,6,8 | 1,411 | 564 | 198 µs | 4.7 ms | 22 ms | 33.5 W |
| PAUSE, k = 1 | E 12,14,16,18 | 139 | 43 | < 0.1 µs | 149 µs | 57 ms | 34.3 W |
| PAUSE, k = 1 | not pinned | 88 | 5 | 0.2 µs | 84 µs | 3.4 ms | 14.7 W |

- With k = 1 the median lateness is below the 0.1 µs the probe prints.
  With k = 128 it is 2.6 µs.
- The unpinned row's 14.7 W is unexplained. Its loop also ran at 97 ns
  against 59 ns pinned, and Windows moved the threads among about ten
  logical processors. Nothing below relies on that row's power figure.

### Timer park plus spin lead

Standalone probe: a per-thread high-resolution waitable timer and a wake
event, then a PAUSE spin for the lead.

| pinned | lead | beats missed | > 100 µs | > 1 ms | late p99 | p99.9 | park overshoot p50 / p99 | wake latency p50 / p99 | package power |
|---|---|---|---|---|---|---|---|---|---|
| P 2,4,6,8 | 0 | 1,628 | 109,950 | 5,697 | 4.4 ms | 10.8 ms | 429 µs / 4.4 ms | 45 µs / 5.6 ms | 20.1 W |
| P 2,4,6,8 | 0.5 ms | 1,660 | 28,858 | 4,776 | 4.5 ms | 10.1 ms | 440 µs / 5.0 ms | 47 µs / 5.1 ms | 23.7 W |
| P 2,4,6,8 | 1 ms | 1,441 | 7,267 | 4,782 | 4.3 ms | 8.4 ms | 450 µs / 5.3 ms | 39 µs / 5.4 ms | 27.1 W |
| P 2,4,6,8 | 1.5 ms | 1,226 | 7,070 | 4,268 | 3.9 ms | 9.3 ms | 475 µs / 5.3 ms | 43 µs / 6.1 ms | 27.9 W |
| not pinned | 1.5 ms | 0 | 823 | 442 | 39 µs | 2.3 ms | 447 µs / 1.2 ms | 16 µs / 162 µs | 30.1 W |

- Pinned, the pool's own configuration with the proposed 1,500 µs default
  lead had 3.7% of beats more than 1 ms late and another 1.1% missed.
- An earlier unpinned run at the same lead had 65 beats over 1 ms and
  p99.9 320 µs. That is the run-to-run spread.

### Where pinned workers lose their core

- The stalls are uneven across cores. In 30-second pinned spins, gaps over
  1 ms per worker were:
  - LP 2, 4, 6, 8: 391, 130, 539, 186;
  - LP 3, 5, 7, 9: 195, 109, 406, 111;
  - LP 10, 11, 0, 1: 23, 20, 43, 28.
- One 30-second run at `THREAD_PRIORITY_TIME_CRITICAL` on LP 2, 4, 6, 8
  had 62 gaps over 1 ms, against 1,246 in a normal-priority run a few
  minutes later. It still had 37 beats over 1 ms late.
- The cause is not established. Core parking is active (most logical
  processors are parked when the machine is idle), but the parked-at-idle
  cores were not consistently the bad ones.

### With a window (OMMe's runs)

OMMe repeated the two waits from a process that owns a window
(`omme-wait-probe`, 2026-11-20, the same machine and background, 4 to 5%
load from other programs). Every run is in the appendix, and in OMMe's
`wiki/threads.md` section 1.5.

- **Waits.** `spin` is PAUSE and a clock read for the whole period. `park`
  is the high-resolution timer until 1,500 µs before the beat, then the
  same spin.
- **Threads.** `nothing` is a thread created and left alone. `qos` adds
  thread high QoS. `pinned` is `pinProcessor` on LP 2, 4, 6, 8 plus QoS.
- **Process.** No `timeBeginPeriod`, process QoS, MMCSS or priority change,
  except in the row that says so.
- **Runs.** Four threads beside the window's thread, 120 s, two series per
  configuration with the second in reverse order. Focus was sampled ten
  times a second and held in every run.

Beats over 1 ms late + beats missed, of about 115,200. First series;
second series.

| wait | threads | process settings | focused | unfocused | no window |
|---|---|---|---|---|---|
| spin | nothing | none | 0 + 0; 0 + 0 | 1 + 0; 0 + 0 | |
| spin | qos | none | 0 + 0; 0 + 0 | 0 + 0; 0 + 0 | 0 + 0 |
| spin | pinned | none | 91 + 11; 104 + 15 | 471 + 136; 616 + 109 | 496 + 89 |
| park | nothing | none | 123 + 0; 73 + 0 | 131 + 0; 364 + 0 | |
| park | qos | none | 216 + 0; 115 + 0 | 115 + 0; 343 + 0 | 293 + 0 |
| park | pinned | none | 372 + 42; 525 + 53 | 1,256 + 522; 1,097 + 543 | 836 + 245 |
| park | nothing | `timeBeginPeriod(1)` + process high QoS | 0 + 0; 0 + 0 | 0 + 0; 0 + 0 | |
| park | nothing | none, the window's thread waits for beats too | 68 + 0 | 258 + 0 | |

What OMMe read from them, counting a difference under 4× as none:

- **Unpinned spinners are reliable.** One beat over 1 ms in nine runs,
  focused or not, and 0 to 8 stalls a run.
- **Pinning hurts under focus too.** Pinned spinners had 102 and 119
  beats over 1 ms late or missed when focused, and 607 and 725 unfocused.
  Focus helps the pinned case by 5× to 6×; it has nothing to improve in
  the unpinned one.
- **An unpinned park keeps a tail** of 68 to 364 beats over 1 ms in every
  state, and focus was not shown to change it (the series disagree). The
  late beats are common to all four threads and nearly all "parked": four
  timers due at one instant are late together. Park overshoot was 0.44 to
  0.50 ms at p50 and 1.0 to 1.2 ms at p99.
- **`timeBeginPeriod(1)` with process high QoS removed that tail:** no
  beat over 1 ms in four runs. Which of the two settings does it was not
  separated.
- **`nothing` is as good as `qos`.** Unpinned threads ran on P-cores for
  94 to 99% of samples either way.
- **No window behaves like an unfocused window.**
- **Against the windowless rows above:** unpinned agrees. Pinned, the
  windowless runs here were worse (park 4,268 + 1,226 against OMMe's
  836 + 245).

Not measured by either session:

- a pinned thread under `timeBeginPeriod(1)` and process high QoS, which
  OMMe always runs with;
- why a pinned thread loses its processor, and why focus helps it.

### Confined to a subset of processors

A stand-in for a group-wide set on a machine with several L3 groups, which
this machine is not: four threads confined to part of it
(`subset_probe.d`, 2026-11-22 05:14 to 05:58, no window).

- **Waits** as in OMMe's runs: `spin`, and `park` with a 1,500 µs lead.
  Every thread has thread high QoS. There is no producer thread and no
  sampler beside the runs.
- **Placements.**
  - unconfined, which is OMMe's `qos` (thread high QoS and nothing else);
  - the pool's pin, one processor per thread (LP 2, 4, 6, 8), and each of
    its halves alone: the hard mask, and the one-id CPU Set;
  - one hard mask shared by the four threads;
  - CPU Sets shared by the four threads.
- **Shared sets.**
  - `{2,4,6,8}` has as many processors as threads.
  - `{0,2,4,6,8,10}` is the six P-core primaries, so two cores are spare.
  - `{2-9}` is four whole P-cores, so the only spare processors are SMT
    siblings.
- **Runs.** 60 s (about 57,600 beats), three series in different orders.
  Every mask and CPU Set was read back after it was set, and no
  hard-masked thread was sampled outside its mask.
- **Conditions.** Other programs used 1.2 to 3.6% of the machine (6.4% in
  one run). The system timer period read 1 ms at all but two run
  boundaries; in OMMe's runs it was mostly 10 ms.

Beats over 1 ms late + beats missed, of about 57,600, in each series.
A stall is a gap over 1 ms between clock reads; a move is a change of
processor between samples. Both are per run, in series order.

Spin:

| placement | processors | series 1 | series 2 | series 3 | stalls | moves |
|---|---|---|---|---|---|---|
| unconfined | all 20 | 0 + 0 | 0 + 0 | 0 + 0 | 6, 2, 0 | 34,084, 28,299, 33,700 |
| pool pin (CPU Set + hard mask) | one each of 2,4,6,8 | 249 + 160 | 57 + 45 | 54 + 38 | 549, 164, 167 | 0, 0, 0 |
| hard mask only | one each of 2,4,6,8 | 400 + 212 | 53 + 46 | 67 + 54 | 921, 151, 201 | 0, 0, 0 |
| CPU Set only | one each of 2,4,6,8 | 260 + 154 | 68 + 60 | 28 + 25 | 486, 189, 104 | 4, 11, 5,269 |
| hard mask, shared | `{2,4,6,8}` | 165 + 107 | 111 + 122 | 62 + 47 | 280, 304, 176 | 9, 21, 5 |
| CPU Sets, shared | `{2,4,6,8}` | 132 + 168 | 112 + 467 | 94 + 292 | 252, 207, 231 | 1, 5, 22 |
| hard mask, shared | `{0,2,4,6,8,10}` | 4 + 1 | 4 + 0 | 3 + 0 | 13, 17, 14 | 26,769, 20,854, 20,288 |
| CPU Sets, shared | `{0,2,4,6,8,10}` | 52 + 27 | 142 + 864 | 27 + 22 | 87, 199, 80 | 23,027, 13,492, 17,094 |
| hard mask, shared | `{2-9}` | 73 + 60 | 36 + 32 | 63 + 39 | 198, 86, 108 | 9,553, 6,137, 7,705 |

Park, 1,500 µs lead:

| placement | processors | series 1 | series 2 | series 3 | park overshoot p99 | worst park overshoot |
|---|---|---|---|---|---|---|
| unconfined | all 20 | 0 + 0 | 0 + 0 | 0 + 0 | 1.10 to 1.12 ms | 1.5 to 1.6 ms |
| pool pin | one each of 2,4,6,8 | 80 + 103 | 100 + 88 | 72 + 57 | 1.09 ms | 30 to 32 ms |
| hard mask, shared | `{2,4,6,8}` | 3 + 0 | 0 + 0 | 0 + 0 | 1.51 ms | 1.7 to 2.8 ms |
| hard mask, shared | `{0,2,4,6,8,10}` | 0 + 0 | 0 + 0 | 0 + 0 | 1.51 ms | 1.6 to 1.8 ms |
| CPU Sets, shared | `{0,2,4,6,8,10}` | 56 + 76 | 34 + 16 | 24 + 4 | 1.52 ms | 7 to 31 ms |

What they show:

- **A hard mask over the six primaries cost four spinners little, but not
  nothing.** They lost 3 to 5 beats in every series, against none
  unconfined and 92 to 409 pinned. Their worst gap was 3.8 to 6.4 ms,
  against 0.8 to 1.8 ms unconfined. The threads did not get there by
  sitting on the two quiet processors: they spent 73 to 80% of each run on
  LP 2, 4, 6, 8 and had 13 stalls a run there, against 176 to 304 when
  confined to those four alone.
- **A set with no spare processor is as bad as a pin.** In `{2,4,6,8}`
  each thread stayed on one processor (5 to 21 moves a run) and lost as
  many beats as a pinned thread. With two spare cores each thread moved
  about 100 times a second.
- **Spare SMT siblings were not shown to help.** `{2-9}` lost 68 to 133
  beats a run against 109 to 272 for `{2,4,6,8}`, which is inside the
  run-to-run spread. The two sets with spares differ in more than cores
  against siblings: the six primaries also include LP 0 and 10, the quiet
  processors. So these runs do not say which of the two matters.
- **CPU Sets were worse than a hard mask where the set had spare cores,
  and no better anywhere.** Over the six primaries they lost 49 to 1,006
  beats a run spinning and 28 to 132 parking, against 3 to 5 and none.
  They also did not hold: parked threads were sampled outside their
  six-processor set for 0.6 to 2.7% of a run, and the one-id sets for 25%
  of one run.
- **Neither half of the pin is the cause alone.** The hard mask only and
  the CPU Set only were each as bad as the two together.
- **Parking threads did well under a hard mask in these six runs, but the
  set with no spare did not hold up in later ones.** Here: 3 beats over
  1 ms in six runs and none missed. The park overshoot p99 rose from
  1.1 ms to 1.5 ms, which is the lead: at least 1% of parks returned
  after the beat, by some 10 µs, and for those the lead bought nothing.
  In the lead series below, four of nine more runs of the no-spare set
  had park overshoots of 5 to 27 ms and missed beats.

What they do not show:

- A machine with several L3 groups. This is one group of 20 processors,
  hybrid, with core parking on.
- Any confined placement at the default system timer period. Every run
  read about 1 ms, and the three near a 15.6 ms reading were 3× to 8×
  worse. The unconfined case is known good at 10 ms from OMMe's rows; the
  hard mask with spares is not. OMMe sets `timeBeginPeriod(1)` itself, so
  this is its condition, but a consumer that does not ask for 1 ms is not
  covered.
- A mask over a whole group, siblings included. The one good set was
  primaries only.
- How many spare cores are enough. Two of six was the only case.
- Sets over E-cores.
- Why any of it happens. Nothing was traced.

Two things to keep in mind when comparing with the tables above:

- The unconfined park had no late beat here, where OMMe's rows without
  process settings had 68 to 364 per 115,200. The 1 ms timer period is the
  likely reason, as in OMMe's `timeBeginPeriod(1)` rows.
- The first three one-processor runs of series 1 were 3× to 8× worse than
  their repeats. The timer period read 15.6 ms at two of those run
  boundaries and 1 ms everywhere else. Whether that is the cause is not
  established.

### Varying the park lead

The park of the series above with the lead as an argument
(`lead_probe.d`, 2026-11-22 07:20 to 07:48, no window).

- **Leads.** 1,000, 500 and 250 µs. The 1,500 µs rows below are the park
  rows of the series above, run two hours earlier with the same loop.
- **Placements.** Unconfined; a hard mask over `{2,4,6,8}` (no spare
  processor); a hard mask over `{0,2,4,6,8,10}` (two spare cores). Four
  threads on one grid, each with thread high QoS.
- **Runs.** 60 s, three series in different orders, 27 runs. Every mask
  read back as set, and no hard-masked thread was sampled outside its
  mask.
- **Conditions.** The system timer period read 1.00 ms at every run
  boundary. Other programs used 1.4 to 3.8% of the machine (6.8 and 7.2%
  in two runs).
- **Reading rule**, fixed before the runs: placements are compared at the
  same lead, and a difference counts only when the ranges over the three
  series do not overlap.

Shares of about 57,600 beats, as the range over the three series.
Lateness percentiles are in µs. The last column is beats over 1 ms late +
beats missed, in series order.

| lead | placement | parks ending at or after the beat | > 100 µs late | > 500 µs late | late p99 | late p99.9 | > 1 ms + missed |
|---|---|---|---|---|---|---|---|
| 1,500 µs | unconfined | under 1% | 0.02 to 0.03% | not recorded | 2.5 to 4.4 | 36 to 41 | 0 + 0, 0 + 0, 0 + 0 |
| 1,500 µs | two spares | at least 1% | 0.02 to 0.03% | not recorded | 10 to 12 | 34 to 48 | 0 + 0, 0 + 0, 0 + 0 |
| 1,500 µs | no spare | at least 1% | 0.02 to 0.05% | not recorded | 12 to 14 | 46 to 57 | 3 + 0, 0 + 0, 0 + 0 |
| 1,000 µs | unconfined | 1.3 to 1.7% | 0.20 to 0.32% | 0 to 0.02% | 17 to 18 | 146 to 229 | 2 + 0, 0 + 0, 0 + 0 |
| 1,000 µs | two spares | 4.9 to 6.4% | 0.28 to 0.40% | 0.01 to 0.02% | 25 to 35 | 165 to 214 | 1 + 0, 0 + 0, 0 + 0 |
| 1,000 µs | no spare | 11.2 to 13.2% | 0.22 to 0.24% | 0.01 to 0.03% | 21 to 30 | 147 to 193 | 0 + 0, 16 + 2, 3 + 0 |
| 500 µs | unconfined | 45.6 to 46.6% | 19.6 to 21.0% | 0.8 to 1.1% | 476 to 506 | 723 to 784 | 4 + 0, 5 + 0, 10 + 0 |
| 500 µs | two spares | 48.2 to 51.8% | 19.6 to 24.2% | 0.7 to 1.3% | 453 to 529 | 686 to 840 | 21 + 0, 17 + 0, 22 + 0 |
| 500 µs | no spare | 47.2 to 49.8% | 20.3 to 24.4% | 1.0 to 1.6% | 499 to 551 | 701 to 778 | 31 + 12, 4 + 0, 9 + 0 |
| 250 µs | unconfined | 89.7 to 93.0% | 61.3 to 62.8% | 5.4 to 9.0% | 672 to 786 | 902 to 1,092 | 19 + 0, 93 + 0, 110 + 0 |
| 250 µs | two spares | 93.2 to 94.2% | 63.7 to 66.6% | 10.0 to 10.4% | 783 to 804 | 1,021 to 1,069 | 116 + 0, 71 + 0, 85 + 0 |
| 250 µs | no spare | 93.8 to 94.3% | 65.0 to 66.5% | 9.6 to 10.0% | 788 to 808 | 1,108 to 7,736 | 219 + 202, 120 + 0, 125 + 12 |

What a lead costs, over all placements:

| lead | spin per beat, mean | the four threads' CPU time, share of the machine |
|---|---|---|
| 1,500 µs | about 0.9 to 1.0 ms (worked out from CPU time) | 4.2 to 4.8% |
| 1,000 µs | 0.46 to 0.54 ms | 2.2 to 2.8% |
| 500 µs | 0.07 to 0.09 ms | 0.3 to 0.5% |
| 250 µs | under 0.01 ms | 0.1% |

Park overshoot past the timer's due time, in the 27 runs: p50 0.40 to
0.51 ms, p99 0.92 to 1.06 ms, and p99.9 1.13 to 1.37 ms in all but one
run. Placement and lead moved these by about 0.1 ms or less, apart from
the two things below.

Parks at the 1,000 µs lead, by when they returned:

| placement | 0 to 50 µs after the beat | of those, returned on a processor where another of the four had seen that beat | later than 50 µs after the beat | of those, the same |
|---|---|---|---|---|
| unconfined | 1.0 to 1.2% | 73 to 80% | 0.3 to 0.4% | 37 to 53% |
| two spares | 4.4 to 5.8% | 94 to 96% | 0.5 to 0.6% | 61 to 66% |
| no spare | 10.8 to 12.9% | 98 to 99% | 0.3 to 0.4% | 71 to 89% |

What they show:

- **The lead has to cover the timer's overshoot, which spans about a
  millisecond.** With 1,000 µs, 0.3 to 0.6% of parks returned more than
  50 µs after the beat. With 500 µs about half the parks returned after
  the beat, and a fifth to a quarter of beats were over 100 µs late. With
  250 µs about two thirds were.
- **1,000 µs against 1,500 µs:** roughly ten times the beats over 100 µs
  late (0.2 to 0.4% against 0.02 to 0.05%), for about half the spin. The
  two leads come from series two hours apart, and single runs put the
  ratio anywhere from 4× to 20×.
- **Placement changed that trade little.** Beats over 100 µs late
  overlapped across the three placements at 1,000 and at 500 µs. At
  250 µs unconfined was about 3 points lower (61 to 63% against 64 to
  67%). The masks' other differences were of that size or smaller: 2 to
  5 points more parks ending after the beat at 500 µs, and with two
  spares 17 to 22 beats over 1 ms late a run at 500 µs against 4 to 10
  unconfined.
- **Under a mask more parks end just after the beat.** At 1,000 µs that
  was 1% of parks unconfined, 4 to 6% with two spares and 11 to 13% with
  no spare, in every series. Most of these parks returned within 20 µs of
  the beat: the no-spare threads' p90 lateness was 6 to 9 µs, against
  0.1 µs or less.
  - The group follows the beat, not the timer. The timer was due 1,000 µs
    before the beat and knows nothing of it, yet the group sits at the
    beat. At 1,500 µs the masked threads' overshoot p99 was 1,509 to
    1,513 µs, which puts it at the beat there too, and a 5 s check of the
    no-spare set at that lead had 3.8% of its parks within 20 µs after
    the beat.
  - The only thing that happens at the beat is that the other threads
    stop spinning and park. My reading is that these parks waited for a
    processor one of the others was spinning on. The processor counts
    lean that way but are weak evidence: 73 to 99% of these parks
    returned where another thread had seen that beat, against 37 to 89%
    of the parks that returned later.
- **With no spare processor, all four threads' parks ran long together in
  4 of 9 runs.** The worst overshoots were 5.2, 5.5, 16.2 and 27.1 ms,
  and those runs missed 2, 12, 12 and 202 beats, spread across the four
  threads. No other run of the 27 had an overshoot over 3.7 ms or a
  missed beat.
  - By the rule above this is not established at any one lead, since no
    lead had it in all three series. Taken over the three leads it is 4
    of 9 runs against 0 of 18.
  - That 5 s check at 1,500 µs, outside the series, had a fifth: a 9.9 ms
    overshoot and 7 beats missed across the four threads. So the three
    60 s runs at 1,500 µs in the series above, which had no long
    overshoot, do not show that lead to be safe.
  - This withdraws the "even with no spare processor" of the series
    above, which rested on those three runs.

What they do not show:

- Any other system timer period. Every run read 1.00 ms.
- A worker that starts a job at the beat. The probe's threads park again
  as soon as they see the beat. If the reading above is right, that is
  what lets a waiting thread in after 20 µs; a pool worker would keep the
  processor for the length of its job. So the 20 µs is what the probe's
  threads lost, not a figure for the pool.
- Threads on different grids, or fewer than four waking at once.
- Whether the two-spare set's clean record comes from its spare cores or
  from LP 0 and 10 being the quiet processors.
- Why the no-spare parks ran long. Nothing was traced.
- The 1,500 µs rows are from the earlier series and lack the new
  counters.

### Power

Idle read 8.5 to 12.0 W across the session, so differences of a few watts
mean nothing. The steps that stand out, for 4 workers:

| state | package power |
|---|---|
| idle | 10 to 12 W |
| park only (lead 0, 960 wakes per second) | 20 W |
| park plus 1.5 ms lead | 28 W |
| PAUSE spin for the whole period | 35 to 37 W |
| spin without PAUSE | 50 W |

TPAUSE (WAITPKG is present on this CPU) was within noise of PAUSE.

## 3. Answer: what PAUSE resolves

- **Resolution is not the limit.** One PAUSE is 36 to 40 ns on a P-core
  and 18 ns on an E-core. A spin that reads the clock every PAUSE reaches a
  deadline within about 60 ns. The pool's own spin polls its body every
  5.5 µs on P-cores and every 1.2 µs on E-cores.
- **A higher-energy final spin buys nothing.** Dropping PAUSE raised
  package power from 37 W to 50 W and reached the beat no sooner at the
  median.
- **What limits a spinner is losing its core.** Pinned P-core spinners on
  the shipped pool were more than 1 ms late for 0.4% of beats.
- **PAUSE for the bulk of the period saves little energy.** A whole-period
  spin costs about 25 W over idle for four cores, a park with a 1.5 ms
  lead about 17 W, and a park alone about 9 W.

## 4. The finding that matters most

On this machine a pinned worker's lateness comes mainly from not getting
its core, not from the wait primitive.

- Pinned timer parks overshoot by 4 to 5 ms at p99, against 1.2 ms
  unpinned.
- Pinned wake latency is 5 to 6 ms at p99, against 0.16 ms unpinned.
- Pinned spinners stall for milliseconds, up to hundreds in the standalone
  probe.

The pool pins every worker to one logical processor by design. The
possible responses are a decision for the owner, not the pool:

- **Thread priority** for time-keeping workers, opt-in per selection. One
  run suggests fewer stalls. It was listed as "not asked now".
- **A wider pin** for time-keeping workers: several logical processors of
  the worker's group instead of one.
- **Choice of processors.** The spread between cores was 10× to 20×.
- **Leave it and report it** through the stats in section 5.

OMMe's runs with a window agree: pinned threads were the only ones that
missed beats, in every focus state. The owner's lean (2026-11-22) is to
leave workers unpinned, at least on Windows.

### How a worker is kept in its cache group today

Workers have no group-level affinity. Three things make the corral:

- **Selection.** One worker per accepted logical processor
  (`resolvePool`). A host narrows that to one group or several
  (`planHost`, `HostGroups`).
- **A label.** Each worker's `llcIndex` is copied from its processor at
  spawn. `home!` and `at!`, the per-group Farms, `wakeLlc`,
  `wakeOneArmedIn` and `Selection.llc` read the label. None of them asks
  where the thread is running.
- **The pin.** `workerMain` calls `pinProcessor`: on Windows a one-id CPU
  Set plus a hard one-processor mask, on Linux a one-CPU
  `sched_setaffinity`. It is the only thing that keeps the thread inside
  its group, and `start` fails if any pin fails.

### What could replace the pin

The label machinery does not depend on the pin, so the two come apart.

- A hard mask over several processors already exists
  (`confineCallingThread`, used only for the caller).
- `SetThreadSelectedCpuSets` takes a count, so a soft set of several
  processors is a small addition.
- Neither is applied to workers today.

What is tied to "a worker is a logical processor":

- The worker count and `PoolResolution.workers` are accepted processors,
  and `skipSmtSiblings` is a choice of processors.
- `pool.tag(ProcessorId, ...)` names a worker by its processor.
- `isP` and `classIndex` come from the processor. They set the spin count,
  the QoS tag, `classP` and `classE`, and the class axis of `install`. In
  a group that mixes P-cores and E-cores, an unpinned worker has no class
  unless its set is that class's processors.
- `StartState.pinned`, `PinOutcome` and the failed-pin error.
- The caller's confine set is "home-group processors no worker owns".

What the measurements cover:

- This machine is one L3 group of all 20 logical processors. "Unpinned"
  and "confined to the group" allow the same processors here, so OMMe's
  `nothing` and `qos` rows are that case.
- Removing the pin with nothing in its place gives up the corral on a
  machine with several L3 groups: a worker still consumes only its own
  group's Farm, but may run on another group's cores.
- The stand-in for that machine ("Confined to a subset of processors" in
  section 2) found one confined placement that kept spinners near the
  unconfined result: a hard mask over P-core primaries with fewer
  spinners than processors in it (four on six). It still cost 3 to 5
  beats a run.
- That result is for a system timer period of 1 ms only, and for a set
  without SMT siblings.
- Parking threads under a hard mask with two spare cores stayed close to
  unconfined at every lead tried (1,500 to 250 µs): the same share of
  beats over 100 µs late down to 500 µs, and about 3 points more at
  250 µs. More of their parks ended within 20 µs after the beat.
- Parking threads under a mask with no spare processor did not hold up:
  4 of 12 runs of 60 s had park overshoots of 5 to 27 ms and missed
  beats.
- CPU Sets did worse than a hard mask, and a set of exactly as many
  processors as spinners was no better than the pin.
- A lead under a millisecond does not cover the timer: at 500 µs a fifth
  to a quarter of beats were over 100 µs late in every placement
  ("Varying the park lead" in section 2).
- All of the parking results are for a system timer period of 1 ms.

## 5. Proposal (fourth revision)

This replaces the third revision's section 5. Its stateless `cadence`,
the sequence-locked `(epoch, period)` pair and the `nudge` verb are
withdrawn in favour of 5.6 and 5.7. Nothing here is implemented.

**Decided by the owner on 2026-11-22**, and written in below:

- The grid moves to the caller; `expect` is the pool's one timing verb
  (5.6, 5.7).
- The default placement changes on Linux as well as Windows. The owner
  will look at Linux's behaviour on the other host (5.2).
- The class axis is dropped (5.1).
- Unstaffed processors by default: none in a group of two or fewer,
  otherwise one in eight and at least one (5.3).
- The API breaks now, in 2.2.2 (5.4).

Also decided that day: the parks of 5.8 as recommended there, and the
six readings of section 6 as written. Nothing is left open.

### 5.1 A worker belongs to a set, not a processor

- A **set** is the accepted processors of one L3 group, P-cores and
  E-cores together.
- A worker has a group and an ordinal in its set. The workers of a set
  are interchangeable.
- **A worker has no class.** `Selection.classP` and `classE`, the class
  axis of `install`, the class argument of `at!` and `setLabel`,
  `WorkerSelf.classIndex` and `isP`, and the separate P and E loops and
  counters all go.
- The filters keep their meaning: `processors`, `enablePCores`,
  `enableECores` and `skipSmtSiblings` decide which processors a set
  holds. A CPU's class stays in the topology and in `CpuDisposition` as
  information.
- Work routing does not change. `home!`, `at!`, the per-group Farms,
  `wakeLlc`, `wakeOneArmedIn` and `Selection.llc` already read the group
  label and never the processor.

### 5.2 Placement

`PoolOptions.placement` says what the OS is told about a worker's set.

| value | a worker may run on | Windows | Linux |
|---|---|---|---|
| `free` | anything the process may | nothing | nothing |
| `group` | its set | one hard `GROUP_AFFINITY` mask over the set | `sched_setaffinity` over the set |
| `pin` | one processor of its set | today's `pinProcessor` | today's one-CPU mask |
| `automatic`, the default | `free` when the process can reach only one L3 group, `group` otherwise | as the row it picks | as the row it picks |

- **`group` uses a hard mask and not CPU Sets.** CPU Sets lost 49 to
  1,006 beats a run where the mask lost 3 to 5, and did not hold.
- **`pin` is today's behaviour**, kept as an option on both systems.
  Pinned threads were the only ones to miss beats in OMMe's runs, and
  here the pin lost 92 to 409 beats a run where unconfined threads lost
  none.
- **The default is the same rule on Linux.** Nothing in this note
  measures Linux natively; the owner is taking that up on the other
  host.
- **Under `free` the filters set the count and nothing else.** No mask
  is applied, so `enableECores = false` or `skipSmtSiblings` no longer
  keeps a worker off an E-core or a sibling; on this host the default is
  `free`. A consumer that needs a filter enforced asks for `group`. This
  follows from the placement and class decisions together; confirmed.
- **Thread QoS and the spin count become one setting each**, since a
  worker has no class: `PoolOptions.qos` (high, eco or unchanged; high by
  default, which is what P workers get today) in place of `highQosOnP`
  and `ecoQosOnE`, and `spinIters` (128) in place of `pSpinIters` and
  `eSpinIters`. This reading of the class decision is confirmed.

What each default would do, with OMMe's options (`HostGroups.all()`, a
Farm per group, `skipSmtSiblings`, the caller left alone, automatic
count):

| machine | today | proposed default | nearest measurement |
|---|---|---|---|
| this host: one L3, 6 P primaries and 8 E-cores accepted | 14 workers, each pinned | 13 workers in one set of 14, `free` | OMMe's `qos` rows, which had four threads on P-cores. Thirteen at once, some of them on E-cores, has no measurement |
| two L3 groups of 8 cores with SMT | 16 workers pinned to the primaries | 7 workers per group, masked to the group's 8 primaries | the stand-in's four threads on six primaries. Seven on eight has no measurement, and the stand-in is not such a machine |

### 5.3 How many workers, and the unstaffed processors

The owner's rule for the automatic count, with `n` the accepted
processors of a set:

| `n` | unstaffed | workers |
|---|---|---|
| 1 or 2 | 0 | `n` |
| 3 to 15 | 1 | `n - 1` |
| 16 to 23 | 2 | `n - 2` |
| 24 to 31 | 3 | `n - 3` |

- **What `n` counts.** I read "group size" as the set's accepted
  processors, after the filters. With `skipSmtSiblings` a group of 8
  cores is 8 and leaves 1 unstaffed; counted as 16 logical processors it
  would leave 2. Confirmed.
- **Rounding.** I read "one in eight, at least one" as `max(1, n / 8)`
  rounded down. Rounded up it would be 2 from 9 processors and 3 from 17.
  Confirmed.
- **Scope.** I apply it to the automatic count under every placement, so
  the count does not change when the placement does. Confirmed.
- **It replaces the caller's reservation.** Today `autoWorkers` keeps one
  home-group processor free when the caller is placed, and staffs every
  processor when it is not. The unstaffed processor is now what a
  confined caller runs on, and in a group of two the caller shares.
- **An explicit `workers` count may fill a set.** The summary then says
  the group has no unstaffed processor.
- **What the rule rests on.** A masked set with no unstaffed processor
  was as bad as the pin for spinners, and had park overshoots of 5 to
  27 ms in 4 of 12 runs. The one ratio measured good is four threads on
  six processors; one in eight is not measured.
- **The caller.** `CallerPlacement.pin` and
  `GroupHostOptions.callerProcessor` take that processor out of the home
  set, as they do today.
  - Under `free` no mask keeps workers off the caller's processor, so
    the reservation is not kept and the summary says so. A caller that
    needs its processor to itself asks for `group`.

### 5.4 What changes in the API

| today | proposed | OMMe |
|---|---|---|
| `PoolResolution.workers` is one `ProcessorId` per worker; `workersIn(llc)` | `sets`: group, processors, worker count, placement. `workerCount` and `workerCountIn(llc)`. `workers` becomes one record per worker: group, ordinal, start state, and its processor under `pin` | `jobs.d:236` tests `workersIn(llc).length == 0`; it becomes `workerCountIn(llc) == 0` |
| `CpuDisposition.workerIndex`; `StartState.pinned` and `pinFailed`; "failed to pin worker(s)" | a start state per worker, `placed` or `placeFailed`; a CPU reports whether it is in a staffed set | none |
| `pool.tag(ProcessorId, label or number)` | `Selection.label(name)` and `Selection.number(n)` through the Director, with `take(n)` and `skip(n)` to choose how many | no call |
| `Selection.classP`, `classE`; `install(bins, plan, classCount)`; the class argument of `at!` and `setLabel`; `WorkerSelf.isP`; `highQosOnP`, `ecoQosOnE`; `pSpinIters`, `eSpinIters` | removed; `PoolOptions.qos` and `spinIters` (5.2) | no call. In this tree the fork-join dispatcher sets both spin counts to 1 (`dispatcher.d:892`) and `tests/threadpool/live_sisyphus.d` reads `isP`; the C ABI exposes none of them |
| `autoWorkers` staffs every accepted processor, less one in the home group when the caller is placed | the rule of 5.3 | 13 workers on this host where there were 14 |
| `CallerPlacement.confine`: home-group processors no worker owns | the home group's eligible processors | uses `unchanged` |
| `Selection.cadence`, `nudge`, `drift`; `Cadence`; `IdleKind.cadence`; `decideCadence`; `effectiveCadencePeriod`; the `ThreadpoolCadence` gate | removed; `expect` and `readiness` (5.6) | no call. `omme.time` calls `cadenceGrid`, which stays |
| `WorkerSelf.cpuSetId`; `WorkerRec.group` and `lpIndex` | meaningful under `pin` only | none |

- **The break is made now, in 2.2.2**, with no deprecated aliases left
  behind.
- `GroupHost.director` and `FarmHost.director` already hand out the
  Director by reference, so OMMe reaches every verb below through its
  `GroupHost`.
- Nothing in the tree calls `tag`, `labeled`, `numbered`, `classP` or
  `classE` outside the pool's own tests.
- These are breaking changes for anything that reads
  `PoolResolution.workers`. The readers in this tree were not listed one
  by one: a loose search finds the names in 28 files, among them the
  fork-join dispatcher, the C ABI (`cabi/source/antfarm/c/groups.d`,
  `runtime.d`), the threadpool tests and several examples. Many of the
  hits are `WorkerSelf.workerIndex`, which does not change.

### 5.5 Hosts

- `HostGroups`, `span`, `farmPerGroup` and the caller options keep their
  meaning. Which groups supply workers is decided as today.
- `planHost` hands workers to sets, home group first, where today it
  picks processors.
- `summary()` names each set, its worker count, the placement and the
  spare. New notes: "group 1 has no spare processor", and "workers are
  not confined to their groups" for `free` on several groups.
- A start fails when the OS refuses a mask, as it fails today for a pin.

### 5.6 Readiness replaces cadence

A worker is told when the next burst of publication is expected, and is
awake and polling around that instant. It is told nothing else: no
period, no beat number, no slew.

```d
// On Director and on Selection.

/// A burst of publication is expected at `atNanos`: MonoTime in
/// nanoseconds, the clock `UvLoop.nanos()` reads.
void expect(long atNanos);

/// How long before the instant to be polling, and how long after it to
/// go on polling when nothing has come.
void readiness(uint leadMicros = 1_500, uint holdMicros = 250);
```

The publisher says when the next burst is due, each time, to the workers
it wants ready:

```d
// OMMe's main thread, each tick.
auto ready = host.director.llc(group).take(4);
waiter.waitUntil(grid.startOf(tick));
fork(tickWork);                        // the burst
ready.expect(grid.startOf(tick + 1));  // when the next is due
```

- **A window costs processor time.** At a 1,500 µs lead and 240 Hz each
  ready worker spins about 0.9 to 1.0 ms of every 4.17 ms, a quarter of
  a processor. At 1,000 µs it is about an eighth.
- **How many workers get a window is the caller's choice.** `expect` on
  the whole Director would ready all 13 workers on this host. That is
  unmeasured, and readying as many workers as a set
  has processors is the no-spare case of section 2. The workers that are
  not selected park as before and are woken by the publication.

What an idle worker does:

1. Inside a window it has entered: one spin round, then back to the pump.
2. The expected instant is less than the lead away, and not more than
   the hold past: enter the window, remember when it ends, one spin
   round.
3. The expected instant is further off: park precisely until the lead
   before it. Work, `signal`, a new `expect` that needs it, a policy
   change or stop ends the park at once. A managed deadline that is
   earlier bounds it, and so does the re-poll bound of a `sleep(d)` base
   policy (5.8).
4. Nothing is expected, or the hold has run out: the base policy (`wait`,
   `sleep`, `sleepUntil`), as today.

- **Where it sits.** Steps 1 and 2 go in the worker loops ahead of
  `prepareWorkerWait` (`hybrid.d:594` for the raw lane OMMe uses,
  `worker.d:129` for the managed lane of the fork-join dispatcher and the
  fibers), so a worker polling in a window is never armed. Steps 3 and 4
  stay in `applyIdlePolicy`, after the arm and the recheck.
- **Order does not matter.** A worker finishes the window it is in, so
  `expect` may come before or after the burst it follows.
- **When `expect` wakes a worker.** It stores the instant, then wakes the
  worker unless the instant it replaces is still ahead and no later than
  the new one. So it wakes when nothing was expected, when the old
  instant has passed, and when the new one is earlier.
  - In the loop above the old instant has just passed, so every `expect`
    wakes every selected worker.
  - Waking a worker that is not armed is one exchange with no system
    call (`wakeWorker`, `win_wait.d:54`). Those workers are running the
    burst or polling in their window, and neither is armed. A burst then
    costs no system call per worker.
  - A worker that has left its window and parked is armed, and the wake
    is a system call. That is the case where it is needed.
- **A tick with nothing to publish** costs the ready workers one window
  of polling. `expect` does not depend on a publication following it.
- **A publisher that stops** stops calling `expect`. The workers fall
  back to their base policy after the hold.
- **`hold` answers the third revision's question 2.** 250 µs is a
  placeholder: it should cover how late the publisher's burst is, which
  the feedback below reports.
- **The lead default stays 1,500 µs** ("Varying the park lead" in
  section 2).

How the three bugs of the shipped cadence go away:

| bug | where it lives | what removes it |
|---|---|---|
| With a spin lead, new work, `nudge` and `drift` wait for the beat | `parkCadence`, `hybrid.d:548`: the spin loop tests only the run flag and the clock | the lead is ordinary idle rounds that return to the pump, so work is seen within one round |
| Each beat gets two pump rounds | `decideCadence`, `hybrid.d:388`, with `WorkerSelf.lastCadenceBeat`: after a beat, `now >= lastBeat + period` holds once more and the worker pulls again before parking | there is no beat state; nothing is pumped per beat |
| `drift` jumps the phase and snaps back | `effectiveCadencePeriod`, `hybrid.d:402`: the changed period goes into `cadenceGrid`, which rescales every beat since the epoch | the pool has no period to slew |

### 5.7 Where the grid lives

Decided on 2026-11-22: **the grid moves to the caller, and Ant Farm
keeps the wait, the window and the arithmetic.** This replaces the
owner's earlier choice, an atomic republish of the `(epoch, period)`
pair to the pool. The reasons, as put to the owner:

What decides it:

- **All three bugs are in the pool's own copy of the grid** (5.6): the
  beat a worker last served, the slew it has applied so far, and a spin
  toward a beat it computed. A pool with no period has none of that
  state to get wrong.
- **Not every burst is on a grid.** OMMe's frame start is a predicted
  present slot less predicted cost, moved frame by frame
  (`presentation.md`). An instant covers that; a period does not.
- **A pool that owns the period keeps waking when the publisher has
  stopped**: a paused server, a loading screen. With `expect` the
  workers stop when the calls stop.

What supports it:

- **OMMe already owns the whole structure.** `omme.time.Grid` has the
  epoch, the period, a bounded nudge, a slew that re-anchors, a
  resynchronization and their accounting, in nanoseconds. From Ant Farm
  it calls one pure function, `cadenceGrid`. The pool's copy is a second
  grid that has to be kept equal to the first.
- **OMMe's grid can move on any tick**: a nudge of up to 25 µs, a slew in
  steps of 10 ppm. Each needs a republish to a pool that holds the pair,
  and none to one that is handed the next instant.

What does not decide it:

- **Truncation.** It can be fixed either way. `MonoTime` ticks are
  100 ns on Windows (10,000,000 per second here), so 4,166,667 ns is
  41,666.67 ticks; a pool that keeps the period would keep it in
  nanoseconds and convert each beat. An absolute instant is converted
  once and is wrong by less than one tick, with nothing to add up.

What stays in Ant Farm:

- The precise park, with its wake path ("Wait primitive" below).
- The window: `expect`, the lead and the hold.
- The feedback: park overshoot, lateness, how late bursts arrive.
- `cadenceGrid` as a pure function on integers in the caller's unit. It
  is correct, OMMe calls it, and it is the natural home for a test at
  4,166,667 ns.

The other way, a period kept by the pool with those faults fixed, costs
the sequence-locked pair, a `nudge` verb, a republish on every slew and a
rule for a stopped publisher. Its one advantage is that the caller makes
no call per burst.

### 5.8 Parks, feedback and reallocation

**Idle policies.** Work that is available is never delayed by a policy.
The Director sets policies per selection and can change them at any
time.

| policy | behaviour | change |
|---|---|---|
| `spin` | never yields; polls every `spinIters` PAUSEs | no longer arms its wake slot each round |
| `wait` | parks until woken (`WaitOnAddress`, futex) | none |
| `sleep(d)` | re-poll bound | none; stays on the OS tick |
| `sleepUntil(t)` | parks to a deadline | removed |
| `expect(t)` | an overlay on any of the above (5.6) | new; replaces `cadence` |

**Which parks become precise.** Decided on 2026-11-22 as the last column of the table below recommends.

What a timed park does today. Every one goes through `parkMs`
(`hybrid.d:570`):

- The time left is turned into whole milliseconds, rounded down, and at
  least 1 (`nsToMs`, `hybrid.d:561`).
- **Windows:** `WaitOnAddress` with that timeout, which ends on the
  system timer tick: 15.6 ms by default, about 1 ms in a process that
  asked for it with `timeBeginPeriod(1)` and had the request honoured.
- **Linux:** a futex wait with a relative timeout of whole milliseconds
  (`linux_wait.d:79`).

What "precise" means:

- **Windows:** the high-resolution waitable timer of "Wait primitive"
  below. Its overshoot was p50 0.40 to 0.51 ms and p99 0.92 to 1.06 ms
  here at a 1 ms system period, and p50 0.42 to 0.50 ms and p99 1.0 to
  1.3 ms in OMMe's runs, mostly at 10 ms.
  - That is a p99 and not a bound: the worst overshoot in a run here was
    1.4 to 3.7 ms, leaving out the no-spare set.
  - It was not measured at the 15.6 ms default period, which is the case
    a program that never asks for a finer one is in.
  - It is not exact, which is why `expect` has a lead.
- **Linux:** the same futex call with an absolute deadline in
  nanoseconds. Not measured natively.
- **Cost.** On Windows a precise park is two system calls (set the
  timer, wait on the timer and the wake event) where `WaitOnAddress` is
  one, and the waker signals an event. On Linux there is no extra call.

**One park can have a precise deadline and a coarse bound at once**, and
has to. Hosts idle with `sleep(1 ms)` by default, and a park under it
ends at the earlier of the re-poll bound and any deadline
(`hybrid.d:465`). If only the deadline were precise, a fiber deadline
5 ms out would still be slept through: the park taken is the 1 ms
re-poll, which lands on the tick.

- **Windows:** one wait on the timer and the wake event. The timer
  carries the precise deadline (the lead before a window, or a managed
  deadline, whichever is earlier), and the wait's own millisecond
  timeout carries the re-poll bound. I expect that timeout to end on the
  system tick as `WaitOnAddress`'s does; it was not measured.
- **Linux:** one futex wait to the earlier of the two.
- A park with no precise deadline stays as it is today.

The four parks:

| park | what it is for | today on Windows | precise would give | what precise costs | my recommendation |
|---|---|---|---|---|---|
| before an `expect` window | being ready for a burst | new | it is the point of the verb | one park per window | precise; it follows from the grid decision |
| a managed deadline | the earliest fiber timer of the worker's group: `FiberDomain.sleepUntil`, `sleepFor`, timeouts | a fiber's sleep ends up to a tick late, 15.6 ms by default, when every worker of its group is idle | late by about 0.5 ms at the median and 1 ms at p99, as measured above | the two calls, and only while a timer is armed | precise |
| `sleepUntil(t)` | idle workers park until `t`, then poll without parking until the policy changes (`hybrid.d:480`) | up to a tick late | the same | nothing recurring | remove it: nothing in the tree or in OMMe calls the Director's `sleepUntil` outside the pool's own test, and `expect` does its job. Precise if it is kept |
| `sleep(d)` | the re-poll bound of a parked host, `idleTimeout`, 1 ms by default: a publication made without a wake is found within `d` | every 15.6 ms; every 1 to 2 ms in a process at a 1 ms period | every `d` for every consumer | at the default, 1,000 wakes a second for each idle worker | leave it on the tick |

- **`sleep(d)` is a safety net and not a timing verb.** Four threads
  waking 240 times a second each drew about 20 W against 8.5 to 12 W
  idle ("Timer park plus spin lead", lead 0). A precise 1 ms re-poll is
  four times that rate per worker.
- **Linux already re-polls at about `d`**, by the code and not by
  measurement: its timeout is relative milliseconds, so an idle worker
  of a default host should wake about 1,000 times a second there today.
  Precise changes that only for a `d` under a millisecond.
- **OMMe pays that rate on Windows too.** By the code, not by
  measurement: it sets `idleTimeout` to 1 ms and asks for a 1 ms timer
  period, so each idle worker wakes every 1 to 2 ms. Every OMMe
  publication calls `wakeLlc` (`jobs.d:301` to `381`), so `Duration.max`
  would serve it, as the fork-join dispatcher does (`dispatcher.d:889`).
- **Managed deadlines: the herd is there already.** By the code, not by
  measurement: every idle worker of a group parks to the same earliest
  timer (`ant_fibers/groups.d:211`), so they wake together at each
  expiry, today on the tick. Precise changes when they wake, not how
  many.
- **A change from the third revision**, which proposed leaving managed
  deadlines alone. A deadline that is up to 15.6 ms late by default is
  the fault the cadence park had, and the cost falls only on a program
  that arms timers. Fiber timer lateness itself is not measured in this
  note.

**Wait primitive.**

- **Windows:** a high-resolution waitable timer and a wake event per
  worker, and a third wake-slot state so a wake reaches a worker waiting
  on the timer. Handles are kept per worker index for the life of the
  process, so a late waker never signals a closed handle.
- **Linux:** a futex wait with an absolute `CLOCK_MONOTONIC` deadline, on
  the same wake word. Numbers come from your native runs.

**Feedback.** One record per worker, written only by that worker, with no
shared counters. Read through a selection, which sums the workers and
takes the worst.

- **Readiness at each expected instant**, in the `WakeKind` vocabulary
  OMMe's `Waiter` uses:
  - ready in time;
  - `parked`: the park returned after the expected instant;
  - `spinning`: the worker lost its processor across the instant;
  - busy: it was running work at the instant.
  Each late kind has counts over 100 µs and over 1 ms, and its worst.
- **Park overshoot** histogram, for choosing the lead.
- **Burst arrival** histogram: from the expected instant to the first
  work found in the window, and the windows in which none came. It is
  for choosing the hold.
- **Stalls** for spinners: gaps between idle polls over 100 µs and over
  1 ms, and the worst.
- **Parks and wakes**, replacing today's `ThreadpoolStats` counters. The
  gate goes if the consumer-bound benchmark shows no cost, and stays if
  it does.

**Runtime reallocation.** The Director already applies any verb to any
selection at any time. `Selection.take(n)` and `skip(n)` choose how many
workers of a selection, which is all a set of interchangeable workers
needs: `director.llc(0).take(4).spin()`, or `expect` for four workers of
a group and plain parking for the rest. Changing the number of workers,
or moving them between groups, is not in this proposal.

**DUB.** The `ThreadpoolCadence` gate goes with the verbs it guarded.

### 5.9 What has not been measured

Every run in sections 1 to 4 measured beat timing. None measured
throughput, and a pinned worker keeps its L1 and L2 warm. Section 7 has a
first, rough throughput comparison from this host; the rest of this list
stands.

1. **Throughput** under `free`, `group` and `pin`, with the benchmarks in
   the tree (`threadpool/benchmarks/live_hybrid.d`,
   `tools/perf/actor_churn.d`).
2. **Whether one unstaffed processor in eight is enough.** The stand-ins
   this host allows: three threads on `{2,4,6,8}` and five on the six
   primaries, spinning and parking.
3. **A mask that includes SMT siblings:** four threads on `{0-11}`.
4. **A masked set that mixes P-cores and E-cores**, which a set now may.
5. **Linux**, natively. The owner's, on the other host.

## 6. Questions

None are open. The owner answered them on 2026-11-22:

1. **Which parks become precise** (5.8): the park before a window and
   managed deadlines become precise, `sleep(d)` stays on the tick, and
   `sleepUntil` is removed.
2. **Group size.** `n` is the accepted processors of the set, after the
   filters, not the logical processors of the group (5.3).
3. **Rounding.** Unstaffed processors are `max(1, n / 8)` rounded down
   for a set of three or more (5.3).
4. **Scope.** That count applies under every placement (5.3).
5. **QoS and spin count.** One `PoolOptions.qos` and one `spinIters` for
   the pool in place of the per-class options, with `enablePCores` and
   `enableECores` kept as processor filters (5.1, 5.2).
6. **Filters under `free`.** They set the count only; `group` is how a
   consumer has them enforced (5.2).
7. **Linux.** `automatic` follows the same rule as on Windows, and `pin`
   stays selectable on both (5.2).

## 7. As built (2026-11-22)

Sections 5 and 6 are implemented in the working tree of `2.2.2-progress`.
The reference for the API is now
[the threadpool design](threadpool.md) and
[its README](../../threadpool/README.md); this section records where the
build differs from section 5, and what the pool itself then measured.

### Where it differs from section 5

- **`free` on Linux sets a mask.** A Linux thread inherits its creator's
  affinity, and a host confines or pins its caller before it starts the
  pool. Left alone, the workers of a host with a pinned caller would all
  have inherited one processor. A `free` worker on Linux therefore takes
  the whole frozen OS scope explicitly. On Windows a new thread has the
  process's affinity, and nothing is set.
- **A set that spans two Windows processor groups cannot be masked.**
  `start` refuses `group` for it before any thread is created.
- **The count rule holds under `pin` as well**, and pinned workers take a
  set's first processors.
- **The count rule can be overridden.** `PoolOptions.workers` gives a bare
  pool an exact total, as the hosts' `workers` option does for a host. The
  performance harnesses use it, with `pin`, so that their recorded cells
  keep the configuration they name.
- **A pinned caller takes the home set's last processor out of the set**
  only when the workers are masked or pinned and an explicit worker count
  does not need that processor. Otherwise it shares, and the plan says so.
- **A window with no lead and no hold is no window**: the instant is
  ignored.
- **`expectNothing()`** withdraws an instant, and **`Selection.count()`**
  counts a selection.
- **The timing record has `missedWindows`.** A park that returns after the
  whole window is counted as a window, as parked late and as missed.
  Instants that pass while a worker is still stalled in an earlier park are
  not recorded at all, so under long stalls `windows` falls short of the
  instants given.
- **The fork-join dispatcher has `DispatcherOptions.placement`**, automatic
  by default. Its steal from an SMT sibling applies to pinned participants
  only.
- **The C ABI's resolve calls** return one entry per worker with its group,
  and `0xFFFF` for group and processor unless the worker is pinned.
- **The `ThreadpoolStats` gate stays.** The per-worker records are always
  on; the process-wide spin and park counters were not re-measured.

### What the pool measured

`expect-test` (`tests/threadpool/expect_live.d`): a publisher thread spins
to each instant, publishes four units of work without a wake, and gives the
next instant to four of the pool's 13 workers. 240 Hz, lead 1,500 µs, hold
250 µs, runs of 20 s for each placement: three in the order unconfined,
masked, pinned, and a fourth in the reverse order, since this laptop slows
as it warms. About 19,200 windows a run. The figures are the workers' own
timing records, in run order with the reversed run last.

| placement | not ready within 100 µs | of those, the park ran past the whole window | over 1 ms late | worst | windows recorded |
|---|---|---|---|---|---|
| `free` | 23, 16, 24, 28 | 9, 3, 15, 8 | 0, 0, 0, 0 | 0.3, 0.3, 0.8, 0.4 ms | 19,200 to 19,204 |
| `group`, a mask over the 14 accepted processors | 273, 300, 550, 1,094 | 249, 277, 530, 1,062 | 1, 0, 2, 0 | 1.1, 0.8, 1.4, 0.8 ms | 19,128 to 19,197 |
| `pin` | 811, 699, 1,221, 1,596 | 699, 635, 1,129, 1,438 | 531, 517, 914, 1,112 | 28.8, 43.3, 15.6, 29.3 ms | 18,252 to 18,757 |

- **The order of section 2 holds through the pool**, in all four runs and
  whichever placement ran first: unconfined, then masked, then pinned.
- **Under the mask the late windows are almost all parks that ended as the
  window closed.** They returned 250 to 850 µs after the instant, the hold
  being 250, and the park overshoot histogram has 256 to 1,138 parks
  between 1.5 and 2 ms against 3 to 15 unconfined. At the end of the hold
  the other ready workers stop polling and park. This is the pile-up of
  "Varying the park lead", with the cost section 2 could not measure: the
  worker is not 20 µs late but absent for the whole window. It was 1.3 to
  5.6% of windows here.
- **Bursts were found without a wake.** Of the windows that found work,
  all but 8 to 36 in about 5,650 found it within 50 µs of the instant.
- **Pinned workers also lost instants outright**: 443 to 948 of 19,200
  were never recorded.
- **Under the hosts' policy.** One second of the same under `sleep(1 ms)`,
  unconfined, in two of the runs: 962 and 963 of 964 windows ready. The
  1 ms bound ended about one park in ten before its lead there; under a
  1 ms system timer period it would end every one, which was not run.

Other measurements, one host and rough:

- **Managed deadlines.** With 13 idle workers given a 5 ms deadline, the
  service hook ran 520 times in 100 ms where it ran 210 times before (two
  calls a wake; one run each, from `host-test`).
- **Throughput.** `actor_churn`'s host path, four consumers under `spin`,
  interleaved against the 2.2.1 loop. Pinned as before: within 1 to 2%
  (ratio 0.99 on both workloads). Unconfined: level on `private`, about 7% lower on `tree`
  over the six pairs before the laptop throttled, and level after. A mask
  over exactly four processors for four consumers: 8% lower on `tree`, and
  as low as 8% of the baseline once the laptop throttled.
- **Fork-join.** fib(36) on 20 participants: 48.0, 48.2 and 48.5 ms pinned,
  masked and unconfined (medians of 7). On the first seven processors, which
  are sibling pairs: 121, 125 and 102 ms, the unconfined workers not being
  held to the siblings.
- **Linux.** The threadpool, fork-join and fibers lanes pass under WSL2
  with LDC, and the unit tests and wait races with DMD; each placement's
  affinity is read back there too. That checks function on one LLC group.
  It is not a Linux timing result.

What these do not show:

- The system timer period during the readiness runs. It was not recorded.
- A machine with several LLC groups, where `group` is the default. The
  masked row is a mask over this host's six P-core primaries and eight
  E-cores.
- Whether readying fewer workers, or workers on separate instants, avoids
  the missed windows under a mask. Not tried.
- Throughput on a host that does not throttle. The 5900XT is the place.

### For OMMe

- `jobs.d:236`: `_host.resolution.workersIn(llc).length == 0` becomes
  `_host.resolution.workerCountIn(llc) == 0`.
- The worker count on this host goes from 14 to 13, and the workers are no
  longer pinned.
- Readiness: `_host.director.llc(g).take(n).expect(grid.startOf(tick + 1))`
  after the tick's fork, with `readiness(lead, hold)` set once. `omme.time`
  is untouched; `cadenceGrid` is still there.
- `idleTimeout` of 1 ms makes each idle worker wake every 1 to 2 ms under
  OMMe's 1 ms timer period. Every publication already calls `wakeLlc`, so
  `Duration.max` would do.

## Appendix: OMMe's runs, one row each

2026-11-20 15:28 to 16:38. `s1` is the first series (focused run first),
`s2` the second (unfocused first). `x` has `timeBeginPeriod(1)` and
process high QoS. In `m` the window's thread also waits for each beat by
the same park, and "main" is that thread. `n` has no window. Lateness is
in microseconds; p50 was 0.0 in every run. "Late" is beats over 100 µs
late, split parked / spinning.

| run | window | wait | threads | beats | missed | > 100 µs | > 1 ms | p99 | p99.9 | max | late: parked / spinning | park overshoot p50 / p99, or stalls > 1 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| s1-01 | focused | spin | nothing | 115,200 | 0 | 19 | 0 | 1.1 | 44.5 | 365 | 0 / 19 | stalls 0, worst 0.8 ms |
| s1-02 | unfocused | spin | nothing | 115,200 | 0 | 30 | 1 | 3.6 | 51.7 | 1,444 | 0 / 30 | stalls 6, worst 1.8 ms |
| s1-03 | focused | spin | qos | 115,200 | 0 | 41 | 0 | 2.5 | 56.7 | 619 | 0 / 41 | stalls 5, worst 1.9 ms |
| s1-04 | unfocused | spin | qos | 115,200 | 0 | 66 | 0 | 11.8 | 79.8 | 887 | 0 / 66 | stalls 4, worst 1.7 ms |
| s1-05 | focused | spin | pinned | 115,189 | 11 | 1,088 | 91 | 79.6 | 697 | 12,090 | 0 / 1,088 | stalls 987, worst 13.9 ms |
| s1-06 | unfocused | spin | pinned | 115,064 | 136 | 1,502 | 471 | 310 | 3,455 | 26,123 | 0 / 1,502 | stalls 1,815, worst 27.7 ms |
| s1-07 | focused | park | nothing | 115,200 | 0 | 321 | 123 | 11.0 | 1,061 | 3,965 | 299 / 22 | overshoot 462 / 1,026 µs |
| s1-08 | unfocused | park | nothing | 115,200 | 0 | 337 | 131 | 8.9 | 1,163 | 3,339 | 295 / 42 | overshoot 440 / 1,014 µs |
| s1-09 | focused | park | qos | 115,200 | 0 | 696 | 216 | 19.3 | 1,268 | 3,120 | 671 / 25 | overshoot 504 / 1,190 µs |
| s1-10 | unfocused | park | qos | 115,200 | 0 | 743 | 115 | 76.1 | 998 | 2,833 | 354 / 389 | overshoot 459 / 1,051 µs |
| s1-11 | focused | park | pinned | 115,158 | 42 | 2,976 | 372 | 437 | 2,017 | 31,701 | 760 / 2,216 | overshoot 482 / 1,263 µs |
| s1-12 | unfocused | park | pinned | 114,678 | 522 | 3,082 | 1,256 | 1,095 | 7,302 | 36,074 | 2,228 / 854 | overshoot 464 / 2,478 µs |
| s2-01 | unfocused | park | pinned | 114,657 | 543 | 2,789 | 1,097 | 933 | 7,382 | 36,278 | 2,261 / 528 | overshoot 497 / 2,377 µs |
| s2-02 | focused | park | pinned | 115,147 | 53 | 1,236 | 525 | 137 | 2,296 | 31,399 | 742 / 494 | overshoot 441 / 1,198 µs |
| s2-03 | unfocused | park | qos | 115,200 | 0 | 841 | 343 | 34.0 | 2,463 | 3,934 | 807 / 34 | overshoot 464 / 1,208 µs |
| s2-04 | focused | park | qos | 115,200 | 0 | 475 | 115 | 7.8 | 997 | 3,479 | 454 / 21 | overshoot 500 / 1,123 µs |
| s2-05 | unfocused | park | nothing | 115,200 | 0 | 751 | 364 | 30.4 | 2,430 | 4,008 | 714 / 37 | overshoot 459 / 1,233 µs |
| s2-06 | focused | park | nothing | 115,200 | 0 | 304 | 73 | 7.9 | 817 | 2,755 | 281 / 23 | overshoot 468 / 1,120 µs |
| s2-07 | unfocused | spin | pinned | 115,091 | 109 | 1,102 | 616 | 80.2 | 3,343 | 33,809 | 0 / 1,102 | stalls 1,491, worst 34.4 ms |
| s2-08 | focused | spin | pinned | 115,185 | 15 | 490 | 104 | 0.3 | 916 | 11,421 | 0 / 490 | stalls 1,029, worst 12.5 ms |
| s2-09 | unfocused | spin | qos | 115,200 | 0 | 62 | 0 | 2.3 | 60.8 | 543 | 0 / 62 | stalls 8, worst 1.8 ms |
| s2-10 | focused | spin | qos | 115,200 | 0 | 17 | 0 | 0.0 | 40.4 | 740 | 0 / 17 | stalls 0, worst 0.8 ms |
| s2-11 | unfocused | spin | nothing | 115,200 | 0 | 138 | 0 | 39.9 | 106 | 876 | 0 / 138 | stalls 8, worst 1.3 ms |
| s2-12 | focused | spin | nothing | 115,200 | 0 | 17 | 0 | 1.2 | 38.3 | 692 | 0 / 17 | stalls 0, worst 0.8 ms |
| x1 | focused | park | nothing | 115,200 | 0 | 38 | 0 | 5.8 | 54.5 | 291 | 18 / 20 | overshoot 415 / 995 µs |
| x2 | unfocused | park | nothing | 115,200 | 0 | 70 | 0 | 7.7 | 65.4 | 821 | 10 / 60 | overshoot 428 / 1,003 µs |
| x3 | unfocused | park | nothing | 115,200 | 0 | 37 | 0 | 8.3 | 53.3 | 790 | 17 / 20 | overshoot 429 / 1,012 µs |
| x4 | focused | park | nothing | 115,200 | 0 | 45 | 0 | 4.8 | 53.4 | 498 | 8 / 37 | overshoot 438 / 974 µs |
| m1 | focused | park | nothing | 115,200 | 0 | 167 | 68 | 0.2 | 510 | 2,832 | 153 / 14 | overshoot 486 / 1,034 µs |
| m1 main | focused | park | the window's thread | 28,800 | 0 | 46 | 17 | 5.6 | 489 | 2,736 | 37 / 9 | overshoot 478 / 1,044 µs |
| m2 | unfocused | park | nothing | 115,200 | 0 | 676 | 258 | 16.1 | 1,515 | 3,553 | 652 / 24 | overshoot 491 / 1,277 µs |
| m2 main | unfocused | park | the window's thread | 28,800 | 0 | 165 | 65 | 13.7 | 1,518 | 3,577 | 164 / 1 | overshoot 492 / 1,278 µs |
| n1 | none | park | pinned | 114,955 | 245 | 4,169 | 836 | 703 | 5,100 | 33,318 | 1,170 / 2,999 | overshoot 490 / 1,638 µs |
| n2 | none | park | qos | 115,200 | 0 | 691 | 293 | 26.9 | 2,317 | 3,557 | 627 / 64 | overshoot 491 / 1,239 µs |
| n3 | none | spin | pinned | 115,111 | 89 | 4,604 | 496 | 460 | 3,251 | 20,667 | 0 / 4,604 | stalls 1,510, worst 21.4 ms |
| n4 | none | spin | qos | 115,200 | 0 | 38 | 0 | 3.2 | 62.3 | 600 | 0 / 38 | stalls 8, worst 1.5 ms |

Differences from this session's probes: no producer thread waking parked
workers, results stored in preallocated arrays, and no power sampler
beside the runs. The system timer period was 10 ms at most run boundaries
and 1 or 6 ms at a few.
