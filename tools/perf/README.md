# Performance harness

Synthetic payload, actor, private-actor, and Fiber benchmarks, plus the
optimization gate. This directory intentionally carries no measurements: current
numbers live in [PERFORMANCE.md](../../PERFORMANCE.md), and the superseded page
is preserved in the [performance archive](../../docs/performance/archive-2026-10-20.md).
Full per-run output stays in local evidence directories. Everything below is a qualitative index of
the harness, not evidence.

## Build

The report driver records an immutable `manifest.json` before measurement:
source revision and working-tree source hashes, executable hashes, host,
configuration and a hash of relevant inherited build/runtime environment
variables (excluding launcher bookkeeping). Resume rejects changed
identities or legacy output without a manifest before overwriting any evidence.
Use a fresh directory after changing sources, binaries, environment or matrix
configuration; `--no-resume` does not bypass this check. Sample counts may be
raised to rerun cells with more samples. Original `metadata.json` is retained.
Fiber configurations are built once into separate executable snapshots under
the output directory and hashed; measurement runs those snapshots directly.

```sh
make -C tools/perf                 # throughput, dual, tail, actor_ready, actor_churn, scaling, offccd, c2c
make -C tools/perf actor_churn_mimalloc   # plus the pinned real mimalloc archive
```

The default toolchain is LDC `-O2 -release`; `DC` and `DFLAGS` override it. The
benchmarks keep their correctness checks (`enforce`) in release builds. Set
`ANTFARM_HUGE_PAGES=0` for ordinary pages unless a run is deliberately testing
the huge-page path. `make -C tools/perf clean` removes the built binaries.

On Windows, first build the Release allocator with
`tests/actors/run-mimalloc-windows.ps1 -Configuration Release` (see
[the allocator recipe](../../tests/actors/README.md)), then run
`./tools/perf/build-windows.ps1` from the repository root. This builds the same
nine harnesses with LDC `-O2 -release` and the static release CRT. The report
driver resolves `.exe` paths on Windows and terminates a timed-out process tree
with `taskkill`; it does not synthesize Linux CPU accounting.
The off-LLC sweep also resolves `.exe` paths for both execution and the binary
hashes in its metadata.

For the multithreaded Fiber benchmark, the report driver uses the full
creation-plus-drain duration. Workers run during creation, so the raw harness's
legacy `run` rate counts the entire burst against only the remaining drain
time and is not valid throughput. Raw readouts remain available for diagnosis;
the report's `total_fibers_s` and `warm_total_fibers_s` use the complete burst.
The harness prints explicit `total` and `total (warm pool)` timings, including
when a residual `run` phase is not measurable. The parser prefers these totals
and accepts older logs only when both numeric phase timings are available.
The single-thread harness's rate-only `total` lines are supported by deriving
auxiliary burst totals from its counted create/run phases; its primary metric
remains drain throughput. Malformed explicit multithread totals remain errors.
Tail summaries record complete-process counts per scene, so a timed-out scene
cannot masquerade as complete merely because the primary metric parsed.
Every cell reports the median and the sample standard deviation of its valid
samples; `--markdown PATH` writes the tables (median ± stddev) that
PERFORMANCE.md uses. Generated tables omit the multithreaded residual drain
rate; its diagnostic fields remain in the raw results. Fiber counts name
pinned workers with a separate control thread, rather than total threads.

The Fiber tools under `tools/perf/ant_fibers` are the `ant_fibers` dub
configurations `benchmark_mt`, `benchmark_vs_druntime`, `benchmark_farm_embed`
and `benchmark_actor_wave`. Each multithreaded scene runs a `FiberGroups`
attachment on a `GroupHost`, which owns the Farm(s) and the pool and whose own
view consumes them. `benchmark_mt [tasks] [private|shared] [workers]` spans
every accepted LLC group (`HostGroups.all`): `private` is `farmPerGroup`, one
Farm and lane per group consumed only by its workers; `shared` is one Farm
consumed by every group's workers. Its former `assist` mode (lane assist
edges) has no GroupHost equivalent and was dropped; its report line prints
`farms=` where it printed `edges=`. The other three bind one Farm to the group
of the first eligible CPU (`HostGroups.only`) with exactly the scene's CPUs as
workers; `actor_wave` keeps its workers spinning through
`GroupHostOptions.idle`, the others park until woken as the raw pool did. The
burst benchmarks size the domain's lifecycle record ring to the burst
(`recordRetention = 4 * tasks`), since the default admits 4096 unretired
tasks. Timing regions and warm-up semantics are unchanged.

The `headline` section sweeps four workloads over `--threads` (default
`2,4,8,16,20,24,32,40`): payload throughput with about one small producer per
two consumers, unpinned (`payload_split`); private root lifecycles
(`private-slab`) and public actor lifecycles (`actor`) with a controller plus
`threads - 1` pinned consumers; and the Fiber burst with that many pinned
workers. Pins follow placement order, one last-level-cache group at a time,
its cores before its SMT siblings (`cpu_order`, from sysfs); counts beyond the
CPU count run unpinned, and Fiber cells beyond it are skipped because the
pool pins. Without sysfs, including Windows, the actor driver falls back to
numeric CPU order. The tail and Fiber executables use native topology instead;
record the actual placement rather than assuming the same core order on every
platform. The `tail` section sweeps `--tail-threads` (default `2,6,12,20,32`)
with `threads - 1` consumers plus the producer. Major performance comparisons
run on the Ryzen 9 5900XT, as described in
[CONTRIBUTING.md](../../CONTRIBUTING.md#performance-make-perf-gate).

For the comprehensive array with five samples per cell, pass `--runs 5
--six-consumer-runs 5`; a larger `--six-consumer-runs` is a per-cell minimum
for six-consumer actor, throughput and tail cases, including when resuming.
The report driver records every attempt and rejects nonzero exits or missing
metrics; it does not reject samples based on background CPU activity. Six
dual-role threads are not six dedicated consumers and retain `--runs`.

To extend the report to larger thread counts without rerunning the default
matrix:

```powershell
python tools/perf/run_performance_md.py --sections dual,private --runs 5 --dual-threads 12,18,20 --private-consumers 11,17,19 --out .build/performance-high-threads
```

Dual-role counts are total worker threads. Private actor counts exclude the
controller, so the example measures 12, 18, and 20 total threads in both
sections. Private workers are pinned to LPs 1 through N and the controller
to LP 0; choose counts whose CPU lists exist and are allowed on the host.
Omitting these options keeps the original matrix. The six-thread dual tuning
cells are included only when `--dual-threads` includes 6.

## Payload benchmarks

- **`throughput`** — synthetic payload publish/consume sweep over a 16 MiB
  Farm. The default mode ranks a payload/table shape search; `--once` runs one
  configuration and `--grid N` sweeps small-producer counts. `--constant-header`
  and `--constant-callandlength` select the common-callback and explicit-length write
  forms; `--full-header-size` sizes the quota-fit check with the pre-compact
  16-word header for comparisons against a runtime before SPEC 2f. Remaining
  shape options (`--ln`, `--k`, `--ns`, `--nc`, `--nb`, `--qs`, `--qb`, `--ac`,
  `--body`, `--batch`, `--small`, `--huge`, `--global-count`, `--sweep`,
  `--repeats`) are documented in the file header.
- **`dual`** — dual-role threads that register a producer and subscribe a
  consumer, interleaving small writes with `consumeNext`. `--grid N` sweeps the
  thread count; `--tier` and `--consume` shape the workload; the header and
  full-size options are the same as `throughput`.
- **`tail`** (with `placement`) — tail-latency harness on the repository's
  threadpool. Consumers are the library's own loop: pinned threads calling
  `consumeNext`; there is no out-of-band signalling path. Scenes are idle,
  mid-drain (table lengths 32 to 8192, callback work 0 to 10 µs), burst and
  near-full. It prints every worker/processor assignment and the placement
  options (`--no-pin`, `--spin`, `--samples`, `--idle-only`, `--mid-only`,
  `--tlen`, `--nc`, `--ac`, `--repeats`, `--small`, `--ln`, `--huge`). A drain
  run discards a missed backlog window rather than reporting idle samples as
  latency. Placement fills one physical group at a time, cores before SMT
  siblings (`placement.d`).

  The report driver's `tail` section runs the default scene schedule at each
  `--tail-threads` count with 10,000 observations per scene and the default
  `avgCost=3` (chunk 16); `--tail-ac N` sets another class. The raw header reports the effective cost, chunk and
  repetition count. `make -C tools/perf check-tail` runs its unit tests,
  including bounded numeric argument parsing; add `DC=dmd` for DMD. The
  mid-drain scene drains and retries a quota-truncated dump that cannot reach
  its backlog trigger.
  Use `check-benchmarks` to include the throughput and dual argument-parsing
  tests as well; their C numeric conversions also respect D string boundaries.

- **`subscribe_cost`** — nanoseconds per subscribe, three `consumeNext`
  calls and unsubscribe on a 1 MiB, K = 8 Farm, once idle and once beside
  two producers and two steady consumers (the T29 geometry); 200,000 cycles
  after 2,000 warm-ups, `RETRY` counted. It times the path the 2.1.1 `Live`
  lifecycle rewrote; the comparison that introduced it is
  [docs/performance/live-lifecycle-ab-2026-10-28.md](../../docs/performance/live-lifecycle-ab-2026-10-28.md).
- **`llc_split.py`** — the headline payload and private child cells laid out
  by the [several-groups rules](../../docs/guide/scaling-groups.md#several-physical-groups).
  Each thread count runs twice in the same session: as the headline command,
  and with every Farm confined to one L3 group, where a count larger than one
  group becomes one independent process per group (shares split evenly,
  started together, confined with `taskset`, actor pins inside the group).
  A per-group payload sample is the total payloads over the longest process
  time, a lower bound; a private sample sums the processes' fixed-window
  rates. Rounds are interleaved over cells. It writes `summary.json`, raw
  output and `tables.md`; Linux only.

- **`offccd`**, **`c2c`** and **`offccd_sweep.py`** — off-CCD scaling for one
  Farm. `offccd` pins a home group (default two producers and five consumers)
  on one L3 group and adds off-CCD consumers or producers on another, in
  streaming or phased (publish-then-consume) mode, and prints one JSON object
  per run: per-thread pin outcome and observed CPU, per-role counters, rates,
  shares and, phased, a claim scan of each round's tables. `c2c` calibrates
  core-to-core ping-pong, contended RMW and one-shot pull. Both place roles on
  eligible primaries per L3 group from the threadpool snapshot
  (`placement.planRoles`); a request that does not fit, a single eligible L3
  group, or a non-L3 grouping (a fallback shard is not a CCD) is reported as
  skipped with a reason, and a refused or moved pin makes the run invalid.
  `offccd_sweep.py` runs the suites (`calib`, `main-c`, `main-p`, `combined`,
  `ctl-*`, `smt`, `layouts`, `perf`, `lat`, `smoke`) in seeded interleaved
  rounds with top-ups for noisy cells and writes an evidence directory
  (metadata, raw runs, summary). Its numbers belong in the evidence store, not
  here. `make -C tools/perf offccd-plan` prints the plan; `offccd-smoke` runs the
  validation suite.

Single-threaded tables use the uniform (2g) layout, or the compact 2f
`[Call][Pbody]` record when the prefix has several callbacks; payload
benchmarks size their quota-fit check accordingly unless `--full-header-size`
is passed.

Keep the write forms separate when checking writer performance.
`--constant-header` exercises `write(callback, bodies)`, which scans body
lengths to infer the table layout. `--constant-callandlength` supplies the length
and uses arithmetic sizing; a good result there does not cover the inferred
scan. The default payload-entry form also detects callback identity, while
`--mixed-calls` forces compact tables. Use the same body, batch and worker
configuration when comparing these forms.

## Actor benchmarks

### C ABI lifecycle comparison

`python3 tools/perf/run_actor_cabi.py --output .build/actor-cabi-measurements`
builds the runtime-free library and a matched native C/D driver, then measures
D erased scalar creation, C scalar creation and C batch creation. States are
64 and 4096 bytes; both paths use the same copy sources, 64-byte alignment,
allocator, cohort capacity and native threads. It covers one/four creators,
local/remote reclamation and CRT/mimalloc. The pinned mimalloc archive must
already exist at `tests/actors/.build/mimalloc-v3.5.3/libmimalloc.a`.

Separate counted runs require exactly one final-state allocation/free and
`state_bytes` allocated bytes per actor; timed runs disable those counters.
Copy counts describe the one explicit final-storage copy in each implementation.
Setup allocations and thread startup before the start barrier are outside the
measurement; barrier release and final joins are inside. Remote runs include
the cohort handoff and waiting overhead. This is descriptive lifecycle timing,
not wave/callback throughput or the regression gate. Output records raw samples,
medians, ranges, toolchain and affinity. No background-load rejection is applied.

### `actor_churn`

```text
actor_churn <mode> [actors=4096] [seconds=3] [consumers=0] [batch=256]
  [warmups=5] [crt|mimalloc|mimalloc64|mimalloc-count]
  [controller-cpu,consumer-cpus|-] [small|bulk] [quota-words=16384]
  [subset-percent=100] [root-hint=auto|0..7]
```

Each cycle runs one complete cohort lifetime and checks zero live, ready, or
stale actors. Modes:

| Mode | What it measures |
| --- | --- |
| `actor` | public cohort create / wake / execute / await / retire / reclaim |
| `inbox` | the same with one caller-owned node per actor instead of a wake |
| `wave` | the same through one sealed public `ActorWave` per cohort |
| `private` | private root dispatch, terminal dispatch, and emitter-side free |
| `private-remote` | the same, but root state is freed on consumers (allocation-locality control) |
| `private-slab`, `private-remote-slab` | the two above with `PrivateStateStorage.cohortSlab` root state: one slab reserved at park create, slots cleared instead of freed (per-object, printing `storage=perObject`, on runtimes without root slabs) |
| `private-tracked` | tracked roots with per-root records, handles, and outcomes |
| `tree`, `tree-wait` | private parents creating child cohorts; `tree-wait` uses `waitChildren` |
| `tree-cont` | child continuations |
| `tree-slab`, `tree-wait-slab`, `tree-cont-slab` | the tree modes with `PrivateStateStorage.cohortSlab` |
| `tree-refill`, `tree-refill-slab` | one cohort refilled eight times (`spawn` → `startFinal` → `waitChildren`) |
| `tree-pipelined` | root → middle → leaf result fold |
| `tree-cancel` | cancellation workload; the dispatched fraction is part of the result |
| `dtor` | non-copyable private state whose destructor publishes its completion |
| `subset`, `subset-whole`, `subset-whole-unreserved` | subset waves at `subset-percent` (1, 10, or 100) |

`mimalloc-count` is a diagnostic allocator that is never used for timing: it
prefixes each allocation with its allocating thread and reports cross-thread
free counts at exit. `root-hint` is `auto` (the default, derived per root
table) or a fixed Farm work-cost class `0..7` (chunk `128 >> class`); see
[the work-cost-hint rule](../../docs/guide/sizing-and-allocation.md#work-cost-hints).
A revision from before `MAX_CHUNK` rose to 128 has classes `0..5` of chunk
`32 >> class`, so a fixed `root-hint` names a chunk four times larger on the
newer revision: compare a base at class `k` with a candidate at class `k + 2`.

The public `actor`, `inbox`, and `wave` modes call `ActorRuntime.flush` and
`ActorWave.publish`, which default to chunk 8. `ANTFARM_PUBLIC_HINT` pins a
fixed work-cost class `0..7` (chunk `128 >> class`) instead; unset keeps the
runtime default. It is an option screen for the public-actor chunk, not a
timing knob. The committed tables report both the default and chunk 32 (class
2 at `MAX_CHUNK` 128). `ANTFARM_CHILD_HINT` does the same for the private
child-wave tables (`start`/`startFinal`/`startSubset`), which default to chunk
32; set it to `0` for chunk 128.

`ANTFARM_CHURN_HOST=1` swaps the benchmark's hand-written consumer loop for a
`FarmHost` that owns the Farm, the control token and the pinned consumers. It
needs the small tier, `consumers > 0` and a CPU list naming the controller
followed by one CPU per consumer (`cpus.length == consumers + 1`); any other
combination is rejected. The host runs `WorkerIdle.spin` and uses the
benchmark's `k = 8`, `ringMiB = 8` and `quota-words`. Unset, the benchmark
drives its own consumers. The host path is the comparison side of
[the tuning guidance](../../docs/guide/scaling-groups.md#performance-and-tuning).

### `actor_ready`

```text
actor_ready <actors=4096> [rounds=200] [batch=32]
```

Times autonomous actor dispatch from a prequeued burst, with actor
construction, verification, and reclamation outside the timed region. The
flush uses the runtime default chunk (8); `ANTFARM_PUBLIC_HINT` pins a fixed
work-cost class as in `actor_churn`.

### `scaling`

```text
scaling create <actors>
scaling lifecycle <fibers> <batch>
```

`create` times actor slot reservation and state allocation for an initially
empty runtime. `lifecycle` times retiring admitted Fiber lifecycle records
`batch` at a time through a counting `onRecord` handler.

## Performance report

`perf_gate.py` is the committed-ref performance report. It builds a base and a
candidate in temporary detached worktrees, times matched pairs on identical
benchmark source, and reports each cell's paired change with its spread. It is
a report for the reader, not a verdict: the reader decides, and records the
result in the change's notes.

```sh
make perf-gate BASE=BASE_SHA CANDIDATE=CANDIDATE_SHA
make perf-gate BASE=BASE_SHA CANDIDATE=CANDIDATE_SHA PERF_GATE_ARGS='--boundary --modes tree --consumers 0'
```

It needs Linux with LDC, Python 3, and the pinned real mimalloc archive, and
the output directory must not already exist. `PERF_GATE_OUTPUT` defaults to
`/tmp/antfarm-perf-gate`; remove it or pass another `--output` before
rerunning. The current measurements and how to regenerate them are in
[PERFORMANCE.md](../../PERFORMANCE.md#method). Run timing serially, with no
other heavy work on the host; nothing else about the host is checked.

Either ref may predate the 1.8.2 layout (`antfarm.d`, `actors/` and
`perftest/actor_churn.d` at the root). The report detects each checkout's layout
and builds it with that layout's paths. It maps the benchmark's imports between
layouts (`actors` and `antfarm.actors`, and so on), so one benchmark source
still counts as identical on both sides; pass `--benchmark-ref` to compile one
revision's benchmark against both runtimes when the benchmark changed.
`metadata.json` records each side's layout and any renames.
`PERF_GATE_ARGS=--build-only` builds both sides, writes the metadata and build
logs, and stops before measuring.

The method:

- **Five pairs per cell, then twenty on flagged cells.** The default screen
  times five matched base/candidate pairs per cell, alternating order. Each
  cell reports the paired median, its descriptive bootstrap interval, the
  paired minimum, maximum, span and standard deviation, and a classification
  against `--threshold` (default −3%): `pass`, `regression` or `inconclusive`.
  A cell is **flagged** when it is not a pass, when its paired ratios span
  more than twice the threshold, or when its paired median is below half the
  threshold. `gate.json` lists the flagged cells; rerun each with `--boundary`
  (20 pairs) and its `--modes`, `--consumers` and `--subset-percent` to
  characterize its distribution. Report flagged cells with their spread.
- **Exit status is about completeness.** 0 means every requested cell has
  its pairs; 2 means the report is incomplete or failed: a mode missing from
  one side, too few accepted pairs, invalid benchmark output, or an error.
  No exit status means "regression"; the classification is in the summary.
- **Background CPU is telemetry.** Every sample records load averages, the
  busy fraction of the CPUs outside the benchmark's pinned set, and the
  system CPU time beyond the benchmark's own. None of that excludes a sample:
  paired, interleaved samples absorb background that touches both sides, and
  it sits below the layout noise. A sample is excluded only when another task
  used the benchmark's pinned CPUs (external busy time above 5% of the
  pinned wall time) or the run timed out. Excluded samples stay in
  `runs.jsonl`; attempts are capped at twice the pair count, after which the
  cell is `insufficient`.
- **Layout moves 0-consumer cells.** Identical binaries have measured
  ±1–2% at six consumers and ±5–12% on the serial subset cells, and a
  candidate that differs only in unused code size can move every function
  under LDC's whole-program layout. Treat a serial cell's swing of that size
  as layout unless a six-consumer cell or an instruction count moves with it.
  `--control` (build the base twice, require byte-identical binaries) measures
  the host's own spread when that is in doubt.
- `tree-cancel` additionally needs valid dispatched fractions with no median
  change beyond `--cancel-fraction-tolerance`; otherwise the cell is
  `inconclusive_work` and flagged, since the two sides did different work.
- Each run records CPU/load, placement, source/binary/archive hashes and the
  build commands in `metadata.json`, `summary.json`, `gate.json` and
  `runs.jsonl`. Keep them locally or archive them; do not commit them.

Decision and option tests run without builds or timing, including on Windows:

```sh
python3 -B -m unittest tools/perf/test_perf_gate.py
```

The real timing run still requires Linux and rejects other hosts before
creating output or starting builds. Only its Linux error-accounting integration
probe is skipped on Windows; the mocked decisions and options remain covered.
See [portable Python checks](../../docs/development/testing.md#portable-python-checks)
for the combined documentation, parser, topology-fixture and grouping suite.

## Sweep driver

`private_lifecycle_matrix.py` runs pinned lifecycle, root-batch/quota/consumer,
and child-limit sweeps over an `actor_churn_mimalloc` binary:

```sh
python3 tools/perf/private_lifecycle_matrix.py --binary tools/perf/actor_churn_mimalloc \
  --suite batch --output /tmp/private-matrix
```

`--suite` is `lifecycle`, `batch`, or `child`; `--seconds`, `--repeats`, and
`--mimalloc-version` shape the run. The output directory is written in place
and contains `metadata.json`, `runs.jsonl`, and `summary.json`. Treat it as
local evidence; nothing under it is checked in.

## Parallel-loop M1

Build the focused range benchmark with `make -C tools/perf parallel`. On
Windows, from the repository root:

```text
ldc2 -O2 -release -I. tools/perf/parallel.d antfarm/parallel.d antfarm/templates.d antfarm/package.d antfarm/allocation.d -of=.build/parallel_bench.exe
```

Run one cell at a time on an otherwise idle host:

```text
tools/perf/parallel --grain=64 --n=100000 --cost=16 --workers=8 --rounds=20
tools/perf/parallel --grain=1 --n=100000 --cost=16 --workers=8 --rounds=20
tools/perf/parallel --grain=64 --n=100000 --cost=16 --workers=8 --embedded=4096
```

The grid axes are grain (1, 8, 64, 256), element count, item cost,
workers, cost class and embedded element count. Grain applies to both ranges;
each grain is one single-shot payload. Workers includes the calling thread.

Use at least 1, 2, 4, 8 and the available larger worker counts, N of 1K,
100K and 1M, and costs 0, 16 and 256. Compare `--embedded=0` with a fixed
additional batch (for example 4096). The batch uses the same item function
and its own range, concatenated into the caller's write. Record OS, compiler,
CPU placement and repeated-run spread alongside the CSV. This harness does
not pin workers, compare OpenMP, or establish the committed-ref perf gate.

CSV reports mean ns and reserved ring words per round after one warm-up.
Every round verifies all outputs. A publication generation plus an idle
acknowledgment from every consumer joins the raw ranges without adding an
atomic per item; this barrier is included in timing. Ring words include
physical tables, shard counters and padding. The harness measures raw range
composition, not the allocation/hook cost of the asynchronous convenience API.

Compare grain sizes at several work costs and worker counts; larger grains
reduce scheduling overhead but offer fewer independently scheduled units. See
[parallel-loop shapes and joins](../../docs/guide/parallel-loops.md).
