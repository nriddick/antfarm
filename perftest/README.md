# Performance harness

Synthetic payload, actor, private-actor, and Fiber benchmarks, plus the
optimization gate. This directory intentionally carries no measurements: the
historical reports and the per-run result trees were archived outside the
repository, and fresh numbers for the release candidate will be recorded in
[PERFORMANCE.md](../PERFORMANCE.md). Everything below is a qualitative index of
the harness, not evidence.

## Build

```sh
make -C perftest                 # throughput, dual, tail, actor_ready, actor_churn, scaling
make -C perftest actor_churn_mimalloc   # plus the pinned real mimalloc archive
```

The default toolchain is LDC `-O2 -release`; `DC` and `DFLAGS` override it. The
benchmarks keep their correctness checks (`enforce`) in release builds. Set
`ANTFARM_HUGE_PAGES=0` for ordinary pages unless a run is deliberately testing
the huge-page path. `make -C perftest clean` removes the built binaries.

## Payload benchmarks

- **`throughput`** — synthetic payload publish/consume sweep over a 16 MiB
  Farm. The default mode ranks a payload/table shape search; `--once` runs one
  configuration and `--grid N` sweeps small-producer counts. `--constant-header`
  and `--constant-callandlength` select the common-header and fixed-width write
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
  threadpool. It prints every worker/processor assignment and the placement
  options (`--no-pin`, `--mailbox`, `--spin`, `--samples`, `--idle-only`,
  `--mid-only`, `--tlen`, `--nc`, `--ac`, `--attr`, `--repeats`, `--small`,
  `--ln`, `--huge`). A drain run discards a missed backlog window rather than
  reporting idle samples as latency.

All-single-threaded tables use the compact `[Call][Pbody]` record; payload
benchmarks size their quota-fit check accordingly unless `--full-header-size`
is passed.

## Actor benchmarks

### `actor_churn`

```text
actor_churn <mode> [actors=4096] [seconds=3] [consumers=0] [batch=256]
  [warmups=5] [crt|mimalloc|mimalloc64|mimalloc-count]
  [controller-cpu,consumer-cpus|-] [small|bulk] [quota-words=16384]
  [subset-percent=100] [root-hint=auto|0..5]
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
table) or a fixed Farm work-cost class `0..5`; see
[the work-cost-hint rule](../docs/guide/sizing-and-allocation.md#work-cost-hints).

### `actor_ready`

```text
actor_ready <actors=4096> [rounds=200] [batch=32]
```

Times autonomous actor dispatch from a prequeued burst, with actor
construction, verification, and reclamation outside the timed region.

### `scaling`

```text
scaling create <actors>
scaling lifecycle <fibers> <batch> [handle|take]
```

`create` times actor slot reservation and state allocation for an initially
empty runtime. `lifecycle` times draining admitted Fiber lifecycle records;
`handle` invokes a counting delegate and `take` also allocates the returned
arrays.

## Optimization gate

`perf_gate.py` is the standard committed-ref gate for optimization changes. It
builds a base and a candidate in temporary detached worktrees, times clean
matched pairs on identical benchmark source, and decides whether the candidate
regresses beyond the project boundary.

```sh
make perf-gate BASE=BASE_SHA CANDIDATE=CANDIDATE_SHA
make perf-gate BASE=CANDIDATE_SHA PERF_GATE_ARGS=--control
```

It needs a quiet Linux host with LDC, Python 3, and the pinned real mimalloc
archive, and the output directory must not already exist. `PERF_GATE_OUTPUT` defaults to the fixed path `/tmp/antfarm-perf-gate`, which
must not already exist; remove it (or pass another `--output`/`PERF_GATE_OUTPUT`)
before rerunning. `PERF_GATE_ARGS` passes
options such as `--archive`, `--boundary`, and `--root-hint`. Run the
correctness gate first and run timing serially, with no other heavy work on the
host.

Decision rules:

- Exit status is **0 pass, 1 regression, 2 inconclusive/error**. Inconclusive
  is never a pass.
- The regression boundary is **−3%** on the paired-median decision. Decisions
  use clean paired ratios and a descriptive bootstrap median interval; legacy
  mean/delta fields include contaminated attempts and cannot establish a pass.
- Near the boundary, collect at least **20 process pairs** (`--boundary`) and a
  **byte-identical control** (`--control`, which builds the base twice and
  requires identical binary hashes). Individual pairs may fall below −3% while
  the cell still passes if the paired-median interval's lower bound is above
  −3%.
- `tree-cancel` additionally requires valid dispatched fractions and no median
  fraction change beyond the default tolerance; unequal cancellation work is
  inconclusive.
- Each run records CPU/load, placement, source/binary/archive hashes, and the
  build commands; attach `gate.json`, `summary.json`, `metadata.json`, the build
  logs, and `runs.jsonl` to the change or PR, or archive them. Do not commit them
  to the repository.

### Code-layout sensitivity at 0 consumers

A candidate that differs only in unused code size can move every function under
LDC's size-driven whole-program layout and measurably change single-threaded
(0-consumer) cells. When a near-boundary 0-consumer cell executes byte-identical
semantics in both binaries, an inert-code control can attribute the swing to
layout. Such a waiver is one-off and must record the ≥20-pair run, the
byte-identical base/base control, and the inert-code control together; it is
not a general precedent and does not relax the boundary.

Gate decision and option tests run without builds or timing:

```sh
python3 -B -m unittest perftest/test_perf_gate.py
```

## Sweep driver

`private_lifecycle_matrix.py` runs pinned lifecycle, root-batch/quota/consumer,
and child-limit sweeps over an `actor_churn_mimalloc` binary:

```sh
python3 perftest/private_lifecycle_matrix.py --binary perftest/actor_churn_mimalloc \
  --suite batch --output /tmp/private-matrix
```

`--suite` is `lifecycle`, `batch`, or `child`; `--seconds`, `--repeats`, and
`--mimalloc-version` shape the run. The output directory is written in place
and contains `metadata.json`, `runs.jsonl`, and `summary.json`. Treat it as
local evidence; nothing under it is checked in.
