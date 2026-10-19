# Ant Farm documentation

This is the map of the repository's documentation and the canonical owner of
each rule. Each rule is stated in full in exactly one place; other pages
summarize it and link. Update the owner first, then the pages that link to it.

New here? Keep [Using Ant Farm well](guide/using-it-well.md) open while you
read: it collects the correctness and performance rules on one page and links
each to its owner.

## Run something

Start with First payload, then follow these steps. Each step pairs a program
with its owning page; leave the path as soon as the layer fits your workload.

1. **See it work (5 min).** Run [First payload](guide/quickstart.md#first-payload)
   on one thread and read the [quickstart model](guide/quickstart.md#what-just-happened).
   Learn the token, published count, cursor, and application completion counter.
2. **Add threads (10 min).** Run [iota_sum.d](../examples/iota_sum.d) with
   `-Ithreadpool/source` using its [command](guide/quickstart.md#first-payload),
   then read [Start workers](../threadpool/README.md#start-workers).
   Leave here for short `@nogc` fan-out.
3. **Host plumbing (10 min).** Run [public_service.d](../examples/public_service.d)
   with `2` workers using the [quickstart](guide/quickstart.md#run-the-public-service),
   then read [FarmHost setup](guide/hosting.md#setup). Import `threadpool.host`.
4. **Pick a representation (15 min).** Use the table in
   [choosing-work.md](guide/choosing-work.md) to choose the next program.
5. **Use that layer.** Public identities: [public_service.d](../examples/public_service.d)
   and [public_wave.d](../examples/public_wave.d), then [public actors](guide/public-actors.md)
   and the [hazards table](reference/actor-lifecycle.md#ranked-misuse-hazards).
   Private trees: [private_tree.d](../examples/private_tree.d) and
   [request_deadline.d](../examples/request_deadline.d), then
   [private actors](guide/private-actors.md) and [outcomes](guide/outcomes-and-cancellation.md).
   Fibers: [hello_fibers.d](../ant_fibers/examples/hello_fibers.d), then
   [Fibers through handles, joins, and cancellation](../ant_fibers/README.md#handles-joins-and-cancellation).
   Leave here if the chosen layer supplies what you need.
6. **Size it.** Read [Sizing one Farm](../ARCHITECTURE.md#sizing-one-farm) and
   [work-cost hints](guide/sizing-and-allocation.md#work-cost-hints).
   When ready to measure, run the default [throughput shape](../tools/perf/README.md#payload-benchmarks)
   once. The [usage rules](guide/using-it-well.md) collect the checks to remember.
7. **Trust it.** Run `make check-quick`, then read
   [testing](development/testing.md) for what it covers and the deferred lanes.

For contribution, start with [CONTRIBUTING.md](../CONTRIBUTING.md), then
[ARCHITECTURE.md](../ARCHITECTURE.md) and [SPEC.md](../SPEC.md).

## Guides

| Page | Read it to |
| --- | --- |
| [quickstart.md](guide/quickstart.md) | run a public service and the single-threaded First payload |
| [using-it-well.md](guide/using-it-well.md) | check correctness and tuning rules before building on the Farm |
| [hosting.md](guide/hosting.md) | run actors on a `FarmHost`: setup, placement, waiting, teardown |
| [choosing-work.md](guide/choosing-work.md) | pick payload, public actor, actor wave, private hierarchy, or Fiber |
| [parallel-loops.md](guide/parallel-loops.md) | compose ranges, choose grains, and join parallel regions |
| [public-actors.md](guide/public-actors.md) | create, address, wake, retire and reclaim public actors and waves |
| [private-actors.md](guide/private-actors.md) | own a private root tree: roots, cohorts, dispatch, retirement |
| [private-results-and-waits.md](guide/private-results-and-waits.md) | result ports, subsets, continuations, `waitChildren`, folds |
| [outcomes-and-cancellation.md](guide/outcomes-and-cancellation.md) | domain failures, tracked roots, deadlines, cooperative cancellation |
| [sizing-and-allocation.md](guide/sizing-and-allocation.md) | choose quotas, batches, work-cost hints, allocators, and slabs |

## Reference and design

| Page | Read it to |
| --- | --- |
| [reference/README.md](reference/README.md) | find the source symbol or generated reference for an API entry |
| [reference/actor-lifecycle.md](reference/actor-lifecycle.md) | get the shared authority, completion, storage, drain and shutdown contracts |
| [design/public-actor-ordering.md](design/public-actor-ordering.md) | see why public activation and wave completion are ordered the way they are |
| [design/private-actor-ordering.md](design/private-actor-ordering.md) | see the private join, outcome and cancellation proof chains |
| [design/private-actor-layout.md](design/private-actor-layout.md) | see the Entry/Service/cohort layout invariants and slab reuse proof |
| [design/farm-ordering.md](design/farm-ordering.md) | see the Farm subscription/reclamation ordering audit and its TSan lanes |
| [design/c-abi.md](design/c-abi.md) | review the proposed ST-only C API, runtime-free core and runtime-inclusive worker pool/host boundary |
| [design/threadpool.md](design/threadpool.md) | get the topology, worker, Director, registry and OS-backend contracts |
| [design/ant_fibers.md](design/ant_fibers.md) | get the Fiber scheduler model and synchronization invariants |

## Development

| Page | Read it to |
| --- | --- |
| [development/testing.md](development/testing.md) | run and understand the local correctness lanes and their limits |
| [development/agent-workflow.md](development/agent-workflow.md) | follow the CyDo agent process for iteration, hardening, TSan budgets and mutation selection |
| [../tools/perf/README.md](../tools/perf/README.md) | run a benchmark and the committed-ref optimization gate |
| [../PERFORMANCE.md](../PERFORMANCE.md) | find the measured numbers, method, and tuning guide |

## Canonical ownership

| Topic | Owner |
| --- | --- |
| Farm algorithm, table layout, compact ST tables, callback header, memory-order contract | [SPEC.md](../SPEC.md) |
| Layers, sizing formulas and entries per quota | [ARCHITECTURE.md](../ARCHITECTURE.md) |
| Work-representation and public/private actor choice | [guide/choosing-work.md](guide/choosing-work.md) |
| Parallel loop API, range ownership, completion proof and blocking restrictions | [guide/parallel-loops.md](guide/parallel-loops.md) |
| First payload and the initial producer/consumer/actor model | [guide/quickstart.md](guide/quickstart.md) |
| Runnable actor programs and their commands | [examples/README.md](../examples/README.md) |
| FarmHost setup, placement, idle and wake, control-thread rule, close and abandon | [guide/hosting.md](guide/hosting.md) |
| Actor API topic index and the generated reference command | [reference/README.md](reference/README.md) |
| Actor authority, completion, storage, drain and shutdown contracts shared by both models | [reference/actor-lifecycle.md](reference/actor-lifecycle.md) |
| Public actor creation, inbox, retirement and wave API | [guide/public-actors.md](guide/public-actors.md) |
| Private roots, cohorts, dispatch and retirement API | [guide/private-actors.md](guide/private-actors.md) |
| Private result ports, subsets, continuations and folds | [guide/private-results-and-waits.md](guide/private-results-and-waits.md) |
| Domain failure, outcomes, deadlines and cancellation | [guide/outcomes-and-cancellation.md](guide/outcomes-and-cancellation.md) |
| Farm and actor sizing decisions, tiers, batches, work-cost hints, allocators and slabs | [guide/sizing-and-allocation.md](guide/sizing-and-allocation.md) |
| Public actor and wave memory-order proofs | [design/public-actor-ordering.md](design/public-actor-ordering.md) |
| Private join, outcome and cancellation proofs | [design/private-actor-ordering.md](design/private-actor-ordering.md) |
| Private Entry/Service/cohort layout and slab reuse proof | [design/private-actor-layout.md](design/private-actor-layout.md) |
| The local correctness gate and its lanes | [development/testing.md](development/testing.md) |
| CyDo agent session routing, iteration, hardening, TSan budgets and mutation selection | [development/agent-workflow.md](development/agent-workflow.md) |
| Farm ordering audit and real-atomic TSan lanes | [design/farm-ordering.md](design/farm-ordering.md) |
| Proposed C ABI scope, compatibility and runtime separation | [design/c-abi.md](design/c-abi.md) |
| Threadpool topology, worker and Director contracts | [design/threadpool.md](design/threadpool.md) |
| Fiber scheduler model and invariants | [design/ant_fibers.md](design/ant_fibers.md) |
| Test suites (`tests/farm`, `tests/actors`, `tests/private`, `tests/threadpool`, `tests/ant_fibers`) | each suite's README and [development/testing.md](development/testing.md) |
| Benchmark harness and optimization gate | [tools/perf/README.md](../tools/perf/README.md) |
| What changed in each candidate and its compatibility notes | [CHANGELOG.md](../CHANGELOG.md) (current and latest release), [releases/](releases/) (older) |
| Remaining and deferred work | [ROADMAP.md](../ROADMAP.md) |
| Threadpool (with FarmHost) and Fibers package entries | [threadpool/README.md](../threadpool/README.md), [ant_fibers/README.md](../ant_fibers/README.md) |
