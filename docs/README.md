# Ant Farm documentation

This is the map of the repository's documentation and the canonical owner of
each rule. Each rule is stated in full in exactly one place; other pages
summarize it and link. Update the owner first, then the pages that link to it.

Three reading paths:

- **Run something.** [README quickstart](../README.md#quickstart-linux) →
  [docs/guide/quickstart.md](guide/quickstart.md) →
  [examples/README.md](../examples/README.md).
- **Choose and use actors.**
  [docs/guide/choosing-work.md](guide/choosing-work.md) →
  [docs/guide/public-actors.md](guide/public-actors.md) or
  [docs/guide/private-actors.md](guide/private-actors.md) →
  [docs/reference/actor-lifecycle.md](reference/actor-lifecycle.md) → the
  matching example.
- **Contribute.** [CONTRIBUTING.md](../CONTRIBUTING.md) →
  [docs/development/testing.md](development/testing.md) →
  [ARCHITECTURE.md](../ARCHITECTURE.md) and [SPEC.md](../SPEC.md).

## Guides

| Page | Read it to |
| --- | --- |
| [quickstart.md](guide/quickstart.md) | build and run a tracked program in a few minutes |
| [choosing-work.md](guide/choosing-work.md) | pick payload, public actor, actor wave, private hierarchy, or Fiber |
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

## Development

| Page | Read it to |
| --- | --- |
| [development/testing.md](development/testing.md) | run and understand the local correctness lanes and their limits |
| [../perftest/README.md](../perftest/README.md) | run a benchmark and the committed-ref optimization gate |
| [../PERFORMANCE.md](../PERFORMANCE.md) | find the performance numbers (measured at the release candidate) |

## Canonical ownership

| Topic | Owner |
| --- | --- |
| Farm algorithm, table layout, compact ST tables, callback header, memory-order contract | [SPEC.md](../SPEC.md) |
| Layers, sizing one Farm, entries per quota, shutdown order | [ARCHITECTURE.md](../ARCHITECTURE.md) |
| Work-representation and public/private actor choice | [guide/choosing-work.md](guide/choosing-work.md) |
| Runnable programs and their commands | [examples/README.md](../examples/README.md) |
| Actor API topic index and the generated reference command | [reference/README.md](reference/README.md) |
| Actor authority, completion, storage, drain and shutdown contracts shared by both models | [reference/actor-lifecycle.md](reference/actor-lifecycle.md) |
| Public actor creation, inbox, retirement and wave API | [guide/public-actors.md](guide/public-actors.md) |
| Private roots, cohorts, dispatch and retirement API | [guide/private-actors.md](guide/private-actors.md) |
| Private result ports, subsets, continuations and folds | [guide/private-results-and-waits.md](guide/private-results-and-waits.md) |
| Domain failure, outcomes, deadlines and cancellation | [guide/outcomes-and-cancellation.md](guide/outcomes-and-cancellation.md) |
| Actor quotas, batches, work-cost hints, allocators and slabs | [guide/sizing-and-allocation.md](guide/sizing-and-allocation.md) |
| Public actor and wave memory-order proofs | [design/public-actor-ordering.md](design/public-actor-ordering.md) |
| Private join, outcome and cancellation proofs | [design/private-actor-ordering.md](design/private-actor-ordering.md) |
| Private Entry/Service/cohort layout and slab reuse proof | [design/private-actor-layout.md](design/private-actor-layout.md) |
| The local correctness gate and its lanes | [development/testing.md](development/testing.md) |
| Farm ordering audit and real-atomic TSan lanes | [review_torture/FARM_ORDERING.md](../review_torture/FARM_ORDERING.md) |
| Benchmark harness and optimization gate | [perftest/README.md](../perftest/README.md) |
| What changed in each candidate and its compatibility notes | [CHANGELOG.md](../CHANGELOG.md) |
| Remaining and deferred work | [ROADMAP.md](../ROADMAP.md) |
| Threadpool and Fibers | [threadpool/README.md](../threadpool/README.md), [fibers/README.md](../fibers/README.md) |
