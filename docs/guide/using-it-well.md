# Using Ant Farm well

Producers publish tables into a fixed ring; any subscribed consumer may claim
work from them. There is no FIFO guarantee and no "all done" signal. A `Token`
is publication authority; a `ConsumerView` is a persistent consumption cursor.
Both are move-only. Completion, parking, and priority are application policy.
The owning model is [SPEC](../../SPEC.md#1-model-scope-and-terminology).

## Correctness rules

1. **Pop only the published prefix.** `write()` returns how many payloads it
   took; pop exactly that many and retry the suffix. With work pending, `0`
   is backpressure; an empty source also returns `0`. It does not advance the caller's range. Never resend a taken prefix.
   See [the write contract](../../SPEC.md#5b-write-call-and-return).
2. **Count your own completions.** `consumeNext() == false` means nothing is
   ready at this cursor now. It means neither empty nor done. See
   [consumers and views](../reference/actor-lifecycle.md#consumers-and-views).
3. **Keep one view per consuming thread.** Subscribe it once, retrying
   `SUBSCRIBE_RETRY`, and unsubscribe on that thread before Farm destruction.
   Move tokens rather than copying them, or pass them by `ref` to one active
   producer. See [views](../reference/actor-lifecycle.md#consumers-and-views)
   and [tokens](../reference/actor-lifecycle.md#tokens-and-farm-provenance).
4. **Respect callback storage.** Callbacks are `nothrow @nogc @system`.
   Raw callbacks borrow a body pointer and word count until return. Packed parameter values are
   copied, but immutable references such as `string` still refer to external
   data: keep that data alive until the job finishes. See
   [the callback contract](../../SPEC.md#9-memory-order-contract) and
   [parameter adaptation](../../antfarm/templates.d).
5. **Drain before destruction.** Stop producing, finish the logical drain,
   join consumers and unsubscribe views, destroy actor infrastructure,
   unregister tokens, then destroy the Farm. Quiescent unsubscribe and
   unregister may happen in either order. `FarmHost` performs teardown after
   the application drains; `close()` refuses undrained work. Hand-written
   pools own the same obligations. Use the
   [shutdown recipe](../reference/actor-lifecycle.md#shutdown-recipe) and
   [host teardown](hosting.md#teardown).
6. **Check each payload shape with `canFit`.** At setup, check singleton cost
   against the intended tier. This uses the live consumer-count snapshot;
   growth can change true to false. Recheck after changing participants. It
   reserves nothing and predicts neither backpressure nor batch admission.
   A later `write()` still validates and aborts on an unfittable payload. See
   [sizing one Farm](../../ARCHITECTURE.md#sizing-one-farm) and
   [the singleton check](../../SPEC.md#5b-write-call-and-return).

## Performance rules

1. **Batch.** Prefer tens to thousands of entries per `write()` when the
   workload allows it. Avoid single-item writes from callbacks; accumulate
   and flush per phase. Each table has fixed overhead. See
   [batch and quota decisions](sizing-and-allocation.md#configuring-batches-and-farm-quota).
2. **Publish homogeneous tables.** All entries sharing one
   callback and one nonzero body width use only the body words per entry.
   A one-word body costs 1 word in that layout, 2 with uniform callback but
   variable width, and 3 with mixed callbacks. Wider bodies add their own
   words; these counts exclude fixed table overhead. Use common-callback and
   fixed-length write forms to reduce producer sizing work. See
   [entries per quota](../../ARCHITECTURE.md#how-many-entries-fit-a-quota) and
   [uniform tables](../../SPEC.md#2g-uniform-call-tables).
3. **Choose `avgCost` per table.** Chunk size is `128 >> avgCost`, with classes
   0 through 7. `write()` defaults to 3, claiming 16 entries; 0 claims 128 for
   very cheap
   uniform work; 5–6 claim 4 or 2 for expensive work where shorter visits and
   tails matter. These are tuning choices, not fixed nanosecond thresholds.
   Group jobs by cost class before flushing: `avgCost` is one value per write,
   hence per table. Small tables assign their work
   to shard 0, still in chunks. See
   [work-cost hints](sizing-and-allocation.md#work-cost-hints).
4. **Size ring and quota separately.** Fit the ring to the intended LLC working
   set and quota to one phase's burst. After resolving defaults,
   `maxBulk * quotaBulk + maxSmall * quotaSmall` must fit `k - 1` segments.
   Each tier's capacity must cover every thread that can hold that tier's
   token simultaneously. See [the formulas](../../ARCHITECTURE.md#sizing-one-farm)
   and [tier decisions](sizing-and-allocation.md#farm-setup-and-tier-choice).
5. **Pin consumers and prefer one Farm per LLC group.** `FarmHost` defaults to
   pinned workers in one group; `FiberServicePlan` defaults to native-only
   service for its LLC lanes. Cross-group service is an explicit choice.
   The reported 42–73% slowdown was an instrumented two-worker Fiber
   assist-ring probe with persistent remote subscriptions, compared with no
   edges on one two-LLC host; it is not a general shared-Farm penalty. See
   [host placement](hosting.md#placement), [LLC service](../../ARCHITECTURE.md#one-llc-and-many-llcs),
   and [assist-ring measurements](../../PERFORMANCE.md#assist-ring-ab).
6. **Account for idle costs.** Polling consumes CPU; arrange waiting through
   the Director or host idle policy. `host.waitUntil` drives control-thread
   progress; it is not automatic core parking. A stalled older segment makes
   idle polls revisit its incomplete suffix, using a persistent sweep cursor.
   Keep callbacks short or publish independent grains with
   `payloadRange!fn(source, grain)`. See
   [host progress and waking](hosting.md#progress-and-waking),
   [reclamation](../../SPEC.md#8-table-accounting-and-segment-reclamation), and
   [payload layout](../../SPEC.md#2d-payload-layout).
   For large cohorts of cheap callbacks, measure the child batch limit and
   host emission burst before adding workers: small tables can spend more
   time waking the pool than doing work. See
   [cheap work with parked workers](hosting.md#cheap-work-with-parked-workers).
7. **Treat `expectedConsumers` as a hint.** A positive value seeds epoch-zero sharding;
   later epochs use the live count. A valid hint far from reality can cost
   performance, never safety. The actual cap is 128 simultaneously subscribed
   views. See [sizing](../../ARCHITECTURE.md#sizing-one-farm).

## Choosing a layer

Short `@nogc` fan-out: payload. Address plus inbox: public actor. A phase over
an externally chosen set: actor wave. One conductor, joined results, and
tracked deadlines: private tree. Work that waits, throws, or allocates:
Fiber. A Fiber that never suspends usually belongs on the payload path.
The [choice guide](choosing-work.md) owns the full comparison.

## Application policies

The core deliberately leaves FIFO, priorities, thread affinity, "all done",
and parking to the application. Several Farms can separate priority classes;
a Farm with one subscribed thread can confine its work. Use an application
counter or a `write` completion hook for completion; the hook joins every
payload callback in one accepted physical table. Exceptions cannot escape
payload callbacks. See [the model](../../SPEC.md#1-model-scope-and-terminology),
[tracked completion](../../SPEC.md#9-memory-order-contract), and
[the Director](../../threadpool/README.md#start-workers).
