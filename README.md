# Ant Farm — build & test

Implementation of the concurrent queue spec in `spec/`. Single-module D
library plus a test runner.

    # debug build
    dmd -g antfarm.d antfarm_test.d -of=antfarm_test && ./antfarm_test

    # release (dmd or ldc2)
    dmd -O -inline -release antfarm.d antfarm_test.d -of=antfarm_test
    ldc2 -O2 -release antfarm.d antfarm_test.d -of=antfarm_test

    # ThreadSanitizer (clean as of this writing)
    ldc2 -O1 -fsanitize=thread antfarm.d antfarm_test.d -of=antfarm_test_tsan

## Files

- `antfarm.d` — the Farm (magic buffer, producer tiers, tallies, tables)
  and `ConsumerView` (subscribe/consume/unsubscribe). All `@nogc nothrow
  @system`; invariant violations abort the process per spec.
- `antfarm_test.d` — arithmetic checks, single-threaded Sub0 lifecycle,
  concurrent exact-call accounting, multi-lap wraparound, subscription cap,
  and subscribe/unsubscribe churn.

## Deviations / decisions worth knowing

- **Subscribe attaches in place.** Instead of the spec's walk-back then
  walk-forward-with-held-Subs, the subscriber scans all K segments once from
  `Eg`, picks the earliest segment with `Rt' > 0`, deposits a `Sub` there,
  and establishes its leaf reference on that same segment regardless of
  window races — the held `Sub` blocks producers from lapping the segment
  until the leaf reference exists. If no pulse exists at all (only possible
  transiently, when the second-to-last consumer unsubscribes mid-subscribe),
  the subscriber stands at the frontier segment `Eg & (K-1)`. One code path,
  no Sub list to unwind.
- **Sub0 is a bit, not a count.** Deposit (last unsubscribe) is an atomic
  OR, clear (first subscriber) is an atomic AND of `~SUB0` — idempotent, and
  unable to corrupt the consumer count in the low half. Uses
  `core.stdc.stdatomic` fetch-and/or where available (LDC/GDC); DMD falls
  back to `core.atomic.atomicOp` (same semantics).
- **Tertiary sweep (beyond spec).** The spec leaves fallback/tertiary
  behavior open; without it, a shard starved of consumers (unsubscribes,
  preemption, IDc nudges collapsing residue classes) never completes and
  consumers stall. Here: after the primary shard is exhausted, a consumer
  sweeps foreign shard counters (plain-load pre-check, then normal chunk
  claiming), and after the secondary MT round-robin it sweeps *all* MT
  payloads (pre-check `calls < Done && claims < MaxCs`, then enter and
  drain). Result: any single consumer passing through a table fully
  completes it. `write()` enforces `Done > 1 => MaxCs >= 2` so a sweeper
  always finds a claim slot (a primary visit burns exactly one claim).
- **Leaf index is cached in the view** (`curLti`) because a segment's `SqCs`
  changes across epochs; decrementing a leaf chosen by a newer `SqCs` would
  corrupt the tallies.
- **Segment-tally transitions are lazy**: a consumer moves its tally when it
  *validates* a table's sentinel in the new segment (which implies the
  producer has initialized that segment's stats), not when it reads the
  `Tnext` link.
- **PayloadHeader is 17 ulongs** (136 bytes): the spec's field list
  (MaxCs/Done, Plen, Call, 6 filler, Pcount, 7 filler) sums to 17, not 16.
- **`PayloadBody = const(ulong)[]`** — a mutable slice of const ulongs
  (`const ulong[]` would const the slice itself and make `PayloadEntry`
  unassignable).
