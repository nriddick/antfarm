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

- **Generation 2 reference model (lossless consumption).** Consumers hold a
  *position* reference plus a local array of *trailing* references on
  segments entered but not yet confirmed complete. The consumer whose
  `Tprogress` add lands on `Tlen` accounts the table's size (stored in
  `Thead[5]`) into a new per-segment field `Sd`, which producers zero when
  re-initializing the segment. A segment is confirmed complete — and its
  trailing references released — when `Seqt[Ki] + Sd[Ki]` reaches the next
  segment boundary (the crossing is impossible while `Wt` is still inside
  the segment) *and* `Sbal[Ki] == 0`. `Sbal` is the signed balance that
  producers increment by table size (before the sentinel release) and
  finishing consumers decrement; it defeats the false completion picture a
  large table finishing ahead of a small one would otherwise give. A
  segment completely spanned by one table gets `Seqt` past its boundary and
  no increments, so it confirms trivially and is released as soon as the
  consumer follows the chain past it. The last unsubscriber plants Sub0 at
  its *unconfirmed* held segment, so a later subscriber re-walks and
  drains the backlog exactly once (testBacklog). Idle consumers re-walk
  their oldest unconfirmed segment claiming leftovers, which keeps the
  system deadlock-free: producer stalls create the consumer idleness that
  resolves them.
- **Completer-as-sweeper (generation 2).** The consumer whose 5e-k add
  completes a shard gains the sweeper role: one linear pass over foreign
  shard counters (plain-load pre-check, then normal chunk claiming), then a
  pre-checked drain of all MT payloads. Sweeps of foreign shards can't
  recursively trigger more sweeps. Starvation requires every shard's
  completer to be gone — i.e., no consumers and hence no progress anyway —
  in which case Sub0 preserves the data. `write()` enforces
  `Done > 1 => MaxCs >= 2` so a sweeper always finds a claim slot (a
  primary visit burns exactly one claim).
- **Undersaturation repair (generation 4).** The complement of the 5h
  oversaturation nudge: a sweeper finding a foreign shard with
  `Z = X - Shiter <= 1` (essentially no native traffic) nudges its IDc by
  the round-robin distance into that shard — `IDc += (shi - myShi + sq) % sq`
  — and migrates its position leaf, patching the hole a pre-empted or
  unsubscribed native left. A laggard native waking to an overbalanced
  shard nudges itself out via 5h. Self-limiting: each entering sweeper's
  failed exit-claim raises the shard's Z, so the signal disappears after a
  couple of adopters. At most one adoption per consumer per table; small
  tables are excluded since their lone shard always reads starved.
- **Carried sweeper role for small tables (generation 5).** Small tables
  concentrate all claims on shard 0 (5e-d), so under churn they can have
  no native consumer at all. A consumer that completed a shard of the
  previous table sweeps a small next table regardless of its own shard
  assignment, chaining the role forward while small tables continue. The
  idle re-walk backstop still applies, but the hot path no longer stalls
  producers waiting for it.
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
- **Leaf tally indices are cached per held reference** (`curLti`,
  `trailLti[]`) because a segment's `SqCs` changes across epochs;
  decrementing a leaf chosen by a newer `SqCs` would corrupt the tallies.
- **Segment-tally transitions are lazy**: a consumer takes its reference
  when it *validates* a table's sentinel in the new segment (which implies
  the producer has initialized that segment's stats), not when it reads the
  `Tnext` link. The old segment's reference is retained (trailing) until
  the segment is confirmed complete via the `Sd` crossing.
- **PayloadHeader is 17 ulongs** (136 bytes): the spec's field list
  (MaxCs/Done, Plen, Call, 6 filler, Pcount, 7 filler) sums to 17, not 16.
- **`PayloadBody = const(ulong)[]`** — a mutable slice of const ulongs
  (`const ulong[]` would const the slice itself and make `PayloadEntry`
  unassignable).
