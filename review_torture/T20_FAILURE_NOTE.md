# T20 confirmed-segment pulse failure: session handoff

Status: observed once on 2026-08-28 while preparing the combined Ant Farm
1.5.0 repository. It did not reproduce on the immediate isolated rerun. Treat
it as an unresolved schedule-sensitive signal, not as a confirmed compiler bug
or a dismissed load artifact.

The relevant release is commit `221f8c9` (`v1.5.0`). The Farm source in that
commit was byte-for-byte identical to the newer copy used by the Fiber work;
the monorepo fold did not edit this algorithm after testing began.

## Environment

- Host: Ryzen 5 5500, Linux, 12 logical processors / 6 SMT cores, one LLC.
- DMD: `DMD64 D Compiler v2.112.0`.
- LDC: `1.42.0`, frontend `v2.112.1`, LLVM `21.1.8`.
- Huge pages were disabled with `ANTFARM_HUGE_PAGES=0`.
- Test: `review_torture/torture_tests.d`, arm `T20`.

## Exact observation

The DMD and LDC torture suites were initially launched at the same time in
separate processes:

```text
ANTFARM_HUGE_PAGES=0 make -C review_torture run-dmd
ANTFARM_HUGE_PAGES=0 make -C review_torture run
```

The LDC process passed all torture tests. Its eight T20 trials reported zero
confirmed segments carrying a pulse.

The DMD process reported:

```text
T20 trial 0: confirmed=15 pulses=1 calls=10000/10000
T20 trial 1: 3 confirmed segments with pulse (expected <= 1)
T20 trial 1: confirmed=15 pulses=3 calls=10000/10000
T20 trial 2: confirmed=15 pulses=1 calls=10000/10000
T20 trial 3: confirmed=15 pulses=1 calls=10000/10000
T20 trial 4: confirmed=15 pulses=1 calls=10000/10000
T20 trial 5: confirmed=15 pulses=1 calls=10000/10000
T20 trial 6: confirmed=15 pulses=1 calls=10000/10000
T20 trial 7: confirmed=15 pulses=1 calls=10000/10000
CHECK FAILED torture_tests.d:1213: T20 stale plant on confirmed segment or lost work
```

There was no lost or repeated payload work: the failing trial still observed
exactly 10,000 calls. The failure was solely the final `Rtlow` invariant scan.

The same DMD target was then rebuilt and run by itself. All eight T20 trials
reported `pulses=0`, exact call counts, and the complete torture suite passed.
Thus the evidence so far is one failure under concurrent host load and one
isolated pass. It is insufficient to estimate a failure rate.

## What T20 is checking

T20 creates a `K=16` Farm, four steady consumers, three subscribe/consume/
unsubscribe churners, and 10,000 single-payload tables. Producer segment
crossings and churner unsubscriptions both exercise
`plantIfUnprotected()`/`plantUnprotectedIncomplete()`.

After producer and consumer threads join, the test classifies a segment as
confirmed when:

```text
Es[Ki] == Ei
Es[Ki+1] == Ei+1
Seqt[Ki] + Sd[Ki] >= Seqt[Ki+1]
```

It then permits at most one confirmed segment with nonzero `Rtlow`: the
last-of-farm empty pulse. More than one means a pulse remained on another
confirmed segment, contrary to the intended confirmed-prefix release
invariant.

The regression this arm originally covered was a stale plant:
`plantIfUnprotected()` could decide a segment was incomplete, race its
finisher, then plant `SUB0` after confirmation. The current implementation
rechecks `Seqt + Sd >= next Seqt` after the plant CAS and tries an exact-value
CAS from `rt + SUB0` back to `rt`.

## What is and is not established

Established:

- A final joined-state scan once saw three confirmed physical slots with
  nonzero `Rtlow` under DMD.
- Payload accounting remained exact.
- Extra host scheduling pressure correlates with the only observation so far,
  but causation is unknown.
- The same source passes LDC and passed the immediate isolated DMD rerun.

Not established:

- Which `Ki` slots carried the extra pulses or the exact `Rt` values.
- Whether each low word contained only one `SUB0` unit, multiple units, or a
  residual count as well as `SUB0`.
- Whether the pulse came from the plant path, an unsubscribe
  `leavePulse=true` path, or a later clear-and-releave sequence after the
  plant's exact retract CAS failed.
- Whether DMD code generation, weak ordering, the test's classification, or a
  real algorithmic lifetime ordering is responsible.
- Whether running another torture process is necessary; it may merely widen a
  timing window.

Do not report this as a merge regression. Do not weaken T20 to allow more than
one pulse without first explaining how producers remain protected and how the
extra confirmed pulses are eventually cleared.

## Recommended next session

1. Build once, then run only T20 repeatedly so compilation and unrelated arms
   do not dilute the schedule:

   ```text
   dmd -g -checkaction=context review_torture/torture_common.d \
       review_torture/torture_tests.d antfarm.d antfarm_templates.d \
       -of=/tmp/antfarm_t20_dmd
   ANTFARM_HUGE_PAGES=0 /tmp/antfarm_t20_dmd T20
   ```

   Run isolated loops first, then repeat while a separate CPU/memory load or
   the LDC T20 binary runs. Record pass counts and timeouts separately.

2. Expand the final failure print before changing synchronization. For every
   offending slot record at least:

   - `ki`, `Es`, next `Es`, `Rt`, `Rt & COUNTMASK`, and
     `(Rt & SUB0MASK) >> 16`;
   - `Seqt`, `Sd`, next `Seqt`, `Wt`, and `Eg`;
   - all nonzero leaf counts for that physical slot.

   The existing output counts offending slots but loses the state needed to
   distinguish stale plant, residual consumer count, and multiple-pulse cases.

3. If needed, add a fixed-size allocation-free trace rather than `printf` in
   the race window. Log successful plants, post-plant completeness, retract
   success/failure, last-root `leavePulse`, and 0-to-1 pulse clearing. Include
   epoch as well as physical slot so wrap reuse is visible. Dump only after a
   failing joined-state scan.

4. Audit these candidate orderings without assuming any is the cause:

   - the exact post-plant retract CAS fails because `Rt` changed, after which a
     later last release leaves a pulse on a segment that has become confirmed;
   - `unsubscribe()` calls `tryReleaseOldest()` once, then uses
     `leavePulse=true` for remaining interior epochs which may confirm during
     another consumer's final work;
   - publication/observation of the relaxed `Sd` completion add relative to
     the release sequence on the segment root;
   - physical-slot wrap reuse between metadata checks and `Rt` mutation.

5. Preserve the two independent properties in any fix: exact payload execution
   and no unprotected incomplete segment. Re-run T18, T20, T21, T23, and T24,
   then the full DMD/LDC torture suites and TSan only if the local sanitizer
   runtime is known to work.

Relevant code in `v1.5.0`:

- `antfarm.d`: `plantIfUnprotected`, `plantUnprotectedIncomplete`,
  `takeRootCount`, `releaseRootLeavePulse`, `decRef`, `unsubscribe`, and the
  final `Sd` accounting in `processShard`.
- `review_torture/torture_tests.d`: `t20_plant_confirmed_segment`.
- `review_torture/Makefile`: `run`, `run-dmd`, and the sanitizer targets.
