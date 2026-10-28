# Proposal: validate the subscription before committing consumer membership

Status: alternative proposal with a tested prototype, 26 October 2026.
Base: `fix/scan-2.1.1`, commit `f593920`. The accompanying
[implementation patch](subscribe-validate-before-cf.patch) applies to that
revision; it has not been applied to either source checkout.

Review update: a further departure race reproduces in both this prototype
and its baseline. The patch closes the two original subscription failures,
but does not yet establish the quiescent single-pulse contract. See
[the unresolved departure race](#unresolved-departure-race) before treating
this as a complete fix.

Separate capacity admission from membership. Reserve a bounded admission
slot, perform the existing pinned walk and all validation, then increment
`Cf` immediately before attaching. A failed attempt never enters `Cf` and
never becomes responsible for the empty-Farm pulse. Successful subscriptions
continue to avoid the unsubscribe mutex.

## Why the earlier proposal needs a different ownership boundary

The [pin-before-Cf proposal](proposal-subscribe-pin-before-cf.md) correctly
identifies that an unprotected prospective subscriber must not make a real
last consumer decide it is not last. Its proposed frontier pin is still
provisional, however: a producer may already have certified that physical
slot between the initial `Wt` read and the pin. Planting a pulse there on
failure turns an intentionally rejected candidate into apparently live
space. The existing retained-quota schedule reproduces a fresh-segment
protection abort after that plant.

The capacity rollback also remains an emptying operation. A rejected
increment can take `Cf` from 128 to 129, pause while all 128 real consumers
leave, then roll back 1 to 0 without leaving a pulse. Being full at the
initial check does not establish nonemptiness at rollback.

This alternative makes both failures occur before membership exists. It
adds no pulse deposition site.

## Two counters with distinct responsibilities

`consumerSlots` counts capacity reservations: probing subscribers, committed
members, and views finishing unsubscription. An acquire-release CAS claims
one slot only while the observed count is below `MAX_CONSUMERS_LIMIT`.
Observing the limit returns `SUBSCRIBE_FULL` without changing either
counter or any root. A failed CAS reloads and retries; this is not a
wait-free admission operation.

`Cf` counts committed memberships. A subscriber enters it only after all
candidate validation passes, while retaining the candidate's `Sub`. From
that point, attachment has no recoverable failure. Only ordinary serialized
unsubscription decrements `Cf`.

The logical invariant is `0 <= Cf <= consumerSlots <= 128`. Separate loads
of the two atomics are not a consistent snapshot of that invariant.

The second counter preserves an existing bound that would otherwise be
lost: moving the capacity check to the end of the walk permits arbitrarily
many uncounted probes to deposit Subs. Here each probe owns one admission
slot before its first pin, holds at most one Sub per physical slot, and
releases its admission slot only after all temporary pins are gone. Each
root therefore still has at most 128 Sub units.

## Subscription and departure protocol

1. Reject a null Farm or an already subscribed view as today.
2. Reserve an admission slot. Return `FULL` if capacity is occupied.
3. Read `Wt`, walk backwards, pin before reading each slot, and select the
   candidate exactly as at `f593920`.
4. Validate initialization and all existing bounds: `Es`, `SqCs`, the
   sequence-epoch range, the wrap bound, and the current-frontier equality
   for a fallback with no live candidate. On any failure, drop every Sub,
   release the admission slot, and return `RETRY`. Neither `Cf` nor Sub0
   changes.
5. With every Sub still held, release-increment `Cf` and obtain `IDc` from
   `Reqs_c`. Capacity is already reserved, so this commit cannot return
   `FULL`. No `RETRY` branch follows it.
6. Attach using the validated `SqCs`; establish the leaf/root reference
   before dropping the Subs. Transfer the admission slot to the view.
7. On unsubscribe, keep the existing mutex and reference-release protocol.
   Use acquire-release for the `Cf` decrement. Release the admission slot
   after the final root release, before unlocking.

The code changes are small: one cache-line-aligned counter, admission
claim/release helpers, relocation of `add_consumer()`, removal of the two
failure-path `sub_consumer()` calls, stronger `Cf` orders, and one slot
release at the end of unsubscribe. The destructor also diagnoses outstanding
admission slots; it still requires externally synchronized exclusive lifetime.

## Ownership and ordering argument

**Before commit, a probe cannot suppress the last consumer's pulse.** It
does not contribute to `Cf`, even while its pins or capacity reservation are
held. If the last consumer leaves during the probe, the existing mutex and
last-releaser protocol retain protection. Dropping the probe's Subs cannot
remove that pulse or a consumer's low-half count.

**After commit, membership represents a validated anchor.** The selected
slot has passed the existing 6b validation while pinned. Live candidates
inherit the established count/pulse ownership history. A zero-low-half
fallback must still be the current frontier; the root/metadata acquire
argument from S1 remains necessary. A Sub alone does not revoke an earlier
quota grant, and this design does not treat it as doing so.

The accepted candidate remains pinned across commit and leaf attachment.
If a leaving consumer's decrement follows the commit in `Cf`'s modification
order, it can classify itself as not last while that validated anchor is
held. If its decrement precedes commit, it follows the existing
last-of-farm path. The later attachment either replaces a pulse or joins a
live count using the existing root protocol.

The handoff ordering is:

```text
candidate Sub release RMW
  -> Cf release increment
  -> leaver's Cf acquire-release decrement
  -> leaver's root release
  -> producer's acquire of that root release
```

All post-initialization `Cf` writes remain RMWs, so intervening commits and
departures retain the release-sequence connection. This orders the
subscriber's preceding pin through the leaver; candidate validation and
I1/I2/I3 supply the separate lifetime and sweep-contiguity argument.

Admission-slot release is ordered after the holder's last pin/root drop.
The acquire-release admission CAS joins that release history when capacity
is reused. This keeps the bound on pin holders separate from pulse ownership.

**Failures have no empty-Farm responsibility.** `RETRY` has never incremented
`Cf`; `FULL` has not even claimed a slot. Neither decrements `Cf` or plants
on a provisional frontier. This also handles multiple failing probes.

The intended pulse invariant must be stated at the right boundary: during
unsubscribe, `Cf` can already be zero while the departing view still owns
the reference it will replace with a pulse. At quiescence an empty, fully
confirmed Farm should have one pulse; abandoned incomplete epochs may
require additional pulses. The departure race below shows that the current
implementation does not establish this contract in every interleaving.

## Costs and observable behavior

The prototype adds 64 bytes to the Farm and one successful admission CAS
plus one eventual admission release per accepted attempt. It retains the
existing `Cf` increment/decrement for successful memberships. Producer and
consumer data paths gain no new operations or locks. Subscription and
departure performance has not been measured for this alternative.

Capacity includes outstanding probes and retiring views. A departing view
frees its slot after its roots are released; a failed probe frees its slot
after its Subs are dropped. A stalled probe can occupy capacity, as it could
when the original implementation counted it in `Cf`.

Producer consumer-count snapshots now exclude uncommitted probes. These
snapshots size the distribution across preallocated leaves; exact membership
was never required for correctness. `Reqs_c` now issues IDs only to committed
subscriptions, rather than consuming IDs for failed walks. The public
success and error values remain the same.

## Prototype evidence

The patch contains `tests/farm/subscription_admission.d` and includes it in
the existing LDC/DMD ordering target. Its seven deterministic schedules cover:

- A stale initial tail read, reclamation and retained quota before the first
  pin, followed by the last consumer's departure and `RETRY`. The certified
  slot stays zero and the retained grant remains usable.
- Two such failed probes nested across the same reclamation. Both release
  their admission slots without leaving an extra pulse.
- Capacity rejection paused while all 128 consumers leave a confirmed
  position. Normal unsubscription leaves the empty-Farm pulse.
- The last consumer leaving before the first pin, after validation but
  before commit, and after commit but before leaf attachment. Each schedule
  checks the surviving protection, refuses a producer write that would
  require sweeping through it, and completes subscription.
- 128 simultaneous uncommitted probes holding Subs. The 129th returns
  `FULL`; the admitted probes can subsequently attach and unsubscribe.

Validation completed on the scratch implementation:

- `check-farm-ordering`: existing S1/S2, completion and leaf-reuse tests,
  plus all seven new schedules, with LDC 1.43.0 / LLVM 22.1.8 and DMD 2.113.0.
- The Farm `baseline` unit suite with LDC.
- T18, T20, T27 and T23 with LDC, both ordinary and TSan builds. T27 reported
  `trials=2000 lost=0 extra=0` in each build.
- The seven new schedules under TSan. Their hooks are synchronous, so this
  verifies the instrumented execution of those logical schedules; it is
  not a separate weak-memory proof.
- `git apply --check` against the `f593920` checkout.

The full repository push gate, performance comparison, and Windows matrix
remain deferred. The patch includes the corresponding SPEC changes and
test documentation, so its implementation and contract can be reviewed
together.

To inspect or try it in a disposable checkout of `f593920`:

```bash
git apply --check /absolute/path/to/subscribe-validate-before-cf.patch
git apply /absolute/path/to/subscribe-validate-before-cf.patch
make -C tests/farm SHELL=/bin/bash check-farm-ordering
make -C tests/farm SHELL=/bin/bash baseline
timeout 45s make -C tests/farm SHELL=/bin/bash run-subscription-admission-tsan
```

## Unresolved departure race

Review of the companion proposal's claim that a pulse cannot be stranded
behind the attach point revealed a missing interleaving. The pulse need
not be behind the subscriber's initial attach point: the subscriber can
already have advanced beyond that point before the pulse is planted.

This deterministic schedule reproduces with the prototype and independently
with `f593920`, using only a scheduling hook immediately after the
unsubscriber computes `lastOfFarm`:

1. Consumer A holds confirmed epoch 0. The write tail is in epoch 1.
2. A acquires the unsubscribe mutex, decrements `Cf` from 1 to 0, records
   `lastOfFarm = true`, and pauses before releasing its position.
3. B subscribes successfully at epoch 0. It traverses the completed tables,
   advances to epoch 1, and drops its reference to epoch 0. B remains
   subscribed; A's reference is the only real reference left on epoch 0.
4. A resumes and releases its final root using its saved last-of-Farm flag.
   It leaves Sub0 on confirmed epoch 0, behind B's current position.
5. A producer requiring a fresh three-segment grant from epoch 1 is blocked
   by that pulse. B's ordinary forward progress cannot revisit epoch 0 to
   clear it. B then unsubscribes from epoch 1, leaving a second pulse.

Observed in both builds:

```text
before old release: Cf=1 root0=1     root1=1 new_position=1
after old release:  Cf=1 root0=10000 root1=1 new_position=1
producer write=0; epoch 0 is confirmed
after both leave: Cf=0 pulses=2
```

Root values are hexadecimal; `10000` is one Sub0. This is a retained-capacity
and progress defect, not a demonstrated overwrite. The existing seven
schedules remain passing: they pause the subscriber while the leaver runs,
whereas this schedule pauses the leaver while the subscriber attaches and
advances. The scratch reproducer is
`/tmp/antfarm-departure-review-r6px9ty3/late_pulse.d`; its `baseline/`
counterpart repeats the schedule against `f593920`.

The proposal needs an additional departure/arrival handshake. Serializing
the membership commit and attachment with the existing unsubscribe mutex
would exclude this interleaving, but would change the proposed guarantee
that successful subscriptions never take that mutex. A mechanism that
retains that guarantee needs a separate ownership proof; merely moving a
`Cf` read closer to the pulse CAS still leaves a race window. Neither
remedy has been implemented or validated here.

Other conclusions from the integrated notes:

- Before commit, existing consumers and pulses carry I3. Do not replace
  that argument with an unconditional claim that the first, potentially
  stale frontier Sub protects every in-flight reservation.
- Capacity below the `Cf` limit can still be occupied by probes or retiring
  views; this is already explicit in the proposal and patched SPEC.
- A 2,000-trial threaded arrival/departure test is useful additional
  coverage, alongside the deterministic schedule above. A quota audit must
  distinguish generations and already-issued grants from later sweeps;
  any historical certification of the same physical slot is not itself a
  failure.
- The extra admission CAS and release are not demonstrated to be immaterial
  under contention. The performance comparison remains outstanding.
- The precise Sub bound is 128 per root, or at most `K * 128` across all
  roots. The existing pre-first-pin test hook is `subscriberProbed`, not
  `subscriberPinned`.
