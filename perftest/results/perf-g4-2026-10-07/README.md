# G4 derived default root work-cost hint results — 2026-10-07

Candidate `edcbe5a` keeps the derived default root work-cost hint. `pump`
with no hint derives the published class per table with
`rootCostHintFor(count, live Cf, farm small threshold)` (antfarm.d, next to
`MAX_CHUNK`/`MAX_AVG_COST`): a table at least the small-table threshold wide
is sharded across every shard and keeps class 0; a wholesale (single-shard)
table gets the cheapest class whose claims still number at least
`min(count, clamp(consumers, 1, 8))` — class 0 whenever it already provides
that many. Explicit `0..5` hints pass through unchanged (`rootCostHintAuto`
sentinel default; `pump(token, limit, 0)` is the pre-G4 behavior). Child wave
emits still publish class 0. The benchmark reports the effective hint
(`root_cost_hint=…`) and accepts `root-hint=auto|0..5`; the gate accepts
`--root-hint`.

Base: `c4ad2795d7055158ef0d090385be15d373863b81`. Candidate: `edcbe5a`
(runtime `9eeaf55`); the batch-1024/512 and root-hint-4 runs below used
exactly those commits. The default-matrix runs (full-5, boundary-0,
control-boundary-0, inert-control-0) were measured on the pre-rewrite
equivalent `865978c` (runtime `838f308`), whose only differences from the
shipped commits are review corrections with no runtime behavior change
(doc-comment wording, the benchmark's `__traits(compiles)` import shape and
a unit-test pin); their pinned benchmark source was the pre-rewrite
`48e22eb` (see [investigation/repro-48e22eb-runs.md](investigation/repro-48e22eb-runs.md)).
On that benchmark the candidate reported `root_cost_hint=0` because
`rootCostHintFor` had moved from `actors` (private_actor) into `antfarm`
while the 48e22eb source still probed the three-argument helper without
importing the symbol into scope, so its static-if probe was false (verify
confirmed the same source reports 4/8 once the import is added); the
timing is unaffected (both sides ran the identical benchmark and the
runtime derivation is the same), and the re-run batch cells below record
the correct reported hint.
Worktree:
`/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree`, branch
`perf/g4-default-hint`. Nothing was pushed or written to the main checkout.

## The pathological case, fixed

Root pump tables of fewer than `smallThreshold` (default 64) entries are
claimed wholesale by shard 0; at class 0 (chunk 32) a table of up to 32
entries is one claim on one worker regardless of consumer count. With batch
1024 and 16 parents, tree/6 fell to about 18 Mleaf/s while every parent
callback allocated a 1,024-child wave. The derived hint publishes class 4
(runs of 2, eight claims) for a 16-entry table at six consumers; batch 512
(32-entry tables) derives class 3 (runs of 4). The re-run batch cells'
`runs.jsonl` record `root_cost_hint=4 root_hint_claims=8` on candidate
batch-1024 tree runs (base: 0 and 1) and `root_cost_hint=3 root_hint_claims=8`
on batch-512.

| Run (5 pairs, consumers 6) | Cell | Base | Candidate | Median delta | Decision |
| --- | --- | ---: | ---: | ---: | --- |
| [batch-1024-6](batch-1024-6/gate.json) | tree | 18.3 | 83.9 | **+359.39%** [+350.44,+365.71] | pass |
| [batch-1024-6](batch-1024-6/gate.json) | tree-wait | 18.4 | 83.6 | **+355.50%** [+344.04,+363.37] | pass |
| [batch-1024-6](batch-1024-6/gate.json) | tree-cont | 32.7 | 33.6 | +2.93% [+2.65,+3.19] | pass |
| [batch-1024-6](batch-1024-6/gate.json) | private | 23.0 | 23.2 | +0.67% [+0.22,+1.24] | pass |
| [batch-512-6](batch-512-6/gate.json) | tree | 18.4 | 78.4 | **+325.13%** [+319.46,+332.07] | pass |
| [batch-512-6](batch-512-6/gate.json) | tree-wait | 18.3 | 78.0 | **+326.66%** [+315.69,+332.19] | pass |
| [batch-512-6](batch-512-6/gate.json) | tree-cont | 32.3 | 34.0 | +5.41% [+4.65,+5.77] | pass |
| [batch-512-6](batch-512-6/gate.json) | private | 23.0 | 23.1 | +0.57% [−0.46,+0.99] | pass |

Rates are Mleaf/s (tree family) or M cycles/s (private). Single-binary
sanity (candidate benchmark, `auto` vs pinned `root-hint=0`): tree/6 batch
1024 measured 81.3 vs 18.0 Mleaf/s in alternating runs.

**Explicit hints are unchanged.** With `--root-hint 4` at batch 1024 (both
runtimes publish literal class 4, recorded as `root_hint=4 root_cost_hint=4
root_hint_claims=8` on both sides),
[root-hint-4-batch-1024-6](root-hint-4-batch-1024-6/gate.json): tree −0.44%
(base 84.2, candidate 83.8 Mleaf/s), tree-wait −0.59% (one wide pair
stretches the 5-pair interval to inconclusive), tree-cont −0.86%, private
+0.36%. `root-hint=0` reproduces the pre-G4 baseline on any runtime.

## Run decisions

| Run | Pairs/cell | Cells | Pass | Inconclusive | Regression | Exit |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [full-5](full-5/gate.json) | 5 | 34 | 28 | 5 | 1 | 1 |
| [boundary-0](boundary-0/gate.json) | 20 | 7 | 2 | 4 | 1 | 1 |
| [control-boundary-0](control-boundary-0/gate.json) | 20 | 4 | 4 | 0 | 0 | 0 |
| [inert-control-0](inert-control-0/gate.json) | 20 | 4 | 0 | 1 | 3 | 1 |
| [inert-control-tree-0](inert-control-tree-0/gate.json) | 20 | 1 | 1 | 0 | 0 | 0 |
| [candidate-tree-0](candidate-tree-0/gate.json) | 20 | 1 | 0 | 1 | 0 | 2 |
| [control-tree-0](control-tree-0/gate.json) | 20 | 1 | 1 | 0 | 0 | 0 |
| [inert-exact-tree-0](inert-exact-tree-0/gate.json) | 20 | 1 | 1 | 0 | 0 | 0 |
| [candidate-tree-0-40](candidate-tree-0-40/gate.json) | 40 | 1 | 1 | 0 | 0 | 0 |
| [control-tree-0-40](control-tree-0-40/gate.json) | 40 | 1 | 1 | 0 | 0 | 0 |
| [batch-1024-6](batch-1024-6/gate.json) | 5 | 4 | 4 | 0 | 0 | 0 |
| [batch-512-6](batch-512-6/gate.json) | 5 | 4 | 4 | 0 | 0 | 0 |
| [root-hint-4-batch-1024-6](root-hint-4-batch-1024-6/gate.json) | 5 | 4 | 3 | 1 | 0 | 2 |
| [superseded unthresholded full-5](superseded-unthresholded-full-5/gate.json) | 5 | 34 | 27 | 5 | 1 | 1 |
| [superseded 48e22eb full-5](superseded-48e22eb-full-5/gate.json) | 5 | 34 | 26 | 8 | 0 | 2 |
| [superseded 48e22eb boundary-0](superseded-48e22eb-boundary-0/gate.json) | 20 | 7 | 3 | 2 | 2 | 1 |

The superseded unthresholded run also carries one `inconclusive_work`
decision (tree-cancel/6, dispatched fraction 0.957 → 0.651) beyond the
pass/inconclusive/regression columns; it is why that variant was
withdrawn. The re-run batch tables above (edcbe5a on both sides) replaced
the pre-review batch runs, which used the 48e22eb benchmark.

All default-matrix /6 cells pass in full-5 (largest medians: tree-wait/6
+2.21%, tree-cancel/6 +3.18% with dispatched fraction 0.9557 vs 0.9488,
inside the 0.01 tolerance). The exit-1/2 statuses come from the /0 layout
cells below; no /6 cell regresses.

## The /0 layout waiver

The full sweep's one regression is `actor/0` −3.82% [−4.26,−3.19]; at 20
pairs it is −4.18% [−4.51,−4.09]. Near it: subset10/0 −3.05% [−3.17,−2.83]
and subset100/0 −3.20% [−3.41,−2.70] (inconclusive), tree/0 −2.27% and
tree-wait/0 −3.08% (both wide-spread inconclusive; the byte-identical
control's tree-wait/0 pairs span −5.93…+10.24%). subset-whole/0 and
subset-whole-unreserved/0 pass.

Every /0 cell executes byte-identical semantics in both binaries: with one
subscriber (the controller's inline view) the derived hint is always class
0 — `rootCostHintFor` guarantees the cheapest class for `consumers <= 1` —
so `actor/0` (public runtime only) and every private-mode /0 cell publish
exactly the base classes. The candidate adds ~2.1 KB of text, moving text
symbols in two regions — the hot public functions at +2080 and the bulk
(1581 of 1903 symbols) at +2176, with 168 at +2192 and 109 unmoved —
changing the
64-byte alignment of every hot public function (createActor 48→16, flush
32→0, consumeNext 16→48, processShard 32→0, sweepCurrentPosition 0→32,
reclaimActor 48→16); see
[investigation/layout-alignments.md](investigation/layout-alignments.md).

An **inert control** proves the gate cannot attribute these cells to
semantics: base plus ~2.1 KB of never-executed text (never-executed probe
functions anchored in pump's always-aborting invalid-batch-limit branch,
reproducing the same alignment pattern; diff in
[investigation/inert-control.patch](investigation/inert-control.patch), run
from a throwaway branch that was removed afterwards)
swings the same cells by more than the candidate itself:

| Cell (20 pairs) | Inert control | Candidate 865978c | Byte-identical control |
| --- | ---: | ---: | ---: |
| actor/0 | **−5.09%** [−5.37,−4.66] | −4.18% [−4.51,−4.09] | +0.09% [−0.10,+0.19] |
| subset10/0 | **−4.53%** [−4.76,−3.70] | −3.05% [−3.17,−2.83] | −0.46% [−0.68,−0.07] |
| subset100/0 | **−4.12%** [−4.27,−3.79] | −3.20% [−3.41,−2.70] | −0.31% [−0.65,−0.20] |
| tree-wait/0 | −2.84% [−4.07,−1.33] | −3.08% [−4.61,−1.57] | −1.03% [−2.63,−0.19] |

The byte-identical base/base control passes all four cells, so the host and
harness are quiet; the loss follows the binary layout, not the rule. Two
earlier candidate layouts regressed `actor/0` too (−3.16% unthresholded,
−3.76% at 48e22eb), including one where the helper lived in
`actors/private_actor.d` instead — LDC's whole-program layout is
size-driven, not module-placement-driven, so re-rolls do not escape it.

**Dispositions of the non-passing /0 cells:**

- `actor/0` (regression, −4.18%): covered by the layout waiver — the inert
  control swings the same cell by −5.09% with byte-identical semantics and
  the same hot-function alignment pattern.
- `subset10/0` (inconclusive, −3.05%) and `subset100/0` (inconclusive,
  −3.20%): covered by the layout waiver (inert −4.53% / −4.12%).
- `tree-wait/0` (inconclusive, −3.08%): covered by the layout waiver — the
  inert control swings it by the same amount (−2.84% [−4.07,−1.33]).
- `tree/0`: **pass.** The 20-pair follow-ups on the edcbe5a benchmark
  source were inconclusive (candidate −1.98% [−3.19,−0.77]; byte-identical
  control +1.00% [−0.87,+2.85]; exact-twin inert control clean at +1.20%
  [−0.25,+1.35], so the layout mechanism does not swing this cell). At the
  decided 40 pairs the candidate passes:
  [candidate-tree-0-40](candidate-tree-0-40/gate.json) is **+1.00% [−0.00,+1.88]** and the
  byte-identical [control-tree-0-40](control-tree-0-40/gate.json) is +0.86%
  [−0.98,+2.14]; the 20-pair medians were a small-sample draw of this cell's
  ±3%-per-pair noise.
- `subset-whole/0` and `subset-whole-unreserved/0` pass (wide /0 spreads,
  as in the byte-identical control).

Under the agreed decision rule the layout waiver is a one-off for the
cells listed above as covered, backed by control evidence and recorded in
[perftest/README.md](../../README.md) as a known gate limitation; it is
not a general precedent.

## Superseded variants (kept as evidence)

- `superseded-unthresholded-full-5`: the first rule shape also reclassed
  sharded tables (target claims from any table). It measured tree/6 +30.77%
  and tree-wait/6 +30.99% at the **default batch 256** — 64-entry tables
  shard into only `sqcsOf(6)=3` shards, so class 0 used 3 of 6 workers —
  but staggering parent activation let early waves' cancellation truncate
  later parents' waves before emission: tree-cancel/6's dispatched fraction
  fell 0.957→0.651 (`inconclusive_work`; equal-work callback rate −16.5%).
  That exceeds the agreed "only removes the pathological single-claim case"
  scope, so the shipped rule keeps sharded tables at class 0. The +31%
  default-batch tree gain is a noted future opportunity that needs its own
  decision (and a tree-cancel work-fraction strategy).
- `superseded-48e22eb-full-5`, `superseded-48e22eb-boundary-0` and the
  first `control-boundary-0`: the threshold-guarded candidate with the
  helper in private_actor.d; same semantics, different layout. Its
  boundary run put actor/0 at −3.76% [−3.99,−3.53] and subset10/0 at
  −3.25% [−3.79,−3.06] with the same clean byte-identical control, which
  motivated the re-roll and then the inert control. The
  `control-boundary-0` numbers in the table above are that run (a
  base/base control is candidate-independent).

## Verification

On `edcbe5a` (both rewritten commits build with LDC and DMD; the runtime
commit `9eeaf55` alone also builds with both, and the benchmark source
builds against the pre-derivation `c4ad279` runtime under both compilers):
`make check` and `make check-soak` were re-run on the rewritten tip. The
pre-rewrite `865978c` passed the same suites before its runs. `make check` passed (Farm LDC/DMD, TSan, torture
suites, private suite on LDC/DMD/TSan, doc examples, death tests) and
`make check-soak` passed (78 TSan loops in 121 s). Unit test
`rootCostHintRule` asserts the derived hint, the claim-count guarantee, the
cheapest-class choice, and unchanged class 0 for sharded and
single-consumer tables.

Every run waited for one-minute load ≤ 1 before admission
(`investigation/scripts/gate.sh`); each gate then applied its own initial
load admission and per-run contamination checks. Binaries were removed
after hashing; [binary-sha256.txt](binary-sha256.txt) records them and
[commands.json](commands.json) every command. The `root_cost_hint` field is
recorded per run in each `runs.jsonl`.
