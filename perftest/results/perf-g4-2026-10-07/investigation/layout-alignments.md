# Binary layout evidence for the G4 /0 regressions

All binaries built with the gate's LDC command. `base` is c4ad279. The
candidate columns use the G4 benchmark source pinned by each gate's own run
(48e22eb for the first candidate/inert pair, edcbe5a for the exact-twin
pair, both functional only in their reported hint field). The inert
candidates are c4ad279 plus never-executed text; diffs in
[inert-control.patch](inert-control.patch) and
[inert-exact-control.patch](inert-exact-control.patch).

## Offset structure (corrected)

The earlier "symbols move uniformly by +2080" summary was measured only on
six hot functions. The full histogram of text-symbol address deltas
(candidate edcbe5a-benchmark build vs base with the same benchmark) is
two-region: the six hot public functions shift +2080, while the bulk
(1581/1903) shifts +2176, 168 shift +2192, 35 shift +2080 and 109 (emitted
before every insertion point) do not move. The candidate additionally
carries the derivation inlined into benchmark wrapper code (`_Dmain` is
128 bytes larger than base), which a semantically null twin cannot
reproduce: padding the twin's body flips LDC's inline threshold and moves
whole regions by 160–480 bytes non-monotonically, so the bulk offset is
not exactly matchable by inert means.

## Hot public functions (address mod 64 and delta vs base)

| function | base | candidate (48e22eb bench) | inert (first, probe blob) | exact twin (edcbe5a bench) |
| --- | ---: | ---: | ---: | ---: |
| ActorRuntime.createActorErased | 48 | 16 (+2080) | 16 (+2144) | 16 (+2080) |
| ActorRuntime.flush | 32 | 0 (+2080) | 0 (+2144) | 0 (+2080) |
| reclaimActor | 48 | 16 (+2080) | 16 (+2144) | 16 (+2080) |
| ConsumerView.consumeNext | 16 | 48 (+2080) | 48 (+2144) | 48 (+2080) |
| ConsumerView.processShard | 32 | 0 (+2080) | 0 (+2144) | 0 (+2080) |
| ConsumerView.sweepCurrentPosition | 0 | 32 (+2080) | 32 (+2144) | 32 (+2080) |

The **exact twin** ([inert-exact-control.patch](inert-exact-control.patch))
reproduces the candidate's +2080 offsets and 64-byte alignments on all six
hot public functions exactly; its bulk region sits 128 bytes short of the
candidate's (+2048 vs +2176) for the reason above. The first inert control
reproduced the alignments with a +2144 shift. 1794/1903 text symbols move
in the candidate; 1699 move in the exact twin.
