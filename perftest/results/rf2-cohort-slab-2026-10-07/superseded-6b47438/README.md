# Superseded: first RF2 API (candidate 6b47438)

These gates measured the first RF2 candidate. Its history was rewritten, so
6b47438 is not on the branch; `metadata.json` records the hashes. That
candidate carried the slab byte cap as a third `PrivateCohortOptions` field
and gave `create()` an extra defaulted `slabByteLimit` parameter. Both changes
also altered default-path code. The runs are kept because they found the
problem that led to the final API.

- `default-gate`, `boundary-default-*`, `control-boundary-*`: base c3e7521
  versus 6b47438, common benchmark 6b47438. At six consumers every cell
  passed. At zero consumers, `subset-whole` regressed by **-11.4%** and
  `subset-whole-unreserved` by **-10.8%** (20 pairs), while the byte-identical
  base/base control passed on the same cells (+0.2% / -0.6%).
- `runtime-only-boundary-0`: the same two cells with the pre-slab benchmark
  8143463, which never instantiates `cohortSlab`. They still regressed
  (-10.7% / -10.6%). The only difference was 96 bytes of cold `.text`.
- Attribution, in `../README.md#superseded-first-api`: instructions per
  callback were identical (60.6) and IPC fell from 2.98 to 2.68. The unchanged
  Farm hot functions moved: `processShard` from offset 0 to 48 mod 64,
  `writeImpl!(ChildBodies)` from 48 to 32. Experiments showed that the extra
  `create()` parameter accounted for 64 bytes of that `.text`, and the options
  field for none. Reverting both restores `.text` that is identical in size
  and layout.
- `slab-gate`: slab gains on the first API (tree-slab/6 +46%, tree-wait-slab/6
  +51%, tree-refill-slab/6 +30%). Re-measured on the final API in
  `../b-slab`.

`run-rf2-6b47438.sh` is the driver as executed for these runs. The final API
(c7b85b8) moves the cap to a slab-only `createChildren` overload and keeps
`PrivateCohortOptions` and `create()` unchanged; see `../text-identity.txt`.
