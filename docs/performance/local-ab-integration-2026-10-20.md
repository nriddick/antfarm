# Selective local_ab integration — October 20, 2026

Integrated onto `ff79278`, the fetched `origin/1.9.0-progress` head containing
the comparison report, current private C exports and public-actor changes:

- `2277679`: port of `edb665a`, specialized inferred table-admission scans.
  Resolved the conflict with `651030b` by replacing only the inferred-sizing
  block. The resulting `antfarm/package.d` is byte-identical to `edb665a`.
  Existing expanded Farm tests remain byte-identical to `ff79278`.
- `759bfa2c`: port of `4630c4b`, restoring the unpublished root suffix to the
  front of its ready FIFO, with its regression test and ordering documentation.
  This applies to the current typed/erased root implementation, preserving the
  newer actor features and C exports.

The five-word header, explicit-length sizing, publication/claim memory ordering,
C headers/export sources and public-actor implementation are unchanged. SPEC's
scan description now reflects validation hoisting and the compact running sum.

## Final runtime validation

Linux x86-64; LDC 1.43.0 / LLVM 22.1.8 and DMD 2.113.0;
`ANTFARM_HUGE_PAGES=0`. All nine recorded commands exited zero:

- Full expanded Farm suite: LDC `-O2 -release`, DMD `-O -release`.
- Private test builds: LDC debug/release and DMD debug/release.
- Full private suite: LDC debug and release, including FIFO, early completion,
  six-consumer reuse/backpressure, nested waves and concurrent creators.
- `root-admission-order`: DMD debug and release.
- Complete `scripts/check-c-abi.py`: LDC and DMD, including native C/C++,
  private actors/cohorts, release fatal contracts, publication checks and hosted
  runtime integration. Both audit 98 core and 14 runtime exports; core archive
  audits and whole-archive native links pass.

Exact commands, exit codes and logs are in `.build/local-ab-integration/`.
An external copy and transfer bundle are retained in the accompanying Savina
workspace at `results/unified/antfarm-integration/20261020/`.
These are focused final-tree integration checks, not a new full `make check`,
TSan, Windows gate or performance comparison. The reported gains remain the
[prior comparison's measurements](local-ab-comparison-2026-10-20.md), not new
measurements of this integrated tree. No push or benchmark repin was performed.

## Repository location

The original `/home/nathan-riddick/pi_ground/antfarm` checkout is mounted
read-only in this session. It remains on `local_ab` at `05afaaf`; its local
`1.9.0-progress` ref was also at `05afaaf` and has not been changed.
Integration is committed on `1.9.0-progress` in the writable clone
`/home/nathan-riddick/pi_ground/antfarm_savina/.build/antfarm-integration-20261020`.
Fetch that branch or the retained bundle into the original repository when it
is writable. Preserve the old local branch before moving it to the integrated
head; do not merge the older whole local_ab branch into the newer remote tree.
