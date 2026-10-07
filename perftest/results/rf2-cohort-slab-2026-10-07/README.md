# RF2: opt-in typed cohort slab (2026-10-07)

`PrivateChildPark!(T, R, PrivateStateStorage.cohortSlab)` reserves one zeroed
state slab per child cohort, under an explicit `slabByteLimit` given to its own
`createChildren` overload. Slots are reused after the retiring join, and the
slab is freed once at disposal. The design and lifetime argument are in
[PRIVATE_ACTORS.md](../../../actors/PRIVATE_ACTORS.md#cohort-state-storage)
and [ACTOR_MEMORYORDER.md](../../../actors/ACTOR_MEMORYORDER.md#private-cohort-slab-reuse).
The policy is opt-in, and the default stays `perObject`.

## Summary

- **The default path is unchanged and passes.** Code that never instantiates
  `cohortSlab` compiles to `.text` identical in size and layout to the base;
  the normalized disassembly differs on 0 lines ([text-identity.txt](text-identity.txt)).
  The pass/fail gate (a) passes every one of 34 cells under the -3% rule:
  5 pairs, plus 20 pairs and byte-identical controls for each inconclusive
  cell.
- **Slab gains (gate (b), slab versus per-object):**
  - Six consumers: tree **+50.7%**, tree-wait **+48.6%**, refill/reuse
    **+32.3%**. tree-cont **-1.0%** is no gain; its 8-leaf cohorts keep
    per-cohort metadata frees.
  - Zero consumers: +10.8% to +26.0%.
- **Cross-thread frees at six consumers:**
  - tree and tree-wait: 0.85 → 0.0096 per leaf (-98.9%).
  - refill: 0.79 → 0.0012 per leaf (-99.8%).
  - tree-cont: 1.28 → 0.44 per leaf (-65.6%).
  - Per-object tree spends **64%** of user cycles in mimalloc, 53% in
    `mi_free_size_aligned`. With the slab, no mimalloc symbol reaches 0.5%.
- **Peak RSS** (VmHWM) is unchanged at this scale. Every cell sits at 18–22
  MiB on both sides; the 20/22 MiB alternation appears in base and candidate
  alike.
- **Correctness:** `make check` and `make check-soak` pass at c7b85b8. The
  611-second private TSan soak (391 suite loops, no failures) ran at the
  superseded commit 6b47438, before the cap moved into the slab-only
  `createChildren` overload. The runtime diff since then is only that
  overload/constraint move. Independent verification at tip 107508e ran
  `make check-soak` (78 loops in 122 s) plus a further 231-second,
  148-loop private TSan soak, with zero races; see
  [correctness.txt](correctness.txt). Every commit builds with LDC and DMD. 24 hand-verified mutations are listed in
  [private_actor_tests/README.md](../../../private_actor_tests/README.md#cohort-slab-storage).
  The independent consultant found the reuse and lifetime argument sound with
  no must-fix items (`/home/nathan-riddick/pi_ground/.cydo/tasks/271/output.md`).
  See [correctness.txt](correctness.txt).
- **Gate (c), harness layout, is NOT a waiver.** The common new benchmark
  links slab-mode instantiations into the candidate binary only, because the
  base runtime falls back to per-object for those modes. That moves unchanged
  code. Its one near-threshold cell, private-remote/6 at -2.58% over 20
  pairs, follows `mi_free_size_aligned` moving from offset 32 to 16 mod 64.
  The same runtime with identical `.text` measured +0.10% in gate (a). The
  default path is proven by (a). The asymmetry disappears in future gates
  once both sides contain the slab code.

## Protocol

Host: Ryzen 5 5500, LDC 1.43.0 `-O2 -release`, pinned mimalloc 3.5.3,
ordinary pages, 16,384 actors, batch/quota 256/16,384, five warmups,
3-second windows, controller plus six consumers on CPUs 0..6. Everything ran
serially on a quiet host, after the correctness checks. The driver is
[run-rf2.sh](run-rf2.sh); tables come from `summarize.py`.

| Gate | Base | Candidate | Benchmark | Role |
| --- | --- | --- | --- | --- |
| (a) `a-*` | c3e7521 | c7b85b8 | 8143463 (pre-slab; never instantiates `cohortSlab`) | **pass/fail default path** |
| (b) `b-slab` | c3e7521 | c7b85b8 | c7b85b8 | slab gains: `*-slab` modes run per-object on the base runtime |
| (c) `c-*` | c3e7521 | c7b85b8 | c7b85b8 | harness-layout evidence only |

Candidate c7b85b8 contains the runtime (2b02865), tests (1a6b077) and
benchmark modes. Test-only accessors compile out of these release builds.
`superseded-6b47438/` holds the gates on the first API, before the cap moved;
see [below](#superseded-first-api).

## (a) Default path: runtime only, pass/fail

[text-identity.txt](text-identity.txt) builds base and candidate with
perf_gate's exact command and benchmark 8143463. `.text` is 788,369 bytes in
both. `processShard` sits at offset 0 mod 64 and `writeImpl!(ChildBodies)` at
48 in both. The normalized instruction streams are identical. The only
differences are rip-relative targets into `.rodata`, which grew by 32 bytes
of longer TypeInfo names placed after `.text`. The gate rebuilt binaries with
exactly those hashes (`b8d69f17…`, `696bab1c…`).

#### a-default-runtime

exit=2 candidate_env=(none) base=c3e7521 candidate=c7b85b8 repeats=5

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| actor/0 | pass | -0.21% | -1.06..-0.01% | 18.768 | 18.705 | 18032 | 18156 |
| wave/0 | pass | -0.23% | -0.27..+0.75% | 23.922 | 23.926 | 18180 | 18112 |
| inbox/0 | pass | -0.15% | -0.90..+0.05% | 15.160 | 15.143 | 20188 | 20244 |
| private/0 | inconclusive | -0.16% | -21.87..+1.00% | 31.697 | 31.711 | 18088 | 18112 |
| private-remote/0 | pass | +0.50% | -0.14..+1.12% | 31.246 | 31.460 | 18032 | 18160 |
| tree/0 | pass | +0.01% | -1.44..+3.20% | 63.630 | 63.634 | 18196 | 18164 |
| tree-wait/0 | inconclusive | -1.51% | -9.18..+3.63% | 63.615 | 62.433 | 18076 | 18072 |
| tree-cont/0 | pass | -0.03% | -0.31..+0.62% | 26.106 | 26.103 | 17984 | 17972 |
| tree-pipelined/0 | pass | +0.08% | -1.69..+0.47% | 27.549 | 27.571 | 18120 | 18160 |
| dtor/0 | pass | +0.76% | -0.74..+4.60% | 31.157 | 31.216 | 18096 | 18064 |
| subset/1%/0 | inconclusive | -1.01% | -3.90..+2.91% | 106.727 | 105.727 | 17996 | 18052 |
| subset/10%/0 | pass | +0.22% | -1.90..+1.34% | 129.093 | 129.382 | 18160 | 18132 |
| subset/100%/0 | pass | +0.26% | -0.75..+0.46% | 133.057 | 133.226 | 18084 | 17980 |
| subset-whole/0 | pass | +0.35% | -1.06..+0.86% | 208.849 | 208.446 | 18088 | 18028 |
| subset-whole-unreserved/0 | inconclusive | -0.44% | -3.41..-0.40% | 208.703 | 207.622 | 16060 | 16084 |
| tree-cancel/0 | pass | -0.17% | -2.33..+0.36% | 53.365 | 53.117 | 18116 | 18112 |
| private-tracked/0 | pass | -0.13% | -0.74..+1.17% | 13.578 | 13.573 | 19980 | 20224 |
| actor/6 | pass | -0.10% | -0.74..+0.65% | 15.053 | 15.050 | 18104 | 18116 |
| wave/6 | pass | -0.01% | -0.41..+0.37% | 15.962 | 15.961 | 18092 | 18052 |
| inbox/6 | pass | +0.13% | +0.02..+0.41% | 12.578 | 12.604 | 20168 | 20148 |
| private/6 | pass | -0.29% | -1.17..+0.46% | 22.829 | 22.854 | 18268 | 18196 |
| private-remote/6 | pass | +0.10% | -1.11..+0.29% | 14.544 | 14.547 | 18196 | 18124 |
| tree/6 | pass | -0.04% | -0.73..+0.56% | 54.292 | 54.388 | 22244 | 22116 |
| tree-wait/6 | pass | +0.06% | -0.73..+0.52% | 55.125 | 55.180 | 20264 | 20172 |
| tree-cont/6 | pass | -0.41% | -0.49..-0.34% | 31.703 | 31.562 | 22212 | 22252 |
| tree-pipelined/6 | pass | +0.17% | -0.16..+3.16% | 31.668 | 31.758 | 22216 | 22264 |
| dtor/6 | pass | -0.10% | -0.19..+0.39% | 22.922 | 22.905 | 18252 | 18188 |
| subset/1%/6 | pass | -0.30% | -0.40..+0.06% | 42.267 | 42.169 | 18308 | 18220 |
| subset/10%/6 | inconclusive | -0.34% | -6.58..+2.02% | 84.553 | 82.690 | 18272 | 18192 |
| subset/100%/6 | pass | +0.36% | -1.04..+1.18% | 93.355 | 93.393 | 18116 | 18140 |
| subset-whole/6 | pass | +0.03% | -0.21..+0.21% | 103.022 | 103.091 | 18224 | 18296 |
| subset-whole-unreserved/6 | pass | -0.04% | -0.20..+0.13% | 103.181 | 103.101 | 16224 | 16148 |
| tree-cancel/6 | pass | -0.42% | -0.46..+0.17% | 67.112 | 66.985 | 22172 | 20208 |
| private-tracked/6 | pass | -0.17% | -1.04..+0.67% | 13.178 | 13.148 | 20256 | 20248 |


The five-pair run left five cells inconclusive: private/0, tree-wait/0,
subset/1%/0, subset-whole-unreserved/0 and subset/10%/6, each from one or
two outlier pairs. As required, the 20-pair rerun includes those cells and
subset-whole/0; the subset mode adds its 10% and 100% cells at zero consumers.
Byte-identical controls ran on the same cells.

#### a-boundary-0

exit=0 candidate_env=(none) base=c3e7521 candidate=c7b85b8 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| private/0 | pass | +0.04% | -21.79..+27.45% | 31.899 | 31.900 | 18110.0 | 18076.0 |
| tree-wait/0 | pass | +1.89% | -7.57..+9.76% | 62.847 | 63.768 | 18072.0 | 18030.0 |
| subset/1%/0 | pass | +1.04% | -1.19..+6.11% | 105.972 | 106.894 | 18078.0 | 18116.0 |
| subset/10%/0 | pass | +0.31% | -3.80..+4.32% | 128.933 | 129.633 | 18034.0 | 18104.0 |
| subset/100%/0 | pass | +1.14% | -1.18..+5.75% | 132.274 | 134.002 | 18054.0 | 18012.0 |
| subset-whole/0 | pass | +1.13% | -11.73..+14.25% | 205.927 | 207.497 | 18096.0 | 18116.0 |
| subset-whole-unreserved/0 | pass | +0.92% | -9.34..+15.22% | 205.172 | 207.609 | 16014.0 | 16054.0 |

#### a-boundary-6

exit=0 candidate_env=(none) base=c3e7521 candidate=c7b85b8 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| subset/10%/6 | pass | +0.05% | -5.08..+5.41% | 80.636 | 80.849 | 18186.0 | 18190.0 |

#### a-control-0

exit=0 candidate_env=(none) base=c3e7521 candidate=c3e7521 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| private/0 | pass | +1.68% | -0.63..+4.84% | 31.578 | 32.041 | 18072.0 | 18100.0 |
| tree-wait/0 | pass | -0.26% | -4.67..+14.43% | 63.671 | 63.374 | 18078.0 | 18038.0 |
| subset/1%/0 | pass | -0.00% | -3.57..+1.70% | 106.728 | 106.692 | 18126.0 | 18146.0 |
| subset/10%/0 | pass | +0.39% | -2.92..+7.17% | 129.512 | 129.261 | 18094.0 | 18106.0 |
| subset/100%/0 | pass | +0.46% | -2.95..+3.16% | 132.092 | 133.131 | 18052.0 | 18056.0 |
| subset-whole/0 | pass | +0.71% | -3.77..+12.85% | 207.005 | 208.321 | 18110.0 | 18062.0 |
| subset-whole-unreserved/0 | pass | +0.68% | -11.43..+14.10% | 207.954 | 208.834 | 16008.0 | 16040.0 |

#### a-control-6

exit=0 candidate_env=(none) base=c3e7521 candidate=c3e7521 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| subset/10%/6 | pass | -0.12% | -7.50..+1.10% | 80.323 | 80.161 | 18198.0 | 18206.0 |


**Verdict: pass.** Every cell passes under the -3% rule, and every control
passes.

## (b) Slab gains

#### b-slab

exit=0 candidate_env=(none) base=c3e7521 candidate=c7b85b8 repeats=5

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tree-slab/0 | pass | +21.36% | +7.53..+33.86% | 63.422 | 75.681 | 18164 | 18060 |
| tree-wait-slab/0 | pass | +21.79% | +17.23..+23.65% | 62.696 | 75.617 | 18112 | 18060 |
| tree-cont-slab/0 | pass | +10.81% | +8.04..+12.45% | 26.108 | 28.929 | 18100 | 18192 |
| tree-refill-slab/0 | pass | +25.96% | +23.76..+27.79% | 67.159 | 84.712 | 18100 | 18216 |
| tree-refill/0 | pass | +1.30% | +0.83..+2.57% | 67.000 | 68.035 | 18208 | 18168 |
| tree-slab/6 | pass | +50.70% | +49.52..+57.35% | 55.405 | 83.458 | 22256 | 20304 |
| tree-wait-slab/6 | pass | +48.63% | +47.71..+53.74% | 54.647 | 81.097 | 20368 | 20336 |
| tree-cont-slab/6 | pass | -1.03% | -1.43..+1.92% | 31.439 | 31.109 | 22264 | 22268 |
| tree-refill-slab/6 | pass | +32.32% | +30.20..+32.81% | 68.198 | 90.215 | 22192 | 20312 |
| tree-refill/6 | pass | -0.01% | -0.33..+2.47% | 68.232 | 68.278 | 22372 | 22136 |


`tree-refill` runs per-object on both sides, as an in-gate control: +1.30%
at /0 and -0.01% at /6. Runs report `storage=cohortSlab` for the 40
candidate slab-mode runs and `storage=perObject` for all base runs.

tree-cont gains at zero consumers (+10.8%) but not at six (-1.0%). Its
leaves form 8-member cohorts, so per-cohort metadata (park, Entry array,
results and now the slab) is allocated per 8 leaves. Those metadata frees
stay cross-thread (table below). Its six-consumer rate is not limited by
per-leaf frees.

### Cross-thread frees ([remote-counts.json](remote-counts.json))

The diagnostic `mimalloc-count` allocator records the allocating thread in a
prefix and counts frees issued by another thread. One 2-second run per mode
on the gate's candidate binary; not a timing measurement. These are
antfarm-level cross-thread frees. mimalloc may still free locally or remotely
depending on page ownership. At zero consumers every count is 0.

| Workload (6 consumers) | per-object allocs/leaf | slab allocs/leaf | per-object cross-thread frees/leaf | slab cross-thread frees/leaf | reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| tree vs tree-slab | 1.0117 | 0.0156 | 0.8573 | 0.0096 | 98.9% |
| tree-wait vs tree-wait-slab | 1.0117 | 0.0156 | 0.8492 | 0.0096 | 98.9% |
| tree-cont vs tree-cont-slab | 1.5156 | 0.5195 | 1.2767 | 0.4388 | 65.6% |
| tree-refill vs tree-refill-slab | 1.0015 | 0.0020 | 0.7875 | 0.0012 | 99.8% |
| private (reference) | 1.0000 | - | 0.0000 | - | - |
| private-remote (reference) | 1.0000 | - | 1.0000 | - | - |

### Profiles ([profile.sh](profile.sh), `profile-*-6.symbols.txt`)

User-cycle profiles, six consumers, gate candidate binary:

| Mode | mimalloc symbols ≥0.5% | Top symbol |
| --- | ---: | --- |
| tree | 64.2% | 53.3% `mi_free_size_aligned` |
| tree-slab | 0.0% | 22.8% `Consumer.run` (consumer idle polling) |
| tree-wait | 64.1% | 53.4% `mi_free_size_aligned` |
| tree-wait-slab | 0.0% | 23.6% `Consumer.run` |
| tree-refill | 65.9% | 54.3% `mi_free_size_aligned` |
| tree-refill-slab | 0.0% | 21.7% `Consumer.run` |

With slabs, consumers wait for work. The tree workloads become limited by
the parents' spawn and publication, not by allocator traffic.

### Memory

Peak RSS medians are in the tables: 18–22 MiB, unchanged. Each slab reserves
exactly `capacity * T.sizeof` (here 256 × 16 B = 4 KiB per cohort; one slab
per live parent). These benchmark cohorts are always full, so they do not
measure the sparse-cohort cost documented in PRIVATE_ACTORS.md.

## (c) Common benchmark: harness-layout evidence, NOT a waiver

#### c-default-common-benchmark

exit=2 candidate_env=(none) base=c3e7521 candidate=c7b85b8 repeats=5

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| actor/0 | pass | -0.04% | -0.35..+0.58% | 18.614 | 18.611 | 18124 | 18136 |
| wave/0 | pass | -0.04% | -0.97..+1.15% | 23.804 | 23.736 | 18160 | 18132 |
| inbox/0 | inconclusive | -0.64% | -3.12..-0.19% | 15.054 | 14.972 | 20236 | 20092 |
| private/0 | pass | -1.60% | -2.06..-0.65% | 31.814 | 31.309 | 18108 | 18064 |
| private-remote/0 | pass | -0.45% | -1.65..-0.25% | 31.081 | 30.938 | 18052 | 18060 |
| tree/0 | pass | -0.34% | -1.51..+4.92% | 61.301 | 61.545 | 18140 | 18004 |
| tree-wait/0 | inconclusive | -1.91% | -4.42..+6.33% | 62.050 | 62.996 | 18140 | 18080 |
| tree-cont/0 | pass | -0.60% | -1.04..+11.74% | 26.124 | 25.910 | 18152 | 18132 |
| tree-pipelined/0 | pass | -0.53% | -1.09..-0.31% | 27.437 | 27.283 | 18152 | 18112 |
| dtor/0 | pass | +0.29% | -1.27..+0.71% | 30.866 | 30.975 | 17904 | 18056 |
| subset/1%/0 | pass | -1.43% | -1.93..-0.53% | 107.009 | 105.640 | 18016 | 18080 |
| subset/10%/0 | pass | +2.69% | +1.99..+10.22% | 124.050 | 129.220 | 18072 | 18148 |
| subset/100%/0 | pass | +10.17% | +6.53..+10.43% | 120.254 | 132.216 | 18052 | 18056 |
| subset-whole/0 | inconclusive | -0.05% | -10.90..+15.92% | 205.310 | 206.567 | 18232 | 18120 |
| subset-whole-unreserved/0 | pass | -0.62% | -1.37..+2.30% | 202.981 | 204.022 | 16008 | 16048 |
| tree-cancel/0 | pass | -0.65% | -0.96..-0.24% | 53.187 | 52.844 | 18036 | 18188 |
| private-tracked/0 | pass | +0.02% | -0.70..+0.48% | 13.472 | 13.470 | 20296 | 20172 |
| actor/6 | pass | +0.38% | -0.07..+1.19% | 14.895 | 14.953 | 18140 | 18184 |
| wave/6 | pass | +1.08% | +0.72..+1.33% | 15.753 | 15.940 | 18264 | 18068 |
| inbox/6 | pass | +0.64% | +0.55..+0.92% | 12.480 | 12.587 | 20124 | 20188 |
| private/6 | pass | -0.35% | -0.94..+0.61% | 22.849 | 22.725 | 18328 | 18272 |
| private-remote/6 | inconclusive | -2.96% | -3.23..-2.62% | 14.661 | 14.221 | 18152 | 18280 |
| tree/6 | pass | -1.20% | -1.47..-0.18% | 55.082 | 54.422 | 22348 | 22192 |
| tree-wait/6 | pass | -0.79% | -1.84..-0.62% | 55.646 | 55.152 | 20148 | 22112 |
| tree-cont/6 | pass | +1.58% | +1.38..+3.74% | 30.744 | 31.261 | 22284 | 22168 |
| tree-pipelined/6 | pass | -1.58% | -1.88..-1.00% | 31.456 | 30.938 | 22256 | 22272 |
| dtor/6 | pass | -0.32% | -0.74..+0.25% | 22.783 | 22.709 | 18244 | 18224 |
| subset/1%/6 | pass | -0.80% | -1.32..-0.52% | 42.754 | 42.425 | 18328 | 18252 |
| subset/10%/6 | pass | -0.06% | -1.32..+1.91% | 80.062 | 79.715 | 18148 | 18260 |
| subset/100%/6 | pass | -0.33% | -1.65..+0.35% | 87.445 | 87.375 | 18232 | 18252 |
| subset-whole/6 | pass | -0.43% | -0.92..-0.36% | 103.020 | 102.606 | 18288 | 18228 |
| subset-whole-unreserved/6 | pass | -0.38% | -0.39..-0.24% | 102.859 | 102.510 | 16248 | 16088 |
| tree-cancel/6 | pass | +0.40% | +0.01..+0.70% | 66.484 | 66.751 | 20172 | 22244 |
| private-tracked/6 | pass | +0.16% | -0.01..+0.23% | 13.086 | 13.107 | 20288 | 20284 |

#### c-boundary-6

exit=0 candidate_env=(none) base=c3e7521 candidate=c7b85b8 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| private-remote/6 | pass | -2.58% | -2.81..-2.26% | 14.734 | 14.362 | 18260.0 | 18224.0 |


The common benchmark contains the slab modes. On the base runtime they
resolve to per-object instantiations; on the candidate they instantiate
`cohortSlab`. The candidate binary therefore has 6,016 more bytes of `.text`
(856,657 versus 850,641), all in benchmark slab-mode code. That moves
unchanged code that follows it ([layout-evidence.txt](layout-evidence.txt)):

- The inconclusive zero-consumer cells tree-wait, inbox and subset-whole
  execute identical instructions per unit in both binaries (212.59, 778.04
  and 60.56). Their IPC is equivalent, so they are noise.
- private-remote/6 is dominated (57–58%) by the byte-identical
  `mi_free_size_aligned`. That function moved from offset 32 to 16 mod 64.
  It measured -2.96% (5 pairs) and -2.58% (20 pairs, all pairs -2.26% to
  -2.81%). The same runtime with identical `.text` measured +0.10% in
  gate (a).

**This is not a waiver.** The default path is proven by gate (a):
applications that do not instantiate `cohortSlab` get `.text`-identical code.
The asymmetry in (c) exists only because the base falls back to per-object for
the slab modes. It disappears in future gates once both sides contain the slab
code.

## Superseded first API

The first candidate (6b47438) carried the cap as a third
`PrivateCohortOptions` field and gave `create()` an extra defaulted
parameter. Its default gate regressed subset-whole/0 by -11.4% and
subset-whole-unreserved/0 by -10.8% (20 pairs), with byte-identical controls
passing. With the pre-slab benchmark the regression remained (-10.7% /
-10.6%), from 96 bytes of cold `.text`.

Instructions per callback were identical (60.6) while IPC fell from 2.98 to
2.68. The unchanged Farm hot functions had moved: `processShard` from offset
0 to 48 mod 64 and `writeImpl!(ChildBodies)` from 48 to 32. Experiments traced
64 of those bytes to the extra `create()` parameter. The orchestrator approved
moving the cap to a slab-only overload and keeping both `create()` signatures
and `PrivateCohortOptions` unchanged. History was rewritten and the gates
rerun. Raw evidence is in [superseded-6b47438/](superseded-6b47438/README.md).
