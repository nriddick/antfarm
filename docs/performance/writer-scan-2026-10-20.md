# Restoring inferred writer sizing — October 20, 2026

`651030b` restores layout-specific sizing scans removed by `fd6271f`.
The affected `write(callback, bodies)` overload infers equal body lengths;
the explicit-length overload already retained arithmetic sizing.
The C API's `af_write_bodies` uses the inferred path too.

This is a confirmed loss in a particular writer path, separate from the
[earlier fourfold environmental slowdown](mt-excision-check-2026-10-20.md).
The earlier headline check supplied a length, so it did not exercise this
scan. Neither finding clears the previous serial private-actor regression:
the actor paths already supply their body lengths.

## Change and contract

The general 1.9 scan recalculated the prospective layout, index count and
record overhead for each entry. The restored scans handle fixed-length
uniform, indexed uniform and mixed-callback prefixes separately. Once a
callback or length changes, scanning resumes at that entry in the next layout.
A broadcast source needs only the two uniform loops.

Every examined candidate still receives callback and singleton-size
validation; sums remain overflow-checked. A rejected larger layout leaves the
accepted prefix's layout intact. Zero-length bodies retain indexes. The
caller range is checkpointed, preparation receives only the accepted prefix,
and a rejected preparation reserves no ring space or completion notification.
Table geometry, publication, joins and memory orderings are unchanged.

New coverage exercises inferred broadcasts over a forward range without a
length property, exact/truncated quotas, a rejected length-changing entry,
empty bodies, and preparation rejection/retry with an exactly-once hook.
Existing tests cover mixed callbacks, wraparound and release-mode failures.

## Focused measurements

Intel i7-12700H under WSL2, LDC 1.42.0 / LLVM 21.1.8, `-O2 -release`,
16 MiB ring, ordinary pages, unpinned workers and no CPU/load filtering.
Five independent processes per revision/case; revision order randomized
inside each round. These are diagnostic throughput measurements, not the
actor regression gate below. Power/frequency and physical P/E residency were
not recorded.

All revisions used the same compatibility benchmark from the preceding
MT-excision investigation. It adapts only callback/header API differences.
The reused baseline executable was built from `743935c`; both compiled
runtime source files are byte-identical to `38118eb`.
The final committed source produces a byte-identical executable to the
measured prototype after a formatting cleanup. All 90 runs verified their
expected completed payload counts.

Rates are median **Mpps**. Change is the restored median divided by the
preceding 1.9 median, minus one; it is not a confidence interval.

| Case | 1.8.3 `207cd94` | Before `38118eb` | Restored `651030b` | Change |
| --- | ---: | ---: | ---: | ---: |
| Inferred common callback, body 1, batch 1700, 1 consumer | 499.176 | 357.531 | 509.199 | +42.4% |
| Inferred common callback, body 1, batch 1700, 4 consumers | 448.092 | 320.651 | 459.631 | +43.3% |
| Inferred common callback, body 16, batch 80, 4 consumers | 134.191 | 121.031 | 141.801 | +17.2% |
| Payload entries, body 1, batch 1700, 4 consumers | 251.418 | 285.961 | 411.940 | +44.1% |
| Mixed callbacks, body 1, batch 1000, 4 consumers | 196.925 | 243.165 | 305.205 | +25.5% |
| Explicit length headline, 4 producers / 8 consumers | 2,162.652 | 2,222.481 | 2,233.380 | +0.5% |

The first two inferred cases were about 28% below 1.8.3 before restoration;
they now recover that throughput. The unchanged explicit-length control
stays close to its previous rate. Do not extrapolate these small-callback
results to application work or actor lifetimes.

The first five cases use one bulk producer. One-word inferred/entry cases
use chunk 128 and 400 million payloads; mixed callbacks use 200 million.
The 16-word case uses chunk 16 and 100 million. The explicit-length headline
uses four small producers, eight consumers, chunk 128, batch 1700 and two
billion payloads. Exact commands, raw samples, min/max and hashes are retained
in the local evidence bundle.

## Validation and actor controls

Linux `make check SHELL=/usr/bin/bash` passed in **1349.44 s**. Its conservatively counted collective sanitizer time was **230.13 s**, within the 600-second budget. Native Windows LDC and DMD release sizing tests and runtime-free C/C++ ABI checks also passed.

The committed-reference performance gate compares `38118eb` with `651030b`
using the same actor benchmark source. It uses 30 matched pairs per cell,
16,384 actors, three seconds per sample, mimalloc 3.5.3, batch 256, ordinary
pages, and controller/consumer guest CPUs `0,2,4,6,8,10,12`. These are WSL
virtual CPU IDs, not verified physical P-core placement. CPU/load telemetry is
retained without exclusions; initial-load admission is effectively disabled.
Timing ran after correctness work completed.

| Actor mode | Consumers | Accepted pairs | Paired median change | Descriptive bootstrap interval | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| private | 0 | 30 | +0.61% | [-0.76%, +2.98%] | pass |
| wave | 0 | 30 | +0.45% | [-1.02%, +1.23%] | pass |
| private | 6 | 30 | -0.71% | [-1.22%, +0.92%] | pass |
| wave | 6 | 30 | +0.23% | [-0.64%, +0.53%] | pass |

Overall gate exit status: **0** (0 pass, 1 regression, 2 inconclusive/error). The two actor executables are byte-identical; these cells are also an identity control, consistent with actors using the unchanged explicit-length path.

This is a comparison against the immediately preceding 1.9 revision, not a
retest of the older 1.8.2 private-actor loss. No tail-latency or complete
PERFORMANCE refresh is claimed.

The evidence archive is `.build/writer-scan-evidence.zip`, with a SHA256
sidecar. It contains the 90 core samples and source/binary provenance, final
binary identity check, correctness logs and sanitizer accounting, native
Windows checks, and the complete actor gate. Gate JSON and binaries remain
local in accordance with the performance-gate evidence policy.
