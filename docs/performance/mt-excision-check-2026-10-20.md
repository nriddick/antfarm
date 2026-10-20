# MT-excision performance check — October 20, 2026

The apparent fourfold loss between the archived 1.8 measurements and the new
1.9 baseline **does not reproduce in matched core measurements**. More
decisively, the exact saved WSL executable that produced the new baseline's
503.212 Mpps headline later produced **2,183.132 Mpps** (three-run median).
No runtime fix or rebuild was involved in that control.

This rules out a source revision or changed executable as the explanation for
that particular gap. It does not identify the environmental cause. WSL itself
is not a sufficient explanation: both the old and current revisions reach
about 2.1–2.2 billion payloads/s there in this session.

The separate, previously measured **−14.1% serial private-actor regression
remains unresolved**. These core probes neither retest nor clear it.

## Method and scope

Intel i7-12700H, LDC 1.42.0 / LLVM 21.1.8, `-O2 -release`, ordinary pages
(`ANTFARM_HUGE_PAGES=0`). Native Windows and WSL2 Linux 6.6.87.2 were run
sequentially. Workers were unpinned; WSL allowed logical CPUs 0–19. The Windows
power plan was Balanced; battery, clock and thermal history were not captured.

Five independent process samples per cell, with seeded randomized revision
order inside each round. No CPU/load rejection. The common benchmark retains
the same workload, per-worker accounting and command-line options; compile-time
branches adapt the three historical callback/header APIs. Every run checks
completion against the expected payload count. The two mixed callbacks have
distinct addresses in both inspected Linux executables (1.8.3 and current).

This is a short diagnostic screen, not the project's statistical regression
gate. Differences between intermediate commits are leads, not established
causal speedups or regressions. There are no six-consumer actor or tail tests
in this screen; their 30-sample/five-process policies remain unchanged.

| Revision | Change isolated by the sequence |
| --- | --- |
| `207cd94` | 1.8.3 progress; compact and uniform ST tables already available |
| `8cc8044` | MT removal and ST grains; eight-word table header retained |
| `fd6271f` | Five-word header, C body-only callback, release callback guard, shared decoding |
| `743935c` | Current 1.9; core runtime matches `2cf59da` used by the baseline |

After `fd6271f`, executable changes in `antfarm/package.d` through the measured
current revision are the fatal-handler facility. Header geometry and normal
dispatch remain the same; nevertheless, function/data layout can change.
This screen does not attribute intermediate differences to the fatal hook.

Cases use a 16 MiB ring and one internal repetition per process:

| Case | Shape |
| --- | --- |
| Fixed uniform headline | 4 small producers, 8 consumers, 1-word bodies, batch 1700, chunk 128, 2 billion payloads, explicit common callback/length |
| Inferred uniform | 1 bulk producer, 4 consumers, 16-word bodies, batch 80, chunk 16, 100 million payloads, payload-entry sizing |
| Compact | 1 bulk producer, 4 consumers, 1-word bodies, batch 1000, chunk 128, 200 million payloads, alternating callbacks |

The inferred-uniform case uses more payloads than the baseline's 16-million
default-shape case to lengthen each sample. Compare revisions within this
screen. Variable-length uniform tables, actors, tails and grains are outside it.

## Results

All entries are **Mpps, median [minimum, maximum]**, five samples each.

### Native Windows

| Case | 1.8.3 `207cd94` | MT removed `8cc8044` | Five-word `fd6271f` | Current `743935c` |
| --- | ---: | ---: | ---: | ---: |
| Fixed uniform headline | 2,245.247 [2,161.263, 2,267.505] | 2,223.566 [2,161.549, 2,300.650] | 2,174.847 [2,074.300, 2,232.855] | 2,291.323 [2,254.094, 2,341.941] |
| Inferred uniform | 112.497 [105.579, 113.406] | 120.362 [117.492, 123.686] | 119.410 [112.664, 122.650] | 119.073 [110.752, 122.297] |
| Compact | 207.371 [202.438, 211.004] | 308.041 [292.469, 314.381] | 317.397 [310.578, 324.951] | 312.314 [308.937, 320.529] |

### WSL2

| Case | 1.8.3 `207cd94` | MT removed `8cc8044` | Five-word `fd6271f` | Current `743935c` |
| --- | ---: | ---: | ---: | ---: |
| Fixed uniform headline | 2,122.793 [1,908.371, 2,172.880] | 2,127.975 [2,107.567, 2,219.581] | 2,142.305 [2,078.295, 2,218.452] | 2,186.354 [1,876.440, 2,229.962] |
| Inferred uniform | 104.572 [97.288, 106.366] | 124.332 [105.119, 127.625] | 120.075 [106.543, 121.368] | 113.353 [101.832, 118.082] |
| Compact | 197.001 [195.398, 207.455] | 274.419 [250.309, 293.587] | 237.787 [224.942, 252.341] | 243.222 [236.255, 249.199] |

Current medians exceed the 1.8.3 medians in these six cells. That supports
retaining the existing fast paths; it is not a blanket performance guarantee.
The WSL compact case falls from 274.419 after MT removal to 237.787 at the
five-word change, then 243.222 currently. That intermediate drop remains a
useful target for a larger controlled layout/dispatch study, even though the
current result exceeds 1.8.3's 197.001.

## Exact saved-executable control

The original baseline executable was preserved before this investigation:

`d6c92abf705361463043df50f4e7f9dd2ef04cee222f94566f24107f1b1f0c6a`

It was rerun under WSL with the original headline command, alternating with
the matched harness executable. Its three rates were **2,183.132, 2,219.092
and 2,116.758 Mpps**; median **2,183.132**. The original five-run session was
**503.212 [485.906, 520.568]**. Both sessions reported ordinary pages.

The adapted harness is not byte-identical to the saved executable; their code
section sizes also differ slightly. The saved-executable control is therefore
reported independently, rather than treating those builds as an identity
control. No source or binary change is required to reproduce the high rate.

The original low-rate observations remain in [PERFORMANCE](../../PERFORMANCE.md)
and its data. They should not be silently discarded or attributed to MT
removal. They also do not establish representative sustainable throughput for
this host. Battery/power limits, host scheduling, throttling and other load
were not controlled sufficiently to identify a cause. The maintainer also
identified placement on efficiency cores or a reduced power state while away
as possibilities; neither was observed in the recorded telemetry. Future
comparisons should capture AC/battery state and effective clocks, and verify
worker placement where the host permits it. WSL's virtual CPU numbering
alone does not identify physical P/E-core residency.

## Evidence and follow-up

The [data bundle](mt-excision-check-2026-10-20.json) contains all 120 successful
screen samples, raw output, commands, source/binary hashes, the common benchmark
source, platform/compiler metadata, and six exact-binary/control samples.
The local archive `.build/mt-excision-investigation-evidence.zip` also retains
the source snapshots, executables and preparation/measurement scripts.

No production runtime changed. Relevant validation was compilation of all
four revisions on both systems, benchmark completion accounting, and a raw
sample/summary audit. This does not claim the full correctness or performance
gate, which are required for an eventual runtime optimization.

The next runtime investigation remains targeted: table spacing and dispatch
around the five-word change, using serial private-root controls and matched
core cases. Preserve joins, the callback guard's release-build contract, and
the five-word metadata unless measurements and correctness evidence justify
a change.
