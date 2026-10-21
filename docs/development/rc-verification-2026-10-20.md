# 1.9.0 release-candidate verification on the i7-12700H

The tested runtime is `f3459f8` on `1.9.0-progress`, fetched before this pass.
No runtime implementation change was needed during verification. Corrections
affect benchmark builds and report presentation only. Measurements on this host
were stopped at the user's request; [PERFORMANCE.md](../../PERFORMANCE.md)
remains unchanged and no new performance baseline is published.

## Results

| Check | Environment | Result |
| --- | --- | --- |
| `make check SHELL=/usr/bin/bash` | WSL2 Linux x86-64; LDC 1.42.0 / LLVM 21.1.8 and DMD 2.112.1 | PASS, `ALL CHECKS PASSED` |
| `python -B scripts/check-windows.py` | Native Windows 11 x64; LDC 1.42.0 and DMD 2.112.1 | PASS, `WINDOWS CHECKS PASSED`; no hardware-skip override |
| Portable documentation, Windows-runner and performance-tool tests | Native Windows Python | PASS, 227 tests, five skips |
| Native performance harness build | LDC `-O2 -release`, static release CRT | PASS, all nine harnesses |
| C/D lifecycle benchmark build and allocation accounting | Linux C11 and LDC BetterC; CRT and mimalloc 3.5.3 | PASS, all 48 scalar/batch accounting cases |

The Linux gate includes native C/C++ ABI checks with both D compilers, the
runtime-free link/symbol checks, the runtime-inclusive host, ThreadSanitizer
lanes, Farm ordering and parallel-loop tests, public/private actor suites,
fatal-contract checks that survive release builds, extracted documentation
programs, and examples. The root-slab and FIFO admission checks passed under
LDC, DMD and ThreadSanitizer, including partial/zero admission, immediate
completion, reuse, tracking, destructors and allocation failure.

The native Windows matrix covers the C/C++ ABI, packages, live affinity and
worker confinement, wait races, host lifecycle, Farm ordering, torture suites,
private actors, extracted programs and examples. Its declared exclusions are
TSan, POSIX fork/death probes, the Life terminal UI, real mimalloc integration
lanes and timing gates. Linux supplies the TSan and death-test coverage.
The separately built native benchmarks use real mimalloc and retain their
release-build result checks.

This host has one LLC and one Windows processor group. Multi-LLC Fiber
scenarios explicitly skip; this pass does not establish live multi-LLC or
multi-group coverage. No extra soak or historical mutation campaign was run.

## Corrections

1. The Windows benchmark build placed `tools/perf` ahead of the Fiber library
   import directory. Its `ant_fibers/actor_wave.d` benchmark shadowed the
   library module and prevented `scaling` from compiling. Library directories
   now precede the benchmark directory; the complete native build passed.
2. The C/D lifecycle benchmark still aggregate-initialized the old actor-kind
   options layout. It now calls `af_actor_kind_options_default` and sets the
   handler, byte size and alignment explicitly. The corrected benchmark builds
   with warnings as errors. Its 48 counted cases verify one final-state
   allocation/free and exactly the requested bytes per actor, across D scalar,
   C scalar and C batch forms, 64/4096-byte states, one/four creators, local/remote
   reclamation and CRT/mimalloc.
3. Generated Markdown exposed the multithreaded Fiber residual drain rate as
   throughput even though workers execute during creation. Tables now use the
   complete warm burst, keep residual fields only in diagnostic data, and label
   the extra controller correctly. A regression test verifies the presentation.

## Run integrity and retained evidence

The first Linux attempt stopped on a missing `actor.o`: an additional benchmark
build launched alongside it reused temporary object names. This was a validation
setup collision. All builds were then serialized and the full Linux gate rerun
successfully; the failed log is retained separately. Initial sandboxed Python
probes could not create temporary subdirectories or terminate a test child.
The successful native pass used a workspace temporary directory and normal
Windows process access, including the timeout-cleanup probe.

Verification and benchmark builds finished before timing started. Timing was
stopped before the full matrix completed. The intended sampling was 30 processes
for six-consumer actor cases and five tail processes with 10,000 observations
per scene; this pass does not establish those completed datasets. Raw process
outputs are retained without a background CPU filter, alongside compiler,
topology, power-plan, source revision and native binary metadata.

Diagnostic controls showed large throughput variation in both the candidate
and an unchanged saved benchmark binary whose hash matched the earlier run.
Restricting execution to performance cores and explicitly disabling process
execution-speed throttling did not establish a stable baseline or isolate the
cause. These incomplete measurements are diagnostic evidence only, not a
release performance verdict.

Local evidence is retained under `.build/rc-12700h/`: `linux-check.log` and its
exit record, `windows-check.log`, `native-checks.json`, `portable-tests.log`,
native build logs, the C/D benchmark and `allocation-checks.json`, the native
CPU-set snapshot, measurement manifest, raw outputs, summaries and report
generation inputs. These ignored build artifacts are not repository sources.
