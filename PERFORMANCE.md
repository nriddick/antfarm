# Ant Farm performance

Performance numbers for the 1.8.0-rc.1 line are measured at the release
candidate and published here. Until then this page is a stub that lists the
sections to come. It carries no numbers.

## Method

How the committed-ref gate and the benchmark harness work: the modes and their
arguments, the acceptance boundary, the matched-pair and control requirements,
and the code-layout caveat. The harness index is
[perftest/README.md](perftest/README.md); the sizing rules that do not depend
on a host are in [ARCHITECTURE.md](ARCHITECTURE.md#sizing-one-farm) and
[SPEC.md](SPEC.md).

## Current numbers

Throughput, latency, and actor-lifecycle measurements, each with the exact
revision, host, compilers, and allocator it was taken on. Historical
measurements were archived outside the repository and are not reproduced
here; the delivered outcomes and accepted trade-offs are summarized in
[CHANGELOG.md](CHANGELOG.md).

## Tuning guidance

How to choose ring size, segment count, quotas, batch limits, work-cost hints,
and allocator policy from those numbers. The stable formulas and defaults live
in [ARCHITECTURE.md](ARCHITECTURE.md#sizing-one-farm); the actor-facing rules
live in the actor guides under [docs/](docs/README.md).
