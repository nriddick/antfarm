#!/usr/bin/env bash
# RF2 cohort slab timing, as executed (serially, quiet host, after make check).
# Base c3e7521 (per-object runtime) vs candidate 6b47438 (slab runtime), common
# benchmark 6b47438. On the base runtime the *-slab modes fall back to
# per-object cohorts (storage=perObject), so slab cells compare per-object
# (base) with slab (candidate) storage on one harness.
set -u
cd "$(dirname "$0")/../../.."
R=perftest/results/rf2-cohort-slab-2026-10-07
waitquiet() { while python3 -c "import os,sys; sys.exit(0 if os.getloadavg()[0] < 0.8 else 1)"; [ $? -ne 0 ]; do sleep 20; done; }
gate() { label=$1; shift; waitquiet; echo "=== $label $(date -Is)"; python3 perftest/perf_gate.py --repo . --benchmark-ref 6b47438 --output $R/$label "$@" > $R/$label.console.log 2>&1; echo "exit $? $(date -Is)"; }
# A: default (perObject) path, every default mode, -3% rule.
gate default-gate --base c3e7521 --candidate 6b47438
# B: slab modes vs per-object on the base runtime; tree-refill is per-object on
# both sides (an in-gate control).
gate slab-gate --base c3e7521 --candidate 6b47438 --modes tree-slab,tree-wait-slab,tree-cont-slab,tree-refill-slab,tree-refill
# Resolve default-gate cells that were inconclusive at five pairs: 20-pair
# boundary runs plus byte-identical base/base controls on the same cells.
gate boundary-default-0 --base c3e7521 --candidate 6b47438 --boundary --modes tree,tree-wait,subset,subset-whole,subset-whole-unreserved --consumers 0
gate boundary-default-6 --base c3e7521 --candidate 6b47438 --boundary --modes private --consumers 6
gate control-boundary-0 --base c3e7521 --control --boundary --modes tree,tree-wait,subset,subset-whole,subset-whole-unreserved --consumers 0
gate control-boundary-6 --base c3e7521 --control --boundary --modes private --consumers 6
# Attribution: the runtime change alone (old benchmark 8143463, which never
# instantiates cohortSlab) on the two regressing cells.
gate runtime-only-boundary-0 --benchmark-ref 8143463 --base c3e7521 --candidate 6b47438 --boundary --modes subset-whole,subset-whole-unreserved --consumers 0
