#!/usr/bin/env bash
# RF2 cohort slab timing, as executed (serially, quiet host, after make check,
# check-soak and the per-commit builds). Base c3e7521; candidate c7b85b8 (RF2
# runtime + tests + benchmark modes). See README.md for the protocol; the
# superseded-6b47438/ runs measured the pre-fix API.
set -u
cd "$(dirname "$0")/../../.."
R=perftest/results/rf2-cohort-slab-2026-10-07
waitquiet() { while python3 -c "import os,sys; sys.exit(0 if os.getloadavg()[0] < 0.8 else 1)"; [ $? -ne 0 ]; do sleep 20; done; }
gate() { label=$1; shift; waitquiet; echo "=== $label $(date -Is)"; python3 perftest/perf_gate.py --repo . --output $R/$label "$@" > $R/$label.console.log 2>&1; echo "exit $? $(date -Is)"; }
step=${1:-all}

# (a) Pass/fail default-path gate: runtime only, pre-slab benchmark 8143463,
# which never instantiates cohortSlab (precondition: text-identity.txt).
if [[ $step == all || $step == a ]]; then
gate a-default-runtime --benchmark-ref 8143463 --base c3e7521 --candidate c7b85b8
fi
# (a) 20 pairs plus byte-identical controls for near-boundary/inconclusive
# cells; always includes tree-wait/0, subset/1%/0, subset-whole/0 and
# subset-whole-unreserved/0.
# The five-pair run left private/0, tree-wait/0, subset/1%/0,
# subset-whole-unreserved/0 and subset/10%/6 inconclusive.
if [[ $step == all || $step == a-boundary ]]; then
gate a-boundary-0 --benchmark-ref 8143463 --base c3e7521 --candidate c7b85b8 --boundary --modes private,tree-wait,subset,subset-whole,subset-whole-unreserved --consumers 0
gate a-boundary-6 --benchmark-ref 8143463 --base c3e7521 --candidate c7b85b8 --boundary --modes subset --subset-percent 10 --consumers 6
gate a-control-0 --benchmark-ref 8143463 --base c3e7521 --control --boundary --modes private,tree-wait,subset,subset-whole,subset-whole-unreserved --consumers 0
gate a-control-6 --benchmark-ref 8143463 --base c3e7521 --control --boundary --modes subset --subset-percent 10 --consumers 6
fi
# (b) Slab gains: slab modes run per-object on the base runtime; tree-refill is
# per-object on both sides (an in-gate control).
if [[ $step == all || $step == b ]]; then
gate b-slab --benchmark-ref c7b85b8 --base c3e7521 --candidate c7b85b8 --modes tree-slab,tree-wait-slab,tree-cont-slab,tree-refill-slab,tree-refill
fi
# (c) Harness-layout evidence, not a pass/fail gate and not a waiver: the
# common new benchmark links slab-mode instantiations into the candidate only.
if [[ $step == all || $step == c ]]; then
gate c-default-common-benchmark --benchmark-ref c7b85b8 --base c3e7521 --candidate c7b85b8
fi
# (c) 20-pair characterization of the one near-threshold six-consumer cell
# (evidence only; see layout-evidence.txt for its hot-symbol placement).
if [[ $step == all || $step == c-boundary ]]; then
gate c-boundary-6 --benchmark-ref c7b85b8 --base c3e7521 --candidate c7b85b8 --boundary --modes private-remote --consumers 6
fi
