#!/usr/bin/env bash
# Timing for the Wt-reservation fix and the compact-ST prototype, as executed
# (serially, quiet host, after make check / check-soak / TSan soaks).
#   2f853e5  base (local main)
#   0469901  Wt reservation rel -> acq_rel (pre-existing bug fix)
#   82cbce0  compact ST prototype + tests, on top of 0469901
# actor_churn.d is identical in all three; the gate compiles the base ref's
# copy against each runtime.
set -u
cd "$(dirname "$0")/../../.."
R=perftest/results/farm-st-compact-2026-10-08
BASE=2f853e5 FIX=0469901 CAND=82cbce0
waitquiet() { while python3 -c "import os,sys; sys.exit(0 if os.getloadavg()[0] < 0.8 else 1)"; [ $? -ne 0 ]; do sleep 20; done; }
gate() { label=$1; shift; waitquiet; echo "=== $label $(date -Is)"; python3 perftest/perf_gate.py --repo . --output $R/$label "$@" > $R/$label.console.log 2>&1; echo "exit $? $(date -Is)"; }
step=${1:-all}

# (i) Compact gate: full actor_churn matrix, 0 and 6 consumers, five pairs.
if [[ $step == all || $step == compact ]]; then
gate gate-compact --benchmark-ref $BASE --base $FIX --candidate $CAND
fi
# (ii) Fix-only gate: same matrix, base vs base+acq_rel.
if [[ $step == all || $step == fix ]]; then
gate gate-fix --benchmark-ref $BASE --base $BASE --candidate $FIX
fi
# (i) follow-up: gate-compact regressed private-remote/6 (-4.42%, all five
# pairs -4.68..-4.31%). 20-pair boundary run plus a byte-identical control
# of the base (0469901) on the same cell.
if [[ $step == all || $step == compact-boundary ]]; then
gate compact-boundary-6 --benchmark-ref $BASE --base $FIX --candidate $CAND --boundary --modes private-remote --consumers 6
gate compact-control-6 --benchmark-ref $BASE --base $FIX --control --boundary --modes private-remote --consumers 6
fi
# (ii) follow-up: gate-fix left five cells inconclusive on single outlier
# pairs (tree-wait/0, subset/100%/0, subset-whole-unreserved/0, subset/10%/6,
# subset/100%/6). 20-pair boundary runs plus byte-identical base controls.
if [[ $step == all || $step == fix-boundary ]]; then
gate fix-boundary-0 --benchmark-ref $BASE --base $BASE --candidate $FIX --boundary --modes tree-wait,subset,subset-whole-unreserved --subset-percent 100 --consumers 0
gate fix-boundary-6 --benchmark-ref $BASE --base $BASE --candidate $FIX --boundary --modes subset --subset-percent 10,100 --consumers 6
gate fix-control-0 --benchmark-ref $BASE --base $BASE --control --boundary --modes tree-wait,subset,subset-whole-unreserved --subset-percent 100 --consumers 0
gate fix-control-6 --benchmark-ref $BASE --base $BASE --control --boundary --modes subset --subset-percent 10,100 --consumers 6
fi
