#!/usr/bin/env bash
# Landing gate for the compact ST tables, as executed (serially, quiet host,
# after make check / check-soak / the Farm TSan soak and the compact-only
# TSan runs recorded in the README's landing section).
#   0801c17  base (local main with the S1/S2 fixes and the consistent
#            TSan lanes)
#   <tip>    feature/farm-st-compact landing commit (compact ST tables on
#            top of that base)
# actor_churn.d is identical in both revisions, so no --benchmark-ref.
set -u
cd "$(dirname "$0")/../../.."
R=perftest/results/farm-st-compact-2026-10-08
BASE=0801c17
CAND=${1:?usage: run-land-gate.sh CANDIDATE_SHA}
waitquiet() { while python3 -c "import os,sys; sys.exit(0 if os.getloadavg()[0] < 0.8 else 1)"; [ $? -ne 0 ]; do sleep 20; done; }
gate() { label=$1; shift; waitquiet; echo "=== $label $(date -Is)"; python3 perftest/perf_gate.py --repo . --output $R/$label "$@" > $R/$label.console.log 2>&1; echo "exit $? $(date -Is)"; }

# Full actor_churn matrix, 0 and 6 consumers, five pairs.
gate land-gate --base $BASE --candidate $CAND
