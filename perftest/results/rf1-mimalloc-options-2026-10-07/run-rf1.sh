#!/usr/bin/env bash
# RF1 mimalloc option screen, as executed (serially, quiet host).
# Every gate is --control: c3e7521 runtime built twice from common benchmark
# 8143463 (byte-identical binaries); only the candidate side's environment
# differs. The baseline side never receives an option.
# The first attempt at the five option configs tripped the gate's initial-load
# guard right after the control finished and wrote only failure stubs; those
# were discarded and the configs rerun with waitquiet.
set -u
cd "$(dirname "$0")/../../.."
R=perftest/results/rf1-mimalloc-options-2026-10-07
SCREEN=(--modes tree,tree-wait,tree-cont,private-remote,private --consumers 0,6)
waitquiet() { while python3 -c "import os,sys; sys.exit(0 if os.getloadavg()[0] < 0.8 else 1)"; [ $? -ne 0 ]; do sleep 20; done; }
gate() { label=$1; shift; waitquiet; echo "=== $label $(date -Is)"; python3 perftest/perf_gate.py --repo . --base c3e7521 --control --benchmark-ref 8143463 --output $R/$label "$@"; echo "exit $? $(date -Is)"; }

# Five-pair screen.
gate control-baseline "${SCREEN[@]}"
gate page_full_retain-neg1 "${SCREEN[@]}" --candidate-env MIMALLOC_PAGE_FULL_RETAIN=-1
gate page_reclaim_on_free-1 "${SCREEN[@]}" --candidate-env MIMALLOC_PAGE_RECLAIM_ON_FREE=1
gate page_reclaim_on_free-neg1 "${SCREEN[@]}" --candidate-env MIMALLOC_PAGE_RECLAIM_ON_FREE=-1
# page_cross_thread_max_reclaim is read only for cross-thread reclaim
# (reclaim_on_free=1) or by threadpool threads, so it is screened with
# reclaim_on_free=1; -1 is unlimited (default 32).
gate reclaim_on_free-1+cross_thread_max_reclaim-neg1 "${SCREEN[@]}" --candidate-env MIMALLOC_PAGE_RECLAIM_ON_FREE=1 --candidate-env MIMALLOC_PAGE_CROSS_THREAD_MAX_RECLAIM=-1
gate thread_in_threadpool "${SCREEN[@]}" --candidate-env ANTFARM_CHURN_MI_THREADPOOL=1

# 20-pair confirmations of the largest page_full_retain=-1 effects.
gate boundary-page_full_retain-neg1-6 --boundary --modes private-remote,tree-cont --consumers 6 --candidate-env MIMALLOC_PAGE_FULL_RETAIN=-1
gate boundary-page_full_retain-neg1-0 --boundary --modes tree,tree-wait --consumers 0 --candidate-env MIMALLOC_PAGE_FULL_RETAIN=-1
