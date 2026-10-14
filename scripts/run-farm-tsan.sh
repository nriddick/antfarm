#!/usr/bin/env bash
# Run the Farm ThreadSanitizer binary. Extra arguments are forwarded
# (for example `join-only` or `stale-quota-only`).
# Full runs also exercise the real leaf-to-root reclamation handoff.
set -euo pipefail
cd "$(dirname "$0")/.."
export ANTFARM_HUGE_PAGES=0
export TSAN_OPTIONS="${TSAN_OPTIONS:-history_size=7:halt_on_error=1}"
if (( $# )); then
    exec ./tests/farm/antfarm_test_tsan "$@"
fi
./tests/farm/antfarm_test_tsan
exec ./tests/farm/antfarm_test_leaf_tsan
