#!/usr/bin/env bash
# Run the Farm ThreadSanitizer binaries. Extra arguments are forwarded to
# the unit suite (for example `join-only` or `stale-quota-only`).
# Full runs also exercise the real leaf-to-root reclamation handoff and the
# hook-driven Live lifecycle schedules.
set -euo pipefail
cd "$(dirname "$0")/.."
export ANTFARM_HUGE_PAGES=0
export TSAN_OPTIONS="${TSAN_OPTIONS:-history_size=7:halt_on_error=1}"
if (( $# )); then
    exec ./tests/farm/antfarm_test_tsan "$@"
fi
./tests/farm/antfarm_test_tsan
./tests/farm/antfarm_test_leaf_tsan
exec ./tests/farm/antfarm_test_subscription_tsan
