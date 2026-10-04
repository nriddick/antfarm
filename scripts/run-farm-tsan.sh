#!/usr/bin/env bash
# Run the Farm ThreadSanitizer binary. Extra arguments are forwarded
# (for example `join-only` once that regression exists).
set -euo pipefail
cd "$(dirname "$0")/.."
export ANTFARM_HUGE_PAGES=0
export TSAN_OPTIONS="${TSAN_OPTIONS:-history_size=7:halt_on_error=1}"
exec ./antfarm_test_tsan "$@"
