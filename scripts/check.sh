#!/usr/bin/env bash
# Linux correctness matrix. Stops at the first failure.
# ANTFARM_HUGE_PAGES=0 forces ordinary pages. Child makes that quote
# variables receive an explicit shell, because GNU make never takes SHELL
# from the environment and this host may not have /bin/sh.
set -euo pipefail
cd "$(dirname "$0")/.."

export ANTFARM_HUGE_PAGES=0
export TSAN_OPTIONS="${TSAN_OPTIONS:-history_size=7:halt_on_error=1}"

make_cmd=(make)
if [[ ! -x /bin/sh ]]; then
    if [[ ! -x /usr/bin/bash ]]; then
        echo "check: no /bin/sh and no /usr/bin/bash" >&2
        exit 1
    fi
    make_cmd+=(SHELL=/usr/bin/bash)
fi

run() {
    echo "==> $*"
    "$@"
}

mimalloc=0
soak=0
if [[ "${1:-}" == "--mimalloc" ]]; then
    mimalloc=1
elif [[ "${1:-}" == "--soak" ]]; then
    soak=1
elif [[ -n "${1:-}" ]]; then
    echo "usage: scripts/check.sh [--mimalloc|--soak]" >&2
    exit 2
fi

if [[ "$soak" -eq 1 ]]; then
    run "${make_cmd[@]}" -C private_actor_tests private_actor_tests_tsan
    soak_started=$SECONDS
    soak_runs=0
    # One extra second keeps the integer clock from shortening the 120s minimum.
    while (( SECONDS - soak_started < 121 )); do
        run env TSAN_OPTIONS=history_size=7:halt_on_error=1 ./private_actor_tests/private_actor_tests_tsan
        soak_runs=$((soak_runs + 1))
    done
    echo "PASS private actor TSan soak: $soak_runs loops in $((SECONDS - soak_started)) seconds"
    exit 0
fi

run dub test --compiler=ldc2
run dub test --compiler=dmd
# Threadpool and fibers are not covered by the root dub test. host-test on
# optimized LDC is what catches closeOrAbandon skipping abandon (LDC -O1+).
run dub test :threadpool --compiler=ldc2
run dub test :threadpool --compiler=dmd
run dub test :fibers --compiler=ldc2
run dub test :fibers --compiler=dmd
run dub run :threadpool -c confine-test --compiler=ldc2
run taskset -c 0-3 dub run :threadpool -c confine-test --compiler=ldc2
# One LLC (this 5900XT: LPs 0-7,16-23). SKIP worker cases fails unless
# ALLOW_HOST_SKIP=1. timeout so an exit hang fails the lane.
host_test() {
    local compiler="$1"
    shift
    local log
    log=$(mktemp)
    echo "==> timeout 300 taskset -c 0-7,16-23 dub run :threadpool -c host-test --compiler=${compiler} $*"
    if ! timeout 300 taskset -c 0-7,16-23 dub run :threadpool -c host-test --compiler="$compiler" "$@" | tee "$log"; then
        rm -f "$log"
        return 1
    fi
    if [[ "${ALLOW_HOST_SKIP:-}" != "1" ]] && grep -q "SKIP worker cases" "$log"; then
        echo "host-test SKIP is a failure (set ALLOW_HOST_SKIP=1 to allow)" >&2
        rm -f "$log"
        return 1
    fi
    rm -f "$log"
}
host_test ldc2
host_test dmd
host_test ldc2 --build=release
run "${make_cmd[@]}" check-docs
run "${make_cmd[@]}" check-farm-ordering
run "${make_cmd[@]}" run-farm-tsan
run "${make_cmd[@]}" -C actor_torture run run-dmd run-tsan
run "${make_cmd[@]}" -C review_torture run run-tsan
run "${make_cmd[@]}" -C private_actor_tests run run-dmd run-tsan doc-example death
run "${make_cmd[@]}" -C examples check

if [[ "$mimalloc" -eq 1 ]]; then
    if [[ ! -f third_party/mimalloc/CMakeLists.txt ]]; then
        echo "check-mimalloc: third_party/mimalloc is not populated" >&2
        exit 1
    fi
    run "${make_cmd[@]}" -C actor_torture run-mimalloc run-mimalloc-tsan
fi

echo "ALL CHECKS PASSED"
