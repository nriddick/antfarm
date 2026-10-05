#!/usr/bin/env bash
# Linux correctness matrix. Stops at the first failure.
# ANTFARM_HUGE_PAGES=0 forces ordinary pages. Child makes that quote
# variables receive an explicit shell, because GNU make never takes SHELL
# from the environment and this host may not have /bin/sh.
set -euo pipefail
cd "$(dirname "$0")/.."

export ANTFARM_HUGE_PAGES=0

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
