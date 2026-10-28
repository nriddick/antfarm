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
quick=0
if [[ "${1:-}" == "--quick" ]]; then
    quick=1
elif [[ "${1:-}" == "--mimalloc" ]]; then
    mimalloc=1
elif [[ "${1:-}" == "--soak" ]]; then
    soak=1
elif [[ -n "${1:-}" && "${1:-}" != "--print-cpus" ]]; then
    echo "usage: scripts/check.sh [--quick|--mimalloc|--soak|--print-cpus]" >&2
    exit 2
fi

select_cpus() {
    local variable="$1" minimum="$2" maximum="$3"
    cpu_pin=()
    # Empty counts as unset; `taskset -c ""` would fail the lane.
    if [[ -n "${!variable:-}" ]]; then
        cpu_pin=(taskset -c "${!variable}")
        return
    fi
    local affinity ranges range first last cpu
    local allowed=()
    affinity=$(taskset -pc $$)
    affinity=${affinity##*: }
    IFS=',' read -r -a ranges <<< "$affinity"
    for range in "${ranges[@]}"; do
        first=${range%-*}
        last=${range#*-}
        for ((cpu=first; cpu<=last; cpu++)); do
            allowed+=("$cpu")
        done
    done
    if (( ${#allowed[@]} < minimum )); then
        echo "check: $variable needs $minimum allowed CPUs; found ${#allowed[@]}; running unpinned"
    else
        if (( maximum > 0 )); then
            local IFS=','
            affinity="${allowed[*]:0:maximum}"
        fi
        cpu_pin=(taskset -c "$affinity")
    fi
}

if [[ "${1:-}" == "--print-cpus" ]]; then
    select_cpus CHECK_SMALL_CPUS 4 4
    echo "CHECK_SMALL_CPUS: ${cpu_pin[*]:-unpinned}"
    select_cpus CHECK_ONE_LLC_CPUS 16 0
    echo "CHECK_ONE_LLC_CPUS: ${cpu_pin[*]:-unpinned}"
    exit 0
fi

if [[ "$quick" -eq 1 ]]; then
    run python3 scripts/check-betterc.py
    run python3 scripts/check-c-abi.py --core-only
    run dub test --compiler=ldc2
    run "${make_cmd[@]}" -C tests/farm run DC=ldc2
    run dub test :threadpool --compiler=ldc2
    run dub test :ant_fibers --compiler=ldc2
    run dub test :forkjoin --compiler=ldc2 -- --DRT-testmode=run-main
    run python3 examples/run_examples.py --ldc-only
    echo "ALL QUICK CHECKS PASSED"
    exit 0
fi

if [[ "$soak" -eq 1 ]]; then
    run "${make_cmd[@]}" -C tests/private private_tests_tsan
    soak_started=$SECONDS
    soak_runs=0
    # One extra second keeps the integer clock from shortening the 120s minimum.
    while (( SECONDS - soak_started < 121 )); do
        run env TSAN_OPTIONS=history_size=7:halt_on_error=1 ./tests/private/private_tests_tsan
        soak_runs=$((soak_runs + 1))
    done
    echo "PASS private actor TSan soak: $soak_runs loops in $((SECONDS - soak_started)) seconds"
    exit 0
fi

run python3 scripts/check-betterc.py
run python3 scripts/check-c-abi.py --compiler ldc2
run python3 scripts/check-c-abi.py --compiler dmd
run python3 scripts/check-c-abi.py --tsan
run dub test --compiler=ldc2
run dub test --compiler=dmd
# Threadpool and fibers are not covered by the root dub test. host-test on
# optimized LDC is what catches closeOrAbandon skipping abandon (LDC -O1+).
run dub test :threadpool --compiler=ldc2
run dub test :threadpool --compiler=dmd
run dub test :ant_fibers --compiler=ldc2
run dub test :ant_fibers --compiler=dmd
run dub test :forkjoin --compiler=ldc2 -- --DRT-testmode=run-main
run dub test :forkjoin --compiler=dmd -- --DRT-testmode=run-main
# The whole fork-join suite under ThreadSanitizer and AddressSanitizer:
# dispatcher, pool parking, foreign submission, nested loops, deque.
run ldc2 -g -O1 -fsanitize=thread -d-version=TSan -i -I. -Ithreadpool/source -Iforkjoin/source -Itests/forkjoin \
    tests/forkjoin/forkjoin_test.d -of=.build/forkjoin_tsan -od=.build/obj-forkjoin-tsan
run env TSAN_OPTIONS=history_size=7:halt_on_error=1:report_thread_leaks=0:suppressions=tests/forkjoin/tsan.supp .build/forkjoin_tsan
run ldc2 -g -O1 -fsanitize=address -i -I. -Ithreadpool/source -Iforkjoin/source -Itests/forkjoin \
    tests/forkjoin/forkjoin_test.d -of=.build/forkjoin_asan -od=.build/obj-forkjoin-asan
# No fake stack: with detect_stack_use_after_return, locals live in a region
# the D GC does not scan, so an object held only by a local (a PoolResolution
# between resolve and the host's start) is collected mid-use and reads as
# garbage. That is an ASan/GC interaction, not a bug the lane should report.
run env ASAN_OPTIONS=detect_leaks=0:detect_stack_use_after_return=0 .build/forkjoin_asan
run dub run :threadpool -c confine-test --compiler=ldc2
select_cpus CHECK_SMALL_CPUS 4 4
run "${cpu_pin[@]}" dub run :threadpool -c confine-test --compiler=ldc2
# CHECK_SMALL_CPUS selects the small mask; CHECK_ONE_LLC_CPUS selects host-test CPUs.
# SKIP worker cases fails unless
# ALLOW_HOST_SKIP=1. timeout so an exit hang fails the lane.
select_cpus CHECK_ONE_LLC_CPUS 16 0
host_pin=("${cpu_pin[@]}")
host_test() {
    local compiler="$1"
    shift
    local log
    log=$(mktemp)
    echo "==> timeout 300 ${host_pin[*]} dub run :threadpool -c host-test --compiler=${compiler} $*"
    if ! timeout 300 "${host_pin[@]}" dub run :threadpool -c host-test --compiler="$compiler" "$@" | tee "$log"; then
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
run "${make_cmd[@]}" -C tests/farm check-farm-ordering
run "${make_cmd[@]}" -C tests/farm run-subscription-live-tsan
run "${make_cmd[@]}" -C tests/farm check-parallel-dmd
run "${make_cmd[@]}" -C tests/farm run-farm-tsan
run "${make_cmd[@]}" -C tests/actors run run-dmd run-tsan death
run "${make_cmd[@]}" -C tests/farm run run-tsan
run "${make_cmd[@]}" -C tests/private run run-dmd run-tsan doc-example death
run "${make_cmd[@]}" -C examples check

if [[ "$mimalloc" -eq 1 ]]; then
    if [[ ! -f third_party/mimalloc/CMakeLists.txt ]]; then
        echo "check-mimalloc: third_party/mimalloc is not populated" >&2
        exit 1
    fi
    run "${make_cmd[@]}" -C tests/actors run-mimalloc run-mimalloc-tsan
fi

echo "ALL CHECKS PASSED"
