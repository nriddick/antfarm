#!/usr/bin/env bash
# Build writer_bench.d against base and candidate antfarm.d (LDC -O2 -release,
# as perf_gate builds actor_churn) and run each pinned to CPU 2, alternating.
# Usage: writer-bench.sh BASE CANDIDATE OUTDIR [REPS] [ROUNDS]
set -euo pipefail
base=$1 cand=$2 out=$3 reps=${4:-2000} rounds=${5:-3}
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../.." && pwd)
mkdir -p "$out"
for side in base cand; do
    rev=$base; [[ $side == cand ]] && rev=$cand
    src="$out/src-$side"; mkdir -p "$src"
    for f in antfarm.d antfarm_allocation.d; do git -C "$repo" show "$rev:$f" > "$src/$f"; done
    ldc2 -O2 -release -I"$src" "$here/writer_bench.d" "$src/antfarm.d" "$src/antfarm_allocation.d" \
        -L-lpthread -of="$out/writer-bench-$side" > "$out/build-$side.log" 2>&1
done
export ANTFARM_HUGE_PAGES=0
for r in $(seq 1 "$rounds"); do
    for side in base cand; do
        echo "## round $r $side" >> "$out/writer-bench.txt"
        taskset -c 2 "$out/writer-bench-$side" "$reps" >> "$out/writer-bench.txt"
    done
done
