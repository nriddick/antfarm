#!/usr/bin/env bash
# Gate (c) evidence: executed work versus code placement for the common-benchmark
# binaries. For each mode, two alternating perf-stat runs per binary report
# user instructions, cycles and branch misses per selected callback (subset
# modes) or per leaf (others), plus .text size and the hot Farm symbols'
# alignment. Usage: layout-evidence.sh GATE_DIR MODE[,MODE...] > layout-evidence.txt
set -euo pipefail
dir=$1; modes=$2
for v in base candidate; do
    echo "== $v $(sha256sum "$dir/$v-churn" | cut -c1-16) $(size -A "$dir/$v-churn" | awk '$1==".text"{print ".text="$2}')"
    nm -S --defined-only "$dir/$v-churn" \
        | grep -E 'writeImplTSQBf__T21PayloadBroadcastRangeTS6actors13private_actor11ChildBodies|ConsumerView12processShard' \
        | while read -r a s t n; do printf '  %s size=%d mod64=%d %.50s\n' "$a" $((16#$s)) $((16#$a % 64)) "$n"; done
done
for mode in ${modes//,/ }; do
    for round in 1 2; do
        for v in base candidate; do
            extra=(); [[ $mode == subset ]] && extra=(1)
            stdout=$(ANTFARM_HUGE_PAGES=0 perf stat -x, -e instructions:u,cycles:u,branch-misses:u -o /tmp/layout-stat.txt -- \
                "$dir/$v-churn" "$mode" 16384 3 0 256 5 mimalloc 0 small 16384 "${extra[@]}")
            python3 - "$v" "$mode" "$stdout" <<'EOF'
import re, sys
v, mode, out = sys.argv[1], sys.argv[2], sys.argv[3]
d = {}
for line in open('/tmp/layout-stat.txt'):
    f = line.strip().split(',')
    if len(f) > 2 and f[0].replace('.', '').isdigit():
        d[f[2]] = float(f[0])
m = re.search(r'verified_waves=(\d+)', out)
selected = int(re.search(r'selected=(\d+)', out).group(1)) if m else 0
units = int(m.group(1)) * selected if m else int(re.search(r'verified_actor_cycles=(\d+)', out).group(1))
rate = re.search(r'(Mactor_cycles/s|Mselected_callbacks/s)=([0-9.]+)', out).group(2)
i, c, b = d['instructions:u'], d['cycles:u'], d['branch-misses:u']
print(f"{mode}/0 {v:9s} rate={rate} instr/unit={i/units:.2f} cycles/unit={c/units:.3f} IPC={i/c:.3f} branch_miss/unit={b/units:.5f}")
EOF
        done
    done
done
