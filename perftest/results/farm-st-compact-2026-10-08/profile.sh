#!/usr/bin/env bash
# User-space cycle profiles (perf_event_paranoid=2) of the compact gate's base
# (0469901) and candidate (82cbce0) actor_churn binaries, after all timing.
# Records are deleted; symbol tables and stdout are kept. Writer share =
# samples in AntFarm.writeImpl instantiations.
set -eu
cd "$(dirname "$0")"
run() { side=$1 mode=$2 cons=$3 cpus=$4 extra=${5:-}
    out=profile-$side-$mode-$cons
    ANTFARM_HUGE_PAGES=0 perf record -q -e cycles:u -F 1999 -o $out.data -- \
        ./gate-compact/$side-churn $mode 16384 3 $cons 256 5 mimalloc $cpus small 16384 $extra > $out.stdout
    perf report -q -i $out.data --stdio --sort symbol --percent-limit 0.5 > $out.symbols.txt
    perf report -q -i $out.data --stdio --sort comm,symbol --percent-limit 0.5 > $out.comm-symbols.txt
    rm -f $out.data
}
for side in base candidate; do
    run $side private-remote 6 0,1,2,3,4,5,6
    run $side private 0 0
    run $side subset 0 0 100
    run $side private 6 0,1,2,3,4,5,6
done
