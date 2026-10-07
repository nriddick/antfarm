#!/usr/bin/env bash
# User-space cycle profiles (perf_event_paranoid=2) of the gate's candidate
# binary, after timing. Records are not kept (*.data is ignored); the symbol
# tables are. Usage: profile.sh BINARY
set -eu
bin=$1
cd "$(dirname "$0")"
for mode in tree tree-slab tree-wait tree-wait-slab tree-refill tree-refill-slab; do
    ANTFARM_HUGE_PAGES=0 perf record -q -e cycles:u -F 1999 -o profile-$mode-6.data -- \
        "$bin" $mode 16384 3 6 256 5 mimalloc 0,1,2,3,4,5,6 small 16384 > profile-$mode-6.stdout
    perf report -q -i profile-$mode-6.data --stdio --sort symbol --percent-limit 0.5 > profile-$mode-6.symbols.txt
    rm -f profile-$mode-6.data
done
