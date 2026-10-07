#!/usr/bin/env bash
# profile.sh BIN TAGPREFIX : record r0860 (ChangeToX) and r0243 (int-cache fills) for the target 6-consumer cells.
set -eu
bin=$1; pre=$2; cd /tmp/g3; mkdir -p prof
for spec in "private 6" "private-remote 6" "tree 6" "actor 6" "wave 6" "tree-cont 6" "subset 6 100"; do
  set -- $spec; m=$1; c=$2; pct=${3:-}; tag=$pre-$m-$c${pct:+-$pct}
  for ev in r0860:5003 r0243:20003; do e=${ev%%:*}; p=${ev##*:}
    ANTFARM_HUGE_PAGES=0 perf record -q -e $e:u -c $p -o prof/$tag.$e.data -- $bin $m 16384 3 $c 256 5 mimalloc 0,1,2,3,4,5,6 small 16384 $pct > prof/$tag.$e.out 2>&1
  done
done
