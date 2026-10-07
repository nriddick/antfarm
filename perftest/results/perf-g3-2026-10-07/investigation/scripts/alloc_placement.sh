#!/usr/bin/env bash
# alloc_placement.sh PROBE_BIN : base root-park placement under each benchmark allocator (3 starts each).
set -u
for alloc in mimalloc mimalloc64 crt; do for m in tree tree-wait tree-cont private-remote private; do for s in 1 2 3; do
  ANTFARM_HUGE_PAGES=0 "$1" $m 16384 0.05 6 256 1 $alloc 0,1,2,3,4,5,6 small 16384 | grep -m1 PROBEROOT | python3 -c "
import sys,re;f=dict(re.findall(r'(\w+)=(\S+)',sys.stdin.read()));H=lambda k:int(f[k],16)
line=H('allocator')//64
print('$alloc','$m','park_mod64='+str(H('park')%64),'allocator line also holds:',','.join(k for k in ('farm','batchLimit','incoming','waiting','rootReturns','childReturns') if H(k)//64==line) or '-')"
done; done; done | sort | uniq -c
