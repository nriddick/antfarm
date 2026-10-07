#!/usr/bin/env python3
"""evsplit.py perf.data: per-event per-thread totals (controller=lowest tid) and controller top syms"""
import subprocess, sys, re, collections
out = subprocess.run(['perf', 'script', '-i', sys.argv[1], '-F', 'tid,event,period,ip,sym'], capture_output=True, text=True).stdout
tot = collections.defaultdict(collections.Counter); syms = collections.defaultdict(collections.Counter)
for line in out.splitlines():
    m = re.match(r'\s*(\d+)\s+(\d+)\s+(\S+):\s+[0-9a-f]+\s+(.*)', line)
    if not m: continue
    tid, per, ev, sym = int(m.group(1)), int(m.group(2)), m.group(3), m.group(4)
    tot[ev][tid] += per
    syms[(ev, tid)][re.sub(r'.*(writeImpl|runChild|processShard|consumeNext|sweepCurrentPosition|Consumer3run|startSubset|emitSubset|pump|currTime|clock_gettime).*', r'\1', sym)[:40]] += per
for ev, c in tot.items():
    tids = sorted(c); ctl = tids[0]
    print(f'{ev}: controller={c[ctl]/1e6:.1f}M workers={sum(c[t] for t in tids[1:])/1e6:.1f}M')
    s = syms[(ev, ctl)]; T = sum(s.values())
    print('   ctl:', ', '.join(f'{k}={100*v/T:.0f}%' for k, v in s.most_common(4)))
