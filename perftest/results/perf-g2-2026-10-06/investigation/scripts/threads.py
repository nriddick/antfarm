#!/usr/bin/env python3
"""threads.py perf.data: per-thread top symbols (share of thread period); controller = lowest tid."""
import subprocess, sys, re, collections
out = subprocess.run(['perf', 'script', '-i', sys.argv[1], '-F', 'tid,period,ip,sym'], capture_output=True, text=True).stdout
per = collections.defaultdict(collections.Counter)
for line in out.splitlines():
    m = re.match(r'\s*(\d+)\s+(\d+)\s+[0-9a-f]+\s+(.*)', line)
    if not m: continue
    sym = re.sub(r'\(scope ref [^)]*\)', '', m.group(3)).strip()
    sym = re.sub(r'actors\.private_actor\.|antfarm\.|actor_churn\.', '', sym)
    per[int(m.group(1))][sym[:150]] += int(m.group(2))
tids = sorted(per)
lim = int(sys.argv[2]) if len(sys.argv) > 2 else 10
def show(name, c):
    tot = sum(c.values()); print(f'--- {name}: {tot/1e9:.2f} Gcyc')
    for s, v in c.most_common(lim): print(f'  {100*v/tot:6.2f}% {s}')
show(f'controller {tids[0]}', per[tids[0]])
w = collections.Counter()
for t in tids[1:]: w.update(per[t])
show('workers', w)
