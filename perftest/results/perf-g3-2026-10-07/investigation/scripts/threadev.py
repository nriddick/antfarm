#!/usr/bin/env python3
"""threadev.py perf.data UNITS: per-thread event samples (count = samples*period) per unit, controller vs workers, top symbols."""
import subprocess,sys,re,collections
data=sys.argv[1];units=float(sys.argv[2]);lim=int(sys.argv[3]) if len(sys.argv)>3 else 6
out=subprocess.run(['perf','script','-i',data,'-F','tid,period,sym'],capture_output=True,text=True).stdout
per=collections.defaultdict(collections.Counter)
for l in out.splitlines():
    m=re.match(r'\s*(\d+)\s+(\d+)\s+(.*)',l)
    if m: per[int(m.group(1))][m.group(3).strip()[:110]]+=int(m.group(2))
t=sorted(per);c=per[t[0]];w=collections.Counter()
for x in t[1:]: w.update(per[x])
for name,cc in (('controller',c),('workers',w)):
    tot=sum(cc.values());print(f'{name}: {tot/units:.3f}/unit')
    for s,v in cc.most_common(lim): print(f'   {v/units:.3f}/unit {s}')
