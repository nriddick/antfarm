#!/usr/bin/env python3
"""statcmp.py MODE CONSUMERS [PCT] [ROUNDS]: alternating base/cand perf stat, counters per counted unit."""
import subprocess,sys,re,os
m,c=sys.argv[1],int(sys.argv[2]);pct=sys.argv[3] if len(sys.argv)>3 and sys.argv[3]!='-' else None;rounds=int(sys.argv[4]) if len(sys.argv)>4 else 2
for i in range(rounds):
  for b in (['base','cand'] if i%2==0 else ['cand','base']):
    cmd=['perf','stat','-x,','-e','cycles:u,instructions:u,r0243:u','-o','/tmp/g3/st.csv','--',f'/tmp/g3/bin/churn-{b}',m,'16384','3',str(c),'256','5','mimalloc',','.join(map(str,range(c+1))),'small','16384']+([pct] if pct else [])
    out=subprocess.run(cmd,capture_output=True,text=True,env={**os.environ,'ANTFARM_HUGE_PAGES':'0'}).stdout
    u=int(re.search(r'verified_\w+=(\d+)',out).group(1));rate=re.search(r'(M\w+/s)=([0-9.]+)',out).group(2)
    v={l.split(',')[2]:float(l.split(',')[0]) for l in open('/tmp/g3/st.csv') if l.count(',')>3}
    print(f"{m}{pct or ''}/{c} {b} rate={rate} cyc/u={v['cycles:u']/u:.2f} ins/u={v['instructions:u']/u:.2f} fills/u={v['r0243:u']/u:.3f} ipc={v['instructions:u']/v['cycles:u']:.3f}",flush=True)
