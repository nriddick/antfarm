#!/usr/bin/env python3
"""hotip.py DATA BINARY SYMSUBSTR [N]: per-instruction sample shares within one symbol, with disassembly context."""
import subprocess,sys,collections,re
data,binary,sub=sys.argv[1:4];n=int(sys.argv[4]) if len(sys.argv)>4 else 25
out=subprocess.run(['perf','script','-i',data,'-F','ip,sym,symoff'],capture_output=True,text=True).stdout
c=collections.Counter();tot=0;name=None
for l in out.splitlines():
    l=l.strip().split(None,1)[-1] if l.strip() else ''
    if not l:continue
    tot+=1
    m=re.match(r'(\S+)\+0x([0-9a-f]+)$',l)
    if m and sub in m.group(1):c[int(m.group(2),16)]+=1;name=m.group(1)
nm=subprocess.run(['nm','--defined-only',binary],capture_output=True,text=True).stdout
base=next(int(l.split()[0],16) for l in nm.splitlines() if l.split()[-1]==name)
dis=subprocess.run(['objdump','-d','--no-show-raw-insn','--start-address',hex(base),'--stop-address',hex(base+20000),binary],capture_output=True,text=True).stdout
lines={};order=[]
for l in dis.splitlines():
    m=re.match(r'\s*([0-9a-f]+):\s+(.*)',l)
    if m:a=int(m.group(1),16)-base;lines[a]=m.group(2);order.append(a)
idx={a:i for i,a in enumerate(order)}
s=sum(c.values());print(f'total={tot} sym={s} ({100*s/max(tot,1):.1f}%)')
for off,k in c.most_common(n):
    i=idx.get(off)
    ctx=' | '.join(f'{lines[order[j]]}' for j in range(max(0,i-3),i+1)) if i is not None else '?'
    print(f'{100*k/s:6.2f}% +{off:x}: {ctx}')
