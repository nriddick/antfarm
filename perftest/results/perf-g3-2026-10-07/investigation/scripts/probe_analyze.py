#!/usr/bin/env python3
"""Summarize probe-raw.json: per-object field line map and co-residence. usage: probe_analyze.py DIR"""
import json,sys,collections,pathlib
d=pathlib.Path(sys.argv[1]);rows=json.load(open(d/'probe-raw.json'))
H=lambda v:int(v,16)
res={}
def linemap(raw,basekey,keys):
    b=H(raw[basekey]);m={}
    for k in keys:
        a=H(raw[k]);m[k]=(a-b,(a//64)-(b//64))
    return b%64,m
out=[]
for kind,base,skip in [('PROBEROOT','park',{'size','align','entriesArray','batchesArray','batchEntriesArray'}),('PROBERUNTIME','runtime',{'size','align','slots','slotSize'})]:
    g=[r for r in rows if r['kind']==kind]
    if not g: continue
    keys=[k for k in g[0]['raw'] if k not in skip and k!=base]
    per=collections.defaultdict(collections.Counter)
    for r in g:
        mod,m=linemap(r['raw'],base,keys)
        sig=tuple((k,m[k][1]) for k in keys)
        per[(r['mode'],r['consumers'],r['raw']['size'],mod)][sig]+=1
    for (mode,c,size,mod),sigs in sorted(per.items()):
        for sig,n in sigs.items():
            lines=collections.defaultdict(list)
            for k,l in sig: lines[l].append(k)
            out.append(f"{kind} {mode}/{c} size={size} base%64={mod} n={n}: "+' ; '.join(f"L{l}:[{','.join(v)}]" for l,v in sorted(lines.items())))
# arrays alignment
for kind,keys in [('PROBEROOT',['entriesArray','batchesArray','batchEntriesArray']),('PROBERUNTIME',['slots'])]:
    g=[r for r in rows if r['kind']==kind]
    c=collections.Counter((r['mode'],r['consumers'],k,H(r['raw'][k])%64) for r in g for k in keys)
    for (mode,cons,k,mod),n in sorted(c.items()): out.append(f"{kind}-array {mode}/{cons} {k} %64={mod} n={n}")
# child parks
g=[r for r in rows if r['kind']=='PROBECHILD']
keys=[k for k in g[0]['raw'] if k not in {'size','align','thread','park','entriesArray'}]
per=collections.defaultdict(collections.Counter);mods=collections.defaultdict(collections.Counter);share=collections.defaultdict(collections.Counter)
emode={}
for r in g:
    mod,m=linemap(r['raw'],'park',keys)
    sig=tuple((k,m[k][1]) for k in keys)
    key=(r['mode'],r['consumers'],r['raw']['size'])
    per[key][(mod,sig)]+=1;mods[key][mod]+=1
    mods[key+('entries',)][H(r['raw']['entriesArray'])%64]+=1
# neighbour sharing: any two cohorts (same mode/start) whose [park,park+size) touch a common line
bystart=collections.defaultdict(list)
for r in g: bystart[(r['mode'],r['consumers'],r['start'])].append((H(r['raw']['park']),int(r['raw']['size']),r['raw']['thread']))
for (mode,c,s),L in bystart.items():
    L=sorted(set(L));
    for (a,sa,ta),(b,sb,tb) in zip(L,L[1:]):
        if (a+sa-1)//64==b//64: share[(mode,c)]['tail/head shared line' + (' (different creator thread)' if ta!=tb else ' (same creator)')]+=1
        else: share[(mode,c)]['separate']+=1
for key,sigs in sorted(per.items()):
    out.append(f"PROBECHILD {key} base%64 histogram={dict(mods[key])} entries%64={dict(mods[key+('entries',)])} neighbours={dict(share[key[:2]])}")
    for (mod,sig),n in sigs.most_common(2):
        lines=collections.defaultdict(list)
        for k,l in sig: lines[l].append(k)
        out.append(f"   base%64={mod} n={n}: "+' ; '.join(f"L{l}:[{','.join(v)}]" for l,v in sorted(lines.items())))
(d/'probe-summary.txt').write_text('\n'.join(out)+'\n');print('\n'.join(out))
