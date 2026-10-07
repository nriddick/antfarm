#!/usr/bin/env python3
"""Cross-thread free counts with the diagnostic mimalloc-count allocator.
Runs the given actor_churn binary once per mode (not a timing measurement),
on the gate's CPU placement, and prints cross-thread frees per leaf lifetime.
usage: count-remote.py BINARY OUT.json"""
import json,re,subprocess,sys,os
binary,out=sys.argv[1],sys.argv[2]
modes=['tree','tree-slab','tree-wait','tree-wait-slab','tree-cont','tree-cont-slab','tree-refill','tree-refill-slab','private','private-remote']
rows=[]
for consumers in (6,0):
    cpus=','.join(str(c) for c in range(consumers+1))
    for mode in modes:
        cmd=[binary,mode,'16384','2',str(consumers),'256','5','mimalloc-count',cpus,'small','16384']
        p=subprocess.run(cmd,capture_output=True,text=True,env={**os.environ,'ANTFARM_HUGE_PAGES':'0'},check=True)
        f={k:int(v) for k,v in re.findall(r'\b(verified_actor_cycles|alloc_calls|free_calls|cross_thread_frees|cross_thread_free_bytes)=(\d+)',p.stdout)}
        storage=(re.search(r'storage=(\w+)',p.stdout) or [None,'perObject'])[1]
        leaves=f['verified_actor_cycles']
        row=dict(mode=mode,consumers=consumers,storage=storage,command=cmd,**f,
            allocs_per_leaf=f['alloc_calls']/leaves,frees_per_leaf=f['free_calls']/leaves,
            cross_thread_frees_per_leaf=f['cross_thread_frees']/leaves,
            cross_thread_bytes_per_leaf=f['cross_thread_free_bytes']/leaves)
        rows.append(row)
        print(f"{mode}/{consumers} storage={storage} allocs/leaf={row['allocs_per_leaf']:.4f} cross_frees/leaf={row['cross_thread_frees_per_leaf']:.4f} cross_bytes/leaf={row['cross_thread_bytes_per_leaf']:.1f}",flush=True)
json.dump(rows,open(out,'w'),indent=1)
