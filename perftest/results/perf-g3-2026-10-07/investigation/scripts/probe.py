#!/usr/bin/env python3
"""Placement probe: instrument a scratch copy of REF, print persistent/cohort field
addresses, run each mode briefly over several starts and classify line co-residence.
Never used for timing.  usage: probe.py REF OUTDIR [starts]"""
import json,os,re,subprocess,sys,collections,pathlib
REPO=pathlib.Path('/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree')
ARCHIVE='/home/nathan-riddick/pi_ground/antfarm/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a'
ref,out=sys.argv[1],pathlib.Path(sys.argv[2]);starts=int(sys.argv[3]) if len(sys.argv)>3 else 10
out.mkdir(parents=True,exist_ok=True);src=out/'src'
subprocess.run(['rm','-rf',str(src)],check=True);src.mkdir()
subprocess.run(['/usr/bin/sh','-c',f'git -C {REPO} archive {ref} | tar -x -C {src}'],check=True)
p=src/'actors/private_actor.d';t=p.read_text()
root_needle='            park.spare_ = batch;\n        }\n        return park;\n'
assert t.count(root_needle)==1
root_probe='''            park.spare_ = batch;
        }
        {
            import core.stdc.stdio : printf;
            printf("PROBEROOT size=%zu align=%zu park=%p farm=%p allocator=%p batchLimit=%p incoming=%p waiting=%p rootReturns=%p childReturns=%p records=%p entries=%p live=%p free=%p ready=%p readyTail=%p spare=%p storageLimit=%p returned=%p outcomeHead=%p entriesArray=%p batchesArray=%p batchEntriesArray=%p\\n",
                PrivateRootPark.sizeof, PrivateRootPark.alignof, cast(void*) park, cast(void*) &park.service_.farm,
                cast(void*) &park.service_.allocator, cast(void*) &park.service_.batchLimit,
                cast(void*) &park.service_.incoming, cast(void*) &park.service_.waiting,
                cast(void*) &park.service_.rootReturns, cast(void*) &park.service_.childReturns,
                cast(void*) &park.service_.records, cast(void*) &park.entries_, cast(void*) &park.live_,
                cast(void*) &park.free_, cast(void*) &park.ready_, cast(void*) &park.readyTail_,
                cast(void*) &park.spare_, cast(void*) &park.storageLimit_, cast(void*) &park.returned_,
                cast(void*) &park.outcomeHead_, cast(void*) park.entries_, cast(void*) park.batches_,
                cast(void*) park.batchEntries_);
        }
        return park;
'''
t=t.replace(root_needle,root_probe)
child_needle='        park.base_.next = parent.children;\n        parent.children = &park.base_;\n        return park;\n'
assert t.count(child_needle)==1
child_probe='''        park.base_.next = parent.children;
        parent.children = &park.base_;
        {
            import core.stdc.stdio : printf;
            import core.sys.posix.pthread : pthread_self;
            import core.atomic : atomicFetchAdd;
            static shared uint probeCount;
            if (atomicFetchAdd(probeCount, 1) < 512)
                printf("PROBECHILD size=%zu align=%zu thread=%lx park=%p service=%p emittableNext=%p pending=%p active=%p failures=%p upstream=%p hook=%p finish=%p cancel=%p dispose=%p resumeNext=%p resumeHead=%p resumeCount=%p resumeScheduled=%p entriesPtr=%p count=%p cursor=%p batchLimitField=%p dispatched=%p retire=%p sel=%p header=%p headerCall=%p entriesArray=%p\\n",
                    PrivateChildPark.sizeof, PrivateChildPark.alignof, cast(ulong) pthread_self(), cast(void*) park,
                    cast(void*) &park.base_.service, cast(void*) &park.base_.wave.emittable.next,
                    cast(void*) &park.base_.wave.pending, cast(void*) &park.base_.wave.active,
                    cast(void*) &park.base_.wave.failures, cast(void*) &park.base_.wave.upstream,
                    cast(void*) &park.base_.wave.hook, cast(void*) &park.base_.wave.finish,
                    cast(void*) &park.base_.wave.cancel, cast(void*) &park.base_.dispose,
                    cast(void*) &park.base_.resume.next, cast(void*) &park.base_.resumeHead,
                    cast(void*) &park.base_.resumeCount, cast(void*) &park.base_.resumeScheduled,
                    cast(void*) &park.entries_, cast(void*) &park.count_, cast(void*) &park.cursor_,
                    cast(void*) &park.batchLimit_, cast(void*) &park.dispatched_, cast(void*) &park.retire_,
                    cast(void*) &park.sel_, cast(void*) &park.header_, cast(void*) &park.header_.call,
                    cast(void*) park.entries_);
        }
        return park;
'''
t=t.replace(child_needle,child_probe);p.write_text(t)
a=src/'actors/actor.d';u=a.read_text()
rt_needle='        runtime.freeSlots = runtime.slots;\n        return runtime;\n'
assert u.count(rt_needle)==1
u=u.replace(rt_needle,'''        runtime.freeSlots = runtime.slots;
        {
            import core.stdc.stdio : printf;
            printf("PROBERUNTIME size=%zu align=%zu runtime=%p allocator=%p slotsField=%p readyHead=%p readyCount=%p liveCount=%p stale=%p readyCarry=%p drainGate=%p freeSlotGate=%p freeSlots=%p header=%p slots=%p slotSize=%zu\\n",
                ActorRuntime.sizeof, ActorRuntime.alignof, cast(void*) runtime, cast(void*) &runtime.allocator,
                cast(void*) &runtime.slots, cast(void*) &runtime.readyHeadWord, cast(void*) &runtime.readyCount,
                cast(void*) &runtime.liveCount, cast(void*) &runtime.staleActivations_, cast(void*) &runtime.readyCarryWord,
                cast(void*) &runtime.readyDrainGate, cast(void*) &runtime.freeSlotGate, cast(void*) &runtime.freeSlots,
                cast(void*) &runtime.actorHeader, cast(void*) runtime.slots, ActorSlot.sizeof);
        }
        return runtime;
''');a.write_text(u)
binary=out/'probe-bin'
cmd=['ldc2','-O2','-release','-d-version=AntfarmMimallocV3','-i','-I.','-Ithreadpool/source','perftest/actor_churn.d','antfarm.d','antfarm_allocation.d','actors/package.d','actors/actor.d','actors/wave.d','actors/private_actor.d','actors/mimalloc.d',ARCHIVE,'-L-lpthread','-L-lrt','-L-latomic','-of='+str(binary),'-od='+str(out/'obj')]
subprocess.run(cmd,cwd=src,check=True)
modes=[('private',6,None),('private-remote',6,None),('tree',6,None),('tree-wait',6,None),('tree-cont',6,None),('subset',6,'100'),('actor',6,None),('wave',6,None),('private',0,None),('tree',0,None)]
rows=[]
for mode,cons,pct in modes:
    for s in range(starts):
        cpus=','.join(map(str,range(0,cons+1)))
        args=[str(binary),mode,'16384','0.05',str(cons),'256','1','mimalloc',cpus,'small','16384']+([pct] if pct else [])
        x=subprocess.run(args,capture_output=True,text=True,env={**os.environ,'ANTFARM_HUGE_PAGES':'0'},timeout=120)
        assert x.returncode==0,(args,x.stderr[-500:])
        for l in x.stdout.splitlines():
            if l.startswith('PROBE'):
                kind=l.split()[0];f=dict(re.findall(r'(\w+)=(\S+)',l))
                rows.append(dict(mode=mode,consumers=cons,start=s,kind=kind,raw=f))
(out/'probe-raw.json').write_text(json.dumps(rows,indent=1)+'\n')
print('rows',len(rows))
