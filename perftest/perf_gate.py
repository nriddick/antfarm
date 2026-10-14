#!/usr/bin/env python3
"""Serial pinned base/candidate gate. Exit 0 pass, 1 regression, 2 inconclusive/error.
No checkout edits. Scratch worktrees removed after builds. Output must be new.
Example: ./perf_gate.py --repo /path/antfarm --base BASE --candidate TIP \
 --archive /path/libmimalloc.a --output /tmp/gate-001 --modes actor,wave,private
Use --benchmark-ref to compile identical benchmark source against both runtimes.
Unsupported API combinations fail explicitly; never substitute another benchmark.
"""
import argparse,hashlib,json,os,pathlib,random,re,statistics,subprocess,sys,tempfile,time
P=pathlib.Path


def require_linux(parser):
    if sys.platform != 'linux':
        parser.error('performance timing gate requires Linux (/proc, sysfs and CPU affinity)')

def sha(path):return hashlib.sha256(P(path).read_bytes()).hexdigest()
def command(cmd,**kw):return subprocess.check_output(cmd,text=True,**kw)
def cpuinfo():
    result=[]
    for b in P('/proc/cpuinfo').read_text().strip().split('\n\n'):
        d=dict(line.split(':',1) for line in b.splitlines() if ':' in line)
        d={k.strip():v.strip() for k,v in d.items()}
        result.append({k:d.get(k) for k in ('processor','physical id','core id','cpu MHz','model name')})
    return result

def ticks():
    result={}
    for line in P('/proc/stat').read_text().splitlines():
        f=line.split()
        if re.fullmatch(r'cpu\d+',f[0]):
            v=list(map(int,f[1:9]));result[int(f[0][3:])]=(sum(v),v[3]+v[4])
    return result

def cpuusage():
    import resource
    r=resource.getrusage(resource.RUSAGE_CHILDREN);return r.ru_utime+r.ru_stime

def runone(cmd,cpus,seconds,child_affinity=None,env=None):
    before=ticks();start=time.monotonic();used=cpuusage();samples=[]
    proc=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
        env={**os.environ,**(env or {}),'ANTFARM_HUGE_PAGES':'0'},
        preexec_fn=(lambda: os.sched_setaffinity(0,child_affinity)) if child_affinity is not None else None)
    expired=False
    while True:
        samples.append(dict(time=time.time(),load=os.getloadavg(),mhz=[x['cpu MHz'] for x in cpuinfo()]))
        try:stdout,stderr=proc.communicate(timeout=.5);break
        except subprocess.TimeoutExpired:
            if time.monotonic()-start>max(90,seconds*4):
                proc.kill();stdout,stderr=proc.communicate();expired=True;break
    elapsed=time.monotonic()-start;after=ticks();child_cpu=cpuusage()-used;hz=os.sysconf('SC_CLK_TCK')
    busy={c:1-(after[c][1]-v[1])/(after[c][0]-v[0]) for c,v in before.items() if c not in cpus and after[c][0]>v[0]}
    outside=statistics.mean(busy.values()) if busy else None
    all_busy=sum((after[c][0]-v[0])-(after[c][1]-v[1]) for c,v in before.items())/hz
    # Includes short monitor overhead; detects tasks sharing pinned CPUs as well.
    external_cpu=max(0,all_busy-child_cpu)/elapsed
    reasons=[]
    if outside is None:reasons.append('no outside-affinity observation')
    if outside is not None and outside>.10:reasons.append('outside mean >10%')
    if max(busy.values(),default=0)>.25:reasons.append('outside CPU >25%')
    if external_cpu>.25:reasons.append('system minus benchmark CPU >0.25 core')
    if expired:reasons.append('timeout')
    rate=re.search(r'\b(Mactor_cycles/s|Mleaf_cycles/s|Mselected_callbacks/s)=([0-9.]+)',stdout)
    fields=dict(re.findall(r'([A-Za-z_]+)=([0-9]+(?:\.[0-9]+)?)',stdout))
    return dict(command=cmd,extra_env=env or {},returncode=proc.returncode,stdout=stdout,stderr=stderr,
        load_before=samples[0]['load'],load_after=os.getloadavg(),load_samples=samples,
        outside_affinity_busy_fraction=outside,outside_per_cpu=busy,external_cpu_cores=external_cpu,
        benchmark_cpu_seconds=child_cpu,wall_seconds=elapsed,contaminated=bool(reasons),reasons=reasons,
        rate=float(rate[2]) if rate else None,metric=rate[1] if rate else None,fields=fields,
        valid=proc.returncode==0 and rate is not None and float(rate[2])>0 and 'mimalloc_version=30503' in stdout and 'hugePages=false' in stdout)

def boot_ci(ratios):
    rng=random.Random(1729);n=len(ratios)
    values=sorted(statistics.median(rng.choices(ratios,k=n)) for _ in range(10000))
    return [values[250],values[9749]]

def main():
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--repo',type=P,default=P(__file__).resolve().parent.parent);p.add_argument('--base',required=True);p.add_argument('--candidate')
    p.add_argument('--archive',type=P,help='Defaults to REPO/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a');p.add_argument('--output',type=P,required=True)
    p.add_argument('--boundary',action='store_true',help='Require >=20 clean matched pairs (default 20)')
    p.add_argument('--control',action='store_true',help='Compare base to itself and require byte-identical binaries')
    p.add_argument('--modes',default='actor,wave,inbox,private,private-remote,tree,tree-wait,tree-cont,tree-pipelined,dtor,subset,subset-whole,subset-whole-unreserved,tree-cancel,private-tracked')
    p.add_argument('--consumers',default='0,6');p.add_argument('--cpus',default='0,1,2,3,4,5,6')
    p.add_argument('--actors',type=int,default=16384);p.add_argument('--seconds',type=float,default=3)
    p.add_argument('--repeats',type=int);p.add_argument('--max-attempts',type=int)
    p.add_argument('--batch',type=int,default=256);p.add_argument('--quota',type=int,default=16384);p.add_argument('--tier',choices=['small','bulk'],default='small')
    p.add_argument('--subset-percent',default='1,10,100');p.add_argument('--benchmark-ref')
    p.add_argument('--root-hint',default='auto',help='Pass a fixed actor_churn root-hint (0..7; chunk 128>>k, or 32>>k on a pre-MAX_CHUNK-128 base) to both builds instead of the default derived policy; auto appends no argument')
    p.add_argument('--allow-benchmark-change',action='store_true',help='Explicitly allow different checkpoint benchmark sources; fixed source is safer')
    p.add_argument('--base-env',action='append',default=[],metavar='KEY=VALUE',help='Extra environment for base runs only (repeatable), e.g. MIMALLOC_* options; recorded per run')
    p.add_argument('--candidate-env',action='append',default=[],metavar='KEY=VALUE',help='Extra environment for candidate runs only (repeatable); with --control this screens a runtime setting on byte-identical binaries')
    p.add_argument('--optimization',choices=['O2','O3'],default='O2');p.add_argument('--initial-load-limit',type=float,default=1.0)
    p.add_argument('--threshold',type=float,default=-.03)
    p.add_argument('--cancel-fraction-tolerance',type=float,default=.01,help='Max absolute median dispatched-fraction change before cancellation timing is inconclusive')
    a=p.parse_args()
    require_linux(p)
    a.repo=a.repo.resolve();a.output=a.output.resolve()
    a.archive=(a.archive or a.repo/'actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a').resolve()
    if a.repeats is None:a.repeats=20 if a.boundary else 5
    if a.max_attempts is None:a.max_attempts=max(10,2*a.repeats)
    if a.boundary and a.repeats<20:p.error('boundary mode requires >=20 repeats')
    if a.control:
        if a.candidate is not None and command(['git','rev-parse',a.base+'^{commit}'],cwd=a.repo)!=command(['git','rev-parse',a.candidate+'^{commit}'],cwd=a.repo):
            p.error('control requires base == candidate')
        a.candidate=a.base
    elif a.candidate is None:p.error('--candidate is required unless --control')
    if a.repeats<5 or a.seconds<3:p.error('gate requires >=5 repeats and >=3 seconds')
    if a.max_attempts<a.repeats:p.error('max-attempts must be >= repeats')
    if not -1<a.threshold<0:p.error('threshold must be a negative fraction between -1 and 0')
    if not 0<=a.cancel_fraction_tolerance<=1:p.error('cancel fraction tolerance must be in [0,1]')
    if a.root_hint!='auto' and not (a.root_hint.isdigit() and 0<=int(a.root_hint)<=7):p.error('root-hint must be auto or 0..7')
    envs={}
    for label,items in (('base',a.base_env),('candidate',a.candidate_env)):
        envs[label]={}
        for item in items:
            key,eq,value=item.partition('=')
            if not eq or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',key) or key=='ANTFARM_HUGE_PAGES':p.error('environment must be KEY=VALUE and cannot override ANTFARM_HUGE_PAGES')
            envs[label][key]=value
    pins=list(map(int,a.cpus.split(',')));profiles=list(map(int,a.consumers.split(',')))
    if not pins or len(set(pins))!=len(pins) or not set(pins)<=os.sched_getaffinity(0):p.error('invalid CPU affinity')
    if any(n<0 or n>=len(pins) for n in profiles):p.error('one controller plus consumers required')
    full_affinity=set(os.sched_getaffinity(0))
    topo=cpuinfo();cores={int(x['processor']):(x['physical id'],x['core id']) for x in topo}
    for n in profiles:
        if len(set(cores[c] for c in pins[1:n+1]))!=n:p.error('consumers must occupy distinct physical cores')
    if a.output.exists():p.error('output must be new (avoid overwriting evidence)')
    a.output.mkdir(parents=True)
    def dump(name,obj):(a.output/name).write_text(json.dumps(obj,indent=2)+'\n')
    governors={}
    for c in pins:
        path=P(f'/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_governor')
        governors[c]=path.read_text().strip() if path.exists() else 'unavailable'
    initial_load=os.getloadavg()
    meta=dict(arguments={k:str(v) if isinstance(v,P) else v for k,v in vars(a).items()},topology=topo,governors=governors,
        aslr=P('/proc/sys/kernel/randomize_va_space').read_text().strip() if P('/proc/sys/kernel/randomize_va_space').exists() else 'unavailable',
        initial_load=initial_load,controller_shares_core_with=[c for c in pins[1:max(profiles)+1] if cores[c]==cores[pins[0]]],compiler=command(['ldc2','--version']),archive_sha256=sha(a.archive),builds={},
        note='Five warmup cycles. Serial builds/runs. Own benchmark sources unless benchmark-ref. Loadavg includes benchmark self-load; admission uses initial load, exclusion uses CPU accounting.')
    dump('metadata.json',meta)
    if initial_load[0]>a.initial_load_limit:
        dump('failure.json',dict(reason='initial host load exceeds limit',load=initial_load))
        dump('summary.json',[]);dump('gate.json',dict(exit_status=2,reason='initial host load exceeds limit'));return 2
    binaries={};available={}
    for label,ref in [('base',a.base),('candidate',a.candidate)]:
        revision=command(['git','rev-parse',ref+'^{commit}'],cwd=a.repo).strip()
        scratch=P(tempfile.mkdtemp(prefix='perf-gate-'+label+'-'));scratch.rmdir()
        created=False
        try:
            command(['git','worktree','add','--detach',str(scratch),revision],cwd=a.repo);created=True
            if a.benchmark_ref:
                source=command(['git','show',a.benchmark_ref+':perftest/actor_churn.d'],cwd=a.repo)
                (scratch/'perftest/actor_churn.d').write_text(source)
            else:source=(scratch/'perftest/actor_churn.d').read_text()
            available[label]=set(re.findall(r'args\[1\] == "([a-z-]+)"',source))
            binary=a.output/(label+'-churn');binaries[label]=binary
            cmd=['ldc2','-'+a.optimization,'-release','-d-version=AntfarmMimallocV3','-i','-I.','-Ithreadpool/source','-Ifarmhost/source','perftest/actor_churn.d','antfarm/package.d','antfarm/allocation.d','antfarm/actors/package.d','antfarm/actors/actor.d','antfarm/actors/wave.d','antfarm/actors/private_actor.d','antfarm/actors/mimalloc.d',str(a.archive),'-L-lpthread','-L-lrt','-L-latomic','-of='+str(binary)]
            with (a.output/(label+'-build.log')).open('w') as log:subprocess.run(cmd,cwd=scratch,stdout=log,stderr=log,check=True)
            meta['builds'][label]=dict(revision=revision,command=cmd,sha256=sha(binary),benchmark_sha256=hashlib.sha256(source.encode()).hexdigest(),modes=sorted(available[label]))
            try:meta['builds'][label]['elf_header']=command(['readelf','-h',str(binary)])
            except (OSError,subprocess.SubprocessError):meta['builds'][label]['elf_header']='unavailable'
            dump('metadata.json',meta)
        finally:
            if created:subprocess.run(['git','worktree','remove','--force',str(scratch)],cwd=a.repo,check=True)
    if a.control and meta['builds']['base']['sha256']!=meta['builds']['candidate']['sha256']:
        dump('summary.json',[])
        dump('gate.json',dict(exit_status=2,reason='control binaries are not byte-identical'))
        return 2
    different_benchmark=meta['builds']['base']['benchmark_sha256']!=meta['builds']['candidate']['benchmark_sha256']
    if different_benchmark and not a.allow_benchmark_change:
        dump('summary.json',[])
        dump('gate.json',dict(exit_status=2,reason='benchmark sources differ; use --benchmark-ref for a common source, or explicitly --allow-benchmark-change'))
        return 2
    observed_pins=set(pins[:max(profiles)+1])
    spare=sorted(full_affinity-observed_pins)
    if not spare:
        dump('gate.json',dict(exit_status=2,reason='no CPU outside benchmark affinity for monitor'))
        dump('summary.json',[]);return 2
    used_cores={cores[c] for c in observed_pins}
    monitor_cpu=next((c for c in spare if cores[c] not in used_cores),spare[0])
    os.sched_setaffinity(0,{monitor_cpu})
    meta['monitor_cpu']=monitor_cpu;meta['child_initial_affinity']=sorted(full_affinity)
    meta['monitor_shares_core_with']=[c for c in observed_pins if cores[c]==cores[monitor_cpu]]
    dump('metadata.json',meta)
    requested=a.modes.split(',');missing={m:[v for v in available if m not in available[v]] for m in requested if any(m not in available[v] for v in available)}
    # Unsupported requested modes are an explicit incomplete gate, never silently passed.
    dump('unavailable.json',missing)
    modes=[m for m in requested if m not in missing];summary=[];status=2 if missing else 0
    with (a.output/'runs.jsonl').open('w') as log:
      for consumers in profiles:
       cpus=pins[:consumers+1]
       for mode in modes:
        for percent in (list(map(int,a.subset_percent.split(','))) if mode=='subset' else [100]):
         pairs=[];attempts=[]
         for repeat in range(a.max_attempts):
          pair={}
          for version in (['base','candidate'] if len(pairs)%2==0 else ['candidate','base']):
            cmd=[str(binaries[version]),mode,str(a.actors),str(a.seconds),str(consumers),str(a.batch),'5','mimalloc',','.join(map(str,cpus)),a.tier,str(a.quota)]
            if mode.startswith('subset'):cmd.append(str(percent))
            if a.root_hint!='auto':cmd.append(a.root_hint)
            row=runone(cmd,cpus,a.seconds,child_affinity=full_affinity,env=envs[version]);row.update(version=version,mode=mode,percent=percent,consumers=consumers,cpus=cpus,repeat=repeat,accepted_pair_target=len(pairs))
            log.write(json.dumps(row)+'\n');log.flush();pair[version]=row
            print(version,mode,percent,consumers,row['rate'],'contaminated',row['reasons'],flush=True)
            if not row['valid']:
                dump('failure.json',row);dump('summary.json',summary)
                dump('gate.json',dict(exit_status=2,reason='benchmark output invalid or failed'));return 2
          attempts.append(pair)
          if all(not r['contaminated'] for r in pair.values()):pairs.append(pair)
          if len(pairs)>=a.repeats:break
         bs=[r['base']['rate'] for r in pairs];cs=[r['candidate']['rate'] for r in pairs]
         allb=[r['base']['rate'] for r in attempts];allc=[r['candidate']['rate'] for r in attempts]
         item=dict(mode=mode,percent=percent,consumers=consumers,cpus=cpus,quiet_n=len(pairs),
            contaminated=sum(x['contaminated'] for z in attempts for x in z.values()),base_mean=statistics.mean(allb),candidate_mean=statistics.mean(allc),delta=statistics.mean(allc)/statistics.mean(allb)-1)
         item['kept_order_counts']={v:sum(next(iter(pair))==v for pair in pairs) for v in ['base','candidate']}
         item['legacy_mean_fields_include_contaminated']=True
         if bs:
            item.update(quiet_base_mean=statistics.mean(bs),quiet_candidate_mean=statistics.mean(cs))
            ratios=[c/b-1 for b,c in zip(bs,cs)];ci=boot_ci(ratios)
            item.update(base_median=statistics.median(bs),candidate_median=statistics.median(cs),base_min=min(bs),base_max=max(bs),candidate_min=min(cs),candidate_max=max(cs),quiet_delta=statistics.mean(cs)/statistics.mean(bs)-1,median_delta=statistics.median(cs)/statistics.median(bs)-1,paired_median_delta=statistics.median(ratios),paired_bootstrap95=ci,paired_ratios=ratios)
            item['decision']='insufficient' if len(pairs)<a.repeats else 'regression' if ci[1]<a.threshold else 'pass' if ci[0]>a.threshold else 'inconclusive'
         else:item.update(quiet_delta=None,decision='insufficient')
         for version,rows in (('base',[x['base'] for x in pairs]),('candidate',[x['candidate'] for x in pairs])):
            rss=[int(x['fields']['peak_rss_kib']) for x in rows if 'peak_rss_kib' in x.get('fields',{})]
            if rss:item[version+'_peak_rss_kib_median']=statistics.median(rss)
         if mode=='tree-cancel' and pairs:
            try:
                bf=[float(x['base']['fields']['dispatched_fraction']) for x in pairs]
                cf=[float(x['candidate']['fields']['dispatched_fraction']) for x in pairs]
                if not all(0<=x<=1 for x in bf+cf):raise ValueError('fraction outside [0,1]')
                br=[r*f for r,f in zip(bs,bf)];cr=[r*f for r,f in zip(cs,cf)]
                item.update(base_dispatched_fraction=statistics.median(bf),candidate_dispatched_fraction=statistics.median(cf),
                    base_dispatched_fraction_range=[min(bf),max(bf)],candidate_dispatched_fraction_range=[min(cf),max(cf)],
                    callback_rate_median_delta=statistics.median(cr)/statistics.median(br)-1 if statistics.median(br)>0 else None)
                if abs(statistics.median(cf)-statistics.median(bf))>a.cancel_fraction_tolerance:
                    item.update(decision='inconclusive_work',reason='cancellation changes the dispatched work fraction')
            except (KeyError,ValueError):
                item.update(decision='inconclusive_work',reason='missing or invalid dispatched fraction')
         # A paired bootstrap interval with n=5 is descriptive; repeat close calls.
         if item['decision']=='regression':status=max(status,1)
         elif item['decision']!='pass':status=2
         summary.append(item);dump('summary.json',summary)
    dump('summary.json',summary)
    status=1 if any(x['decision']=='regression' for x in summary) else 2 if missing or any(x['decision']!='pass' for x in summary) else 0
    screen='Conservative all-pairs threshold screen at n=5; median interval bounds effectively equal min/max paired ratios.' if a.repeats==5 else 'Paired-median interval threshold screen; individual pairs may fall below the threshold and still pass.'
    dump('gate.json',dict(exit_status=status,threshold=a.threshold,unavailable=missing,interpretation='Bootstrap interval is descriptive. '+screen+' Not a calibrated 95% confidence guarantee. Inconclusive is not pass. Legacy mean fields include all attempts; use quiet fields and decision. Run correctness separately before timing.'))
    return status
if __name__=='__main__':
    try:sys.exit(main())
    except Exception as e:
        print('ERROR:',e,file=sys.stderr)
        # Keep script/parse/build errors distinct from measured regressions.
        try:
            arg=next((x.split('=',1)[1] for x in sys.argv if x.startswith('--output=')),None)
            dest=P(arg if arg is not None else sys.argv[sys.argv.index('--output')+1])
            if dest.is_dir():
                (dest/'gate.json').write_text(json.dumps(dict(exit_status=2,error=type(e).__name__,reason=str(e)),indent=2)+'\n')
                if not (dest/'summary.json').exists():(dest/'summary.json').write_text('[]\n')
        except Exception:pass
        sys.exit(2)
