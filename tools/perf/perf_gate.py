#!/usr/bin/env python3
"""Serial pinned base/candidate performance report. Exit 0: the report is
complete (every requested cell has its matched pairs). Exit 2: incomplete or
error (missing modes, too few pairs, invalid benchmark output). Each cell is
classified against --threshold and flagged when it is not a pass, when its
paired ratios span more than 6%, or when the paired median is below -1.5%;
rerun flagged cells with --boundary (20 pairs). The classification is a
report for the reader, not a verdict: layout moves 0-consumer cells by more
than the threshold on identical semantics.
Background CPU is recorded as telemetry. A sample is excluded only when
another task used the benchmark's pinned CPUs (external busy time above 5% of
the pinned wall time) or the run timed out.
No checkout edits. Scratch worktrees removed after builds. Output must be new.
Example: ./perf_gate.py --repo /path/antfarm --base BASE --candidate TIP \
 --archive /path/libmimalloc.a --output /tmp/gate-001 --modes actor,wave,private
Use --benchmark-ref to compile identical benchmark source against both runtimes.
Unsupported API combinations fail explicitly; never substitute another benchmark.
Each revision builds with its own repository layout (flat before 1.8.2, the
antfarm package after); benchmark imports are mapped between layouts.
--build-only builds both revisions and stops before any measurement.
"""
import argparse,hashlib,json,os,pathlib,random,re,statistics,subprocess,sys,tempfile,time
P=pathlib.Path


def require_linux(parser):
    if sys.platform != 'linux':
        parser.error('performance timing gate requires Linux (/proc, sysfs and CPU affinity)')

# Repository layouts. Before 1.8.2 the runtime sat at the root (antfarm.d,
# actors/) with the benchmark in perftest/; 1.8.2 made it the antfarm package
# and moved the benchmark to tools/perf/.
LAYOUTS=dict(
    package=dict(marker='antfarm/package.d',benchmark='tools/perf/actor_churn.d',
        archive='tests/actors/.build/mimalloc-v3.5.3/libmimalloc.a',
        sources=['antfarm/package.d','antfarm/allocation.d','antfarm/actors/package.d','antfarm/actors/actor.d',
            'antfarm/actors/wave.d','antfarm/actors/private_actor.d','antfarm/actors/mimalloc.d']),
    flat=dict(marker='antfarm.d',benchmark='perftest/actor_churn.d',
        archive='actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a',
        sources=['antfarm.d','antfarm_allocation.d','actors/package.d','actors/actor.d',
            'actors/wave.d','actors/private_actor.d','actors/mimalloc.d']))
# Flat module name -> package module name (and submodules); inverted for package -> flat.
MODULE_MAP={'actors':'antfarm.actors','antfarm_templates':'antfarm.templates',
    'antfarm_allocation':'antfarm.allocation','antfarm_fibers':'ant_fibers'}

def find_layout(has,where):
    for name,layout in LAYOUTS.items():
        if has(layout['marker']):return name
    raise RuntimeError(f'{where}: unknown repository layout (neither antfarm/package.d nor antfarm.d)')

def tree_layout(root):return find_layout(lambda marker:(P(root)/marker).is_file(),root)

def ref_layout(repo,ref):
    present=command(['git','ls-tree','--name-only',ref,'--',*(x['marker'] for x in LAYOUTS.values())],cwd=repo).split()
    return find_layout(present.__contains__,ref)

def map_modules(source,origin,target):
    """Rewrite the imported module names in D `source` from layout `origin` to
    layout `target`. Returns the text and the sorted renames applied."""
    if origin==target:return source,[]
    table=MODULE_MAP if target=='package' else {new:old for old,new in MODULE_MAP.items()}
    applied=set()
    def module(m):
        name=m.group(0)
        for old,new in table.items():
            if name==old or name.startswith(old+'.'):
                applied.add(old+' -> '+new);return new+name[len(old):]
        return name
    def imports(m):return m.group(1)+re.sub(r'(?<![\w.])[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*',module,m.group(2))
    return re.sub(r'(\bimport\s+)([^;:]+)',imports,source),sorted(applied)

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

PINNED_SHARE_LIMIT=.05

def runone(cmd,cpus,seconds,child_affinity=None,env=None):
    load_before=os.getloadavg();before=ticks();start=time.monotonic();used=cpuusage()
    proc=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
        env={**os.environ,**(env or {}),'ANTFARM_HUGE_PAGES':'0'},
        preexec_fn=(lambda: os.sched_setaffinity(0,child_affinity)) if child_affinity is not None else None)
    expired=False
    # The monitor sleeps between checks; it reads /proc only before and after.
    while True:
        try:stdout,stderr=proc.communicate(timeout=.5);break
        except subprocess.TimeoutExpired:
            if time.monotonic()-start>max(90,seconds*4):
                proc.kill();stdout,stderr=proc.communicate();expired=True;break
    elapsed=time.monotonic()-start;after=ticks();child_cpu=cpuusage()-used;hz=os.sysconf('SC_CLK_TCK')
    busy={c:1-(after[c][1]-v[1])/(after[c][0]-v[0]) for c,v in before.items() if c not in cpus and after[c][0]>v[0]}
    outside=statistics.mean(busy.values()) if busy else None
    all_busy=sum((after[c][0]-v[0])-(after[c][1]-v[1]) for c,v in before.items())/hz
    # Telemetry only: background work off the pinned CPUs is recorded, not excluded.
    # Paired, interleaved samples absorb it, and it is below the layout noise.
    external_cpu=max(0,all_busy-child_cpu)/elapsed
    # Exclusion: someone else ran on the measured CPUs. Busy time on the pinned
    # set beyond the benchmark's own CPU time, as a fraction of pinned wall time.
    pinned_busy=sum((after[c][0]-v[0])-(after[c][1]-v[1]) for c,v in before.items() if c in cpus)/hz
    pinned_external=max(0,pinned_busy-child_cpu)/(elapsed*len(cpus)) if cpus else 0
    reasons=[]
    if pinned_external>PINNED_SHARE_LIMIT:reasons.append(f'pinned CPUs shared: external busy {pinned_external:.1%} of pinned wall time')
    if expired:reasons.append('timeout')
    rate=re.search(r'\b(Mactor_cycles/s|Mleaf_cycles/s|Mselected_callbacks/s)=([0-9.]+)',stdout)
    fields=dict(re.findall(r'([A-Za-z_]+)=([0-9]+(?:\.[0-9]+)?)',stdout))
    return dict(command=cmd,extra_env=env or {},returncode=proc.returncode,stdout=stdout,stderr=stderr,
        load_before=load_before,load_after=os.getloadavg(),
        outside_affinity_busy_fraction=outside,outside_per_cpu=busy,external_cpu_cores=external_cpu,
        pinned_external_fraction=pinned_external,benchmark_cpu_seconds=child_cpu,wall_seconds=elapsed,
        contaminated=bool(reasons),reasons=reasons,
        rate=float(rate[2]) if rate else None,metric=rate[1] if rate else None,fields=fields,
        valid=proc.returncode==0 and rate is not None and float(rate[2])>0 and 'mimalloc_version=30503' in stdout and 'hugePages=false' in stdout)

def boot_ci(ratios):
    rng=random.Random(1729);n=len(ratios)
    values=sorted(statistics.median(rng.choices(ratios,k=n)) for _ in range(10000))
    return [values[250],values[9749]]

def main():
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--repo',type=P,default=P(__file__).resolve().parents[2]);p.add_argument('--base',required=True);p.add_argument('--candidate')
    p.add_argument('--archive',type=P,help="Defaults to the REPO layout's archive: tests/actors/.build/mimalloc-v3.5.3/libmimalloc.a (actor_torture/.build/... before 1.8.2)");p.add_argument('--output',type=P,required=True)
    p.add_argument('--boundary',action='store_true',help='Collect >=20 matched pairs per cell (default 20): the rerun for flagged cells')
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
    p.add_argument('--optimization',choices=['O2','O3'],default='O2')
    p.add_argument('--threshold',type=float,default=-.03,help='Classification threshold on the paired-median interval (informational)')
    p.add_argument('--build-only',action='store_true',help='Build base and candidate, record metadata and stop: no CPU checks, no measurements')
    p.add_argument('--cancel-fraction-tolerance',type=float,default=.01,help='Max absolute median dispatched-fraction change before cancellation timing is inconclusive')
    a=p.parse_args()
    require_linux(p)
    a.repo=a.repo.resolve();a.output=a.output.resolve()
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
    full_affinity=set(os.sched_getaffinity(0))
    topo=cpuinfo();cores={int(x['processor']):(x['physical id'],x['core id']) for x in topo}
    if not a.build_only:
        if not pins or len(set(pins))!=len(pins) or not set(pins)<=full_affinity:p.error('invalid CPU affinity')
        if any(n<0 or n>=len(pins) for n in profiles):p.error('one controller plus consumers required')
        for n in profiles:
            if len(set(cores[c] for c in pins[1:n+1]))!=n:p.error('consumers must occupy distinct physical cores')
    if a.output.exists():p.error('output must be new (avoid overwriting evidence)')
    a.output.mkdir(parents=True)
    a.archive=(a.archive or a.repo/LAYOUTS[tree_layout(a.repo)]['archive']).resolve()
    def dump(name,obj):(a.output/name).write_text(json.dumps(obj,indent=2)+'\n')
    governors={}
    for c in pins:
        path=P(f'/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_governor')
        governors[c]=path.read_text().strip() if path.exists() else 'unavailable'
    initial_load=os.getloadavg()
    meta=dict(arguments={k:str(v) if isinstance(v,P) else v for k,v in vars(a).items()},harness_sha256=sha(__file__),topology=topo,governors=governors,
        aslr=P('/proc/sys/kernel/randomize_va_space').read_text().strip() if P('/proc/sys/kernel/randomize_va_space').exists() else 'unavailable',
        initial_load=initial_load,controller_shares_core_with=[] if a.build_only else [c for c in pins[1:max(profiles)+1] if cores[c]==cores[pins[0]]],compiler=command(['ldc2','--version']),archive_sha256=sha(a.archive),builds={},
        note='Five warmup cycles. Serial builds/runs. Own benchmark sources unless benchmark-ref. Initial load is recorded, not gated. Background CPU off the pinned set is telemetry; a sample is excluded only when the pinned CPUs were shared or the run timed out.')
    dump('metadata.json',meta)
    binaries={};available={}
    benchmark_layout=ref_layout(a.repo,a.benchmark_ref) if a.benchmark_ref else None
    for label,ref in [('base',a.base),('candidate',a.candidate)]:
        revision=command(['git','rev-parse',ref+'^{commit}'],cwd=a.repo).strip()
        scratch=P(tempfile.mkdtemp(prefix='perf-gate-'+label+'-'));scratch.rmdir()
        created=False
        try:
            command(['git','worktree','add','--detach',str(scratch),revision],cwd=a.repo);created=True
            layout=tree_layout(scratch);benchmark=LAYOUTS[layout]['benchmark']
            if a.benchmark_ref:
                origin=benchmark_layout
                source=command(['git','show',a.benchmark_ref+':'+LAYOUTS[origin]['benchmark']],cwd=a.repo)
            else:origin=layout;source=(scratch/benchmark).read_text()
            compiled,renames=map_modules(source,origin,layout)
            if renames:print(f'{label}: benchmark imports mapped {origin} -> {layout} layout: '+', '.join(renames),flush=True)
            if a.benchmark_ref:(scratch/benchmark).write_text(compiled)
            available[label]=set(re.findall(r'args\[1\] == "([a-z-]+)"',compiled))
            binary=a.output/(label+'-churn');binaries[label]=binary
            cmd=['ldc2','-'+a.optimization,'-release','-d-version=AntfarmMimallocV3','-i','-I.','-Ithreadpool/source',benchmark,*LAYOUTS[layout]['sources'],str(a.archive),'-L-lpthread','-L-lrt','-L-latomic','-of='+str(binary)]
            with (a.output/(label+'-build.log')).open('w') as log:subprocess.run(cmd,cwd=scratch,stdout=log,stderr=log,check=True)
            # benchmark_sha256 hashes the source in package-layout form, so the
            # same benchmark compares equal across layouts.
            meta['builds'][label]=dict(revision=revision,layout=layout,benchmark_layout=origin,module_renames=renames,command=cmd,sha256=sha(binary),
                benchmark_sha256=hashlib.sha256(map_modules(source,origin,'package')[0].encode()).hexdigest(),
                compiled_benchmark_sha256=hashlib.sha256(compiled.encode()).hexdigest(),modes=sorted(available[label]))
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
    if a.build_only:
        dump('summary.json',[])
        dump('gate.json',dict(exit_status=0,build_only=True,reason='build-only: both revisions built; no measurements'))
        return 0
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
            print(version,mode,percent,consumers,row['rate'],'excluded' if row['contaminated'] else 'ok',row['reasons'],flush=True)
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
            item.update(base_median=statistics.median(bs),candidate_median=statistics.median(cs),base_min=min(bs),base_max=max(bs),candidate_min=min(cs),candidate_max=max(cs),quiet_delta=statistics.mean(cs)/statistics.mean(bs)-1,median_delta=statistics.median(cs)/statistics.median(bs)-1,paired_median_delta=statistics.median(ratios),paired_bootstrap95=ci,paired_ratios=ratios,
                paired_min=min(ratios),paired_max=max(ratios),paired_span=max(ratios)-min(ratios),paired_stddev=statistics.pstdev(ratios))
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
         # A cell is flagged for a 20-pair rerun when its classification is not
         # a pass, its paired ratios spread wider than twice the threshold, or
         # its paired median is below half the threshold.
         item['flagged']=item['decision']!='pass' or item.get('paired_span',0)>2*abs(a.threshold) or (item.get('paired_median_delta') is not None and item['paired_median_delta']<a.threshold/2)
         summary.append(item);dump('summary.json',summary)
    dump('summary.json',summary)
    incomplete=bool(missing) or any(x['decision']=='insufficient' for x in summary)
    status=2 if incomplete else 0
    flagged=[dict(mode=x['mode'],percent=x['percent'],consumers=x['consumers'],decision=x['decision'],paired_median_delta=x.get('paired_median_delta'),paired_span=x.get('paired_span')) for x in summary if x['flagged']]
    for x in flagged:print('flagged',x['mode'],x['percent'],x['consumers'],x['decision'],'median',x['paired_median_delta'],'span',x['paired_span'],flush=True)
    screen='Paired-median bootstrap interval at n=5 is descriptive; its bounds are effectively the min and max paired ratios.' if a.repeats==5 else 'Paired-median bootstrap interval; individual pairs may fall below the threshold while the cell is classified pass.'
    dump('gate.json',dict(exit_status=status,threshold=a.threshold,unavailable=missing,flagged=flagged,
        rerun='rerun each flagged cell with --boundary and its --modes/--consumers/--subset-percent' if flagged else None,
        interpretation='This is a report, not a verdict. '+screen+' Classification uses the threshold; the reader decides, with the paired median, min, max and stddev. 0-consumer cells move with code layout on identical semantics. Legacy mean fields include excluded attempts; use quiet fields. Run correctness separately before timing.'))
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
