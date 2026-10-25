#!/usr/bin/env python3
"""Reproducible warm Fiber dispatch experiment; no production scheduler changes."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / '.build/warm-fibers'


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def sample(mode, threads, iterations, slots, yields, chunk):
    completed = subprocess.run([str(BINARY), mode, str(threads), str(iterations), str(slots), str(yields), str(chunk)],
                               cwd=ROOT, env={**os.environ, 'ANTFARM_HUGE_PAGES': '0'},
                               capture_output=True, text=True, check=True, timeout=90)
    return json.loads(completed.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--threads', type=int, nargs='+', default=[1, 2, 4, 8, 16, 32])
    parser.add_argument('--yields', type=int, nargs='+', default=[0, 3])
    parser.add_argument('--modes', nargs='+', default=['local', 'ring', 'deque', 'deque-fair', 'domain'])
    parser.add_argument('--slots', type=int, default=64)
    parser.add_argument('--chunk', type=int, default=8)
    parser.add_argument('--seconds', type=float, default=.4)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--no-build', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    build = ['ldc2', '-O3', '-release', '-mcpu=native', '-i', '-I.', '-Ithreadpool/source', '-Iforkjoin/source',
             '-Iant_fibers/source', 'tools/coroutines/warm_fibers.d', '-of=.build/warm-fibers', '-od=.build/obj-warm-fibers']
    if not args.no_build:
        subprocess.run(build, cwd=ROOT, check=True, timeout=180)
    sources = [ROOT/'tools/coroutines/warm_fibers.d', ROOT/'forkjoin/source/forkjoin/deque.d',
               ROOT/'ant_fibers/source/ant_fibers/scheduler.d', ROOT/'antfarm/package.d',
               Path('/usr/include/dlang/ldc/core/thread/fiber/base.d'), Path('/usr/include/dlang/ldc/core/thread/fiber/package.d')]
    metadata = dict(date=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), platform=platform.platform(),
                    cpu=command('lscpu'), compiler=command('ldc2', '--version'), commit=command('git','rev-parse','HEAD'),
                    status=command('git','status','--short'), build=build, arguments={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
                    sha256={str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
                    binary_sha256=hashlib.sha256(BINARY.read_bytes()).hexdigest(), affinity=sorted(os.sched_getaffinity(0)),
                    collection_disabled=True, notes='N dispatch threads includes main; no extra polling thread. Startup and four warm lifetimes/slot excluded. Domain includes spawn/flush/completion/release and GC allocations. Raw modes omit lifecycle/waits/cancellation. Each dispatch is a Fiber.call, not a single context switch.')
    (args.output/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    cells=[(m,t,y) for y in args.yields for t in args.threads for m in args.modes]
    counts={}
    calibration=[]
    for mode,threads,yields in cells:
        row=sample(mode,threads,128,args.slots,yields,args.chunk)
        if row['seconds']<.01:
            row=sample(mode,threads,2048,args.slots,yields,args.chunk)
        count=max(16, math.ceil(row['iterations_per_slot']*args.seconds/row['seconds']))
        counts[mode,threads,yields]=count
        calibration.append(row)
    (args.output/'calibration.json').write_text(json.dumps(calibration,indent=2)+'\n')
    schedule=[(rep,*cell) for rep in range(args.repeats) for cell in cells]
    random.Random(20261025).shuffle(schedule)
    rows=[]
    with (args.output/'samples.jsonl').open('w') as output:
        for idx,(rep,mode,threads,yields) in enumerate(schedule):
            row=sample(mode,threads,counts[mode,threads,yields],args.slots,yields,args.chunk)
            row['repeat']=rep
            rows.append(row); output.write(json.dumps(row)+'\n'); output.flush()
            print(f'{idx+1}/{len(schedule)} {mode:10} t={threads:2} y={yields} {row["fibers_per_second"]/1e6:.3f} M fibers/s ({row["seconds"]:.3f}s)',flush=True)
    summary=[]
    for mode,threads,yields in cells:
        values=[r for r in rows if (r['mode'],r['threads'],r['yields'])==(mode,threads,yields)]
        rates=[r['fibers_per_second'] for r in values]
        summary.append(dict(mode=mode,threads=threads,yields=yields,warm_fibers=threads*args.slots,
                            median=statistics.median(rates),minimum=min(rates),maximum=max(rates),
                            median_dispatches=statistics.median(r['dispatches_per_second'] for r in values),
                            gc_bytes_per_lifetime=statistics.median(r['gc_bytes']/r['completions'] for r in values),
                            migrations_per_dispatch=None if mode=='domain' else statistics.median(r['migrations']/r['dispatches'] for r in values),
                            samples=len(values)))
    (args.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')


if __name__=='__main__':
    main()
