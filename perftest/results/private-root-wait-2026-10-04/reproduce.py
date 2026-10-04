#!/usr/bin/env python3
"""Alternating Phase E1 gate; build both binaries before running on an idle host."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess

p = argparse.ArgumentParser()
p.add_argument('--base', type=Path, required=True)
p.add_argument('--new', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--allocator', choices=['crt', 'mimalloc'], default='crt')
p.add_argument('--repeats', type=int, default=5)
p.add_argument('--seconds', type=float, default=3)
p.add_argument('--case', help='one configuration to repeat, as mode,batch,consumers')
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=True)
binaries = {'base': a.base.resolve(), 'new': a.new.resolve()}
# Match the lifecycle profiles and the plan's public 0/6-consumer gate.
cases = [(mode, 256, n) for mode in ['private', 'private-remote', 'tree']
         for n in [0, 1, 5, 11]]
cases += [(mode, batch, n) for mode, batch in
          [('actor', 256), ('wave', 256), ('wave', 16384)] for n in [0, 6]]
wait_cases = [('tree-wait', 256, n) for n in [0, 1, 5, 11]]
if a.case:
    mode, batch, consumers = a.case.split(',')
    cases = [(mode, int(batch), int(consumers))]
    wait_cases = []
metadata = dict(base_commit='c5a174ff669b1cd274e31b143e7f90b7c1993beb',
    new_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
    new_diff=subprocess.check_output(['git', 'diff', '--', 'perftest/actor_churn.d',
                                     'actors/private_actor.d'], text=True),
    compiler=subprocess.check_output(['ldc2', '--version'], text=True),
    timestamp=datetime.now(timezone.utc).isoformat(), allocator=a.allocator,
    allocator_reason=('mimalloc submodule unavailable; approved CRT fallback' if a.allocator == 'crt'
                      else 'mimalloc selected; version reported in raw output'),
    gate_decision='No regression below -3%; favourable deviations outside +3% are reported separately',
    flags='-O2 -release -i', actors=16384, warmups=5, seconds=a.seconds,
    repeats=a.repeats, cases=cases, tree_wait_cases=wait_cases,
    ring_mib=8, huge_pages=False, tier='small', quota_words=16384,
    pin='controller CPU 0; consumers CPUs 1..N; 0/6, 1/7, ... are SMT siblings',
    cpuinfo=(Path('/proc/cpuinfo').read_text().split('\n\n')[0]
             if Path('/proc/cpuinfo').exists() else platform.processor()),
    affinity=sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None,
    binaries={name: dict(path=str(binary), sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
              for name, binary in binaries.items()})
(a.output / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
rows = []
with (a.output / 'runs.jsonl').open('w') as out:
    for mode, batch, consumers in cases + wait_cases:
        for repeat in range(a.repeats):
            order = ['base', 'new'] if repeat % 2 == 0 else ['new', 'base']
            if mode == 'tree-wait':
                order = ['new']
            for build in order:
                cpus = ','.join(map(str, range(consumers + 1)))
                cmd = [str(binaries[build]), mode, '16384', str(a.seconds), str(consumers),
                       str(batch), '5', a.allocator, cpus, 'small', '16384']
                run = subprocess.run(cmd, env={**os.environ, 'ANTFARM_HUGE_PAGES': '0'},
                                     text=True, capture_output=True, timeout=90)
                match = re.search(r'Mactor_cycles/s=([0-9.]+)', run.stdout)
                if run.returncode or not match or 'hugePages=false' not in run.stdout:
                    (a.output / 'failure.json').write_text(json.dumps(dict(command=cmd,
                        returncode=run.returncode, stdout=run.stdout, stderr=run.stderr), indent=2))
                    raise RuntimeError(run.stdout + run.stderr)
                row = dict(build=build, mode=mode, batch=batch, consumers=consumers,
                           repeat=repeat, rate=float(match[1]), command=cmd,
                           stdout=run.stdout, stderr=run.stderr)
                rows.append(row)
                out.write(json.dumps(row) + '\n')
                out.flush()
                print(f'{len(rows):3d} {build} {mode}/{batch} consumers={consumers} {row["rate"]:.6f}', flush=True)
summary = []
for mode, batch, consumers in cases + wait_cases:
    stats = {}
    for build in ['base', 'new']:
        values = [r['rate'] for r in rows if (r['build'], r['mode'], r['batch'], r['consumers'])
                  == (build, mode, batch, consumers)]
        if values:
            stats[build] = dict(median=statistics.median(values), minimum=min(values), maximum=max(values))
    delta = (stats['new']['median'] / stats['base']['median'] - 1) * 100 if 'base' in stats else None
    summary.append(dict(mode=mode, batch=batch, consumers=consumers, **stats, delta_percent=delta,
                        within_symmetric_band=abs(delta) <= 3 if delta is not None else None,
                        no_regression_pass=delta >= -3 if delta is not None else None))
(a.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
for row in summary:
    print(json.dumps(row), flush=True)
