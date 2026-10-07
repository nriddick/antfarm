#!/usr/bin/env python3
"""Serial quiet-host baseline profiles, using the gate's contamination screen."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time

out = Path(__file__).resolve().parent
root = out.parents[3]
spec = importlib.util.spec_from_file_location('perf_gate', root / 'perftest/perf_gate.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
while os.getloadavg()[0] > 1:
    print('Waiting for initial load:', os.getloadavg(), flush=True)
    time.sleep(10)
affinity = os.sched_getaffinity(0)
os.sched_setaffinity(0, {7})
rows = []
for mode in ('actor', 'inbox', 'wave'):
    bench = [str(out / 'base-o2-churn'), mode, '16384', '3', '6', '256', '5', 'mimalloc', '0,1,2,3,4,5,6', 'small', '16384']
    for kind in ('stat', 'record'):
        if kind == 'stat':
            cmd = ['perf', 'stat', '-x,', '-o', str(out / (mode + '-stat.csv')), '-e', 'cycles,instructions,branches,branch-misses,cache-references,cache-misses', '--', *bench]
        else:
            cmd = ['perf', 'record', '-e', 'cycles:u', '-F', '199', '-o', str(out / (mode + '.data')), '--', *bench]
        run = gate.runone(cmd, list(range(7)), 3, child_affinity=affinity)
        rows.append(dict(mode=mode, kind=kind, **run))
        (out / 'profiles.json').write_text(json.dumps(rows, indent=2) + '\n')
        print(mode, kind, 'valid=', run['valid'], 'contaminated=', run['contaminated'], 'reasons=', run['reasons'], flush=True)
        if not run['valid'] or run['contaminated']:
            raise SystemExit(2)
        if kind == 'record':
            with (out / (mode + '-report.txt')).open('w') as log:
                subprocess.run(['perf', 'report', '--stdio', '--no-children', '-i', str(out / (mode + '.data')), '--percent-limit', '0.5'], stdout=log, stderr=subprocess.STDOUT, check=True)
