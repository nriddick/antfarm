#!/usr/bin/env python3
"""Reproduce the alternating unused-result-port gate against local main."""
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys

base, candidate = map(lambda p: Path(p).resolve(), sys.argv[1:3])
out = Path(__file__).resolve().parent
binaries = {'base': base, 'candidate': candidate}
labels = {'base': 'BASELINE/perftest/actor_churn', 'candidate': 'perftest/actor_churn'}
metadata = dict(base='374909c', allocator='crt', reason='mimalloc submodule unavailable',
                path_convention='BASELINE denotes the isolated archive; candidate paths are relative to the worktree.',
                seconds=3, repeats=5, warmups=5, actors=16384, batch=256,
                compiler=subprocess.check_output(['ldc2', '--version'], text=True),
                binaries={k: dict(path=labels[k], sha256=hashlib.sha256(v.read_bytes()).hexdigest())
                          for k, v in binaries.items()},
                diff=subprocess.check_output(['git', 'diff', '374909c', '--',
                                             'actors/private_actor.d'], text=True))
(out / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
rows = []
with (out / 'runs.jsonl').open('w') as f:
    for consumers, cpus in [(0, '0'), (6, '0,1,2,3,4,5,6')]:
        for mode in ['actor', 'wave', 'private', 'private-remote', 'tree', 'tree-wait']:
            for repeat in range(5):
                for version in (['base', 'candidate'] if repeat % 2 == 0 else ['candidate', 'base']):
                    cmd = [str(binaries[version]), mode, '16384', '3', str(consumers),
                           '256', '5', 'crt', cpus, 'small', '16384']
                    run = subprocess.run(cmd, text=True, capture_output=True, timeout=90,
                                         env={**os.environ, 'ANTFARM_HUGE_PAGES': '0'})
                    match = re.search(r'Mactor_cycles/s=([0-9.]+)', run.stdout)
                    if run.returncode or not match or 'hugePages=false' not in run.stdout:
                        (out / 'failure.json').write_text(json.dumps(dict(command=cmd,
                            status=run.returncode, stdout=run.stdout, stderr=run.stderr), indent=2))
                        raise RuntimeError(run.stdout + run.stderr)
                    row = dict(version=version, mode=mode, consumers=consumers, cpus=cpus,
                               repeat=repeat, rate=float(match[1]), command=[labels[version], *cmd[1:]],
                               stdout=run.stdout, stderr=run.stderr)
                    rows.append(row)
                    f.write(json.dumps(row) + '\n'); f.flush()
                    print(f'{len(rows)}/120 {version} {mode}/{consumers}: {row["rate"]}', flush=True)
summary = []
for consumers in [0, 6]:
    for mode in ['actor', 'wave', 'private', 'private-remote', 'tree', 'tree-wait']:
        medians = {v: statistics.median(r['rate'] for r in rows
                   if (r['version'], r['mode'], r['consumers']) == (v, mode, consumers))
                   for v in binaries}
        change = 100 * (medians['candidate'] / medians['base'] - 1)
        summary.append(dict(mode=mode, consumers=consumers, **medians,
                            change_percent=change, passed=change >= -3))
(out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
