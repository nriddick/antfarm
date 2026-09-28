#!/usr/bin/env python3
"""Paired full-lifecycle runs; run after correctness tests, on an idle host."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess

p = argparse.ArgumentParser()
p.add_argument('--binary', default='./actor_churn_mimalloc')
p.add_argument('--output', type=Path, required=True)
p.add_argument('--seconds', type=float, default=2)
p.add_argument('--repeats', type=int, default=5)
p.add_argument('--mimalloc-version', type=int, default=30503,
               help='expected mi_version() value (default: 30503 for 3.5.3)')
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=True)
binary = Path(a.binary).resolve()
profiles = [(0, '0'), (1, '0,1'), (5, '0,1,2,3,4,5'),
            (11, ','.join(map(str, range(12))))]
cases = [('actor', 256), ('wave', 256), ('wave', 16384), ('private', 256), ('private-remote', 256), ('tree', 256)]
metadata = dict(binary=str(binary), sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                seconds=a.seconds, repeats=a.repeats, actors=16384, warmups=5,
                profiles=profiles, cases=cases, expected_mimalloc_version=a.mimalloc_version,
                compiler=subprocess.check_output(['ldc2', '--version'], text=True),
                base=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip())
(a.output / 'metadata.json').write_text(json.dumps(metadata, indent=2))
rows = []
with (a.output / 'runs.jsonl').open('w') as out:
    for allocator, selected in [('mimalloc', profiles), ('crt', [profiles[2]])]:
        for repeat in range(a.repeats):
            for consumers, cpus in selected:
                ordered = cases if repeat % 2 == 0 else list(reversed(cases))
                for mode, batch in ordered:
                    cmd = [str(binary), mode, '16384', str(a.seconds), str(consumers),
                           str(batch), '5', allocator, cpus]
                    run = subprocess.run(cmd, env={**os.environ, 'ANTFARM_HUGE_PAGES': '0'},
                                         text=True, capture_output=True, timeout=90)
                    if run.returncode:
                        (a.output / 'failure.json').write_text(json.dumps(dict(command=cmd,
                            returncode=run.returncode, stdout=run.stdout, stderr=run.stderr), indent=2))
                        raise RuntimeError(run.stdout + run.stderr)
                    match = re.search(r'Mactor_cycles/s=([0-9.]+)', run.stdout)
                    if not match or 'hugePages=false' not in run.stdout:
                        raise RuntimeError(run.stdout + run.stderr)
                    version_match = re.search(r'\bmimalloc_version=(\d+)\b', run.stdout)
                    mimalloc_version = int(version_match[1]) if version_match else None
                    if allocator == 'mimalloc' and mimalloc_version != a.mimalloc_version:
                        raise RuntimeError(f'Expected mi_version()={a.mimalloc_version}, '
                                           f'got {mimalloc_version}')
                    row = dict(allocator=allocator, consumers=consumers, cpus=cpus,
                               repeat=repeat, mode=mode, batch=batch,
                               rate=float(match[1]), mimalloc_version=mimalloc_version, command=cmd,
                               stdout=run.stdout, stderr=run.stderr)
                    rows.append(row)
                    out.write(json.dumps(row) + '\n'); out.flush()
                    print(f'{len(rows):3d} {allocator:8s} {consumers:2d} {mode:7s}/{batch:5d} {row["rate"]:.3f}', flush=True)
summary = []
for allocator, selected in [('mimalloc', profiles), ('crt', [profiles[2]])]:
    for consumers, cpus in selected:
        for mode, batch in cases:
            group = [r['rate'] for r in rows if (r['allocator'], r['consumers'], r['mode'], r['batch'])
                     == (allocator, consumers, mode, batch)]
            summary.append(dict(allocator=allocator, consumers=consumers, mode=mode,
                                batch=batch, median=statistics.median(group),
                                minimum=min(group), maximum=max(group)))
(a.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
