#!/usr/bin/env python3
"""Pinned lifecycle, root-batch and child-cohort measurements on an idle host."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess


def case_key(allocator, consumers, cpus, mode, batch, tier, quota):
    """Full identity of one measured case; every field can change the rate."""
    return (allocator, consumers, cpus, mode, batch, tier, quota)


def summarize(rows, cases):
    """Median/min/max across repeats, grouped by the full case identity."""
    summary = []
    for allocator, consumers, cpus, mode, batch, tier, quota in cases:
        key = case_key(allocator, consumers, cpus, mode, batch, tier, quota)
        group = [r['rate'] for r in rows if case_key(
            r['allocator'], r['consumers'], r['cpus'], r['mode'],
            r['batch'], r['tier'], r['quota']) == key]
        summary.append(dict(allocator=allocator, consumers=consumers, cpus=cpus,
                            mode=mode, batch=batch, tier=tier, quota=quota,
                            median=statistics.median(group),
                            minimum=min(group), maximum=max(group)))
    return summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--binary', default='./actor_churn_mimalloc')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--seconds', type=float, default=3)
    p.add_argument('--repeats', type=int, default=5)
    p.add_argument('--suite', choices=['lifecycle', 'batch', 'child'], default='lifecycle')
    p.add_argument('--mimalloc-version', type=int, default=30503,
                   help='expected mi_version() value (30500 reproduces the historical pin)')
    a = p.parse_args()
    a.output.mkdir(parents=True, exist_ok=True)
    binary = Path(a.binary).resolve()
    profiles = [(0, '0'), (1, '0,1'), (5, '0,1,2,3,4,5'),
                (11, ','.join(map(str, range(12))))]
    cases = []
    if a.suite == 'lifecycle':
        for allocator, selected in [('mimalloc', profiles), ('crt', [profiles[2]])]:
            for consumers, cpus in selected:
                for mode, batch in [('actor', 256), ('wave', 256), ('wave', 16384),
                                    ('private', 256), ('private-remote', 256), ('tree', 256),
                                    ('tree-wait', 256)]:
                    cases.append((allocator, consumers, cpus, mode, batch, 'small', 16384))
    elif a.suite == 'batch':
        for consumers, cpus in profiles:
            for tier, quota in [('small', 4096), ('small', 16384), ('bulk', 131072)]:
                for batch in [32, 64, 128, 256, 512, 1024, 4096]:
                    cases.append(('mimalloc', consumers, cpus, 'private', batch, tier, quota))
    else:
        for consumers, cpus in [profiles[0], profiles[2]]:
            for tier, quota in [('small', 16384), ('bulk', 131072)]:
                for batch in [256, 1024, 4096]:
                    for mode in ['tree', 'tree-wait']:
                        cases.append(('mimalloc', consumers, cpus, mode, batch, tier, quota))
    metadata = dict(binary=str(binary), sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                    seconds=a.seconds, repeats=a.repeats, actors=16384, warmups=5,
                    suite=a.suite, cases=cases, expected_mimalloc_version=a.mimalloc_version,
                    compiler=subprocess.check_output(['ldc2', '--version'], text=True),
                    base=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                    working_tree_diff=subprocess.check_output(['git', 'diff', '--',
                        'actors/private_actor.d', 'perftest/actor_churn.d'], text=True))
    (a.output / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    rows = []
    with (a.output / 'runs.jsonl').open('w') as out:
        for repeat in range(a.repeats):
            ordered = cases if repeat % 2 == 0 else list(reversed(cases))
            for allocator, consumers, cpus, mode, batch, tier, quota in ordered:
                cmd = [str(binary), mode, '16384', str(a.seconds), str(consumers),
                       str(batch), '5', allocator, cpus, tier, str(quota)]
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
                    raise RuntimeError(f'Expected mi_version()={a.mimalloc_version}, got {mimalloc_version}')
                row = dict(allocator=allocator, consumers=consumers, cpus=cpus,
                           repeat=repeat, mode=mode, batch=batch, tier=tier, quota=quota,
                           rate=float(match[1]), mimalloc_version=mimalloc_version, command=cmd,
                           stdout=run.stdout, stderr=run.stderr)
                rows.append(row)
                out.write(json.dumps(row) + '\n')
                out.flush()
                print(f'{len(rows):3d} {allocator:8s} {consumers:2d} {mode:7s}/{batch:5d} '
                      f'{tier}/{quota} {row["rate"]:.3f}', flush=True)
    (a.output / 'summary.json').write_text(json.dumps(summarize(rows, cases), indent=2) + '\n')


if __name__ == '__main__':
    main()
