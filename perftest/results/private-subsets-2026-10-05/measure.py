#!/usr/bin/env python3
"""Pinned alternating E2-2 gate and persistent-cohort subset measurements."""
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys

base, candidate, archive = (Path(p).resolve() for p in sys.argv[1:4])
out = Path(__file__).resolve().parent
binaries = {'base': base, 'candidate': candidate}
labels = {'base': 'BASELINE/perftest/actor_churn_mimalloc',
          'candidate': 'perftest/actor_churn_mimalloc'}
metadata = dict(base='3441b8b', candidate=subprocess.check_output(
    ['git', 'rev-parse', 'HEAD'], text=True).strip(), allocator='mimalloc',
    mimalloc_version=30503, seconds=3, repeats=5, warmups=5, actors=16384,
    batch=256, tier='small', quota_words=16384, huge_pages=False,
    profiles=[dict(consumers=0, cpus='0'), dict(consumers=6, cpus='0,1,2,3,4,5,6')],
    build_flags='-O2 -release -d-version=AntfarmMimallocV3 -i',
    build_command='make -C perftest -o ARCHIVE actor_churn_mimalloc MIMALLOC_LIBRARY=ARCHIVE',
    path_convention='BASELINE is an isolated git archive of 3441b8b; ARCHIVE is the read-only prebuilt mimalloc archive from the main checkout.',
    subset_model='One persistent root/cohort; setup and teardown excluded. Each no-op wave joins via waitChildren. Reversed selection; integer percent rounding. Whole comparators with and without reserved selection storage.',
    compiler=subprocess.check_output(['ldc2', '--version'], text=True),
    dmd=subprocess.check_output(['dmd', '--version'], text=True),
    cpu=Path('/proc/cpuinfo').read_text().split('\n\n')[0],
    archive=dict(path='MAIN/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a',
                 sha256=hashlib.sha256(archive.read_bytes()).hexdigest()),
    binaries={k: dict(path=labels[k], sha256=hashlib.sha256(v.read_bytes()).hexdigest())
              for k, v in binaries.items()},
    runtime_sha256=hashlib.sha256(Path('actors/private_actor.d').read_bytes()).hexdigest(),
    benchmark_sha256=hashlib.sha256(Path('perftest/actor_churn.d').read_bytes()).hexdigest(),
    diff=subprocess.check_output(['git', 'diff', '3441b8b', '--',
        'actors/private_actor.d', 'perftest/actor_churn.d'], text=True))
(out / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
rows = []
with (out / 'runs.jsonl').open('w') as f:
    for suite in ['unused', 'subset']:
        for consumers, cpus in [(0, '0'), (6, '0,1,2,3,4,5,6')]:
            cases = ([(v, mode, None) for mode in
                      ['private', 'private-remote', 'tree', 'tree-wait', 'dtor']
                      for v in ['base', 'candidate']] if suite == 'unused' else
                     [('candidate', 'subset', p) for p in [1, 10, 100]] +
                     [('candidate', mode, None) for mode in
                      ['subset-whole', 'subset-whole-unreserved']])
            for repeat in range(5):
                ordered = cases if repeat % 2 == 0 else list(reversed(cases))
                for version, mode, percent in ordered:
                    cmd = [str(binaries[version]), mode, '16384', '3', str(consumers),
                           '256', '5', 'mimalloc', cpus, 'small', '16384']
                    if percent is not None:
                        cmd.append(str(percent))
                    run = subprocess.run(cmd, text=True, capture_output=True, timeout=90,
                                         env={**os.environ, 'ANTFARM_HUGE_PAGES': '0'})
                    metric = 'Mactor_cycles/s' if suite == 'unused' else 'waves/s'
                    match = re.search(r'\b' + re.escape(metric) + r'=([0-9.]+)', run.stdout)
                    if (run.returncode or not match or 'hugePages=false' not in run.stdout
                            or 'mimalloc_version=30503' not in run.stdout):
                        (out / 'failure.json').write_text(json.dumps(dict(command=cmd,
                            status=run.returncode, stdout=run.stdout, stderr=run.stderr), indent=2))
                        raise RuntimeError(run.stdout + run.stderr)
                    row = dict(suite=suite, version=version, mode=mode, percent=percent,
                               consumers=consumers, cpus=cpus, repeat=repeat, metric=metric,
                               rate=float(match[1]), command=[labels[version], *cmd[1:]],
                               stdout=run.stdout, stderr=run.stderr)
                    if suite == 'subset':
                        row['Mselected_callbacks/s'] = float(re.search(
                            r'Mselected_callbacks/s=([0-9.]+)', run.stdout)[1])
                        row['selected'] = int(re.search(r'\bselected=(\d+)', run.stdout)[1])
                    rows.append(row)
                    f.write(json.dumps(row) + '\n')
                    f.flush()
                    print(f'{len(rows)}/150 {suite} {version} {mode}/{percent}/{consumers}: '
                          f'{row["rate"]:.6f}', flush=True)
summary = []
for consumers in [0, 6]:
    for mode in ['private', 'private-remote', 'tree', 'tree-wait', 'dtor']:
        medians = {v: statistics.median(r['rate'] for r in rows
                   if (r['suite'], r['version'], r['mode'], r['consumers'])
                   == ('unused', v, mode, consumers)) for v in binaries}
        change = 100 * (medians['candidate'] / medians['base'] - 1)
        summary.append(dict(suite='unused', mode=mode, consumers=consumers, **medians,
                            change_percent=change, passed=change >= -3))
    whole = statistics.median(r['rate'] for r in rows if
        (r['mode'], r['consumers']) == ('subset-whole', consumers))
    unreserved = statistics.median(r['rate'] for r in rows if
        (r['mode'], r['consumers']) == ('subset-whole-unreserved', consumers))
    for mode, percent in [('subset', 1), ('subset', 10), ('subset', 100),
                          ('subset-whole', None), ('subset-whole-unreserved', None)]:
        group = [r for r in rows if (r['mode'], r['percent'], r['consumers'])
                 == (mode, percent, consumers)]
        median = statistics.median(r['rate'] for r in group)
        summary.append(dict(suite='subset', mode=mode, percent=percent, consumers=consumers,
                            selected=group[0]['selected'], waves_per_second=median,
                            Mselected_callbacks_per_second=statistics.median(
                                r['Mselected_callbacks/s'] for r in group),
                            vs_reserved_whole=median / whole,
                            vs_unreserved_whole=median / unreserved,
                            minimum=min(r['rate'] for r in group), maximum=max(r['rate'] for r in group)))
(out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
