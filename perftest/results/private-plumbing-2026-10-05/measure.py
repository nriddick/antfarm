#!/usr/bin/env python3
"""Pinned alternating E2-3 plumbing gate: base 370dfbc versus the plumbing branch."""
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys

base, candidate, archive = (Path(p).resolve() for p in sys.argv[1:4])
repeats = int(sys.argv[4]) if len(sys.argv) > 4 else 7
out = Path(__file__).resolve().parent
binaries = {'base': base, 'candidate': candidate}
labels = {'base': 'BASELINE/perftest/actor_churn_mimalloc',
          'candidate': 'perftest/actor_churn_mimalloc'}
metadata = dict(base='370dfbc', candidate=subprocess.check_output(
    ['git', 'rev-parse', 'HEAD'], text=True).strip(), allocator='mimalloc',
    mimalloc_version=30503, seconds=3, repeats=repeats, warmups=5, actors=16384,
    batch=256, tier='small', quota_words=16384, huge_pages=False,
    profiles=[dict(consumers=0, cpus='0'), dict(consumers=6, cpus='0,1,2,3,4,5,6')],
    build_flags='-O2 -release -d-version=AntfarmMimallocV3 -i',
    build_command='make -C perftest -o ARCHIVE actor_churn_mimalloc MIMALLOC_LIBRARY=ARCHIVE',
    path_convention='BASELINE is an isolated git archive of 370dfbc; ARCHIVE is the read-only prebuilt mimalloc archive from the main checkout.',
    modes='private, private-remote, tree, tree-wait, dtor (Mactor_cycles/s); subset at 1% and 100% (waves/s). No tree-results mode exists in actor_churn.',
    compiler=subprocess.check_output(['ldc2', '--version'], text=True),
    cpu=Path('/proc/cpuinfo').read_text().split('\n\n')[0],
    archive=dict(path='MAIN/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a',
                 sha256=hashlib.sha256(archive.read_bytes()).hexdigest()),
    binaries={k: dict(path=labels[k], sha256=hashlib.sha256(v.read_bytes()).hexdigest())
              for k, v in binaries.items()},
    runtime_sha256=hashlib.sha256(Path('actors/private_actor.d').read_bytes()).hexdigest(),
    benchmark_sha256=hashlib.sha256(Path('perftest/actor_churn.d').read_bytes()).hexdigest())
(out / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')

cases = [('unused', mode, None) for mode in
         ['private', 'private-remote', 'tree', 'tree-wait', 'dtor']] + \
        [('subset', 'subset', p) for p in [1, 100]]
rows = []
with (out / 'runs.jsonl').open('w') as f:
    for consumers, cpus in [(0, '0'), (6, '0,1,2,3,4,5,6')]:
        for repeat in range(repeats):
            order = [(v, c) for c in cases for v in ['base', 'candidate']]
            if repeat % 2: order.reverse()
            for version, (suite, mode, percent) in order:
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
                rows.append(row)
                f.write(json.dumps(row) + '\n')
                f.flush()
                print(f'{len(rows)} {version} {mode}/{percent}/{consumers}: {row["rate"]:.6f}', flush=True)

summary = []
for consumers in [0, 6]:
    for suite, mode, percent in cases:
        medians, spread = {}, {}
        for v in binaries:
            rates = [r['rate'] for r in rows if (r['version'], r['mode'], r['percent'], r['consumers'])
                     == (v, mode, percent, consumers)]
            medians[v] = statistics.median(rates)
            spread[v] = [min(rates), max(rates)]
        change = 100 * (medians['candidate'] / medians['base'] - 1)
        summary.append(dict(mode=mode, percent=percent, consumers=consumers,
                            metric='Mactor_cycles/s' if suite == 'unused' else 'waves/s',
                            **medians, range=spread, change_percent=change,
                            passed=change >= -3, gain_over_3=change > 3))
(out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
for s in summary:
    print(f"{s['consumers']} {s['mode']}/{s['percent']}: {s['base']:.4f} -> {s['candidate']:.4f} {s['change_percent']:+.2f}% {'PASS' if s['passed'] else 'FAIL'}")
