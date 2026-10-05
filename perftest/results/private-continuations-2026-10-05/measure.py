#!/usr/bin/env python3
"""Pinned alternating E2-4 runs; relaxed gate on a host with erratic load."""
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys

base, candidate, archive = (Path(p).resolve() for p in sys.argv[1:4])
repeats = int(sys.argv[4]) if len(sys.argv) > 4 else 5
out = Path(__file__).resolve().parent
binaries = {'base': base, 'candidate': candidate}
labels = {'base': 'BASELINE/perftest/actor_churn_mimalloc',
          'candidate': 'perftest/actor_churn_mimalloc'}
metadata = dict(base='632c7f6', candidate_head=subprocess.check_output(
    ['git', 'rev-parse', 'HEAD'], text=True).strip(), allocator='mimalloc',
    mimalloc_version=30503, seconds=3, repeats=repeats, warmups=5, actors=16384,
    batch=256, fanout=8, tier='small', quota_words=16384, huge_pages=False,
    profiles=[dict(consumers=0, cpus='0'), dict(consumers=6, cpus='0,1,2,3,4,5,6')],
    build_flags='-O2 -release -d-version=AntfarmMimallocV3 -i',
    build_command='make -C perftest -o ARCHIVE actor_churn_mimalloc MIMALLOC_LIBRARY=ARCHIVE',
    path_convention='BASELINE is an isolated git archive of 632c7f6; ARCHIVE is read-only in the main checkout.',
    comparison='tree-pipelined and tree-cont are both new; same root/mid/result-leaf topology, fanout 8. Existing tree/tree-wait have only root/plain leaves.',
    gate='User relaxed -3% gate due to erratic CPU load; dedicated perf round remains. Investigate/escalate only a reproducible quiet >5% regression.',
    contamination_rule='Flag individual runs if mean busy fraction on CPUs outside benchmark affinity exceeds 10%; also report case spread >10%. This does not detect every source of contention.',
    compiler=subprocess.check_output(['ldc2', '--version'], text=True),
    cpu=Path('/proc/cpuinfo').read_text().split('\n\n')[0],
    affinity=sorted(os.sched_getaffinity(0)),
    archive=dict(path='MAIN/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a',
                 sha256=hashlib.sha256(archive.read_bytes()).hexdigest()),
    binaries={k: dict(path=labels[k], sha256=hashlib.sha256(v.read_bytes()).hexdigest()) for k, v in binaries.items()},
    runtime_sha256=hashlib.sha256(Path('actors/private_actor.d').read_bytes()).hexdigest(),
    benchmark_sha256=hashlib.sha256(Path('perftest/actor_churn.d').read_bytes()).hexdigest())
(out / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')

def cpu_ticks():
    result = {}
    for line in Path('/proc/stat').read_text().splitlines():
        fields = line.split()
        if re.fullmatch(r'cpu\d+', fields[0]):
            values = list(map(int, fields[1:9]))
            result[int(fields[0][3:])] = (sum(values), values[3] + values[4])
    return result

cases = [('unused', mode, None) for mode in ['private', 'private-remote', 'tree', 'tree-wait', 'dtor']] + [('subset', 'subset', p) for p in [1, 100]]
rows = []
with (out / 'runs.jsonl').open('w') as log:
    for consumers, cpus in [(0, '0'), (6, '0,1,2,3,4,5,6')]:
        for repeat in range(repeats):
            order = [(v, c) for c in cases for v in ['base', 'candidate']]
            order += [('candidate', ('fold', mode, None)) for mode in ['tree-pipelined', 'tree-cont']]
            if repeat % 2: order.reverse()
            for version, (suite, mode, percent) in order:
                command = [str(binaries[version]), mode, '16384', '3', str(consumers), '256', '5', 'mimalloc', cpus, 'small', '16384']
                if percent is not None: command.append(str(percent))
                before = cpu_ticks()
                load_before = os.getloadavg()
                run = subprocess.run(command, text=True, capture_output=True, timeout=90, env={**os.environ, 'ANTFARM_HUGE_PAGES': '0'})
                after = cpu_ticks()
                metric = 'waves/s' if suite == 'subset' else 'Mactor_cycles/s'
                match = re.search(r'\b' + re.escape(metric) + r'=([0-9.]+)', run.stdout)
                if run.returncode or not match or 'hugePages=false' not in run.stdout or 'mimalloc_version=30503' not in run.stdout:
                    (out / 'failure.json').write_text(json.dumps(dict(command=command, status=run.returncode, stdout=run.stdout, stderr=run.stderr), indent=2))
                    raise RuntimeError(run.stdout + run.stderr)
                pinned = set(map(int, cpus.split(',')))
                busy = [1 - (after[c][1] - before[c][1]) / (after[c][0] - before[c][0]) for c in before if c not in pinned and after[c][0] > before[c][0]]
                outside_busy = statistics.mean(busy) if busy else 0
                row = dict(suite=suite, version=version, mode=mode, percent=percent, consumers=consumers, cpus=cpus, repeat=repeat, metric=metric,
                    rate=float(match[1]), command=[labels[version], *command[1:]], stdout=run.stdout, stderr=run.stderr,
                    load_before=load_before, load_after=os.getloadavg(), outside_affinity_busy_fraction=outside_busy, contaminated=outside_busy > .1)
                rows.append(row)
                log.write(json.dumps(row) + '\n'); log.flush()
                print(f'{len(rows)} {version} {mode}/{percent}/{consumers}: {row["rate"]:.6f} contaminated={row["contaminated"]}', flush=True)

summary = []
for consumers in [0, 6]:
    for suite, mode, percent in cases:
        groups = {v: [r for r in rows if (r['version'], r['mode'], r['percent'], r['consumers']) == (v, mode, percent, consumers)] for v in binaries}
        medians = {v: statistics.median(r['rate'] for r in group) for v, group in groups.items()}
        ranges = {v: [min(r['rate'] for r in group), max(r['rate'] for r in group)] for v, group in groups.items()}
        summary.append(dict(mode=mode, percent=percent, consumers=consumers, metric='waves/s' if suite == 'subset' else 'Mactor_cycles/s', **medians,
            range=ranges, change_percent=100 * (medians['candidate'] / medians['base'] - 1),
            contaminated_runs=sum(r['contaminated'] for group in groups.values() for r in group),
            spread_over_10_percent=any((bounds[1]-bounds[0])/medians[v] > .1 for v, bounds in ranges.items())))
features = []
for consumers in [0, 6]:
    medians = {mode: statistics.median(r['rate'] for r in rows if r['mode']==mode and r['consumers']==consumers) for mode in ['tree-pipelined', 'tree-cont']}
    for mode in ['tree', 'tree-wait']:
        medians[mode] = statistics.median(r['rate'] for r in rows if r['mode']==mode and r['consumers']==consumers and r['version']=='candidate')
    features.append(dict(consumers=consumers, median_Mleaf_cycles_per_second=medians,
        cont_vs_pipelined_percent=100 * (medians['tree-cont'] / medians['tree-pipelined'] - 1)))
(out / 'summary.json').write_text(json.dumps(dict(unused=summary, feature=features), indent=2) + '\n')
print(json.dumps(dict(unused=summary, feature=features), indent=2))
