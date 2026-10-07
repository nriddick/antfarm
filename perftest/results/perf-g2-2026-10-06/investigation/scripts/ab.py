#!/usr/bin/env python3
"""Quick alternating A/B: ab.py MODE PERCENT CONSUMERS PAIRS binA binB [seconds]"""
import subprocess, sys, re, os, statistics, json
mode, pct, cons, pairs, A, B = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5], sys.argv[6]
secs = sys.argv[7] if len(sys.argv) > 7 else '3'
cpus = ','.join(map(str, range(cons + 1)))
def run(b):
    cmd = [b, mode, '16384', secs, str(cons), '256', '5', 'mimalloc', cpus, 'small', '16384']
    if mode.startswith('subset'): cmd.append(pct)
    out = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'ANTFARM_HUGE_PAGES': '0'}).stdout
    return float(re.search(r'\b(?:Mactor_cycles/s|Mleaf_cycles/s|Mselected_callbacks/s)=([0-9.]+)', out).group(1))
ds = []; ra = []; rb = []
for i in range(pairs):
    order = [A, B] if i % 2 == 0 else [B, A]
    r = {b: run(b) for b in order}
    ra.append(r[A]); rb.append(r[B]); ds.append(r[B] / r[A] - 1)
    print(f'{i} A={r[A]:.3f} B={r[B]:.3f} d={ds[-1]*100:+.2f}% load={os.getloadavg()[0]:.2f}', flush=True)
print(f'{mode}{pct}/{cons}: medA={statistics.median(ra):.3f} medB={statistics.median(rb):.3f} paired median={statistics.median(ds)*100:+.2f}% min={min(ds)*100:+.2f}% max={max(ds)*100:+.2f}%')
