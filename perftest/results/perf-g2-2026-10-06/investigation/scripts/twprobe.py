#!/usr/bin/env python3
"""twprobe.py N bin... : run tree-wait/0 under perf stat cycles:u; report rate, user/sys s, user GHz, cpu0 MHz samples."""
import subprocess, sys, re, os, resource, time, statistics
def mhz0():
    for l in open('/proc/cpuinfo'):
        if l.startswith('cpu MHz'): return float(l.split(':')[1])
N = int(sys.argv[1]); bins = sys.argv[2:]
for i in range(N):
    for b in (bins if i % 2 == 0 else bins[::-1]):
        r0 = resource.getrusage(resource.RUSAGE_CHILDREN)
        p = subprocess.Popen(['perf', 'stat', '-x,', '-o', '/tmp/g2/prof/twp.csv', '-e', 'cycles:u', b, 'tree-wait', '16384', '3', '0', '256', '5', 'mimalloc', '0', 'small', '16384'],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env={**os.environ, 'ANTFARM_HUGE_PAGES': '0'})
        m = []
        while p.poll() is None: m.append(mhz0()); time.sleep(0.2)
        out = p.stdout.read(); r1 = resource.getrusage(resource.RUSAGE_CHILDREN)
        rate = float(re.search(r'Mactor_cycles/s=([0-9.]+)', out).group(1))
        cyc = [float(l.split(',')[0]) for l in open('/tmp/g2/prof/twp.csv') if 'cycles' in l][0]
        u, s = r1.ru_utime - r0.ru_utime, r1.ru_stime - r0.ru_stime
        print(f'{os.path.basename(b):11s} rate={rate:6.2f} user={u:.2f}s sys={s:.3f}s userGHz={cyc/u/1e9:.3f} mhz0 med={statistics.median(m):.0f} min={min(m):.0f}', flush=True)
