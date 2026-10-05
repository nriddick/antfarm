#!/usr/bin/env python3
"""Private TSan soak; retain duration/counts and log head/tail, not repeated output."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

parser = argparse.ArgumentParser()
parser.add_argument('--loaded-seconds', type=int,
                    help='run only a loaded soak of this minimum duration')
args = parser.parse_args()
out = Path(__file__).resolve().parent
root = out.parents[2]
env = {**os.environ, 'ANTFARM_HUGE_PAGES': '0',
       'TSAN_OPTIONS': 'history_size=7:halt_on_error=1'}
summary_path = out / 'soaks.json'
summary = json.loads(summary_path.read_text()) if summary_path.exists() else []
cases = [('tsan-10min', 601, 0), ('tsan-loaded-3min', 181, 16)]
if args.loaded_seconds is not None:
    cases = [('tsan-loaded-revision', args.loaded_seconds, 16)]
for name, duration, load in cases:
    stress = []
    try:
        for _ in range(load):
            stress.append(subprocess.Popen([sys.executable, '-c', 'while True: pass'],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        start = time.monotonic()
        runs = 0
        with tempfile.NamedTemporaryFile(mode='w', prefix=name+'-', suffix='.log', delete=False) as log:
            raw_path = Path(log.name)
            command = ['./private_actor_tests/private_actor_tests_tsan']
            log.write('$ ANTFARM_HUGE_PAGES=0 TSAN_OPTIONS=history_size=7:halt_on_error=1 '
                      + ' '.join(command) + '\n'); log.flush()
            while time.monotonic() - start < duration:
                run = subprocess.run(command, cwd=root, stdout=log,
                                     stderr=subprocess.STDOUT, env=env, timeout=90)
                if run.returncode:
                    log.write(f'FAIL run={runs + 1} exit={run.returncode}\n'); log.flush()
                    print(f'Failure log: {raw_path}', flush=True)
                    raise SystemExit(run.returncode)
                runs += 1
                log.flush()
            elapsed = time.monotonic() - start
            log.write(f'PASS {name}: {runs} full suite loops in {elapsed:.3f} seconds; stress_processes={load}\n')
        raw = raw_path.read_bytes()
        lines = raw.decode().splitlines(True)
        result = dict(name=name, loops=runs, elapsed_seconds=elapsed,
                      stress_processes=load, passed=True,
                      raw_lines=len(lines), raw_sha256=hashlib.sha256(raw).hexdigest())
        excerpt = ''.join(lines) if len(lines) <= 100 else (
            ''.join(lines[:40]) + f'\n... {len(lines)-80} middle lines omitted ...\n\n'
            + ''.join(lines[-40:]))
        (out / f'{name}.txt').write_text(json.dumps(result, indent=2) + '\n\n' + excerpt)
        summary = [item for item in summary if item['name'] != name] + [result]
        summary_path.write_text(json.dumps(summary, indent=2) + '\n')
        print(result, flush=True)
    finally:
        for proc in stress:
            proc.terminate()
        for proc in stress:
            proc.wait()
