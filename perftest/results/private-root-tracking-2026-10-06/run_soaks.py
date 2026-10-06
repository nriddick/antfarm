#!/usr/bin/env python3
"""F3 TSan soaks, run concurrently: the full private suite and the focused
tracked torture (fresh seed per loop), each for at least 601 seconds.
Keeps counts, duration and the log head/tail, not repeated output."""
import json
import os
from pathlib import Path
import subprocess
import threading
import time

out = Path(__file__).resolve().parent
root = out.parents[2]
env = {**os.environ, 'ANTFARM_HUGE_PAGES': '0',
       'TSAN_OPTIONS': 'history_size=7:halt_on_error=1'}
binary = './private_actor_tests/private_actor_tests_tsan'
duration = 601
results = {}

def soak(name, command_for):
    start = time.monotonic()
    runs = 0
    lines = []
    while time.monotonic() - start < duration:
        command = command_for(runs)
        run = subprocess.run(command, cwd=root, capture_output=True, text=True, env=env, timeout=600)
        output = (run.stdout + run.stderr).splitlines()
        if runs == 0:
            lines += ['$ ' + ' '.join(command)] + output[:6]
        if run.returncode:
            results[name] = dict(status='FAIL', run=runs + 1, exit=run.returncode,
                                 command=command, tail=output[-40:])
            return
        runs += 1
        last = output[-4:]
    elapsed = time.monotonic() - start
    results[name] = dict(status='PASS', loops=runs, seconds=round(elapsed, 3),
                         head=lines, last_tail=last)

cases = [
    ('tsan-suite-10min', lambda i: [binary]),
    ('tsan-tracked-torture-10min', lambda i: [binary, 'tracked-torture', '300', str(1000 + i)]),
]
threads = [threading.Thread(target=soak, args=case) for case in cases]
for t in threads: t.start()
for t in threads: t.join()
results['_meta'] = dict(head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
                        tsan_options=env['TSAN_OPTIONS'], concurrent=True, minimum_seconds=duration)
(out / 'soaks.json').write_text(json.dumps(results, indent=2) + '\n')
for name, r in results.items():
    if not name.startswith('_'):
        print(name, r['status'], r.get('loops'), r.get('seconds'), flush=True)
raise SystemExit(0 if all(r.get('status') == 'PASS' for k, r in results.items() if not k.startswith('_')) else 1)
