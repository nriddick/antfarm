#!/usr/bin/env python3
"""Wait for quiet admission, then invoke the unchanged standard G5 gate."""
import os
from pathlib import Path
import subprocess
import sys
import time

out = Path(__file__).resolve().parent
root = out.parents[2]
label, *args = sys.argv[1:]
while os.getloadavg()[0] > 1:
    print('Waiting for initial load:', os.getloadavg(), flush=True)
    time.sleep(10)
cmd = [sys.executable, 'perftest/perf_gate.py', '--base', 'fcbf122',
       '--output', str(out / label), *args]
(out / (label + '.command.txt')).write_text(repr(cmd) + '\n')
with (out / (label + '.console.log')).open('w') as log:
    result = subprocess.run(cmd, cwd=root, stdout=log, stderr=subprocess.STDOUT)
(out / (label + '.exit-status')).write_text(str(result.returncode) + '\n')
print(label, 'gate exit:', result.returncode, flush=True)
raise SystemExit(result.returncode)
