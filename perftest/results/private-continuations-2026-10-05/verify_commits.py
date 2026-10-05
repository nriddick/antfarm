#!/usr/bin/env python3
"""Verify isolated commit archives and, optionally, the evidence commit's source tree."""
import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument('commits', nargs='*')
parser.add_argument('--worktree', action='store_true',
                    help='also verify the uncommitted evidence tree; label it this commit')
args = parser.parse_args()
out = Path(__file__).resolve().parent
root = out.parents[2]
commits = args.commits or subprocess.check_output(
    ['git', 'rev-list', '--reverse', '632c7f6..HEAD'], cwd=root, text=True).split()
commits = [subprocess.check_output(['git', 'rev-parse', c], cwd=root, text=True).strip()
           for c in commits]
env = {**os.environ, 'ANTFARM_HUGE_PAGES': '0'}


def verify(folder, label, log):
    command = ['make', '-C', str(folder / 'private_actor_tests'),
               'SHELL=/usr/bin/bash', '-B', 'run', 'run-dmd']
    log.write(f'COMMIT {label}\n$ ' + ' '.join(command) + '\n'); log.flush()
    run = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         env=env, text=True)
    lines = run.stdout.splitlines(True)
    log.write(''.join(lines[:8]))
    if len(lines) > 20:
        log.write(f'... {len(lines)-20} middle lines omitted ...\n')
        log.write(''.join(lines[-12:]))
    else:
        log.write(''.join(lines[8:]))
    log.write(f'PASS lines: {sum(line.startswith("PASS ") for line in lines)}; '
              f'complete output SHA256: {hashlib.sha256(run.stdout.encode()).hexdigest()}\n')
    log.write(f'RESULT {label}: {run.returncode}\n\n'); log.flush()
    if run.returncode: raise SystemExit(run.returncode)
    print(f'PASS {label} LDC/DMD private suites', flush=True)


with (out / 'commit-builds.txt').open('w') as log:
    for commit in commits:
        with tempfile.TemporaryDirectory(prefix='e24-commit-') as folder_name:
            folder = Path(folder_name)
            archive = subprocess.Popen(['git', 'archive', commit], cwd=root,
                                       stdout=subprocess.PIPE)
            subprocess.run(['tar', '-x', '-C', str(folder)], stdin=archive.stdout, check=True)
            archive.stdout.close()
            if archive.wait(): raise RuntimeError('git archive failed')
            verify(folder, commit, log)
    if args.worktree:
        label = 'the evidence commit (this commit)'
        log.write('The exact tip source tree is verified before committing. Only evidence\n'
                  'is recorded afterward; a tracked report cannot contain its own commit SHA.\n'
                  'Benchmark build/smoke verification is in revision-benchmark.txt.\n\n')
        verify(root, label, log)
        for name in ('actors/actor.d', 'actors/private_actor.d', 'private_actor_tests/main.d',
                     'private_actor_tests/extract_doc_examples.py', 'actors/PRIVATE_ACTORS.md',
                     'perftest/actor_churn.d'):
            log.write(f'SOURCE SHA256 {hashlib.sha256((root/name).read_bytes()).hexdigest()} {name}\n')
