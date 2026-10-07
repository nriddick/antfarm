#!/usr/bin/env python3
"""Reproduce serial G5 release binary and assembly builds from the current tree."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[4]
out = Path(__file__).resolve().parent
label, compiler, optimization = sys.argv[1:]
sources = ['perftest/actor_churn.d', 'antfarm.d', 'antfarm_allocation.d',
           'actors/package.d', 'actors/actor.d', 'actors/wave.d',
           'actors/private_actor.d', 'actors/mimalloc.d']
archive = root / 'actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a'
version = '-d-version=AntfarmMimallocV3' if compiler == 'ldc2' else '-version=AntfarmMimallocV3'
flags = ['-' + optimization, '-release', version, '-i', '-I.', '-Ithreadpool/source']
if compiler == 'dmd':
    flags.append('-inline')
cmd = [compiler, *flags, *sources, str(archive), '-L-lpthread', '-L-lrt', '-L-latomic', '-of=' + str(out / (label + '-churn'))]
commands = [cmd]
with (out / (label + '-build.log')).open('w') as log:
    subprocess.run(cmd, cwd=root, stdout=log, stderr=subprocess.STDOUT, check=True)
    if compiler == 'ldc2':
        assembly = [compiler, *flags, '-c', '-output-s', '-singleobj', *sources, '-of=' + str(out / (label + '.s'))]
        commands.append(assembly)
        subprocess.run(assembly, cwd=root, stdout=log, stderr=subprocess.STDOUT, check=True)
binary = out / (label + '-churn')
metadata = dict(commands=commands, compiler=subprocess.check_output([compiler, '--version'], text=True),
                revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
                source_hashes={f: hashlib.sha256((root / f).read_bytes()).hexdigest() for f in sources},
                archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                size=subprocess.check_output(['size', str(binary)], text=True))
(out / (label + '-build.json')).write_text(json.dumps(metadata, indent=2) + '\n')
print(metadata['size'], end='')
