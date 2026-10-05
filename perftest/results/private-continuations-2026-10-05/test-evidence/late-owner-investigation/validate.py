#!/usr/bin/env python3
"""Rebuild and validate canonical files after the serial mutation phase."""
import hashlib, json, os, pathlib, subprocess, time
root=pathlib.Path(__file__).resolve().parents[5];os.chdir(root)
e=pathlib.Path(__file__).resolve().parent
runtime=pathlib.Path('actors/private_actor.d');tests=pathlib.Path('private_actor_tests/main.d')
runtime_hash=hashlib.sha256(runtime.read_bytes()).hexdigest()
assert runtime_hash=='34abc0bc615910d574308c91c1d633f995f59d8aa45bed20d08f01059145929d'
test_hash=hashlib.sha256(tests.read_bytes()).hexdigest()
env=os.environ.copy();env.update(ANTFARM_HUGE_PAGES='0',TSAN_OPTIONS='history_size=7:halt_on_error=1')
binaries=['private_actor_tests','private_actor_tests_dmd','private_actor_tests_release','private_actor_tests_dmd_release','private_actor_tests_tsan']
cmd=['make','-C','private_actor_tests',*binaries,'-j3'];start=time.monotonic()
p=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
(e/'canonical-build-after-fix.log').write_text('Run: '+' '.join(cmd)+f'\nExit: {p.returncode}\nElapsed: {time.monotonic()-start:.3f}s\n'+p.stdout)
print('Build exit',p.returncode,flush=True);assert p.returncode==0
outcomes=[]
for binary in binaries:
 for selector in (['full','cont'] if binary.endswith('tsan') else ['full','cont','death','death-negative-control']):
  cmd=['timeout','120','private_actor_tests/'+binary]
  if selector!='full':cmd += [selector]
  start=time.monotonic();p=subprocess.run(cmd,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True);elapsed=time.monotonic()-start
  (e/(binary+'-'+selector+'.log')).write_text('Runtime SHA256: '+runtime_hash+'\nTest SHA256: '+test_hash+'\nRun: ANTFARM_HUGE_PAGES=0 TSAN_OPTIONS=history_size=7:halt_on_error=1 '+' '.join(cmd)+f'\nExit: {p.returncode}\nElapsed: {elapsed:.3f}s\n'+p.stdout)
  outcomes.append(dict(binary=binary,selector=selector,exit=p.returncode,elapsed=round(elapsed,3)))
  print(binary,selector,p.returncode,f'{elapsed:.3f}s',flush=True)
  if p.returncode: print(p.stdout);raise SystemExit(1)
assert hashlib.sha256(runtime.read_bytes()).hexdigest()==runtime_hash
assert hashlib.sha256(tests.read_bytes()).hexdigest()==test_hash
(e/'validation.json').write_text(json.dumps(dict(runtime_sha256=runtime_hash,tests_sha256=test_hash,outcomes=outcomes),indent=2)+'\n')
