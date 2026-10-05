#!/usr/bin/env python3
"""Run immutable canonical test binaries with concurrent CPU load; no build/mutation."""
import hashlib, os, pathlib, subprocess, sys, time
root=pathlib.Path(__file__).resolve().parents[5]
os.chdir(root)
label,selector,loops=sys.argv[1],sys.argv[2],int(sys.argv[3])
stress_count=int(sys.argv[4]) if len(sys.argv)>4 else 12
out=pathlib.Path(__file__).resolve().parent/(label+'.log')
env=os.environ.copy();env.update(ANTFARM_HUGE_PAGES='0',TSAN_OPTIONS='history_size=7:halt_on_error=1')
command=['timeout','35','private_actor_tests/private_actor_tests_tsan']
if selector != 'full': command += [selector]
workers=[];start=time.monotonic(); failed=False
with out.open('w',buffering=1) as log:
 log.write('Test SHA256: '+hashlib.sha256(pathlib.Path('private_actor_tests/main.d').read_bytes()).hexdigest()+'\n')
 log.write('Runtime SHA256: '+hashlib.sha256(pathlib.Path('actors/private_actor.d').read_bytes()).hexdigest()+'\n')
 log.write('Affinity: '+str(sorted(os.sched_getaffinity(0)))+'\n')
 log.write('Stress: '+str(stress_count)+' concurrent python3 -c "while True: pass" processes\n')
 log.write('Run: ANTFARM_HUGE_PAGES=0 TSAN_OPTIONS=history_size=7:halt_on_error=1 '+' '.join(command)+'\n')
 try:
  for _ in range(stress_count): workers.append(subprocess.Popen(['python3','-c','while True: pass']))
  for i in range(1,loops+1):
   tick=time.monotonic();p=subprocess.run(command,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
   line=f'Run {i}: exit={p.returncode} elapsed={time.monotonic()-tick:.3f}s\n'
   log.write(line+p.stdout);print(label,line.strip(),flush=True)
   if p.returncode: failed=True;break
 finally:
  for worker in workers: worker.terminate()
  for worker in workers: worker.wait()
  log.write(f'Total elapsed: {time.monotonic()-start:.3f}s; failed={failed}\n')
sys.exit(1 if failed else 0)
