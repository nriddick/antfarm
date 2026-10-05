#!/usr/bin/env python3
import pathlib, subprocess, sys, time
p=pathlib.Path(__file__).resolve().parent;start=time.monotonic()
for label,selector,loops in [('after-fix-c4','cont-activation-first',1000),('after-fix-c7','cont-seal-final-owner',200),('after-fix-full','full',20)]:
 cmd=['python3',str(p/'loaded_repeat.py'),label,selector,str(loops),'16']
 print('Starting '+' '.join(cmd),flush=True)
 with (p/(label+'-progress.log')).open('w') as log:
  result=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
 print(label,'exit',result.returncode,'total elapsed',round(time.monotonic()-start,3),flush=True)
 if result.returncode:print((p/(label+'.log')).read_text()[-6000:]);sys.exit(result.returncode)
print('All loaded groups passed; total elapsed',round(time.monotonic()-start,3),flush=True)
