import subprocess, shutil, sys
from pathlib import Path
A="/home/nathan-riddick/pi_ground/antfarm/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a"
WT="/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree"
SPAWN = """        if (service_.records !is null)
        {
            // VACANT(g) -> LIVE(g)."""
RECLAIM = """        if (service_.records !is null)
        {
            retainOutcome(entry);
            return;
        }
"""
SCOPE = "    if (base.service.records !is null && rootCancelledNow(base.service, root)) return true;\n"
def cut_spawn(s):
    i = s.index(SPAWN); j = s.index("        appendReady(entry);\n        ++live_;", i)
    return s[:i] + s[j:]
edits = {
 "nospawn": [cut_spawn],
 "noreclaim": [lambda s: s.replace(RECLAIM, "")],
 "noscope": [lambda s: s.replace(SCOPE, "")],
 "none": [cut_spawn, lambda s: s.replace(RECLAIM, ""), lambda s: s.replace(SCOPE, "")],
}
for name in sys.argv[1:]:
    d = Path(f"/tmp/f3/v_{name}")
    if d.exists(): shutil.rmtree(d)
    d.mkdir(parents=True)
    subprocess.run(f"git -C {WT} archive HEAD | tar -x -C {d}", shell=True, executable="/usr/bin/bash", check=True)
    p = d/"actors/private_actor.d"; s = p.read_text(); before = s
    for e in edits[name]: s = e(s)
    assert s != before
    p.write_text(s)
    subprocess.run(["ldc2","-O2","-release","-d-version=AntfarmMimallocV3","-i","-I..","-I../threadpool/source","actor_churn.d","../antfarm.d","../antfarm_allocation.d","../actors/package.d","../actors/actor.d","../actors/wave.d","../actors/private_actor.d","../actors/mimalloc.d",A,"-L-lpthread","-L-lrt","-L-latomic","-of=actor_churn_mimalloc"], cwd=d/"perftest", check=True)
    print("built", name)
