import subprocess, re, os, statistics, sys
bins = {"base": "/tmp/f3/base/perftest/actor_churn_mimalloc",
        "cand": "/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree/perftest/actor_churn_mimalloc",
        "mv": "/tmp/f3/mv/perftest/actor_churn_mimalloc"}
modes = sys.argv[1].split(",")
res = {}
for rep in range(int(sys.argv[2])):
    for consumers, cpus in [(0, "0"), (6, "0,1,2,3,4,5,6")]:
        for mode in modes:
            order = list(bins) if rep % 2 == 0 else list(reversed(bins))
            for name in order:
                out = subprocess.run([bins[name], mode, "16384", "3", str(consumers), "256", "5", "mimalloc", cpus, "small", "16384"],
                    capture_output=True, text=True, env={**os.environ, "ANTFARM_HUGE_PAGES": "0"}).stdout
                rate = float(re.search(r"Mactor_cycles/s=([0-9.]+)", out)[1])
                res.setdefault((mode, consumers, name), []).append(rate)
for (mode, c, name), v in sorted(res.items()):
    base = statistics.mean(res[(mode, c, "base")])
    print(f"{mode}/{c} {name}: mean={statistics.mean(v):.3f} delta={statistics.mean(v)/base-1:+.2%} runs={[round(x,2) for x in v]}")
