#!/usr/bin/env python3
"""Pinned alternating F1 unused-feature runs. Gate relaxed to -5% quiet."""
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys

base, candidate, archive = (Path(p).resolve() for p in sys.argv[1:4])
repeats = int(sys.argv[4]) if len(sys.argv) > 4 else 5
out = Path(__file__).resolve().parent
binaries = {"base": base, "candidate": candidate}
labels = {"base": "BASE/perftest/actor_churn_mimalloc",
          "candidate": "perftest/actor_churn_mimalloc"}
metadata = dict(
    base="20e6b6d",
    candidate_head=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    allocator="mimalloc", mimalloc_version=30503, seconds=3, repeats=repeats,
    warmups=5, actors=16384, batch=256, tier="small", quota_words=16384,
    huge_pages=False,
    profiles=[dict(consumers=0, cpus="0"), dict(consumers=6, cpus="0,1,2,3,4,5,6")],
    modes=["private", "private-remote", "tree", "tree-wait", "tree-cont"],
    build_flags="-O2 -release -d-version=AntfarmMimallocV3 -i",
    archive=str(archive),
    archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
    gate="User relaxed -3% gate; escalate only a quiet reproducible regression worse than -5%.",
    contamination_rule="Flag a run if mean busy fraction outside benchmark affinity exceeds 10%.",
    compiler=subprocess.check_output(["ldc2", "--version"], text=True),
    affinity=sorted(os.sched_getaffinity(0)),
    note="tree-results is not a benchmark mode; tree-cont is the continuation tree. Parks are untracked.",
)
(out / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")

def cpu_ticks():
    result = {}
    for line in Path("/proc/stat").read_text().splitlines():
        fields = line.split()
        if re.fullmatch(r"cpu\d+", fields[0]):
            values = list(map(int, fields[1:9]))
            result[int(fields[0][3:])] = (sum(values), values[3] + values[4])
    return result

cases = metadata["modes"]
rows = []
with (out / "runs.jsonl").open("w") as log:
    for consumers, cpus in [(0, "0"), (6, "0,1,2,3,4,5,6")]:
        for repeat in range(repeats):
            order = [(version, mode) for mode in cases for version in ("base", "candidate")]
            if repeat % 2:
                order.reverse()
            for version, mode in order:
                command = [str(binaries[version]), mode, "16384", "3", str(consumers),
                           "256", "5", "mimalloc", cpus, "small", "16384"]
                before = cpu_ticks()
                load_before = os.getloadavg()
                run = subprocess.run(command, text=True, capture_output=True, timeout=90,
                                     env={**os.environ, "ANTFARM_HUGE_PAGES": "0"})
                after = cpu_ticks()
                match = re.search(r"\bMactor_cycles/s=([0-9.]+)", run.stdout)
                if run.returncode or not match or "hugePages=false" not in run.stdout or "mimalloc_version=30503" not in run.stdout:
                    (out / "failure.json").write_text(json.dumps(dict(
                        command=command, status=run.returncode, stdout=run.stdout, stderr=run.stderr), indent=2))
                    raise SystemExit(run.stdout + run.stderr)
                pinned = set(map(int, cpus.split(",")))
                busy = [1 - (after[c][1] - before[c][1]) / (after[c][0] - before[c][0])
                        for c in before if c not in pinned and after[c][0] > before[c][0]]
                outside = statistics.mean(busy) if busy else 0
                row = dict(version=version, mode=mode, consumers=consumers, cpus=cpus, repeat=repeat,
                           rate=float(match[1]), stdout=run.stdout.strip(),
                           load_before=load_before, load_after=os.getloadavg(),
                           outside_affinity_busy_fraction=outside, contaminated=outside > 0.1)
                rows.append(row)
                log.write(json.dumps(row) + "\n")
                log.flush()
                print(f"{len(rows)} {version} {mode}/{consumers}: {row['rate']:.4f} contaminated={row['contaminated']}", flush=True)

summary = []
for consumers in (0, 6):
    for mode in cases:
        def rates(version, quiet):
            picked = [r["rate"] for r in rows if r["version"] == version and r["mode"] == mode
                      and r["consumers"] == consumers and (not quiet or not r["contaminated"])]
            return picked
        base_all, cand_all = rates("base", False), rates("candidate", False)
        base_q, cand_q = rates("base", True), rates("candidate", True)
        def ratio(b, c):
            if not b or not c:
                return None
            return statistics.mean(c) / statistics.mean(b) - 1
        item = dict(mode=mode, consumers=consumers,
                    base_mean=statistics.mean(base_all), candidate_mean=statistics.mean(cand_all),
                    delta=ratio(base_all, cand_all),
                    quiet_delta=ratio(base_q, cand_q),
                    quiet_n=min(len(base_q), len(cand_q)),
                    contaminated=sum(1 for r in rows if r["mode"] == mode and r["consumers"] == consumers and r["contaminated"]))
        summary.append(item)
        print(f"SUMMARY {mode}/{consumers}: delta={item['delta']:+.2%} quiet={item['quiet_delta']} contaminated={item['contaminated']}", flush=True)
(out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
