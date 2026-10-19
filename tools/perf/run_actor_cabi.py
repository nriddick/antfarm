#!/usr/bin/env python3
"""Matched Linux C/D lifecycle timings and separate exact-allocation diagnostics."""
import argparse
import importlib.util
import itertools
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--build-only", action="store_true")
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--seconds", type=float, default=0.4)
    args = p.parse_args()
    if platform.system() != "Linux": p.error("this harness uses Linux affinity and pthread barriers")
    if args.repeats < 3 or args.seconds <= 0: p.error("use >=3 repeats and a positive target duration")
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    spec = importlib.util.spec_from_file_location("build_api", ROOT / "scripts/build-c-abi.py")
    build = importlib.util.module_from_spec(spec); spec.loader.exec_module(build)
    lib = build.build_core("ldc2", out)
    obj, binary = out / "d.o", out / "actor_cabi"
    build.run(["ldc2", "-betterC", "-O2", "-release", "-I.", "-Icabi/source", "-c",
               "tools/perf/actor_cabi.d", "-of=" + str(obj)])
    archive = ROOT / "tests/actors/.build/mimalloc-v3.5.3/libmimalloc.a"
    build.run(["cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-Iinclude",
               "-Ithird_party/mimalloc/include", "tools/perf/actor_cabi.c", obj, lib,
               archive, "-pthread", "-lrt", "-latomic", "-o", binary])
    metadata = {"date": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "platform": platform.platform(),
                "affinity": sorted(os.sched_getaffinity(0)), "repeats": args.repeats,
                "target_seconds": args.seconds, "compiler": subprocess.check_output(["ldc2", "--version"], text=True),
                "scope": "create/copy + idle retire/reclaim; D erased scalar versus C scalar/batch; no callbacks or wave throughput",
                "counting": "separate allocator diagnostics, timing counters disabled; logical copies from one memcpy per actor",
                "limitations": "descriptive microbenchmark, no background-load rejection; pinning uses first allowed logical CPUs"}
    (out / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    if args.build_only: return
    env = {**os.environ, "ANTFARM_HUGE_PAGES": "0"}
    def run(mode, size, creators, remote, mimalloc, rounds, counted):
        cmd = [str(binary), mode, str(size), str(creators), "64", str(rounds),
               str(remote), str(mimalloc), str(counted), "42"]
        result = subprocess.run(cmd, cwd=ROOT, env=env, check=True, capture_output=True, text=True, timeout=45)
        return json.loads(result.stdout)
    summary = []
    with (out / "runs.jsonl").open("w") as log:
        for size, creators, remote, mimalloc in itertools.product((64, 4096), (1, 4), (0, 1), (0, 1)):
            timings = {mode: [] for mode in ("d", "c", "batch")}
            rounds = {}
            for mode in timings:
                diagnostic = run(mode, size, creators, remote, mimalloc, 100, 1)
                log.write(json.dumps({**diagnostic, "stage": "allocation"}) + "\n")
                pilot = run(mode, size, creators, remote, mimalloc, 1000, 0)
                rounds[mode] = max(1000, min(200000, int(1000 * args.seconds / pilot["seconds"])))
            for repeat in range(args.repeats):
                modes = ("d", "c", "batch") if repeat % 2 == 0 else ("batch", "c", "d")
                for mode in modes:
                    row = run(mode, size, creators, remote, mimalloc, rounds[mode], 0)
                    timings[mode].append(row["ns_actor"])
                    log.write(json.dumps({**row, "stage": "timing", "repeat": repeat}) + "\n")
                    log.flush()
            row = dict(bytes=size, creators=creators, remote=remote, mimalloc=mimalloc,
                       ns_actor={k: statistics.median(v) for k, v in timings.items()},
                       ranges={k: [min(v), max(v)] for k, v in timings.items()})
            summary.append(row)
            print(json.dumps(row), flush=True)
            (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
if __name__ == "__main__":
    main()
