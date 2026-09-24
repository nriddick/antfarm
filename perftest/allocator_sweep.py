#!/usr/bin/env python3
"""Serial, pinned actor-lifecycle allocator comparison on Linux.

Build actor_churn_mimalloc first. Alternative mallocs use LD_PRELOAD only in
their child process; the benchmark reports the resolved allocation libraries.
"""

import argparse
import ctypes.util
import json
import os
from pathlib import Path
import random
import re
import statistics
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path,
                        default=Path(__file__).resolve().with_name("actor_churn_mimalloc"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpus", action="append", required=True,
                        help="controller then consumers; repeat for each placement")
    parser.add_argument("--actors", type=int, default=16384)
    parser.add_argument("--seconds", type=float, default=2)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.actors < 1 or args.seconds <= 0 or args.repeats < 1:
        parser.error("actors, seconds, and repeats must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    binary = args.binary.resolve()
    env = {k: v for k, v in os.environ.items()
           if k not in ("LD_PRELOAD", "GLIBC_TUNABLES")
           and not k.startswith(("MALLOC_", "MIMALLOC_", "TCMALLOC_", "TBB_MALLOC"))}
    env["ANTFARM_HUGE_PAGES"] = "0"
    configs = [(name, policy, None) for name, policy in (
        ("system-64", "crt"), ("system", "malloc"),
        ("mimalloc", "mimalloc"), ("mimalloc-64", "mimalloc64"),
        ("pool", "pool"), ("arena", "arena"))]
    for name, library in (("jemalloc", "jemalloc"),
                          ("tcmalloc", "tcmalloc_minimal"),
                          ("tbbmalloc", "tbbmalloc_proxy")):
        resolved = ctypes.util.find_library(library)
        if resolved is None:
            print(f"Skipping {name}: {library} is not installed.", flush=True)
            continue
        configs.extend(((name, "malloc", resolved), (name + "-64", "crt", resolved)))

    # Establish the unpreloaded provider before randomizing the measured runs.
    probe = subprocess.run(
        [str(binary), "actor", "1", "0.01", "0", "1", "1", "malloc",
         args.cpus[0].split(",")[0]],
        env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        check=True, timeout=30)
    provider_pattern = r"(malloc|free|aligned_alloc)_provider=(\S+)"
    system_providers = dict(re.findall(provider_pattern, probe.stdout))
    if len(system_providers) != 3:
        raise RuntimeError("benchmark did not identify the system allocation functions")
    (args.output / "configuration.json").write_text(json.dumps({
        "binary": str(binary), "actors": args.actors, "seconds": args.seconds,
        "repeats": args.repeats, "cpus": args.cpus, "configs": configs,
        "system_providers": system_providers,
        "environment": "ordinary pages; allocator tuning variables removed",
    }, indent=2) + "\n")

    rng = random.Random(0xA110C)
    records = []
    for cpus in args.cpus:
        consumers = len(cpus.split(",")) - 1
        for mode in ("actor", "wave"):
            batch = 256 if mode == "actor" else args.actors
            for repeat in range(args.repeats):
                order = configs.copy()
                rng.shuffle(order)
                for name, policy, preload in order:
                    command = [str(binary), mode, str(args.actors), str(args.seconds),
                               str(consumers), str(batch), "5", policy, cpus]
                    child_env = dict(env)
                    if preload:
                        child_env["LD_PRELOAD"] = preload
                    run = subprocess.run(command, env=child_env, text=True,
                                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                         timeout=max(120, args.seconds + 90))
                    case = f"{mode}-{cpus.replace(',', '_')}-{name}"
                    (args.output / f"{case}-{repeat + 1}.log").write_text(run.stdout)
                    if run.returncode:
                        raise RuntimeError((command, run.returncode, run.stdout))
                    providers = dict(re.findall(provider_pattern, run.stdout))
                    if preload:
                        if len(providers) != 3 or not all(
                                Path(preload).name in value for value in providers.values()):
                            raise RuntimeError(f"{name} did not replace all C allocators: {providers}")
                    elif providers != system_providers:
                        raise RuntimeError(f"unexpected C allocator: {providers}")
                    metrics = {k: float(v) for k, v in
                               re.findall(r"([\w/]+)=([\d.]+)", run.stdout)}
                    records.append(dict(case=case, mode=mode, cpus=cpus,
                                        allocator=name, policy=policy, preload=preload,
                                        repeat=repeat + 1, command=command,
                                        providers=providers, metrics=metrics))
                    (args.output / "raw.json").write_text(json.dumps(records, indent=2) + "\n")
                    print(case, repeat + 1, metrics["Mactor_cycles/s"], flush=True)

    summary = {}
    for case in dict.fromkeys(r["case"] for r in records):
        rows = [r["metrics"] for r in records if r["case"] == case]
        rates = [r["Mactor_cycles/s"] for r in rows]
        summary[case] = dict(
            samples=rates, median=statistics.median(rates),
            phase_ns_per_actor={
                key: statistics.median(r[key] * 1e9 / (r["actors"] * r["rounds"]) for r in rows)
                for key in ("create_s", "dispatch_s", "retire_reclaim_verify_s")})
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Completed {len(records)} verified runs; results in {args.output}", flush=True)


if __name__ == "__main__":
    main()
