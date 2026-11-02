"""Run file_loading configurations interleaved, several processes each, and
summarize: throughput medians over every round of every process, latency
quantiles as the median and the worst across processes. A process that
reports a GC collection fails the run.

    python3 -B file_loading_matrix.py BINARY DATA OUT.json PROCESSES CONFIG...

A CONFIG is NAME=ARGS, with ARGS space-separated, and optionally ENV:K=V
entries (ENV:LD_LIBRARY_PATH=/x).
"""
import json
import os
import re
import statistics
import subprocess
import sys

binary, data, out, processes = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
configs = []
for spec in sys.argv[5:]:
    name, args = spec.split("=", 1)
    env = dict(os.environ)
    argv = []
    for word in args.split():
        if word.startswith("ENV:"):
            k, v = word[4:].split("=", 1)
            env[k] = v
        else:
            argv.append(word)
    configs.append((name, argv, env))

line = re.compile(r"^(RESULT|LATENCY) (.*)$")
samples = {}   # (config, kind, scenario) -> list of dicts
for p in range(processes):
    for name, argv, env in configs:
        cmd = [binary, "--dir", data, "--rounds", "3", *argv]
        r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=1800)
        if r.returncode:
            print(f"{name} process {p} failed:\n{r.stderr}", file=sys.stderr)
            sys.exit(1)
        for text in r.stdout.splitlines():
            gc = re.match(r"^GC impl=\S+ collections=(\d+)$", text)
            if gc and int(gc.group(1)) != 0:
                print(f"{name} process {p}: {gc.group(1)} GC collections", file=sys.stderr)
                sys.exit(1)
            m = line.match(text)
            if not m:
                continue
            fields = dict(kv.split("=", 1) for kv in m.group(2).split())
            fields["process"] = p
            samples.setdefault((name, m.group(1), fields["scenario"]), []).append(fields)
        print(f"process {p + 1}/{processes} {name} done", flush=True)

summary = []
for (name, kind, scenario), rows in samples.items():
    cpu = [float(r["cpu_per_wall"]) for r in rows if "cpu_per_wall" in r]
    if kind == "RESULT":
        def med(k):
            return statistics.median(float(r[k]) for r in rows)
        summary.append(dict(config=name, kind="throughput", scenario=scenario, samples=len(rows),
                            wall_ms=med("wall_ms"), mib_per_s=med("mib_per_s"), ops_per_s=med("ops_per_s"),
                            submit_us_per_op=med("submit_us_per_op"),
                            handle_us_per_op=med("handle_us_per_op"),
                            mib_per_s_min=min(float(r["mib_per_s"]) for r in rows),
                            mib_per_s_max=max(float(r["mib_per_s"]) for r in rows),
                            cpu_per_wall=statistics.median(cpu), cpu_per_wall_max=max(cpu)))
    else:
        entry = dict(config=name, kind="latency", scenario=scenario, samples=len(rows),
                     cpu_per_wall=statistics.median(cpu), cpu_per_wall_max=max(cpu))
        for q in ("p50_us", "p90_us", "p99_us", "p999_us"):
            values = [float(r[q]) for r in rows]
            entry[q] = statistics.median(values)
            entry[q + "_worst"] = max(values)
        summary.append(entry)

with open(out, "w") as f:
    json.dump(dict(configs=[(n, a) for n, a, _ in configs], processes=processes, summary=summary,
                   raw=[dict(config=k[0], kind=k[1], **r) for k, rows in samples.items() for r in rows]),
              f, indent=1)

for e in sorted(summary, key=lambda e: (e["kind"], e["scenario"], e["config"])):
    if e["kind"] == "throughput":
        print(f"{e['scenario']:15} {e['config']:22} {e['mib_per_s']:9.1f} MiB/s ({e['mib_per_s_min']:.0f}-{e['mib_per_s_max']:.0f}) "
              f"{e['ops_per_s']:9.0f} op/s  submit {e['submit_us_per_op']:8.2f} us/op  handle {e['handle_us_per_op']:8.2f} us/op"
              f"  cpu/wall {e['cpu_per_wall']:.2f} (max {e['cpu_per_wall_max']:.2f})")
    else:
        print(f"{e['scenario']:15} {e['config']:22} p50 {e['p50_us']:9.2f} (worst {e['p50_us_worst']:9.2f})  "
              f"p99 {e['p99_us']:9.2f} (worst {e['p99_us_worst']:9.2f})  p99.9 {e['p999_us']:9.2f} us"
              f"  cpu/wall {e['cpu_per_wall']:.2f} (max {e['cpu_per_wall_max']:.2f})")
