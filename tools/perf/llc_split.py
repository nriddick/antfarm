#!/usr/bin/env python3
"""Headline payloads and private child lifecycles, laid out per LLC group.

Applies the "Several physical groups" rules of docs/guide/hosting.md to the
two headline workloads that cross the CCD boundary: every Farm's threads live
in one last-level-cache group, and a thread count larger than one group runs
as one independent Farm per group (the guide's "one process per group" form),
started together, with nothing shared between them. Each cell is measured in
both layouts in the same session:

  headline  the PERFORMANCE.md headline command: payloads unpinned, private
            children pinned in placement order across groups;
  per-llc   the threads split evenly over as few groups as hold them; each
            group's share is one process confined to that group.

    python3 -B tools/perf/llc_split.py --out .build/llc-split --runs 5

A per-llc payload sample is the total payloads of every process over the
longest process time (a lower bound, since all start together); a private
sample is the sum of the processes' rates over their equal fixed windows.
Every part must exit 0 and parse for the sample to count. Linux only.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_performance_md as rpm  # noqa: E402


def llc_groups(sys_root=Path("/sys")):
    """Logical CPUs per L3 group, each in placement order (cores, then SMT
    siblings), groups ordered by their lowest CPU."""
    order = rpm.cpu_order(sys_root)
    base = sys_root / "devices/system/cpu"
    groups = {}
    for cpu in order:
        path = base / f"cpu{cpu}/cache/index3/shared_cpu_list"
        llc = path.read_text().strip() if path.exists() else "all"
        groups.setdefault(llc, []).append(cpu)
    return list(groups.values())


def split(threads, groups):
    """Thread shares per group: one group when it holds them all, otherwise
    an even split over the fewest groups that do (all groups when even they
    cannot, oversubscribing each evenly). Returns [(share, group cpus)]."""
    size = len(groups[0])
    need = min(len(groups), -(-threads // size))
    base, extra = divmod(threads, need)
    return [(base + (i < extra), groups[i]) for i in range(need)]


def cpu_list(cpus):
    return ",".join(str(c) for c in cpus)


def payload_cmd(threads, n):
    producers, consumers = rpm.payload_split(threads)
    return [rpm.benchmark_path("throughput"), "--once", "--nc", str(consumers), "--nb", "0",
            "--ns", str(producers), "--body", "1", "--batch", "1700", "--repeats", "1",
            "--ac", "0", "--n", str(n), "--constant-callandlength"]


def private_cmd(threads, pins):
    return [rpm.benchmark_path("actor_churn_mimalloc"), "tree-refill-slab", "16384", "3",
            str(threads - 1), "256", "5", "mimalloc", pins, "small", "16384"]


def parts_for(workload, layout, threads, groups, n):
    """The commands one sample runs concurrently."""
    if layout == "headline":
        if workload == "payload":
            return [payload_cmd(threads, n)]
        order = [c for g in groups for c in g]
        return [private_cmd(threads, rpm.pins_for(threads, order))]
    parts = []
    for share, cpus in split(threads, groups):
        fits = share <= len(cpus)
        confine = ["taskset", "-c", cpu_list(cpus[:share] if fits else cpus)]
        if workload == "payload":
            parts.append(confine + payload_cmd(share, n))
        else:
            pins = cpu_list(cpus[:share]) if fits else "-"
            parts.append(confine + private_cmd(share, pins))
    return parts


def combine(workload, parsed):
    if workload == "payload":
        return sum(p["n"] for p in parsed) / max(p["sec"] for p in parsed) / 1e6
    return sum(p["metric"] for p in parsed)


def run_parts(parts, env, timeout):
    """Start every part together, each in its own session; kill all on a
    timeout or interrupt. Returns [(stdout, stderr, rc, wall)]."""
    procs = []
    try:
        for cmd in parts:
            procs.append((time.monotonic(), subprocess.Popen(
                cmd, cwd=str(rpm.ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, env=env, start_new_session=True)))
        deadline = time.monotonic() + timeout
        results = []
        for started, proc in procs:
            try:
                out, err = proc.communicate(timeout=max(0.0, deadline - time.monotonic()))
                rc = proc.returncode
            except subprocess.TimeoutExpired:
                rpm.kill_process_group(proc)
                out, err = proc.communicate()
                rc = "timeout"
            results.append((out, err, rc, time.monotonic() - started))
        return results
    finally:
        for _, proc in procs:
            if proc.poll() is None:
                rpm.kill_process_group(proc)


def measure(args, raw_dir):
    groups = llc_groups()
    cells = [(w, layout, t) for w in ("payload", "private") for t in args.threads
             for layout in ("headline", "per-llc")]
    samples = {c: [] for c in cells}
    attempts = {c: [] for c in cells}
    env = {**os.environ, "ANTFARM_HUGE_PAGES": "0"}
    for r in range(args.runs):  # rounds outermost, so drift spreads over every cell
        for cell in cells:
            workload, layout, t = cell
            parts = parts_for(workload, layout, t, groups, args.n)
            load = rpm._loadavg()
            results = run_parts(parts, env, args.timeout)
            name = f"{workload}_{layout}_t{t}__run{r}"
            (raw_dir / f"{name}.txt").write_text("".join(
                f"$ {' '.join(cmd)}\nrc={rc} wall={wall:.3f}s load_before={load}\n"
                f"=== stdout ===\n{out}\n=== stderr ===\n{err}\n"
                for cmd, (out, err, rc, wall) in zip(parts, results)))
            parse = rpm.parse_throughput if workload == "payload" else rpm.parse_churn
            parsed = []
            for out, _, rc, _ in results:
                try:
                    parsed.append(parse(out) if rc == 0 else None)
                except (ValueError, KeyError):
                    parsed.append(None)
            ok = all(p is not None for p in parsed)
            value = combine(workload, parsed) if ok else None
            per_part = [p["metric"] if p else None for p in parsed]
            attempts[cell].append({"rc": [res[2] for res in results], "load_before": load,
                                   "parts": per_part, "metric": value})
            if ok:
                samples[cell].append(value)
            print(f"  {name}: {value if value is None else round(value, 1)} "
                  f"parts={[None if v is None else round(v, 1) for v in per_part]}", flush=True)
    rows = []
    for cell in cells:
        workload, layout, t = cell
        values = samples[cell]
        row = {"workload": workload, "layout": layout, "threads": t,
               "parts": parts_for(workload, layout, t, groups, args.n),
               "attempts": attempts[cell], "valid_runs": len(values),
               "requested_runs": args.runs,
               "status": "ok" if len(values) == args.runs else "failed"}
        if values:
            row.update(median=statistics.median(values),
                       stddev=statistics.stdev(values) if len(values) > 1 else 0.0,
                       values=values)
        rows.append(row)
    return groups, rows


def write_markdown(rows, threads, path):
    by = {(r["workload"], r["layout"], r["threads"]): r for r in rows}
    lines = ["| Threads | Payloads, headline, Mpps | Payloads, per LLC, Mpps | "
             "Private children, headline, Mcycles/s | Private children, per LLC, Mcycles/s |",
             "| ---: | ---: | ---: | ---: | ---: |"]
    for t in threads:
        cells = [rpm._cell(by.get((w, layout, t)), digits=1)
                 for w in ("payload", "private") for layout in ("headline", "per-llc")]
        lines.append(f"| {t} | " + " | ".join(cells) + " |")
    path.write_text("\n".join(lines) + "\n")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--threads", type=rpm.thread_counts, default=[2, 4, 8, 16, 20, 24, 32, 40])
    ap.add_argument("--n", type=int, default=2_000_000_000,
                    help="payloads per payload process")
    ap.add_argument("--timeout", type=float, default=600.0)
    args = ap.parse_args(argv)
    if any(t < 2 for t in args.threads):
        ap.error("thread counts must be at least 2")
    args.out.mkdir(parents=True, exist_ok=False)
    raw_dir = args.out / "raw"
    raw_dir.mkdir()
    meta = rpm.host_metadata()
    groups, rows = measure(args, raw_dir)
    (args.out / "summary.json").write_text(json.dumps(
        {"metadata": meta, "llc_groups": groups, "runs": args.runs, "n": args.n,
         "results": rows}, indent=2))
    write_markdown(rows, args.threads, args.out / "tables.md")
    failed = [r for r in rows if r["status"] != "ok"]
    for r in failed:
        print(f"FAILED {r['workload']} {r['layout']} t{r['threads']}: "
              f"{r['valid_runs']} / {r['requested_runs']}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
