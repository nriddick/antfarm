#!/usr/bin/env python3
"""Regenerate the raw numbers behind PERFORMANCE.md.

Runs the committed perftest binaries (and the Fiber benchmarks) over a fixed
matrix, writes every raw process output and a machine-readable summary to an
output directory. Nothing here is checked in: point --out at an ignored or
scratch path.

    python3 perftest/run_performance_md.py --out /tmp/antfarm-perf-report
    python3 perftest/run_performance_md.py --out /tmp/antfarm-perf-report --sections farm_best

A run counts only when its process exits 0 and its metric parses; a cell with
fewer than --runs valid samples is marked failed and the script exits nonzero.
The host must be quiet while this runs. Each cell is measured --runs times
(default 5) and reported as median [min, max]. Every benchmark is run with
ANTFARM_HUGE_PAGES=0 unless a case sets it. Pass --fake-topology when
/sys/devices/system/cpu is absent; the tail and benchmark_mt cells need it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PERFTEST = ROOT / "perftest"
MIMALLOC = ROOT / "actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a"


# ---------------------------------------------------------------- parsing


def _num(s: str) -> float:
    return float(s)


def parse_throughput(out: str) -> dict:
    """First successful throughput/dual row."""
    row = re.compile(
        r"^\s*(\d+)\s+(\d+)\s+(\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+"
        r"([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(\d+)\s*$"
    )
    for line in out.splitlines():
        m = row.match(line)
        if m:
            return {
                "k": int(m[1]), "nc": int(m[2]), "nb_or_nd": int(m[3]),
                "ns": int(m[4]), "body": int(m[5]), "batch": int(m[6]),
                "n": int(m[7]), "sec": _num(m[8]), "Mpps": _num(m[9]),
                "MiB_s": _num(m[10]), "stalls": int(m[11]),
                "metric": _num(m[9]),
            }
    raise ValueError("no throughput row")


def parse_dual(out: str) -> dict:
    """First successful dual-role row."""
    row = re.compile(
        r"^\s*(\d+)\s+(\d+)\s+(\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+"
        r"([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(\d+)\s*$"
    )
    for line in out.splitlines():
        m = row.match(line)
        if m:
            return {
                "k": int(m[1]), "nd": int(m[2]), "tier": m[3], "body": int(m[4]),
                "batch": int(m[5]), "consume": int(m[6]), "n": int(m[7]),
                "sec": _num(m[8]), "Mpps": _num(m[9]), "MiB_s": _num(m[10]),
                "stalls": int(m[11]), "metric": _num(m[9]),
            }
    raise ValueError("no dual row")


def parse_churn(out: str) -> dict:
    d: dict = {}
    m = re.search(r"Mactor_cycles/s=([\d.]+)", out)
    if m:
        d["Mactor_cycles_s"] = _num(m[1])
    m = re.search(r"Mselected_callbacks/s=([\d.]+)", out)
    if m:
        d["Mselected_callbacks_s"] = _num(m[1])
    m = re.search(r"ns/actor_cycle=([\d.]+)", out)
    if m:
        d["ns_per_cycle"] = _num(m[1])
    m = re.search(r"ns/wave=([\d.]+)", out)
    if m:
        d["ns_per_wave"] = _num(m[1])
    m = re.search(r"dispatched_fraction=([\d.]+)", out)
    if m:
        d["dispatched_fraction"] = _num(m[1])
    m = re.search(r"cross_thread_frees=(\d+)", out)
    if m:
        d["cross_thread_frees"] = int(m[1])
    m = re.search(r"cross_thread_free_bytes=(\d+)", out)
    if m:
        d["cross_thread_free_bytes"] = int(m[1])
    m = re.search(r"root_hint=(\S+) root_cost_hint=(\d+) root_hint_table=(\d+) root_hint_claims=(\d+)", out)
    if m:
        d["root_cost_hint"] = int(m[2])
        d["root_hint_table"] = int(m[3])
        d["root_hint_claims"] = int(m[4])
    m = re.search(r"peak_rss_kib=(\d+)", out)
    if m:
        d["peak_rss_kib"] = int(m[1])
    if "Mactor_cycles_s" in d:
        d["metric"] = d["Mactor_cycles_s"]
    elif "Mselected_callbacks_s" in d:
        d["metric"] = d["Mselected_callbacks_s"]
    else:
        raise ValueError("no churn metric")
    return d


def parse_ready(out: str) -> dict:
    m = re.search(r"Mactor/s=([\d.]+)", out)
    if not m:
        raise ValueError("no actor_ready metric")
    return {"Mactor_s": _num(m[1]), "metric": _num(m[1])}


def parse_scaling(out: str) -> dict:
    m = re.search(r"ms=([\d.]+)", out)
    if not m:
        raise ValueError("no scaling metric")
    return {"ms": _num(m[1]), "metric": _num(m[1])}


def parse_tail(out: str) -> dict:
    """All scene rows -> list of dicts; metric is p99 in ns."""
    rows = []
    pat = re.compile(
        r"^\s*(idle|mid-drain|burst|near-full|mailbox)\s+(\d+)\s+(\d+)\s+(\S+)\s+"
        r"(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\d+)\s+(\d+)"
    )
    for line in out.splitlines():
        m = pat.match(line)
        if not m:
            continue
        rows.append({
            "scene": m[1], "nc": int(m[2]), "tlen": int(m[3]), "spin": m[4],
            "n": m[5], "p50_ns": _parse_tail_ns(m[6]), "p99_ns": _parse_tail_ns(m[7]),
            "p999_ns": _parse_tail_ns(m[8]), "max_ns": _parse_tail_ns(m[9]),
            "miss": int(m[10]), "zero": int(m[11]),
        })
    if not rows:
        raise ValueError("no tail rows")
    prim = next((r for r in rows if r["scene"] == "mid-drain" and r["tlen"] == 256
                 and r["spin"] == "1us"), rows[0])
    return {"rows": rows, "metric": prim["p99_ns"]}


def _parse_tail_ns(text: str):
    if text in ("-", ""):
        return None
    m = re.match(r"([\d.]+)(ns|us|ms)", text)
    if not m:
        return None
    v = float(m[1])
    return v * {"ns": 1.0, "us": 1e3, "ms": 1e6}[m[2]]


def parse_fiber(out: str) -> dict:
    d = {}
    for label, pat in (
        ("create_fibers_s", r"^create:\s+\d+ fibers in [\d.]+ s: ([\d.]+) fibers/s"),
        ("run_fibers_s", r"^run\s*:\s+\d+ fibers in [\d.]+ s: ([\d.]+) fibers/s"),
        ("warm_run_fibers_s", r"^run\s+\(warm pool\):\s+\d+ fibers in [\d.]+ s: ([\d.]+) fibers/s"),
    ):
        m = re.search(pat, out, re.M)
        if m:
            d[label] = _num(m[1])
    if "run_fibers_s" not in d:
        raise ValueError("no fiber metric")
    return d | {"metric": d["run_fibers_s"]}


PARSERS = {
    "throughput": parse_throughput,
    "dual": parse_dual,
    "actor_churn": parse_churn,
    "actor_churn_mimalloc": parse_churn,
    "actor_ready": parse_ready,
    "scaling": parse_scaling,
    "tail": parse_tail,
    "dub_fiber": parse_fiber,
}


# ---------------------------------------------------------------- cases


def case(section, name, cmd, cwd=None, env=None, note=""):
    return {"section": section, "name": name, "cmd": cmd, "cwd": cwd,
            "env": env or {}, "note": note}


def cpus_for(consumers):
    return "-" if consumers == 0 else ",".join(str(i) for i in range(consumers + 1))


def churn(section, name, mode, consumers, *, actors=16384, seconds=3, batch=256,
          warmups=5, allocator="mimalloc", tier="small", quota=16384,
          subset_percent=None, root_hint=None, binary="actor_churn_mimalloc",
          unpinned=False, env=None, public_hint=None, child_hint=None, note=""):
    cmd = [str(PERFTEST / binary), mode, str(actors), str(seconds), str(consumers),
           str(batch), str(warmups), allocator,
           "-" if unpinned else cpus_for(consumers), tier, str(quota)]
    if mode.startswith("subset"):
        if subset_percent is not None:
            cmd.append(str(subset_percent))
        if root_hint is not None:
            if subset_percent is None:
                cmd.append("100")
            cmd.append(str(root_hint))
    elif root_hint is not None:
        cmd.append(str(root_hint))
    if public_hint is not None:
        env = dict(env or {})
        env["ANTFARM_PUBLIC_HINT"] = str(public_hint)
    if child_hint is not None:
        env = dict(env or {})
        env["ANTFARM_CHILD_HINT"] = str(child_hint)
    return case(section, name, cmd, env=env, note=note)


def tp(section, name, *, nc, nb=1, ns=0, body=16, batch=80, ac=None, n=None,
       ln=None, k=None, write=None, huge=False, small=None, env=None, note=""):
    cmd = [str(PERFTEST / "throughput"), "--once", "--nc", str(nc), "--nb", str(nb),
           "--ns", str(ns), "--body", str(body), "--batch", str(batch), "--repeats", "1"]
    if ac is not None:
        cmd += ["--ac", str(ac)]
    if n is not None:
        cmd += ["--n", str(n)]
    if ln is not None:
        cmd += ["--ln", str(ln)]
    if k is not None:
        cmd += ["--k", str(k)]
    if write:
        cmd += [write]
    if huge:
        cmd += ["--huge"]
    if small is not None:
        cmd += ["--small", str(small)]
    return case(section, name, cmd, env=env, note=note)


def dcase(section, name, *, nd, body=16, batch=256, consume=1, ac=None, n=None,
          tier=None, huge=False, note=""):
    cmd = [str(PERFTEST / "dual"), "--nd", str(nd), "--body", str(body),
           "--batch", str(batch), "--consume", str(consume), "--repeats", "1"]
    if ac is not None:
        cmd += ["--ac", str(ac)]
    if n is not None:
        cmd += ["--n", str(n)]
    if tier:
        cmd += ["--tier", tier]
    if huge:
        cmd += ["--huge"]
    return case(section, name, cmd, note=note)


def cases_for(section, args):
    C = []
    if section == "farm_consumers":
        for nc in (1, 2, 4, 6, 11):
            C.append(tp("farm_consumers", f"nc{nc}", nc=nc))
    elif section == "farm_chunk_batch":
        for ac in (0, 1, 2, 3):
            for batch in (64, 128, 256, 512, 1024, 2048):
                C.append(tp("farm_chunk_batch", f"ac{ac}_b{batch}", nc=4, ac=ac, batch=batch))
    elif section == "farm_best":
        C.append(tp("farm_best", "headline", nc=8, ns=4, nb=0, body=1, batch=1700,
                    ac=0, n=args.best_n, write="--constant-callandlength",
                    note=">1B items/s headline"))
        C.append(tp("farm_best", "headline_default_chunk", nc=8, ns=4, nb=0, body=1,
                    batch=1700, ac=3, n=args.best_n, write="--constant-callandlength"))
        C.append(tp("farm_best", "single_producer", nc=11, ns=1, nb=0, body=1,
                    batch=1700, ac=0, n=args.best_n, write="--constant-callandlength"))
        C.append(tp("farm_best", "default_shape", nc=4, body=16, batch=80, ac=3,
                    n=16_000_000))
    elif section == "huge":
        C.append(tp("huge", "headline_huge", nc=8, ns=4, nb=0, body=1, batch=1700,
                    ac=0, n=args.best_n, write="--constant-callandlength",
                    huge=True, env={"ANTFARM_HUGE_PAGES": "1"}))
        C.append(churn("huge", "actor_c6_huge", "actor", 6,
                       env={"ANTFARM_HUGE_PAGES": "1"}))
        C.append(churn("huge", "tree_c6_huge", "tree", 6,
                       env={"ANTFARM_HUGE_PAGES": "1"}))
    elif section == "pinning":
        for mode in ("actor", "tree"):
            C.append(churn("pinning", f"{mode}_c6_pinned", mode, 6))
            C.append(churn("pinning", f"{mode}_c6_unpinned", mode, 6, unpinned=True))
    elif section == "dual":
        for nd in (1, 2, 4, 6, 8, 11):
            C.append(dcase("dual", f"nd{nd}", nd=nd, batch=256, ac=3))
        C.append(dcase("dual", "nd6_b256_ac0", nd=6, batch=256, ac=0))
        C.append(dcase("dual", "nd6_b2048_ac0", nd=6, batch=2048, ac=0))
        C.append(dcase("dual", "nd6_b256_ac3", nd=6, batch=256, ac=3))
    elif section == "tail":
        C.append(case("tail", "mid_default",
                      [str(PERFTEST / "tail"), "--mid-only", "--samples", "10000",
                       "--repeats", "1"], note="pinned, synthesized sysfs topology"))
        C.append(case("tail", "idle_default",
                      [str(PERFTEST / "tail"), "--idle-only", "--samples", "10000",
                       "--repeats", "1"]))
    elif section == "public":
        # Public flush/wave default to chunk 8 (ActorRuntime.flush / ActorWave
        # .publish); ANTFARM_PUBLIC_HINT=2 pins chunk 32 at MAX_CHUNK 128.
        for mode in ("actor", "wave", "inbox"):
            for cons in (0, 6):
                C.append(churn("public", f"{mode}_c{cons}_default", mode, cons,
                               note="mimalloc, public flush/wave default chunk"))
                C.append(churn("public", f"{mode}_c{cons}_chunk32", mode, cons,
                               public_hint=2,
                               note="mimalloc, public flush/wave chunk 32 (class 2)"))
        for cons in (1, 2, 4, 6, 11):
            C.append(churn("public", f"actor_curve_c{cons}", "actor", cons))
        C.append(case("public", "actor_ready_default",
                      [str(PERFTEST / "actor_ready"), "16384", "200", "32"],
                      note="autonomous flush from a prequeued burst, default chunk"))
        C.append(case("public", "actor_ready_chunk32",
                      [str(PERFTEST / "actor_ready"), "16384", "200", "32"],
                      env={"ANTFARM_PUBLIC_HINT": "2"},
                      note="autonomous flush from a prequeued burst, chunk 32 (class 2)"))
    elif section == "private":
        modes = [
            ("private", {}), ("private-remote", {}), ("private-tracked", {}),
            ("tree", {}), ("tree-wait", {}), ("tree-cont", {}),
            ("tree-slab", {}), ("tree-wait-slab", {}), ("tree-cont-slab", {}),
            ("tree-refill", {}), ("tree-refill-slab", {}), ("tree-pipelined", {}),
            ("tree-cancel", {}), ("dtor", {}),
            ("subset", {"subset_percent": 1}), ("subset", {"subset_percent": 10}),
            ("subset", {"subset_percent": 100}),
            ("subset-whole", {}), ("subset-whole-unreserved", {}),
        ]
        for mode, kw in modes:
            for cons in (0, 6):
                tag = mode
                if "subset_percent" in kw:
                    tag = f"{mode}{kw['subset_percent']}"
                C.append(churn("private", f"{tag}_c{cons}", mode, cons,
                               note="mimalloc", **kw))
    elif section == "private_tuning":
        # Root-hint: derived default vs explicit classes (chunk 128>>k).
        for mode in ("tree", "private"):
            for batch in (256, 1024):
                C.append(churn("private_tuning", f"{mode}_b{batch}_auto", mode, 6,
                               batch=batch, note="root-hint auto"))
                C.append(churn("private_tuning", f"{mode}_b{batch}_h2", mode, 6,
                               batch=batch, root_hint=2, note="root-hint 2 (chunk 32)"))
                C.append(churn("private_tuning", f"{mode}_b{batch}_h0", mode, 6,
                               batch=batch, root_hint=0, note="root-hint 0 (chunk 128)"))
        for batch in (256, 1024, 2048):
            C.append(churn("private_tuning", f"private_b{batch}", "private", 6,
                           batch=batch, note="batch-limit"))
        # Per-wave child cost hint: the default chunk 32 against chunk 128
        # (class 0) on the tree families.
        for mode in ("tree", "tree-slab"):
            C.append(churn("private_tuning", f"{mode}_child_default", mode, 6,
                           note="child wave default (chunk 32)"))
            C.append(churn("private_tuning", f"{mode}_child_h0", mode, 6,
                           child_hint=0, note="child wave chunk 128 (class 0)"))
    elif section == "allocators":
        for mode in ("actor", "wave", "tree", "private-remote"):
            C.append(churn("allocators", f"{mode}_mimalloc", mode, 6,
                           allocator="mimalloc", binary="actor_churn_mimalloc"))
            C.append(churn("allocators", f"{mode}_crt", mode, 6,
                           allocator="crt", binary="actor_churn"))
        C.append(churn("allocators", "private_emitter", "private", 6,
                       binary="actor_churn_mimalloc", note="emitter-side free"))
        C.append(churn("allocators", "private_count", "private-remote", 6,
                       allocator="mimalloc-count", binary="actor_churn_mimalloc",
                       note="cross-thread free diagnostic"))
        C.append(churn("allocators", "private_emitter_count", "private", 6,
                       allocator="mimalloc-count", binary="actor_churn_mimalloc",
                       note="cross-thread free diagnostic"))
    elif section == "fibers":
        C.append(case("fibers", "benchmark_st",
                      ["dub", "run", "-c", "benchmark", "--build=release",
                       "--compiler=ldc2", "--", "200000", "256", "2"],
                      cwd=str(ROOT / "fibers")))
        C.append(case("fibers", "benchmark_mt",
                      ["dub", "run", "-c", "benchmark_mt", "--build=release",
                       "--compiler=ldc2", "--", "100000"],
                      cwd=str(ROOT / "fibers")))
    elif section == "scaling":
        C.append(case("scaling", "create_16384",
                      [str(PERFTEST / "scaling"), "create", "16384"]))
        C.append(case("scaling", "lifecycle_16384_b32",
                      [str(PERFTEST / "scaling"), "lifecycle", "16384", "32", "handle"]))
    return C


ALL_SECTIONS = ["host", "farm_consumers", "farm_chunk_batch", "farm_best", "dual",
                "tail", "public", "private", "private_tuning", "allocators",
                "huge", "pinning", "fibers", "scaling"]


# ---------------------------------------------------------------- driver


def _loadavg():
    try:
        return Path("/proc/loadavg").read_text().split()[0]
    except OSError:
        return None


def _as_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def kill_process_group(proc):
    """SIGTERM then SIGKILL the child's process group and reap the leader."""
    if proc.poll() is not None:
        return
    for sig, wait in ((signal.SIGTERM, 2), (signal.SIGKILL, 5)):
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            proc.wait(timeout=wait)
            return
        except subprocess.TimeoutExpired:
            continue


def _interrupt_handler(signum, frame):
    # A benchmark runs in its own session and can fork (dub run does), so kill
    # the whole group. Raising runs main()'s finally cleanup.
    proc = _CURRENT_PROC["proc"]
    if proc is not None:
        kill_process_group(proc)
    raise KeyboardInterrupt


def install_signal_handlers():
    previous = {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous[sig] = signal.signal(sig, _interrupt_handler)
    return previous


def restore_signal_handlers(previous):
    for sig, handler in previous.items():
        signal.signal(sig, handler)


_CURRENT_PROC = {"proc": None}


def run_cmd(cmd, cwd, env, timeout):
    """Run one benchmark in its own session. Returns (stdout, stderr, rc)."""
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, env=env,
                            start_new_session=True)
    _CURRENT_PROC["proc"] = proc
    try:
        try:
            out, err = proc.communicate(timeout=timeout)
            return _as_text(out), _as_text(err), proc.returncode
        except subprocess.TimeoutExpired:
            kill_process_group(proc)
            try:
                out, err = proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                out, err = b"", b""
            return _as_text(out), _as_text(err), "timeout"
    finally:
        _CURRENT_PROC["proc"] = None


def parser_for(cmd):
    name = Path(cmd[0]).name
    return PARSERS["dub_fiber" if cmd[0].startswith("dub") else name]


def cell_is_cached(prev, cell, runs):
    """A cached cell is reusable only when it succeeded and matches exactly."""
    if prev is None or prev.get("status") != "ok":
        return False
    if len(prev.get("values", [])) < runs:
        return False
    return (prev.get("cmd") == cell["cmd"] and prev.get("cwd") == cell["cwd"]
            and prev.get("env", {}) == cell["env"])


def _stats(values):
    if not values:
        return None
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def field_stats(parsed, fields):
    return {f: _stats([p[f] for p in parsed if p.get(f) is not None])
            for f in fields if any(p.get(f) is not None for p in parsed)}


def aggregate_tail(parsed):
    """Per-(scene, tlen, spin) medians across the valid runs of one cell."""
    groups = {}
    for p in parsed:
        for row in p.get("rows", []):
            groups.setdefault((row["scene"], row["tlen"], row["spin"]), []).append(row)
    out = []
    for (scene, tlen, spin), rows in groups.items():
        def stats(field):
            vals = [r[field] for r in rows if r.get(field) is not None]
            return _stats(vals)
        out.append({"scene": scene, "tlen": tlen, "spin": spin, "runs": len(rows),
                    "p50_ns": stats("p50_ns"), "p99_ns": stats("p99_ns"),
                    "p999_ns": stats("p999_ns"), "max_ns": stats("max_ns"),
                    "miss_median": statistics.median([r["miss"] for r in rows])})
    out.sort(key=lambda r: (r["scene"], r["tlen"], r["spin"]))
    return out


FIBER_FIELDS = ("create_fibers_s", "run_fibers_s", "warm_run_fibers_s")
SYS_ROOT = Path("/sys")
# Cache sizes are for this host (Ryzen 5 5500); adjust for other CPUs. The
# last field is the sharing scope: "smt" is one SMT-sibling set, "all" is
# every CPU. The L3 entry used to be a False flag that fell through the
# sibling branch, so the tree described one LLC per SMT pair instead of one.
_FAKE_CACHES = ((1, "Data", "32K", "smt"), (1, "Instruction", "32K", "smt"),
                (2, "Unified", "512K", "smt"), (3, "Unified", "16M", "all"))


class TopologyError(Exception):
    """A --fake-topology request that must not proceed."""


def _is_sysfs_mount(root, mounts_path=Path("/proc/self/mounts")):
    try:
        lines = Path(mounts_path).read_text().splitlines()
    except OSError:
        return False
    target = Path(root).resolve()
    for line in lines:
        parts = line.split()
        if len(parts) >= 3 and parts[2] == "sysfs":
            try:
                if Path(parts[1]).resolve() == target:
                    return True
            except OSError:
                continue
    return False


def _topmost_missing(path):
    missing = []
    probe = Path(path)
    while not probe.exists() and probe != probe.parent:
        missing.append(probe)
        probe = probe.parent
    return missing[-1] if missing else None


def _format_cpu_list(cpus):
    """Compact sysfs CPU list: [0, 1, 2, 8] -> "0-2,8"."""
    out = []
    start = prev = None
    for cpu in sorted(set(cpus)):
        if start is None:
            start = prev = cpu
        elif cpu == prev + 1:
            prev = cpu
        else:
            out.append(f"{start}-{prev}" if start != prev else str(start))
            start = prev = cpu
    if start is not None:
        out.append(f"{start}-{prev}" if start != prev else str(start))
    return ",".join(out)


def _parse_cpuinfo(text):
    procs = []
    for block in text.strip().split("\n\n"):
        fields = {k.strip(): v.strip()
                  for line in block.splitlines() if ":" in line
                  for k, v in [line.split(":", 1)]}
        if "processor" in fields:
            procs.append(fields)
    return procs


def fake_topology_plan(sys_root=None, cpuinfo=None, l3_groups=None):
    """File plan for a /proc/cpuinfo-derived sysfs tree, or None if present.

    SMT siblings key on `(physical package, core id)` and CPU lists are
    formatted as ranges, so non-contiguous numbering and multi-package
    layouts are described without assuming adjacency. `l3_groups` optionally
    replaces the single all-CPU L3 set with explicit CPU groups, which a test
    uses for a multi-CCD layout that `/proc/cpuinfo` cannot express.
    """
    sys_root = SYS_ROOT if sys_root is None else sys_root
    cpu_root = Path(sys_root) / "devices" / "system" / "cpu"
    if (cpu_root / "present").exists():
        return None
    if cpuinfo is None:
        cpuinfo = Path("/proc/cpuinfo").read_text()
    procs = _parse_cpuinfo(cpuinfo)
    siblings = {}
    for d in procs:
        package = int(d.get("physical id", "0"))
        core = int(d.get("core id", d["processor"]))
        siblings.setdefault((package, core), []).append(int(d["processor"]))
    all_cpus = [int(d["processor"]) for d in procs]
    l3_of_cpu = ({cpu: group for group in l3_groups for cpu in group}
                 if l3_groups is not None else None)
    plan = [(cpu_root / f, _format_cpu_list(all_cpus) + "\n")
            for f in ("present", "possible", "online")]
    for d in procs:
        cpu = int(d["processor"])
        package = int(d.get("physical id", "0"))
        core = int(d.get("core id", cpu))
        sib = siblings[(package, core)]
        topo = cpu_root / f"cpu{cpu}" / "topology"
        plan += [(topo / "core_id", f"{core}\n"),
                 (topo / "physical_package_id", f"{package}\n"),
                 (topo / "thread_siblings_list", _format_cpu_list(sib) + "\n"),
                 (cpu_root / f"cpu{cpu}" / "online", "1\n")]
        cache = cpu_root / f"cpu{cpu}" / "cache"
        for idx, (level, typ, size, scope) in enumerate(_FAKE_CACHES):
            if level == 3 and l3_of_cpu is not None:
                shared = l3_of_cpu[cpu]
            else:
                shared = sib if scope == "smt" else all_cpus
            plan += [(cache / f"index{idx}" / key, value) for key, value in (
                ("level", f"{level}\n"), ("type", f"{typ}\n"), ("size", f"{size}\n"),
                ("coherency_line_size", "64\n"),
                ("shared_cpu_list", _format_cpu_list(shared) + "\n"),
                ("ways_of_associativity", "8\n"), ("id", f"{idx}\n"))]
    return plan


def _write_plan(plan):
    for path, text in plan:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)


def preflight_fake_topology(sys_root=None):
    """Refuse a --fake-topology request that must not run. No writes."""
    sys_root = SYS_ROOT if sys_root is None else sys_root
    root = Path(sys_root)
    devices = root / "devices"
    cpu_root = devices / "system" / "cpu"
    if _is_sysfs_mount(root):
        raise TopologyError(f"{root} is a real sysfs mount; refusing to synthesize")
    if cpu_root.exists():
        raise TopologyError(f"{cpu_root} already exists; refusing to synthesize")
    if devices.exists():
        raise TopologyError(f"{devices} already exists; refusing to synthesize")


def install_fake_topology(sys_root=None):
    """Create the sysfs tree the binaries hardcode; return the created dir.

    A failure or interruption removes anything this call created.
    """
    sys_root = SYS_ROOT if sys_root is None else sys_root
    devices = Path(sys_root) / "devices"
    preflight_fake_topology(sys_root)
    plan = fake_topology_plan(sys_root)
    if plan is None:
        raise TopologyError(f"{devices}/system/cpu already exists; refusing to synthesize")
    created = _topmost_missing(devices)
    try:
        _write_plan(plan)
    except BaseException:
        remove_fake_topology(created)
        raise
    return created


def remove_fake_topology(created):
    if created is not None:
        shutil.rmtree(created, ignore_errors=True)


def measure_cell(args, cell, raw_dir, section, runner=None):
    """Run one cell --runs times and return its summary row."""
    runner = run_cmd if runner is None else runner
    parse = parser_for(cell["cmd"])
    runs = []
    valid = []
    for i in range(args.runs):
        env = {**os.environ, "ANTFARM_HUGE_PAGES": "0", **cell["env"]}
        load_before = _loadavg()
        started = time.monotonic()
        out, err, rc = runner(cell["cmd"], cell["cwd"] or str(ROOT), env, args.timeout)
        wall = time.monotonic() - started
        load_after = _loadavg()
        (raw_dir / f"{cell['name']}__run{i}.txt").write_text(
            f"$ {' '.join(cell['cmd'])}\n[cwd={cell['cwd'] or ROOT}] rc={rc} "
            f"wall={wall:.3f}s load_before={load_before} load_after={load_after}\n"
            f"=== stdout ===\n{out}\n=== stderr ===\n{err}\n")
        parsed = None
        if rc == 0:
            try:
                parsed = parse(out)
            except (ValueError, KeyError) as exc:
                print(f"  !! {cell['name']} run{i}: parse failed: {exc}", flush=True)
        if parsed is not None and parsed.get("metric") is not None:
            valid.append(parsed)
        runs.append({"rc": rc, "load_before": load_before, "load_after": load_after,
                     "metric": parsed.get("metric") if parsed else None})
        print(f"  {cell['name']} run{i}: rc={rc} "
              f"metric={parsed.get('metric') if parsed else None}", flush=True)
    row = {"section": section, "name": cell["name"], "cmd": cell["cmd"],
           "cwd": cell["cwd"], "env": cell["env"], "note": cell["note"],
           "runs": runs, "valid_runs": len(valid)}
    if valid:
        values = [p["metric"] for p in valid]
        row.update(median=statistics.median(values), minimum=min(values),
                   maximum=max(values), values=values, config=valid[-1])
        stats = field_stats(valid, FIBER_FIELDS)
        if stats:
            row["field_stats"] = stats
        if any("rows" in p for p in valid):
            row["tail_rows"] = aggregate_tail(valid)
    row["status"] = "ok" if len(valid) >= args.runs else "failed"
    return row


def host_metadata():
    def read(p):
        try:
            return Path(p).read_text().strip()
        except OSError:
            return None

    ldc = subprocess.run(["ldc2", "--version"], capture_output=True, text=True).stdout.splitlines()
    dmd = subprocess.run(["dmd", "--version"], capture_output=True, text=True).stdout.splitlines()
    cpuinfo = read("/proc/cpuinfo") or ""
    model = re.search(r"model name\s*:\s*(.+)", cpuinfo)
    return {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "uname": platform.uname()._asdict(),
        "cpu_model": model.group(1) if model else None,
        "logical_cpus": len(re.findall(r"^processor\s*:", cpuinfo, re.M)),
        "loadavg_before": read("/proc/loadavg"),
        "governor": [read(f"/sys/devices/system/cpu/cpu{i}/cpufreq/scaling_governor")
                     for i in range(12)],
        "thp_enabled": read("/sys/kernel/mm/transparent_hugepage/enabled"),
        "compilers": {"ldc2": ldc[:2], "dmd": dmd[:2]},
        "mimalloc_archive": str(MIMALLOC),
        "mimalloc_sha256": hashlib.sha256(MIMALLOC.read_bytes()).hexdigest()
        if MIMALLOC.exists() else None,
        "git_rev": subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"],
                                  capture_output=True, text=True).stdout.strip(),
        "git_status": subprocess.run(["git", "-C", str(ROOT), "status", "--porcelain"],
                                     capture_output=True, text=True).stdout.splitlines(),
    }


def build_parser():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=Path("/tmp/antfarm-perf-report"))
    ap.add_argument("--sections", default=",".join(ALL_SECTIONS))
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--best-n", type=int, default=2_000_000_000)
    ap.add_argument("--timeout", type=float, default=600.0)
    ap.add_argument("--no-resume", action="store_true",
                    help="rerun cells even if summary.json already has them")
    ap.add_argument("--fake-topology", action="store_true",
                    help="create the hardcoded /sys/devices/system/cpu tree from "
                         "/proc/cpuinfo only when /sys/devices is entirely absent; "
                         "refuses on a real sysfs mount; needed by tail and "
                         "benchmark_mt")
    return ap


def main(argv=None):
    ap = build_parser()
    args = ap.parse_args(argv)
    if args.runs < 1:
        ap.error("--runs must be at least 1")
    sections = list(dict.fromkeys(s for s in args.sections.split(",") if s))
    for s in sections:
        if s not in ALL_SECTIONS:
            ap.error(f"unknown section {s!r}")
    # Refuse a fake topology before creating or overwriting any output file.
    if args.fake_topology:
        try:
            preflight_fake_topology()
        except TopologyError as exc:
            print(f"run_performance_md: {exc}", file=sys.stderr)
            return 2
    args.out.mkdir(parents=True, exist_ok=True)
    raw = args.out / "raw"
    raw.mkdir(exist_ok=True)

    meta = host_metadata()
    (args.out / "metadata.json").write_text(json.dumps(meta, indent=2) + "\n")

    results = []
    summary_path = args.out / "summary.json"
    if summary_path.exists():
        try:
            loaded = json.loads(summary_path.read_text())
            if not isinstance(loaded, list):
                raise ValueError("summary is not a list")
            results = loaded
        except (ValueError, OSError) as exc:
            print(f"warning: ignoring unreadable {summary_path}: {exc}", file=sys.stderr)
            results = []
    by_key = {(r["section"], r["name"]): r for r in results}

    def flush():
        summary_path.write_text(json.dumps(results, indent=2) + "\n")

    previous_signals = install_signal_handlers()
    created = None
    failed = False
    try:
        if args.fake_topology:
            try:
                created = install_fake_topology()
            except TopologyError as exc:
                print(f"run_performance_md: {exc}", file=sys.stderr)
                return 2
        for section in sections:
            if section == "host":
                continue
            cells = cases_for(section, args)
            (raw / section).mkdir(exist_ok=True)
            print(f"\n===== {section} ({len(cells)} cells x {args.runs} runs) =====",
                  flush=True)
            for cell in cells:
                prev = by_key.get((section, cell["name"]))
                if not args.no_resume and cell_is_cached(prev, cell, args.runs):
                    print(f"  {cell['name']}: cached ({len(prev['values'])} runs)", flush=True)
                    continue
                row = measure_cell(args, cell, raw / section, section)
                if row["status"] != "ok":
                    failed = True
                if prev is not None:
                    results[results.index(prev)] = row
                else:
                    results.append(row)
                flush()
    finally:
        remove_fake_topology(created)
        restore_signal_handlers(previous_signals)
    # Stable order: section order, then case order.
    order = {s: i for i, s in enumerate(ALL_SECTIONS)}
    index = {}
    for s in ALL_SECTIONS:
        for i, c in enumerate(cases_for(s, args)):
            index[(s, c["name"])] = i
    results.sort(key=lambda r: (order.get(r["section"], 99),
                                index.get((r["section"], r["name"]), 0)))
    flush()
    failures = sum(1 for r in results if r.get("status") == "failed")
    print(f"\nwrote {args.out}/summary.json ({failures} failed cells)", flush=True)
    return 1 if (failed or failures) else 0


if __name__ == "__main__":
    sys.exit(main())
