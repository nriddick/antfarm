#!/usr/bin/env python3
"""Apply each F3 T-mutation alone to a copy of the sources, build LDC debug and
-release, run its selector and record how it fails. The worktree is not edited."""
import json, os, shutil, subprocess, sys, time
from pathlib import Path

root = Path(__file__).resolve().parents[3]
work = Path("/tmp/f3/mut")
sources = ["private_actor_tests/main.d", "antfarm.d", "antfarm_allocation.d",
           "actors/actor.d", "actors/wave.d", "actors/private_actor.d"]
RT = "actors/private_actor.d"
TEST = "private_actor_tests/main.d"

CANCEL_CAS = """            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(&record_.control, &w,
                    w | controlCancelled | causeBits(PrivateCancelCause.requested)))
                return PrivateCancelResult.requested;
"""
TERMINAL_LOOP = """        while (true)
        {
            version (AntfarmActorTestHooks)
                actorTestPoint(ActorTestPoint.privateRootTerminalBeforeCas, null);
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(&record.control, &w, w & ~controlLive))
                break;
        }
"""
STATUS = """        record.status = record.failed ? PrivateOutcomeStatus.failed
            : (w & controlCancelled) ? PrivateOutcomeStatus.cancelled
            : PrivateOutcomeStatus.completed;
"""
M = {
    # T1a: the canceller does a load+store instead of the CAS.
    "T1a": ("root-t1", [(RT, CANCEL_CAS,
        "            atomicStore!(MemoryOrder.rel)(record_.control, w | controlCancelled"
        " | causeBits(PrivateCancelCause.requested)); // MUTATION\n"
        "            return PrivateCancelResult.requested;\n")]),
    # T1b: take recycles the slot without bumping the generation.
    "T1b": ("root-t1", [(RT, "(record.control, (generation + 1) << controlGenerationShift);",
        "(record.control, generation << controlGenerationShift); // MUTATION")]),
    # T2: the terminal transition is a plain store from the first load.
    "T2": ("root-t2", [(RT, TERMINAL_LOOP,
        "        version (AntfarmActorTestHooks)\n"
        "            actorTestPoint(ActorTestPoint.privateRootTerminalBeforeCas, null);\n"
        "        atomicStore!(MemoryOrder.rel)(record.control, w & ~controlLive); // MUTATION\n")]),
    # T3: the emitter's (and members') scope check skips the root record.
    "T3": ("root-t3", [(RT, "    if (base.tracked && rootCancelledNow(base.service, root)) return true;\n", "")]),
    # T4: the armed flag is ignored; every check reads the clock.
    "T4": ("root-t4", [(RT, "    if (!(w & controlArmed) || privateClockTicks() < record.deadline) return false;\n",
        "    if (privateClockTicks() < record.deadline) return false; // MUTATION\n")]),
    # T5: the slot is recycled at collection.
    "T5": ("root-t5", [(RT, "            retainOutcome(entry);\n            return;\n", "            retainOutcome(entry);\n")]),
    # T7a (test side, documented recipe): drain without takeOutcome.
    "T7a": ("root-t7", [(TEST, "        PrivateRootOutcome o;\n        while (park.takeOutcome(o)) taken ~= o;\n        namedCheck(MonoTime.currTime < deadline, name);\n    }\n}\n\n// The same with the lockstep",
        "        namedCheck(MonoTime.currTime < deadline, name);\n    }\n}\n\n// The same with the lockstep")]),
    # T7b: cancelled outranks failed.
    "T7b": ("root-t7", [(RT, STATUS,
        "        record.status = (w & controlCancelled) ? PrivateOutcomeStatus.cancelled // MUTATION\n"
        "            : record.failed ? PrivateOutcomeStatus.failed\n"
        "            : PrivateOutcomeStatus.completed;\n")]),
    # T7c: a root's later fail overwrites the first code.
    "T7c": ("root-t7", [(RT, "            if (!record.failed)\n            {\n                record.failed = true;\n                record.failureCode = code;\n            }\n",
        "            record.failed = true; // MUTATION\n            record.failureCode = code;\n")]),
    # T7d: cancelAll ignores expired deadlines (always cause requested).
    "T7d": ("root-t7", [(RT, "                    if (now >= record.deadline) cause = PrivateCancelCause.deadline;\n", "")]),
    # T7e: cancelAll also marks held (finished) outcomes.
    "T7e": ("root-t7", [(RT, "            while ((w & controlLive) && !(w & controlCancelled))\n",
        "            while (!(w & controlCancelled)) // MUTATION\n")]),
    # T9: no generation wrap check.
    "T9": ("root-tracking-death", [(RT, "        if (generation == maxRootGeneration) fatal(\"private root generation wrap\");\n", "")]),
}

def build(src_dir, out, flags):
    cmd = ["ldc2", "-g", "-O1", *flags, "-d-version=AntfarmWriteAuditHooks",
           "-d-version=AntfarmActorTestHooks", "-I.", *sources, f"-of={out}"]
    return subprocess.run(cmd, cwd=src_dir, capture_output=True, text=True)

def main():
    names = sys.argv[1:] or list(M)
    results = {}
    for name in names:
        selector, edits = M[name]
        src_dir = work / name
        if src_dir.exists():
            shutil.rmtree(src_dir)
        for rel in sources + ["actors/package.d", "actors/mimalloc.d"]:
            (src_dir / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(root / rel, src_dir / rel)
        for rel, old, new in edits:
            path = src_dir / rel
            text = path.read_text()
            assert text.count(old) == 1, (name, old)
            path.write_text(text.replace(old, new))
        for flags in ([], ["-release"]):
            label = name + ("-release" if flags else "")
            out = str(src_dir / ("mut" + ("_release" if flags else "")))
            b = build(src_dir, out, flags)
            if b.returncode:
                results[label] = dict(build_failed=b.stderr[-2000:])
                print(label, "BUILD FAILED", b.stderr[-800:], flush=True)
                continue
            t0 = time.time()
            try:
                r = subprocess.run([out, selector], cwd=src_dir / "private_actor_tests", capture_output=True,
                                   text=True, timeout=120, env={**os.environ, "ANTFARM_HUGE_PAGES": "0"})
                status, tail = r.returncode, (r.stdout + r.stderr).strip().splitlines()[-3:]
            except subprocess.TimeoutExpired:
                status, tail = "timeout(120s)", []
            results[label] = dict(selector=selector, flags=" ".join(flags) or "debug", status=status,
                                  tail=tail, seconds=round(time.time() - t0, 1))
            print(label, selector, status, tail, flush=True)
    (work / "results.json").write_text(json.dumps(results, indent=2))

main()
