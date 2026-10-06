#!/usr/bin/env python3
"""Apply each F2 C-mutation alone, build LDC (debug; some also -release), run its selector, restore."""
import subprocess, sys, json, os, time
from pathlib import Path
root = Path("/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree")
src = root / "actors/private_actor.d"
orig = src.read_text()
T = "        dispatched_ = cursor_; // BEFORE the seal\n        if (retire_) return false;\n"
SEAL_TAIL = "        releaseWave(wave); // seal: last park access\n        return true;\n    }\n    static bool emitResume"
M = {
 "C1a": ("cancel-c1", [("        dispatched_ = cursor_; // BEFORE the seal\n", "")], ["", "-release"]),
 "C1b": ("cancel-c1", [(SEAL_TAIL, "        return true; // MUTATION: no seal\n    }\n    static bool emitResume")], [""]),
 "C2a": ("cancel-c2", [("        if (retire_) return false;\n", "")], [""]),
 "C2b": ("cancel-c2", [("        if (retire_) return false;\n", ""),
          ("        if (park.retire_) park.count_ = 0;\n",
           "        if (park.retire_) foreach (ref e; park.entries_[park.dispatched_ .. park.count_]) retireMember!T(park.base_.service, &e);\n        if (park.retire_) park.count_ = 0;\n")], [""]),
 "C3": ("cancel-c3", [("            cleanupHeader = park.header_;\n            cleanupHeader.call = &runSkippedRetire;\n            header = &cleanupHeader;\n",
          "            park.header_.call = &runSkippedRetire;\n")], [""]),
 "C4": ("cancel-c4", [("    atomicStore!(MemoryOrder.rel)(ChildBase.fromWave(w).root.subtreeCancel, true);\n", "")], [""]),
 "C5": ("cancel-c5", [("        atomicStore!(MemoryOrder.raw)(entry.subtreeCancel, false);\n", "")], [""]),
 "C6": ("cancel-c6", [("        atomicStore!(MemoryOrder.raw)(wave.cancel, false);\n", "")], [""]),
 "C7": ("cancel-c7", [(T + "        version (AntfarmActorTestHooks)\n            actorTestPoint(ActorTestPoint.privateTruncateBeforeSeal, null);\n" + SEAL_TAIL,
          "        if (retire_) { dispatched_ = cursor_; return false; }\n        version (AntfarmActorTestHooks)\n            actorTestPoint(ActorTestPoint.privateTruncateBeforeSeal, null);\n        releaseWave(wave);\n        dispatched_ = cursor_; // MUTATION: after the seal\n        return true;\n    }\n    static bool emitResume")], [""]),
 "C8": ("cancel-c8", [("        cancelWave(enclosing_);\n    }\n",
          "        cancelWave(enclosing_);\n        auto mbase = ChildBase.fromWave(enclosing_);\n        if (mbase.parent is mbase.root) releaseRoot(mbase.root); // MUTATION: early wake\n    }\n")], [""]),
 "C9": ("cancel-c9", [("        version (AntfarmActorTestHooks)\n            actorTestPoint(ActorTestPoint.privateEmitBeforeSeal, null);\n        releaseWave(wave); // seal: last access, including park and its owner\n",
          "        version (AntfarmActorTestHooks)\n            actorTestPoint(ActorTestPoint.privateEmitBeforeSeal, null);\n        if (scopeCancelled(wave)) { park.dispatched_ = park.cursor_; releaseWave(wave); } // MUTATION\n        releaseWave(wave); // seal: last access, including park and its owner\n")], [""]),
 "C12": ("cancel-c12", [("        resetOutcome(k_);\n", "        resetOutcome(count_);\n")], [""]),
 "C13": ("cancel-c13", [("            return resultCount_ < dispatched_ ? resultCount_ : dispatched_;\n",
          "            return resultCount_ < count_ ? resultCount_ : count_;\n")], ["", "-release"]),
}
sources = ["private_actor_tests/main.d", "antfarm.d", "antfarm_allocation.d", "actors/actor.d", "actors/wave.d", "actors/private_actor.d"]
names = sys.argv[1:] or list(M)
results = {}
try:
    for name in names:
        selector, edits, flagsets = M[name]
        text = orig
        for a, b in edits:
            assert text.count(a) == 1, (name, a)
            text = text.replace(a, b)
        src.write_text(text)
        for flags in flagsets:
            out = f"/tmp/f2/mut/{name}{flags}"
            cmd = ["ldc2", "-g", "-O1"] + ([flags] if flags else []) + ["-d-version=AntfarmWriteAuditHooks", "-d-version=AntfarmActorTestHooks", "-I.", *sources, f"-of={out}"]
            b = subprocess.run(cmd, cwd=root, capture_output=True, text=True)
            if b.returncode:
                results[name + flags] = dict(build_failed=b.stderr[-2000:]); print(name, flags, "BUILD FAILED", b.stderr[-800:]); continue
            t0 = time.time()
            try:
                r = subprocess.run([out, selector], cwd=root, capture_output=True, text=True, timeout=90, env={**os.environ, "ANTFARM_HUGE_PAGES": "0"})
                status, tail = r.returncode, (r.stdout + r.stderr).strip().splitlines()[-3:]
            except subprocess.TimeoutExpired as e:
                status, tail = "timeout(90s)", [(e.stdout or b"").decode()[-200:] if isinstance(e.stdout, bytes) else str(e.stdout)[-200:]]
            results[name + flags] = dict(selector=selector, flags=flags or "debug", status=status, tail=tail, seconds=round(time.time()-t0, 1))
            print(name, flags or "debug", selector, status, tail, flush=True)
finally:
    src.write_text(orig)
Path("/tmp/f2/mut/results.json").write_text(json.dumps(results, indent=2))
