#!/usr/bin/env python3
"""G2 mutation checks. Each mutation is applied alone to a scratch copy of the
committed tree (git archive REF), built with LDC (debug and -release) and run
on its selector. The worktree is never edited. Usage: mutate.py REF OUTDIR [names...]"""
import subprocess, sys, json, os, time, shutil
from pathlib import Path
repo = Path("/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree")
ref, outdir = sys.argv[1], Path(sys.argv[2]); outdir.mkdir(parents=True, exist_ok=True)
SRC = "actors/private_actor.d"
PLAIN_RETIRE = "            if (retiring) retireMember!T(park.base_.service, entry);\n        }\n        return 1;\n"
PLAIN_RESULT = ("                static if (subset)\n"
    "                    park.writeResult(park.sel_[park.capacity_ + (entry - park.entries_)], operation(borrow, context));\n"
    "                else\n"
    "                    park.writeResult(entry - park.entries_, operation(borrow, context));\n"
    "            }\n" + PLAIN_RETIRE)
M = {
 # D-M4: retiring branch destroys once more before retireMember (double destruction).
 "D-M4": ("dtor-child", [(PLAIN_RETIRE, "            if (retiring) { destroyState!T(entry); retireMember!T(park.base_.service, entry); }\n        }\n        return 1;\n")]),
 # D-M4b: destroy after every operation, not only when retiring.
 "D-M4b": ("dtor-child", [(PLAIN_RETIRE, "            destroyState!T(entry);\n" + PLAIN_RETIRE)]),
 # D-M6: retire before writing the result slot.
 "D-M6": ("dtor-results", [(PLAIN_RESULT,
    "                static if (subset)\n"
    "                    park.writeResult(park.sel_[park.capacity_ + (entry - park.entries_)], operation(borrow, context));\n"
    "                else\n"
    "                {\n"
    "                    auto result = operation(borrow, context);\n"
    "                    if (retiring) retireMember!T(park.base_.service, entry);\n"
    "                    park.writeResult(entry - park.entries_, result);\n"
    "                }\n"
    "            }\n"
    "            static if (kind == OpKind.plain || subset) if (retiring) retireMember!T(park.base_.service, entry);\n        }\n        return 1;\n")]),
 # F2 cancellation mutations on the retiring-wave cleanup-header path.
 "C2a": ("cancel-c2", [("        if (retire_) return false;\n", "")]),
 "C2b": ("cancel-c2", [("        if (retire_) return false;\n", ""),
          ("        if (park.retire_) park.count_ = 0;\n",
           "        if (park.retire_) foreach (ref e; park.entries_[park.dispatched_ .. park.count_]) retireMember!T(park.base_.service, &e);\n        if (park.retire_) park.count_ = 0;\n")]),
 "C3": ("cancel-c3", [("            cleanupHeader = park.header_;\n            cleanupHeader.call = &runSkippedRetire;\n            header = &cleanupHeader;\n",
          "            park.header_.call = &runSkippedRetire;\n")]),
 # Hardening: keep the member count after terminal completion (refill).
 "keep-count": ("refill-after-final", [("        if (park.retire_) park.count_ = 0;\n", "")]),
 # G2: install the opposite callback variant at start (policy/header mismatch).
 "W1-swap-variant": ("dtor-child", [("        header_.call = retire ? &runChild!(operation, false, true)\n            : &runChild!(operation, false, false);\n",
          "        header_.call = retire ? &runChild!(operation, false, false)\n            : &runChild!(operation, false, true);\n")]),
 # G2: retiring continuation completes as non-retiring.
 "W2-cont-done": ("cont-final-retire", [("                next = retiring ? ChildStep.doneRetire : ChildStep.done;\n", "                next = ChildStep.done;\n")]),
}
sources = ["private_actor_tests/main.d", "antfarm.d", "antfarm_allocation.d", "actors/actor.d", "actors/wave.d", "actors/private_actor.d"]
names = sys.argv[3:] or list(M)
results = {}
for name in names:
    selector, edits = M[name]
    work = Path(f"/tmp/g2/mut/{name}"); shutil.rmtree(work, ignore_errors=True); work.mkdir(parents=True)
    subprocess.run(f"git -C {repo} archive {ref} | tar -x -C {work}", shell=True, check=True, executable="/usr/bin/bash")
    src = work / SRC; orig = src.read_text(); text = orig
    for a, b in edits:
        assert text.count(a) == 1, (name, a)
        text = text.replace(a, b)
    src.write_text(text)
    (outdir / f"{name}.diff").write_text(subprocess.run(["diff", "-u", "--label", f"a/{SRC}", "--label", f"b/{SRC}", "-", str(src)], input=orig, capture_output=True, text=True).stdout)
    for flags in ["", "-release"]:
        out = work / f"t{flags or '-debug'}"
        cmd = ["ldc2", "-g", "-O1"] + ([flags] if flags else []) + ["-d-version=AntfarmWriteAuditHooks", "-d-version=AntfarmActorTestHooks", "-I.", *sources, f"-of={out}"]
        b = subprocess.run(cmd, cwd=work, capture_output=True, text=True)
        key = name + (flags or "-debug")
        if b.returncode:
            results[key] = dict(build_failed=b.stderr[-2000:]); print(key, "BUILD FAILED", b.stderr[-800:], flush=True); continue
        t0 = time.time()
        try:
            r = subprocess.run([str(out), selector], cwd=work, capture_output=True, text=True, timeout=90, env={**os.environ, "ANTFARM_HUGE_PAGES": "0"})
            status, tail = r.returncode, (r.stdout + r.stderr).strip().splitlines()[-3:]
        except subprocess.TimeoutExpired:
            status, tail = "timeout(90s)", []
        results[key] = dict(selector=selector, flags=flags or "debug", status=status, tail=tail, seconds=round(time.time() - t0, 1))
        print(key, selector, status, tail, flush=True)
    shutil.rmtree(work)
(outdir / "mutations.json").write_text(json.dumps(results, indent=1) + "\n")
