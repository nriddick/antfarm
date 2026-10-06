#!/usr/bin/env python3
"""Hand-run F4b mutations against actor_torture in a scratch tree.

usage: mutate.py SCRATCH_TREE [ldc|dmd ...]
SCRATCH_TREE is an unpacked `git archive` of the commit under test; its
actors/actor.d is mutated in place and restored afterwards.
"""
import json
import subprocess
import sys
from pathlib import Path

tree = Path(sys.argv[1]).resolve()
compilers = sys.argv[2:] or ["ldc"]
actor = tree / "actors" / "actor.d"
original = actor.read_text()

WAVE = """            interrupted ? phaseScheduled : phaseIdle)
            | (observed & retireModeMask);"""


def replace(old, new, occurrence=None):
    def apply(src):
        if occurrence is None:
            assert src.count(old) == 1, old
            return src.replace(old, new)
        assert src.count(old) == 2, old
        index = src.index(old)
        if occurrence == 1:
            index = src.index(old, index + 1)
        return src[:index] + new + src[index + len(old):]
    return apply


MUTATIONS = {
    "cancelling-reads-retireBit": replace(
        "return (atomicLoad!(MemoryOrder.acq)(slot_.lifecycle) & cancelBit) != 0;",
        "return (atomicLoad!(MemoryOrder.acq)(slot_.lifecycle) & retireBit) != 0;"),
    "cancelling-false": replace(
        "return (atomicLoad!(MemoryOrder.acq)(slot_.lifecycle) & cancelBit) != 0;",
        "return false;"),
    "escalation-early-return-on-retireBit": replace(
        "        if ((observed & modeBits) == modeBits)\n            return satisfied;",
        "        if ((observed & retireBit) != 0)\n            return satisfied;"),
    "S1-entry-reschedule-retire-only": replace(
        "                        generation, phaseScheduled)\n                        | (observed & retireModeMask);",
        "                        generation, phaseScheduled)\n                        | (observed & retireBit);"),
    "S2-entry-running-retire-only": replace(
        "                    generation, phaseRunning)\n                    | (observed & retireModeMask);",
        "                    generation, phaseRunning)\n                    | (observed & retireBit);"),
    "S3-exit-retire-only": replace(
        "| (nextPhase != phaseRetired ? observed & retireModeMask : 0);",
        "| (nextPhase != phaseRetired ? observed & retireBit : 0);"),
    "S4-signal-idle-retire-only": replace(
        "            replacement = lifecycleWord(generation, phaseScheduled)\n                | (observed & retireModeMask);",
        "            replacement = lifecycleWord(generation, phaseScheduled)\n                | (observed & retireBit);"),
    "S5-drive-idle-retire-only": replace(
        "            immutable replacement = lifecycleWord(generation, phaseScheduled)\n                | (observed & retireModeMask);",
        "            immutable replacement = lifecycleWord(generation, phaseScheduled)\n                | (observed & retireBit);"),
    "S6-wave-rollback-retire-only": replace(
        WAVE, WAVE.replace("retireModeMask", "retireBit"), occurrence=0),
    "S7-wave-exit-retire-only": replace(
        WAVE, WAVE.replace("retireModeMask", "retireBit"), occurrence=1),
    "cancelInbox-ignores-carry": replace(
        "        size_t count;\n        for (auto node",
        "        size_t count;\n        inboxLocal_ = null;\n        for (auto node"),
}

# Ensure the occurrence-based wave mutations hit the intended functions.
assert original.index("package void cancelActorWavePayload") < original.index(WAVE) \
    < original.index("package bool dispatchActorWavePayload") \
    < original.index(WAVE, original.index(WAVE) + 1)

targets = {"ldc": ("actor_torture", "./actor_torture/actor_torture"),
           "dmd": ("run-dmd", None)}
results = []
try:
    for name, mutate in MUTATIONS.items():
        actor.write_text(mutate(original))
        for compiler in compilers:
            target, binary = targets[compiler]
            if binary:
                build = subprocess.run(["make", "SHELL=/usr/bin/bash", "-C",
                                        "actor_torture", target], cwd=tree,
                                       capture_output=True, text=True)
                assert build.returncode == 0, build.stderr[-2000:]
                run = subprocess.run([binary], cwd=tree, capture_output=True,
                                     text=True, timeout=600)
            else:
                run = subprocess.run(["make", "SHELL=/usr/bin/bash", "-C",
                                      "actor_torture", target], cwd=tree,
                                     capture_output=True, text=True, timeout=900)
            lines = (run.stdout + run.stderr).splitlines()
            failure = next((l for l in lines if l.startswith("FAIL")), None)
            row = dict(mutation=name, compiler=compiler,
                       caught=run.returncode != 0, failure=failure)
            results.append(row)
            print(json.dumps(row), flush=True)
finally:
    actor.write_text(original)

out = Path(__file__).resolve().parent / "mutations.json"
out.write_text(json.dumps(results, indent=2) + "\n")
if not all(r["caught"] for r in results):
    raise SystemExit("a mutation survived")
