#!/usr/bin/env python3
"""Adopted forcing reproducer for the C13/C-M12 return-CAS barrier.

Origin: verifier task 148 artifact ``force_cas_stall.py``. It injects a
write-audit hook that holds the emitter's second table publication (A's initial member table)
until producer A has reached its return CAS. That forces the schedule in
which A reaches its held CAS before B's table is published; the fixture
then publishes B and forces A to retry against B's competing return head.

Before the test-barrier fix this schedule aborted 3/3 with
``FAIL cont first CAS release:0`` (A blocked while the emitter waited for unpublished B). After the fix, ``contCasRetry`` keeps pumping until B starts,
so the schedule must complete with child exit 0.

This copy differs from the task-148 verifier only where the revision
requires:

* child exit is expected to be 0 (the fixed behavior), not SIGABRT;
* all outputs go to a writable ``--out`` directory (default: a fresh
  ``tempfile.mkdtemp``), never to the read-only task-37 root;
* it runs the requested 3 normal and 100 TSan invocations by default and
  records compact per-run outcomes rather than bulk raw logs.

The temporary in-tree probe source is removed in ``finally`` and the
canonical ``private_actor_tests/main.d`` is never written.

Exit status is 0 only when every requested invocation exits 0, emits the
forced-schedule marker, and (for TSan) produces no sanitizer warning.
"""

import argparse
import difflib
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time

FORCED_MARKER = "FORCED A CAS before pump returns"

# Injected immediately before the C13 comment. Keeps the forcing hook that
# made the pre-fix failure deterministic.
HOOK = '''
version (AntfarmWriteAuditHooks) {
    __gshared uint vCasPublications;
    void vForceCas(AntFarm*, WriteAuditPhase phase, ulong, ulong) nothrow @nogc @system {
        if (phase != WriteAuditPhase.tablePublished || ++vCasPublications != 2) return;
        awaitRaw(crControl.firstAtCas, 1u, "force A CAS before emitter pump returns");
        fprintf(stderr, "FORCED A CAS before pump returns: firstAtCas=%u secondStarted=%u secondReturned=%u firstGo=%u\\n",
            atomicLoad!(MemoryOrder.raw)(crControl.firstAtCas), atomicLoad!(MemoryOrder.raw)(crControl.secondStarted),
            atomicLoad!(MemoryOrder.raw)(crControl.secondReturned), atomicLoad!(MemoryOrder.raw)(crControl.firstGo));
    }
}
'''

ANCHOR = "// C13: C-M12 omits head reload after a forced failed CAS."
INSTALL_OLD = "crControl = &c; setActorTestHook(&crCasHook);"
INSTALL_NEW = (
    INSTALL_OLD
    + "\n        version (AntfarmWriteAuditHooks) { vCasPublications = 0; writeAuditHook = &vForceCas; }"
    + "\n        scope(exit) { version (AntfarmWriteAuditHooks) writeAuditHook = null; }"
)

SOURCES = [
    "antfarm.d",
    "antfarm_allocation.d",
    "actors/actor.d",
    "actors/wave.d",
    "actors/private_actor.d",
]


def locate_worktree(start):
    p = start.resolve()
    while True:
        if (p / "private_actor_tests" / "main.d").is_file():
            return p
        if p.parent == p:
            raise SystemExit(
                "error: could not locate worktree root containing "
                "private_actor_tests/main.d from %s" % start
            )
        p = p.parent


def inject(canonical):
    s = canonical
    assert s.count(ANCHOR) == 1, "C13 anchor not unique"
    s = s.replace(ANCHOR, HOOK + "\n" + ANCHOR, 1)
    a = s.index("void contCasRetry(")
    b = s.index("\nversion (AntfarmActorTestHooks)", a)
    section = s[a:b]
    assert section.count(INSTALL_OLD) == 1, "install site not unique"
    section = section.replace(INSTALL_OLD, INSTALL_NEW)
    return s[:a] + section + s[b:]


def build(cmd, log):
    log.write("$ " + " ".join(cmd) + "\n")
    log.flush()
    start = time.monotonic()
    p = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, timeout=600)
    elapsed = time.monotonic() - start
    log.write("BUILD EXIT %d (%.3fs)\n" % (p.returncode, elapsed))
    log.flush()
    return p.returncode


def run_one(binary, env, timeout, log):
    cmd = [str(binary), "cont-return-cas-retry"]
    start = time.monotonic()
    try:
        p = subprocess.run(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        )
        out = p.stdout.decode("utf-8", "replace")
        rc = p.returncode
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or b"").decode("utf-8", "replace")
        rc = "timeout"
    elapsed = time.monotonic() - start
    return rc, out, elapsed


def main():
    here = pathlib.Path(__file__).resolve()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--worktree",
        default=str(locate_worktree(here.parent)),
        help="worktree root (default: nearest ancestor with private_actor_tests/main.d)",
    )
    ap.add_argument(
        "--out",
        default=None,
        help="writable output directory (default: fresh tempfile.mkdtemp)",
    )
    ap.add_argument("--normal-runs", type=int, default=3)
    ap.add_argument("--tsan-runs", type=int, default=100)
    ap.add_argument("--timeout", type=float, default=30.0)
    args = ap.parse_args()

    root = pathlib.Path(args.worktree).resolve()
    out = pathlib.Path(args.out).resolve() if args.out else pathlib.Path(
        tempfile.mkdtemp(prefix="force-cas-stall-")
    )
    out.mkdir(parents=True, exist_ok=True)

    canonical = (root / "private_actor_tests" / "main.d").read_text()
    source = root / "private_actor_tests" / "cas_stall_probe.d"
    patched = inject(canonical)
    (out / "force-cas-stall.patch").write_text(
        "".join(
            difflib.unified_diff(
                canonical.splitlines(True),
                patched.splitlines(True),
                fromfile="a/private_actor_tests/main.d",
                tofile="b/private_actor_tests/main.d",
            )
        )
    )

    base = ["ldc2", "-g", "-O1",
            "-d-version=AntfarmWriteAuditHooks",
            "-d-version=AntfarmActorTestHooks", "-I."]
    normal_bin = out / "forced-cas-stall"
    tsan_bin = out / "forced-cas-stall-tsan"

    env_normal = {**os.environ, "ANTFARM_HUGE_PAGES": "0"}
    env_tsan = {
        **os.environ,
        "ANTFARM_HUGE_PAGES": "0",
        "TSAN_OPTIONS": "history_size=7:halt_on_error=1",
    }

    summary = {
        "worktree": str(root),
        "out": str(out),
        "canonical_main_d": str(root / "private_actor_tests" / "main.d"),
        "probe_source": str(source),
        "expected_exit": 0,
        "forced_marker": FORCED_MARKER,
        "normal": {"runs": args.normal_runs},
        "tsan": {"runs": args.tsan_runs},
    }
    failures = 0
    try:
        source.write_text(patched)
        with (out / "force-cas-stall.log").open("w") as log:
            log.write("== forcing reproducer (adopted from task 148) ==\n")
            log.write("worktree: %s\nout: %s\n" % (root, out))
            log.write("expectation: child exit 0 for every invocation\n\n")

            # ---- normal build + runs ----
            cmd = base + [str(source)] + SOURCES + ["-of=" + str(normal_bin)]
            rc = build(cmd, log)
            summary["normal"]["build_exit"] = rc
            if rc != 0:
                raise RuntimeError("normal build failed: %d" % rc)
            exits = []
            missing_marker = 0
            for n in range(args.normal_runs):
                r, out_txt, dt = run_one(normal_bin, env_normal, args.timeout, log)
                exits.append(r)
                ok = r == 0 and FORCED_MARKER in out_txt
                if FORCED_MARKER not in out_txt:
                    missing_marker += 1
                if not ok:
                    failures += 1
                    log.write("RUN %d FAIL exit=%r (%.3fs)\n" % (n + 1, r, dt))
                    log.write(out_txt)
                    log.flush()
                else:
                    log.write("RUN %d ok exit=0 (%.3fs)\n" % (n + 1, dt))
                    log.flush()
            summary["normal"].update(
                normal_exits=exits,
                passed=sum(1 for r in exits if r == 0) - missing_marker,
                missing_marker=missing_marker,
            )

            # ---- TSan build + runs ----
            tcmd = base + ["-fsanitize=thread", "-d-version=TSan",
                           str(source)] + SOURCES + ["-of=" + str(tsan_bin)]
            rc = build(tcmd, log)
            summary["tsan"]["build_exit"] = rc
            if rc != 0:
                raise RuntimeError("tsan build failed: %d" % rc)
            texits = []
            tmissing = 0
            for n in range(args.tsan_runs):
                r, out_txt, dt = run_one(tsan_bin, env_tsan, args.timeout, log)
                texits.append(r)
                ok = r == 0 and FORCED_MARKER in out_txt
                if FORCED_MARKER not in out_txt:
                    tmissing += 1
                if not ok:
                    failures += 1
                    # Keep only the first few failing outputs to avoid bulk logs.
                    log.write("TSAN RUN %d FAIL exit=%r (%.3fs)\n" % (n + 1, r, dt))
                    log.write(out_txt)
                    log.flush()
                else:
                    log.write("TSAN RUN %d ok exit=0 (%.3fs)\n" % (n + 1, dt))
                    log.flush()
            summary["tsan"].update(
                tsan_exits=texits,
                passed=sum(1 for r in texits if r == 0) - tmissing,
                missing_marker=tmissing,
            )
            log.write("\n== summary ==\n")
            log.write(json.dumps(summary, indent=2) + "\n")
            log.write("failures: %d\n" % failures)
    finally:
        source.unlink(missing_ok=True)

    (out / "force-cas-stall-summary.json").write_text(
        json.dumps(summary, indent=2) + "\n"
    )
    print(json.dumps(summary, indent=2))
    print("failures:", failures)
    if failures:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
