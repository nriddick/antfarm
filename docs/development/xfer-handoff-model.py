"""Bounded SC membership/handoff model accompanying proposal-fmembers-xfer-live.md.

Run with xgen (expected to finish) or consumers (expected to expose the
positive-consumer-recheck failure). Each mode exits nonzero if its expected
result changes. Three actors each subscribe and unsubscribe twice.

No roots, producers, capacity rejection, weak memory, or finite spin budget
are modelled. A waiting subscriber releases the mutex. Attachment and each
locked Xfer/Xgen mutation are abstract steps. This is not a Farm safety proof.
"""
from collections import deque
import sys


def explore(mode, actors=3, rounds=2):
    # Consumers, Subs, Xfer present, Xgen, mutex owner, actors(pc, rounds, stamp)
    start = (0, 0, True, 0, -1, tuple(("start", rounds, -1) for _ in range(actors)))
    todo = deque([start])
    seen = {start: None}
    mismatches = 0
    while todo:
        state = todo.popleft()
        c, s, x, g, lock, threads = state
        assert c >= 0 and s >= 0
        counted = {"first_lock", "take", "waiting", "attach", "active"}
        assert c == sum(t[0] in counted for t in threads)
        assert s == sum(t[0] == "promote" for t in threads)
        assert x == (g % 2 == 0)
        if all(t[0] == "done" for t in threads):
            assert (c, s, x, lock) == (0, 0, True, -1)
            continue
        successors = []
        for i, (pc, left, stamp) in enumerate(threads):
            nc, ns, nx, ng, nl = c, s, x, g, lock
            nleft, nstamp = left, stamp
            if pc == "start":
                ns += 1
                npc = "promote"
                label = "announce Subs"
            elif pc == "promote":
                nc += 1
                ns -= 1
                npc = "first_lock" if c == 0 else "attach"
                label = "promote; first" if c == 0 else "promote; ordinary"
            elif pc == "attach":
                npc = "active"
                label = "ordinary attachment completes"
            elif pc == "first_lock" and lock == -1:
                nl = i
                npc = "take"
                label = "first takes mutex"
            elif pc == "take":
                assert lock == i
                nl = -1
                if x:
                    nx = False
                    ng += 1
                    npc = "active"
                    label = "attach; consume Xfer; Xgen++; unlock"
                else:
                    npc = "waiting"
                    label = "Xfer absent; unlock and wait"
            elif pc == "waiting" and x:
                npc = "first_lock"
                label = "publication wakes first"
            elif pc == "active":
                nc -= 1
                npc = "snapshot" if c == 1 else "finish"
                label = "Consumers--; last" if c == 1 else "Consumers--; ordinary"
            elif pc == "snapshot":
                nstamp = g
                npc = "last_lock"
                label = "snapshot Xgen after decrement"
            elif pc == "last_lock" and lock == -1:
                nl = i
                npc = "publish"
                label = "last takes mutex"
            elif pc == "publish":
                assert lock == i
                nl = -1
                npc = "finish"
                if mode == "consumers" and c > 0:
                    label = "positive Consumers: skip publication; unlock"
                elif g != stamp:
                    mismatches += 1
                    label = "changed Xgen: skip publication; unlock"
                elif x:
                    label = "existing Xfer: coalesce; unlock"
                else:
                    nx = True
                    ng += 1
                    label = "publish Xfer; Xgen++; unlock"
            elif pc == "finish":
                nleft -= 1
                npc = "start" if nleft else "done"
                nstamp = -1
                label = "finish departure"
            else:
                continue
            nt = list(threads)
            nt[i] = (npc, nleft, nstamp)
            new = (nc, ns, nx, ng, nl, tuple(nt))
            successors.append(new)
            if new not in seen:
                seen[new] = (
                    state,
                    f"actor {i}: {label} -> C={nc}, S={ns}, X={int(nx)}, G={ng}",
                )
                todo.append(new)
        if not successors:
            trace = []
            while seen[state] is not None:
                state, label = seen[state]
                trace.append(label)
            print(f"{mode}: blocked first subscriber; visited={len(seen)}")
            print("\n".join(reversed(trace)))
            return False
    print(
        f"{mode}: {len(seen)} states, no blocked terminal state; "
        f"Xgen-mismatch branches={mismatches}"
    )
    return True


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in {"xgen", "consumers"}:
        raise SystemExit("usage: xfer-handoff-model.py {xgen|consumers}")
    result = explore(sys.argv[1])
    if result != (sys.argv[1] == "xgen"):
        raise SystemExit("model result differs from the expected outcome")
