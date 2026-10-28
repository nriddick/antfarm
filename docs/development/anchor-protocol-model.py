#!/usr/bin/env python3
"""Exhaustive interleaving model of the Farm's empty-Farm anchor protocol.

Models only the membership and anchor logic: a packed consumer word, K root
tallies (count, Sub0 units, Sub pins), a write frontier F that a producer
advances when the next root reads zero, members that advance toward F or
leave, and subscribers that admit, pin, pick the earliest nonzero root,
attach, and commit. Table accounting is abstracted: every root below F is
confirmed, root F is the unconfirmed frontier (a drop to zero there leaves
one unit, the I3 rule). There is no wrap, so the subscriber's bound checks
are not modelled; a subscriber attaches in place at the candidate it read.
Taking a count is two RMWs, as in takeRootCount: the add, then the clear
of the units the add observed on a 0->1 edge.

Every interleaving of the scenario is explored breadth-first, so the first
violation printed comes with a shortest trace.

  python3 anchor-protocol-model.py VARIANT [member positions...] [--F n] [--subs n] [--prod n] [--adv n]

Checked in every state: tally underflow, an unprotected ring (all roots
zero), a closer whose root is not alone, deadlock. Checked at quiescence
(no thread mid-protocol, no lock held, not closing): the counters match the
live views; an empty Farm has a pulse and no count; a non-empty Farm has no
pulse behind every member and no pulse joined to a count (nobody would
clear either).
"""
import sys
from collections import deque

K = 4

VARIANTS = {
    # today (fix/scan-2.1.1 f593920): mutex, members counted at admission,
    # lastOfFarm decided at the decrement, anchor dropped afterwards
    "current":   dict(lock=True,  adm="members", door=False, hold=False, closing=False, retirees=False, commit_first=False),
    # validate-before-Cf patch: mutex, admission slot, members counted after
    # validation and before attach, slot released at the end of unsubscribe
    "validate":  dict(lock=True,  adm="slots",   door=False, hold=False, closing=False, retirees=False, commit_first=True),
    # the proposal as stated: door unit, probes in the high half, the last
    # leaver holds for probes, bare revoke, no lock, no closing
    "door":      dict(lock=False, adm="probes",  door=True,  hold=True,  closing=False, retirees=False, commit_first=False),
    # the proposal plus a closing bit around the anchor drop
    "door+close": dict(lock=False, adm="probes", door=True,  hold=True,  closing=True,  retirees=False, commit_first=False),
    # hold + closing, mutex kept for leavers, no door
    "hold+close+lock": dict(lock=True, adm="probes", door=False, hold=True, closing=True, retirees=False, commit_first=False),
    # hold + closing, no mutex: retirees counted, the last member waits for
    # earlier retirees to drop
    "hold+close+retire": dict(lock=False, adm="probes", door=False, hold=True, closing=True, retirees=True, commit_first=False),
    # the same without the closing bit (shows the bit is load-bearing)
    "hold+retire": dict(lock=False, adm="probes", door=False, hold=True, closing=False, retirees=True, commit_first=False),
    # hold + closing + lock, without the hold (shows the hold is load-bearing)
    "close+lock": dict(lock=True, adm="probes", door=False, hold=False, closing=True, retirees=False, commit_first=False),
    # the complete lock-free form with the door kept (shows it is redundant)
    "door+hold+close+retire": dict(lock=False, adm="probes", door=True, hold=True, closing=True, retirees=True, commit_first=False),
}

RETRY_MAX = 1


class Violation(Exception):
    pass


def root_drop(root, own, anchor, at_frontier):
    count, units, sub = root
    if count == 0:
        raise Violation("root count underflow")
    count -= 1
    if own:
        if units == 0:
            raise Violation("own door unit missing at drop")
        units -= 1
    if count == 0 and (anchor or at_frontier) and units == 0:
        units = 1
    return (count, units, sub)


# thread tuple fields
KIND, PC, POS, CAND, AUX, RETRIES, ADV, LAST, OWN, EXTRA = range(10)
FIELD = dict(kind=KIND, pc=PC, pos=POS, cand=CAND, aux=AUX, retries=RETRIES, adv=ADV, last=LAST, own=OWN, extra=EXTRA)


def mk_sub():
    return ("S", "start", -1, -1, -1, 0, 0, 0, 0, 0)


def mk_member(pos, adv):
    return ("M", "live", pos, -1, -1, 0, adv, 0, 0, 0)


def with_(t, **kw):
    t = list(t)
    for k, v in kw.items():
        t[FIELD[k]] = v
    return tuple(t)


# state fields
F_, MEM, PRB, RET, CLO, ROOTS, LOCK, THR, PROD = range(9)
SFIELD = dict(F=F_, mem=MEM, prb=PRB, ret=RET, clo=CLO, roots=ROOTS, lock=LOCK, threads=THR, prod=PROD)


def set_root(roots, r, val):
    roots = list(roots)
    roots[r] = val
    return tuple(roots)


def thread_steps(s, i, V):
    """Enabled transitions of thread i: list of (label, new state)."""
    out = []
    F, mem, prb, ret, clo, roots, lock, threads, prod = s
    t = threads[i]
    kind, pc, pos, cand, aux, retries, adv, last, own, extra = t
    name = "%s%d" % (kind, i)

    def emit(label, **kw):
        ns = list(s)
        for k, v in kw.items():
            ns[SFIELD[k]] = v
        out.append((name + ": " + label, tuple(ns)))

    def thr(nt):
        th = list(threads)
        th[i] = nt
        return tuple(th)

    def unpinned(upto):
        nr = list(roots)
        for r in range(upto + 1):
            c, u, b = nr[r]
            nr[r] = (c, u, b - 1)
        return tuple(nr)

    if pc == "done":
        return out

    if kind == "S":
        if pc == "start":
            if V["adm"] == "members":
                emit("admit (members+1)", mem=mem + 1, threads=thr(with_(t, pc="pin")))
            elif V["adm"] == "slots":
                emit("admit (slot+1)", prb=prb + 1, threads=thr(with_(t, pc="pin")))
            else:
                npc = "waitclose" if (V["closing"] and clo) else "pin"
                emit("admit (probes+1)" + (", sees closing" if npc == "waitclose" else ""),
                     prb=prb + 1, threads=thr(with_(t, pc=npc)))
        elif pc == "waitclose":
            if not clo:
                emit("closing cleared; proceed", threads=thr(with_(t, pc="pin")))
        elif pc == "pin":
            nr = list(roots)
            c = -1
            for r in range(F + 1):
                cnt, un, sb = nr[r]
                nr[r] = (cnt, un, sb + 1)
                if c < 0 and cnt + un > 0:
                    c = r
            if c < 0:
                c = F
            emit("pin 0..%d, candidate %d (earliest nonzero)" % (F, c), roots=tuple(nr),
                 threads=thr(with_(t, pc="pinned", cand=c, aux=F)))
        elif pc == "pinned":
            if retries < RETRY_MAX:
                kw = dict(roots=unpinned(aux), threads=thr(with_(t, pc="start", retries=retries + 1, cand=-1, aux=-1)))
                if V["adm"] == "members":
                    kw["mem"] = mem - 1
                else:
                    kw["prb"] = prb - 1
                emit("RETRY: unpin, undo admission", **kw)
            if V["commit_first"]:
                emit("commit (members+1) before attach", mem=mem + 1, threads=thr(with_(t, pc="committed1")))
            else:
                cnt, un, sb = roots[cand]
                emit("attach: take count on %d" % cand, roots=set_root(roots, cand, (cnt + 1, un, sb)),
                     threads=thr(with_(t, pc="taken", extra=un if cnt == 0 else 0)))
        elif pc == "committed1":
            cnt, un, sb = roots[cand]
            emit("attach: take count on %d" % cand, roots=set_root(roots, cand, (cnt + 1, un, sb)),
                 threads=thr(with_(t, pc="taken", extra=un if cnt == 0 else 0)))
        elif pc == "taken":
            if extra:
                cnt, un, sb = roots[cand]
                if un < extra:
                    raise Violation("0->1 clear underflows: observed %d units, %d left" % (extra, un))
                emit("attach: 0->1 edge clears %d unit(s) on %d" % (extra, cand),
                     roots=set_root(roots, cand, (cnt, un - extra, sb)), threads=thr(with_(t, pc="attached", extra=0)))
            else:
                pc = "attached"
        if pc == "attached":
            if V["adm"] == "probes":
                emit("commit (members+1, probes-1)", mem=mem + 1, prb=prb - 1, threads=thr(with_(t, pc="committed")))
            else:
                pc = "committed"
        if pc == "committed":
            emit("unpin; now a member at %d" % cand, roots=unpinned(aux), threads=thr(mk_member(cand, 1)))
        return out

    # member / leaver
    at_f = pos == F
    if pc == "live":
        if pos < F and adv > 0:
            cnt, un, sb = roots[pos + 1]
            emit("advance: take count on %d" % (pos + 1), roots=set_root(roots, pos + 1, (cnt + 1, un, sb)),
                 threads=thr(with_(t, pc="advtaken", aux=pos + 1, extra=un if cnt == 0 else 0)))
        first = "lock" if V["lock"] else ("door" if V["door"] else ("retire" if V["retirees"] else "dec"))
        emit("unsubscribe begins", threads=thr(with_(t, pc=first)))
    elif pc == "advtaken":
        if extra:
            cnt, un, sb = roots[aux]
            if un < extra:
                raise Violation("0->1 clear underflows on %d" % aux)
            emit("advance: 0->1 edge clears %d unit(s) on %d" % (extra, aux),
                 roots=set_root(roots, aux, (cnt, un - extra, sb)), threads=thr(with_(t, pc="adv", extra=0)))
        else:
            emit("advance: drop %d" % pos, roots=set_root(roots, pos, root_drop(roots[pos], False, False, False)),
                 threads=thr(with_(t, pc="live", pos=aux, aux=-1, adv=adv - 1)))
    elif pc == "adv":
        emit("advance: drop %d" % pos, roots=set_root(roots, pos, root_drop(roots[pos], False, False, False)),
             threads=thr(with_(t, pc="live", pos=aux, aux=-1, adv=adv - 1)))
    elif pc == "lock":
        if lock < 0:
            nxt = "door" if V["door"] else "dec"
            emit("take the unsubscribe mutex", lock=i, threads=thr(with_(t, pc=nxt)))
    elif pc == "door":
        cnt, un, sb = roots[pos]
        nxt = "retire" if V["retirees"] else "dec"
        emit("door: Sub0 unit on %d (count held)" % pos, roots=set_root(roots, pos, (cnt, un + 1, sb)),
             threads=thr(with_(t, pc=nxt, own=1)))
    elif pc in ("dec", "retire"):
        if mem <= 0:
            raise Violation("members underflow")
        is_last = 1 if mem == 1 else 0
        if V["hold"]:
            nxt = "hold" if is_last else ("revoke" if own else "drop")
        elif V["closing"]:
            nxt = "close" if is_last else "drop"
        else:
            nxt = "anchor" if is_last else "drop"
        kw = dict(mem=mem - 1, threads=thr(with_(t, pc=nxt, last=is_last)))
        label = "members-1 (%s)" % ("last" if is_last else "not last")
        if pc == "retire":
            kw["ret"] = ret + 1
            label = "retire: members-1, retirees+1 (%s)" % ("last" if is_last else "not last")
        emit(label, **kw)
    elif pc == "close":
        emit("close without waiting (CAS closing=1)", clo=1, threads=thr(with_(t, pc="anchor")))
    elif pc == "hold":
        if mem > 0:
            emit("hold: a member committed; not last", threads=thr(with_(t, pc="revoke" if own else "drop", last=0)))
        elif V["retirees"] and clo:
            emit("hold: another retiree is closing; not last", threads=thr(with_(t, pc="revoke" if own else "drop", last=0)))
        elif prb > 0:
            pass  # blocked until every probe resolves
        elif V["closing"]:
            nxt = "closedwait" if V["retirees"] else "anchor"
            emit("hold: nobody in flight; close (CAS closing=1)", clo=1, threads=thr(with_(t, pc=nxt)))
        else:
            emit("hold: nobody in flight; anchor", threads=thr(with_(t, pc="anchor")))
    elif pc == "closedwait":
        if ret == 1:
            emit("closed: every other retiree has dropped", threads=thr(with_(t, pc="anchor")))
        # else blocked until earlier retirees drop and unretire
    elif pc == "revoke":
        cnt, un, sb = roots[pos]
        if un == 0:
            raise Violation("revoke underflow on %d" % pos)
        emit("revoke door unit on %d" % pos, roots=set_root(roots, pos, (cnt, un - 1, sb)),
             threads=thr(with_(t, pc="drop", own=0)))
    elif pc == "anchor":
        if V["closing"] and (V["lock"] or V["retirees"]) and roots[pos][0] != 1:
            raise Violation("closer is not alone on its root %d: %s" % (pos, roots[pos]))
        nr = root_drop(roots[pos], False, True, at_f)
        nxt = "unclose" if V["closing"] else "finish"
        emit("anchor drop on %d (count->%d, units %d)" % (pos, nr[0], nr[1]), roots=set_root(roots, pos, nr),
             threads=thr(with_(t, pc=nxt, own=0)))
    elif pc == "drop":
        nr = root_drop(roots[pos], bool(own), False, at_f)
        emit("plain drop on %d (count->%d, units %d)" % (pos, nr[0], nr[1]), roots=set_root(roots, pos, nr),
             threads=thr(with_(t, pc="finish", own=0)))
    elif pc == "unclose":
        emit("unclose", clo=0, threads=thr(with_(t, pc="finish")))
    elif pc == "finish":
        if V["retirees"]:
            emit("unretire (retirees-1)", ret=ret - 1, threads=thr(with_(t, pc="done")))
        elif V["adm"] == "slots":
            emit("release slot", prb=prb - 1, threads=thr(with_(t, pc="unlock")))
        elif V["lock"]:
            emit("unlock", lock=-1, threads=thr(with_(t, pc="done")))
        else:
            emit("done", threads=thr(with_(t, pc="done")))
    elif pc == "unlock":
        emit("unlock", lock=-1, threads=thr(with_(t, pc="done")))
    return out


def successors(s, V):
    out = []
    F, mem, prb, ret, clo, roots, lock, threads, prod = s
    if prod > 0 and F + 1 < K and roots[F + 1] == (0, 0, 0):
        ns = list(s)
        ns[F_] = F + 1
        ns[PROD] = prod - 1
        out.append(("P: frontier -> %d (root read zero)" % (F + 1), tuple(ns)))
    for i in range(len(threads)):
        try:
            out.extend(thread_steps(s, i, V))
        except Violation as v:
            out.append(("%s%d: %s" % (threads[i][KIND], i, v), None))
    return out


def check(s, V):
    F, mem, prb, ret, clo, roots, lock, threads, prod = s
    if mem < 0 or prb < 0 or ret < 0:
        raise Violation("counter underflow")
    for r in roots:
        if min(r) < 0:
            raise Violation("tally underflow %s" % (r,))
    if all(r == (0, 0, 0) for r in roots):
        raise Violation("unprotected ring: every root is zero")
    live = [t for t in threads if t[KIND] == "M" and t[PC] in ("live", "advtaken", "adv")]
    quiet = all((t[KIND] == "M" and t[PC] == "live") or (t[KIND] == "S" and t[PC] == "start") or t[PC] == "done"
                for t in threads) and lock < 0 and clo == 0
    if not quiet:
        return
    if mem != len(live):
        raise Violation("members=%d but %d live views" % (mem, len(live)))
    want_prb = len(live) if V["adm"] == "slots" else 0
    if prb != want_prb or ret != 0:
        raise Violation("probes=%d retirees=%d at quiescence" % (prb, ret))
    if not live:
        if any(r[0] > 0 for r in roots):
            raise Violation("empty Farm with a root count")
        if not any(r[1] > 0 for r in roots):
            raise Violation("empty Farm without a pulse")
        return
    lo = min(t[POS] for t in live)
    for r in range(K):
        cnt, un, sb = roots[r]
        if un > 0 and r < lo:
            raise Violation("stranded pulse on %d behind every member (lowest member at %d)" % (r, lo))
        if un > 0 and cnt > 0:
            raise Violation("pulse joined to a count on %d: nobody will clear it" % r)


def trace_of(parent, s):
    trace = []
    cur = s
    while parent[cur] is not None:
        p, label = parent[cur]
        trace.append(label)
        cur = p
    trace.reverse()
    return trace


def run(variant, members, F0, subs, prod, adv):
    V = VARIANTS[variant]
    threads = tuple(mk_member(p, adv) for p in members) + tuple(mk_sub() for _ in range(subs))
    F = max(members + [F0])
    roots = [[0, 0, 0] for _ in range(K)]
    for p in members:
        roots[p][0] += 1
    if not members:
        roots[F][1] = 1   # empty Farm starts with its anchor
    prb0 = len(members) if V["adm"] == "slots" else 0
    s0 = (F, len(members), prb0, 0, 0, tuple(tuple(r) for r in roots), -1, threads, prod)
    parent = {s0: None}
    q = deque([s0])
    n = 0
    desc = "members at %s, F=%d, %d subscriber(s), %d producer step(s), %d advance(s) each" % (members, F, subs, prod, adv)
    while q:
        s = q.popleft()
        n += 1
        succ = successors(s, V)
        if not succ and any(t[PC] != "done" for t in s[THR]):
            return report(variant, n, "deadlock: a thread waits and nothing else can run", trace_of(parent, s), s)
        for label, ns in succ:
            if ns is None:
                return report(variant, n, label, trace_of(parent, s) + [label], s)
            if ns in parent:
                continue
            parent[ns] = (s, label)
            try:
                check(ns, V)
            except Violation as v:
                return report(variant, n, str(v), trace_of(parent, ns), ns)
            q.append(ns)
    print("%-24s OK   %7d states   %s" % (variant, n, desc))
    return True


def report(variant, n, msg, trace, s):
    print("%-24s FAIL %7d states   %s" % (variant, n, msg))
    for k, label in enumerate(trace, 1):
        print("    %2d. %s" % (k, label))
    F, mem, prb, ret, clo, roots, lock, threads, prod = s
    print("    end: F=%d members=%d probes=%d retirees=%d closing=%d roots=%s" % (F, mem, prb, ret, clo, list(roots)))
    return False


if __name__ == "__main__":
    args = sys.argv[1:]
    variant = args[0]
    subs, prod, adv, F0 = 1, 1, 1, 0
    members = []
    i = 1
    while i < len(args):
        if args[i] == "--subs":
            subs = int(args[i + 1]); i += 2
        elif args[i] == "--prod":
            prod = int(args[i + 1]); i += 2
        elif args[i] == "--adv":
            adv = int(args[i + 1]); i += 2
        elif args[i] == "--F":
            F0 = int(args[i + 1]); i += 2
        else:
            members.append(int(args[i])); i += 1
    sys.exit(0 if run(variant, members, F0, subs, prod, adv) else 1)
