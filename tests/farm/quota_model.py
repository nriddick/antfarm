"""Bounded, sequentially consistent model of the producer quota arithmetic.

This is deliberately not a model of all Ant Farm: publication, Sub0, initial
subscription, leaf/root propagation and weak memory are omitted. One established
consumer holds a contiguous [oldest, newest] range; it may extend up to Wt's
epoch and release its prefix. Producer probes, individual forward loads, grants
and reservations are separate interleavable steps. A reserve may use any positive
portion of its quota, and a refresh may discard any remainder. This over-approximates
table sizing and consumer progress to look for arithmetic counterexamples.

The negative control restores automatic refill after *every* reservation. It
demonstrates that the oracle catches unbounded quota spending; it is not an exact
model of the historical opportunistic-renewal condition.
"""
import argparse
import collections
import json


def explore(k, cap, quotas, laps, automatic=False, state_limit=1_000_000):
    exmax = sum(quotas)
    assert 0 < exmax <= (k - 1) * cap
    need = (exmax + cap - 1) // cap
    # Producer: (balance, anchor epoch, next scan index), index 0 = idle,
    # need+1 = successful scan waiting to grant.
    start = (0, 0, 0, tuple((0, 0, 0) for _ in quotas))
    pending = collections.deque([start])
    predecessor = {start: None}
    edges = 0
    max_wt = k * cap * laps

    def trace(state, final):
        out = [final]
        while predecessor[state] is not None:
            state, action = predecessor[state]
            out.append(action)
        return list(reversed(out))

    while pending:
        state = pending.popleft()
        wt, oldest, newest, producers = state
        pinned = {e % k for e in range(oldest, newest + 1)}
        next_states = []
        if newest < wt // cap:
            next_states.append(((wt, oldest, newest + 1, producers), "consumer pin ahead"))
        if oldest < newest:
            next_states.append(((wt, oldest + 1, newest, producers), "consumer drop oldest"))
        for i, (balance, anchor, step) in enumerate(producers):
            def changed(p, tail=wt):
                ps = list(producers[:i] + (p,) + producers[i + 1:])
                # Identical quota roles are symmetric; canonical slot numbers
                # in a trace are not persistent OS-thread identities.
                for q in set(quotas):
                    indices = [j for j, quota in enumerate(quotas) if quota == q]
                    ordered = sorted(ps[j] for j in indices)
                    for j, value in zip(indices, ordered):
                        ps[j] = value
                return (tail, oldest, newest, tuple(ps))
            if step == 0:
                next_states.append((changed((balance, wt // cap, 1)), f"P{i} probe Wt={wt}"))
                for size in range(1, balance + 1):
                    end = wt + size
                    if end > max_wt:
                        continue
                    for epoch in range(wt // cap + 1, end // cap + 1):
                        if epoch % k in pinned:
                            return dict(status="counterexample", states=len(predecessor), edges=edges,
                                        trace=trace(state, f"P{i} reserve [{wt},{end}) enters pinned epoch {epoch}; held=[{oldest},{newest}]"))
                    remaining = quotas[i] if automatic else balance - size
                    next_states.append((changed((remaining, 0, 0), end), f"P{i} reserve [{wt},{end})"))
            elif step == need + 1:
                next_states.append((changed((quotas[i], 0, 0)), f"P{i} grant {quotas[i]}"))
            elif (anchor + step) % k in pinned:
                next_states.append((changed((balance, 0, 0)), f"P{i} scan blocked"))
            else:
                next_states.append((changed((balance, anchor, step + 1)), f"P{i} scan zero slot {(anchor + step) % k}"))
        for nxt, action in next_states:
            edges += 1
            if nxt not in predecessor:
                predecessor[nxt] = (state, action)
                if len(predecessor) >= state_limit:
                    return dict(status="state-limit", states=len(predecessor), edges=edges)
                pending.append(nxt)
    return dict(status="bounded-pass", states=len(predecessor), edges=edges, max_wt=max_wt)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--laps", type=int, default=2)
    parser.add_argument("--state-limit", type=int, default=1_000_000)
    args = parser.parse_args()
    configs = [(2, 2, (1, 1)), (2, 4, (1, 2)), (4, 2, (1, 1)),
               (4, 2, (1, 2)), (4, 2, (2, 2)), (4, 2, (1, 1, 1))]
    results = []
    for k, cap, quotas in configs:
        r = explore(k, cap, quotas, args.laps, state_limit=args.state_limit)
        r.update(k=k, cap=cap, quotas=quotas, automatic=False)
        results.append(r)
        print(json.dumps(r), flush=True)
    negative = explore(4, 2, (1, 1), args.laps, automatic=True, state_limit=args.state_limit)
    negative.update(k=4, cap=2, quotas=(1, 1), automatic=True)
    print(json.dumps(negative), flush=True)
    assert all(r["status"] == "bounded-pass" for r in results), "strict model did not finish cleanly"
    assert negative["status"] == "counterexample", "negative-control oracle failed"


if __name__ == "__main__":
    main()
