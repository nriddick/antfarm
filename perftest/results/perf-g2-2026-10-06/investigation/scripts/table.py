#!/usr/bin/env python3
import json, sys
for d in sys.argv[1:]:
    s = json.load(open(d + '/summary.json'))
    print(f'### {d.rstrip("/").split("/")[-1]}\n\n| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |\n| --- | ---: | ---: | ---: | ---: | --- | --- | --- |')
    for c in s:
        r = c['paired_ratios']; lo, hi = c['paired_bootstrap95']
        f = lambda x: f'{x*100:+.2f}%'
        print(f"| {c['mode']} | {c['percent']} | {c['consumers']} | {c['quiet_n']} | {f(c['paired_median_delta'])} | {f(lo)[:-1]}…{f(hi)} | {f(min(r))[:-1]}…{f(max(r))} | {c['decision']} |")
    print()
