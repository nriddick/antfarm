#!/usr/bin/env python3
"""Markdown table of writer-bench.txt: per (API, body, batch) the median over
rounds of each metric, base -> candidate."""
import re,collections,statistics as st,sys
d=collections.defaultdict(lambda: collections.defaultdict(list))
side=None
for l in open(sys.argv[1] if len(sys.argv)>1 else 'writer-bench.txt'):
    if l.startswith('##'): side=l.split()[-1]; continue
    kv=dict(re.findall(r'(\w+)=([\d.]+)',l)); api=l.split()[0]
    d[(api,int(kv['body']),int(kv['batch']))][side].append(kv)
print("| API | body | batch | admitted base → cand | ring words/entry | agg ticks/entry | median ticks/entry (full tables) |")
print("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
for k in sorted(d):
    b=d[k]['base']; c=d[k]['cand']
    f=lambda L,x: st.median(float(v[x]) for v in L)
    print(f"| {k[0]} | {k[1]} | {k[2]} | {int(f(b,'admitted'))} → {int(f(c,'admitted'))} | {f(b,'words_per_entry'):.2f} → {f(c,'words_per_entry'):.2f} | {f(b,'agg_ticks_per_entry'):.1f} → {f(c,'agg_ticks_per_entry'):.1f} | {f(b,'median_ticks_per_entry'):.1f} → {f(c,'median_ticks_per_entry'):.1f} |")
