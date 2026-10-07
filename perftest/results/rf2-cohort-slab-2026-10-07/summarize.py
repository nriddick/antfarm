#!/usr/bin/env python3
"""Markdown table of one or more perf_gate output directories.
usage: summarize.py DIR [DIR...]  (each DIR has summary.json, gate.json, metadata.json)"""
import json,sys,pathlib
for d in map(pathlib.Path,sys.argv[1:]):
    gate=json.loads((d/'gate.json').read_text());meta=json.loads((d/'metadata.json').read_text())
    args=meta['arguments']
    env=' '.join(args.get('candidate_env') or []) or '(none)'
    print(f"### {d.name}\n\nexit={gate['exit_status']} candidate_env={env} base={meta['builds']['base']['revision'][:7]} candidate={meta['builds']['candidate']['revision'][:7]} repeats={args['repeats']}\n")
    print('| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |')
    print('| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |')
    for x in json.loads((d/'summary.json').read_text()):
        cell=f"{x['mode']}{'/'+str(x['percent'])+'%' if x['mode']=='subset' else ''}/{x['consumers']}"
        r=x.get('paired_ratios') or []
        pm=x.get('paired_median_delta')
        print(f"| {cell} | {x['decision']} | {pm*100:+.2f}% | {min(r)*100:+.2f}..{max(r)*100:+.2f}% | {x.get('base_median',0):.3f} | {x.get('candidate_median',0):.3f} | {x.get('base_peak_rss_kib_median','-')} | {x.get('candidate_peak_rss_kib_median','-')} |" if r else f"| {cell} | {x['decision']} | - | - | - | - | - | - |")
    print()
