#!/usr/bin/env python3
"""Plot the retained benchmark medians and min/max ranges without network assets."""
import argparse
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('results',type=Path)
a=p.parse_args()
rows=json.loads((a.results/'summary.json').read_text())
series=[('local','Direct call/reset','#64748b'),('ring','Farm ring (chunk 8)','#d97706'),
        ('deque','Deque LIFO','#2563eb'),('deque-fair','Deque + oldest/32','#059669'),
        ('domain','Existing FiberDomain','#9333ea')]
plt.rcParams.update({'font.size':10,'svg.fonttype':'none'})
for yields in (0,3):
    fig,ax=plt.subplots(figsize=(8,4.6),layout='constrained')
    for mode,label,color in series:
        rs=sorted((r for r in rows if r['mode']==mode and r['yields']==yields),key=lambda r:r['threads'])
        med=[r['median']/1e6 for r in rs]
        ax.errorbar([r['threads'] for r in rs],med,
                    yerr=[[ (r['median']-r['minimum'])/1e6 for r in rs],[(r['maximum']-r['median'])/1e6 for r in rs]],
                    label=label,color=color,marker='o',markersize=4,capsize=3,linewidth=1.8)
    ax.set_xscale('log',base=2);ax.set_yscale('log')
    ax.set_xticks([1,2,4,8,16,32]);ax.xaxis.set_major_formatter(ScalarFormatter())
    ax.yaxis.set_major_formatter(ScalarFormatter())
    ax.set_yticks([1,2,5,10,20,50,100,200,400] if yields==0 else [1,2,5,10,20,50,100,200])
    ax.set_ylim((1,450) if yields==0 else (.8,160))
    ax.set_xlabel('Total dispatch threads (includes caller)')
    ax.set_ylabel('Million completed warm Fiber lifetimes / s (log scale)')
    ax.set_title(f'{yields} yields per lifetime · Ryzen 9 5900XT · 5 samples per cell',loc='left',fontweight='bold')
    ax.grid(True,which='major',alpha=.2);ax.spines[['top','right']].set_visible(False)
    ax.legend(loc='upper left',fontsize=8.5,ncol=2,framealpha=.9)
    svg=a.results/f'warm-{yields}-yields.svg'
    fig.savefig(svg)
    svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines())+'\n')
    fig.savefig(a.results/f'warm-{yields}-yields.png',dpi=150)
    plt.close(fig)
