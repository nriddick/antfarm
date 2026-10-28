"""Exhaustive RC11 (Lahav et al. 2017, no SC accesses) check of the
spanned-interior attach chain of SPEC 6b-c and 6d: the one happens-before gap
the Live-lifecycle audit found, and the successor Es acquire that closes it.
Written for the second review of commit 8275359; kept here as the model
behind docs/design/farm-live-lifecycle.md section 5. Run it with
`python3 -I tests/farm/rc11_spanned_attach.py`; it takes a few seconds and
needs nothing beyond the standard library.

Each line reports every consistent execution of one variant, tagged [SC] when
it is also sequentially consistent. Expected: without the acquire, eight
STALE executions, all RC11-only; with it, no STALE at all, and rejections
only in RC11-only executions. The last variant lets a later lap's producer
overwrite es3 without modelling the quota sweep, so it admits executions the
subscriber's Sub and reference forbid in the real protocol; its two SC
outcomes (one REJECT, one STALE with es3=7) are those and not reachable.

Threads (one lap, K large enough that no slot is reused):
  P  spanning producer, reservation crosses epochs 1..3 (eold=0, enew=3):
       per epoch e: W_rlx(sq_e,1); W_rel(es_e,e)   (seqt/sd folded into sq_e)
       then table + W_rel(sent,1)
  F  consumer that finishes P's table: R_acq(sent)==1; FADD_rlx(sd0,+1)
  D  discharger of epoch 0 (never acquired sent): confirmed(0) reads
       R_acq(es1)==1, R_rlx(sd0)==1, then CAS_acqrel(live0: 1->0)
  S  fresh subscriber: R_acq(live0)==0 (epoch 0 discharged), validation
       R_acq(es1)==1 (candidate c=1, Seqt epoch 3), [fix: R_acq(es3)==3],
       first hot-path moveRef(3): R_acq(es2), R_rlx(sq2), R_acq(es3), R_rlx(sq3)
Question: can moveRef see an uninitialized interior (es2!=2, sq2==0, es3!=3,
sq3==0) in a consistent execution where S attached?
Variants also cover the round-1 alternative (Sd add release, confirmer acquire).
"""
import itertools

def run(fix, sd_relacq=False, mixed=False):
    ev = []          # (tid, idx, kind, loc, ord, wval|None, cas_expect)
    def T(tid, ops):
        for i,o in enumerate(ops): ev.append((tid,i)+o)
    rel_sd = 'rel' if sd_relacq else 'rlx'
    acq_sd = 'acq' if sd_relacq else 'rlx'
    P = []
    for e in (1,2,3):
        P += [('W','sq%d'%e,'rlx',1,None), ('W','es%d'%e,'rel',e,None)]
    P += [('W','sent','rel',1,None)]
    T('P', P)
    T('F', [('R','sent','acq',None,None), ('U','sd0',rel_sd,'+1',None)])
    T('D', [('R','es1','acq',None,None), ('R','sd0',acq_sd,None,None),
            ('U','live0','acq_rel','cas1->0',None)])
    S = [('R','live0','acq',None,None), ('R','es1','acq',None,None)]
    if fix: S += [('R','es3','acq',None,None)]
    S += [('R','es2','acq',None,None), ('R','sq2','rlx',None,None),
          ('R','es3','acq',None,None), ('R','sq3','rlx',None,None)]
    T('S', S)
    if mixed:
        # A later lap's producer writes es3 := 3+K (K=4) after reserving past
        # slot 3; S may read it. It must never be accepted as epoch 3.
        T('Q', [('W','es3','rel',7,None)])
    n = len(ev)
    init = {}
    locs = sorted({e[3] for e in ev})
    for l in locs: init[l] = 1 if l=='live0' else 0
    W = [i for i,e in enumerate(ev) if e[2] in 'WU']
    R = [i for i,e in enumerate(ev) if e[2] in 'RU']
    # write candidates per location: 'I' = init
    wl = {l:['I']+[i for i in W if ev[i][3]==l] for l in locs}
    sb = set()
    for i in range(n):
        for j in range(n):
            if ev[i][0]==ev[j][0] and ev[i][1]<ev[j][1]: sb.add((i,j))
    def isacq(o): return o in ('acq','acq_rel')
    def isrel(o): return o in ('rel','acq_rel')
    results = {}
    rf_choices = [wl[ev[r][3]] for r in R]
    mo_choices = []
    for l in locs:
        ws = [i for i in W if ev[i][3]==l]
        mo_choices.append([('I',)+p for p in itertools.permutations(ws)])
    for mos in itertools.product(*mo_choices):
        mo = {l:m for l,m in zip(locs,mos)}
        pos = {}
        for l in locs:
            for k,w in enumerate(mo[l]): pos[(l,w)] = k
        for rfs in itertools.product(*rf_choices):
            rf = dict(zip(R, rfs))
            # values
            val = {}
            ok = True
            def wval(w, l):
                if w=='I': return init[l]
                return val.get(('w',w))
            # compute values in a fixpoint (RMW values depend on reads)
            for _ in range(n+2):
                for i,e in enumerate(ev):
                    if e[2]=='W': val[('w',i)] = e[5]
                for r in R:
                    l = ev[r][3]; v = wval(rf[r], l)
                    if v is None: continue
                    val[('r',r)] = v
                    if ev[r][2]=='U':
                        op = ev[r][5]
                        if op=='+1': val[('w',r)] = v+1
                        elif op=='cas1->0':
                            val[('w',r)] = 0 if v==1 else None
            # a failed CAS is a plain read: treat as no write -> skip such execs
            if any(ev[r][2]=='U' and val.get(('w',r)) is None for r in R): continue
            if any(('r',r) not in val for r in R): continue
            # RMW atomicity: reads from immediate mo-predecessor
            for r in R:
                if ev[r][2]=='U':
                    l = ev[r][3]
                    if pos[(l,rf[r])] != pos[(l,r)]-1: ok=False; break
            if not ok: continue
            # rf must not read own or sb-later write
            for r in R:
                w = rf[r]
                if w!='I' and (w==r or (r,w) in sb): ok=False; break
            if not ok: continue
            # sw: release write w (or RMW) heads release sequence: w then
            # contiguous RMWs after it in mo; acquire read reading from rs(w)
            sw = set()
            for r in R:
                if not isacq(ev[r][4]): continue
                l = ev[r][3]; w = rf[r]
                if w=='I': continue
                # walk back mo from w over RMWs to find release heads
                k = pos[(l,w)]
                while True:
                    cand = mo[l][k]
                    if cand!='I' and isrel(ev[cand][4]) and ev[cand][3]==l:
                        sw.add((cand,r))
                    if cand!='I' and ev[cand][2]=='U' and k>0:
                        k -= 1; continue
                    break
            hb = set(sb)|sw
            # transitive closure
            changed=True
            while changed:
                changed=False
                new = {(a,c) for (a,b) in hb for (b2,c) in hb if b==b2}
                if not new<=hb: hb|=new; changed=True
            # coherence via standard per-location shapes (equivalent to
            # irreflexive(hb;eco?) for this program):
            def mob(a,b,l): return pos[(l,a)]<pos[(l,b)]
            bad=False
            for l in locs:
                ws=[w for w in W if ev[w][3]==l]; rs=[r for r in R if ev[r][3]==l]
                # CoWW
                for a in ws:
                    for b in ws:
                        if a!=b and (a,b) in hb and mob(b,a,l): bad=True
                # CoWR: w hb r, r reads w' mo-before w
                for w in ws:
                    for r in rs:
                        if w!=r and (w,r) in hb and rf[r]!=w and (rf[r]=='I' or mob(rf[r],w,l)): bad=True
                # CoRW: r hb w, r reads w' with w mo-before-or-equal w'
                for r in rs:
                    for w in ws:
                        if w!=r and (r,w) in hb and rf[r]!='I' and (rf[r]==w or mob(w,rf[r],l)): bad=True
                # CoRR
                for r1 in rs:
                    for r2 in rs:
                        if r1!=r2 and (r1,r2) in hb:
                            a,b=rf[r1],rf[r2]
                            if a!=b and (b=='I' or (a!='I' and mob(b,a,l))): bad=True
            if bad: continue
            # no-thin-air: acyclic(sb U rf)
            g = {i:set() for i in range(n)}
            for a,b in sb: g[a].add(b)
            for r,w in rf.items():
                if w!='I': g[w].add(r)
            color={}
            def dfs(u):
                color[u]=1
                for v in g[u]:
                    if color.get(v)==1: return True
                    if v not in color and dfs(v): return True
                color[u]=2; return False
            if any(i not in color and dfs(i) for i in range(n)): continue
            # scenario filter: F saw sent, D confirmed and discharged, S attached
            idx = {(e[0],e[1]):i for i,e in enumerate(ev)}
            rv = lambda t,k: val[('r',idx[(t,k)])]
            if rv('F',0)!=1: continue
            if rv('D',0)!=1 or rv('D',1)!=1: continue
            if rv('S',0)!=0 or rv('S',1)!=1: continue
            # SC-consistency of this execution: acyclic(po U rf U mo U fr)
            g2 = {i:set() for i in range(n)}
            for a,b in sb: g2[a].add(b)
            for r_,w_ in rf.items():
                if w_!='I': g2[w_].add(r_)
            for l in locs:
                ws=[w for w in W if ev[w][3]==l]
                for a in ws:
                    for b in ws:
                        if a!=b and mob(a,b,l): g2[a].add(b)
                for r_ in [r for r in R if ev[r][3]==l]:
                    for b in ws:
                        if b!=r_ and (rf[r_]=='I' or mob(rf[r_],b,l)): g2[r_].add(b)
            col={}
            def dfs2(u):
                col[u]=1
                for v in g2[u]:
                    if col.get(v)==1: return True
                    if v not in col and dfs2(v): return True
                col[u]=2; return False
            sc = not any(i not in col and dfs2(i) for i in range(n))
            tag = ' [SC]' if sc else ' [RC11-only]'
            k=2
            if fix:
                if rv('S',2)!=3:
                    out='REJECT (es3=%d at validation)'%rv('S',2)+tag
                    results[out]=results.get(out,0)+1
                    continue
                k=3
            es2,sq2,es3,sq3 = rv('S',k),rv('S',k+1),rv('S',k+2),rv('S',k+3)
            out = ('OK' if (es2,sq2,es3,sq3)==(2,1,3,1) else 'STALE es2=%d sq2=%d es3=%d sq3=%d'%(es2,sq2,es3,sq3))+tag
            results[out] = results.get(out,0)+1
    return results

for name,kw in [('8275359 (no seqtEs acquire)',dict(fix=False)),
                ('with seqtEs acquire',dict(fix=True)),
                ('round-1 alt: Sd add rel + confirmer acq, no seqtEs',dict(fix=False,sd_relacq=True)),
                ('with seqtEs acquire + later-lap es3 writer',dict(fix=True,mixed=True))]:
    r = run(**kw)
    print('%-52s' % name, ', '.join('%s x%d'%(k,v) for k,v in sorted(r.items())))
