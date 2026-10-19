#!/usr/bin/env python3
"""Off-CCD measurement runner and summarizer for tools/perf/offccd and tools/perf/c2c.

  python3 -B offccd_sweep.py --suite smoke --out /tmp/offccd-smoke --runs 2 --window-ms 300
  python3 -B offccd_sweep.py --suite calib,main-c,main-p --out DIR --probe-perf
  python3 -B offccd_sweep.py --summarize DIR [DIR ...]

Cells run in seeded-shuffled interleaved rounds (no quiescence waits, nothing
discarded for noise); cells with CV or range above the thresholds get top-up
runs. Placement comes from the drivers (threadpool eligible L3 groups); the
runner never computes topology. It records the first driver run's topology
in metadata.json and refuses to merge runs from different CPU models, OS
scopes or grouping sources. Skipped and invalid runs never enter statistics.
"""
import argparse
import hashlib
import json
import math
import os
import pathlib
import platform
import random
import re
import signal
import statistics
import subprocess
import sys
import tempfile
import time

P = pathlib.Path
HERE = P(__file__).resolve().parent
ROOT = HERE.parents[1]

COMBOS4 = [('touch', 1700, 'stream'), ('touch', 1700, 'phased'),
           ('touch', 16, 'stream'), ('touch', 16, 'phased')]
COMBOS8 = [(cost, batch, mode) for cost in ('touch', 'w100') for batch in (16, 1700)
           for mode in ('stream', 'phased')]
SUITES = ('calib', 'main-c', 'main-p', 'combined', 'ctl-equal', 'ctl-small', 'ctl-pack',
          'ctl-mirror', 'ctl-rolesplit', 'smt', 'layouts', 'perf', 'lat', 'smoke')
OFF_SWEEPS = ('S_C', 'S_P', 'combined', 'small-off', 'mirror', 'rolesplit')


class MixedRunsError(ValueError):
    """Runs from different hosts, OS scopes or grouping sources."""


# ---- suites -----------------------------------------------------------------

def cell(suite, sweep, args, k=0, role=None, arm=None, kp=None, cost='touch', batch=1700,
         mode='stream', layout='uniform', driver='offccd'):
    labels = dict(suite=suite, sweep=sweep, k=k, role=role, arm=arm, kp=kp,
                  cost=cost, batch=batch, mode=mode, layout=layout)
    cid = '/'.join(str(x) for x in (suite, sweep, arm or '-', role or '-',
                                     f'k{k}' if kp is None else f'kp{kp}kc{k}',
                                     cost, batch, mode, layout))
    return dict(id=cid, driver=driver, labels=labels, args=list(args))


def suite_cells(name, mirror_llc=1):
    """Pure: the cells of one suite. `mirror_llc` is the group the mirror
    controls use as home (the plan's remote group); None marks them skipped."""
    cells = []
    mirror = ['--home-llc', str(mirror_llc)] if mirror_llc is not None else None
    if name == 'calib':
        return [cell('calib', 'calib', ['--mode', m] + (['--pairs', 'all'] if m == 'pingpong' else []),
                     arm=m, driver='c2c') for m in ('pingpong', 'rmw', 'pull')]
    if name == 'main-c':
        for k in range(9):
            for cost, batch, mode in COMBOS8:
                cells.append(cell(name, 'S_C', ['--off-c', str(k)], k, 'consumer',
                                  cost=cost, batch=batch, mode=mode))
    elif name == 'main-p':
        for k in range(1, 9):
            for cost, batch, mode in COMBOS8:
                cells.append(cell(name, 'S_P', ['--off-p', str(k)], k, 'producer',
                                  cost=cost, batch=batch, mode=mode))
    elif name == 'combined':
        for kk in (1, 2, 4):
            for cost, batch, mode in COMBOS4:
                cells.append(cell(name, 'combined', ['--off-p', str(kk), '--off-c', str(kk)],
                                  kk, 'both', kp=kk, cost=cost, batch=batch, mode=mode))
    elif name == 'ctl-equal':
        for cost, batch, mode in COMBOS4:
            cells.append(cell(name, 'equal', ['--home-extra-c', '1'], 1, 'consumer', 'on',
                              cost=cost, batch=batch, mode=mode))
    elif name == 'ctl-small':
        small = ['--home-p', '1', '--home-c', '2']
        for cost, batch, mode in (('touch', 1700, 'stream'), ('touch', 1700, 'phased'),
                                  ('touch', 16, 'stream')):
            cells.append(cell(name, 'small', small, 0, None, 'base', cost=cost, batch=batch, mode=mode))
            for role, flag in (('consumer', 'c'), ('producer', 'p')):
                for k in (1, 3, 5):
                    for arm, where in (('off', f'--off-{flag}'), ('on', f'--home-extra-{flag}')):
                        cells.append(cell(name, 'small', small + [where, str(k)], k, role, arm,
                                          cost=cost, batch=batch, mode=mode))
    elif name == 'ctl-pack':
        for n in range(1, 7):
            for batch in (1700, 16):
                cells.append(cell(name, 'pack', ['--home-c', str(n)], n, 'consumer',
                                  batch=batch))
    elif name == 'ctl-mirror':
        placements = [(0, None, [])]
        placements += [(k, 'consumer', ['--off-c', str(k)]) for k in (1, 4, 8)]
        placements += [(k, 'producer', ['--off-p', str(k)]) for k in (1, 4, 8)]
        for k, role, extra in placements:
            for cost, batch, mode in COMBOS4:
                c = cell(name, 'mirror', (mirror or []) + extra, k, role, 'mirror',
                         cost=cost, batch=batch, mode=mode)
                if mirror is None:
                    c['skip'] = 'single-llc'
                cells.append(c)
    elif name == 'ctl-rolesplit':
        split = ['--home-p', '2', '--home-c', '0', '--off-c', '5']
        for arm, extra in (('main', []), ('mirror', mirror)):
            for cost, batch, mode in COMBOS4:
                c = cell(name, 'rolesplit', (extra or []) + split, 5, 'consumer', arm,
                         cost=cost, batch=batch, mode=mode)
                if extra is None:
                    c['skip'] = 'single-llc'
                cells.append(c)
    elif name == 'smt':
        for role, flag, ks in (('consumer', 'c', (1, 3, 5)), ('producer', 'p', (1, 2))):
            for k in ks:
                for batch in (1700, 16):
                    cells.append(cell(name, 'smt', [f'--smt-{flag}', str(k)], k, role, batch=batch))
    elif name == 'layouts':
        for layout in ('compact',):
            for mode in ('stream', 'phased'):
                for k in (0, 1, 4, 8):
                    cells.append(cell(name, 'S_C', ['--off-c', str(k)], k, 'consumer', batch=256,
                                      mode=mode, layout=layout))
                for k in (1, 4, 8):
                    cells.append(cell(name, 'S_P', ['--off-p', str(k)], k, 'producer', batch=256,
                                      mode=mode, layout=layout))
    elif name == 'perf':
        for k in (0, 1, 4, 8):
            for batch in (16, 1700):
                c = cell(name, 'perf', ['--off-c', str(k)], k, 'consumer', batch=batch)
                c['perf'] = True
                cells.append(c)
    elif name == 'lat':
        for k in (0, 1, 4, 8):
            cells.append(cell(name, 'lat', ['--off-c', str(k), '--lat-every', '64'], k, 'consumer'))
    elif name == 'smoke':
        for mode in ('stream', 'phased'):
            for k in (0, 1, 8):
                cells.append(cell(name, 'S_C', ['--off-c', str(k)], k, 'consumer', mode=mode))
            for k in (1, 8):
                cells.append(cell(name, 'S_P', ['--off-p', str(k)], k, 'producer', mode=mode))
        cells.append(cell(name, 'equal', ['--home-extra-c', '1'], 1, 'consumer', 'on'))
        cells.append(cell(name, 'calib', ['--mode', 'pingpong', '--pairs', 'classes'],
                          arm='pingpong', driver='c2c'))
    else:
        raise ValueError(f'unknown suite {name}')
    return cells


def executable_path(bin_dir, name):
    return P(bin_dir) / (name + '.exe' if sys.platform == 'win32' else name)


def offccd_argv(c, bin_dir, window_ms, seed):
    lb = c['labels']
    return [str(executable_path(bin_dir, 'offccd'))] + c['args'] + [
        '--cost', lb['cost'], '--batch', str(lb['batch']), '--mode', lb['mode'],
        '--layout', lb['layout'], '--window-ms', str(window_ms), '--seed', str(seed)]


# ---- process helpers --------------------------------------------------------

def sha(path):
    return hashlib.sha256(P(path).read_bytes()).hexdigest()


def kill_process_group(proc):
    if proc.poll() is not None:
        return
    if os.name == 'nt':
        subprocess.run(['taskkill', '/PID', str(proc.pid), '/T', '/F'], capture_output=True)
        proc.wait(timeout=5)
        return
    for sig, wait in ((signal.SIGTERM, 2), (signal.SIGKILL, 5)):
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            proc.wait(timeout=wait)
            return
        except subprocess.TimeoutExpired:
            continue


def run_cmd(cmd, timeout, cwd=None):
    """One driver in its own session. Returns (stdout, stderr, rc|'timeout')."""
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, start_new_session=True)
    try:
        out, err = proc.communicate(timeout=timeout)
        return out, err, proc.returncode
    except subprocess.TimeoutExpired:
        kill_process_group(proc)
        try:
            out, err = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            out, err = '', ''
        return out or '', err or '', 'timeout'


def try_cmd(cmd, timeout=20):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout or '') + (r.stderr or '')
    except (OSError, subprocess.TimeoutExpired) as e:
        return None, str(e)


def read_text(path, default=None):
    try:
        return P(path).read_text().strip()
    except OSError:
        return default


def loadavg():
    try:
        return list(os.getloadavg())
    except (AttributeError, OSError):
        return None


def online_cpus():
    spec = read_text('/sys/devices/system/cpu/online', '')
    cpus = []
    for part in filter(None, spec.split(',')):
        a, _, b = part.partition('-')
        cpus += list(range(int(a), int(b or a) + 1))
    return cpus


def cpu_model():
    for line in (read_text('/proc/cpuinfo', '') or '').splitlines():
        if line.startswith('model name'):
            return line.split(':', 1)[1].strip()
    return platform.processor() or platform.machine()


# ---- host metadata and perf probe --------------------------------------------

PERF_EVENT_RE = re.compile(r'fills_from_sys|ext_cache|l3_', re.I)


def probe_perf():
    """Record perf availability; never changes perf_event_paranoid."""
    probe = dict(paranoid=read_text('/proc/sys/kernel/perf_event_paranoid'),
                 event_source_visible=P('/sys/bus/event_source/devices').exists(),
                 ibs_op=P('/sys/bus/event_source/devices/ibs_op').exists())
    rc, out = try_cmd(['perf', '--version'])
    probe['version'] = out.strip() if rc == 0 else None
    if rc != 0:
        probe.update(stat='unavailable', c2c='unavailable', events=[], reason='perf not found')
        return probe
    rc, out = try_cmd(['perf', 'stat', '-x,', '-e', 'cycles:u', '--', 'true'])
    probe['stat_generic'] = 'available' if rc == 0 and 'not supported' not in out else 'unavailable'
    rc, out = try_cmd(['perf', 'list'], timeout=60)
    # Event names only (section headers end in ':'); remote-CCX fills first.
    names = {ln.split()[0] for ln in out.splitlines() if ln.strip()} if rc == 0 else set()
    events = sorted((n for n in names if PERF_EVENT_RE.search(n) and not n.endswith(':')),
                    key=lambda n: ('fills_from_sys' not in n, 'ext_cache' not in n, n))
    probe['events'] = events
    probe['event'] = events[0] if events else None
    if events:
        rc, out = try_cmd(['perf', 'stat', '-x,', '-e', events[0] + ':u', '--', 'true'])
        ok = rc == 0 and 'not supported' not in out and '<not counted>' not in out
        probe['stat'] = 'available' if ok else 'unavailable'
        probe['stat_output'] = out.strip()[-400:]
    else:
        probe['stat'] = 'unavailable'
        probe['stat_output'] = 'no matching remote-fill event in perf list'
    with tempfile.TemporaryDirectory() as d:
        rc, out = try_cmd(['perf', 'c2c', 'record', '-o', str(P(d) / 'c2c.data'), '--', 'sleep', '0.05'])
    probe['c2c'] = 'available' if rc == 0 else 'unavailable'
    probe['c2c_output'] = out.strip()[-400:]
    return probe


def git_state():
    rc, head = try_cmd(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'])
    _, status = try_cmd(['git', '-C', str(ROOT), 'status', '--porcelain', '--untracked-files=no'])
    _, diff = try_cmd(['git', '-C', str(ROOT), 'diff', 'HEAD'])
    return dict(head=head.strip() if rc == 0 else None, dirty=bool(status.strip()),
                diff_sha256=hashlib.sha256(diff.encode()).hexdigest())


def host_metadata(bin_dir, seed, host_note, perf):
    cpus = online_cpus()
    def per_cpu(name):
        return {c: read_text(f'/sys/devices/system/cpu/cpu{c}/cpufreq/{name}', 'unavailable')
                for c in cpus}
    binaries = {n: sha(path) for n in ('offccd', 'c2c')
                if (path := executable_path(bin_dir, n)).is_file()}
    tools = {}
    for tool in ('ldc2', 'dmd'):
        rc, out = try_cmd([tool, '--version'])
        tools[tool] = out.splitlines()[0] if rc == 0 and out else None
    affinity = sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None
    return dict(
        cpu_model=cpu_model(), uname=' '.join(platform.uname()), tools=tools, git=git_state(),
        binaries=binaries, governors=per_cpu('scaling_governor'),
        epp=per_cpu('energy_performance_preference'),
        thp=read_text('/sys/kernel/mm/transparent_hugepage/enabled'),
        perf_event_paranoid=read_text('/proc/sys/kernel/perf_event_paranoid'),
        runner_affinity=affinity, seed=seed, perf=perf, host_note=host_note,
        topology=None)


# ---- running ----------------------------------------------------------------

def parse_driver(out):
    for line in reversed(out.strip().splitlines()):
        try:
            return json.loads(line)
        except ValueError:
            continue
    return None


def run_cell(c, a, round_idx, order, topup, cpu_model_, perf_event=None, perf_dir=None):
    """Run one offccd cell; returns the runs.jsonl row."""
    argv = offccd_argv(c, a.bin_dir, a.window_ms, a.seed)
    perf_file = None
    if perf_event:
        perf_file = P(perf_dir) / f'{len(list(P(perf_dir).iterdir())):05d}.csv'
        argv = ['perf', 'stat', '-x,', '-o', str(perf_file), '-e', perf_event + ':u', '--'] + argv
    la0 = loadavg()
    t0 = time.monotonic()
    out, err, rc = run_cmd(argv, a.timeout)
    wall = time.monotonic() - t0
    result = parse_driver(out)
    if result is None:
        result = dict(status='invalid', reason='timeout' if rc == 'timeout' else 'driver-error')
    row = dict(cell=c['id'], labels=c['labels'], argv=argv, rc=rc, wall_s=wall,
               loadavg_start=la0, loadavg_end=loadavg(), round=round_idx, order=order,
               topup=topup, cpu_model=cpu_model_, result=result, stderr=err[-2000:])
    if perf_file is not None and perf_file.exists():
        row['perf'] = parse_perf_stat(perf_file.read_text())
    return row


def parse_perf_stat(text):
    counts = {}
    for line in text.splitlines():
        f = line.split(',')
        if len(f) >= 3 and not line.startswith('#'):
            try:
                counts[f[2]] = float(f[0])
            except ValueError:
                counts[f[2]] = None
    return counts


def skip_row(c, reason, cpu_model_):
    return dict(cell=c['id'], labels=c['labels'], argv=None, rc=None, wall_s=0.0, round=0,
                order=0, topup=False, cpu_model=cpu_model_,
                result=dict(status='skipped', reason=reason))


def run_calib(c, a, out_dir):
    argv = [str(executable_path(a.bin_dir, 'c2c'))] + c['args']
    out, err, rc = run_cmd(argv, max(a.timeout, 600))
    lines = []
    for ln in out.splitlines():
        try:
            lines.append(json.loads(ln))
        except ValueError:
            pass
    (P(out_dir) / 'calib').mkdir(exist_ok=True)
    with (P(out_dir) / 'calib' / f"{c['labels']['arm']}.jsonl").open('a') as f:
        for ln in lines:
            f.write(json.dumps(ln) + '\n')
    return dict(cell=c['id'], argv=argv, rc=rc, lines=len(lines), stderr=err[-2000:])


def mirror_group(topo):
    """Pure: the group the mirror controls use as home, or None. Only the
    plan's own remote group qualifies, and never on a single-LLC scope, where
    a "mirror" would be a second home-only run."""
    if not topo or topo.get('single_llc'):
        return None
    return topo.get('remote_llc')


def run_suites(a):
    out = P(a.out)
    if out.exists():
        raise SystemExit(f'--out {out} must not exist (evidence is never overwritten)')
    out.mkdir(parents=True)
    (out / 'raw').mkdir()
    perf = probe_perf() if a.probe_perf or 'perf' in a.suites else None
    meta = host_metadata(a.bin_dir, a.seed, a.host_note, perf)
    meta['arguments'] = {k: v for k, v in vars(a).items() if k != 'func'}
    def dump_meta():
        (out / 'metadata.json').write_text(json.dumps(meta, indent=2, default=str) + '\n')
    dump_meta()

    # The first driver run: the plan, whose topology the runner records.
    probe_out, _, _ = run_cmd([str(executable_path(a.bin_dir, 'offccd')), '--plan-only', '--off-c', '1'], a.timeout)
    first = parse_driver(probe_out) or {}
    topo = first.get('topology')
    meta['topology'] = topo
    dump_meta()
    mirror_llc = mirror_group(topo)

    cells = []
    for s in a.suites:
        cells += suite_cells(s, mirror_llc)
    calib = [c for c in cells if c['driver'] == 'c2c']
    runs_cells = [c for c in cells if c['driver'] == 'offccd']
    perf_event = perf.get('event') if perf and perf.get('stat') == 'available' else None
    perf_dir = out / 'perf'
    if any(c.get('perf') for c in runs_cells) and perf_event:
        perf_dir.mkdir()

    meta['calib_runs'] = [run_calib(c, a, out) for c in calib]
    dump_meta()

    rows_path = out / 'raw' / 'runs.jsonl'
    rows = []
    model = meta['cpu_model']
    with rows_path.open('w') as log:
        def write(row):
            rows.append(row)
            log.write(json.dumps(row) + '\n')
            log.flush()
        settled = {}
        for c in runs_cells:
            if c.get('skip'):
                settled[c['id']] = c['skip']
            elif c.get('perf') and not perf_event:
                settled[c['id']] = 'perf-unavailable'
        for c in runs_cells:
            if c['id'] in settled:
                write(skip_row(c, settled[c['id']], model))

        def one_round(todo, round_idx, topup):
            order = list(todo)
            random.Random(a.seed * 1000 + round_idx).shuffle(order)
            for pos, c in enumerate(order):
                if c['id'] in settled:
                    continue
                row = run_cell(c, a, round_idx, pos, topup, model,
                               perf_event if c.get('perf') else None, perf_dir)
                write(row)
                if row['result'].get('status') == 'skipped':
                    settled[c['id']] = row['result'].get('reason')
                print(f"[{round_idx}:{pos + 1}/{len(order)}] {c['id']} "
                      f"{row['result'].get('status')} {row['result'].get('reason') or ''} "
                      f"{row['wall_s']:.1f}s", flush=True)

        for r in range(a.runs):
            one_round(runs_cells, r, False)
        if a.topup_runs:
            noisy = select_topup(rows, a.cv_threshold, a.range_threshold)
            todo = [c for c in runs_cells if c['id'] in noisy]
            for r in range(a.topup_runs):
                one_round(todo, a.runs + r, True)
            meta['topup_cells'] = sorted(noisy)
            dump_meta()
    write_summary(out, rows, load_calib(out), meta, a)


def load_calib(out):
    lines = []
    for f in sorted((P(out) / 'calib').glob('*.jsonl')) if (P(out) / 'calib').exists() else []:
        lines += [json.loads(ln) for ln in f.read_text().splitlines() if ln.strip()]
    return lines


def write_summary(out, rows, calib, meta, a):
    summary = summarize(rows, calib, meta, a.cv_threshold, a.range_threshold)
    (P(out) / 'summary.json').write_text(json.dumps(summary, indent=2, default=str) + '\n')
    (P(out) / 'summary.md').write_text(render_markdown(summary))
    return summary


# ---- statistics ---------------------------------------------------------------

def stats(xs):
    xs = [x for x in xs if x is not None and not (isinstance(x, float) and math.isnan(x))]
    if not xs:
        return None
    med = statistics.median(xs)
    sd = statistics.stdev(xs) if len(xs) > 1 else 0.0
    mean = statistics.fmean(xs)
    return dict(median=med, min=min(xs), max=max(xs), stddev=sd,
                cv=sd / mean if mean else 0.0, n=len(xs))


def run_metrics(result):
    """Per-run metrics from one ok offccd JSON object."""
    agg = result.get('aggregate', {})
    cons = result.get('consumers', [])
    prods = result.get('producers', [])
    cfg = result.get('config', {})
    m = dict(rate=agg.get('rate_consumed'), rate_window=agg.get('rate_window_consumed'),
             produced=agg.get('rate_produced'), home_rate=agg.get('home_consumer_rate'),
             off_rate=agg.get('off_consumer_rate'), sqcs=cfg.get('sqcs'),
             total_c=len(cons), total_p=len(prods))
    for key, share in (('c', agg.get('consumer_share')), ('p', agg.get('producer_share'))):
        share = share or {}
        m[f'{key}_home_share'] = share.get('home_mean')
        m[f'{key}_off_share'] = share.get('off_mean')
        m[f'{key}_smt_share'] = share.get('smt_mean')
        m[f'{key}_jain'] = share.get('jain')
    sub = agg.get('subwindow_rates') or []
    if sub:
        med = statistics.median(sub)
        m['subwindow_median'] = med
        m['subwindow_spread'] = (max(sub) - min(sub)) / med if med else None
    items = sum(c.get('items_total', 0) for c in cons)
    m['empty_per_1k'] = sum(c.get('empty_visits', 0) for c in cons) * 1000.0 / items if items else None
    vt = sum(c.get('visits_true', 0) for c in cons)
    vf = sum(c.get('visits_false', 0) for c in cons)
    m['idle_pct'] = 100.0 * vf / (vt + vf) if vt + vf else None
    calls = sum(p.get('write_calls', 0) for p in prods)
    m['write0_pct'] = 100.0 * sum(p.get('write_zero', 0) for p in prods) / calls if calls else None
    m['items_per_write'] = sum(p.get('items_total', 0) for p in prods) / calls if calls else None
    m['phase_overflow'] = sum(p.get('phase_overflow', 0) for p in prods)
    ph = result.get('phased')
    if ph:
        for k in ('R_pub', 'R_con', 'R_e2e', 'R_overlap'):
            m[k] = ph.get(k)
        m['wt_words_per_round_max'] = ph.get('wt_words_per_round_max')
        m['round_budget_words'] = ph.get('round_budget_words')
        for loc in ('home', 'off'):
            p50 = [c['first_cb_after_flip_ns'].get('p50') for c in cons
                   if (c['loc'] == 'off') == (loc == 'off') and c.get('first_cb_after_flip_ns')]
            p50 = [x for x in p50 if x is not None]
            m[f'first_cb_{loc}_p50'] = statistics.median(p50) if p50 else None
    scan = result.get('claim_scan')
    if scan:
        m['failed_per_table'] = scan.get('failed_per_table')
        m['failed_per_1k'] = scan.get('failed_per_1k_items')
        m['tprogress_ok'] = 1.0 if scan.get('tprogress_ok') else 0.0
        m['executor_ok'] = 1.0 if scan.get('executor_ok') else 0.0
    for loc in ('home', 'off'):
        lat = [c['latency_ns'].get('p50') for c in cons if c.get('latency_ns')
               and (c['loc'] == 'off') == (loc == 'off')]
        lat = [x for x in lat if x is not None]
        if lat:
            m[f'lat_{loc}_p50'] = statistics.median(lat)
    mhz = [t.get('mhz_mid') for t in result.get('threads', []) if t.get('mhz_mid')]
    m['mhz_mean'] = statistics.fmean(mhz) if mhz else None
    return m


def plan_key(result):
    plan = result.get('plan') or {}
    cpus = plan.get('cpus') or {}
    ctl = (plan.get('controller') or {}).get('cpu')
    return ';'.join(f'{k}={cpus[k]}' for k in sorted(cpus)) + (f';controller={ctl}' if ctl else '')


def check_mergeable(rows):
    """Refuse runs from different CPU models, OS scopes or grouping sources."""
    seen = {}
    for r in rows:
        res = r.get('result') or {}
        topo = res.get('topology') or {}
        keys = dict(cpu_model=r.get('cpu_model'),
                    os_scope=(topo.get('os_scope') or {}).get('allowed') if topo else None,
                    grouping_source=topo.get('grouping_source') if topo else None)
        for k, v in keys.items():
            if v is None:
                continue
            if k in seen and seen[k] != v:
                raise MixedRunsError(f'refusing to merge runs with different {k}: '
                                     f'{seen[k]!r} vs {v!r}')
            seen[k] = v
    return seen


def group_cells(rows):
    """Group rows by cell id plus placement plan; ok runs feed statistics,
    skipped and invalid runs are only counted. A non-ok run joins its cell's
    plan group when the cell has exactly one ok plan."""
    plans = {}
    for r in rows:
        res = r.get('result') or {}
        if res.get('status') == 'ok':
            plans.setdefault(r['cell'], set()).add(plan_key(res))
    cells = {}
    for r in rows:
        res = r.get('result') or {}
        if res.get('status') == 'ok':
            key = (r['cell'], plan_key(res))
        else:
            known = plans.get(r['cell'], set())
            key = (r['cell'], next(iter(known)) if len(known) == 1 else '')
        g = cells.setdefault(key, dict(id=r['cell'], labels=r['labels'], plan=key[1], runs=[],
                                       skipped=[], invalid={}, scope_drift=0, subwindow_spread_runs=0,
                                       crossing_runs=0))
        status = res.get('status')
        if status == 'ok':
            g['runs'].append(run_metrics(res))
            if (res.get('topology') or {}).get('crosses_llc'):
                g['crossing_runs'] += 1
            if (res.get('topology') or {}).get('scope_drift'):
                g['scope_drift'] += 1
        elif status == 'skipped':
            g['skipped'].append(res.get('reason'))
        else:
            reason = res.get('reason') or 'unknown'
            g['invalid'][reason] = g['invalid'].get(reason, 0) + 1
    return cells


def cell_stats(g):
    keys = sorted({k for m in g['runs'] for k in m})
    return {k: stats([m.get(k) for m in g['runs']]) for k in keys}


def select_topup(rows, cv_threshold, range_threshold):
    """Cells whose rate CV or range/median exceeds the thresholds."""
    noisy = set()
    for g in group_cells(rows).values():
        s = stats([m.get('rate') for m in g['runs']])
        if s and s['n'] >= 2 and s['median'] and (
                s['cv'] > cv_threshold or (s['max'] - s['min']) / s['median'] > range_threshold):
            noisy.add(g['id'])
    return noisy


def summarize(rows, calib, meta, cv_threshold=0.03, range_threshold=0.06):
    """Pure: grouped statistics, derived comparisons and sensitive results."""
    host = check_mergeable(rows)
    groups = group_cells(rows)
    cells = []
    invalid = {}
    scope_drift = 0
    for (cid, pk), g in sorted(groups.items()):
        st = cell_stats(g)
        for k, v in g['invalid'].items():
            invalid[k] = invalid.get(k, 0) + v
        scope_drift += g['scope_drift']
        status = 'ok' if g['runs'] else ('skipped' if g['skipped'] else 'invalid')
        cells.append(dict(id=cid, labels=g['labels'], plan=pk, status=status,
                          reason=g['skipped'][0] if status == 'skipped' else None,
                          n=len(g['runs']), invalid=g['invalid'], stats=st,
                          subwindow_spread_runs=sum(1 for m in g['runs']
                                                    if (m.get('subwindow_spread') or 0) > 0.10),
                          scope_drift=g['scope_drift'], crossing_runs=g['crossing_runs']))
    off = [c for c in cells if c['labels']['sweep'] in OFF_SWEEPS and
           (c['labels']['k'] or c['labels']['sweep'] in ('mirror', 'rolesplit'))]
    # Only a run that placed threads on a second group measures off-CCD: the
    # mirror controls at k=0 are home-only runs on the other group.
    off_measured = [c for c in off if c['status'] == 'ok' and c['crossing_runs']]
    skip_reasons = sorted({c['reason'] for c in off if c['status'] == 'skipped' and c['reason']})
    topo = meta.get('topology') or {}
    summary = dict(
        host=dict(cpu_model=host.get('cpu_model') or meta.get('cpu_model'),
                  git=meta.get('git'), binaries=meta.get('binaries'), seed=meta.get('seed'),
                  host_note=meta.get('host_note'), perf=meta.get('perf'),
                  arguments=meta.get('arguments')),
        topology=dict(grouping_source=topo.get('grouping_source'),
                      os_scope=topo.get('os_scope'),
                      eligible_llc_count=topo.get('eligible_llc_count'),
                      llc_count=topo.get('llc_count'), single_llc=topo.get('single_llc'),
                      home_llc=topo.get('home_llc'), remote_llc=topo.get('remote_llc')),
        off_ccd_measured=bool(off_measured) or not off,
        off_ccd_skip_reasons=skip_reasons,
        invalid_by_reason=invalid, scope_drift_runs=scope_drift,
        runs=len(rows), wall_s=sum(r.get('wall_s') or 0 for r in rows),
        cells=cells, calibration=summarize_calib(calib),
        thresholds=dict(cv=cv_threshold, range=range_threshold))
    summary['derived'] = derive(cells)
    summary['sensitive'] = sensitive(cells, summary, cv_threshold)
    return summary


def summarize_calib(lines):
    out = dict(pingpong={}, rmw=[], pull=[], skipped=[])
    for ln in lines:
        kind = ln.get('kind')
        if ln.get('status') == 'skipped':
            out['skipped'].append(dict(kind=kind, reason=ln.get('reason'),
                                       what=ln.get('class') or ln.get('config') or ln.get('direction')))
        elif kind == 'pingpong-class' and ln.get('status') == 'ok':
            out['pingpong'][ln['class']] = ln.get('oneway_ns_over_pair_medians')
        elif kind == 'rmw' and ln.get('status') == 'ok':
            out['rmw'].append(dict(config=ln['config'], mops=ln['mops'], ns_per_op=ln['ns_per_op']))
        elif kind == 'pull' and ln.get('status') == 'ok':
            out['pull'].append(dict(direction=ln['direction'], gbps=ln['gbps'],
                                    ns_per_line=ln['ns_per_line']))
        elif ln.get('status') == 'invalid':
            out.setdefault('invalid', []).append(ln.get('reason'))
    pp = out['pingpong']
    if (pp.get('cross-llc') or {}).get('median') and (pp.get('intra-llc') or {}).get('median'):
        out['cross_intra_ratio'] = pp['cross-llc']['median'] / pp['intra-llc']['median']
    return out


def med(cell, key):
    s = (cell or {}).get('stats', {}).get(key)
    return s['median'] if s else None


def find(cells, **want):
    for c in cells:
        if c['status'] == 'ok' and all(c['labels'].get(k) == v for k, v in want.items()):
            return c
    return None


def combo_of(c):
    lb = c['labels']
    return (lb['cost'], lb['batch'], lb['mode'], lb['layout'])


def derive(cells):
    """Δ vs k=0, marginal steps, combined-diagonal prediction, control ratios."""
    d = dict(sweeps=[], combined=[], controls=[])
    for sweep in ('S_C', 'S_P'):
        combos = sorted({combo_of(c) for c in cells if c['labels']['sweep'] == sweep
                         and c['status'] == 'ok'})
        for cost, batch, mode, layout in combos:
            base = find(cells, sweep='S_C', k=0, cost=cost, batch=batch, mode=mode, layout=layout)
            prev, kprev = base, 0
            for k in range(1, 9):
                c = find(cells, sweep=sweep, k=k, cost=cost, batch=batch, mode=mode, layout=layout)
                if c is None:
                    continue
                r, r0, rp = med(c, 'rate'), med(base, 'rate'), med(prev, 'rate')
                # Per added thread: the geometric step over a gap in k.
                d['sweeps'].append(dict(sweep=sweep, k=k, cost=cost, batch=batch, mode=mode,
                                        layout=layout, delta_vs_k0=r / r0 - 1 if r and r0 else None,
                                        marginal=(r / rp) ** (1 / (k - kprev)) - 1 if r and rp else None))
                prev, kprev = c, k
    for c in cells:
        lb = c['labels']
        if lb['sweep'] != 'combined' or c['status'] != 'ok':
            continue
        kw = dict(cost=lb['cost'], batch=lb['batch'], mode=lb['mode'], layout=lb['layout'])
        s0 = med(find(cells, sweep='S_C', k=0, **kw), 'rate')
        rc = med(find(cells, sweep='S_C', k=lb['k'], **kw), 'rate')
        rp = med(find(cells, sweep='S_P', k=lb['kp'], **kw), 'rate')
        pred = s0 * (rc / s0) * (rp / s0) if s0 and rc and rp else None
        d['combined'].append(dict(kp=lb['kp'], kc=lb['k'], measured=med(c, 'rate'),
                                  predicted=pred, **kw))
    for c in cells:
        lb = c['labels']
        kw = dict(cost=lb['cost'], batch=lb['batch'], mode=lb['mode'], layout=lb['layout'])
        if c['status'] != 'ok':
            continue
        if lb['sweep'] == 'equal':
            off = find(cells, sweep='S_C', k=1, **kw)
            d['controls'].append(dict(control='equal', on=med(c, 'rate'), off=med(off, 'rate'),
                                      ratio=ratio(med(off, 'rate'), med(c, 'rate')), **kw))
        elif lb['sweep'] == 'small' and lb['arm'] == 'off':
            on = find(cells, sweep='small', arm='on', role=lb['role'], k=lb['k'], **kw)
            d['controls'].append(dict(control='small', role=lb['role'], k=lb['k'],
                                      off=med(c, 'rate'), on=med(on, 'rate'),
                                      ratio=ratio(med(c, 'rate'), med(on, 'rate')), **kw))
        elif lb['sweep'] == 'mirror':
            sweep = 'S_C' if lb['role'] in (None, 'consumer') else 'S_P'
            main = find(cells, sweep=sweep, k=lb['k'], **kw)
            d['controls'].append(dict(control='mirror', role=lb['role'], k=lb['k'],
                                      main=med(main, 'rate'), mirror=med(c, 'rate'),
                                      ratio=ratio(med(c, 'rate'), med(main, 'rate')), **kw))
        elif lb['sweep'] == 'rolesplit' and lb['arm'] == 'main':
            mir = find(cells, sweep='rolesplit', arm='mirror', **kw)
            d['controls'].append(dict(control='rolesplit', main=med(c, 'rate'),
                                      mirror=med(mir, 'rate'),
                                      ratio=ratio(med(mir, 'rate'), med(c, 'rate')), **kw))
    return d


def ratio(a, b):
    return a / b if a is not None and b else None


def sensitive(cells, summary, cv_threshold):
    out = []
    for c in cells:
        s = c['stats'].get('rate')
        if s and s['n'] >= 2 and s['cv'] > cv_threshold:
            out.append(f"noisy: {c['id']} CV {s['cv']:.1%} (n={s['n']})")
        if c['subwindow_spread_runs']:
            out.append(f"sub-window spread > 10%: {c['id']} in {c['subwindow_spread_runs']} run(s)")
        if c['scope_drift']:
            out.append(f"scope drift: {c['id']} in {c['scope_drift']} run(s)")
    for sweep in ('S_C', 'S_P'):
        for c in cells:
            lb = c['labels']
            if lb['sweep'] != sweep or c['status'] != 'ok' or not lb['k']:
                continue
            prevk = lb['k'] - 1
            p = find(cells, sweep='S_C' if prevk == 0 else sweep, k=prevk, cost=lb['cost'],
                     batch=lb['batch'], mode=lb['mode'], layout=lb['layout'])
            a, b = c['stats'].get('rate'), (p or {}).get('stats', {}).get('rate')
            if a and b and abs(a['median'] - b['median']) < max(a['max'] - a['min'], b['max'] - b['min']):
                out.append(f"not resolved: {c['id']} vs k={prevk}")
            ma, mb = med(c, 'mhz_mean'), med(p, 'mhz_mean')
            if ma and mb and abs(ma / mb - 1) > 0.03:
                out.append(f"MHz differs {abs(ma / mb - 1):.1%}: {c['id']} vs k={prevk}")
    for ctl in summary['derived']['controls']:
        if ctl['control'] == 'mirror' and ctl.get('ratio') and abs(ctl['ratio'] - 1) > 0.05:
            out.append(f"mirror vs main disagree {ctl['ratio'] - 1:+.1%}: role={ctl['role']} "
                       f"k={ctl['k']} {ctl['cost']}/{ctl['batch']}/{ctl['mode']}/{ctl['layout']}")
    for reason, n in sorted(summary['invalid_by_reason'].items()):
        out.append(f'invalid runs: {n} × {reason}')
    return out


# ---- markdown -----------------------------------------------------------------

def fmt(s, scale=1.0, digits=1):
    if not s:
        return 'n/a'
    return (f"{s['median'] / scale:.{digits}f} [{s['min'] / scale:.{digits}f}–"
            f"{s['max'] / scale:.{digits}f}] σ{s['stddev'] / scale:.{digits}f} n{s['n']}")


def pct(x):
    return 'n/a' if x is None else f'{x:+.1%}'


def num(x, digits=2):
    return 'n/a' if x is None else f'{x:.{digits}f}'


def table(header, rows):
    if not rows:
        return '_not run_\n'
    lines = ['| ' + ' | '.join(header) + ' |', '|' + '---|' * len(header)]
    lines += ['| ' + ' | '.join(str(x) for x in r) + ' |' for r in rows]
    return '\n'.join(lines) + '\n'


def render_markdown(s):
    cells = s['cells']
    out = ['# Off-CCD measurement summary\n']
    h, t = s['host'], s['topology']
    out.append('## 0. Header\n')
    perf = h.get('perf') or {}
    out.append('\n'.join([
        f"- host: {h.get('cpu_model')}",
        f"- commit: {(h.get('git') or {}).get('head')} dirty={(h.get('git') or {}).get('dirty')}",
        f"- binaries: {h.get('binaries')}",
        f"- seed: {h.get('seed')}; runs: {s['runs']}; wall: {s['wall_s']:.0f} s",
        f"- grouping_source: {t.get('grouping_source')}; eligible_llc_count: "
        f"{t.get('eligible_llc_count')} (physical {t.get('llc_count')})",
        f"- OS scope: {(t.get('os_scope') or {}).get('source')} "
        f"{(t.get('os_scope') or {}).get('allowed')}",
        f"- single_llc: {t.get('single_llc')}",
        f"- perf: stat={perf.get('stat', 'not probed')} c2c={perf.get('c2c', 'not probed')} "
        f"paranoid={perf.get('paranoid')}",
        f"- invalid runs by reason: {s['invalid_by_reason'] or 'none'}",
        f"- host note (BIOS/PBO/CPPC): {h.get('host_note') or 'not recorded'}",
    ]) + '\n')
    if not s['off_ccd_measured']:
        out.append(f"\n**single-LLC host: off-CCD sweeps not measured "
                   f"({', '.join(s['off_ccd_skip_reasons']) or 'skipped'})**\n")
    out.append('\nNumbers are `median [min–max] σ n`; rates in Mitems/s. Smoke runs are '
               'validation, not performance claims.\n')

    cal = s['calibration']
    out.append('\n## 1. Calibration\n')
    if cal['pingpong'] or cal['rmw'] or cal['pull']:
        out.append(table(['class', 'one-way ns over pair medians'],
                         [(k, fmt(v, 1, 1)) for k, v in sorted(cal['pingpong'].items())]))
        if cal.get('cross_intra_ratio'):
            out.append(f"\ncross/intra one-way ratio: {cal['cross_intra_ratio']:.2f}\n")
        out.append('\nrmw:\n\n' + table(['rmw home:remote', 'Mops/s', 'ns/op'],
                                [(r['config'], num(r['mops'], 1), num(r['ns_per_op'], 1))
                                 for r in cal['rmw']]))
        out.append('\npull:\n\n' + table(['pull', 'GB/s', 'ns/line'],
                                [(p['direction'], fmt(p['gbps'], 1, 1), fmt(p['ns_per_line'], 1, 2))
                                 for p in cal['pull']]))
        if cal['skipped']:
            out.append('\nskipped: ' + ', '.join(f"{x['kind']} {x['what']} ({x['reason']})"
                                                for x in cal['skipped']) + '\n')
    else:
        out.append('_not run_\n')

    deltas = {(x['sweep'], x['k'], x['cost'], x['batch'], x['mode'], x['layout']): x
              for x in s['derived']['sweeps']}
    def sweep_rows(sweep, cost, batch, mode, layout):
        rows = []
        for c in sorted((c for c in cells if c['labels']['sweep'] == sweep and combo_of(c) ==
                         (cost, batch, mode, layout)), key=lambda c: c['labels']['k']):
            st = c['stats']
            dx = deltas.get((sweep, c['labels']['k'], cost, batch, mode, layout), {})
            if c['status'] != 'ok':
                rows.append([c['labels']['k'], f"{c['status']} ({c['reason'] or ''})"] + [''] * 9)
                continue
            if sweep == 'S_C':
                rows.append([c['labels']['k'], num(med(c, 'total_c'), 0), num(med(c, 'sqcs'), 0),
                             fmt(st.get('rate'), 1e6), pct(dx.get('delta_vs_k0')), pct(dx.get('marginal')),
                             fmt(st.get('home_rate'), 1e6), fmt(st.get('off_rate'), 1e6),
                             num(med(c, 'c_off_share')), num(med(c, 'c_home_share')),
                             num(med(c, 'c_jain'), 3), num(med(c, 'empty_per_1k')),
                             num(med(c, 'write0_pct'), 1), num(med(c, 'failed_per_table')),
                             num(med(c, 'mhz_mean'), 0)])
            else:
                rows.append([c['labels']['k'], num(med(c, 'total_p'), 0), fmt(st.get('rate'), 1e6),
                             pct(dx.get('delta_vs_k0')), num(med(c, 'p_home_share')),
                             num(med(c, 'p_off_share')), num(med(c, 'items_per_write'), 1),
                             num(med(c, 'write0_pct'), 1), num(med(c, 'idle_pct'), 1),
                             num(med(c, 'failed_per_table')), num(med(c, 'mhz_mean'), 0)])
        return rows
    hdr_c = ['k off-C', 'total C', 'SqCs', 'agg Mitems/s', 'Δ vs k=0', 'Δ marginal', 'home-C',
             'off-C', 'off norm. share', 'home norm. share', 'Jain', 'empty visits /1k',
             'write-0 %', 'failed claims /table', 'MHz']
    hdr_p = ['k off-P', 'total P', 'agg consumed Mitems/s', 'Δ vs k=0', 'home-P write share',
             'off-P write share', 'items/write', 'write-0 %', 'consumer idle %',
             'failed claims /table', 'MHz']
    for num_, sweep, hdr, title in ((2, 'S_C', hdr_c, 'Off-CCD consumers (S_C)'),
                                    (3, 'S_P', hdr_p, 'Off-CCD producers (S_P)')):
        out.append(f'\n## {num_}. {title}\n')
        combos = sorted({combo_of(c) for c in cells if c['labels']['sweep'] == sweep
                         and c['labels']['layout'] == 'uniform'})
        if not combos:
            out.append('_not run_\n')
        for cost, batch, mode, layout in combos:
            out.append(f'\n### {cost} / batch {batch} / {mode}\n')
            if sweep == 'S_P':
                base = find(cells, sweep='S_C', k=0, cost=cost, batch=batch, mode=mode, layout=layout)
                if base:
                    out.append(f"k=0 (shared with S_C): {fmt(base['stats'].get('rate'), 1e6)}\n\n")
            out.append(table(hdr, sweep_rows(sweep, cost, batch, mode, layout)))

    out.append('\n## 4. Streaming vs phased\n')
    rows = []
    for c in cells:
        lb = c['labels']
        if lb['mode'] != 'phased' or c['status'] != 'ok' or lb['sweep'] not in ('S_C', 'S_P', 'equal'):
            continue
        st_ = find(cells, sweep=lb['sweep'], k=lb['k'], cost=lb['cost'], batch=lb['batch'],
                   mode='stream', layout=lb['layout'])
        rs = med(st_, 'rate')
        rows.append([lb['sweep'], lb['k'], f"{lb['cost']}/{lb['batch']}/{lb['layout']}",
                     fmt((st_ or {}).get('stats', {}).get('rate'), 1e6),
                     fmt(c['stats'].get('R_pub'), 1e6), fmt(c['stats'].get('R_con'), 1e6),
                     fmt(c['stats'].get('R_e2e'), 1e6), fmt(c['stats'].get('R_overlap'), 1e6),
                     num(ratio(med(c, 'R_overlap'), rs)),
                     f"{num(med(c, 'first_cb_home_p50'), 0)} / {num(med(c, 'first_cb_off_p50'), 0)}",
                     f"{num(med(c, 'phase_overflow'), 0)} / {num(med(c, 'wt_words_per_round_max'), 0)}"
                     f" ≤ {num(med(c, 'round_budget_words'), 0)}"])
    out.append(table(['sweep', 'k', 'combo', 'R_stream', 'R_pub', 'R_con', 'R_e2e', 'R_overlap',
                      'R_overlap/R_stream', 'first-cb p50 home / off (ns)',
                      'overflow / Wt words per round ≤ budget'], rows))

    out.append('\n## 5. Controls\n')
    ctl = s['derived']['controls']
    for name, hdr, getter in (
            ('equal', ['combo', 'off (S_C k=1)', 'on (2P+6C on H)', 'off/on'],
             lambda x: [f"{x['cost']}/{x['batch']}/{x['mode']}", num(_m(x['off'])), num(_m(x['on'])),
                        num(x['ratio'], 3)]),
            ('small', ['role', 'k', 'combo', 'on-H', 'on-R', 'R/H'],
             lambda x: [x['role'], x['k'], f"{x['cost']}/{x['batch']}/{x['mode']}", num(_m(x['on'])),
                        num(_m(x['off'])), num(x['ratio'], 3)]),
            ('mirror', ['role', 'k', 'combo', 'H=home', 'H=mirror', 'mirror/main'],
             lambda x: [x['role'] or '-', x['k'], f"{x['cost']}/{x['batch']}/{x['mode']}",
                        num(_m(x['main'])), num(_m(x['mirror'])), num(x['ratio'], 3)]),
            ('rolesplit', ['combo', '2P H + 5C R', 'mirrored', 'mirror/main'],
             lambda x: [f"{x['cost']}/{x['batch']}/{x['mode']}", num(_m(x['main'])),
                        num(_m(x['mirror'])), num(x['ratio'], 3)])):
        out.append(f'\n### {name}\n')
        out.append(table(hdr, [getter(x) for x in ctl if x['control'] == name]))
    out.append('\n### combined diagonal\n')
    out.append(table(['kP', 'kC', 'combo', 'measured', 'additive prediction', 'measured/predicted'],
                     [[x['kp'], x['kc'], f"{x['cost']}/{x['batch']}/{x['mode']}", num(_m(x['measured'])),
                       num(_m(x['predicted'])), num(ratio(x['measured'], x['predicted']), 3)]
                      for x in s['derived']['combined']]))
    skipped_ctl = [c for c in cells if c['labels']['sweep'] in ('mirror', 'rolesplit', 'small', 'equal',
                                                                'pack', 'combined')
                   and c['status'] == 'skipped']
    if skipped_ctl:
        out.append('\nskipped controls: ' + ', '.join(f"{c['id']} ({c['reason']})" for c in skipped_ctl) + '\n')
    pack = sorted((c for c in cells if c['labels']['sweep'] == 'pack'), key=lambda c: (c['labels']['batch'], c['labels']['k']))
    out.append('\n### pack ladder (2P + nC on H)\n')
    out.append(table(['n', 'batch', 'SqCs', 'agg Mitems/s'],
                     [[c['labels']['k'], c['labels']['batch'], num(med(c, 'sqcs'), 0),
                       fmt(c['stats'].get('rate'), 1e6) if c['status'] == 'ok' else c['status']]
                      for c in pack]))

    out.append('\n## 6. SMT control (separate; not on the S axis)\n')
    smt = sorted((c for c in cells if c['labels']['sweep'] == 'smt'),
                 key=lambda c: (c['labels']['role'], c['labels']['batch'], c['labels']['k']))
    out.append(table(['role', 'k', 'batch', 'SMT Mitems/s', 'off-CCD (S) same k', 'SMT/off'],
                     [[c['labels']['role'], c['labels']['k'], c['labels']['batch'],
                       fmt(c['stats'].get('rate'), 1e6) if c['status'] == 'ok' else f"{c['status']} ({c['reason']})",
                       num(_m(med(find(cells, sweep='S_C' if c['labels']['role'] == 'consumer' else 'S_P',
                                       k=c['labels']['k'], cost='touch', batch=c['labels']['batch'],
                                       mode='stream', layout='uniform'), 'rate'))),
                       num(ratio(med(c, 'rate'), med(find(cells, sweep='S_C' if c['labels']['role'] == 'consumer'
                                                          else 'S_P', k=c['labels']['k'], cost='touch',
                                                          batch=c['labels']['batch'], mode='stream',
                                                          layout='uniform'), 'rate')), 3)]
                      for c in smt]))

    out.append('\n## 7. Layouts\n')
    lay = sorted((c for c in cells if c['labels']['layout'] != 'uniform'),
                 key=lambda c: (c['labels']['layout'], c['labels']['sweep'], c['labels']['mode'], c['labels']['k']))
    out.append(table(['layout', 'sweep', 'mode', 'k', 'Mitems/s'],
                     [[c['labels']['layout'], c['labels']['sweep'], c['labels']['mode'], c['labels']['k'],
                       fmt(c['stats'].get('rate'), 1e6) if c['status'] == 'ok' else c['status']] for c in lay]))

    out.append('\n## 8. Perf counters / c2c\n')
    pc = [c for c in cells if c['labels']['sweep'] == 'perf']
    if not pc or all(c['status'] != 'ok' for c in pc):
        out.append(f"_not run_ (perf stat {perf.get('stat', 'not probed')}, c2c {perf.get('c2c', 'not probed')})\n")
    else:
        out.append(table(['k', 'batch', 'Mitems/s'], [[c['labels']['k'], c['labels']['batch'],
                                                       fmt(c['stats'].get('rate'), 1e6)] for c in pc]))
    lat = [c for c in cells if c['labels']['sweep'] == 'lat']
    if lat:
        out.append('\nlatency slice (queue depth + handoff; compare home vs off within a cell):\n\n')
        out.append(table(['k', 'home p50 ns', 'off p50 ns'],
                         [[c['labels']['k'], num(med(c, 'lat_home_p50'), 0), num(med(c, 'lat_off_p50'), 0)]
                          for c in sorted(lat, key=lambda c: c['labels']['k'])]))

    out.append('\n## 9. Sensitive results\n')
    out.append('\n'.join(f'- {x}' for x in s['sensitive']) + '\n' if s['sensitive'] else '_none_\n')

    out.append('\n## 10. Answers\n')
    out.append(answers(s) + '\n')
    return ''.join(out)


def _m(x):
    return None if x is None else x / 1e6


def answers(s):
    lines = []
    cells = s['cells']
    for sweep, who in (('S_C', 'consumer'), ('S_P', 'producer')):
        for cost, batch, mode, layout in sorted({combo_of(c) for c in cells if c['labels']['sweep'] == sweep
                                                 and c['labels']['layout'] == 'uniform'}):
            steps = [x['marginal'] for x in s['derived']['sweeps'] if x['sweep'] == sweep and x['cost'] == cost
                     and x['batch'] == batch and x['mode'] == mode and x['layout'] == layout
                     and x['marginal'] is not None]
            if not steps:
                continue
            ks = sorted(c['labels']['k'] for c in cells if c['labels']['sweep'] == sweep and combo_of(c) ==
                        (cost, batch, mode, layout) and c['status'] == 'ok')
            kmax = ks[-1] if ks else None
            last = find(cells, sweep=sweep, k=kmax, cost=cost, batch=batch, mode=mode, layout=layout)
            base = find(cells, sweep='S_C', k=0, cost=cost, batch=batch, mode=mode, layout=layout)
            text = (f"- {cost}/{batch}/{mode}: each added off-CCD {who} changes aggregate throughput by "
                    f"{statistics.median(steps):+.1%} (range {min(steps):+.1%} to {max(steps):+.1%}, "
                    f"k up to {kmax})")
            if sweep == 'S_C' and last and base:
                home = ratio(med(last, 'home_rate'), med(base, 'home_rate'))
                text += (f"; home consumers {pct(home - 1 if home else None)} at k={kmax}; "
                         f"off consumers take {num(med(last, 'c_off_share'))}× a fair share")
            lines.append(text + '.')
    for c in cells:
        lb = c['labels']
        if lb['mode'] == 'phased' and lb['sweep'] == 'S_C' and c['status'] == 'ok':
            st_ = find(cells, sweep='S_C', k=lb['k'], cost=lb['cost'], batch=lb['batch'], mode='stream',
                       layout=lb['layout'])
            lines.append(f"- phased S_C k={lb['k']} {lb['cost']}/{lb['batch']}/{lb['layout']}: R_con "
                         f"{num(_m(med(c, 'R_con')))} vs R_stream {num(_m(med(st_, 'rate')))} Mitems/s; "
                         f"R_overlap/R_stream = {num(ratio(med(c, 'R_overlap'), med(st_, 'rate')))}.")
    return '\n'.join(lines) if lines else '_not enough measured cells_'


# ---- CLI ----------------------------------------------------------------------

def summarize_dirs(dirs, a):
    rows, calib, meta = [], [], None
    for d in dirs:
        rows += [json.loads(ln) for ln in (P(d) / 'raw' / 'runs.jsonl').read_text().splitlines() if ln.strip()]
        calib += load_calib(d)
        meta = meta or json.loads((P(d) / 'metadata.json').read_text())
    target = P(dirs[0])
    return write_summary(target, rows, calib, meta, a)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--suite', default='smoke', help='comma-separated: ' + ','.join(SUITES))
    p.add_argument('--out')
    p.add_argument('--runs', type=int, default=5)
    p.add_argument('--topup-runs', type=int, default=6)
    p.add_argument('--cv-threshold', type=float, default=0.03)
    p.add_argument('--range-threshold', type=float, default=0.06)
    p.add_argument('--seed', type=int, default=1)
    p.add_argument('--window-ms', type=int, default=1000)
    p.add_argument('--probe-perf', action='store_true')
    p.add_argument('--bin-dir', default=str(HERE))
    p.add_argument('--timeout', type=float, default=30)
    p.add_argument('--host-note', default=None, help='free text: BIOS/PBO/CPPC state')
    p.add_argument('--summarize', nargs='+', metavar='DIR')
    a = p.parse_args(argv)
    if a.summarize:
        try:
            summarize_dirs(a.summarize, a)
        except MixedRunsError as e:
            print(f'offccd_sweep: {e}', file=sys.stderr)
            return 2
        return 0
    a.suites = [s for s in a.suite.split(',') if s]
    bad = [s for s in a.suites if s not in SUITES]
    if bad or not a.out:
        p.error(f'unknown suite(s) {bad}' if bad else '--out is required')
    run_suites(a)
    return 0


if __name__ == '__main__':
    sys.exit(main())
