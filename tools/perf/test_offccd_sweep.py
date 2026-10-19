#!/usr/bin/env python3
"""Unit tests for offccd_sweep.py: suites, statistics, merge refusal and rendering.

Run: python3 -B -m unittest tools/perf/test_offccd_sweep.py
Driver output is synthetic; nothing is built or timed.
"""
import contextlib
import hashlib
import importlib.util
import io
import pathlib
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('offccd_sweep', HERE / 'offccd_sweep.py')
sweep = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sweep)

TOPO = dict(grouping_source='l3', os_scope=dict(source='processLaunch', allowed='0-31'),
            eligible_llc_count=2, llc_count=2, single_llc=False, home_llc=0, remote_llc=1,
            scope_drift=False, crosses_llc=True)

LLCS = [dict(llc=0), dict(llc=1)]
# `offccd --plan-only --off-c 1` under taskset -c 0-7,16-23 and 8-15,24-31:
# one eligible group, which is the home; there is no remote.
FIRST_L3_ONLY = dict(TOPO, eligible_llc_count=1, single_llc=True, home_llc=0, remote_llc=None,
                     llcs=LLCS)
SECOND_L3_ONLY = dict(FIRST_L3_ONLY, home_llc=1)


def result(rate, status='ok', reason=None, cpus=None, topology=None, mhz=4600.0,
           off_share=0.8, sub=None):
    if status != 'ok':
        return dict(status=status, reason=reason, topology=topology or TOPO)
    cpus = cpus or {'home-producer': '0-1', 'home-consumer': '2-6'}
    sub = sub or [rate] * 10
    return dict(
        status='ok', reason=None, topology=topology or TOPO,
        plan=dict(cpus=cpus, controller=dict(cpu='0:7')),
        config=dict(sqcs=3),
        aggregate=dict(rate_consumed=rate, rate_window_consumed=rate, rate_produced=rate,
                       home_consumer_rate=rate * 0.9, off_consumer_rate=rate * 0.1,
                       subwindow_rates=sub,
                       consumer_share=dict(home_mean=1.0, off_mean=off_share, jain=0.99),
                       producer_share=dict(home_mean=1.0, off_mean=None, jain=1.0)),
        consumers=[dict(loc='home', items_total=1000, empty_visits=1, visits_true=10,
                        visits_false=2)],
        producers=[dict(loc='home', items_total=1000, write_calls=10, write_zero=1,
                        phase_overflow=0)],
        threads=[dict(mhz_mid=mhz)])


def row(cell_id, res, sweep_='S_C', k=0, model='AMD Ryzen 9 5900XT', mode='stream', arm=None,
        role='consumer'):
    labels = dict(suite='t', sweep=sweep_, k=k, role=role, arm=arm, kp=None, cost='touch',
                  batch=1700, mode=mode, layout='uniform')
    return dict(cell=cell_id, labels=labels, cpu_model=model, result=res, wall_s=1.0)


def cell_by(summary, cid):
    return [c for c in summary['cells'] if c['id'] == cid]


class SuiteTests(unittest.TestCase):
    def test_executable_hashes_match_native_launch_paths(self):
        for host, suffix in (('win32', '.exe'), ('linux', '')):
            with self.subTest(host=host), tempfile.TemporaryDirectory() as d, \
                    mock.patch.object(sweep.sys, 'platform', host), \
                    mock.patch.object(sweep, 'try_cmd', return_value=(1, '')):
                root = pathlib.Path(d)
                expected = {}
                for name in ('offccd', 'c2c'):
                    payload = (name + host).encode()
                    (root / (name + suffix)).write_bytes(payload)
                    (root / (name + ('' if suffix else '.exe'))).write_bytes(b'wrong binary')
                    expected[name] = hashlib.sha256(payload).hexdigest()
                self.assertEqual(sweep.host_metadata(d, 1, None, None)['binaries'], expected)
                cell = sweep.suite_cells('smoke')[0]
                self.assertEqual(sweep.offccd_argv(cell, d, 100, 1)[0], str(root / ('offccd' + suffix)))
                with mock.patch.object(sweep, 'run_cmd', return_value=('', '', 0)) as run:
                    sweep.run_calib(sweep.suite_cells('calib')[0],
                                    SimpleNamespace(bin_dir=d, timeout=1), root)
                self.assertEqual(run.call_args.args[0][0], str(root / ('c2c' + suffix)))

    def test_counts_match_plan(self):
        expected = {'calib': 3, 'main-c': 72, 'main-p': 64, 'combined': 12, 'ctl-equal': 4,
                    'ctl-small': 39, 'ctl-pack': 12, 'ctl-mirror': 28, 'ctl-rolesplit': 8,
                    'smt': 10, 'layouts': 14, 'lat': 4, 'smoke': 12}
        for name, n in expected.items():
            self.assertEqual(len(sweep.suite_cells(name)), n, name)

    def test_ids_unique_and_mirror_uses_remote_group(self):
        cells = [c for s in sweep.SUITES for c in sweep.suite_cells(s, 3)]
        self.assertEqual(len({c['id'] for c in cells}), len(cells))
        for c in sweep.suite_cells('ctl-mirror', 3):
            self.assertEqual(c['args'][:2], ['--home-llc', '3'])

    def test_mirror_without_second_group_is_skipped(self):
        cells = sweep.suite_cells('ctl-mirror', None) + sweep.suite_cells('ctl-rolesplit', None)
        self.assertTrue(all(c.get('skip') == 'single-llc' for c in cells
                            if c['labels']['arm'] == 'mirror'))
        self.assertTrue(all('skip' not in c for c in cells if c['labels']['arm'] == 'main'))

    def test_mirror_group_follows_the_plan_remote(self):
        self.assertEqual(sweep.mirror_group(TOPO), 1)
        self.assertEqual(sweep.mirror_group(dict(TOPO, home_llc=1, remote_llc=0)), 0)
        self.assertIsNone(sweep.mirror_group(None))
        # A single-LLC plan has no remote; the other physical group is not a mirror.
        for topo in (FIRST_L3_ONLY, SECOND_L3_ONLY):
            self.assertIsNone(sweep.mirror_group(topo))
        self.assertIsNone(sweep.mirror_group(dict(SECOND_L3_ONLY, remote_llc=0)))

    def test_argv_carries_combo(self):
        c = sweep.suite_cells('main-c')[0]
        argv = sweep.offccd_argv(c, '/bin', 300, 7)
        expected = 'offccd.exe' if sys.platform == 'win32' else 'offccd'
        self.assertEqual(argv[0], str(pathlib.Path('/bin') / expected))
        for flag in ('--cost', '--batch', '--mode', '--layout', '--window-ms', '--seed', '--off-c'):
            self.assertIn(flag, argv)


class SummaryTests(unittest.TestCase):
    def test_statistics(self):
        s = sweep.stats([10.0, 12.0, 14.0])
        self.assertEqual((s['median'], s['min'], s['max'], s['n']), (12.0, 10.0, 14.0, 3))
        self.assertAlmostEqual(s['stddev'], 2.0)
        self.assertAlmostEqual(s['cv'], 2.0 / 12.0)
        self.assertIsNone(sweep.stats([]))

    def test_skipped_and_invalid_never_enter_statistics(self):
        rows = [row('a', result(100.0)), row('a', result(200.0)),
                row('a', result(None, 'invalid', 'pin-outsideOsScope')),
                row('a', result(None, 'invalid', 'pin-mismatch')),
                row('a', result(None, 'invalid', 'pin-outsideOsScope')),
                row('b', result(None, 'skipped', 'single-llc'), k=1)]
        s = sweep.summarize(rows, [], {})
        a = cell_by(s, 'a')
        self.assertEqual(len(a), 1)
        self.assertEqual(a[0]['stats']['rate']['n'], 2)
        self.assertEqual(a[0]['stats']['rate']['median'], 150.0)
        self.assertEqual(a[0]['invalid'], {'pin-outsideOsScope': 2, 'pin-mismatch': 1})
        self.assertEqual(s['invalid_by_reason'], {'pin-outsideOsScope': 2, 'pin-mismatch': 1})
        b = cell_by(s, 'b')[0]
        self.assertEqual((b['status'], b['reason'], b['n']), ('skipped', 'single-llc', 0))
        self.assertIn('invalid runs: 2 × pin-outsideOsScope', s['sensitive'])

    def test_grouping_keeps_plans_apart(self):
        other = {'home-producer': '8-9', 'home-consumer': '10-14'}
        rows = [row('a', result(100.0)), row('a', result(300.0, cpus=other))]
        s = sweep.summarize(rows, [], {})
        self.assertEqual(sorted(c['stats']['rate']['median'] for c in cell_by(s, 'a')), [100.0, 300.0])

    def test_topup_picks_only_noisy_cells(self):
        rows = [row('steady', result(100.0)), row('steady', result(101.0)),
                row('noisy', result(100.0)), row('noisy', result(120.0)),
                row('skip', result(None, 'skipped', 'single-llc'))]
        self.assertEqual(sweep.select_topup(rows, 0.03, 0.06), {'noisy'})

    def test_mixed_hosts_scopes_and_grouping_refused(self):
        restricted = dict(TOPO, os_scope=dict(source='processLaunch', allowed='0-7,16-23'))
        fallback = dict(TOPO, grouping_source='l2Fallback')
        for bad in (row('a', result(1.0), model='AMD Ryzen 5 5500'),
                    row('a', result(1.0, topology=restricted)),
                    row('a', result(1.0, topology=fallback))):
            with self.assertRaises(sweep.MixedRunsError):
                sweep.summarize([row('a', result(1.0)), bad], [], {})

    def test_sensitive_rules(self):
        rows = []
        # Noisy cell, unresolved k step, MHz difference, wide sub-windows.
        for r in (100.0, 130.0):
            rows.append(row('k0', result(r, mhz=4700.0), k=0))
        for r in (110.0, 115.0):
            rows.append(row('k1', result(r, mhz=4400.0, sub=[50.0, 100.0, 150.0]), k=1))
        # Mirror disagreeing with main by more than 5%.
        rows.append(row('m1', result(150.0), sweep_='mirror', k=1, arm='mirror'))
        rows.append(row('drift', result(1.0, topology=dict(TOPO, scope_drift=True)), k=5))
        s = sweep.summarize(rows, [], {})
        text = '\n'.join(s['sensitive'])
        self.assertIn('noisy: k0', text)
        self.assertIn('not resolved: k1 vs k=0', text)
        self.assertIn('MHz differs', text)
        self.assertIn('sub-window spread > 10%: k1', text)
        self.assertIn('mirror vs main disagree', text)
        self.assertIn('scope drift: drift', text)
        self.assertEqual(s['scope_drift_runs'], 1)

    def test_marginal_is_per_added_thread(self):
        rows = [row('c0', result(100.0), k=0), row('c1', result(90.0), k=1),
                row('c3', result(72.9), k=3)]
        steps = {x['k']: x['marginal'] for x in sweep.summarize(rows, [], {})['derived']['sweeps']}
        self.assertAlmostEqual(steps[1], -0.10)
        self.assertAlmostEqual(steps[3], -0.10)

    def test_combined_prediction(self):
        rows = [row('c0', result(100.0), k=0), row('c2', result(80.0), k=2),
                row('p2', result(90.0), sweep_='S_P', k=2, role='producer')]
        comb = row('d', result(70.0), sweep_='combined', k=2, role='both')
        comb['labels']['kp'] = 2
        s = sweep.summarize(rows + [comb], [], {})
        [d] = s['derived']['combined']
        self.assertAlmostEqual(d['predicted'], 100.0 * 0.8 * 0.9)
        self.assertEqual(d['measured'], 70.0)


class RenderTests(unittest.TestCase):
    def test_sections_and_single_llc_header(self):
        rows = [row('k0', result(100.0), k=0),
                row('k1', result(None, 'skipped', 'single-llc'), k=1)]
        calib = [dict(kind='pingpong-class', status='ok', **{'class': 'intra-llc'},
                      oneway_ns_over_pair_medians=dict(median=20.0, min=19.0, max=21.0, stddev=1.0, n=3)),
                 dict(kind='pingpong-class', status='skipped', reason='single-llc', **{'class': 'cross-llc'})]
        md = sweep.render_markdown(sweep.summarize(rows, calib, dict(host_note='PBO off')))
        for n in range(11):
            self.assertIn(f'## {n}. ', md)
        self.assertIn('single-LLC host: off-CCD sweeps not measured (single-llc)', md)
        self.assertIn('| k off-C | total C | SqCs | agg Mitems/s', md)
        self.assertIn('PBO off', md)
        self.assertIn('cross-llc (single-llc)', md)

    def test_single_llc_scope_is_never_measured_off_ccd(self):
        # Either one-L3 restriction: the mirror cells are skipped (no mirror
        # group), so a k=0 cell cannot stand in for an off-CCD measurement.
        for topo in (FIRST_L3_ONLY, SECOND_L3_ONLY):
            mirror = sweep.mirror_group(topo)
            rows = [row('main-k0', result(100.0, topology=dict(topo, crosses_llc=False)), k=0),
                    row('main-k1', result(None, 'skipped', 'single-llc', topology=topo), k=1)]
            for c in sweep.suite_cells('ctl-mirror', mirror):
                rows.append(row(c['id'], result(None, 'skipped', 'single-llc', topology=topo),
                                sweep_='mirror', k=c['labels']['k'], arm='mirror'))
            s = sweep.summarize(rows, [], dict(topology=topo))
            self.assertFalse(s['off_ccd_measured'])
            self.assertEqual(s['off_ccd_skip_reasons'], ['single-llc'])
            self.assertIn('single-LLC host: off-CCD sweeps not measured',
                          sweep.render_markdown(s))

    def test_home_only_mirror_run_does_not_count_as_off_ccd(self):
        # The pre-fix runner picked the only eligible group as the "mirror" and
        # recorded its k=0 cells as ok home-only runs.
        topo = dict(SECOND_L3_ONLY, crosses_llc=False)
        rows = [row('mirror-k0', result(100.0, topology=topo), sweep_='mirror', k=0, arm='mirror'),
                row('mirror-k1', result(None, 'skipped', 'single-llc', topology=topo),
                    sweep_='mirror', k=1, arm='mirror')]
        s = sweep.summarize(rows, [], dict(topology=topo))
        self.assertFalse(s['off_ccd_measured'])
        self.assertIn('single-LLC host', sweep.render_markdown(s))

    def test_two_llc_off_runs_are_measured(self):
        rows = [row('k0', result(100.0, topology=dict(TOPO, crosses_llc=False)), k=0),
                row('k1', result(90.0), k=1)]
        self.assertTrue(sweep.summarize(rows, [], dict(topology=TOPO))['off_ccd_measured'])

    def test_invalid_pingpong_class_is_not_a_measurement(self):
        calib = [dict(kind='pingpong-class', status='invalid', reason='no-valid-pairs',
                      **{'class': 'intra-llc'}, pairs=3, oneway_ns_over_pair_medians=dict(n=0)),
                 dict(kind='pingpong-class', status='ok', **{'class': 'cross-llc'},
                      oneway_ns_over_pair_medians=dict(median=60.0, min=59.0, max=61.0, stddev=1.0, n=3))]
        cal = sweep.summarize_calib(calib)
        self.assertEqual(sorted(cal['pingpong']), ['cross-llc'])
        self.assertEqual(cal['invalid'], ['no-valid-pairs'])
        self.assertNotIn('cross_intra_ratio', cal)

    def test_host_note_in_metadata(self):
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(sweep, 'try_cmd', return_value=(1, '')):
            meta = sweep.host_metadata(d, 5, 'BIOS 1.2, PBO off, CPPC on', None)
            self.assertIsNone(sweep.host_metadata(d, 5, None, None)['host_note'])
        self.assertEqual(meta['host_note'], 'BIOS 1.2, PBO off, CPPC on')
        self.assertEqual(meta['seed'], 5)

    def test_perf_probe_picks_event_names(self):
        listing = ('l3_cache:\n  l3_read_miss_latency\n  ls_any_fills_from_sys.ext_cache_remote\n'
                   '  [Kernel PMU event]\n')
        calls = []
        def fake(cmd, timeout=20):
            calls.append(cmd)
            if cmd[:2] == ['perf', 'list']:
                return 0, listing
            return 0, 'perf version 1' if cmd[1] == '--version' else '1,,x,1,100,,'
        with mock.patch.object(sweep, 'try_cmd', side_effect=fake):
            probe = sweep.probe_perf()
        self.assertEqual(probe['event'], 'ls_any_fills_from_sys.ext_cache_remote')
        self.assertNotIn('l3_cache:', probe['events'])
        self.assertEqual(probe['stat'], 'available')
        self.assertFalse(any('sysctl' in ' '.join(c) or 'perf_event_paranoid' in ' '.join(c)
                             for c in calls))

    def test_main_rejects_unknown_suite(self):
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            sweep.main(['--suite', 'nope', '--out', '/tmp/x'])


if __name__ == '__main__':
    unittest.main()
