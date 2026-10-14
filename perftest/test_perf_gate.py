#!/usr/bin/env python3
"""Unit tests for perf_gate.py decisions and exit semantics.

Run: python3 -m unittest perftest/test_perf_gate.py
Git, the compiler, the benchmark and CPU accounting are all faked, so no
build or timing work happens and the tests run in well under a second each.
"""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('perf_gate', HERE / 'perf_gate.py')
perf_gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(perf_gate)

ALL_MODES = ('actor,wave,inbox,private,private-remote,tree,tree-wait,tree-cont,'
             'tree-pipelined,dtor,subset,subset-whole,subset-whole-unreserved,'
             'tree-cancel,private-tracked').split(',')
BASE_SHA = 'a' * 40
CAND_SHA = 'b' * 40
REFS = {'BASE': BASE_SHA, BASE_SHA: BASE_SHA, 'CAND': CAND_SHA, CAND_SHA: CAND_SHA,
        'ALIAS': BASE_SHA, 'HEAD': BASE_SHA}
ARCHIVE_REL = 'tests/actors/.build/mimalloc-v3.5.3/libmimalloc.a'
BASE_RATE = 1000.0


def source_for(modes, extra=''):
    return '\n'.join('if (args[1] == "%s") {}' % m for m in modes) + '\n' + extra


def rel(ratio):
    """Rate function: candidate runs at `ratio` of the base rate."""
    return lambda version, mode, consumers, percent, n: dict(
        rate=BASE_RATE * (ratio if version == 'candidate' else 1))


class Result:
    pass


class GateCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = pathlib.Path(tmp.name)
        self.repo = self.tmp / 'repo'
        (self.repo / pathlib.Path(ARCHIVE_REL).parent).mkdir(parents=True)
        (self.repo / ARCHIVE_REL).write_bytes(b'default-archive')
        self.n_out = 0

    def out(self):
        self.n_out += 1
        return self.tmp / ('out%d' % self.n_out)

    def gate(self, args, rate=None, sources=None, bench_sources=None, build=None,
             load=0.1, fail_build=False):
        """Run perf_gate.main() with everything external faked.

        rate(version, mode, consumers, percent, n) -> dict overriding the fake
        row for the n-th (0-based) run of that version in that cell.
        sources: revision sha -> actor_churn.d text; bench_sources: ref -> text.
        build(label, revision, serial) -> bytes written as the binary.
        """
        r = Result()
        r.runs, r.builds, r.adds, r.removes, r.scratch = [], [], [], [], []
        default_source = source_for(ALL_MODES)
        sources = sources or {}
        bench_sources = bench_sources or {}
        rate = rate or rel(1.0)
        counters = {}
        build = build or (lambda label, revision, serial: b'binary-' + revision.encode())
        out = self.out()
        r.out = out

        def fake_command(cmd, **kw):
            if cmd[:2] == ['git', 'rev-parse']:
                ref = cmd[2][:-len('^{commit}')]
                if ref not in REFS:
                    raise subprocess.CalledProcessError(128, cmd)
                return REFS[ref] + '\n'
            if cmd[:3] == ['git', 'worktree', 'add']:
                scratch = pathlib.Path(cmd[4])
                (scratch / 'perftest').mkdir(parents=True)
                (scratch / 'perftest/actor_churn.d').write_text(
                    sources.get(cmd[5], default_source))
                r.adds.append(scratch)
                return ''
            if cmd[:2] == ['git', 'show']:
                ref = cmd[2].split(':', 1)[0]
                return bench_sources.get(ref, default_source)
            if cmd[0] == 'ldc2':
                return 'LDC fake'
            if cmd[0] == 'readelf':
                return 'ELF fake'
            raise AssertionError('unexpected command ' + repr(cmd))

        def fake_run(cmd, **kw):
            if cmd[:3] == ['git', 'worktree', 'remove']:
                shutil.rmtree(cmd[4])
                r.removes.append(pathlib.Path(cmd[4]))
                return mock.Mock(returncode=0)
            assert cmd[0] == 'ldc2', cmd
            if fail_build:
                raise subprocess.CalledProcessError(1, cmd)
            binary = pathlib.Path(next(x for x in cmd if x.startswith('-of='))[4:])
            label = binary.name.split('-')[0]
            r.builds.append(dict(label=label, cmd=cmd, cwd=kw['cwd']))
            serial = len(r.builds) - 1
            rev = REFS[self.refs[label]]
            binary.write_bytes(build(label, rev, serial))
            return mock.Mock(returncode=0)

        def fake_runone(cmd, cpus, seconds, child_affinity=None, env=None):
            version = pathlib.Path(cmd[0]).name.split('-')[0]
            mode, consumers = cmd[1], int(cmd[4])
            percent = int(cmd[11]) if len(cmd) > 11 else 100
            key = (version, mode, consumers, percent)
            n = counters.get(key, 0)
            counters[key] = n + 1
            row = dict(command=cmd, returncode=0, stdout='', stderr='', contaminated=False,
                       reasons=[], rate=BASE_RATE, fields={}, valid=True)
            row.update(rate(version, mode, consumers, percent, n))
            if row['contaminated'] and not row['reasons']:
                row['reasons'] = ['fake contamination']
            r.runs.append(dict(version=version, mode=mode, consumers=consumers,
                               percent=percent, cmd=cmd, n=n, env=env))
            return row

        cpuinfo = [dict(processor=str(i), **{'physical id': '0', 'core id': str(i),
                   'cpu MHz': '1000', 'model name': 'fake'}) for i in range(8)]
        # The refs given on the command line decide what each label builds.
        self.refs = dict(base=args[args.index('--base') + 1])
        self.refs['candidate'] = (args[args.index('--candidate') + 1]
                                  if '--candidate' in args else self.refs['base'])
        argv = ['perf_gate.py', '--repo', str(self.repo), '--output', str(out)] + list(args)
        patches = [
            mock.patch.object(sys, 'argv', argv),
            mock.patch.object(perf_gate, 'require_linux'),
            mock.patch.object(perf_gate, 'command', fake_command),
            mock.patch.object(perf_gate, 'cpuinfo', lambda: cpuinfo),
            mock.patch.object(perf_gate, 'runone', fake_runone),
            mock.patch.object(perf_gate.subprocess, 'run', fake_run),
            mock.patch.object(perf_gate.os, 'sched_getaffinity', lambda pid: set(range(8)), create=True),
            mock.patch.object(perf_gate.os, 'sched_setaffinity', lambda pid, cpus: None, create=True),
            mock.patch.object(perf_gate.os, 'getloadavg', lambda: (load, load, load), create=True),
        ]
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            r.status = perf_gate.main()
        return r

    def usage_error(self, args):
        """Run main() expecting an argparse rejection; returns its exit code."""
        with self.assertRaises(SystemExit) as cm, \
                contextlib.redirect_stderr(io.StringIO()):
            self.gate(args)
        return cm.exception.code

    @staticmethod
    def load(r, name):
        return json.loads((r.out / name).read_text())

    def decisions(self, r):
        return {(x['mode'], x['consumers'], x['percent']): x['decision']
                for x in self.load(r, 'summary.json')}

    def assert_exit_recorded(self, r, status):
        self.assertEqual(r.status, status)
        self.assertEqual(self.load(r, 'gate.json')['exit_status'], status)

    def assert_scratch_cleaned(self, r):
        self.assertEqual(len(r.adds), len(r.removes))
        for scratch in r.adds:
            self.assertFalse(scratch.exists())


FAST = ['--base', 'BASE', '--candidate', 'CAND', '--modes', 'actor', '--consumers', '0']


class DecisionTests(GateCase):
    def test_equal_performance_passes(self):
        r = self.gate(FAST)
        self.assert_exit_recorded(r, 0)
        self.assertEqual(self.decisions(r), {('actor', 0, 100): 'pass'})
        self.assert_scratch_cleaned(r)

    def test_default_threshold_is_minus_three_percent(self):
        self.assertEqual(self.gate(FAST, rate=rel(0.971)).status, 0)
        r = self.gate(FAST, rate=rel(0.969))
        self.assert_exit_recorded(r, 1)
        self.assertEqual(self.decisions(r), {('actor', 0, 100): 'regression'})

    def test_interval_straddling_threshold_is_inconclusive_not_pass(self):
        ratios = [0.90, 1.10, 0.95, 1.05, 1.00]
        r = self.gate(FAST, rate=lambda v, m, c, p, n: dict(
            rate=BASE_RATE * (ratios[n] if v == 'candidate' else 1)))
        self.assert_exit_recorded(r, 2)
        self.assertEqual(self.decisions(r), {('actor', 0, 100): 'inconclusive'})

    def test_one_bad_pair_blocks_pass(self):
        ratios = [1.0, 1.0, 1.0, 1.0, 0.90]
        r = self.gate(FAST, rate=lambda v, m, c, p, n: dict(
            rate=BASE_RATE * (ratios[n] if v == 'candidate' else 1)))
        self.assertEqual(r.status, 2)

    def test_regression_in_any_cell_fails_the_gate(self):
        r = self.gate(['--base', 'BASE', '--candidate', 'CAND', '--modes', 'actor,wave',
                       '--consumers', '0'],
                      rate=lambda v, m, c, p, n: dict(
                          rate=BASE_RATE * (0.8 if v == 'candidate' and m == 'wave' else 1)))
        self.assert_exit_recorded(r, 1)
        self.assertEqual(self.decisions(r), {('actor', 0, 100): 'pass',
                                             ('wave', 0, 100): 'regression'})

    def test_custom_threshold_is_honoured(self):
        r = self.gate(FAST + ['--threshold', '-0.20'], rate=rel(0.9))
        self.assertEqual(r.status, 0)

    def test_pair_order_alternates_between_accepted_pairs(self):
        r = self.gate(FAST)
        order = [x['version'] for x in r.runs]
        self.assertEqual(order, ['base', 'candidate', 'candidate', 'base', 'base',
                                 'candidate', 'candidate', 'base', 'base', 'candidate'])
        counts = self.load(r, 'summary.json')[0]['kept_order_counts']
        self.assertEqual(counts, {'base': 3, 'candidate': 2})

    def test_default_coverage_and_parameters(self):
        r = self.gate(['--base', 'BASE', '--candidate', 'CAND'],
                      rate=lambda v, m, c, p, n: dict(fields=dict(dispatched_fraction='0.5')))
        self.assert_exit_recorded(r, 0)
        cells = {(x['mode'], x['consumers'], x['percent'])
                 for x in self.load(r, 'summary.json')}
        expected = set()
        for consumers in (0, 6):
            for mode in ALL_MODES:
                for percent in ((1, 10, 100) if mode == 'subset' else (100,)):
                    expected.add((mode, consumers, percent))
        self.assertEqual(cells, expected)
        for cell in self.load(r, 'summary.json'):
            self.assertEqual(cell['quiet_n'], 5)
        cmd = r.runs[0]['cmd']
        self.assertEqual([float(x) for x in cmd[2:7]], [16384, 3, 0, 256, 5])
        self.assertEqual(cmd[10], '16384')
        self.assertEqual(self.load(r, 'metadata.json')['arguments']['repeats'], 5)
        self.assertEqual(self.load(r, 'metadata.json')['arguments']['max_attempts'], 10)

    def test_subset_modes_pass_percent_to_the_benchmark(self):
        r = self.gate(['--base', 'BASE', '--candidate', 'CAND', '--modes', 'subset',
                       '--consumers', '0'])
        self.assertEqual({x['cmd'][11] for x in r.runs}, {'1', '10', '100'})


class UsageValidationTests(GateCase):
    def test_candidate_required_without_control(self):
        self.assertEqual(self.usage_error(['--base', 'BASE']), 2)

    def test_too_few_repeats_or_seconds_rejected(self):
        self.assertEqual(self.usage_error(FAST + ['--repeats', '4']), 2)
        self.assertEqual(self.usage_error(FAST + ['--seconds', '2.5']), 2)

    def test_max_attempts_below_repeats_rejected(self):
        self.assertEqual(self.usage_error(FAST + ['--repeats', '6', '--max-attempts', '5']), 2)

    def test_existing_output_directory_rejected(self):
        existing = self.tmp / 'taken'
        existing.mkdir()
        (existing / 'precious').write_text('evidence')
        with self.assertRaises(SystemExit) as cm, contextlib.redirect_stderr(io.StringIO()), \
                mock.patch.object(sys, 'argv', ['perf_gate.py', '--repo', str(self.repo),
                                                '--output', str(existing)] + FAST), \
                mock.patch.object(perf_gate, 'require_linux'), \
                mock.patch.object(perf_gate.os, 'sched_getaffinity', lambda pid: set(range(8)), create=True), \
                mock.patch.object(perf_gate, 'cpuinfo', lambda: [
                    dict(processor=str(i), **{'physical id': '0', 'core id': str(i)})
                    for i in range(8)]):
            perf_gate.main()
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual((existing / 'precious').read_text(), 'evidence')
        self.assertEqual([p.name for p in existing.iterdir()], ['precious'])


class HostConditionTests(GateCase):
    def test_busy_host_is_inconclusive_before_any_build(self):
        r = self.gate(FAST, load=5.0)
        self.assert_exit_recorded(r, 2)
        self.assertEqual(r.builds, [])
        self.assertEqual(r.runs, [])

    def test_invalid_benchmark_output_is_error_not_pass(self):
        r = self.gate(FAST, rate=lambda v, m, c, p, n: dict(valid=False))
        self.assert_exit_recorded(r, 2)
        self.assertTrue((r.out / 'failure.json').exists())
        self.assertEqual(len(r.runs), 1)

    def test_build_failure_still_removes_scratch_worktree(self):
        with self.assertRaises(subprocess.CalledProcessError):
            self.gate(FAST, fail_build=True)
        # main() raises; the script wrapper turns this into exit 2 (see below).
        scratch = list(pathlib.Path(tempfile.gettempdir()).glob('perf-gate-base-*'))
        self.assertEqual(scratch, [])


class ContaminationTests(GateCase):
    def test_contaminated_pairs_are_discarded_not_counted(self):
        # The first two attempts look like a huge regression but are contaminated.
        def rate(v, m, c, p, n):
            if n < 2:
                return dict(rate=BASE_RATE * (0.5 if v == 'candidate' else 1), contaminated=True)
            return dict(rate=BASE_RATE)
        r = self.gate(FAST, rate=rate)
        self.assert_exit_recorded(r, 0)
        cell = self.load(r, 'summary.json')[0]
        self.assertEqual(cell['quiet_n'], 5)
        self.assertEqual(cell['contaminated'], 4)  # runs, two per discarded pair
        self.assertEqual(cell['decision'], 'pass')
        rows = [json.loads(x) for x in (r.out / 'runs.jsonl').read_text().splitlines()]
        self.assertEqual(len(rows), 14)  # evidence keeps contaminated attempts

    def test_contamination_cannot_hide_a_regression(self):
        # Contaminated attempts are wonderful; clean attempts regress.
        def rate(v, m, c, p, n):
            if n < 3:
                return dict(rate=BASE_RATE * (2 if v == 'candidate' else 1), contaminated=True)
            return dict(rate=BASE_RATE * (0.8 if v == 'candidate' else 1))
        r = self.gate(FAST, rate=rate)
        self.assert_exit_recorded(r, 1)

    def test_one_contaminated_run_discards_the_whole_pair(self):
        r = self.gate(FAST, rate=lambda v, m, c, p, n: dict(
            contaminated=(v == 'candidate' and n == 0)))
        cell = self.load(r, 'summary.json')[0]
        self.assertEqual(cell['quiet_n'], 5)
        self.assertEqual(len([x for x in r.runs if x['version'] == 'base']), 6)

    def test_all_contaminated_is_insufficient_not_pass(self):
        r = self.gate(FAST, rate=lambda v, m, c, p, n: dict(contaminated=True))
        self.assert_exit_recorded(r, 2)
        cell = self.load(r, 'summary.json')[0]
        self.assertEqual(cell['quiet_n'], 0)
        self.assertEqual(cell['decision'], 'insufficient')
        self.assertEqual(len(r.runs), 20)  # default attempt limit 10 pairs

    def test_too_few_clean_pairs_is_insufficient_even_if_all_good(self):
        r = self.gate(FAST, rate=lambda v, m, c, p, n: dict(contaminated=n >= 4))
        self.assert_exit_recorded(r, 2)
        cell = self.load(r, 'summary.json')[0]
        self.assertEqual(cell['quiet_n'], 4)
        self.assertEqual(cell['decision'], 'insufficient')

    def test_insufficient_pairs_do_not_mask_other_cells_regression(self):
        def rate(v, m, c, p, n):
            if m == 'actor':
                return dict(contaminated=True)
            return dict(rate=BASE_RATE * (0.5 if v == 'candidate' else 1))
        r = self.gate(['--base', 'BASE', '--candidate', 'CAND', '--modes', 'actor,wave',
                       '--consumers', '0'], rate=rate)
        self.assert_exit_recorded(r, 1)
        self.assertEqual(self.decisions(r), {('actor', 0, 100): 'insufficient',
                                             ('wave', 0, 100): 'regression'})


class BoundaryTests(GateCase):
    def test_boundary_defaults_to_twenty_pairs_and_double_attempts(self):
        r = self.gate(FAST + ['--boundary'])
        self.assert_exit_recorded(r, 0)
        cell = self.load(r, 'summary.json')[0]
        self.assertEqual(cell['quiet_n'], 20)
        self.assertEqual(len(r.runs), 40)
        args = self.load(r, 'metadata.json')['arguments']
        self.assertEqual((args['repeats'], args['max_attempts']), (20, 40))

    def test_boundary_rejects_fewer_than_twenty_repeats(self):
        for repeats in ('5', '19'):
            self.assertEqual(self.usage_error(FAST + ['--boundary', '--repeats', repeats]), 2)
        self.assertEqual(self.n_out, 2)
        self.assertFalse(any(p.exists() for p in self.tmp.glob('out*')))  # no evidence dir

    def test_boundary_accepts_more_than_twenty(self):
        r = self.gate(FAST + ['--boundary', '--repeats', '25'])
        self.assertEqual(self.load(r, 'summary.json')[0]['quiet_n'], 25)
        self.assertEqual(self.load(r, 'metadata.json')['arguments']['max_attempts'], 50)

    def test_boundary_with_nineteen_clean_pairs_is_insufficient(self):
        r = self.gate(FAST + ['--boundary'], rate=lambda v, m, c, p, n: dict(
            contaminated=n >= 19))
        self.assert_exit_recorded(r, 2)
        cell = self.load(r, 'summary.json')[0]
        self.assertEqual((cell['quiet_n'], cell['decision']), (19, 'insufficient'))

    def test_boundary_with_twenty_clean_pairs_can_pass(self):
        r = self.gate(FAST + ['--boundary'], rate=lambda v, m, c, p, n: dict(
            contaminated=n >= 20))
        self.assert_exit_recorded(r, 0)

    def test_boundary_regression_still_exits_one(self):
        self.assertEqual(self.gate(FAST + ['--boundary'], rate=rel(0.9)).status, 1)


class InterpretationTests(GateCase):
    """gate.json must name the threshold screen actually in force."""

    def test_default_five_repeats_wording_is_all_pairs(self):
        r = self.gate(FAST)
        self.assert_exit_recorded(r, 0)
        interpretation = self.load(r, 'gate.json')['interpretation']
        self.assertIn('Conservative all-pairs threshold screen at n=5', interpretation)
        self.assertNotIn('Paired-median', interpretation)

    def test_boundary_wording_is_paired_median_and_permits_one_outlier_pair(self):
        # 19 equal pairs plus a single -4% pair: that pair sits below the
        # threshold, but the paired-median interval stays above it, so
        # --boundary still passes overall.
        r = self.gate(FAST + ['--boundary'], rate=lambda v, m, c, p, n: dict(
            rate=BASE_RATE * (0.96 if v == 'candidate' and n == 19 else 1)))
        self.assert_exit_recorded(r, 0)
        cell = self.load(r, 'summary.json')[0]
        self.assertEqual(cell['decision'], 'pass')
        worst = min(cell['paired_ratios'])
        self.assertAlmostEqual(worst, -0.04)
        self.assertLess(worst, -0.03)  # one pair below the threshold, gate passes anyway
        interpretation = self.load(r, 'gate.json')['interpretation']
        self.assertIn('Paired-median interval threshold screen; '
                      'individual pairs may fall below the threshold and still pass',
                      interpretation)
        self.assertNotIn('all-pairs', interpretation)

    def test_repeats_twenty_without_boundary_uses_paired_median_wording(self):
        r = self.gate(FAST + ['--repeats', '20'])
        self.assert_exit_recorded(r, 0)
        interpretation = self.load(r, 'gate.json')['interpretation']
        self.assertIn('Paired-median interval threshold screen', interpretation)
        self.assertNotIn('all-pairs', interpretation)


class ArchiveTests(GateCase):
    def build_archives(self, r):
        return {next(pathlib.Path(x) for x in b['cmd'] if x.endswith('.a'))
                for b in r.builds}

    def test_default_archive_is_repo_relative(self):
        r = self.gate(FAST)
        expected = (self.repo / ARCHIVE_REL).resolve()
        self.assertEqual(self.build_archives(r), {expected})
        meta = self.load(r, 'metadata.json')
        self.assertEqual(meta['archive_sha256'], hashlib.sha256(b'default-archive').hexdigest())

    def test_archive_override_replaces_default(self):
        other = self.tmp / 'elsewhere' / 'libmimalloc.a'
        other.parent.mkdir()
        other.write_bytes(b'override-archive')
        r = self.gate(FAST + ['--archive', str(other)])
        self.assertEqual(self.build_archives(r), {other.resolve()})
        meta = self.load(r, 'metadata.json')
        self.assertEqual(meta['archive_sha256'], hashlib.sha256(b'override-archive').hexdigest())

    def test_default_archive_follows_selected_repo(self):
        other_repo = self.tmp / 'other-repo'
        archive = other_repo / ARCHIVE_REL
        archive.parent.mkdir(parents=True)
        archive.write_bytes(b'other-repo-archive')
        self.repo = other_repo
        r = self.gate(FAST)
        self.assertEqual(self.build_archives(r), {archive.resolve()})


class ControlTests(GateCase):
    CONTROL = ['--base', 'BASE', '--control', '--modes', 'actor', '--consumers', '0']

    def test_control_builds_the_base_ref_twice_and_passes_when_identical(self):
        r = self.gate(self.CONTROL)
        self.assert_exit_recorded(r, 0)
        meta = self.load(r, 'metadata.json')
        self.assertEqual(meta['builds']['base']['revision'], BASE_SHA)
        self.assertEqual(meta['builds']['candidate']['revision'], BASE_SHA)
        self.assertEqual(meta['builds']['base']['sha256'], meta['builds']['candidate']['sha256'])
        self.assertEqual([b['label'] for b in r.builds], ['base', 'candidate'])
        self.assert_scratch_cleaned(r)

    def test_control_rejects_nonidentical_binaries(self):
        r = self.gate(self.CONTROL, build=lambda label, rev, serial: b'build-%d' % serial)
        self.assert_exit_recorded(r, 2)
        self.assertIn('byte-identical', self.load(r, 'gate.json')['reason'])
        self.assertEqual(self.load(r, 'summary.json'), [])
        self.assertEqual(r.runs, [])  # no timing run may follow a failed control
        self.assert_scratch_cleaned(r)

    def test_control_rejects_a_different_candidate_ref(self):
        self.assertEqual(self.usage_error(self.CONTROL + ['--candidate', 'CAND']), 2)
        self.assertFalse(any(p.exists() for p in self.tmp.glob('out*')))

    def test_control_accepts_candidate_naming_same_commit(self):
        # Compared by resolved commit, not by spelling.
        r = self.gate(self.CONTROL + ['--candidate', 'ALIAS'])
        self.assert_exit_recorded(r, 0)

    def test_control_never_substitutes_a_candidate_build(self):
        r = self.gate(self.CONTROL)
        meta = self.load(r, 'metadata.json')
        self.assertNotEqual(meta['builds']['base']['revision'], CAND_SHA)
        self.assertEqual(self.load(r, 'summary.json')[0]['decision'], 'pass')

    def test_control_with_regressing_noise_is_still_judged(self):
        r = self.gate(self.CONTROL, rate=rel(0.8))
        self.assert_exit_recorded(r, 1)

    def test_control_and_boundary_combine(self):
        r = self.gate(self.CONTROL + ['--boundary'])
        self.assert_exit_recorded(r, 0)
        self.assertEqual(self.load(r, 'summary.json')[0]['quiet_n'], 20)
        self.assertEqual(len(r.builds), 2)

    def test_control_with_boundary_still_rejects_short_runs(self):
        self.assertEqual(self.usage_error(self.CONTROL + ['--boundary', '--repeats', '10']), 2)


class EnvironmentTests(GateCase):
    def test_side_environment_reaches_only_that_side(self):
        r = self.gate(['--base', 'BASE', '--control', '--modes', 'actor', '--consumers', '0',
                       '--candidate-env', 'MIMALLOC_PAGE_FULL_RETAIN=-1',
                       '--candidate-env', 'X=a=b', '--base-env', 'Y='])
        self.assert_exit_recorded(r, 0)
        for run in r.runs:
            expected = ({'MIMALLOC_PAGE_FULL_RETAIN': '-1', 'X': 'a=b'}
                        if run['version'] == 'candidate' else {'Y': ''})
            self.assertEqual(run['env'], expected)
        args = self.load(r, 'metadata.json')['arguments']
        self.assertEqual(args['candidate_env'], ['MIMALLOC_PAGE_FULL_RETAIN=-1', 'X=a=b'])

    def test_default_environment_is_empty_on_both_sides(self):
        r = self.gate(FAST)
        self.assertTrue(all(run['env'] == {} for run in r.runs))

    def test_malformed_or_page_mode_environment_rejected(self):
        for bad in ('NOEQUALS', '=1', '1X=2', 'ANTFARM_HUGE_PAGES=1'):
            self.assertEqual(self.usage_error(FAST + ['--candidate-env', bad]), 2)

    def test_peak_rss_medians_are_summarised_per_side(self):
        r = self.gate(FAST, rate=lambda v, m, c, p, n: dict(
            fields=dict(peak_rss_kib=str(100 + n if v == 'base' else 200 + n))))
        cell = self.load(r, 'summary.json')[0]
        self.assertEqual(cell['base_peak_rss_kib_median'], 102)
        self.assertEqual(cell['candidate_peak_rss_kib_median'], 202)


class BenchmarkSourceTests(GateCase):
    def test_differing_benchmark_sources_are_inconclusive(self):
        sources = {BASE_SHA: source_for(ALL_MODES), CAND_SHA: source_for(ALL_MODES, '// changed')}
        r = self.gate(FAST, sources=sources)
        self.assert_exit_recorded(r, 2)
        self.assertIn('benchmark sources differ', self.load(r, 'gate.json')['reason'])
        self.assertEqual(r.runs, [])

    def test_benchmark_ref_supplies_a_common_source(self):
        sources = {BASE_SHA: source_for(ALL_MODES), CAND_SHA: source_for(ALL_MODES, '// changed')}
        r = self.gate(FAST + ['--benchmark-ref', 'BASE'], sources=sources,
                      bench_sources={'BASE': source_for(ALL_MODES)})
        self.assert_exit_recorded(r, 0)
        meta = self.load(r, 'metadata.json')['builds']
        self.assertEqual(meta['base']['benchmark_sha256'], meta['candidate']['benchmark_sha256'])

    def test_allow_benchmark_change_is_explicit_opt_in(self):
        sources = {BASE_SHA: source_for(ALL_MODES), CAND_SHA: source_for(ALL_MODES, '// changed')}
        r = self.gate(FAST + ['--allow-benchmark-change'], sources=sources)
        self.assert_exit_recorded(r, 0)


class UnavailableModeTests(GateCase):
    def test_unavailable_mode_is_never_a_pass(self):
        sources = {BASE_SHA: source_for(['actor']), CAND_SHA: source_for(['actor'])}
        r = self.gate(['--base', 'BASE', '--candidate', 'CAND', '--modes', 'actor,wave',
                       '--consumers', '0'], sources=sources)
        self.assert_exit_recorded(r, 2)
        self.assertEqual(self.load(r, 'unavailable.json'), {'wave': ['base', 'candidate']})
        self.assertEqual(self.decisions(r), {('actor', 0, 100): 'pass'})
        self.assertEqual({x['mode'] for x in r.runs}, {'actor'})  # never substituted

    def test_mode_missing_from_only_one_side_is_reported(self):
        sources = {BASE_SHA: source_for(ALL_MODES), CAND_SHA: source_for(['actor'])}
        r = self.gate(['--base', 'BASE', '--candidate', 'CAND', '--modes', 'actor,wave',
                       '--consumers', '0', '--allow-benchmark-change'], sources=sources)
        self.assert_exit_recorded(r, 2)
        self.assertEqual(self.load(r, 'unavailable.json'), {'wave': ['candidate']})

    def test_only_unavailable_modes_requested_is_inconclusive(self):
        sources = {BASE_SHA: source_for(['actor']), CAND_SHA: source_for(['actor'])}
        r = self.gate(['--base', 'BASE', '--candidate', 'CAND', '--modes', 'tree-cancel',
                       '--consumers', '0'], sources=sources)
        self.assert_exit_recorded(r, 2)
        self.assertEqual(self.load(r, 'summary.json'), [])
        self.assertEqual(r.runs, [])

    def test_regression_takes_precedence_over_unavailable_mode(self):
        sources = {BASE_SHA: source_for(['actor']), CAND_SHA: source_for(['actor'])}
        r = self.gate(['--base', 'BASE', '--candidate', 'CAND', '--modes', 'actor,wave',
                       '--consumers', '0'], sources=sources, rate=rel(0.8))
        self.assert_exit_recorded(r, 1)
        self.assertEqual(self.load(r, 'unavailable.json'), {'wave': ['base', 'candidate']})


class TreeCancelTests(GateCase):
    ARGS = ['--base', 'BASE', '--candidate', 'CAND', '--modes', 'tree-cancel', '--consumers', '0']

    @staticmethod
    def fractions(base, cand, **extra):
        def rate(v, m, c, p, n):
            frac = base if v == 'base' else cand
            return dict(fields={} if frac is None else dict(dispatched_fraction=str(frac)), **extra)
        return rate

    def test_equal_work_can_pass(self):
        r = self.gate(self.ARGS, rate=self.fractions(0.5, 0.5))
        self.assert_exit_recorded(r, 0)
        cell = self.load(r, 'summary.json')[0]
        self.assertEqual(cell['decision'], 'pass')
        self.assertEqual(cell['base_dispatched_fraction'], 0.5)

    def test_unequal_work_is_inconclusive_even_if_timing_improves(self):
        def rate(v, m, c, p, n):
            return dict(rate=BASE_RATE * (1.5 if v == 'candidate' else 1),
                        fields=dict(dispatched_fraction='0.5' if v == 'base' else '0.2'))
        r = self.gate(self.ARGS, rate=rate)
        self.assert_exit_recorded(r, 2)
        cell = self.load(r, 'summary.json')[0]
        self.assertEqual(cell['decision'], 'inconclusive_work')

    def test_fraction_within_default_tolerance_is_comparable(self):
        self.assertEqual(self.gate(self.ARGS, rate=self.fractions(0.50, 0.505)).status, 0)

    def test_fraction_beyond_default_tolerance_is_not_comparable(self):
        self.assertEqual(self.gate(self.ARGS, rate=self.fractions(0.50, 0.52)).status, 2)

    def test_custom_tolerance_is_honoured(self):
        r = self.gate(self.ARGS + ['--cancel-fraction-tolerance', '0.05'],
                      rate=self.fractions(0.50, 0.52))
        self.assertEqual(r.status, 0)

    def test_tolerance_out_of_range_is_rejected(self):
        self.assertEqual(self.usage_error(self.ARGS + ['--cancel-fraction-tolerance', '1.5']), 2)

    def test_missing_fraction_is_inconclusive(self):
        for base, cand in ((None, 0.5), (0.5, None), (None, None)):
            r = self.gate(self.ARGS, rate=self.fractions(base, cand))
            self.assertEqual(r.status, 2, (base, cand))
            cell = self.load(r, 'summary.json')[0]
            self.assertEqual(cell['decision'], 'inconclusive_work')

    def test_fraction_outside_unit_interval_is_inconclusive(self):
        for base, cand in ((1.5, 1.5), (0.5, 1.2)):
            r = self.gate(self.ARGS, rate=self.fractions(base, cand))
            self.assertEqual(r.status, 2, (base, cand))

    def test_regression_with_equal_work_still_fails(self):
        def rate(v, m, c, p, n):
            return dict(rate=BASE_RATE * (0.8 if v == 'candidate' else 1),
                        fields=dict(dispatched_fraction='0.5'))
        r = self.gate(self.ARGS, rate=rate)
        self.assert_exit_recorded(r, 1)

    def test_unequal_work_reports_cancel_inconclusive_next_to_other_regression(self):
        def rate(v, m, c, p, n):
            return dict(rate=BASE_RATE * (0.8 if v == 'candidate' and m == 'actor' else 1),
                        fields=dict(dispatched_fraction='0.5' if v == 'base' else '0.1'))
        r = self.gate(['--base', 'BASE', '--candidate', 'CAND', '--modes', 'actor,tree-cancel',
                       '--consumers', '0'], rate=rate)
        self.assert_exit_recorded(r, 1)
        self.assertEqual(self.decisions(r), {('actor', 0, 100): 'regression',
                                             ('tree-cancel', 0, 100): 'inconclusive_work'})


class ScriptEntryPointTests(unittest.TestCase):
    """Run the script for real; failures must exit 2, never 1 or 0."""

    @unittest.skipUnless(sys.platform == 'linux', 'requires Linux CPU affinity and /proc accounting')
    def test_internal_error_exits_two_and_writes_gate_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            repo = tmp / 'empty-repo'
            repo.mkdir()
            out = tmp / 'out'
            cpu = min(os.sched_getaffinity(0))
            proc = subprocess.run(
                [sys.executable, str(HERE / 'perf_gate.py'), '--repo', str(repo),
                 '--base', 'nonexistent', '--candidate', 'nonexistent', '--cpus', str(cpu),
                 '--consumers', '0', '--output', str(out)],
                capture_output=True, text=True, timeout=60)
            self.assertEqual(proc.returncode, 2, proc.stderr)
            self.assertIn('ERROR', proc.stderr)
            gate = json.loads((out / 'gate.json').read_text())
            self.assertEqual(gate['exit_status'], 2)
            self.assertEqual(json.loads((out / 'summary.json').read_text()), [])

    def test_usage_error_exits_two(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp) / 'never-created'
            proc = subprocess.run([sys.executable, str(HERE / 'perf_gate.py'),
                                   '--output', str(out)],
                                  capture_output=True, text=True, timeout=60)
            self.assertEqual(proc.returncode, 2)
            self.assertIn('--base', proc.stderr)
            self.assertFalse(out.exists())

    def test_help_is_available_without_linux_accounting(self):
        proc = subprocess.run([sys.executable, str(HERE / 'perf_gate.py'), '--help'],
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn('--boundary', proc.stdout)

    def test_unsupported_host_rejected_before_side_effects(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp) / 'never-created'
            with mock.patch.object(sys, 'platform', 'win32'), \
                    mock.patch.object(sys, 'argv', ['perf_gate.py', '--output', str(out)] + FAST), \
                    mock.patch.object(perf_gate, 'command') as command, \
                    mock.patch.object(perf_gate, 'cpuinfo') as cpuinfo:
                with self.assertRaises(SystemExit) as caught, \
                        contextlib.redirect_stderr(io.StringIO()) as err:
                    perf_gate.main()
                self.assertEqual(caught.exception.code, 2)
                self.assertIn('requires Linux', err.getvalue())
                command.assert_not_called()
                cpuinfo.assert_not_called()
            self.assertFalse(out.exists())

    @unittest.skipIf(sys.platform == 'linux', 'exercises the non-Linux CLI rejection')
    def test_unsupported_host_exits_two_without_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp) / 'never-created'
            proc = subprocess.run([sys.executable, str(HERE / 'perf_gate.py'),
                                   '--output', str(out)] + FAST,
                                  capture_output=True, text=True, timeout=60)
            self.assertEqual(proc.returncode, 2)
            self.assertIn('requires Linux', proc.stderr)
            self.assertNotIn('Traceback', proc.stderr)
            self.assertFalse(out.exists())

    @unittest.skipIf(sys.platform == 'linux', 'exercises the non-Linux CLI rejection')
    def test_unsupported_host_preserves_existing_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp)
            gate = out / 'gate.json'
            gate.write_bytes(b'previous evidence\n')
            proc = subprocess.run([sys.executable, str(HERE / 'perf_gate.py'),
                                   '--output', str(out)] + FAST,
                                  capture_output=True, text=True, timeout=60)
            self.assertEqual(proc.returncode, 2)
            self.assertIn('requires Linux', proc.stderr)
            self.assertEqual(gate.read_bytes(), b'previous evidence\n')
            self.assertEqual(list(out.iterdir()), [gate])


if __name__ == '__main__':
    unittest.main()
