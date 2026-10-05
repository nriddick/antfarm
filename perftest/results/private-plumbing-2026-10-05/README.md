# E2-3 plumbing gate — protocol, file map and assessment

Base: git archive of local main `370dfbc`. Candidate: branch
`feature/private-plumbing`. Binaries built with LDC 1.43.0
`-O2 -release -d-version=AntfarmMimallocV3 -i` against the main checkout's
read-only prebuilt mimalloc v3.5.3 archive
(sha256 551ee9a97e46b331736163a17cb3514b940d57b68cc82206723e0ccc0e8a1bec).
Benchmark: `perftest/actor_churn` — modes private, private-remote, tree,
tree-wait, dtor (Mactor_cycles/s) and subset at k=1% and k=100%
(waves/s); actor_churn has no tree-results mode. 16,384 actors, batch 256,
tier small, quota 16,384 words, five warmups, three-second windows,
`ANTFARM_HUGE_PAGES=0`. Profiles: consumers=0 pinned to CPU 0, and
consumers=6 pinned to CPUs 0-6. Within every repeat the 14 measurements run
base/candidate interleaved, and the order reverses on successive repeats.

## Gate verdict

**Passed on byte-identical binaries; the fresh at-tip runs were contaminated
by erratic external host CPU load.** The two complete clean runs (below)
measured base `336c751d…` against candidate `a52b2531…`. The candidate
binary is bit-for-bit the binary produced at the final tip `cf09780`
(rebuilt and sha256-verified; the tip adds only documentation and commit
messages relative to `cbd3093`, no compiled source differs). All 14
mode-configs pass the −3% median threshold in both clean runs.

Three fresh attempts against the tip (same protocol, 11 repeats) were each
invalidated by external load bursts on the shared host (loadavg spikes to
~7-15 with no process visible inside the container; base and candidate
depressed together far outside the clean runs' run-to-run variance, with
same-repeat interleaved pairs diverging exactly where bursts land). Per the
agreed rule, a burst that depresses base and candidate alike invalidates an
attempt; it is not a candidate FAIL. Attempt 2's first profile (consumers=0,
taken during a quiet window) is clean, in the established base ranges, and
passes all 7 mode-configs (worst −1.81%, gains `tree` +4.93% and
`tree-wait` +3.15% reported); its second profile was hit by a
burst. Attempt 3 ran under sustained load (every base median 15-35% below
the clean ranges). Attempts were capped at three; host CPU remained erratic
thereafter, so the tip re-measurement is deferred to a dedicated
examination rather than retried against noise.

## File map

- `runs-run1.jsonl`, `summary-run1.json`, `metadata-run1.json` — clean
  7-repeat alternating run at `cbd3093` (previous session). All 14 PASS;
  worst −2.16% (private, 0 consumers).
- `summary-cbd3093-11rep.txt` — clean 11-repeat alternating run at
  `cbd3093` (previous session). All 14 PASS; worst −2.64% (subset k=1%, 0
  consumers). Primary gate table. Raw rows were overwritten by fresh
  attempt 1 before preservation; the summary is reconstructed verbatim
  from the run log.
- `attempt1-summary.txt` — fresh tip attempt 1 (11 repeats): 0-consumer
  profile depressed 25-40% below clean ranges (burst); raw rows overwritten
  by attempt 2 before preservation.
- `runs-attempt2.jsonl`, `summary-attempt2.json`, `metadata-attempt2.json`
  — fresh tip attempt 2: consumers=0 profile clean and all-PASS;
  consumers=6 profile contaminated from repeat 5 (both versions drop
  together, e.g. subset k=100% ~4880 -> ~3800 waves/s).
- `runs-attempt3.jsonl`, `summary-attempt3.json`, `metadata-attempt3.json`
  — fresh tip attempt 3: sustained load; every base median below the clean
  ranges in both profiles.
- `measure.py` — the pinned alternating driver (writes `runs.jsonl`,
  `summary.json`, `metadata.json` in this directory; renamed per attempt).
- `mutations.md` — E2-3 mutation evidence and the entry.service audit.
- `check-tip.txt`, `soak.txt`, `commit-builds.txt` — `make check`,
  `make check-soak` and per-commit LDC+DMD private-suite output at the tip.
