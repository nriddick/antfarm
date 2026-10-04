# Phase E1 root resumption gate

The [full table and analysis](../../PRIVATE_LIFECYCLE.md#phase-e1-root-wait-gate--2026-10-04)
records the CRT gate against c5a174f. `assessment.json` is the final decision:
no regressions below −3%, with favourable deviations outside the original ±3%
band explicitly reported under the orchestrator's clarification. `summary.json`
records both the symmetric-band result and the clarified no-regression result.
`interim-summary.json` preserves the literal-band assessment at the first stop.

There are 180 paired unused-feature samples, 20 new tree-wait samples, and ten
additional paired samples for polling tree at one consumer. No sample was removed.
`runs.jsonl` and `tree-one-repeat/runs.jsonl` retain commands and full stdout/stderr.
Both metadata files retain binary hashes, compiler information, source revision
and uncommitted benchmark diff. Binary paths are relative to the repository root;
`base-c5a174f/` denotes the separate base archive. `validation.md` includes the Entry layout,
compiler/build evidence, complete check/soak results, and all eight mutations.

Build a clean c5a174f archive and the candidate independently, then reproduce
on an idle Ryzen 5 5500 with this CPU layout. Never benchmark concurrently with
builds, tests, or another benchmark. This worktree's mimalloc submodule was absent;
these commands use the approved CRT fallback.

```sh
python3 perftest/results/private-root-wait-2026-10-04/reproduce.py \
  --base ./base-c5a174f/perftest/actor_churn \
  --new ./perftest/actor_churn --output ./new-results
python3 perftest/results/private-root-wait-2026-10-04/reproduce.py \
  --base ./base-c5a174f/perftest/actor_churn \
  --new ./perftest/actor_churn --output ./new-results/tree-one-repeat \
  --case tree,256,1
```

The runner defaults to five repeats, five warmups, three seconds, pinned CPU 0
for the controller and CPUs 1..N for consumers. Adjust the CPU mapping in the
runner before using another host. The `--allocator mimalloc` option requires two
binaries linked to the same allocator pin; its version is in their raw output.
