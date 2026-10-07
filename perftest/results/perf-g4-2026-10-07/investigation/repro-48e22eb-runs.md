# Reproducing the 48e22eb-benchmark runs

full-5, boundary-0, control-boundary-0, inert-control-tree-0 and the
superseded runs pinned `--benchmark-ref 48e22eb`; inert-control-0 pinned
`--benchmark-ref c4ad279` (the original benchmark). Commit 48e22eb is a
pre-rewrite commit that is not reachable from `perf/g4-default-hint`; the
other pre-rewrite revisions cited by this evidence — 48e22eb, 865978c,
838f308, c37082e and b61c2a6 — are likewise not reachable from any branch
and still resolve in this repository's object store while its reflogs
survive (~90 days by default). 9a40481 and 3d4e019 (the inert-control
candidates) are reachable only from the local, unpushed evidence branches
`perf-g4-inert-control` and `perf-g4-inert-exact`. The exact benchmark
source used by those runs is preserved verbatim in
[actor_churn-48e22eb.d](actor_churn-48e22eb.d).

To rebuild a benchmark-ref without the dangling commit:

```sh
git checkout -b g4-repro-bench c4ad279
cp perftest/results/perf-g4-2026-10-07/investigation/actor_churn-48e22eb.d \
   perftest/actor_churn.d
git commit -am "repro: pin the 48e22eb actor_churn benchmark"
python3 perftest/perf_gate.py --base c4ad279 --candidate <cand> \
  --benchmark-ref g4-repro-bench ...   # as recorded in commands.json
```

The inert-control candidate (b61c2a6, recreated as 9a40481 during review)
is base c4ad279 plus [inert-control.patch](inert-control.patch) applied to
actors/private_actor.d:

```sh
git checkout -b perf-g4-inert-control c4ad279
git apply perftest/results/perf-g4-2026-10-07/investigation/inert-control.patch
git commit -am "inert layout control"
```

Binary-equivalence caveat: the re-run gate batches (batch-1024-6,
batch-512-6, root-hint-4-batch-1024-6) used `--benchmark-ref edcbe5a`,
whose only functional difference from the 48e22eb benchmark is the
`__traits(compiles)` detection/import of `rootCostHintFor`; on the
pre-derivation base both report class 0 and on the candidate runtime both
take the same measurement path, so the timing evidence is comparable.
