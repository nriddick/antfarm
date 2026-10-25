# Warm Fiber dispatch experiment

`warm_fibers.d` is an isolated D executable. It changes no production Fiber
scheduler or C ABI, and is not a complete coroutine runtime. The compared
mechanisms, ownership limits, raw results and reproduction command are in the
[measurement record](../../docs/performance/coroutines-2026-10-25/README.md).
The [design proposal](../../docs/design/coroutine-replacement.md) explains the
next full-feature prototype and the two proposed custom-runtime tiers.

```sh
python3 tools/coroutines/run.py --output .build/coroutines
```

The default run takes five shuffled samples of each mode at 1/2/4/8/16/32
threads and zero/three yields. `--threads`, `--yields`, `--modes`, `--slots`,
`--chunk`, `--seconds` and `--repeats` select smaller experiments. For example,
`--modes ring --chunk 1` tests single-item Farm claims, not the default chunk 8.
Every child process has a 90-second timeout; failed checks stop the run.
