# Postmortem: Ant Farm + threadpool + Fiber backend

This note collates what it was like to *use* the three layers together:
Ant Farm as a job ring, `threadpool_llc` as topology/cadence/wake, and
`antfarm_fibers` as a freely migrating druntime-`Fiber` scheduler on top.
It is an experiment log, not a replacement for `README.md` or `PROGRESS.md`.

The stack is not trying to be a faster `core.thread.Fiber` for
single-thread async. It exists so Fiber-shaped control flow and ordinary
serial/parallel Farm jobs can share one worker pool. That goal is met.
The cost is real, and several APIs fight a D programmer who brings Phobos
Fiber habits with them.

Host for the numbers below: Ryzen 5 5500, 12 logical processors, one LLC,
LDC release, Ant Farm huge pages on (`THP=always`). Characterization, not
a guarantee. This box is expected to be a pessimistic concurrency-scaling
case versus the Intel host already used to profile Ant Farm (12700H,
~200–390M items/s). Extra-worker and SMT oversubscribe results here are
a lower bound on scaling, not the Farm's ceiling.

## How the three layers fit

```text
user Fiber body / Farm payload
        │
        ▼
antfarm_fibers          lifecycle, waits, timers, cancel, recycle
        │  ready activations only
        ▼
Ant Farm                tables of mixed ST and MT jobs, consumeNext
        ▲
threadpool_llc          pin, LLC bins, Director park/cadence/wake
```

Ant Farm never owns a Fiber. It carries one-shot resume payloads
(`maxCs=1, done=1`, body = task pointer) and, separately, MT range
payloads for `parallelFor` grains. Waits, timers, cancellation, GC roots,
and stack recycling stay in the scheduler. The pool does not own work
items; a worker is a locator that subscribes a `ConsumerView` and
optionally flushes a producer `Token`.

One `FiberLane` per LLC is the intended embedding. A `FiberDomain` holds
generations, roots, waits, and completions for every lane that shares it.
`FiberLane(farm)` still builds a private one-lane domain for small tests.

## Brief user guide

### 1. Pick a job shape first

| Work | Put it here |
|---|---|
| Short `nothrow @nogc` compute, no suspend | Farm payload (`PayloadHeader.call`) |
| Parallel `0 .. N` over a range | `FiberDomain.parallelFor` (grains are payloads, not Fibers) |
| Needs `scope(exit)`, heap, or to wait | Managed Fiber (`spawn` + `yieldReady` / `await` / `sleep*` / `joinFiber`) |
| Ordinary D `Fiber.yield` / `Thread.sleep` | Not scheduler suspends; do not use them in spawned bodies |

Empty Fiber bodies are about an order of magnitude slower than the same
increment as a payload. If the body never waits, it should not be a Fiber.

### 2. Single-thread embedding (tests, tools)

```d
auto farm = AntFarm.create(1 << 20, 8, 1, 0, 0, 1, 16_384); // huge pages default on
auto domain = new FiberDomain(farm);
domain.reserve(expectedLive);          // avoids roots realloc
auto token = farm.registerProducer(Tier.small);
ConsumerView consumer;
auto sub = consumer.subscribe(farm);   // keep this value; do not copy consumer
assert(sub >= 0);

domain.spawn({
    FiberDomain.yieldReady();          // not Fiber.yield()
    FiberDomain.sleepFor(msecs(1));    // not Thread.sleep
});

while (!domain.drained)
{
    if (domain.ready != 0)
        domain.flush(token, 256, 0);   // table size, not a quantum skip
    consumer.consumeNext();
}

auto done = domain.takeCompletions();
domain.releaseAll(done);               // recycle Fiber/stack
```

`ConsumerView` disables postblit. Subscribe it on the stack (or a class
field) and call `consumeNext` on that same instance. Do not pass it by
value, and do not let a nested timing lambda bitwise-copy it.

### 3. Bound threadpool embedding (the real product)

```d
auto topology = CacheAwarePool.topology();
FiberLane[] lanes;
foreach (i; 0 .. topology.llcCount)
    lanes ~= new FiberLane(makeFarmForLlc(i));  // size expectedConsumers to LPs on that LLC

foreach (lane; lanes)
{
    lane.flushBatch = 256;             // this host: large tables help Fibers
    lane.avgCost = 0;
}

PoolOptions opt;
opt.managedWorker = fiberWorkerHooks();
// opt.skipSmtSiblings = true;         // better for tiny payloads; worse for Fibers here
auto pool = new CacheAwarePool(opt);
installFiberLanes(lanes, pool);
pool.start();

lanes[0].spawn({ /* Fiber body; may migrate across workers of this lane */ });
pool.wakeAll();                        // spawn from a non-worker must wake

// later
pool.shutdown();
uninstallFiberLanes();
```

Workers register a consumer and a small-producer token at managed start
and keep them until stop. Coverage is a barrier: pumps do no scheduler
work until every selected worker has registered, and a lane with no
sweeper fails explicitly.

External spawn/signal and `requestCancel` should go through `FiberLane` so a
parked pool/control loop is woken. Cancellation carries the exact task
generation and is applied by the Director via `drainCancellationRequests`;
arbitrary callers do not mutate task control. Default wake policy broadcasts;
`directorOwned` coalesces bits for a control thread that owns
`pool.director()`.

### 4. Fiber bodies

A spawned body is druntime `Fiber` control flow with scheduler checkpoints:

- return / throw → terminal (`completed` / `failed`)
- `FiberDomain.yieldReady()` → republish on the selected lane
- `await` / `FiberEvent.wait` / `FiberSemaphore.wait` / `sleep*` /
  `joinFiber` / `parallelFor` → park, then one ready activation
- `cancel` is cooperative: `FiberCancelled` at the next checkpoint, or
  skip of a never-entered body
- `scope(exit)` and dtors run on those paths

`Fiber.getThis()` is the running Fiber. Stack locals survive suspend.
TLS, `errno`, and affinity do **not**. `TaskHandle` is generation-safe
across recycle; a raw `FiberTask*` is not.

### 5. Recycling

`takeCompletions` then `release`/`releaseAll`. Steady-state `spawn` is
`Fiber.reset` on a pooled stack, no mmap. The pool is the concurrent
high-water mark, bucketed by stack size. Hold `task.handle`, not the
slot, if identity must outlive `release`.

## Points of friction

These are the places this stack repeatedly fought the user (and the
tests).

**Phobos Fiber vocabulary.** `Fiber.yield()` is not a scheduler suspend.
The generation is failed with a retained exception rather than stranded,
but it still is not what a D programmer typed. `Thread.sleep` blocks the
OS worker. There is no compiler help.

**TLS across suspend is UB, and LLVM will prove it.** Addresses of
`thread_local` / `__gshared` TLS can be hoisted across `Fiber.call`.
Work that needs thread identity must finish inside one payload invocation
or one unsuspended Fiber slice. Pinning is the embedding’s policy, not
the scheduler’s.

**`ConsumerView` is a unique, non-copyable cursor.** `@disable this(this)`
is load-bearing. Nested D delegates that capture a stack `ConsumerView`
can leave `consumeNext` running on an unsubscribed copy (`hasRef=false`,
ready work sitting in the ring, `active` stuck). Timing helpers must
either call `consumeNext` in the same function that subscribed, or pass
`ref ConsumerView`. Release builds strip `assert(subscribe >= 0)`, so a
failed subscribe becomes a silent hang unless you check the return.

**Huge pages.** Ant Farm’s `create(..., hugePages=true)` default is the
fast path for sequential table walks. `ANTFARM_HUGE_PAGES=0` forces 4K
(useful in tests). Benchmarks that passed `false` were measuring the
wrong Farm. On a host with `THP=always`, the first drain after a cold map
can still pay a collapse tax; warm drain is the number that matters.

**Table size vs dropout.** `flushBatch` / `avgCost` size a Farm table.
`consumeNext` finishes that table (or first-claimant-yields).
`consumeQuantum` skips the rest of the table and fights sweepers; it is a
one-resume test probe, not a latency knob. Lower `flushBatch` if a
responder must get back to timers.

**Coverage and idle consume.** `consumeNext()==false` is a hole or `Wt`,
not emptiness. Responders keep sweeping while a published-but-not-entered
count is nonzero. A lane with workers but no responder throws. Install,
then start, then spawn.

**Explicit recycle.** Forgetting `takeCompletions` + `release` leaks
stacks at the high-water mark. Using a `FiberTask` after `release` can
observe the next generation. Handles exist because that was a real bug
class.

**Cooperative cancellation only.** A running body that never hits a
scheduler checkpoint will not unwind. Pre-entry cancel (CANCEL before
ENTER) skips the delegate. Running cancel is a flag observed at
`yieldReady` / `await` / post-yield.

**Spawn still serializes on the domain admission mutex, including cold
`new Fiber` + mmap.** Multi-threaded spawners will convoy. Warm
`Fiber.reset` is cheaper but still control-thread bound in the current
benchmarks.

**GC.disable is a D habit that does not compose.** It is fine around a
pure `Fiber.call` loop that does not allocate. Wrapping Farm consume or
first Fiber entry can stall (stack registration, mutexes, write-full
busy loops). Do not copy that trick onto the scheduler path.

**64-bit only.** The control word, Farm sequences, and threadpool
topology assume LP64.

## Concurrent system vs single-thread D fibers

Ameliorated DRuntime baseline (reserved `Fiber[]`, `reset`, `GC.disable`
on non-allocating `call` loops) versus this stack, 100k empty bodies
unless noted.

| Path | Rate (this host) |
|---|---:|
| DRuntime reserved `Fiber.call` (ST) | ~7.0–7.5M /s |
| DRuntime 1× Fiber reset+run (serial reuse, no N stacks) | ~27M /s |
| DRuntime 1-yield two-pass | ~3.0M fibers/s |
| antfarm Fiber run ST | ~4.0M /s |
| antfarm Fiber 1-yield ST | ~1.8M fibers/s |
| antfarm Fiber run, 12 SMT, flush 256 / avgCost 0 | **~8.3M /s** |
| antfarm Fiber run, 6 physical cores | ~6.7M /s |
| antfarm payload ST | ~54–63M /s |
| antfarm payload, 6 phys, flush 32 / avgCost 2 | ~53M /s |
| Cold Fiber create (either engine) | ~150–175k /s |

**As a single-thread async runtime it loses.** Folding a Fiber through
MPSC + Farm + `Fiber.call` is about half a well-tuned same-thread
`Fiber.call` loop. A yield is two Farm visits, again about half a
two-pass DRuntime call. Cold create is the same mmap either way. If the
program is “N coroutines on one OS thread,” stay on druntime Fibers (or
one recycled Fiber).

**As a concurrent job system it starts to pay rent.** On this 1-LLC box,
12 SMT workers with large tables drain empty Fibers at ~8.3M/s, above
the ST DRuntime loop, because there are twelve `Fiber.call` sites and
the Farm is a decent distributor. Tiny payloads do the opposite: extra
SMT consumers fight over the ring; 6 physical cores plus smaller chunks
match Ant Farm’s own spinning consumers (~55M), and 12-way spin drops to
~30M. That SMT-hurts-payloads split is expected to be more pessimistic
here than on the Intel Ant Farm host.

**The threadpool is not the tax.** A consume-only CacheAwarePool pump
that always retries matches pinned OS threads in a tight `consumeNext`
loop on 6 cores (~55M). `fiberWorkerHooks` (coverage, timers, flush) is
within ~6% of that. Parking idle workers costs ~13% on that job and
*helps* SMT oversubscribe, because extras leave the ring. Do not spin
the managed pump to “go faster” unless the payload is actually Farm-bound
and the worker set is the physical-core set.

**Two ceilings, not one.** Fiber drain is limited by `Fiber.call` and
control-word handoff. Payload drain is limited by Farm produce/consume.
Topology that wins for one loses for the other. Default
`flushBatch=32, avgCost=2` is conservative for mixed latency; empty
Fiber throughput on this CPU wanted `256 / 0`.

## Suggestions

### Make the Fiber-shaped path harder to misuse

- Name the scheduler API so Phobos collisions are obvious:
  `FiberDomain.yield` that is *only* `yieldReady`, and a documented
  trap for `core.thread.Fiber.yield`. A debug-mode TLS canary that
  `Fiber.yield` happened without a scheduler park already exists as
  “fail the generation”; surface it in the README recipe, not only
  after a failed outcome.
- `spawn` should take `void function()` as well as `void delegate()` so
  a module-level body does not allocate a nested closure per spawn.
- Check `ConsumerView.subscribe` in examples with `if (sub < 0) throw`,
  never `assert`. Same for producer tokens.
- Provide a tiny `drainUntilEmpty(domain, token, consumer)` helper so
  tests stop reinventing the flush/consume loop (and stop capturing
  `consumer` in `measure` lambdas).

### Make the concurrent path easier to aim

- Document two presets: **fiber-throughput** (all LPs, `flushBatch=256`,
  `avgCost=0`) and **payload-throughput** (`skipSmtSiblings`, smaller
  chunks). Default `flushBatch=32` surprised the Fiber numbers.
- Size Farm `expectedConsumers` / small-producer slots to
  workers + covering sweepers + any control-thread producer. Getting
  this wrong is a silent subscribe/register failure in release.
- `FiberDomain.reserve(n)` before a known burst. Cold create will still
  mmap; it at least will not realloc `roots`.
- Drop the admission mutex around `new Fiber`/mmap, or offer a
  `prepare()` that allocates stacks off the mutex. Multi-producer spawn
  is otherwise a convoy.
- Huge pages on in every non-test `AntFarm.create`. Tests keep
  `ANTFARM_HUGE_PAGES=0`.

### Make mixed workloads honest

- Keep payloads as the fast path and Fibers as the waiting path. A
  `spawn` that never suspends should be easy to grep out of profiles.
- `parallelFor` grains are already `@nogc` Farm iterations; a later
  foreach adapter should not turn each iteration into a Fiber.
- If a control thread only produces, prefer 6 physical consumers on
  this CPU, not 12. If workers both produce (flush) and consume, 12 SMT
  helped Fibers. Revisit worker count on the Intel Ant Farm host; this
  Ryzen is expected to understate concurrency scaling.

### Performance work that is actually on the critical path

- Fiber run is not Farm-bound for empty bodies; do not tune `avgCost`
  expecting 2× Fiber/s. Measure with a body that waits or a payload
  that does real work.
- Recycle is already the right create path. The remaining spawn cost is
  generation stores, root insertion, and MPSC publish — plus mmap when
  the pool misses.
- ST `Fiber.call` will remain the gold standard for “async on one
  thread.” Competing with it is the wrong race. Competing with “Fibers
  and payloads on the same pinned workers, without blocking those
  workers” is the one this stack can win.

## Command map

```text
ANTFARM_HUGE_PAGES=0 dub test
dub run -c stress --build=release
dub run -c benchmark --build=release --compiler=ldc2 -- 200000 256 0
dub run -c benchmark_mt --build=release --compiler=ldc2 -- 100000
dub run -c benchmark_vs_druntime --build=release --compiler=ldc2 -- 100000
dub run -c benchmark_farm_embed --build=release --compiler=ldc2 -- 2000000
```

`benchmark_vs_druntime` is DRuntime ST vs Fiber vs payload, then a
topology sweep. `benchmark_farm_embed` is Ant Farm spin vs threadpool
pump vs Fiber managed pump on the same payload.
