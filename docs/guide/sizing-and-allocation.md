# Sizing and allocation

Sizing a Farm, choosing a producer tier, and choosing an actor allocator are
separate decisions. The normative Farm formulas are in
[ARCHITECTURE.md](../../ARCHITECTURE.md#sizing-one-farm) and
[SPEC.md](../../SPEC.md); this page adds the actor-facing rules and the
allocator options.

## Farm setup and tier choice

`AntFarm.create` takes ring size in MiB, not ulongs. `ringMiB` is a power of
two, at least 2. Producer quotas are ulongs. `expectedConsumers` is a sharding
hint; the hard cap is 128 subscribed views. Invalid geometry or an impossible
quota budget is fatal at construction. Defaults are enough for a first
program; tune only after the sizing model, not by guessing.

The budget is the sum of simultaneous grants, `maxBulk * quotaBulk +
maxSmall * quotaSmall`. It must fit in `k - 1` segments. A tier left at
`max* = 0` is disabled. One active producer needs one slot of the tier it
registers. Register a bulk token only when a small quota cannot admit the batch
you actually publish.

Private root batches and the Farm quota are different knobs. The park's
configured batch limit fixes how many root-table slots are reserved.
`pump(token, limit)` may use a smaller limit; that does not add slots. How many
private entries one table admits per quota depends on the Farm's shard count and
the entry's body width; see
[How many entries fit a quota](../../ARCHITECTURE.md#how-many-entries-fit-a-quota).
A larger configured batch is split into accepted prefixes. Child waves inherit
the creating context's limit unless creation passes an override.

## Memory backing

Farms use ordinary 4 KiB pages by default. Huge pages remain an exploitable
option for low-level payload workloads that move very large quantities of work
through the ring. They reduce translation overhead during long sequential table
walks, but are not a universal win and can add promotion or first-touch costs to
shorter and Fiber-backed workloads.

Opt in per Farm with the final construction argument or for a complete process
with the environment override:

```d
// fragment: construction arguments only; consumers and producers are omitted
auto farm = AntFarm.create(ringMiB: 32, k: 8, expectedConsumers: consumers,
                           maxBulk: 0, maxSmall: producers, quotaSmall: quota,
                           hugePages: true);
```

```text
ANTFARM_HUGE_PAGES=1 ./payload_benchmark
```

`ANTFARM_HUGE_PAGES` accepts `0` (force 4 KiB) or `1` (force huge pages); any
other non-empty value is a fatal error rather than being silently ignored.

On Linux this requests `MADV_HUGEPAGE`; the kernel still decides whether to
promote the shared mapping. On Windows it uses `SEC_LARGE_PAGES` and requires the
Lock Pages in Memory right; `grant_lock_pages.d` builds the privilege helper.
`farm.usedLargePages` reports that the requested platform path was applied, not
proof of Linux promotion. Benchmark the actual payload and inspect the live
mapping when page size is material to the result.

## Configuring batches and Farm quota

`PrivateRootPark.create(farm, capacity, batchLimit = defaultPrivateBatchLimit,
allocator = ActorAllocator.cRuntime())` fixes the maximum root batch size.
`create(farm, capacity, PrivateRootOptions)` takes the same two settings plus
`tracking`. The park allocates each batch's entry storage through its allocator
at creation, using `storageLimit = min(batchLimit, capacity)` entries per batch.
It reserves `ceil(capacity / storageLimit)` root table slots, equivalent to
`ceil(capacity / batchLimit)` for every positive capacity. A configured limit
larger than capacity therefore does not reserve unusable entry storage.
Allocation sizes that overflow return null. A zero configured limit is fatal.

`pump(token)` uses that configured limit. `pump(token, limit,
avgCost = rootCostHintAuto)` accepts 1 through the configured maximum; zero or
an oversized override is fatal. Choosing a smaller per-call limit does not
increase the table pool. Choose a smaller configured limit when more small
tables must be in flight.

`context.createChildren!T(capacity)` inherits the immediate parent's effective
limit: a root's children use the root park's configured `batchLimit`, while a
child's children use that child's cohort limit. An explicit override therefore
propagates down the hierarchy until another override replaces it.
`context.createChildren!T(capacity, childBatchLimit)` supplies a positive
override, with no fixed cap. The child's existing member array supplies each
accepted prefix, so even a large limit does not allocate extra child dispatch
storage. Zero is fatal; overflowing member capacity returns null. Values above
256 are legal for both roots and children.

### Work-cost hints

The trailing `avgCost` argument is the Farm work-cost class the table publishes
(`write`'s `avgCost`, SPEC 7a): consumers claim a table in runs of
`MAX_CHUNK >> avgCost` entries, and a table below the small-table threshold is
claimed wholesale by shard 0. `MAX_CHUNK` is 128 and a class is `0 .. 7`
(`MAX_AVG_COST`; anything higher is fatal), so the runs are 128, 64, 32, 16, 8,
4, 2 or 1 entries. Every `write` overload defaults to `DEFAULT_AVG_COST` (3,
runs of 16). Classes 0 through 2 (runs of 128, 64 and 32) are for very cheap
uniform callbacks, where a larger run amortizes the claim; higher classes are
for expensive callbacks, where a shorter run shortens each visit and the
leftover tail. `avgCostForChunk(n)` returns the class for runs of `n` entries.
A class below 3 is an opt-in: the default keeps the 16-entry run, and a table
below the small-table threshold (64 by default) is one claim of its whole
length whenever its run is at least as long as the table. With no hint — the default `rootCostHintAuto` —
`pump` derives the class per table with `rootCostHintFor(count, live consumers,
small threshold)`, where the consumer count is the farm's live subscribed-view
count (`Cf`), which can include the emitter's own view. A table at least the
small-table threshold wide is sharded across every shard and keeps the cheapest
class the derived hint names (runs of `rootCostHintMaxChunk`, 32 entries, which
is class 2); a wholesale table gets the cheapest class from there whose runs
still split it into at least `min(count, clamp(consumers, 1, rootCostHintMaxSplits))`
claims (runs of 32 whenever they already provide that many). That lets a small expensive root
table — for example parents that allocate a child wave each — occupy up to
about consumer-count workers instead of one, while every sharded table and every
single-consumer table keeps its exact prior claim sequence. An explicit
`0 .. MAX_AVG_COST` hint is published unchanged and keeps its exact meaning;
`pump(token, limit, 2)` is the pre-derivation fixed-hint behavior (runs of 32).
Since `MAX_CHUNK` rose from 32 to 128, a given class names a run four times
longer than it did, so a hint chosen as class `k` before now wants `k + 2`.
The measured effect of the derived default is recorded in
[PERFORMANCE.md](../../PERFORMANCE.md#batch-limit-and-work-cost-hints).

Child wave tables (`emit`, `emitSubset`, resume emits) always publish the class
of 32-entry runs (class 2). Any child table below the small-table threshold is
claimed wholesale by shard 0, and one of at most 32 entries runs in a single
claim on one worker: that
includes every child table while the cohort's batch limit is 32 or less, as well
as a small final partial table under a larger limit. Child waves take no
cost-hint argument, so a caller whose child callbacks are too expensive for one
claim should keep the child batch limit at least the small-table threshold so
tables shard across consumers.

The Farm producer tier bounds actual admission. Its default small quota is 4096
ulongs; how many private entries that admits in one table depends on the shard
count and the entry's body width. A larger batch is split into accepted prefixes
and requeued suffixes. To admit larger tables, configure a larger `quotaSmall`,
or configure a bulk producer (`maxBulk` / `quotaBulk`) and register a
`Tier.bulk` token. All quotas must still fit the Farm's segment and total
producer-quota constraints.

Larger batches amortize table overhead, but more roots wait behind one slow
callback because a whole physical table returns together. Smaller batches limit
that delay and reserve more table slots. The measured batch/quota/consumer
trade-off is recorded in
[PERFORMANCE.md](../../PERFORMANCE.md#batch-limit-and-work-cost-hints).

## Allocators

The default `ActorAllocator.cRuntime()` uses the repository's 64-byte-aligned
C-runtime pair. It supports alignments through 64 bytes and returns null beyond
that. Public actor state is still POD: creation copies bytes and there is no
destructor. Private state may be a non-POD struct with a `nothrow @nogc`
destructor. The library moves it and destroys the stored instance once, on a
consumer (roots follow the park's reclamation policy, emitter by default).
Result type `R` stays POD.

POD does not mean pointer-free, and it does not mean the GC scans the
allocation. Actor storage is outside the ring and outside the GC unless you opt
in. `ActorAllocator.gcRooted(&inner)` registers each allocation with
`GC.addRange` and removes the range before free, after destruction. `inner` and
its context must outlive every allocation and the hierarchy. Descendants inherit
the root allocator. Registration takes the global GC lock; skip it when state
holds only manually managed resources.

Callback threads must be druntime-registered. Threadpool workers already are. A
raw native thread needs `thread_attachThis`. The emitter thread must be
registered too when state holds GC references: a root spawn argument sits on its
stack while it pumps, and default emitter reclamation frees those ranges there.

The allocator must allow allocate on one thread and free on another. The
allocating thread is not necessarily the freeing thread. Non-thread-safe hooks
are not checked.

### The mimalloc v3 adapter

`actors/mimalloc.d` is a deliberately thin, optional mimalloc v3 adapter. It
calls the public C ABI
[`mi_malloc_aligned(size, alignment)`](https://microsoft.github.io/mimalloc/group__aligned.html)
and
[`mi_free_size_aligned(pointer, size, alignment)`](https://microsoft.github.io/mimalloc/group__posix.html).
The latter matches `ActorAllocator`: actor allocation preserves the exact
requested byte count and alignment through reclamation, so the adapter can use
v3's sized-free path without allocation headers.

The adapter does not replace process `malloc`, own mimalloc heap objects, or
change the default allocator. Compile with `AntfarmMimallocV3` and link
mimalloc, or select Dub's opt-in `mimalloc-v3` configuration:

```d
// fragment: the adapter plugs into the same ActorRuntime.create call
import actors.mimalloc;

auto runtime = ActorRuntime.create(farm, capacity,
    mimallocV3ActorAllocator());
```

The upstream source is pinned as a submodule at v3.5.3, commit
`d4881d338125e1cb7c47ba4cfb398d6f7c0c8d45`. The actor torture binary keeps a
fast local C-ABI stub and adds a real static-link lane that asserts
`mi_version() == 30503`. With `MI_OVERRIDE=OFF`, only the explicit actor policy
uses mimalloc; the D runtime and ordinary `malloc` remain unchanged.

Long-running resident-memory behavior and throughput/latency benchmarks remain
deferred. Platform-specific static/shared link coverage also remains before
promotion. Explicit `mi_heap_t` or arena ownership belongs in a later arena
phase because its destroy/reset fence must be tied to aggregate retirement.

## Cohort state storage

By default (`PrivateStateStorage.perObject`) a child `spawn` allocates its state
through the root allocator, and retirement frees it on the retiring consumer. In
tree workloads one parent callback allocates a whole cohort and several
consumers free it. With mimalloc each of those frees is a remote free: a CAS on
the owning page's shared free list; a large share of cache fills land there. The
measured cross-thread free counts and actor-mode rates are in
[PERFORMANCE.md](../../PERFORMANCE.md#allocators).

An opt-in cohort slab reserves every member's state when the cohort is created.
The conductor below refills one slab cohort three times. Each refill reuses the
slots that the previous retiring wave cleared. A child cohort can opt into one
reserved state slab, `PrivateStateStorage.cohortSlab`: `capacity * T.sizeof`
bytes are allocated when the cohort is created, up to the explicit
`slabByteLimit` argument of its `createChildren` overload. Allocation failure
therefore shows up as a null `createChildren`, never as a failed `spawn`.
Retirement destroys and zeroes a slot, and a refill reuses it only after the
retiring wave's join. The slab is freed once, when the cohort is disposed, on
whichever consumer disposes it. Under `gcRooted` it is a single range for the
cohort's life: one registration instead of one per member, but every collection
scans the whole reservation. The default per-object storage is unchanged.

```d
import antfarm;
import actors;
import std.stdio : writeln;

struct Item { ulong input; }
ulong square(scope ref PrivateBorrow!Item actor, scope ref PrivateContext context)
    nothrow @nogc @system
{
    return actor.value.input * actor.value.input;
}
enum members = 4;
alias Items = PrivateChildPark!(Item, ulong, PrivateStateStorage.cohortSlab);
struct Conductor
{
    Items* children;
    ulong* total;
    uint wave;
}
PrivateRootDisposition conduct(scope ref PrivateBorrow!Conductor actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    with (actor.value)
    {
        if (children is null)
        {
            // One zeroed slab of members * Item.sizeof bytes, under an explicit cap
            // (the last argument).
            children = context.createChildren!(Item, ulong, PrivateStateStorage.cohortSlab)(
                members, PrivateCohortOptions(0, false), members * Item.sizeof);
            if (children is null) fatal("child park or slab allocation failed");
        }
        else
        {
            foreach (r; children.results(context)) *total += r;
            if (++wave == 3) return PrivateRootDisposition.finish;
        }
        // Slot i serves Entry i; spawn cannot fail with allocationFailed.
        foreach (i; 0 .. members)
            if (children.spawn(context, Item(wave * members + i + 1)) != PrivateSpawnResult.spawned)
                fatal("child spawn failed");
        if (children.startFinal!square(context) != PrivateStartResult.started)
            fatal("child wave failed");
        return PrivateRootDisposition.waitChildren;
    }
}
void main()
{
    ulong total;
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");
    ConsumerView view;
    if (view.subscribe(farm) < 0) fatal("consumer subscription failed");
    auto park = PrivateRootPark!(Conductor, conduct).create(farm, 1);
    if (park is null) fatal("private park allocation failed");
    if (park.spawn(Conductor(null, &total)) != PrivateSpawnResult.spawned)
        fatal("private spawn failed");
    while (park.live != 0)
    {
        park.pump(token);
        while (view.consumeNext()) {}
    }
    if (total != 650) fatal("joined result mismatch"); // 1^2 + ... + 12^2
    writeln("slab cohort refilled three times");
    park.destroy();
    park = null;
    view.unsubscribe();
    farm.unregisterProducer(token);
    farm.destroy();
}
```

- **Type and API.** The storage policy is the park's third template argument, so
  pointer fields name it. `PrivateChildPark!(T, R)` stays per-object with an
  unchanged layout and unchanged code. A slab cohort comes only from
  `createChildren!(T, R, PrivateStateStorage.cohortSlab)(capacity, options,
  slabByteLimit)`. The other three overloads remain per-object only, and
  `PrivateCohortOptions` is unchanged.
- **Creation and the byte cap.** Creation makes one allocation of
  `capacity * T.sizeof` bytes at `T.alignof`, through the hierarchy's allocator,
  after the Entry, result, and selection arrays. It is zeroed before the park is
  published. `createChildren` returns null, allocating nothing, if
  `capacity * T.sizeof` overflows or exceeds `slabByteLimit`. `slabByteLimit` is
  a required, explicit reservation budget: 0 rejects every slab. It also returns
  null if any allocation fails or the alignment exceeds the allocator's limit
  (64 bytes for `cRuntime`); every earlier allocation is unwound first.
- **Failure moves from spawn to create.** Entry i always uses slot i, so `spawn`
  never returns `allocationFailed` for a slab cohort. `capacityFull`,
  `waveActive`, and `retiring` are unchanged.
- **Retirement.** Each retirement path keeps its consumer sequence: child
  cohorts, then `~T`. The slot is then zeroed and the member's state pointer
  cleared. The slot is never freed on its own.
- **Reuse.** A slot is reused only when `spawn` refills a cohort whose retiring
  wave (`startFinal`, or `start` with `retire = true`) has joined. Nonretiring
  and subset waves keep their members. There is no per-member reuse or
  compaction. A cancelled retiring wave still retires its undispatched suffix
  through cleanup tables before the join. The proof is in
  [../design/private-actor-layout.md](../design/private-actor-layout.md#cohort-slab-reuse).
- **Disposal.** The slab is freed once, at cohort disposal, after every retained
  member's descendants and destructor. The consumer that disposes the cohort
  performs that one larger free, which may be cross-thread. Result, Entry, and
  selection storage remain separate allocations with unchanged lifetimes.
- **Memory cost.** The full `capacity * T.sizeof` is reserved for the cohort's
  life, whatever its occupancy, plus one pointer in the park.
- **GC granularity.** Under `gcRooted` the slab is one range, added at creation
  and removed at disposal. This replaces one `GC.addRange`/`removeRange` pair
  per member. Every collection scans the whole slab, unspawned and retired slots
  included; those slots are zero, so they retain nothing. `gcRooted` registers
  the range just before creation zeroes it, so a collection that races creation
  can see stale allocator bytes. That is conservative retention, never a live
  `T`.
- **Diagnostics.** The borrow contract already forbids escaping pointers into
  state. Such a pointer now dangles into a slot that stays mapped and may be
  reused, so allocator or sanitizer use-after-free detection no longer sees it.
  Debug builds check that `spawn` writes into a zeroed slot.

Measurements, cross-thread free counts, and peak RSS are recorded in
[PERFORMANCE.md](../../PERFORMANCE.md#cohort-slab-vs-per-object).

## GC references

`ActorAllocator.gcRooted(&inner)` registers each allocation with `GC.addRange`
before returning it and removes the range before freeing, after destruction. The
`inner` allocator object and its context must outlive all allocations and the
hierarchy. It can wrap C runtime or mimalloc allocation and also registers
cohort metadata and result slots. Registration takes the global GC lock per
allocation/free; omit it for state holding only manually managed resources.
Callback threads must be druntime-registered. The emitter thread must also be
registered, not only callback threads: a root `spawn` argument holding GC
references sits on its stack while it pumps, and under the default emitter
reclamation it is the thread that removes the GC ranges when freeing.
