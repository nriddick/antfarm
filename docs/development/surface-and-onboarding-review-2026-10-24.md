# Surface and onboarding review, October 24, 2026

Written after integrating the `forkjoin` subpackage, by reading every
onboarding page as a newcomer would follow the [documentation
map](../README.md#run-something): README, quickstart, using-it-well,
choosing-work, hosting, parallel-loops, the threadpool and fiber READMEs,
ARCHITECTURE, CONTRIBUTING, testing, ROADMAP and the examples index. Two
questions: which features now overlap enough that one should absorb or
retire the other, and where the onboarding path costs a reader time it does
not need to. Each item names the pages involved and a concrete suggestion.
Nothing here is applied; it is a list for the maintainer.

## Overlapping features

### Three ways to own the pool

`FarmHost` (one Farm, one group, caller token, pinned workers, ordered
teardown), `forkjoin.Dispatcher` (one Farm or one per group, caller as
participant 0, pinned workers, parked bitmap, ordered teardown) and the
hand-written shape hosting.md recommends for several groups (`makeBins`,
`at!T(llc)`, your own loop) each own the same four things: topology
selection, caller placement, a Farm per group with a token per thread, and
shutdown order. `Dispatcher.start` is a third copy of code the roadmap's
"per-group-Farm host" would provide once.

Suggestion: build the per-group-Farm host and make `FarmHost` and
`Dispatcher` consumers of it. The dispatcher needs exactly what the roadmap
item lists (one Farm per accepted group, a token per Farm, `at!T(llc)`
routing) plus a per-participant worker body. hosting.md's "Several physical
groups" would then stop describing a hand-written shape.

### Two representations for a computation tree

The private actor hierarchy and `forkjoin` both run a tree with a join.
choosing-work.md now draws the line (bare frames and speed against typed
result ports, non-POD state, cancellation, deadlines, tracked outcomes), but
the private tree also carries the cohort slab, `startSubset`, pipelined
folds and three guide pages (private-actors, private-results-and-waits,
outcomes-and-cancellation, 860 lines together). On the fork-join benchmarks
the private port was not in the default set because fib(39) needed minutes
and gigabytes.

Suggestion: keep both, but say in private-actors.md's first paragraph that a
tree whose nodes need none of the private features belongs on `forkjoin`,
and consider whether cancellation and deadlines for fork-join frames (a
cooperative flag read by `run`, a sentinel join) would let the private tree
retreat to its real niche: externally owned result storage and non-POD state
with destructors.

### Two parallel-loop paths, soon three

`antfarm.parallel` offers blocking loops (`parallelForWait`, the `parallel`
foreach) and asynchronous `parallelFor` with a hook; `FarmHost.parallelFor`
wraps the blocking form; `payloadRange!fn(range, grain)` is the raw table
form the quickstart teaches. A fork-join over a range (split in halves down
to a grain, as the matmul and nqueens ports do) is a fourth way and, with
stealing, the one that balances uneven iterations. The blocking loops carry
their own hazard (fatal inside a callback, a parked peer pinning
reclamation, the `wake`/`wakeContext` options) that the dispatcher does not
have because its join never blocks.

Suggestion: once `forkjoin` has a range splitter (two functions), route
parallel-loops.md's blocking section to it and keep `antfarm.parallel` for
the asynchronous hooked form and the grain adapter. That removes the "never
call a blocking loop inside a callback" rule from using-it-well.

### Worker idle machinery

The Director offers `spin`, `wait`, `sleep`, `sleepUntil`, `cadence`,
`signal`, `nudge` and `drift`; `FarmHost` adds `WorkerIdle.park` with
`idleTimeout`; `forkjoin` uses `wait` plus `wakeWorker` and its own parked
bitmap; `ant_fibers` has `FiberWakePolicy.servingGroups`. hosting.md
measures the pool's spin path at -7.8% against a hand-written loop and
explains it by the per-pause `atomicFetchAdd` on a process-wide spin
counter. Nothing in the tree uses `cadence` or `drift` except their tests.

Suggestion: make the spin-count atomics debug-only (they are statistics), so
`WorkerIdle.spin` stops costing throughput, and decide whether `cadence`,
`nudge` and `drift` are a product feature or a test fixture; if the latter,
move them behind a version. A parked bitmap like `forkjoin`'s (who is
asleep, wake exactly one) is useful to every host and could live in the
pool beside `wakeAll` and `wakeLlc`.

### Surfaces that exist as stubs or opt-outs

`threadpool.exchange` describes itself as a hypothetical cross-NUMA
exchange, v1 stub, with pairwise matrices and draining not implemented.
`HostGroups.span` exists so a program can do what the next section measures
and advises against. Fiber `assist` edges are experimental and opt-in.
`ManagedWorkerHooks` is a second worker lane whose only in-tree user is
`ant_fibers`. The MT payload form was removed in 1.9 but its removal history
still opens SPEC section 11, ahead of the lineage passage (Piquer, SNZI)
that the section exists for.

Suggestion: delete `exchange` until a NUMA host exists to validate it; keep
`span` but move it out of the hosting guide's placement section into a
"when you know better" note; move the MT-payload removal history to the
archive and keep the rationale appendix, whose Piquer and SNZI lineage
passage is the one place that explains the tally's design. The managed lane
stays as long as fibers need it, but its README section could say so.

### Duplicated numbers and status

The current release number appears in README, ARCHITECTURE and ROADMAP;
ROADMAP still has a "1.9.0 progress" section under a 1.9.1 release;
the 517-to-207 cliff figure is quoted in PERFORMANCE, hosting, using-it-well
and ARCHITECTURE; `make check-quick`'s duration is quoted in CONTRIBUTING to
the millisecond with a date. Each is a maintenance point.

Suggestion: one `VERSION` line that the pages read by reference (or a
release checklist that lists the three places); move measured figures out of
the guides into PERFORMANCE and link; drop the quoted duration.

## Onboarding

What a reader meets in order, with the cost it carries.

1. **README leads with C and C++.** The language a D library's README opens
   with sets expectations; the D path is second and the native path has two
   build commands before the first D program. The "Three rules" that follow
   are the best paragraph in the tree and should come first.
   Suggestion: open with the three rules and the representation table, then
   the two entry paths side by side.
2. **First payload is 50 lines with a nine-parameter constructor.** It is
   honest (named arguments, retry loop, completion counter) but a newcomer
   has to understand quota tiers before the first run. The example also
   teaches the manual `write`/`popFrontN`/`consumeNext` retry that the
   guides later say to avoid in favour of adapters.
   Suggestion: a ten-line first program on `FarmHost` or `forkjoin` (both
   hide the ring), with the raw ring program as the second step titled "what
   the host does for you".
3. **`import threadpool;` does not give you `FarmHost`.** hosting.md and the
   quickstart both warn about it, which means readers hit it. Suggestion:
   re-export `threadpool.host` from `threadpool`, or move the host to its
   own subpackage name the examples import.
4. **Build lines differ per program.** The quickstart's single-file Dub
   header, the examples' `-I. -Iexamples -Ithreadpool/source`, the fiber
   package's own commands and now `-Iforkjoin/source` are four shapes.
   Suggestion: every example a `dub run --single`, and the README showing
   only that form; compiler-direct lines belong in examples/README.md.
5. **The map promises seven steps in about an hour, and the steps are long
   pages.** hosting.md is 409 lines and holds a benchmark comparison, a
   coherence study and a cross-CCD guidance list that newcomers do not need
   before their first host. using-it-well is 132 lines of rules with
   measured figures inline.
   Suggestion: split hosting.md into "run a host" (setup, placement
   defaults, progress, teardown) and "scaling across groups" (the study and
   rules); keep using-it-well to the rule sentences and move every number to
   the page it links.
6. **Maintainer vocabulary in user pages.** "CyDo agent process",
   "T19", "rc.1", "pulse", "Sub0", gate percentages and dates appear in
   CONTRIBUTING, ROADMAP and the testing page, and the guides cite commit
   hashes. A contributor needs them; a user does not.
   Suggestion: keep ROADMAP and docs/development as the maintainer record
   they are, and remove hashes and gate records from the guides.
7. **The representation table has six rows and now seven.** Each row is a
   whole subsystem with its own guide. The table is right, but the newcomer
   has no default. Suggestion: name a default per shape in one sentence
   above the table: "independent elements: payload tables; a recursive
   tree: forkjoin; a service with an inbox: public actor; everything that
   waits: a Fiber", and let the table carry the exceptions.
8. **`make check-quick` is the trust step and takes a minute and a half.**
   That is fine for contributors; for a user it is the wrong step 7.
   Suggestion: step 7 becomes "run the example matching your shape with
   `dub run --single`"; the gate stays in CONTRIBUTING.

## What is good and should stay

The ownership rule (one page states each rule, others link) is applied
consistently and makes the tree navigable once learned. The three rules
paragraph. `choosing-work.md`'s public-versus-private section. The hosting
guide's progress contract ("neither call ever infers idle from a zero flush")
is exactly the sentence a reader needs. The testing lane table is complete
and honest about what each lane does not cover. The ROADMAP records negative
results and unresolved measurements instead of deleting them, which is rare
and worth keeping even if it reads as a log.
