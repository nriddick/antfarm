# Actor torture suite

This suite promotes the A2 actor inbox beyond ordinary contention coverage.
It compiles deterministic test hooks into `actors.actor` and forces senders
to stop at each ownership/publication boundary while activation and retirement
proceed on other threads.

Coverage includes:

- node claim before inbox publication;
- close after reservation, claim, publication, and activation signal;
- eight simultaneous pre-close reservations racing to claim one node;
- accepted-before-close drain and reservation quiescence;
- an engine-side module-generation fence model which closes admission and
  aggregates resident type-erased actor owners with old-generation code and
  ownership claims;
- independent unload stalls for a running actor callback, a module-owned
  destructor, a sender paused after its actor submission reservation has been
  released, and a popped-but-uncompleted inbox node;
- proof in the sender and popped-node cases that the actor can be reclaimed
  first while the remaining engine claim still forbids the simulated unload;
- wake and send in the wave lifecycle-CAS/owner-store gap, plus a sender
  admitted before the wave and signalled after owner publication: the wave
  completes with `interrupted && !failed`, and exactly one later activation
  honours the signal (and completes the sent node). Descriptor reuse clears
  outcomes and rejects old handles; duplicate admission still sets `failed`;
- a wave-reservation/retirement race proving that aggregate membership pins a
  retired actor and its runtime until the open wave is sealed;
- immediate reuse of a released wave member as another wave's head, proving
  that the old wave finishes its link writes before releasing the lifetime pin;
- a late accepted sender queuing work between retirement's initial idle
  observation and its submission-gate join, proving the lifecycle is rechecked
  before using the idle retirement store;
- F4b public cancel-mode retirement: a 32-node backlog cancelled in one
  `cancelling` activation with every sender reading `cancelled` after
  `available`, a drain-mode control on the identical backlog that completes
  every node and never reports `cancelling`, and a budgeted first activation
  whose carried remainder is cancelled by the next activation's
  `cancelInbox`;
- sequential drain-to-cancel escalation (`requested`), repeated cancel
  (`alreadyRetired`), drain-after-cancel keeping the cancel mode, and
  cancel-after-RETIRED, exercised against consumer entry (sender paused
  after its activation signal), consumer exit (activation stalled inside the
  handler while the owner escalates), and a drain request parked at
  `retirementIdleObserved` while a late sender's signal reschedules the
  actor; the same cancel and drain-to-cancel escalation through the
  type-erased `ActorErasedOwner`, whose handler must observe `cancelling`;
- cancel while SCHEDULED with a queued inbox (the entry word), cancel of an
  idle actor whose submission gate is held by a paused sender (retirement's
  idle reschedule and the empty-inbox consumer reschedule, both without a
  handler call), and a sender's signal landing on IDLE|RETIRE|CANCEL while
  the retirement thread is parked on its idle observation;
- cancel between a wave's lifecycle reservation and its membership
  publication, on both the admission path (the wave operation exits
  interrupted and hands the actor back with the mode preserved) and the
  rollback path (a non-idle second member fails the reservation and
  `cancelActorWavePayload` restores the mode), in both cases with a sender
  that passed the zero `waveOwnerWord` check observing `cancelled` rather
  than `completed`;
- an idle, empty actor cancelled directly retiring without any handler
  invocation, documenting that no final handler call is guaranteed;
- multi-actor ready-queue contention with six producers and six consumers;
- a paused ready-list detach proving concurrent flushers return promptly,
  wakes remain publishable, and the next drainer acquires the saved backlog;
- a 1,024-actor backlog with partial writes, repeated Farm backpressure,
  four concurrent flushers, two consumers, and exact self-republication counts;
- FIFO order per producer, exact-once delivery, payload checksums, bounded
  carry republishing, 32 rounds of generation reuse, and a disabled GC;
- balanced cold-path allocation and a flat warm-path allocation count;
- free-slot recovery after repeated state-allocation failures, exact capacity
  exhaustion, and eight-thread creation/reclamation through 64 generations;
- the optional mimalloc v3 adapter's exact pointer/size/alignment forwarding
  and 128-byte-aligned actor state, using a local ABI stub rather than
  requiring mimalloc for the core suite;
- a real static link against the pinned mimalloc v3.5.3 submodule, checked at
  runtime with `mi_version() == 30503`; and
- 8,192 actor-state frees from eight threads other than the allocation thread,
  plus foreign-thread frees of each runtime and stable-slot array. The same
  test runs against release, `MI_DEBUG=FULL`, and mimalloc-instrumented
  ThreadSanitizer builds.

Run with LDC, DMD, or LDC ThreadSanitizer:

```text
make -C actor_torture run
make -C actor_torture run-dmd
make -C actor_torture run-tsan
```

`make -C actor_torture death` runs the child-process diagnostics table
(`actor_torture death`) on LDC and DMD, debug and `-release`. Each row
re-executes `/proc/self/exe` as `child:<mode>` and checks the exit status and
stderr: fatal sites must abort with their message, debug diagnostics such as a
dropped live `ActorOwner` must print and exit 0, and silent rows must leave
stderr empty. Rows for `version(assert)`-only diagnostics print `SKIP` under
`-release`.

For the wave lifecycle and outcome checks only, build with `make -C
actor_torture` and run `actor_torture/actor_torture --waves`. The individual
signal regressions accept `--wave-wake`, `--wave-send`, and `--wave-late-send`.
Each fails against the old dispatch-to-`failed` mapping after verifying the
operation and autonomous signal both ran, at "benign wave signal must not fail
the completed wave".

After initializing submodules, run the real mimalloc lanes with:

```text
make -C actor_torture run-mimalloc
make -C actor_torture run-mimalloc-dmd
make -C actor_torture run-mimalloc-debug
make -C actor_torture run-mimalloc-tsan
```

The real TSan lane requires Clang for mimalloc and LDC for the D test binary.

On Windows x64, with CMake, Visual Studio and the D compiler on PATH, run
the static allocator lanes from the repository root:

```powershell
./actor_torture/run-mimalloc-windows.ps1 -Compiler dmd -Configuration Debug
./actor_torture/run-mimalloc-windows.ps1 -Compiler ldc2 -Configuration Debug
./actor_torture/run-mimalloc-windows.ps1 -Compiler ldc2 -Configuration Release
```

`-CMake` accepts a full path to CMake when it is not on PATH. Debug uses
unoptimized allocator code and `MI_DEBUG=FULL`, with the static release CRT
(`CMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded`, D `-mscrtlib=libcmt`).
Using CMake's default Debug CRT instead introduces an unresolved
`__imp__CrtDbgReport` dependency. The script builds separate Debug/Release
archives, links the real allocator, and runs the full actor suite including
cross-thread frees. It does not modify the pinned submodule.

The ordinary library build contains neither the deterministic hook branches
nor a mimalloc link dependency. The Makefile builds the pinned static source
with allocator override disabled. The root Dub configuration `mimalloc-v3`
builds and links that pinned archive for Linux x86-64 and Windows x64. The
pinned correctness lane links the same source directly.

The module-generation fence in this suite is deliberately a test-side engine
model. Its close/count gate is the unload authority, while its per-kind counts
are diagnostics and its actor registry is driven by one coordinator. It does
not add module hierarchy to `actors`, implement dynamic-library loading,
or define non-POD actor-state destruction. The "destructor" claimant models
module-owned resource or message destructor code admitted before close.
