# E2-4 self-review

Reviewed runtime, tests, docs and benchmark diff against the approved protocol.

- Reuse: retained retainJoinCredit, releaseWave, retireMember, existing Farm
  prepareTableFrontN contract and test allocation/barrier/death scaffolding.
  opKind is shared by whole/subset start and dispatch; writeResult is shared
  by ordinary and continuation result writes. Child return CAS mirrors the
  root loop intentionally: its target, hook and final-access ownership differ;
  no generic MPSC helper exists here. No surrounding refactor is needed.
- Quality: metadata remains in the planned Entry union and per-cohort descriptor;
  no redundant per-member side array or state flag was introduced. Debug write
  tags are the orchestrator-approved mutation enforcement, appended to the
  existing result allocation. Runtime completion never allocates.
- Efficiency: ordinary dispatch has no new RMW. The pump adds its raw empty
  hint; resume tables retain W before visibility and rollback zero admission.
  Benchmark launch/fold routines are reused between matched feature modes;
  both use the existing completion array rather than allocating a duplicate.
- Lifetime: independently reviewed in lifetime-review.md; H precedes custody
  release, P is the final park access, range cursors are independent, prepare
  clears the tail before visibility, and emitter-final retirement only queues
  consumer cleanup. Every credit target is snapshotted before idle.
- Test forcing: the initial C4 soak stall was a proved scaffold self-deadlock.
  The corrected gate observes leaf arrival after pump return before opening
  the leaf, forcing consumer-owned notification. Diagnostic stack and exact
  reproducer are in test-evidence/late-owner-investigation/. Runtime unchanged.
- Portability: production code and positive tests use existing D runtime,
  atomics and allocator APIs. Linux-only death/perf/soak drivers follow the
  repository's existing local-gate convention. Windows smoke remains external.

No required lifetime-protocol fix emerged from review. The rootService guard depends on
structural caller proof, not pointer bits. C-M10 and C-M13 detection is expressly
a debug invariant check, not an observed release-mode result mismatch/race.

Verifier revision: added the same ambiguous-result template guard already used by PrivateChildPark to PrivateChildStep itself, with two negative compile controls. The C13 emitter now continues publication until the competing member starts; its post-CAS return barrier still prevents the C-M12 mutation from being hidden by draining the head early. The forced schedule passed 3 normal and 100 TSan invocations; C-M12 was caught again. The requested C3/C4/C5/C7/C9/C14 publication/barrier audit found no additional changes. A one-line releaseRoot comment identifies its root-only callers. No new production state, helper or execution-time operation was added in this revision.
