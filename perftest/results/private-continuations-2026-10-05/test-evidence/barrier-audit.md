# Publication/barrier audit after verifier task 148

Audited C3, C4, C5, C7, C9 and C14 for an emitter waiting on work whose table it has not published. No additional changes were needed.

| Case | Why the emitter can make progress |
| --- | --- |
| C3, descendants first | The custody seam blocks a consumer. The emitter continues pumping while polling descendantReleased, so it publishes the descendant before releasing the held activation. |
| C4, activation first | The emitter observes leafArrived only after pump returns, then opens leafGo. This proves the leaf table was published and its producer seal completed. The finish hook asserts consumer ownership; the outer emitter loop continues pumping until it releases hookGo. |
| C5, hold/late hold | The consumer's activation epilogue is held. The emitter keeps pumping while polling midReturned/activeSeen, permitting the resumer and polling parent to run before it opens hookGo. |
| C7, final retirement | The interleave variant keeps pumping until the polling parent observes the active wave. In the seal-owner variant, the root and middle tables are each published before the emitter waits for their returns. After forced zero admission, the filler is drained before the leaf publication. The leaf return wait is inside tablePublished, after that leaf table is visible. |
| C9, before/after P | The emitter keeps pumping until pollEntered proves the polling parent is already running. Both resume-unschedule hooks run after the resume table was published. They wait only for that callback's return and progress of the already-running polling parent; no future root publication is required. |
| C14, accepted prefix | The publication hook opens the consumer only after the accepted resume table is visible and waits only for members in that table. Partial admission waits for A alone at limit one. |
| C14, full drain overlap | Two initial one-member tables are published before the consumer go flag. The emitter observes B started before arming the later publication hook; that hook waits for A in its just-published resume table and B in its already-published initial table. |

C13 was the exception: A could reach its held CAS while B's initial table remained unpublished. The emitter now pumps until secondStarted before waiting for secondReturned. It then waits for firstReturned before draining the competing head, preserving the C-M12 stale-head retry check. The verifier's exact forcing schedule failed three times before this change and passed three normal plus 100 TSan invocations afterward; see force-cas-stall.md.

Self-review: the step-type guard uses the existing cohort guard's D template pattern and diagnostic. No helper, allocation, state field, atomic operation in production execution, or protocol change was added. releaseRoot's only callers are root-batch completion and child-wave finish guarded by the captured parentIsRoot value, so its null enclosing-wave proof is sound.
