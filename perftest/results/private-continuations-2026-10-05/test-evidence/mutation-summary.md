All 18 required mutations caught in the original implementation gate (runtime SHA256 34abc0bc615910d574308c91c1d633f995f59d8aa45bed20d08f01059145929d).

| Mutation | Exit | Detection |
|---|---:|---|
| C-M1 | -6 | antfarm fatal: private child result: unwritten slot |
| C-M2 | -6 | FAIL cont yield exactly one activation per pump:0 |
| C-M3 | -6 | FAIL cont interleave completion deadline:0 |
| C-M4 | -6 | FAIL cont interleave completion deadline:0 |
| C-M5a | -6 | antfarm fatal: private child result: unwritten slot |
| C-M5b | -6 | FAIL cont old activation table still pins cohort after resume:0 |
| C-M6 | -6 | FAIL cont interleave completion deadline:0 |
| C-M7 | -6 | FAIL cont final stored destructor on consumer:0 |
| C-M7b | -6 | FAIL private_actor_tests/main.d:6436 |
| C-M8 | 66 | WARNING: ThreadSanitizer: data race (pid=8740) |
| C-M8b | -6 | FAIL cont admission retry deadline:0 |
| C-M9a | 66 | WARNING: ThreadSanitizer: data race (pid=8767) |
| C-M9b | 66 | WARNING: ThreadSanitizer: heap-use-after-free (pid=8789) |
| C-M10 | -6 | antfarm fatal: private child result: slot written twice |
| C-M11 | -6 | antfarm fatal: private child credit underflow |
| C-M12 | -6 | FAIL cont first CAS retry completed before drain:0 |
| C-M13 | -6 | antfarm fatal: private resume FIFO invariant |
| C-M14 | -6 | FAIL cont FIFO accepted prefix returned before writeTracked returns:0 |

Follow-up C4 forcing correction: affected C-M3/C-M4/C-M7/C-M7b all rechecked/caught on final test source. C-M7/C-M7b also caught directly by the dedicated forced emitter-seal selector. See late-owner-investigation/mutations/, seal-mutations/, and README.md for exact commands, patches and outcomes.

After verifier task 148's CAS publication-barrier fix, C-M12 was re-run and still caught: exit -6, `FAIL cont first CAS retry completed before drain:0`, 20.605 seconds. Its current exact patch/log and `outcomes-latest.json` replace the pre-revision C-M12 artifacts; `outcomes-all.json` retains the original full matrix. Canonical runtime restored to SHA256 cde26e9eae2af02f24685784ccd0d44f1204bf38e2505538dba4b64c0b0b6ad9.
