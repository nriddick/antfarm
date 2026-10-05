Follow-up correction and final validation: [late-owner-investigation/README.md](late-owner-investigation/README.md). The initial validation below did not expose the emitter-owner test deadlock; it is retained as historical evidence.

# Initial canonical validation (before C4 forcing correction)

All checks below passed after the final mutation restore. `canonical-build.log` records all five builds. `environment.log` records compiler versions, branch/base and hashes; its test-source hash matches the final source.

| Binary / selector | Exit | Elapsed |
|---|---:|---:|
| private_actor_tests-cont | 0 | 0.044 s |
| private_actor_tests-death-negative-control | 0 | 0.109 s |
| private_actor_tests-death | 0 | 5.411 s |
| private_actor_tests-suite | 0 | 0.819 s |
| private_actor_tests_dmd-cont | 0 | 0.118 s |
| private_actor_tests_dmd-death-negative-control | 0 | 0.090 s |
| private_actor_tests_dmd-death | 0 | 4.517 s |
| private_actor_tests_dmd-suite | 0 | 1.311 s |
| private_actor_tests_dmd_release-cont | 0 | 0.114 s |
| private_actor_tests_dmd_release-death-negative-control | 0 | 0.095 s |
| private_actor_tests_dmd_release-death | 0 | 4.029 s |
| private_actor_tests_release-cont | 0 | 0.044 s |
| private_actor_tests_release-death-negative-control | 0 | 0.100 s |
| private_actor_tests_release-death | 0 | 4.796 s |
| private_actor_tests_tsan-cont | 0 | 0.365 s |
| private_actor_tests_tsan-suite | 0 | 1.477 s |

The full private TSan suite took 1.477 seconds; the continuation-only TSan suite took 0.365 seconds. An additional 20 continuation repetitions per compiler passed: LDC 0.895 seconds, DMD 2.335 seconds, TSan 7.177 seconds. Complete commands/output are in the individual logs and `continuation-repeat.log`.

All 18 C-M mutations, including every required suffix, are caught. See `mutation-summary.md`, `mutations/outcomes-all.json`, and each exact `.patch` / `.log`. Earlier weak-barrier survivors of M5b and M12 remain archived and explained in README; final records show both caught.

No commits were made. Runtime was restored exactly to the parent's frozen canonical spike (SHA256 34abc0bc615910d574308c91c1d633f995f59d8aa45bed20d08f01059145929d). No erased callback, temporary layout change, or mutation remains. `git diff --check` passes.

Parent-owned remaining gates: full make check, check-soak, ten-minute TSan soak, docs/performance, independent review integration, and commits. These short validation runs are not claimed as the ten-minute soak.

These are the original implementation results. The verifier-revision compile controls, forced schedule, and refreshed private suites are recorded in force-cas-stall.md; the refreshed project and soak gates are in the parent evidence README.
