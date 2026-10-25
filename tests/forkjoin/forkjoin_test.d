// SPDX-License-Identifier: MIT
/// `dub test :forkjoin`: the deque exactly-once test and the dispatcher
/// admission test. Prints `passed` on success.
module forkjoin_test;

// The admission probe measures whole-process CPU while dispatcher workers
// are parked. On Windows, the supported druntime's parallel GC helpers can
// keep consuming CPU after the deque test's large allocations, even with
// every pool worker asleep. Keep those unrelated helpers out of this test
// executable's idle measurement; this does not change the library's GC policy.
version (Windows)
    extern(C) __gshared string[] rt_options = ["gcopt=parallel:0"];

import deque_test;
import admission_test;
import paths_test;
import range_test;
import completion_test;
import frames_test;
import features_test;
import composed_test;
import core.stdc.stdio : printf;
import core.stdc.stdlib : exit;

void main()
{
    if (!runDequeTest()) { printf("FAILED deque_test\n"); exit(1); }
    if (!runAdmissionTest(8)) { printf("FAILED admission_test\n"); exit(1); }
    if (!runPathsTest()) { printf("FAILED paths_test\n"); exit(1); }
    if (!runRangeTest()) { printf("FAILED range_test\n"); exit(1); }
    if (!runCompletionTest()) { printf("FAILED completion_test\n"); exit(1); }
    if (!runFramesTest()) { printf("FAILED frames_test\n"); exit(1); }
    if (!runFeaturesTest()) { printf("FAILED features_test\n"); exit(1); }
    if (!runComposedTest()) { printf("FAILED composed_test\n"); exit(1); }
    printf("passed\n");
}
