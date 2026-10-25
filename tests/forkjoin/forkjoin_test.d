// SPDX-License-Identifier: MIT
/// `dub test :forkjoin`: the deque exactly-once test and the dispatcher
/// admission test. Prints `passed` on success.
module forkjoin_test;

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
