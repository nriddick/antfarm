// SPDX-License-Identifier: MIT (see forkjoin/LICENSE)
/// `dub test :forkjoin`: the deque exactly-once test and the dispatcher
/// admission test. Prints `passed` on success.
module forkjoin_test;

import deque_test;
import admission_test;
import paths_test;
import core.stdc.stdio : printf;
import core.stdc.stdlib : exit;

void main()
{
    if (!runDequeTest()) { printf("FAILED deque_test\n"); exit(1); }
    if (!runAdmissionTest(8)) { printf("FAILED admission_test\n"); exit(1); }
    if (!runPathsTest()) { printf("FAILED paths_test\n"); exit(1); }
    printf("passed\n");
}
