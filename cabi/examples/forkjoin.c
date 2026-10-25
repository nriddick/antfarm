/* cc -std=c11 -Iinclude -c cabi/examples/forkjoin.c -o forkjoin.o
   ldc2 forkjoin.o libantfarm_runtime.a libantfarm_core.a -of=forkjoin */
#include "antfarm_forkjoin.h"
#include <stdio.h>
#include <stdlib.h>
static void check(af_status s) {
    if (s != AF_OK) { fprintf(stderr, "%s\n", af_last_error_message()); exit(1); }
}
static void leaf(void *ctx, size_t begin, size_t end, const void *seed, void *out) {
    (void)ctx;
    uint64_t sum = *(const uint64_t *)seed;
    for (size_t i = begin; i < end; ++i) sum += i;
    *(uint64_t *)out = sum;
}
static void join(void *ctx, const void *left, const void *right, void *out) {
    (void)ctx;
    *(uint64_t *)out = *(const uint64_t *)left + *(const uint64_t *)right;
}
int main(void) {
    check(af_druntime_init());
    af_dispatcher *d = NULL;
    check(af_dispatcher_start(NULL, NULL, 0, &d));
    uint64_t seed = 0, sum = 0;
    check(af_forkjoin_reduce(d, 0, 1000000, 4096, sizeof sum, &seed, leaf, join, NULL, &sum));
    printf("sum = %llu\n", (unsigned long long)sum);
    check(af_dispatcher_stop(d));
    af_dispatcher_destroy(d);
    check(af_druntime_shutdown());
    return sum == UINT64_C(499999500000) ? 0 : 1;
}
