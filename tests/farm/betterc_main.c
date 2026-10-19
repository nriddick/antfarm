/* Native C main: link without druntime and call back through the actual ABI. */
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
typedef void (*callback)(const uint64_t *, size_t);
typedef struct hook { void *context; void (*call)(void *); } hook;
_Static_assert(sizeof(hook) == 16, "hook size");
_Static_assert(offsetof(hook, context) == 0, "context offset");
_Static_assert(offsetof(hook, call) == 8, "callback offset");
extern int antfarm_test_c(callback, hook *);
static uint64_t sum;
static unsigned calls;
static void work(const uint64_t *body, size_t length) {
    for (size_t i = 0; i < length; ++i) sum += body[i];
    ++calls;
}
static void joined(void *context) {
    unsigned *notifications = context;
    ++*notifications;
    assert(calls == 3 * *notifications);
    assert(sum == (*notifications == 1 ? 12u : *notifications == 2 ? 24u : 30u));
}
int main(void) {
    unsigned notifications = 0;
    hook done = { &notifications, joined };
    assert(antfarm_test_c(work, &done) == 0);
    assert(notifications == 3 && calls == 9 && sum == 30);
    puts("PASS native C main: BetterC core, C callbacks and hooks, no druntime");
    return 0;
}
