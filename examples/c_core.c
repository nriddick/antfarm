/* cc -std=c11 -Iinclude examples/c_core.c libantfarm_core.a -pthread */
#include "antfarm.h"
#include <stdio.h>
#include <stdlib.h>
static unsigned finished;
static uint64_t sum;
static void add(const uint64_t *body, size_t words) {
    for (size_t i = 0; i < words; ++i) sum += body[i];
}
static void complete(void *context) { *(unsigned *)context = 1; }
static void check(af_status status) {
    if (status != AF_OK) { fprintf(stderr, "%s\n", af_last_error_message()); exit(1); }
}
int main(void) {
    af_farm *farm = NULL; af_token *token = NULL; af_view *view = NULL;
    check(af_create(NULL, &farm));
    check(af_register_producer(farm, AF_SMALL, &token));
    check(af_view_create(&view)); check(af_subscribe(view, farm));
    const uint64_t values[] = {2, 3, 5};
    const af_completion_hook hook = {&finished, complete};
    if (af_write_fixed(farm, add, values, 1, 3, token, AF_DEFAULT_COST, &hook) != 3) return 1;
    while (!finished) af_consume_next(view);
    printf("sum = %llu\n", (unsigned long long)sum);
    af_unsubscribe(view); af_view_destroy(view);
    af_unregister_producer(farm, token); af_destroy(farm);
    return sum == 10 ? 0 : 1;
}
