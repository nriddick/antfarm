/* Linux lifecycle comparison; no callbacks/waves are timed. Same state bytes,
   allocator and admission capacity for D scalar, C scalar and C batch.
   Usage: actor_cabi d|c|batch 64|4096 creators batch rounds remote mimalloc count seed
   count=1 measures allocation counts separately; never use its timings. */
#define _GNU_SOURCE
#include "antfarm_actors.h"
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mimalloc.h"
extern void *bench_d_runtime(af_farm *, size_t, af_actor_allocator);
extern void bench_d_create(void *, const void *, size_t, af_actor_owner *, size_t);
extern void bench_d_reclaim(void *, af_actor_owner *, size_t);
extern void bench_d_destroy(void *);
#define CHECK(c) do { if (!(c)) { fprintf(stderr, "failure %d: %s\n", __LINE__, #c); abort(); } } while (0)
typedef struct config {
    const char *mode; size_t bytes, creators, batch, rounds;
    int remote, mimalloc, counted, cpus[128]; size_t cpu_count;
    _Atomic size_t allocations, frees, allocated_bytes;
    pthread_barrier_t start;
    void *d; af_actor_runtime *c; const af_actor_kind *kind;
} config;
typedef struct __attribute__((aligned(64))) worker {
    config *c; size_t id; af_actor_owner *owners; unsigned char *initial;
    uint32_t *results; _Atomic unsigned phase; /* 0=creator, 1=reclaimer, 2=done */
} worker;
static void *allocate_state(void *user, size_t bytes, size_t alignment) {
    config *c = user; void *p = NULL;
    if (c->mimalloc) p = mi_malloc_aligned(bytes, alignment);
    else if (posix_memalign(&p, alignment < sizeof(void *) ? sizeof(void *) : alignment, bytes)) p = NULL;
    if (c->counted && p) { atomic_fetch_add(&c->allocations, 1); atomic_fetch_add(&c->allocated_bytes, bytes); }
    return p;
}
static void free_state(void *user, void *p, size_t bytes, size_t alignment) {
    config *c = user; (void)bytes; (void)alignment;
    if (c->counted) atomic_fetch_add(&c->frees, 1);
    if (c->mimalloc) mi_free(p); else free(p);
}
static void idle(void *u, void *s, size_t n, af_actor_context *ctx) { (void)u; (void)s; (void)n; (void)ctx; }
static void pin(config *c, size_t index) {
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(c->cpus[index], &set);
    CHECK(pthread_setaffinity_np(pthread_self(), sizeof set, &set) == 0);
}
static void create(worker *w) {
    config *c = w->c;
    if (!strcmp(c->mode, "d")) bench_d_create(c->d, w->initial, c->bytes, w->owners, c->batch);
    else if (!strcmp(c->mode, "c"))
        for (size_t i = 0; i < c->batch; ++i)
            CHECK(af_actor_create_copy(c->c, c->kind, w->initial + i * c->bytes, w->owners + i) == AF_OK);
    else {
        af_actor_state_span source = {w->initial, c->bytes, c->batch};
        af_actor_owner_output_span owners = {w->owners, sizeof *w->owners, c->batch};
        af_actor_batch_result r = af_actor_create_copy_batch(c->c, c->kind, source, owners);
        CHECK(r.status == AF_OK && r.count == c->batch);
    }
}
static void reclaim(worker *w) {
    config *c = w->c;
    if (!strcmp(c->mode, "d")) bench_d_reclaim(c->d, w->owners, c->batch);
    else if (!strcmp(c->mode, "c"))
        for (size_t i = 0; i < c->batch; ++i) {
            CHECK(af_actor_request_retire(c->c, w->owners + i, AF_ACTOR_DRAIN) == AF_ACTOR_RETIRE_REQUESTED);
            CHECK(af_actor_reclaim(c->c, w->owners + i) == AF_ACTOR_RECLAIMED);
        }
    else {
        af_actor_owner_span input = {w->owners, sizeof *w->owners, c->batch};
        af_actor_owner_output_span owners = {w->owners, sizeof *w->owners, c->batch};
        af_actor_result_span results = {w->results, sizeof *w->results, c->batch};
        af_actor_batch_result r = af_actor_request_retire_batch(c->c, input, AF_ACTOR_DRAIN, results);
        CHECK(r.status == AF_OK && r.count == c->batch);
        r = af_actor_reclaim_batch(c->c, owners, results);
        CHECK(r.status == AF_OK && r.count == c->batch);
    }
}
static void wait_phase(worker *w, unsigned value) {
    while (atomic_load_explicit(&w->phase, memory_order_acquire) != value) __asm__ volatile("pause");
}
static void *producer(void *user) {
    worker *w = user; config *c = w->c; pin(c, w->id);
    pthread_barrier_wait(&c->start);
    for (size_t i = 0; i < c->rounds; ++i) {
        if (c->remote) wait_phase(w, 0);
        create(w);
        if (c->remote) atomic_store_explicit(&w->phase, 1, memory_order_release);
        else reclaim(w);
    }
    if (c->remote) { wait_phase(w, 0); atomic_store_explicit(&w->phase, 2, memory_order_release); }
    return NULL;
}
static void *remote_reclaimer(void *user) {
    worker *w = user; config *c = w->c; pin(c, w->id + c->creators);
    pthread_barrier_wait(&c->start);
    for (;;) {
        unsigned phase = atomic_load_explicit(&w->phase, memory_order_acquire);
        if (phase == 2) break;
        if (phase == 1) { reclaim(w); atomic_store_explicit(&w->phase, 0, memory_order_release); }
        else __asm__ volatile("pause");
    }
    return NULL;
}
static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec * 1e-9; }
int main(int argc, char **argv) {
    CHECK(argc == 10); config c = {0}; c.mode = argv[1]; c.bytes = strtoull(argv[2], NULL, 10);
    c.creators = strtoull(argv[3], NULL, 10); c.batch = strtoull(argv[4], NULL, 10);
    c.rounds = strtoull(argv[5], NULL, 10); c.remote = atoi(argv[6]); c.mimalloc = atoi(argv[7]); c.counted = atoi(argv[8]);
    unsigned seed = (unsigned)strtoul(argv[9], NULL, 10);
    CHECK(c.bytes && c.creators && c.batch && c.rounds && c.creators <= 32);
    cpu_set_t allowed; CHECK(sched_getaffinity(0, sizeof allowed, &allowed) == 0);
    for (int i = 0; i < CPU_SETSIZE && c.cpu_count < 128; ++i) if (CPU_ISSET(i, &allowed)) c.cpus[c.cpu_count++] = i;
    CHECK(c.cpu_count >= c.creators * (c.remote ? 2 : 1));
    af_options options; af_options_default(&options); options.ring_mib = 2;
    af_farm *farm = NULL; CHECK(af_create(&options, &farm) == AF_OK);
    af_actor_allocator allocator = {&c, allocate_state, free_state};
    if (!strcmp(c.mode, "d")) c.d = bench_d_runtime(farm, c.creators * c.batch, allocator);
    else {
        CHECK(!strcmp(c.mode, "c") || !strcmp(c.mode, "batch"));
        CHECK(af_actor_runtime_create(farm, c.creators * c.batch, &allocator, &c.c) == AF_OK);
        af_actor_kind_options kind;
        af_actor_kind_options_default(&kind);
        kind.handler = idle;
        kind.state_bytes = c.bytes;
        kind.state_alignment = 64;
        CHECK(af_actor_kind_register(c.c, &kind, &c.kind) == AF_OK);
    }
    atomic_store(&c.allocations, 0); atomic_store(&c.frees, 0); atomic_store(&c.allocated_bytes, 0);
    worker *workers = NULL;
    CHECK(posix_memalign((void **)&workers, 64, c.creators * sizeof *workers) == 0);
    memset(workers, 0, c.creators * sizeof *workers);
    pthread_t *threads = calloc(c.creators * 2, sizeof *threads); CHECK(workers && threads);
    CHECK(pthread_barrier_init(&c.start, NULL, (unsigned)(1 + c.creators * (c.remote ? 2 : 1))) == 0);
    for (size_t i = 0; i < c.creators; ++i) {
        worker *w = workers + i; w->c = &c; w->id = i;
        w->owners = calloc(c.batch, sizeof *w->owners); w->results = calloc(c.batch, sizeof *w->results);
        w->initial = malloc(c.bytes * c.batch); CHECK(w->owners && w->results && w->initial);
        memset(w->initial, (int)(seed % 251), c.bytes * c.batch);
        CHECK(pthread_create(threads + i, NULL, producer, w) == 0);
        if (c.remote) CHECK(pthread_create(threads + c.creators + i, NULL, remote_reclaimer, w) == 0);
    }
    double start = now(); pthread_barrier_wait(&c.start);
    for (size_t i = 0; i < c.creators * (c.remote ? 2 : 1); ++i) CHECK(pthread_join(threads[i], NULL) == 0);
    double elapsed = now() - start; size_t total = c.creators * c.batch * c.rounds;
    size_t allocations = atomic_load(&c.allocations), frees = atomic_load(&c.frees), bytes = atomic_load(&c.allocated_bytes);
    if (c.counted) CHECK(allocations == total && frees == total && bytes == total * c.bytes);
    printf("{\"mode\":\"%s\",\"bytes\":%zu,\"creators\":%zu,\"batch\":%zu,\"remote\":%d,\"mimalloc\":%d,\"counted\":%d,\"actors\":%zu,\"seconds\":%.6f,\"ns_actor\":%.3f,\"allocations\":%zu,\"frees\":%zu,\"allocated_bytes\":%zu,\"logical_copies\":%zu}\n",
        c.mode, c.bytes, c.creators, c.batch, c.remote, c.mimalloc, c.counted, total, elapsed, elapsed * 1e9 / total, allocations, frees, bytes, total);
    if (c.d) bench_d_destroy(c.d); else { CHECK(!af_actor_runtime_live(c.c)); af_actor_runtime_destroy(c.c); }
    af_destroy(farm);
    for (size_t i = 0; i < c.creators; ++i) { free(workers[i].owners); free(workers[i].results); free(workers[i].initial); }
    pthread_barrier_destroy(&c.start); free(workers); free(threads); return 0;
}
