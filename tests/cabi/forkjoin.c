#include "antfarm_forkjoin.h"
#include "threads.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static af_dispatcher *dispatch;
static void ok(af_status s) { if (s) { fprintf(stderr, "%u: %s\n", s, af_last_error_message()); abort(); } }
static void fatal_notice(const char *s, size_t n, void *ctx) {
    (void)ctx; fprintf(stderr, "C_FATAL_OBSERVER: %.*s\n", (int)n, s); fflush(stderr);
}
typedef struct fib { af_node node; struct fib *kids; uint64_t value; unsigned n; } fib;
static af_node *fib_run(af_node *);
static af_node *fib_join(af_node *node) {
    fib *f = (fib *)node;
    f->value = f->kids[0].value + f->kids[1].value;
    free(f->kids); f->kids = NULL; return NULL;
}
static void init_fib(fib *f, unsigned n) { memset(f, 0, sizeof *f); f->n = n; af_node_init(&f->node, fib_run, fib_join, NULL); }
static af_node *fib_run(af_node *node) {
    fib *f = (fib *)node;
    if (f->n < 2) { f->value = f->n; return af_complete(node); }
    f->kids = (fib *)malloc(2 * sizeof(fib)); CHECK(f->kids);
    init_fib(&f->kids[0], f->n - 1); init_fib(&f->kids[1], f->n - 2);
    af_node *kids[2] = {&f->kids[0].node, &f->kids[1].node};
    return af_fork(node, kids, 2);
}
static af_node *leaf_run(af_node *n) { if (n->context) test_add((test_atomic *)n->context, 1); return af_complete(n); }
static void visit(void *p, size_t i) { ((unsigned *)p)[i]++; }
static void reduce_leaf(void *ctx, size_t b, size_t e, const void *seed, void *out) {
    (void)ctx; uint64_t v = *(const uint64_t *)seed;
    for (size_t i = b; i < e; ++i) v = v * UINT64_C(6364136223846793005) + i;
    *(uint64_t *)out = v;
}
static void reduce_join(void *ctx, const void *a, const void *b, void *out) {
    (void)ctx; *(uint64_t *)out = *(const uint64_t *)a * 17 + *(const uint64_t *)b;
}
static uint64_t reference(size_t b, size_t e, size_t grain, uint64_t seed) {
    if (e-b <= grain) { uint64_t r; reduce_leaf(NULL,b,e,&seed,&r); return r; }
    size_t m=b+(e-b)/2; return reference(b,m,grain,seed)*17+reference(m,e,grain,seed);
}
static void ranges(void) {
    unsigned visits[1003] = {0};
    ok(af_forkjoin_for(dispatch, 3, 1003, 7, visit, visits));
    for (size_t i=0;i<1003;++i) CHECK(visits[i] == (i>=3));
    uint64_t seed=11, out=0;
    ok(af_forkjoin_reduce(dispatch,3,1003,7,sizeof out,&seed,reduce_leaf,reduce_join,NULL,&out));
    CHECK(out == reference(3,1003,7,seed));
    ok(af_forkjoin_reduce(dispatch,8,8,7,sizeof out,&seed,reduce_leaf,reduce_join,NULL,&out)); CHECK(out==seed);
    ok(af_forkjoin_reduce(dispatch,3,1003,7,sizeof seed,&seed,reduce_leaf,reduce_join,NULL,&seed));
    CHECK(seed == reference(3,1003,7,11));
    CHECK(af_forkjoin_for(dispatch,0,1,0,visit,visits)==AF_INVALID);
    CHECK(af_forkjoin_for(dispatch,5,4,1,visit,visits)==AF_INVALID);
    CHECK(af_forkjoin_reduce(dispatch,0,1,1,SIZE_MAX,&seed,reduce_leaf,reduce_join,NULL,&out)==AF_INVALID);
}
static af_node *nested_run(af_node *n) {
    ranges(); fib f; init_fib(&f,12); ok(af_run_root(dispatch,&f.node)); CHECK(f.value==144);
    return af_complete(n);
}
typedef struct flat { af_node node; af_node children[300]; } flat;
static af_node *flat_join(af_node *n) { (void)n; return NULL; }
static af_node *flat_run(af_node *n) {
    flat *f=(flat *)n; af_node *kids[300];
    for (size_t i=0;i<300;++i) { af_node_init(&f->children[i],nested_run,NULL,NULL); kids[i]=&f->children[i]; }
    return af_fork(n,kids,300);
}
typedef struct phases { af_node node, child; unsigned rounds; } phases;
static af_node *phase_run(af_node *n) {
    phases *p=(phases *)n; af_node_init(&p->child,leaf_run,NULL,NULL);
    af_node *kids[1]={&p->child}; return af_fork(n,kids,1);
}
static af_node *phase_join(af_node *n) {
    phases *p=(phases *)n;
    if (++p->rounds==5) return NULL;
    return phase_run(n); /* join returns a continuation after another fork */
}
TEST_THREAD(rejected_producer) {
    (void)arg; ok(af_druntime_attach_thread());
    af_root_completion *c; ok(af_root_completion_create(&c));
    af_node n; af_node_init(&n,leaf_run,NULL,NULL);
    CHECK(af_submit_root(dispatch,&n,c)==AF_INVALID);
    CHECK(af_submit_detached(dispatch,&n)==AF_INVALID);
    CHECK(af_run_root(dispatch,&n)==AF_INVALID);
    CHECK(af_dispatcher_stop(dispatch)==AF_INVALID);
    ok(af_root_completion_destroy(c)); ok(af_druntime_detach_thread()); TEST_RETURN;
}
static af_node detached[128], late_root;
static af_root_completion *late_completion;
static test_atomic finished;
TEST_THREAD(producer) {
    (void)arg;
    af_root_completion *c=(af_root_completion *)(uintptr_t)1;
    CHECK(af_root_completion_create(&c)==AF_INVALID && c==NULL);
    ok(af_druntime_attach_thread()); ok(af_root_completion_create(&c));
    CHECK(af_wait_root(c)==AF_INVALID);
    for (unsigned i=0;i<4000;++i) {
        af_node leaf; af_node_init(&leaf,leaf_run,NULL,&finished);
        ok(af_submit_root(dispatch,&leaf,c));
        CHECK(af_submit_root(dispatch,&leaf,c)==AF_BUSY);
        CHECK(af_root_completion_destroy(c)==AF_BUSY);
        ok(af_wait_root(c));
    }
    ranges();
    fib f; init_fib(&f,20); ok(af_submit_root(dispatch,&f.node,c)); ok(af_wait_root(c)); CHECK(f.value==6765);
    ok(af_root_completion_destroy(c));
    for (unsigned i=0;i<128;++i) { af_node_init(&detached[i],leaf_run,NULL,&finished); ok(af_submit_detached(dispatch,&detached[i])); }
    ok(af_root_completion_create(&late_completion)); af_node_init(&late_root,leaf_run,NULL,&finished);
    ok(af_submit_root(dispatch,&late_root,late_completion));
    ok(af_druntime_detach_thread()); TEST_RETURN;
}
static const char *death_case;
static af_node *bad_run(af_node *n) {
    if (!strcmp(death_case,"zero")) return af_fork(n,NULL,0);
    if (!strcmp(death_case,"double")) { (void)af_complete(n); return af_complete(n); }
    if (!strcmp(death_case,"stop")) { af_dispatcher_stop(dispatch); return NULL; }
    af_node *kids[1]={n}; return af_fork(n,kids,1);
}
static af_node *bad_join(af_node *n) { return af_complete(n); }
static af_node *join_death_run(af_node *n) {
    static af_node child; af_node_init(&child,leaf_run,NULL,NULL);
    af_node *kids[1]={&child}; return af_fork(n,kids,1);
}
int main(int argc,char **argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
    af_dispatcher *bad=(af_dispatcher *)(uintptr_t)1;
    CHECK(af_dispatcher_start(NULL,NULL,0,&bad)==AF_INVALID && bad==NULL);
    ok(af_druntime_init()); af_set_fatal_handler(fatal_notice,NULL);
    size_t count; ok(af_processors(NULL,0,&count)); CHECK(count>0);
    af_processor *cpus=(af_processor *)malloc(count*sizeof *cpus); CHECK(cpus);
    if (count>1) { size_t needed; CHECK(af_processors(cpus,1,&needed)==AF_FULL && needed==count); }
    ok(af_processors(cpus,count,&count));
    af_dispatcher_options o; af_dispatcher_options_default(&o);
    o.body_steps=0; CHECK(af_dispatcher_start(&o,cpus,1,&bad)==AF_INVALID && bad==NULL);
    af_dispatcher_options_default(&o); o.deque_capacity=64; o.ring_mib=2;
    if (argc>1) {
        death_case=argv[1]; ok(af_dispatcher_start(&o,cpus,1,&dispatch));
        af_node n; af_node_init(&n,bad_run,flat_join,NULL);
        if (!strcmp(death_case,"outside")) af_complete(&n);
        if (!strcmp(death_case,"join")) af_node_init(&n,join_death_run,bad_join,NULL);
        if (!strcmp(death_case,"destroy")) af_dispatcher_destroy(dispatch);
        if (!strcmp(death_case,"uninitialized")) memset(&n,0,sizeof n);
        af_run_root(dispatch,&n); return 0;
    }
    for (size_t threads=1;threads<=count && threads<=4;threads*=2) {
        o.farm_per_group = threads>1;
        ok(af_dispatcher_start(&o,cpus,threads,&dispatch));
        CHECK(af_dispatcher_start(NULL,NULL,0,&bad)==AF_BUSY && bad==NULL);
        CHECK(af_druntime_shutdown()==AF_BUSY);
        af_dispatcher_stats stats; CHECK(af_dispatcher_counters(dispatch,&stats)==AF_BUSY);
        fib f; init_fib(&f,22); ok(af_run_root(dispatch,&f.node)); CHECK(f.value==17711);
        ranges(); flat tree; af_node_init(&tree.node,flat_run,flat_join,NULL); ok(af_run_root(dispatch,&tree.node));
        phases phases_root; memset(&phases_root,0,sizeof phases_root);
        af_node_init(&phases_root.node,phase_run,phase_join,NULL);
        ok(af_run_root(dispatch,&phases_root.node)); CHECK(phases_root.rounds==5);
        af_root_completion *c; ok(af_root_completion_create(&c));
        CHECK(af_submit_root(dispatch,&f.node,c)==AF_INVALID); ok(af_root_completion_destroy(c));
        if (threads>1) {
            test_store(&finished,0); test_thread t=test_start(producer,NULL); test_join(t);
        } else {
            test_thread t=test_start(rejected_producer,NULL); test_join(t);
        }
        ok(af_dispatcher_stop(dispatch)); ok(af_dispatcher_stop(dispatch));
        ok(af_dispatcher_counters(dispatch,&stats));
        if (threads>1) {
            CHECK(test_load(&finished)==4129); ok(af_wait_root(late_completion)); ok(af_root_completion_destroy(late_completion));
        }
        af_dispatcher_destroy(dispatch); dispatch=NULL;
        printf("C forkjoin passed: %zu participants, deterministic fold, nested overflow, foreign roots\n",threads);
    }
    if (count>1) {
        o.caller_consumes=1; ok(af_dispatcher_start(&o,cpus,2,&dispatch));
        test_thread t=test_start(rejected_producer,NULL); test_join(t);
        ok(af_dispatcher_stop(dispatch)); af_dispatcher_destroy(dispatch);
    }
    free(cpus); ok(af_druntime_shutdown()); return 0;
}
