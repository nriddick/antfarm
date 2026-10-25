#include "antfarm_groups.h"
#include "threads.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static void ok(af_status s) { if (s) { fprintf(stderr,"%u: %s\n",s,af_last_error_message()); abort(); } }
typedef struct worker_state { af_view *view; unsigned calls; } worker_state;
static worker_state workers[128];
static test_atomic hits, starts, stops, ready, release_foreign;
static af_group_host *host;
static uint32_t home;
static void job(const uint64_t *p,size_t n) { CHECK(n==1 && p[0]==42); test_add(&hits,1); }
static uint32_t body(af_group_host *h,uint32_t index,uint32_t llc,void *context) {
    CHECK(context==workers && index<128);
    worker_state *s=&workers[index];
    if (!s->view) {
        af_farm *f; ok(af_group_farm(h,llc,&f));
        ok(af_view_create(&s->view)); ok(af_subscribe(s->view,f));
        af_token *a,*b; ok(af_group_token(h,llc,&a)); ok(af_group_token(h,llc,&b)); CHECK(a==b);
        test_add(&starts,1);
    }
    ++s->calls;
    return af_consume_next(s->view);
}
static void stop(af_group_host *h,uint32_t index,uint32_t llc,void *context) {
    (void)h; (void)llc; CHECK(context==workers);
    if (workers[index].view) { af_unsubscribe(workers[index].view); af_view_destroy(workers[index].view); workers[index].view=NULL; }
    test_add(&stops,1);
}
TEST_THREAD(foreign) {
    (void)arg;
    ok(af_druntime_attach_thread()); af_token *token;
    CHECK(af_group_stop_workers(host)==AF_INVALID);
    ok(af_group_token(host,home,&token));
    CHECK(af_druntime_detach_thread()==AF_BUSY);
    test_store(&ready,1);
    while (!test_load(&release_foreign)) test_yield();
    ok(af_group_release_tokens(host)); ok(af_druntime_detach_thread()); TEST_RETURN;
}
static void exercise(unsigned nworkers) {
    memset(workers,0,sizeof workers); test_store(&hits,0); test_store(&starts,0); test_store(&stops,0);
    af_group_options o; af_group_options_default(&o);
    o.host.workers=nworkers; o.host.extra_consumers=1; o.host.extra_small_producers=2;
    o.host.idle_timeout_ns=AF_WAIT_FOREVER; o.host.ring_mib=2;
    o.group_mode=AF_GROUP_ALL;
    size_t count; ok(af_group_resolve(&o,NULL,0,&count)); CHECK(count==nworkers);
    af_processor cpus[128]; ok(af_group_resolve(&o,cpus,128,&count));
    ok(af_group_start(&o,body,stop,workers,&host));
    /* Pool startup publishes the workers' go flag, not a user-body barrier.
       Exercise their resource cleanup only after each body actually entered. */
    while (test_load(&starts)<nworkers) { ok(af_group_wake(host,UINT32_MAX)); test_yield(); }
    af_group_host *rejected=(af_group_host *)(uintptr_t)1;
    CHECK(af_group_start(&o,body,stop,workers,&rejected)==AF_BUSY && rejected==NULL);
    CHECK(af_group_close(host)==AF_BUSY); CHECK(af_druntime_shutdown()==AF_BUSY);
    ok(af_group_caller_llc(host,&home));
    af_farm *f=(af_farm *)(uintptr_t)1;
    CHECK(af_group_farm(host,UINT32_MAX,&f)==AF_INVALID && f==NULL);
    ok(af_group_farm(host,home,&f));
    af_token *t,*again; ok(af_group_token(host,home,&t)); ok(af_group_token(host,home,&again)); CHECK(t==again);
    af_view *view; ok(af_view_create(&view)); ok(af_subscribe(view,f));
    uint64_t data[4096]; for (size_t i=0;i<4096;++i) data[i]=42;
    for (size_t offset=0;offset<4096;) {
        offset+=af_write_fixed(f,job,data+offset,1,4096-offset,t,7,NULL);
        ok(af_group_wake(host,home)); af_consume_next(view);
    }
    while (test_load(&hits)<4096) { af_consume_next(view); ok(af_group_wake(host,UINT32_MAX)); }
    /* A separate attached thread's cached token keeps Farms alive after workers stop. */
    test_store(&ready,0); test_store(&release_foreign,0);
    test_thread thread=test_start(foreign,NULL);
    while (!test_load(&ready)) test_yield();
    ok(af_group_stop_workers(host)); ok(af_group_stop_workers(host));
    CHECK(test_load(&stops)==nworkers);
    CHECK(af_group_close(host)==AF_BUSY); /* external view and foreign token */
    af_unsubscribe(view); af_view_destroy(view);
    CHECK(af_group_close(host)==AF_BUSY);
    test_store(&release_foreign,1); test_join(thread);
    ok(af_group_close(host)); host=NULL;
    printf("C GroupHost passed: %u workers, borrowed resources, token cache, stop/close\n",nworkers);
}
int main(void) {
    ok(af_druntime_init());
    af_group_options o; af_group_options_default(&o);
    o.host.workers=100000; af_group_host *bad=(af_group_host *)(uintptr_t)1;
    CHECK(af_group_start(&o,body,stop,workers,&bad)==AF_INVALID && bad==NULL);
    af_group_options_default(&o); o.reserved=1;
    size_t count=99; CHECK(af_group_resolve(&o,NULL,0,&count)==AF_INVALID && count==0);
    exercise(0); exercise(2);
    /* An immediate stop may precede a worker's first body. Such a worker has
       no user resources to clean up and need not call the user stop hook. */
    af_group_options_default(&o); o.host.workers=2;
    for (unsigned i=0;i<8;++i) {
        memset(workers,0,sizeof workers); test_store(&starts,0); test_store(&stops,0);
        ok(af_group_start(&o,body,stop,workers,&host));
        ok(af_group_stop_workers(host)); CHECK(test_load(&stops)<=2);
        ok(af_group_close(host));
    }
    ok(af_druntime_shutdown()); return 0;
}
