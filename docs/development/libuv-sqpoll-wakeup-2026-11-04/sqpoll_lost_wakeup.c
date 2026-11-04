/* A lost SQPOLL wakeup in libuv's io_uring submit path.
 *
 * With UV_LOOP_USE_IO_URING_SQPOLL, libuv submits file operations to an
 * io_uring whose kernel thread goes to sleep after 10 ms idle. Before it
 * sleeps, the kernel sets IORING_SQ_NEED_WAKEUP and re-reads the queue's
 * tail; a submitter must publish its tail, then read the flag, with a full
 * fence between the two. libuv's uv__iou_submit stores the tail with
 * release ordering and loads the flag with acquire ordering, which does not
 * order a store before a later load on x86. So the submitter can read "no
 * wakeup needed" while its tail store is still invisible to the kernel
 * thread, which then sleeps with the entry unread. Nothing completes it
 * until another submission happens to wake the thread.
 *
 * This program submits one stat through the ring, waits for it, and sleeps
 * a random 9 to 12 ms before the next one, so submissions land all around
 * the moment the kernel thread decides to sleep, which it does 10 to 11 ms
 * after its last work (its idle time is counted in 1 ms jiffies). When a
 * stat has not completed after two seconds, it submits a second one: if
 * both then complete at once, the first had been sitting in the ring
 * unread, which is the lost wakeup. The window is narrow, so run several
 * copies at once and give them minutes; run.sh does.
 *
 *   cc -O1 sqpoll_lost_wakeup.c -luv -o sqpoll_lost_wakeup
 *   ./sqpoll_lost_wakeup [iterations] [min_sleep_us] [max_sleep_us]
 *
 * Exit 0: every stat completed in time. Exit 1: a stat completed only after
 * the second submission (the lost wakeup). Exit 2: a stat never completed.
 * Exit 3: the ring was not in use. Linux 5.10.186 or later; libuv 1.49 or
 * later built with io_uring; sets UV_USE_IO_URING=1 for itself.
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <uv.h>

static uv_loop_t loop;
static uv_timer_t watchdog;
static uv_fs_t first, second;
static int first_done, second_done, kicked;
static long iteration;
static struct timespec submitted_at, kicked_at;
static double first_done_ms, second_done_ms; /* since the second submission */

static double ms_since(const struct timespec* t)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (now.tv_sec - t->tv_sec) * 1e3 + (now.tv_nsec - t->tv_nsec) / 1e6;
}

static int sqpoll_threads(void)
{
    /* The kernel names a ring's polling thread iou-sqp-<pid>. */
    DIR* tasks = opendir("/proc/self/task");
    struct dirent* entry;
    int count = 0;
    if (tasks == NULL) return -1;
    while ((entry = readdir(tasks)) != NULL) {
        char path[sizeof "/proc/self/task//comm" + sizeof entry->d_name], comm[32] = "";
        FILE* f;
        if (entry->d_name[0] == '.') continue;
        snprintf(path, sizeof path, "/proc/self/task/%s/comm", entry->d_name);
        f = fopen(path, "r");
        if (f == NULL) continue;
        if (fgets(comm, sizeof comm, f) != NULL && strncmp(comm, "iou-sqp", 7) == 0) count++;
        fclose(f);
    }
    closedir(tasks);
    return count;
}

static void on_first(uv_fs_t* req)
{
    first_done = 1;
    uv_fs_req_cleanup(req);
    if (!kicked) {
        uv_timer_stop(&watchdog);
        return;
    }
    first_done_ms = ms_since(&kicked_at);
    if (second_done) uv_timer_stop(&watchdog);
}

static void on_second(uv_fs_t* req)
{
    second_done = 1;
    second_done_ms = ms_since(&kicked_at);
    uv_fs_req_cleanup(req);
    if (first_done) uv_timer_stop(&watchdog);
}

static void on_watchdog(uv_timer_t* timer)
{
    if (!kicked) {
        /* The first stat is 2 s overdue. Submit another: it reads the flag
         * the kernel thread set when it went to sleep, and wakes it. */
        kicked = 1;
        printf("iteration %ld: the stat has not completed after %.0f ms; submitting another\n",
               iteration, ms_since(&submitted_at));
        fflush(stdout);
        clock_gettime(CLOCK_MONOTONIC, &kicked_at);
        uv_fs_stat(&loop, &second, "/", on_second);
        uv_timer_start(timer, on_watchdog, 2000, 0);
        return;
    }
    uv_timer_stop(timer);
}

int main(int argc, char** argv)
{
    long iterations = argc > 1 ? atol(argv[1]) : 20000;
    long min_us = argc > 2 ? atol(argv[2]) : 9000;
    long max_us = argc > 3 ? atol(argv[3]) : 12000;
    int threads;

    srand((unsigned) (getpid() ^ time(NULL)));

    setenv("UV_USE_IO_URING", "1", 0);
    if (uv_loop_init(&loop) != 0) return 3;
    if (uv_loop_configure(&loop, UV_LOOP_USE_IO_URING_SQPOLL) != 0) {
        fprintf(stderr, "UV_LOOP_USE_IO_URING_SQPOLL not accepted: libuv %s\n", uv_version_string());
        return 3;
    }
    uv_timer_init(&loop, &watchdog);

    for (iteration = 1; iteration <= iterations; iteration++) {
        first_done = second_done = kicked = 0;
        clock_gettime(CLOCK_MONOTONIC, &submitted_at);
        if (uv_fs_stat(&loop, &first, "/", on_first) != 0) return 3;
        uv_timer_start(&watchdog, on_watchdog, 2000, 0);
        uv_run(&loop, UV_RUN_DEFAULT);
        if (iteration == 1) {
            threads = sqpoll_threads();
            if (threads < 1) {
                fprintf(stderr, "no io_uring polling thread: file operations are not on the ring "
                                "(libuv %s)\n", uv_version_string());
                return 3;
            }
            printf("libuv %s, %d polling thread(s); %ld iterations, %ld to %ld us between them\n",
                   uv_version_string(), threads, iterations, min_us, max_us);
            fflush(stdout);
        }
        if (kicked) {
            if (first_done && second_done) {
                printf("iteration %ld: the first stat completed %.3f ms after the second was "
                       "submitted (the second %.3f ms after): it had been in the ring, unread, "
                       "until the second submission woke the polling thread (lost wakeup)\n",
                       iteration, first_done_ms, second_done_ms);
                return 1;
            }
            printf("iteration %ld: still incomplete after the second submission (first %d, "
                   "second %d)\n", iteration, first_done, second_done);
            return 2;
        }
        if (iteration % 1000 == 0) {
            printf("iteration %ld: fine so far\n", iteration);
            fflush(stdout);
        }
        usleep(min_us + (max_us > min_us ? rand() % (max_us - min_us + 1) : 0));
    }
    printf("%ld iterations, every stat completed in time\n", iterations);
    uv_close((uv_handle_t*) &watchdog, NULL);
    uv_run(&loop, UV_RUN_DEFAULT);
    uv_loop_close(&loop);
    return 0;
}
