#include "check.h"
#ifndef AF_TEST_THREADS_H
#define AF_TEST_THREADS_H

#include <stdint.h>
#ifdef _WIN32
#include <windows.h>
typedef struct { volatile LONG value; } test_atomic;
typedef HANDLE test_thread;
#define TEST_THREAD(name) static DWORD WINAPI name(void *arg)
#define TEST_RETURN return 0
static inline unsigned test_load(test_atomic *p) { return (unsigned)InterlockedCompareExchange(&p->value, 0, 0); }
static inline void test_store(test_atomic *p, unsigned v) { InterlockedExchange(&p->value, (LONG)v); }
static inline unsigned test_add(test_atomic *p, unsigned v) { return (unsigned)InterlockedExchangeAdd(&p->value, (LONG)v); }
static inline void test_yield(void) { SwitchToThread(); }
static inline test_thread test_start(LPTHREAD_START_ROUTINE fn, void *arg) {
    HANDLE t = CreateThread(NULL, 0, fn, arg, 0, NULL); CHECK(t); return t;
}
static inline void test_join(test_thread t) { CHECK(WaitForSingleObject(t, 20000) == WAIT_OBJECT_0); CloseHandle(t); }
#else
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
typedef struct { _Atomic unsigned value; } test_atomic;
typedef pthread_t test_thread;
#define TEST_THREAD(name) static void *name(void *arg)
#define TEST_RETURN return NULL
static inline unsigned test_load(test_atomic *p) { return atomic_load(&p->value); }
static inline void test_store(test_atomic *p, unsigned v) { atomic_store(&p->value, v); }
static inline unsigned test_add(test_atomic *p, unsigned v) { return atomic_fetch_add(&p->value, v); }
static inline void test_yield(void) { sched_yield(); }
static inline test_thread test_start(void *(*fn)(void *), void *arg) {
    pthread_t t; CHECK(pthread_create(&t, NULL, fn, arg) == 0); return t;
}
static inline void test_join(test_thread t) { CHECK(pthread_join(t, NULL) == 0); }
#endif
#endif
