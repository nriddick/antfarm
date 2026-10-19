#ifndef AF_TEST_CHECK_H
#define AF_TEST_CHECK_H
#include <stdio.h>
#include <stdlib.h>
/* Test operations and checks must execute even in an NDEBUG client build. */
#define CHECK(condition) do { if (!(condition)) { \
    fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition); \
    abort(); \
} } while (0)
#endif
