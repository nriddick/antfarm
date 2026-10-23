#ifndef ANTFARM_TEST_TSAN_PREINIT_H
#define ANTFARM_TEST_TSAN_PREINIT_H
#ifdef AF_TEST_TSAN
// check-c-abi.py --tsan force-includes this header (-include) for each C++ TU.
// LDC links the TSan archive without extracting its preinit-only object.
// Initialize before libstdc++'s constructors call intercepted pthread routines.
extern "C" void __tsan_init();
__attribute__((section(".preinit_array"), used))
static void (*const tsan_preinit)() = __tsan_init;
#endif
#endif
