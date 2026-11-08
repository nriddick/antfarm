# ant_uv tests: the binding's layout against the real uv.h, and UvLoop
# behaviour (lifecycle, placement, ordering, timers, files from worker
# Fibers, cancellation, backpressure, close, failure, abandonment, Fibers
# on the loop thread, readFile's allocators). Links the pinned libuv that
# scripts/build-libuv.py builds (UV_PREFIX names another installation).
# run-mimalloc adds reads into the real mimalloc v3, built from
# the third_party/mimalloc submodule by the actors suite's rule.
ROOT := ../..
DC ?= ldc2
DFLAGS ?= -g -O1
CC ?= cc
# The pinned libuv, built by scripts/build-libuv.py from the pinned source
# archive, the same as on Windows. UV_PREFIX names another installation
# (include/uv.h, lib/libuv.a).
UV_PREFIX ?= $(ROOT)/.build/libuv/pinned
UV_LIBRARY := $(UV_PREFIX)/lib/libuv.a
UV_LIBS ?= $(UV_LIBRARY) -L-ldl
UV_CFLAGS ?= -I$(UV_PREFIX)/include
TSAN_OPTIONS ?= history_size=7 halt_on_error=1 report_thread_leaks=0
IMPORTS := -I$(ROOT) -I$(ROOT)/threadpool/source -I$(ROOT)/ant_fibers/source -I$(ROOT)/ant_uv/source
MIMALLOC_LIBRARY := $(ROOT)/tests/actors/.build/mimalloc-v3.5.3/libmimalloc.a
MIMALLOC_SYSTEM_LIBS ?= -L-lpthread -L-lrt -L-latomic

.PHONY: run run-iouring run-dmd run-tsan run-dip1000 run-mimalloc clean

$(UV_LIBRARY):
	python3 $(ROOT)/scripts/build-libuv.py

layout.o: layout.c $(UV_LIBRARY)
	$(CC) -O1 $(UV_CFLAGS) -c layout.c -o $@

uv_test: uv_test.d layout.o $(wildcard $(ROOT)/ant_uv/source/ant_uv/*.d) $(ROOT)/antfarm/ring.d $(UV_LIBRARY)
	$(DC) $(DFLAGS) -i $(IMPORTS) uv_test.d layout.o $(UV_LIBS) -of=$@ -od=.obj-ldc

run: uv_test
	./uv_test

# Every loop with UvLoopOptions.ioUring: open, close, read, write, stat and
# fstat through io_uring. Linux 5.10.186 and libuv 1.49 or later.
run-iouring: uv_test
	ANTUV_TEST_IO_URING=1 ./uv_test

run-dmd: layout.o
	dmd -g -i $(IMPORTS) uv_test.d layout.o $(UV_LIBS) -of=uv_test_dmd -od=.obj-dmd
	./uv_test_dmd

run-tsan: layout.o
	ldc2 -g -O1 -fsanitize=thread -d-version=TSan -i $(IMPORTS) uv_test.d layout.o $(UV_LIBS) -of=uv_test_tsan -od=.obj-tsan
	TSAN_OPTIONS='$(TSAN_OPTIONS) suppressions=tsan.supp' ./uv_test_tsan

# The suite built with -preview=dip1000, which holds the @safe surface's
# scope rules (ant_uv.safe) at compile time and runs its compile-time
# escape checks.
run-dip1000: layout.o
	$(DC) $(DFLAGS) -preview=dip1000 -i $(IMPORTS) uv_test.d layout.o $(UV_LIBS) -of=uv_test_dip1000 -od=.obj-dip1000
	./uv_test_dip1000

$(MIMALLOC_LIBRARY):
	$(MAKE) -C $(ROOT)/tests/actors .build/mimalloc-v3.5.3/libmimalloc.a

run-mimalloc: layout.o $(MIMALLOC_LIBRARY)
	$(DC) $(DFLAGS) -d-version=AntfarmMimallocV3 -i $(IMPORTS) uv_test.d layout.o \
		$(MIMALLOC_LIBRARY) $(MIMALLOC_SYSTEM_LIBS) $(UV_LIBS) -of=uv_test_mimalloc -od=.obj-mimalloc
	./uv_test_mimalloc

clean:
	rm -rf uv_test uv_test_dmd uv_test_tsan uv_test_dip1000 uv_test_mimalloc layout.o .obj-ldc \
		.obj-dmd .obj-tsan .obj-dip1000 .obj-mimalloc
