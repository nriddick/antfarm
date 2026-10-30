# ant_uv tests: the binding's layout against the real uv.h, and UvLoop
# behaviour (lifecycle, placement, ordering, timers, files from worker
# Fibers, cancellation, backpressure, close, failure, abandonment, Fibers
# on the loop thread). Needs libuv 1.19 or later and its header.
ROOT := ../..
DC ?= ldc2
DFLAGS ?= -g -O1
CC ?= cc
UV_LIBS ?= -luv
TSAN_OPTIONS ?= history_size=7 halt_on_error=1 report_thread_leaks=0
IMPORTS := -I$(ROOT) -I$(ROOT)/threadpool/source -I$(ROOT)/ant_fibers/source -I$(ROOT)/ant_uv/source

.PHONY: run run-dmd run-tsan clean

layout.o: layout.c
	$(CC) -O1 -c layout.c -o $@

uv_test: uv_test.d layout.o $(wildcard $(ROOT)/ant_uv/source/ant_uv/*.d) $(ROOT)/antfarm/ring.d
	$(DC) $(DFLAGS) -i $(IMPORTS) uv_test.d layout.o -L$(UV_LIBS) -of=$@ -od=.obj-ldc

run: uv_test
	./uv_test

run-dmd: layout.o
	dmd -g -i $(IMPORTS) uv_test.d layout.o -L$(UV_LIBS) -of=uv_test_dmd -od=.obj-dmd
	./uv_test_dmd

run-tsan: layout.o
	ldc2 -g -O1 -fsanitize=thread -d-version=TSan -i $(IMPORTS) uv_test.d layout.o -L$(UV_LIBS) -of=uv_test_tsan -od=.obj-tsan
	TSAN_OPTIONS='$(TSAN_OPTIONS) suppressions=tsan.supp' ./uv_test_tsan

clean:
	rm -rf uv_test uv_test_dmd uv_test_tsan layout.o .obj-ldc .obj-dmd .obj-tsan
