# Multi-threaded SlotRing tests: LDC, DMD and ThreadSanitizer lanes.
ROOT := ../..
DC ?= ldc2
DFLAGS ?= -g -O1
TSAN_OPTIONS ?= history_size=7 halt_on_error=1
SOURCES := ring_test.d $(ROOT)/antfarm/ring.d $(ROOT)/antfarm/allocation.d

.PHONY: run run-dmd run-tsan clean

ring_test: $(SOURCES)
	$(DC) $(DFLAGS) -d-version=AntfarmRingTestHooks -I$(ROOT) $(SOURCES) -of=$@

run: ring_test
	./ring_test

run-dmd:
	dmd -g -O -checkaction=context -version=AntfarmRingTestHooks -I$(ROOT) $(SOURCES) -of=ring_test_dmd
	./ring_test_dmd

run-tsan:
	ldc2 -g -O1 -fsanitize=thread -d-version=TSan -d-version=AntfarmRingTestHooks -I$(ROOT) $(SOURCES) -of=ring_test_tsan
	TSAN_OPTIONS='$(TSAN_OPTIONS)' ./ring_test_tsan

clean:
	rm -f ring_test ring_test_dmd ring_test_tsan *.o
