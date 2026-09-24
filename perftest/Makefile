# Synthetic payload throughput and autonomous actor benchmarks.
# Release flags on purpose: this is a perf tool, not a correctness suite.
ROOT := ..
DC ?= ldc2
DFLAGS ?= -O2 -release
FARM_SOURCES := $(ROOT)/antfarm.d $(ROOT)/antfarm_allocation.d
ACTOR_SOURCES := $(ROOT)/actors/package.d $(ROOT)/actors/actor.d \
	$(ROOT)/actors/wave.d $(ROOT)/actors/mimalloc.d
FIBER_SOURCES := $(wildcard $(ROOT)/fibers/source/antfarm_fibers/*.d)
THREADPOOL_SOURCES := $(wildcard $(ROOT)/threadpool/source/threadpool/*.d) \
	$(wildcard $(ROOT)/threadpool/source/threadpool/sys/*.d)
MIMALLOC_LIBRARY := $(ROOT)/actor_torture/.build/mimalloc-v3.5.0/libmimalloc.a
MIMALLOC_VERSION ?= -d-version=AntfarmMimallocV3
MIMALLOC_SYSTEM_LIBS ?= -L-lpthread -L-lrt -L-latomic

.PHONY: all run run-once run-once-constant-header run-once-constant-callandlength dual-run dual-grid-run dual-run-constant-header dual-run-constant-callandlength digest-run tail-run grid-run run-grid run-huge run-once-huge dual-run-huge digest-run-huge tail-run-huge clean

all: throughput dual digest tail actor_ready actor_churn scaling

actor_churn: actor_churn.d $(FARM_SOURCES) $(ACTOR_SOURCES) $(THREADPOOL_SOURCES)
	$(DC) $(DFLAGS) -i -I$(ROOT) -I$(ROOT)/threadpool/source \
		actor_churn.d $(FARM_SOURCES) $(ACTOR_SOURCES) -of=$@

$(MIMALLOC_LIBRARY): $(ROOT)/third_party/mimalloc/CMakeLists.txt
	$(MAKE) -C $(ROOT)/actor_torture .build/mimalloc-v3.5.0/libmimalloc.a

actor_churn_mimalloc: actor_churn.d $(FARM_SOURCES) $(ACTOR_SOURCES) $(THREADPOOL_SOURCES) $(MIMALLOC_LIBRARY)
	$(DC) $(DFLAGS) $(MIMALLOC_VERSION) -i -I$(ROOT) -I$(ROOT)/threadpool/source actor_churn.d \
		$(FARM_SOURCES) $(ACTOR_SOURCES) $(MIMALLOC_LIBRARY) \
		$(MIMALLOC_SYSTEM_LIBS) -of=$@

scaling: scaling.d $(FARM_SOURCES) $(ACTOR_SOURCES) $(FIBER_SOURCES) $(THREADPOOL_SOURCES)
	$(DC) $(DFLAGS) -i -I$(ROOT) -I$(ROOT)/fibers/source \
		-I$(ROOT)/threadpool/source scaling.d -of=$@

actor_ready: actor_ready.d $(FARM_SOURCES) $(ACTOR_SOURCES)
	$(DC) $(DFLAGS) -I$(ROOT) actor_ready.d $(FARM_SOURCES) $(ACTOR_SOURCES) -of=$@

throughput: throughput.d $(FARM_SOURCES)
	$(DC) $(DFLAGS) throughput.d $(FARM_SOURCES) -of=$@

digest: digest.d $(FARM_SOURCES)
	$(DC) $(DFLAGS) digest.d $(FARM_SOURCES) -of=$@

tail: tail.d placement.d $(FARM_SOURCES) $(wildcard $(ROOT)/threadpool/source/threadpool/*.d) $(wildcard $(ROOT)/threadpool/source/threadpool/sys/*.d)
	$(DC) $(DFLAGS) -i -I$(ROOT)/threadpool/source tail.d $(FARM_SOURCES) -of=$@

dual: dual.d $(FARM_SOURCES)
	$(DC) $(DFLAGS) dual.d $(FARM_SOURCES) -of=$@

run: throughput
	./throughput

run-once: throughput
	./throughput --once

run-once-constant-header: throughput
	./throughput --once --constant-header

run-once-constant-callandlength: throughput
	./throughput --once --constant-callandlength

grid-run: throughput
	./throughput --grid 8

run-grid: grid-run

dual-run: dual
	./dual

dual-grid-run: dual
	./dual --grid 8

dual-run-constant-header: dual
	./dual --constant-header

dual-run-constant-callandlength: dual
	./dual --constant-callandlength

digest-run: digest
	./digest

tail-run: tail
	./tail | tee last_tail.txt

run-huge: throughput
	./throughput --huge

run-once-huge: throughput
	./throughput --once --huge

dual-run-huge: dual
	./dual --huge

digest-run-huge: digest
	./digest --huge

tail-run-huge: tail
	./tail --huge | tee last_tail_huge.txt

clean:
	rm -f throughput digest tail dual actor_ready actor_churn actor_churn_mimalloc scaling *.o
