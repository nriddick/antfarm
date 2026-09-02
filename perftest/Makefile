# Synthetic throughput sweep for a 16 MiB farm.
# Release flags on purpose: this is a perf tool, not a correctness suite.
ROOT := ..
DC ?= ldc2
DFLAGS ?= -O2 -release
FARM_SOURCES := $(ROOT)/antfarm.d $(ROOT)/antfarm_allocation.d

.PHONY: all run run-once run-once-constant-header run-once-constant-callandlength dual-run dual-grid-run dual-run-constant-header dual-run-constant-callandlength digest-run tail-run grid-run run-grid run-huge run-once-huge dual-run-huge digest-run-huge tail-run-huge clean

all: throughput dual digest tail

throughput: throughput.d $(FARM_SOURCES)
	$(DC) $(DFLAGS) throughput.d $(FARM_SOURCES) -of=$@

digest: digest.d $(FARM_SOURCES)
	$(DC) $(DFLAGS) digest.d $(FARM_SOURCES) -of=$@

tail: tail.d $(FARM_SOURCES)
	$(DC) $(DFLAGS) tail.d $(FARM_SOURCES) -of=$@

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
	rm -f throughput digest tail dual *.o
