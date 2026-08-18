# Synthetic throughput sweep for a 16 MiB farm.
# Release flags on purpose: this is a perf tool, not a correctness suite.
ROOT := ..
DC ?= ldc2
DFLAGS ?= -O2 -release

.PHONY: all run run-once dual-run digest-run tail-run run-huge run-once-huge dual-run-huge digest-run-huge tail-run-huge clean

all: throughput dual digest tail tail

throughput: throughput.d $(ROOT)/antfarm.d
	$(DC) $(DFLAGS) throughput.d $(ROOT)/antfarm.d -of=$@

digest: digest.d $(ROOT)/antfarm.d
	$(DC) $(DFLAGS) digest.d $(ROOT)/antfarm.d -of=$@

tail: tail.d $(ROOT)/antfarm.d
	$(DC) $(DFLAGS) tail.d $(ROOT)/antfarm.d -of=$@

dual: dual.d $(ROOT)/antfarm.d
	$(DC) $(DFLAGS) dual.d $(ROOT)/antfarm.d -of=$@

run: throughput
	./throughput

run-once: throughput
	./throughput --once

dual-run: dual
	./dual

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
