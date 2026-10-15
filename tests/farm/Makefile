# Farm suites: the adversarial review torture tests, the antfarm_test.d unit
# suite under TSan, and the controlled ordering schedules.
ROOT := ../..
DFLAGS ?= -g -O1
DC ?= ldc2
# Default for the TSan binaries; also baked into __tsan_default_options.
TSAN_OPTIONS ?= history_size=7 halt_on_error=1

.PHONY: all run run-tsan run-dmd run-t19 clean baseline allwrite run-allwrite \
	farm-tsan run-farm-tsan check-farm-ordering

all: torture_tests t19 sweep_cursor

allwrite: write_protection_allwrite boundary_runoff_allwrite

write_protection_allwrite: write_protection.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d
	$(DC) $(DFLAGS) -d-version=AntfarmWriteAuditHooks $^ -of=$@

boundary_runoff_allwrite: boundary_runoff.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d
	$(DC) $(DFLAGS) $^ -of=$@

run-allwrite: allwrite
	./boundary_runoff_allwrite
	./boundary_runoff_allwrite pulse
	./write_protection_allwrite

torture_tests: torture_common.d torture_tests.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d $(ROOT)/antfarm/templates.d
	$(DC) $(DFLAGS) torture_common.d torture_tests.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d $(ROOT)/antfarm/templates.d -of=$@

t19: torture_common.d t19_flood_lap.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d $(ROOT)/antfarm/templates.d
	$(DC) $(DFLAGS) torture_common.d t19_flood_lap.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d $(ROOT)/antfarm/templates.d -of=$@

sweep_cursor: torture_common.d sweep_cursor.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d $(ROOT)/antfarm/templates.d
	$(DC) $(DFLAGS) -d-version=AntfarmSweepStats torture_common.d sweep_cursor.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d $(ROOT)/antfarm/templates.d -of=$@

run: torture_tests sweep_cursor
	./torture_tests
	./sweep_cursor

run-t19: t19
	./t19

run-tsan:
	$(DC) -g -O1 -fsanitize=thread -d-version=TSan torture_common.d torture_tests.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d $(ROOT)/antfarm/templates.d -of=torture_tests_tsan
	TSAN_OPTIONS='$(TSAN_OPTIONS)' ./torture_tests_tsan

run-t19-tsan:
	$(DC) -g -O1 -fsanitize=thread -d-version=TSan torture_common.d t19_flood_lap.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d $(ROOT)/antfarm/templates.d -of=t19_tsan
	TSAN_OPTIONS='$(TSAN_OPTIONS)' ./t19_tsan

run-dmd:
	dmd -g -checkaction=context torture_common.d torture_tests.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d $(ROOT)/antfarm/templates.d -of=torture_tests_dmd
	./torture_tests_dmd

run-t19-dmd:
	dmd -g -checkaction=context torture_common.d t19_flood_lap.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d $(ROOT)/antfarm/templates.d -of=t19_dmd
	./t19_dmd

FARM_TEST_SOURCES := $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d $(ROOT)/antfarm/templates.d \
	$(ROOT)/antfarm/actors/package.d $(ROOT)/antfarm/actors/actor.d $(ROOT)/antfarm/actors/wave.d \
	$(ROOT)/antfarm/actors/private_actor.d $(ROOT)/antfarm/actors/mimalloc.d antfarm_test.d

# The Farm unit suite (antfarm_test.d) built with $(DC) outside dub.
baseline: $(FARM_TEST_SOURCES)
	$(DC) $(DFLAGS) -I$(ROOT) $(FARM_TEST_SOURCES) -of=antfarm_test_review
	./antfarm_test_review

# ThreadSanitizer build of the Farm unit suite (antfarm_test.d). Part of `make check`.
farm-tsan: $(FARM_TEST_SOURCES) leaf_reuse.d
	ldc2 -fsanitize=thread -d-version=TSan -g -O1 -I$(ROOT) $(FARM_TEST_SOURCES) -of=antfarm_test_tsan
	ldc2 -fsanitize=thread -d-version=TSan -g -O1 -I$(ROOT) leaf_reuse.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d -of=antfarm_test_leaf_tsan

run-farm-tsan: farm-tsan
	$(ROOT)/scripts/run-farm-tsan.sh

# Controlled subscription/reclamation schedules; hooks compile out normally.
check-farm-ordering:
	python3 $(ROOT)/scripts/check-ring-stores.py
	ldc2 -g -O1 -d-version=AntfarmWriteAuditHooks -I$(ROOT) farm_ordering.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d -of=antfarm_test_ordering
	./antfarm_test_ordering
	dmd -g -version=AntfarmWriteAuditHooks -I$(ROOT) farm_ordering.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d -of=antfarm_test_ordering_dmd
	./antfarm_test_ordering_dmd
	ldc2 -g -O1 -I$(ROOT) leaf_reuse.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d -of=antfarm_test_leaf
	./antfarm_test_leaf
	dmd -g -I$(ROOT) leaf_reuse.d $(ROOT)/antfarm/package.d $(ROOT)/antfarm/allocation.d -of=antfarm_test_leaf_dmd
	./antfarm_test_leaf_dmd

clean:
	rm -f torture_tests sweep_cursor torture_tests_tsan torture_tests_dmd t19 t19_tsan t19_dmd write_protection_allwrite boundary_runoff_allwrite *.o
	rm -f antfarm_test_review antfarm_test_tsan antfarm_test_leaf_tsan antfarm_test_ordering antfarm_test_ordering_dmd antfarm_test_leaf antfarm_test_leaf_dmd
