# Antfarm review torture suite
ROOT := ..
DFLAGS ?= -g -O1
DC ?= ldc2

.PHONY: all run run-tsan run-dmd run-t19 clean baseline

all: torture_tests t19

torture_tests: torture_common.d torture_tests.d $(ROOT)/antfarm.d
	$(DC) $(DFLAGS) torture_common.d torture_tests.d $(ROOT)/antfarm.d -of=$@

t19: torture_common.d t19_flood_lap.d $(ROOT)/antfarm.d
	$(DC) $(DFLAGS) torture_common.d t19_flood_lap.d $(ROOT)/antfarm.d -of=$@

run: torture_tests
	./torture_tests

run-t19: t19
	./t19

run-tsan:
	$(DC) -g -O1 -fsanitize=thread torture_common.d torture_tests.d $(ROOT)/antfarm.d -of=torture_tests_tsan
	TSAN_OPTIONS='halt_on_error=1:history_size=7' ./torture_tests_tsan

run-t19-tsan:
	$(DC) -g -O1 -fsanitize=thread torture_common.d t19_flood_lap.d $(ROOT)/antfarm.d -of=t19_tsan
	TSAN_OPTIONS='halt_on_error=1:history_size=7' ./t19_tsan

run-dmd:
	dmd -g -checkaction=context torture_common.d torture_tests.d $(ROOT)/antfarm.d -of=torture_tests_dmd
	./torture_tests_dmd

run-t19-dmd:
	dmd -g -checkaction=context torture_common.d t19_flood_lap.d $(ROOT)/antfarm.d -of=t19_dmd
	./t19_dmd

baseline:
	$(DC) $(DFLAGS) $(ROOT)/antfarm.d $(ROOT)/antfarm_test.d -of=../antfarm_test_review
	../antfarm_test_review

clean:
	rm -f torture_tests torture_tests_tsan torture_tests_dmd t19 t19_tsan t19_dmd *.o
