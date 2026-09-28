ROOT := ..
DC ?= ldc2
DFLAGS ?= -g -O1
SOURCES := main.d $(ROOT)/antfarm.d $(ROOT)/antfarm_allocation.d $(ROOT)/actors/actor.d $(ROOT)/actors/wave.d $(ROOT)/actors/private_actor.d
.PHONY: run run-dmd run-tsan clean
private_actor_tests: $(SOURCES)
	$(DC) $(DFLAGS) -d-version=AntfarmWriteAuditHooks -I$(ROOT) $(SOURCES) -of=$@
run: private_actor_tests
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests
run-dmd:
	dmd -g -version=AntfarmWriteAuditHooks -I$(ROOT) $(SOURCES) -of=private_actor_tests_dmd
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests_dmd
run-tsan:
	ldc2 -g -O1 -fsanitize=thread -d-version=AntfarmWriteAuditHooks -I$(ROOT) $(SOURCES) -of=private_actor_tests_tsan
	ANTFARM_HUGE_PAGES=0 TSAN_OPTIONS=history_size=7:halt_on_error=1 ./private_actor_tests_tsan
clean:
	rm -f private_actor_tests private_actor_tests_dmd private_actor_tests_tsan *.o
