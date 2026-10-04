ROOT := ..
DC ?= ldc2
DFLAGS ?= -g -O1
SOURCES := main.d $(ROOT)/antfarm.d $(ROOT)/antfarm_allocation.d $(ROOT)/actors/actor.d $(ROOT)/actors/wave.d $(ROOT)/actors/private_actor.d
.PHONY: doc-example death run run-dmd run-tsan run-death run-death-dmd clean
private_actor_tests: $(SOURCES)
	$(DC) $(DFLAGS) -d-version=AntfarmWriteAuditHooks -I$(ROOT) $(SOURCES) -of=$@
private_actor_tests_dmd: $(SOURCES)
	dmd -g -version=AntfarmWriteAuditHooks -I$(ROOT) $(SOURCES) -of=$@
run: private_actor_tests
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests
run-dmd: private_actor_tests_dmd
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests_dmd
run-tsan:
	ldc2 -g -O1 -fsanitize=thread -d-version=AntfarmWriteAuditHooks -I$(ROOT) $(SOURCES) -of=private_actor_tests_tsan
	ANTFARM_HUGE_PAGES=0 TSAN_OPTIONS=history_size=7:halt_on_error=1 ./private_actor_tests_tsan

# Fatal-death harness, built in both debug and -release so the checks cannot
# hide behind asserts (which -release removes). `death` re-execs
# /proc/self/exe, requires a nonzero death and the exact "antfarm fatal:"
# stderr text; `death-negative-control` proves the harness rejects a
# zero-exit survivor and a nonzero death with the wrong stderr text.
private_actor_tests_release: $(SOURCES)
	$(DC) $(DFLAGS) -release -d-version=AntfarmWriteAuditHooks -I$(ROOT) $(SOURCES) -of=$@
private_actor_tests_dmd_release: $(SOURCES)
	dmd -g -release -version=AntfarmWriteAuditHooks -I$(ROOT) $(SOURCES) -of=$@
death: run-death run-death-dmd

run-death: private_actor_tests private_actor_tests_release
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests death
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests death-negative-control
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests_release death
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests_release death-negative-control
run-death-dmd: private_actor_tests_dmd private_actor_tests_dmd_release
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests_dmd death
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests_dmd death-negative-control
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests_dmd_release death
	ANTFARM_HUGE_PAGES=0 ./private_actor_tests_dmd_release death-negative-control

clean:
	rm -f private_actor_tests private_actor_tests_release private_actor_tests_dmd \
		private_actor_tests_dmd_release private_actor_tests_tsan *.o

doc-example:
	python3 extract_doc_examples.py
	for source in .doc-examples/*.d; do \
		for compiler in ldc2 dmd; do \
			for flags in -g -release; do \
				$$compiler $$flags -I$(ROOT) $$source $(filter-out main.d,$(SOURCES)) -of=.doc-examples/example || exit $$?; \
				ANTFARM_HUGE_PAGES=0 ./.doc-examples/example || exit $$?; \
			done; \
		done; \
	done
