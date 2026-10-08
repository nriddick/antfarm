# Local correctness matrix. There is no CI.
#
# `make check` does not need /bin/sh. GNU make execs a recipe with no shell
# metacharacters directly, and scripts/check.sh carries its own bash shebang.
# That script passes an explicit SHELL to child makes, because this host has
# no /bin/sh and their TSan recipes quote TSAN_OPTIONS.
#
# Windows is the other supported target and is tested by hand (`dub test`,
# `actor_torture`, `antfarm_test`). New tests stay on druntime threads and
# atomics; this matrix itself is the Linux gate.

.PHONY: check check-soak check-mimalloc farm-tsan run-farm-tsan perf-gate

check:
	./scripts/check.sh

check-soak:
	./scripts/check.sh --soak

check-mimalloc:
	./scripts/check.sh --mimalloc

FARM_TSAN_SOURCES := antfarm.d antfarm_allocation.d antfarm_templates.d \
	actors/package.d actors/actor.d actors/wave.d actors/private_actor.d actors/mimalloc.d \
	antfarm_test.d

# ThreadSanitizer build of the Farm suite. Part of `make check`.
farm-tsan: $(FARM_TSAN_SOURCES)
	ldc2 -fsanitize=thread -g -O1 -I. $(FARM_TSAN_SOURCES) -of=antfarm_test_tsan

run-farm-tsan: farm-tsan
	./scripts/run-farm-tsan.sh

# Standalone timing gate; intentionally outside the correctness matrix.
PERF_GATE_OUTPUT ?= perftest/results/perf-gate
PERF_GATE_ARGS ?=
perf-gate:
	python3 perftest/perf_gate.py --base $(BASE) $(if $(CANDIDATE),--candidate $(CANDIDATE)) --output $(PERF_GATE_OUTPUT) $(PERF_GATE_ARGS)

# Controlled subscription/reclamation schedules; hooks compile out normally.
.PHONY: check-farm-ordering
check-farm-ordering:
	ldc2 -g -O1 -d-version=AntfarmWriteAuditHooks -I. review_torture/farm_ordering.d antfarm.d antfarm_allocation.d -of=antfarm_test_ordering
	./antfarm_test_ordering
	dmd -g -version=AntfarmWriteAuditHooks -I. review_torture/farm_ordering.d antfarm.d antfarm_allocation.d -of=antfarm_test_ordering_dmd
	./antfarm_test_ordering_dmd
