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

.PHONY: help check check-soak check-mimalloc farm-tsan run-farm-tsan perf-gate

# `help` is the first rule, so name the default goal explicitly.
.DEFAULT_GOAL := check

define HELP_TEXT
Ant Farm local targets (there is no CI). See CONTRIBUTING.md for details.

Correctness gate (Linux x86-64; needs DMD, LDC with the TSan runtime, Dub, Python 3):
  make check               full matrix, LDC and DMD, stops at the first failure
                           (bare `make` runs this)
  make check-farm-ordering Farm ordering and leaf-reuse tests, LDC and DMD
  make run-farm-tsan       Farm suite under ThreadSanitizer (LDC)
  make -C actor_torture run run-dmd run-tsan
                           public actor suite
  make -C review_torture run run-tsan
                           adversarial Farm suite
  make -C private_actor_tests run run-dmd run-tsan doc-example death
                           private actor suite, doc programs and death tests
  make -C examples check   README payload program and the actor examples

Longer or optional (not run by make check):
  make check-soak          private-actor TSan loop for at least 121 s
  make check-mimalloc      make check plus real mimalloc lanes (needs the submodule)
  make -C actor_torture run-mimalloc-debug
                           other allocator lanes (see actor_torture/Makefile)

Performance (not part of make check; quiet Linux host, LDC, Python 3, mimalloc):
  make perf-gate BASE=<sha> [CANDIDATE=<sha>]
                           see perftest/README.md
endef

help:
	$(info $(HELP_TEXT))
	@true

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
farm-tsan: $(FARM_TSAN_SOURCES) review_torture/leaf_reuse.d
	ldc2 -fsanitize=thread -d-version=TSan -g -O1 -I. $(FARM_TSAN_SOURCES) -of=antfarm_test_tsan
	ldc2 -fsanitize=thread -d-version=TSan -g -O1 -I. review_torture/leaf_reuse.d antfarm.d antfarm_allocation.d -of=antfarm_test_leaf_tsan

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
	ldc2 -g -O1 -I. review_torture/leaf_reuse.d antfarm.d antfarm_allocation.d -of=antfarm_test_leaf
	./antfarm_test_leaf
	dmd -g -I. review_torture/leaf_reuse.d antfarm.d antfarm_allocation.d -of=antfarm_test_leaf_dmd
	./antfarm_test_leaf_dmd
