# Local correctness matrix. There is no CI.
#
# `make check` does not need /bin/sh. GNU make execs a recipe with no shell
# metacharacters directly, and scripts/check.sh carries its own bash shebang.
# That script passes an explicit SHELL to child makes, because this host has
# no /bin/sh and their TSan recipes quote TSAN_OPTIONS.
#
# Windows has a separate native Python runner. D tests use druntime threads
# and atomics; native C ABI tests verify C threads and attachment explicitly.
# The full correctness gate remains the Linux matrix.

.PHONY: help check check-betterc check-c-abi c-abi check-quick check-windows check-docs check-soak check-mimalloc docs perf-gate

# `help` is the first rule, so name the default goal explicitly.
.DEFAULT_GOAL := check

define HELP_TEXT
Ant Farm local targets (there is no CI). See CONTRIBUTING.md for details.

Correctness gate (Linux x86-64; needs DMD, LDC with the TSan runtime, Dub, Python 3):
  make check               full matrix, LDC and DMD, stops at the first failure
                           (bare `make` runs this)
  make check-quick         core C/C++ ABI, root, review, threadpool, fibers, examples
                           (LDC/Dub, Python 3, C11/C++11; no DMD/TSan)
  make check-betterc       BetterC modules and native C callback/hook smoke
  make check-c-abi         native C/C++ core and non-Fiber runtime ABI checks
  make c-abi              build static core; C_ABI_ARGS=--runtime adds the host
  make check-docs          tracked Markdown relative links and anchors (no network)
                           plus the API reference symbol/coverage check (needs DMD)
  make -C tests/farm check-parallel
                          ST grain ranges, joins and callback guards
  make -C tests/farm check-farm-ordering
                           Farm ordering and leaf-reuse tests, LDC and DMD
                           plus the writeImpl raw ring-store check
  make -C tests/farm run-farm-tsan
                           Farm unit suite under ThreadSanitizer (LDC)
  make -C tests/farm run run-tsan
                           adversarial Farm suite
  make -C tests/actors run run-dmd run-tsan death
                           public actor suite and child-process diagnostics
  make -C tests/private run run-dmd run-tsan doc-example death
                           private actor suite, doc programs and death tests
  make -C examples check   quickstart payload program and the actor examples

Reference (needs DMD):
  make docs               generate the HTML API reference into .build/docs/
                          (ignored; open .build/docs/antfarm/package.html first)

Windows integration (native x64, Dub, Python 3, LDC and DMD; no Bash or TSan):
  make check-windows      native Windows matrix (WINDOWS_CHECK_ARGS selects lanes)
  python scripts/check-windows.py --quick
                          focused LDC packages, review and examples; no make needed

Longer or optional (not run by make check):
  make check-soak          private-actor TSan loop for at least 121 s
  make check-mimalloc      make check plus real mimalloc lanes (needs the submodule)
  make -C tests/actors run-mimalloc-debug
                           other allocator lanes (see tests/actors/Makefile)

Performance report (not part of make check; Linux, LDC, Python 3, mimalloc):
  make perf-gate BASE=<sha> [CANDIDATE=<sha>]
                           see tools/perf/README.md
endef

help:
	$(info $(HELP_TEXT))
	@true

check:
	./scripts/check.sh

check-quick:
	./scripts/check.sh --quick

WINDOWS_PYTHON ?= python
WINDOWS_CHECK_ARGS ?=
check-windows:
	$(WINDOWS_PYTHON) -B scripts/check-windows.py $(WINDOWS_CHECK_ARGS)

check-betterc:
	python3 scripts/check-betterc.py

C_ABI_ARGS ?=
c-abi:
	python3 scripts/build-c-abi.py $(C_ABI_ARGS)

check-c-abi:
	python3 scripts/check-c-abi.py $(C_ABI_ARGS)

check-docs:
	python3 scripts/check-doc-links.py
	python3 -B scripts/test_check_doc_links.py
	python3 scripts/check-api-docs.py

check-soak:
	./scripts/check.sh --soak

check-mimalloc:
	./scripts/check.sh --mimalloc

# Generated HTML API reference. DMD creates the -Dd output directory
# itself, so the recipe is a single dmd exec with no shell metacharacters.
# -op keeps source paths, so the two package.d files get separate pages.
# mimalloc is included with its own version so the optional allocator
# adapter appears in the reference without a C dependency.
DOCS_SOURCES := antfarm/package.d antfarm/templates.d antfarm/parallel.d \
	antfarm/actors/package.d antfarm/actors/actor.d antfarm/actors/wave.d antfarm/actors/private_actor.d \
	antfarm/actors/mimalloc.d threadpool/source/threadpool/host.d threadpool/source/threadpool/groups.d \
	forkjoin/source/forkjoin/package.d forkjoin/source/forkjoin/node.d forkjoin/source/forkjoin/deque.d \
	forkjoin/source/forkjoin/dispatcher.d forkjoin/source/forkjoin/range.d forkjoin/source/forkjoin/frames.d
DOCS_DIR := .build/docs

docs:
	dmd -D -Dd$(DOCS_DIR) -op -o- -version=AntfarmMimallocV3 -I. -Ithreadpool/source -Iforkjoin/source $(DOCS_SOURCES)

# Standalone timing gate; intentionally outside the correctness matrix.
PERF_GATE_OUTPUT ?= /tmp/antfarm-perf-gate
PERF_GATE_ARGS ?=
perf-gate:
	python3 tools/perf/perf_gate.py --base $(BASE) $(if $(CANDIDATE),--candidate $(CANDIDATE)) --output $(PERF_GATE_OUTPUT) $(PERF_GATE_ARGS)
