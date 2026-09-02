# Ant Farm actor deterministic-interleaving and sustained-contention suite
ROOT := ..
DFLAGS ?= -g -O1
DC ?= ldc2
TSAN_OPTIONS ?= history_size=7 halt_on_error=1
SOURCES := actor_torture.d $(ROOT)/antfarm.d $(ROOT)/antfarm_allocation.d \
	$(ROOT)/antfarm_templates.d $(ROOT)/actors/package.d \
	$(ROOT)/actors/actor.d $(ROOT)/actors/wave.d \
	$(ROOT)/actors/mimalloc.d
LDC_VERSIONS := -d-version=AntfarmActorTestHooks -d-version=AntfarmMimallocV3 \
	-d-version=AntfarmMimallocStub
DMD_VERSIONS := -version=AntfarmActorTestHooks -version=AntfarmMimallocV3 \
	-version=AntfarmMimallocStub
LDC_REAL_VERSIONS := -d-version=AntfarmActorTestHooks \
	-d-version=AntfarmMimallocV3 -d-version=AntfarmMimallocReal
DMD_REAL_VERSIONS := -version=AntfarmActorTestHooks \
	-version=AntfarmMimallocV3 -version=AntfarmMimallocReal
MIMALLOC_SOURCE := $(ROOT)/third_party/mimalloc
MIMALLOC_BUILD := .build/mimalloc-v3.5.0
MIMALLOC_LIBRARY := $(MIMALLOC_BUILD)/libmimalloc.a
MIMALLOC_DEBUG_BUILD := .build/mimalloc-v3.5.0-debug
MIMALLOC_DEBUG_LIBRARY := $(MIMALLOC_DEBUG_BUILD)/libmimalloc-debug.a
MIMALLOC_TSAN_BUILD := .build/mimalloc-v3.5.0-tsan
MIMALLOC_TSAN_LIBRARY := $(MIMALLOC_TSAN_BUILD)/libmimalloc-debug.a
MIMALLOC_TSAN_CC ?= clang
MIMALLOC_SYSTEM_LIBS ?= -L-lpthread -L-lrt -L-latomic

.PHONY: all run run-tsan run-dmd run-mimalloc run-mimalloc-dmd \
	run-mimalloc-debug run-mimalloc-tsan clean

all: actor_torture

actor_torture: $(SOURCES)
	$(DC) $(DFLAGS) $(LDC_VERSIONS) $(SOURCES) -of=$@

run: actor_torture
	./actor_torture

run-tsan:
	$(DC) -g -O1 -fsanitize=thread -d-version=TSan $(LDC_VERSIONS) $(SOURCES) -of=actor_torture_tsan
	TSAN_OPTIONS='$(TSAN_OPTIONS)' ./actor_torture_tsan

run-dmd:
	dmd -g -checkaction=context $(DMD_VERSIONS) $(SOURCES) -of=actor_torture_dmd
	./actor_torture_dmd

$(MIMALLOC_LIBRARY): $(MIMALLOC_SOURCE)/CMakeLists.txt
	cmake -S $(MIMALLOC_SOURCE) -B $(MIMALLOC_BUILD) \
		-DCMAKE_BUILD_TYPE=Release -DMI_OVERRIDE=OFF \
		-DMI_BUILD_SHARED=OFF -DMI_BUILD_STATIC=ON \
		-DMI_BUILD_OBJECT=OFF -DMI_BUILD_TESTS=OFF -DMI_OPT_ARCH=OFF
	cmake --build $(MIMALLOC_BUILD) --config Release \
		--target mimalloc-static --parallel

$(MIMALLOC_DEBUG_LIBRARY): $(MIMALLOC_SOURCE)/CMakeLists.txt
	cmake -S $(MIMALLOC_SOURCE) -B $(MIMALLOC_DEBUG_BUILD) \
		-DCMAKE_BUILD_TYPE=Debug -DMI_DEBUG=FULL -DMI_OVERRIDE=OFF \
		-DMI_BUILD_SHARED=OFF -DMI_BUILD_STATIC=ON \
		-DMI_BUILD_OBJECT=OFF -DMI_BUILD_TESTS=OFF -DMI_OPT_ARCH=OFF
	cmake --build $(MIMALLOC_DEBUG_BUILD) --config Debug \
		--target mimalloc-static --parallel

$(MIMALLOC_TSAN_LIBRARY): $(MIMALLOC_SOURCE)/CMakeLists.txt
	cmake -S $(MIMALLOC_SOURCE) -B $(MIMALLOC_TSAN_BUILD) \
		-DCMAKE_C_COMPILER=$(MIMALLOC_TSAN_CC) \
		-DCMAKE_BUILD_TYPE=Debug -DMI_DEBUG_TSAN=ON -DMI_OVERRIDE=OFF \
		-DMI_BUILD_SHARED=OFF -DMI_BUILD_STATIC=ON \
		-DMI_BUILD_OBJECT=OFF -DMI_BUILD_TESTS=OFF -DMI_OPT_ARCH=OFF
	cmake --build $(MIMALLOC_TSAN_BUILD) --config Debug \
		--target mimalloc-static --parallel

actor_torture_mimalloc: $(SOURCES) $(MIMALLOC_LIBRARY)
	$(DC) $(DFLAGS) $(LDC_REAL_VERSIONS) $(SOURCES) \
		$(MIMALLOC_LIBRARY) $(MIMALLOC_SYSTEM_LIBS) -of=$@

run-mimalloc: actor_torture_mimalloc
	./actor_torture_mimalloc

run-mimalloc-dmd: $(SOURCES) $(MIMALLOC_LIBRARY)
	dmd -g -checkaction=context $(DMD_REAL_VERSIONS) $(SOURCES) \
		$(MIMALLOC_LIBRARY) $(MIMALLOC_SYSTEM_LIBS) \
		-of=actor_torture_mimalloc_dmd
	./actor_torture_mimalloc_dmd

run-mimalloc-debug: $(SOURCES) $(MIMALLOC_DEBUG_LIBRARY)
	$(DC) -g -O0 $(LDC_REAL_VERSIONS) $(SOURCES) \
		$(MIMALLOC_DEBUG_LIBRARY) $(MIMALLOC_SYSTEM_LIBS) \
		-of=actor_torture_mimalloc_debug
	./actor_torture_mimalloc_debug

run-mimalloc-tsan: $(SOURCES) $(MIMALLOC_TSAN_LIBRARY)
	$(DC) -g -O1 -fsanitize=thread -d-version=TSan \
		$(LDC_REAL_VERSIONS) $(SOURCES) $(MIMALLOC_TSAN_LIBRARY) \
		$(MIMALLOC_SYSTEM_LIBS) -of=actor_torture_mimalloc_tsan
	TSAN_OPTIONS='$(TSAN_OPTIONS)' ./actor_torture_mimalloc_tsan

clean:
	rm -f actor_torture actor_torture_tsan actor_torture_dmd \
		actor_torture_mimalloc actor_torture_mimalloc_dmd \
		actor_torture_mimalloc_debug actor_torture_mimalloc_tsan *.o
	rm -rf .build
