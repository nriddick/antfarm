# The secure mimalloc instance behind antfarm.secure_allocation: linked
# beside the fast instance (run, run-dmd), alone (run-alone), and the
# module's C runtime fallback without the option (run-off). Every lane also
# runs the unittests of antfarm.secure_allocation and antfarm.allocation.
# Both archives come from the actors suite's rules (the pinned
# third_party/mimalloc).
ROOT := ../..
DC ?= ldc2
DFLAGS ?= -g -O1
MIMALLOC_LIBRARY := $(ROOT)/tests/actors/.build/mimalloc-v3.7.0/libmimalloc.a
MIMALLOC_SECURE_LIBRARY := $(ROOT)/tests/actors/.build/mimalloc-v3.7.0-secure/libmimalloc-secure.a
MIMALLOC_SYSTEM_LIBS ?= -L-lpthread -L-lrt -L-latomic
SOURCES := secure_test.d $(ROOT)/antfarm/secure_allocation.d $(ROOT)/antfarm/allocation.d

.PHONY: run run-dmd run-alone run-off all clean

all: run run-dmd run-alone run-off

$(MIMALLOC_LIBRARY) $(MIMALLOC_SECURE_LIBRARY):
	$(MAKE) -C $(ROOT)/tests/actors $(patsubst $(ROOT)/tests/actors/%,%,$@)

run: $(MIMALLOC_LIBRARY) $(MIMALLOC_SECURE_LIBRARY)
	$(DC) $(DFLAGS) -unittest -d-version=AntfarmMimallocV3 -d-version=AntfarmMimallocSecure -I$(ROOT) \
		$(SOURCES) $(MIMALLOC_LIBRARY) $(MIMALLOC_SECURE_LIBRARY) $(MIMALLOC_SYSTEM_LIBS) -of=secure_test -od=.obj-ldc
	./secure_test --DRT-testmode=run-main

run-dmd: $(MIMALLOC_LIBRARY) $(MIMALLOC_SECURE_LIBRARY)
	dmd -g -unittest -version=AntfarmMimallocV3 -version=AntfarmMimallocSecure -I$(ROOT) \
		$(SOURCES) $(MIMALLOC_LIBRARY) $(MIMALLOC_SECURE_LIBRARY) $(MIMALLOC_SYSTEM_LIBS) -of=secure_test_dmd -od=.obj-dmd
	./secure_test_dmd --DRT-testmode=run-main

run-alone: $(MIMALLOC_SECURE_LIBRARY)
	$(DC) $(DFLAGS) -unittest -d-version=AntfarmMimallocSecure -I$(ROOT) \
		$(SOURCES) $(MIMALLOC_SECURE_LIBRARY) $(MIMALLOC_SYSTEM_LIBS) -of=secure_test_alone -od=.obj-alone
	./secure_test_alone --DRT-testmode=run-main

run-off:
	$(DC) $(DFLAGS) -unittest -I$(ROOT) $(SOURCES) -of=secure_test_off -od=.obj-off
	./secure_test_off --DRT-testmode=run-main

clean:
	rm -rf secure_test secure_test_dmd secure_test_alone secure_test_off .obj-ldc .obj-dmd .obj-alone .obj-off
