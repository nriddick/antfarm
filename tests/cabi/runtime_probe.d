module cabi_runtime_probe;
import core.memory : GC;
extern(C) void antfarm_test_collect() { GC.collect(); }
