// A D main owns the original druntime reference; the C wrapper must balance
// only its own reference and must not detach this already-registered thread.
module cabi_embedded;
import antfarm.c.core;
import antfarm.c.runtime;
import core.thread : Thread;
import core.memory : GC;
import std.exception : enforce;
void main()
{
    auto original = Thread.getThis();
    enforce(af_runtime_init() == AF_OK);
    enforce(af_runtime_attach_thread() == AF_OK);
    enforce(af_runtime_detach_thread() == AF_OK);
    enforce(Thread.getThis() is original);
    af_host_options options;
    af_host_options_default(&options);
    options.workers = 0;
    af_host* host;
    enforce(af_host_create(&options, &host) == AF_OK);
    GC.collect();
    enforce(af_host_close(host) == AF_OK);
    enforce(af_runtime_shutdown() == AF_OK);
    enforce(Thread.getThis() is original);
    auto thread = new Thread({ auto value = new int(42); enforce(*value == 42); });
    thread.start(); thread.join(); GC.collect();
}
