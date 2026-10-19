module cabi_runtime_probe;
import core.memory : GC;
import antfarm.c.runtime : af_host;
import threadpool.host : HostAttachment;
extern(C) void antfarm_test_collect() { GC.collect(); }
// Exercise the D refusal path, beyond the C wrapper's participant prechecks.
extern(C) void antfarm_test_busy_attachment(af_host* h, int* blocked)
{
    h.value.attach(HostAttachment("test busy", null, () => *blocked == 0, null));
}
extern(C) void antfarm_test_cleanup_failure(af_host* h)
{
    h.value.attach(HostAttachment("test cleanup", null, null,
        () { throw new Exception("injected terminal cleanup failure"); }));
}
