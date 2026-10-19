module cabi_audit;
import antfarm;
extern(C) alias AfterPublish = void function() nothrow @nogc @system;
private __gshared AfterPublish callback;
private void observe(AntFarm*, WriteAuditPhase phase, ulong, ulong) nothrow @nogc @system
{
    if (phase == WriteAuditPhase.tablePublished && callback !is null) callback();
}
extern(C) void antfarm_test_after_publish(AfterPublish fn) nothrow @nogc @system
{
    callback = fn;
    writeAuditHook = fn is null ? null : &observe;
}
