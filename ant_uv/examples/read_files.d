/+ dub.sdl:
    name "read_files"
    dependency "antfarm:ant_uv" path="../.."
+/
/// Worker Fibers on a GroupHost read files through one libuv loop thread.
///
///     dub run --single ant_uv/examples/read_files.d -- /etc/hostname /etc/os-release
module read_files;

import ant_fibers;
import ant_uv;
import core.atomic;
import core.thread : Thread;
import core.time : msecs;
import std.stdio : writefln, writeln;
import std.string : toStringz;
import threadpool.groups : GroupHost, GroupHostOptions;
import threadpool.host : CallerPlacement, HostGroups;

// One Fiber per file: submit a whole-file read, park on an event until the
// loop publishes it, and take the loop's allocation.
void delegate() reader(UvLoop loop, FiberDomain domain, string path, shared(long)* total)
{
    return () {
        UvRequest request;
        request.op = UvOp.readFile;
        request.path = path.toStringz; // copied by libuv when the open starts
        request.event = new FiberEvent(domain);
        if (submitAndWait(loop, &request) != UvSubmit.accepted || !request.succeeded)
        {
            writefln("%s: %s", path, request.errorName);
            return;
        }
        auto file = request.takeBuffer();
        scope (exit) file.free(); // back to the allocator that made it
        atomicOp!"+="(*total, cast(long) file.bytes.length);
        writefln("%s: %s bytes", path, file.bytes.length);
    };
}

void main(string[] args)
{
    auto paths = args.length > 1 ? args[1 .. $] : ["/etc/hostname", "/etc/os-release"];

    // The application's one loop thread. UvLoopOptions.cpu pins it to a
    // CPU kept out of the worker set; auxCpus places libuv's own threads.
    auto loop = UvLoop.create();

    auto domain = new FiberDomain;
    auto fibers = new FiberGroups(domain);
    GroupHostOptions options;
    options.caller = CallerPlacement.unchanged;
    options.attachments = [fibers.attachment];
    auto host = new GroupHost(options);

    shared long total;
    foreach (path; paths)
        fibers.spawn(reader(loop, domain, path, &total));
    while (!domain.drained) Thread.sleep(1.msecs);

    // Shutdown order: the Fibers finished, so retire them; close the loop
    // (it settles anything outstanding); then the host and the domain.
    cast(void) domain.retire();
    auto report = loop.close();
    host.close();
    domain.beginShutdown();
    writeln(atomicLoad(total), " bytes in ", report.completed, " requests on ", host.workers, " workers");
}
