/++
 + FarmHost adapter for a caller-driven `FiberDomain`. Not re-exported by
 + `import ant_fibers;`: import `ant_fibers.host`.
 +
 + Fiber task payloads run on the host's existing consumers (same Farm, same
 + group), so attaching a domain adds no consumer, no producer and no
 + cross-group edge.
 +/
module ant_fibers.host;

import antfarm : Token;
import ant_fibers.scheduler : FiberDomain, FiberRetirement;
import threadpool.host : FarmHost, HostAttachment;

/// Drive `domain` from every `host.step()`: one `FiberDomain.serve` pass
/// with the host token, as the domain's timer responder, which also retires
/// records and terminal tasks as they arrive. The control thread that calls
/// `step()` must therefore be the domain's Director (construct the domain on
/// it) unless the domain uses `FiberRetirement.serviceThreads`; this checks
/// that the calling thread is. `close()` refuses until `domain.drained` (call
/// `domain.beginShutdown()` and wait for it); after the consumer join it
/// retires whatever remains before the Farm is destroyed. `domain` must be a
/// one-lane domain built on `host.farm`.
void attachFiberDomain(FarmHost host, FiberDomain domain)
{
    if (domain.options.retirement == FiberRetirement.director
        && !domain.isDirectorThread)
        throw new Exception("ant_fibers: attachFiberDomain: the host's control "
            ~ "thread must be the domain's Director (construct the domain on it)");
    host.attach(HostAttachment("fiber domain",
        (ref Token token) => domain.serve(token).flushed,
        () => domain.drained,
        () { domain.retire(); }));
}
