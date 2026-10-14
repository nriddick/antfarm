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
import ant_fibers.scheduler : FiberDomain;
import farmhost : FarmHost, HostAttachment;

/// Drive `domain` from every `host.step()`: `pollTimers`,
/// `pollGenerationTriggers`, then `flush(host.token)`. `close()` refuses
/// until `domain.drained` (call `domain.beginShutdown()` and wait for it);
/// after the consumer join it releases the remaining completions
/// (`releaseAll(takeCompletions())`) before the Farm is destroyed. `domain`
/// must be built on `host.farm`.
void attachFiberDomain(FarmHost host, FiberDomain domain)
{
    host.attach(HostAttachment("fiber domain",
        (ref Token token) {
            domain.pollTimers();
            domain.pollGenerationTriggers();
            return domain.flush(token);
        },
        () => domain.drained,
        () { domain.releaseAll(domain.takeCompletions()); }));
}
