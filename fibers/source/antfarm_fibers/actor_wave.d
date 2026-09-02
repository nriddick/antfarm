/++ Scheduler bridge for actor-wave completion. +/
module antfarm_fibers.actor_wave;

import actors : ActorWaveCompletionHook;
import antfarm_fibers.scheduler : FiberDomain, FiberGenerationTrigger;

/// One serialized actor-wave stream observed by one orchestration Fiber.
/// Completion may occur in an Ant Farm payload callback, so the hook advances
/// an allocation-free deferred trigger rather than touching the scheduler's
/// locked wait table directly.
final class ActorWaveTrigger
{
    private FiberGenerationTrigger trigger_;

    this(FiberDomain domain)
    {
        trigger_ = new FiberGenerationTrigger(domain);
    }

    @property ActorWaveCompletionHook hook() nothrow @nogc @system
    {
        return ActorWaveCompletionHook(cast(void*) trigger_,
            &actorWaveCompleted);
    }

    @property ulong completed() const nothrow @nogc
    {
        return trigger_.completed;
    }

    /// Suspend until the next wave in this trigger's serialized stream ends.
    void waitNext()
    {
        trigger_.waitNext();
    }
}

private void actorWaveCompleted(void* context) nothrow @nogc @system
{
    auto trigger = cast(FiberGenerationTrigger) context;
    assert(trigger !is null, "antfarm_fibers: null actor-wave trigger");
    trigger.advance();
}
