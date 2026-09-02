/++
 + Phase-oriented actor dispatch built from Ant Farm payload tables.
 +
 + One ActorWave may publish several physical tables. Each table's
 + Tprogress-to-Tlen transition advances the wave once; sealing the wave
 + completes it when every published table has advanced. Actor operations
 + are phase-specific and receive only a callback-local ActorBorrow, so they
 + cannot republish or retire themselves through ActorContext.
 +/
module antfarm_actors.wave;

public import antfarm_actors.actor : ActorBorrow, ActorHandle;

import antfarm : AntFarm, PayloadBody, PayloadHeader, TableCompletionHook,
    Tier, Token, fatal;
import antfarm_actors.actor : ActorSlot, cancelActorWavePayload,
    dispatchActorWavePayload, releaseActorWaveMembership,
    reserveActorWavePayload;
import core.atomic : MemoryOrder, atomicFetchAdd, atomicFetchSub, atomicLoad,
    atomicStore, cas;

private enum uint waveSealedBit = 1U << 0;
private enum uint waveFinishedBit = 1U << 1;
private enum uint waveFailedBit = 1U << 2;
private enum uint waveFinishingBit = 1U << 3;

/// Copyable observation capability for a stable ActorWave. An acquire load
/// that observes `finished` also observes every actor operation in the wave.
/// The owning ActorWave storage must outlive all of its handles.
struct ActorWaveHandle
{
    private ActorWave* wave_;
    private ulong generation_;

    @property ulong generation() const pure nothrow @nogc @safe
    {
        return generation_;
    }

    @property bool valid() const nothrow @nogc @system
    {
        if (wave_ is null || generation_ == 0) return false;
        return atomicLoad!(MemoryOrder.acq)(wave_.generation_) == generation_;
    }

    @property bool finished() const nothrow @nogc @system
    {
        if (wave_ is null || generation_ == 0) return false;
        immutable before = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        if (before != generation_) return false;
        immutable status = atomicLoad!(MemoryOrder.acq)(wave_.status_);
        immutable after = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        return after == generation_
            && (status & waveFinishedBit) != 0;
    }

    @property bool failed() const nothrow @nogc @system
    {
        if (wave_ is null || generation_ == 0) return false;
        immutable before = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        if (before != generation_) return false;
        immutable status = atomicLoad!(MemoryOrder.acq)(wave_.status_);
        immutable after = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        return after == generation_ && (status & waveFailedBit) != 0;
    }

    @property ulong progress() const nothrow @nogc @system
    {
        if (wave_ is null || generation_ == 0) return 0;
        immutable before = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        if (before != generation_) return 0;
        immutable result = atomicLoad!(MemoryOrder.acq)(
            wave_.tablesCompleted_);
        immutable after = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        return after == generation_ ? result : 0;
    }

    @property ulong length() const nothrow @nogc @system
    {
        if (wave_ is null || generation_ == 0) return 0;
        immutable before = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        if (before != generation_) return 0;
        immutable result = atomicLoad!(MemoryOrder.acq)(
            wave_.tablesPublished_);
        immutable after = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        return after == generation_ ? result : 0;
    }
}

/// Stable, reusable owner of one actor-wave generation. `begin` and
/// `publish` are single-orchestrator operations. Consumers may complete
/// tables concurrently, and any number of observers may poll handles.
///
/// A wave operation is accepted only when every offered actor is idle. The
/// reservation is made before Farm publication, preventing another wave or
/// autonomous activation from borrowing that actor generation in parallel.
/// Intrusive membership and its lifetime pin remain until aggregate finish,
/// including after an individual actor's table has completed.
align(64) struct ActorWave
{
    @disable this(this);

private:
    shared ulong generation_;
    shared ulong tablesPublished_; // Wlen
    shared ulong tablesCompleted_; // Wprogress
    shared uint status_;
    uint padding_;
    AntFarm* farm_;
    TableCompletionHook tableCompletion_;
    ActorSlot* membersHead_;
    ulong padding2_;

public:
    /// Start the first generation, or reuse this storage after its previous
    /// generation finished. Reuse invalidates every older handle.
    void begin(AntFarm* farm) nothrow @nogc @system
    {
        if (farm is null) fatal("actor wave requires a Farm");
        immutable previous = atomicLoad!(MemoryOrder.acq)(generation_);
        if (previous != 0
                && (atomicLoad!(MemoryOrder.acq)(status_)
                    & waveFinishedBit) == 0)
            fatal("reuse unfinished actor wave");
        immutable next = previous + 1;
        if (next == 0) fatal("actor wave generation wrap");

        // Invalidate old observers while resetting the reusable descriptor.
        atomicStore!(MemoryOrder.rel)(generation_, 0UL);
        atomicStore!(MemoryOrder.raw)(tablesPublished_, 0UL);
        atomicStore!(MemoryOrder.raw)(tablesCompleted_, 0UL);
        atomicStore!(MemoryOrder.raw)(status_, 0U);
        farm_ = farm;
        membersHead_ = null;
        tableCompletion_.context = cast(void*) &this;
        tableCompletion_.call = &actorWaveTableCompleted;
        atomicStore!(MemoryOrder.rel)(generation_, next);
    }

    /// Current observation handle. It may be handed to a polling
    /// orchestrator or used as the predicate for a scheduler-side waiter.
    @property ActorWaveHandle handle() nothrow @nogc @system
    {
        ActorWaveHandle result;
        result.wave_ = &this;
        result.generation_ = atomicLoad!(MemoryOrder.acq)(generation_);
        return result;
    }

    /// Publish at most one physical table from `actors`. `operation` must be
    /// `void function(scope ref ActorBorrow!T) nothrow @nogc @system`.
    /// The return value is the actor prefix accepted by that table; callers
    /// advance by it and retry on backpressure. A zero return with `failed`
    /// set means an actor was stale, non-idle, duplicated, or from another
    /// Farm, and this wave should be sealed and discarded.
    size_t publish(alias operation, T)(scope ActorHandle!T[] actors,
            ref Token token, uint avgCost = 2) nothrow @nogc @system
    {
        static assert(is(typeof(&operation) : void function(
                scope ref ActorBorrow!T) nothrow @nogc @system),
            "wave operation must accept scope ref ActorBorrow!T and be nothrow @nogc @system");
        if (actors.length == 0) return 0;
        if (atomicLoad!(MemoryOrder.acq)(generation_) == 0)
            fatal("publish uninitialized actor wave");
        if ((atomicLoad!(MemoryOrder.acq)(status_) & waveSealedBit) != 0)
            fatal("publish sealed actor wave");

        size_t reserved;
        foreach (i, actor; actors)
        {
            if (!reserveActorWavePayload(actor.slot_, actor.generation_,
                    farm_, T.sizeof, T.alignof, cast(void*) &this,
                    membersHead_))
            {
                foreach_reverse (j; 0 .. reserved)
                {
                    if (membersHead_ !is actors[j].slot_)
                        fatal("actor wave member list corrupt");
                    membersHead_ = membersHead_.waveNext;
                    cancelActorWavePayload(actors[j].slot_,
                        actors[j].generation_, cast(void*) &this);
                }
                setStatusBits(waveFailedBit);
                return 0;
            }
            reserved = i + 1;
        }

        immutable oldLength = atomicFetchAdd!(MemoryOrder.acq_rel)(
            tablesPublished_, 1UL);
        if (oldLength == ulong.max) fatal("actor wave table count wrap");

        PayloadHeader header;
        header.maxCs = 1;
        header.done = 1;
        header.plen = 3;
        header.call = &actorWavePayloadCallback!(T, operation);
        auto bodies = ActorWaveBodyRange!T(actors, &this);
        immutable written = cast(size_t) farm_.writeTracked(header, bodies,
            3, &tableCompletion_, token, avgCost);

        foreach_reverse (i; written .. reserved)
        {
            if (membersHead_ !is actors[i].slot_)
                fatal("actor wave member list corrupt");
            membersHead_ = membersHead_.waveNext;
            cancelActorWavePayload(actors[i].slot_, actors[i].generation_,
                cast(void*) &this);
        }
        if (written == 0)
        {
            immutable before = atomicFetchSub!(MemoryOrder.acq_rel)(
                tablesPublished_, 1UL);
            if (before == 0) fatal("actor wave table count underflow");
        }
        return written;
    }

    /// Close publication. Equality between Wprogress and Wlen becomes a
    /// completion signal only after this seal; either the sealer or the last
    /// table consumer may win the exactly-once finished transition.
    ActorWaveHandle seal() nothrow @nogc @system
    {
        if (atomicLoad!(MemoryOrder.acq)(generation_) == 0)
            fatal("seal uninitialized actor wave");
        auto observed = atomicLoad!(MemoryOrder.acq)(status_);
        while (true)
        {
            if ((observed & waveSealedBit) != 0)
                fatal("seal actor wave twice");
            immutable replacement = observed | waveSealedBit;
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &status_, observed, replacement))
                break;
            observed = atomicLoad!(MemoryOrder.acq)(status_);
        }
        tryFinish();
        return handle;
    }

private:
    void setStatusBits(uint bits) nothrow @nogc @system
    {
        auto observed = atomicLoad!(MemoryOrder.acq)(status_);
        while ((observed & bits) != bits)
        {
            immutable replacement = observed | bits;
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &status_, observed, replacement))
                return;
            observed = atomicLoad!(MemoryOrder.acq)(status_);
        }
    }

    void tableCompleted() nothrow @nogc @system
    {
        immutable before = atomicFetchAdd!(MemoryOrder.acq_rel)(
            tablesCompleted_, 1UL);
        immutable length = atomicLoad!(MemoryOrder.acq)(tablesPublished_);
        if (before >= length)
            fatal("actor wave progress exceeds published tables");
        tryFinish();
    }

    void tryFinish() nothrow @nogc @system
    {
        if (atomicLoad!(MemoryOrder.acq)(tablesCompleted_)
                != atomicLoad!(MemoryOrder.acq)(tablesPublished_))
            return;
        auto observed = atomicLoad!(MemoryOrder.acq)(status_);
        while ((observed & waveSealedBit) != 0
                && (observed & (waveFinishingBit | waveFinishedBit)) == 0)
        {
            immutable replacement = observed | waveFinishingBit;
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &status_, observed, replacement))
            {
                releaseMembers();
                setStatusBits(waveFinishedBit);
                return;
            }
            observed = atomicLoad!(MemoryOrder.acq)(status_);
        }
    }

    void releaseMembers() nothrow @nogc @system
    {
        while (membersHead_ !is null)
        {
            auto slot = membersHead_;
            membersHead_ = slot.waveNext;
            releaseActorWaveMembership(slot, cast(void*) &this);
        }
    }

    void failActor() nothrow @nogc @system
    {
        setStatusBits(waveFailedBit);
    }
}

private struct ActorWaveBodyRange(T)
{
    ActorHandle!T[] actors_;
    ActorWave* wave_;
    ulong[3] scratch_;

    @property bool empty() const pure nothrow @nogc @safe
    {
        return actors_.length == 0;
    }

    @property PayloadBody front() nothrow @nogc @system
    {
        scratch_[0] = cast(ulong) cast(void*) actors_[0].slot_;
        scratch_[1] = actors_[0].generation_;
        scratch_[2] = cast(ulong) cast(void*) wave_;
        return cast(PayloadBody) scratch_[];
    }

    void popFront() nothrow @nogc @system
    {
        actors_ = actors_[1 .. $];
    }

    @property ActorWaveBodyRange save() nothrow @nogc @system
    {
        return this;
    }

    @property size_t length() const pure nothrow @nogc @safe
    {
        return actors_.length;
    }
}

private ulong actorWaveBodyWord(PayloadBody body, size_t index)
    nothrow @nogc @system
{
    return atomicLoad!(MemoryOrder.raw)(
        *cast(shared ulong*) (body.ptr + index));
}

private long actorWavePayloadCallback(T, alias operation)(PayloadHeader*,
        PayloadBody body, ulong) nothrow @nogc @system
{
    if (body.length != 3)
        fatal("actor wave payload length corrupt");
    auto slot = cast(ActorSlot*) cast(void*) actorWaveBodyWord(body, 0);
    immutable generation = actorWaveBodyWord(body, 1);
    auto wave = cast(ActorWave*) cast(void*) actorWaveBodyWord(body, 2);
    if (slot is null || wave is null)
        fatal("actor wave payload identity corrupt");
    if (!dispatchActorWavePayload!(T, operation)(slot, generation))
        wave.failActor();
    return 1;
}

private void actorWaveTableCompleted(void* context) nothrow @nogc @system
{
    auto wave = cast(ActorWave*) context;
    if (wave is null) fatal("actor wave completion lost owner");
    wave.tableCompleted();
}
