/++
 + Phase-oriented actor dispatch built from Ant Farm payload tables.
 +
 + Responsibility: one bulk-synchronous phase over one or more physical
 + tables, with a sealed, generation-tagged completion. It depends on
 + `antfarm` and the package wave entry points in `antfarm.actors.actor`.
 + Canonical docs: docs/guide/public-actors.md#waves,
 + docs/reference/actor-lifecycle.md and
 + docs/design/public-actor-ordering.md; docs/README.md is the map.
 + Section map: completion callback/hook/handle, ActorWave, then the private
 + payload body range and callback. The AntfarmActorTestHooks seam is
 + production-elided.
 +
 + One ActorWave may publish several physical tables. Each table's
 + Tprogress-to-Tlen transition advances the wave once; sealing the wave
 + completes it when every published table has advanced. Actor operations
 + are phase-specific and receive only a callback-local ActorBorrow, so they
 + cannot republish or retire themselves through ActorContext.
 +/
module antfarm.actors.wave;

public import antfarm.actors.actor : ActorBorrow, ActorHandle;

import antfarm : AntFarm, PayloadBody, Callback, TableCompletionHook,
    Tier, Token, avgCostForChunk, fatal;
import antfarm.actors.actor : ActorSlot, cancelActorWavePayload,
    dispatchActorWavePayload, releaseActorWaveMembership,
    reserveActorWavePayload;
import core.atomic : MemoryOrder, atomicFetchAdd, atomicFetchSub, atomicLoad,
    atomicStore, cas;
version (AntfarmActorTestHooks)
    import antfarm.actors.actor : ActorTestPoint, actorTestPoint;

private enum uint waveSealedBit = 1U << 0;
private enum uint waveFinishedBit = 1U << 1;
private enum uint waveFailedBit = 1U << 2;
private enum uint waveInterruptedBit = 1U << 3;
// Cost class of a published wave table: claims of 8 entries.
private enum uint publishAvgCost = avgCostForChunk(8);

// ==== Section: completion callback, hook and handle ====
/// Signature of ActorWaveCompletionHook.call: invoked exactly once, after
/// the wave has published `finished`, with the hook's context.
alias ActorWaveCompletionCallback = void function(void* context)
    nothrow @nogc @system;

/// Optional exactly-once notification issued after a wave has release-
/// published `finished`. `finished` does not mean this callback has returned:
/// the owner may observe it and `begin` a new generation while the old
/// callback is still running. The completion context must outlive the
/// callback's last access. The callback must not destroy the wave storage
/// before it returns, and an old callback must not touch a reused descriptor
/// without separate synchronization.
struct ActorWaveCompletionHook
{
    /// Opaque argument passed back to `call`.
    void* context;
    /// Invoked exactly once, after the wave's finished publication.
    ActorWaveCompletionCallback call;
}

/// Copyable observation capability for a stable ActorWave. An acquire load
/// that observes `finished` also observes every actor operation in the wave.
/// The owning ActorWave storage must outlive all of its handles.
struct ActorWaveHandle
{
    private ActorWave* wave_;
    private ulong generation_;

    /// The generation this handle observes.
    @property ulong generation() const pure nothrow @nogc @safe
    {
        return generation_;
    }

    /// True while the observed generation still owns the wave storage.
    @property bool valid() const nothrow @nogc @system
    {
        if (wave_ is null || generation_ == 0) return false;
        return atomicLoad!(MemoryOrder.acq)(wave_.generation_) == generation_;
    }

    /// True when the observed generation has finished. An acquire load that
    /// observes finished also observes every actor operation in the wave;
    /// completion-hook tails may still be running.
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

    /// True when admission rejected a stale, non-idle, duplicate or foreign
    /// actor; discard the wave after draining. Observed wake/send/retirement
    /// interference sets interrupted instead, and does not fail an operation.
    @property bool failed() const nothrow @nogc @system
    {
        if (wave_ is null || generation_ == 0) return false;
        immutable before = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        if (before != generation_) return false;
        immutable status = atomicLoad!(MemoryOrder.acq)(wave_.status_);
        immutable after = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        return after == generation_ && (status & waveFailedBit) != 0;
    }

    /// True when an executed operation observed a concurrent wake, send or
    /// retirement request and handed it to autonomous dispatch after its
    /// borrow ended. Sticky for this generation; independent of failed.
    /// Finished does not join that autonomous work. Requests arriving after
    /// the operation's exit transition need not set this diagnostic bit.
    @property bool interrupted() const nothrow @nogc @system
    {
        if (wave_ is null || generation_ == 0) return false;
        // Match failed's acquire generation sandwich: reject a reused handle
        // and acquire the status publication for this observed generation.
        immutable before = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        if (before != generation_) return false;
        immutable status = atomicLoad!(MemoryOrder.acq)(wave_.status_);
        immutable after = atomicLoad!(MemoryOrder.acq)(wave_.generation_);
        return after == generation_ && (status & waveInterruptedBit) != 0;
    }

    /// Tables completed by the observed generation so far.
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

    /// Tables published by the observed generation so far.
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

// ==== Section: ActorWave ====
/// Stable, reusable owner of one actor-wave generation. `begin` and
/// `publish` are single-orchestrator operations. Consumers may complete
/// tables concurrently, and any number of observers may poll handles.
///
/// A wave operation is accepted only when every offered actor is idle. The
/// reservation is made before Farm publication, preventing another wave or
/// autonomous activation from borrowing that actor generation in parallel.
/// Intrusive membership and its lifetime pin remain until aggregate finish,
/// including after an individual actor's table has completed.
/// See_Also:
///     docs/guide/public-actors.md (waves), docs/design/public-actor-ordering.md
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
    ActorWaveCompletionHook completion_;
    ActorSlot* membersHead_;
    shared ulong pendingCredits_; // producer hold + physical table hooks

public:
    version (AntfarmActorTestHooks)
    {
        /// Preset a finished wave's generation with no concurrent descriptor
        /// accesses or completion callbacks. The next begin increments it.
        void testPresetGeneration(ulong generation) nothrow @nogc @system
        {
            // Debug-only test-hook precondition, outside production wave paths.
            version (assert)
                if (!handle.finished || generation == 0)
                    fatal("preset actor wave generation requires a finished wave and nonzero generation");
            atomicStore!(MemoryOrder.raw)(generation_, generation);
        }
    }

    /// Start the first generation, or reuse this storage after its previous
    /// generation finished. Reuse invalidates every older handle.
    void begin(AntFarm* farm,
            ActorWaveCompletionHook completion = ActorWaveCompletionHook.init)
        nothrow @nogc @system
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
        atomicStore!(MemoryOrder.raw)(pendingCredits_, 1UL);
        farm_ = farm;
        completion_ = completion;
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
    /// Farm, and this wave should be sealed and discarded. `avgCost` is the
    /// table's Farm work-cost class (`0 .. MAX_AVG_COST`, consumers claim
    /// `MAX_CHUNK >> avgCost` entries at a time); the default claims 8.
    size_t publish(alias operation, T)(scope ActorHandle!T[] actors,
            ref Token token, uint avgCost = publishAvgCost) nothrow @nogc @system
    {
        static assert(is(typeof(&operation) : void function(
                scope ref ActorBorrow!T) nothrow @nogc @system),
            "wave operation must accept scope ref ActorBorrow!T and be nothrow @nogc @system");
        if (actors.length == 0) return 0;
        if (atomicLoad!(MemoryOrder.acq)(generation_) == 0)
            fatal("publish uninitialized actor wave");
        if ((atomicLoad!(MemoryOrder.acq)(status_) & waveSealedBit) != 0)
            fatal("publish sealed actor wave");

        immutable oldLength = atomicFetchAdd!(MemoryOrder.acq_rel)(
            tablesPublished_, 1UL);
        if (oldLength == ulong.max) fatal("actor wave table count wrap");
        atomicFetchAdd!(MemoryOrder.acq_rel)(pendingCredits_, 1UL);

        Callback header;

        header = &actorWavePayloadCallback!(T, operation);
        auto bodies = ActorWaveBodyRange!T(actors, &this);
        immutable written = cast(size_t) farm_.write(header, bodies, 3, token, avgCost, &tableCompletion_);
        if (written == 0)
        {
            immutable before = atomicFetchSub!(MemoryOrder.acq_rel)(
                tablesPublished_, 1UL);
            if (before == 0) fatal("actor wave table count underflow");
            releaseCredit(); // producer hold prevents completion here
        }
        return written;
    }

    /// Close publication. Equality between Wprogress and Wlen is diagnostic
    /// only: the producer hold and every table notification must
    /// release their credits before the exactly-once finished transition.
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
        auto result = handle;
        releaseCredit(); // final descriptor access; may publish finished
        return result;
    }

private:
    // Call only while holding the producer credit or before the payload's
    // table credit is released. The acq_rel CAS preserves concurrent sticky
    // bits; the credit release/acquire chain joins every update before the
    // final FINISHED release store. No status writer may outlive its credit.
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
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.waveTableProgressRecorded, null);
        releaseCredit();
    }

    // Every participant drops its credit as its final descriptor access.
    // Equality of public progress counters is not a callback-lifetime fence.
    void releaseCredit() nothrow @nogc @system
    {
        immutable before = atomicFetchSub!(MemoryOrder.acq_rel)(pendingCredits_, 1UL);
        if (before == 0) fatal("actor wave credit underflow");
        if (before == 1)
        {
            auto completion = completion_;
            immutable status = atomicLoad!(MemoryOrder.raw)(status_);
            if ((status & waveSealedBit) == 0) fatal("finish unsealed actor wave");
            releaseMembers();
            atomicStore!(MemoryOrder.rel)(status_, status | waveFinishedBit);
            if (completion.call !is null)
                completion.call(completion.context);
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

// ==== Section: private payload body range and callback ====
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

    /// Called by the Farm after it has computed the exact physical-table
    /// prefix but before it reserves ring space. This avoids reserving and
    /// then cancelling every actor in the caller's unwritten tail.
    bool prepareTableFrontN(size_t count) nothrow @nogc @system
    {
        if (count > actors_.length)
            fatal("actor wave preparation exceeds member range");
        size_t reserved;
        foreach (i; 0 .. count)
        {
            auto actor = actors_[i];
            if (!reserveActorWavePayload(actor.slot_, actor.generation_,
                    wave_.farm_, T.sizeof, T.alignof, cast(void*) wave_,
                    wave_.membersHead_))
            {
                foreach_reverse (j; 0 .. reserved)
                {
                    if (wave_.membersHead_ !is actors_[j].slot_)
                        fatal("actor wave member list corrupt");
                    wave_.membersHead_ = wave_.membersHead_.waveNext;
                    cancelActorWavePayload(actors_[j].slot_,
                        actors_[j].generation_, cast(void*) wave_);
                }
                wave_.failActor();
                return false;
            }
            reserved = i + 1;
        }
        return true;
    }
}

private ulong actorWaveBodyWord(PayloadBody body, size_t index)
    nothrow @nogc @system
{
    return atomicLoad!(MemoryOrder.raw)(
        *cast(shared ulong*) (body.ptr + index));
}

private extern(C) void actorWavePayloadCallback(T, alias operation)(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    auto body = bodyPtr[0 .. bodyWords];
    if (body.length != 3)
        fatal("actor wave payload length corrupt");
    auto slot = cast(ActorSlot*) cast(void*) actorWaveBodyWord(body, 0);
    immutable generation = actorWaveBodyWord(body, 1);
    auto wave = cast(ActorWave*) cast(void*) actorWaveBodyWord(body, 2);
    if (slot is null || wave is null)
        fatal("actor wave payload identity corrupt");
    if (!dispatchActorWavePayload!(T, operation)(slot, generation))
        wave.setStatusBits(waveInterruptedBit);
    return;
}

private extern(C) void actorWaveTableCompleted(void* context) nothrow @nogc @system
{
    auto wave = cast(ActorWave*) context;
    if (wave is null) fatal("actor wave completion lost owner");
    wave.tableCompleted();
}
