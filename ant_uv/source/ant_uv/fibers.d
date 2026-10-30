/++
 + Fibers and `UvLoop`: a private Fiber domain served on the loop thread
 + (`UvFibers`), and the wait a managed Fiber anywhere uses for a request
 + (`submitAndWait`).
 +
 + `UvFibers` is the shape for logic that should live beside the loop, such
 + as a resource manager: a private Farm with the loop thread as its only
 + consumer and producer, and a `FiberDomain` whose Director is the loop
 + thread. Its Fibers wait on `FiberEvent`s that the loop's completion sink
 + sets on the same thread, so suspended I/O work resumes where it was
 + issued. It does not touch the worker pool's `GroupHost`.
 +/
module ant_uv.fibers;

import ant_fibers.scheduler;
import ant_uv.loop;
import antfarm;
import core.thread : Thread;
import core.time : Duration, MonoTime, seconds;

/// In a managed Fiber: submit `request` and park on `request.event` until
/// the loop publishes it. A full request ring yields the Fiber and retries,
/// so the caller never spins a thread the loop or the pool needs. Returns
/// `accepted` once the request is published (then read `request.state`), or
/// the refusal (`closed`, `invalid`) with the request still the caller's.
/// `request.event` must be a `FiberEvent` of the calling Fiber's domain.
///
/// Cancelling the Fiber while it waits cancels the request and waits for
/// its publication before rethrowing `FiberCancelled`: a Fiber's
/// cancellation is sticky, so it cannot park again, and the loop must never
/// outlive a request on the Fiber's stack. On the loop thread the wait
/// serves the loop; on any other thread it holds the thread in short sleeps.
/// Queued work and timers publish at once on cancel; file work libuv already
/// started holds the wait until it finishes.
UvSubmit submitAndWait(UvLoop loop, UvRequest* request)
{
    import core.time : usecs;
    if (request is null || request.event is null)
        throw new Exception("ant_uv: submitAndWait needs a request with an event");
    for (;;)
    {
        immutable r = loop.submit(request);
        if (r == UvSubmit.accepted) break;
        if (r != UvSubmit.full) return r;
        FiberDomain.yieldReady(); // a cancellation here leaves the request the caller's
    }
    try
    {
        // An auto-reset event may carry a post from an earlier request: the
        // state, not the wake, is the authority.
        while (!request.finished)
            request.event.wait();
    }
    catch (FiberCancelled cancelled)
    {
        if (loop.onLoopThread)
            loop.serveUntilPublished(request);
        else
        {
            while (!request.finished && !loop.cancel(request))
                Thread.sleep(50.usecs);
            while (!request.finished)
                Thread.sleep(50.usecs);
        }
        throw cancelled;
    }
    return UvSubmit.accepted;
}

/// A ready lane on the loop thread's private Farm whose wakes reach the loop.
private final class UvFiberLane : FiberReadyLane
{
    private UvLoop loop_;

    this(UvLoop loop, AntFarm* farm)
    {
        super(farm);
        loop_ = loop;
    }

    protected override void noteExternalReady() nothrow @nogc { loop_.wake(); }
    protected override void noteCancellationRequested() nothrow @nogc { loop_.wake(); }
    protected override void noteTimerArmed() nothrow @nogc { loop_.wake(); }
    protected override void noteFlushed(size_t written, bool refused) nothrow @nogc
    {
        if (refused) loop_.wake();
    }
}

/// Fibers on the loop thread. Build it in the loop's `start` hook, return
/// its `step` from the loop's `step` hook, and call `shutdown` from the
/// loop's `stop` hook. Every member is loop-thread only except `domain`'s
/// own cross-thread operations (events, cancellation requests).
final class UvFibers
{
    private UvLoop loop_;
    private AntFarm* farm_;
    private Token token_;
    private ConsumerView view_;
    private bool subscribed_;
    private FiberDomain domain_;
    private UvFiberLane lane_;
    private FiberServicePass pass_;
    private size_t consumeBudget_;

    /// A private Farm of `ringMiB` and a domain with `options`, on the loop
    /// thread of `loop`. `consumeBudget` bounds the Farm visits per step.
    this(UvLoop loop, FiberDomainOptions options = FiberDomainOptions.init,
        uint ringMiB = 1, size_t consumeBudget = 64)
    {
        if (loop is null || !loop.onLoopThread)
            throw new Exception("ant_uv: build UvFibers on the loop thread (the start hook)");
        if (consumeBudget == 0)
            throw new Exception("ant_uv: consumeBudget must be at least 1");
        loop_ = loop;
        consumeBudget_ = consumeBudget;
        FarmCreateError error;
        farm_ = AntFarm.tryCreate(ringMiB, 8, 1, 0, 0, 1, 4096,
            DEFAULT_SMALL_TABLE_THRESHOLD, false, &error);
        if (farm_ is null)
            throw new Exception("ant_uv: UvFibers Farm: " ~ error.detail.idup);
        scope (failure) release();
        token_ = farm_.registerProducer(Tier.small);
        if (!token_.valid)
            throw new Exception("ant_uv: UvFibers producer registration failed");
        if (view_.subscribe(farm_) < 0)
            throw new Exception("ant_uv: UvFibers view subscription failed");
        subscribed_ = true;
        domain_ = new FiberDomain(options);
        lane_ = new UvFiberLane(loop, farm_);
        domain_.attachReadyLane(lane_);
    }

    /// The domain: spawn into it, and make `FiberEvent`s for its Fibers.
    @property FiberDomain domain() nothrow @nogc { return domain_; }

    /// Spawn a Fiber that runs on the loop thread.
    FiberTask spawn(void delegate() body, size_t stackSize = 0)
    {
        auto task = domain_.spawn(body, stackSize);
        loop_.wake();
        return task;
    }

    /// ditto
    TaskHandle spawnHandle(void delegate() body, size_t stackSize = 0)
    {
        auto handle = domain_.spawnHandle(body, stackSize);
        loop_.wake();
        return handle;
    }

    /// One service pass and up to `consumeBudget` Farm visits: the loop's
    /// step hook. Reports the domain's earliest timer as the deadline.
    UvStepResult step()
    {
        auto served = lane_.serve(token_, pass_);
        bool consumed;
        foreach (i; 0 .. consumeBudget_)
        {
            if (!view_.consumeNext()) break;
            consumed = true;
        }
        return UvStepResult(served.progress || consumed, served.deadline);
    }

    /// Stop admission, cancel every active Fiber, run them out, retire, and
    /// release the Farm. The loop's stop hook. Throws when the Fibers do not
    /// finish within `timeout`.
    void shutdown(Duration timeout = 5.seconds)
    {
        if (domain_ is null) return;
        domain_.beginShutdown(true);
        immutable deadline = MonoTime.currTime + timeout;
        while (!domain_.drained)
        {
            if (!step().progress)
            {
                if (MonoTime.currTime >= deadline)
                    throw new Exception("ant_uv: UvFibers did not drain before shutdown");
                Thread.yield();
            }
        }
        cast(void) domain_.retire();
        domain_ = null;
        release();
    }

    private void release()
    {
        if (subscribed_)
        {
            view_.unsubscribe();
            subscribed_ = false;
        }
        if (token_.valid) farm_.unregisterProducer(token_);
        if (farm_ !is null) farm_.destroy();
        farm_ = null;
    }
}
