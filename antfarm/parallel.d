/++
 + Bounded parallel loops over finite, sliceable random-access ranges.
 +
 + `parallelRange` composes with ordinary Farm writes.
 + `parallelFor` owns asynchronous submission/completion state; retry its
 + `publish` method after backpressure until `submitted` is true.
 + `parallelForWait` and the `parallel` foreach adapter help-drain a caller's
 + subscribed view until the join. Blocking inside a Farm callback or table
 + completion hook is forbidden (fatal in assertion builds).
 +
 + All APIs are nothrow, @nogc and @system. Asynchronous inputs must be POD
 + without unshared mutable aliases. Referenced storage must outlive the join.
 + The synchronous foreach adapter permits borrowed mutable data and a scope
 + delegate; the caller must keep concurrent accesses disjoint or synchronized.
 + See docs/guide/parallel-loops.md for lifetime and progress contracts.
 +/
module antfarm.parallel;

public import antfarm;
import antfarm.templates : callbackFor, argsConvert, payloadRange, PayloadGrainRange;
import core.atomic;
import core.stdc.stdlib : malloc, free;
import core.stdc.string : memcpy;
import core.thread : Thread;
import std.range.primitives : ElementType, hasLength, hasSlicing,
    isRandomAccessRange;
import std.traits : hasUnsharedAliasing, Parameters;

/// Scheduling hints for a parallel region.
struct Options
{
    size_t grain = 1;                    /// Elements per ST callback; must be positive.
    uint costClass = DEFAULT_AVG_COST;   /// Farm avgCost, in 0 .. MAX_AVG_COST.
}

private enum suitableRange(R) = isRandomAccessRange!R && hasLength!R && hasSlicing!R;

private void validateRange(R, bool borrowed = false)() nothrow @nogc @system
{
    static assert(suitableRange!R, "parallel loops require a finite sliceable random-access range");
    static assert(__traits(isPOD, R) && R.alignof <= ulong.alignof,
        "parallel range descriptors must be POD with alignment at most 8");
    static if (!borrowed)
        static assert(!hasUnsharedAliasing!R,
            "asynchronous parallel ranges must not contain unshared mutable aliases");
}

private void validateFunction(alias fn, R)() nothrow @nogc @system
{
    static assert(Parameters!fn.length == 1 && argsConvert!(fn, ElementType!R),
        "parallel callback must accept one range element without narrowing");
    // Reuse the packing API's complete signature/aliasing checks.
    auto checked = callbackFor!fn();
}

private size_t ceilDiv(size_t n, size_t d) pure nothrow @nogc @safe
{
    return n / d + (n % d != 0);
}

private T loadBody(T)(PayloadBody body) nothrow @nogc @system
{
    enum words = (T.sizeof + 7) / 8;
    ulong[words] copy;
    foreach (i; 0 .. words)
        copy[i] = atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*) (body.ptr + i));
    T value = void;
    memcpy(&value, copy.ptr, T.sizeof);
    return value;
}

/// Grain adapter shared with the packed-argument ST helpers.
alias ParallelRange(alias fn, R) = PayloadGrainRange!(fn, R);

/// One ST payload per element, using minimal argument packing.
auto parallelRange(alias fn, R)(R source) nothrow @nogc @system
    if (suitableRange!R)
{
    validateRange!R();
    validateFunction!(fn, R)();
    return payloadRange!fn(source);
}

/// Each ST payload executes up to grain consecutive elements.
auto parallelRange(alias fn, R)(R source, size_t grain) nothrow @nogc @system
    if (suitableRange!R)
{
    validateFunction!(fn, R)();
    return payloadRange!fn(source, grain);
}

// The producer owns the input cursor. Consumers only read source/options and
// mutate the completion credits. A producer credit prevents completion while
// write is still returning its accepted prefix.
private struct Region(R)
{
    AntFarm* farm;
    R source;
    Options options;
    size_t count;
    size_t next;
    shared size_t credits = 1;
    shared uint finished;
    shared uint references = 2; // handle plus outstanding execution
    bool heapOwned;
    bool submitted;
    TableCompletionHook userHook;
    TableCompletionHook tableHook;
    void function(Region*, size_t, size_t) nothrow @nogc @system execute;
    int delegate(ElementType!R) nothrow @nogc @system body;
}

private void releaseRegion(R)(Region!R* state) nothrow @nogc @system
{
    if (atomicFetchSub!(MemoryOrder.acq_rel)(state.references, 1U) == 1) free(state);
}

private void finishCredit(R)(Region!R* state) nothrow @nogc @system
{
    if (atomicFetchSub!(MemoryOrder.acq_rel)(state.credits, size_t(1)) != 1) return;
    // The acq_rel chain joins all table finishers, and
    // publication's final producer credit. Snapshot the hook before invoking.
    auto hook = state.userHook;
    immutable heapOwned = state.heapOwned;
    if (hook.valid) hook.call(hook.context);
    atomicStore!(MemoryOrder.rel)(state.finished, 1U);
    // For a stack region the release store above is the final access.
    if (heapOwned) releaseRegion(state);
}

private void tableFinished(R)(void* context) nothrow @nogc @system
{
    finishCredit(cast(Region!R*) context);
}

private void initialize(R)(Region!R* state, AntFarm* farm, R source, Options options,
    TableCompletionHook* done) nothrow @nogc @system
{
    if (farm is null) fatal("parallel region requires a Farm");
    if (options.grain == 0) fatal("parallel grain must be positive");
    if (options.costClass > MAX_AVG_COST) fatal("parallel costClass out of range");
    state.farm = farm;
    state.source = source;
    state.options = options;
    if (done !is null) state.userHook = *done;
    state.tableHook = TableCompletionHook(state, &tableFinished!R);
    immutable n = cast(size_t) source.length;
    state.count = ceilDiv(n, options.grain);
}

private struct Submission(R)
{
    Region!R* state;
    size_t position;
    PayloadHeader header;
    ulong[2] words;

    this(Region!R* s) nothrow @nogc @system
    {
        state = s;
        position = s.next;
        header.plen = words.length;
        header.call = &invoke;
    }
    enum ulong fixedPayloadLength = 2;
    @property PayloadHeader* commonPayloadHeader() nothrow @nogc @system { return &header; }
    @property bool empty() nothrow @nogc @system { return position == state.count; }
    @property size_t length() nothrow @nogc @system { return state.count - position; }
    @property typeof(this) save() nothrow @nogc @system { return this; }
    @property PayloadEntry front() nothrow @nogc @system
    {
        words[0] = cast(ulong) state;
        words[1] = position * state.options.grain;
        return PayloadEntry(&header, words[]);
    }
    void popFront() nothrow @nogc @system { ++position; }

    static long invoke(PayloadHeader*, PayloadBody body)
        nothrow @nogc @system
    {
        auto values = loadBody!(typeof(words))(body);
        auto state = cast(Region!R*) values[0];
        immutable first = cast(size_t) values[1];
        immutable remaining = cast(size_t) state.source.length - first;
        immutable grain = state.options.grain;
        state.execute(state, first, first + (grain < remaining ? grain : remaining));
        return 0;
    }
}

// Return on the first full write; never spin or consume from a callback.
private bool publishRegion(R)(Region!R* state, ref Token token) nothrow @nogc @system
{
    if (state.submitted) return true;
    while (state.next != state.count)
    {
        auto entries = Submission!R(state);
        atomicFetchAdd!(MemoryOrder.raw)(state.credits, size_t(1));
        immutable accepted = cast(size_t) state.farm.write(entries, token,
            state.options.costClass, &state.tableHook);
        if (accepted == 0) finishCredit(state); // undo the unadmitted table
        if (accepted == 0) return false;
        state.next += accepted;
    }
    state.submitted = true;
    finishCredit(state); // relinquish the producer credit; final access
    return true;
}

/++
 + Move-only owner of an asynchronous region. `publish` retries any remaining
 + input after backpressure; it never waits for space. The handle may be dropped
 + after `submitted`, and execution retains its state until the completion hook
 + returns. Dropping an incompletely submitted handle is fatal, since doing so
 + would abandon work. Keep the Farm and referenced input/output alive through
 + completion. Only the producer thread may access the handle.
 +/
struct ParallelHandle(R)
{
    private Region!R* state;
    @disable this(this);
    private this(Region!R* s) nothrow @nogc @system { state = s; }

    /// Release the handle's ownership; submitted work retains its own reference.
    ~this() nothrow @nogc @system
    {
        if (state is null) return;
        if (!state.submitted) fatal("parallel handle dropped before submission completed");
        releaseRegion(state);
    }
    /// True once all entries have reached the Farm.
    @property bool submitted() nothrow @nogc @system { return state is null || state.submitted; }
    /// Acquire the joined callback writes and completion hook's return.
    @property bool complete() nothrow @nogc @system
    {
        return state is null || atomicLoad!(MemoryOrder.acq)(state.finished) != 0;
    }
    /// Publish until admitted or backpressured. Returns submitted.
    bool publish(ref Token token) nothrow @nogc @system
    {
        return state is null || publishRegion(state, token);
    }
}

private void executeFunction(alias fn, R)(Region!R* state, size_t first, size_t last)
    nothrow @nogc @system
{
    foreach (i; first .. last) fn(state.source[i]);
}

/// Start an asynchronous region with one malloc allocation. The hook is copied,
/// fires exactly once (also for empty input), and may run before this returns.
/// Retry handle.publish(token) on backpressure; consumers must be driven/woken
/// by the caller's host. This function never waits or re-enters a consumer view.
ParallelHandle!R parallelFor(alias fn, R)(AntFarm* farm, R source, ref Token token,
    Options options = Options.init, TableCompletionHook* done = null)
    nothrow @nogc @system if (suitableRange!R)
{
    validateRange!R();
    validateFunction!(fn, R)();
    auto state = cast(Region!R*) malloc(Region!R.sizeof);
    if (state is null) fatal("parallel region allocation failed");
    *state = Region!R.init;
    state.heapOwned = true;
    initialize(state, farm, source, options, done);
    state.execute = &executeFunction!(fn, R);
    auto handle = ParallelHandle!R(state);
    handle.publish(token);
    return handle;
}

private void checkWait(ref ConsumerView view) nothrow @nogc @system
{
    version (assert)
        if (inFarmCallback()) fatal("blocking parallel loop inside a Farm callback");
    if (view.F is null || !view.hasRef) fatal("parallel wait requires a subscribed view");
}

private void waitRegion(R)(Region!R* state, ref Token token, ref ConsumerView view)
    nothrow @nogc @system
{
    while (!publishRegion(state, token))
        if (!view.consumeNext()) Thread.yield();
    while (atomicLoad!(MemoryOrder.acq)(state.finished) == 0)
        if (!view.consumeNext()) Thread.yield();
}

/// Publish with retry and help-drain view until all callbacks have returned.
/// The region state lives on the caller's stack; no allocation is needed.
void parallelForWait(alias fn, R)(R source, ref Token token, ref ConsumerView view,
    Options options = Options.init) nothrow @nogc @system if (suitableRange!R)
{
    checkWait(view);
    validateRange!R();
    validateFunction!(fn, R)();
    Region!R state;
    initialize(&state, view.F, source, options, null);
    state.execute = &executeFunction!(fn, R);
    waitRegion(&state, token, view);
}

private void executeDelegate(R)(Region!R* state, size_t first, size_t last)
    nothrow @nogc @system
{
    foreach (i; first .. last)
    {
        if (state.body(state.source[i]) != 0)
            fatal("parallel foreach bodies cannot break or return");
    }
}

/// Synchronous foreach region. The scope delegate may borrow the caller's
/// frame; it must be nothrow @nogc. Break, nonlocal return, and goto out of
/// the body are forbidden: their lowered delegates share unsynchronized
/// control/return storage. A nonzero delegate result is fatal in all builds.
struct ParallelLoop(R)
{
    private ConsumerView* view;
    private Token* token;
    private R source;
    private Options options;

    /// Invoke and join every loop body; the delegate must always return zero.
    int opApply(scope int delegate(ElementType!R) nothrow @nogc @system body)
        nothrow @nogc @system
    {
        checkWait(*view);
        validateRange!(R, true)();
        Region!R state;
        initialize(&state, view.F, source, options, null);
        state.body = body;
        state.execute = &executeDelegate!R;
        waitRegion(&state, *token, *view);
        return 0;
    }
}

/// D foreach adapter: foreach (i; parallel(view, token, iota(n), options)).
auto parallel(R)(ref ConsumerView view, ref Token token, R source,
    Options options = Options.init) nothrow @nogc @system if (suitableRange!R)
{
    return ParallelLoop!R(&view, &token, source, options);
}
