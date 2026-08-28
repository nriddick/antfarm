/++
 + Ant Farm templates: generate type-erased callbacks and packed payload
 + entries from ordinary D functions.
 +
 + The function's parameters are serialized, in declaration order, into the
 + payload body as contiguous ulongs. Each parameter occupies
 + ceil(T.sizeof / 8) ulongs; the callback reconstructs the parameter by
 + casting the body slot back to the parameter type.
 +
 + With `withIteration = true`, the function's last parameter must be
 + `ulong`; it is not packed into the body but receives the callback's
 + `iteration` argument instead. Use that for multithreaded payloads
 + (`done > 1`) that need to know which iteration they are running.
 +
 + Restrictions (enforced by static asserts where possible):
 +   - The function must be callable as nothrow @nogc @system.
 +   - Packed parameters must be plain by-value (no ref/out/lazy/in/scope/
 +     return), POD, with no unshared aliasing (mutable pointers/dynamic
 +     arrays are rejected; immutable references such as `string` or
 +     `immutable(int)*` are allowed and copied by reference), and with
 +     alignment <= 8.
 +   - The function must return void or a type implicitly convertible to long.
 +
 + `antfarm_templates` publicly imports `antfarm`, so importing this
 + module also brings in `AntFarm`, `ConsumerView`, `PayloadHeader`,
 + `PayloadEntry`, `Token`, and `Tier`.
 +
 + The produced PayloadEntry's header is initialized in place: `call` is the
 + generated type-erased callback, `maxCs`/`done` are the values given to
 + `payloadEntry`, and `plen` is the packed parameter body length in ulongs.
 + The body slice aliases `buf[0 .. packedLen!fn]`; `write()` copies both
 + header and body into the ring before returning, so `buf` and `header` only
 + need to stay alive until `write()` returns.
 +
 + Note: for reference-like parameters that pass the no-unshared-aliasing
 + rule (notably `string`), the body stores the reference, not the referred
 + data. The referred data must stay alive until the job executes.
 +/
module antfarm_templates;

public import antfarm;
import core.atomic;
import core.stdc.string : memcpy;
import std.traits : Parameters, ReturnType, Unqual, fullyQualifiedName,
    hasUnsharedAliasing;
import std.range.primitives : ElementType, isInputRange;
import std.typecons : Tuple, tuple;

/// Packed body length, in ulongs, of `fn`'s packed parameter list.
/// With `withIteration = true` the trailing `ulong` iteration parameter is
/// excluded.
template packedLen(alias fn, bool withIteration = false)
{
    enum size_t packedLen = packedLenImpl!(packedParams!(fn, withIteration));
}

private template packedLenImpl(T...)
{
    static if (T.length == 0)
        enum size_t packedLenImpl = 0;
    else
        enum size_t packedLenImpl = ((T[0].sizeof + 7) / 8) + packedLenImpl!(T[1 .. $]);
}

/// Offset, in ulongs, of packed parameter `i` inside the body.
/// With `withIteration = true` the trailing `ulong` iteration parameter is
/// excluded from the packing layout.
template paramOffset(alias fn, size_t i, bool withIteration = false)
{
    static if (i == 0)
        enum size_t paramOffset = 0;
    else
        enum size_t paramOffset =
            ((packedParams!(fn, withIteration)[i - 1].sizeof + 7) / 8) +
            paramOffset!(fn, i - 1, withIteration);
}

/// Returns the generated type-erased Callback for `fn`.
Callback callbackFor(alias fn, bool withIteration = false)() nothrow @nogc @system
{
    static assert(validSignature!(fn, withIteration),
        "antfarm_templates: unsupported function signature");
    return &callbackImpl!(fn, withIteration);
}

/// Initializes `h` as the header for a packed call to `fn`.
void initPayloadHeader(alias fn, bool withIteration = false)(
    PayloadHeader* h, uint maxCs, uint done) nothrow @nogc @system
{
    static assert(validSignature!(fn, withIteration),
        "antfarm_templates: unsupported function signature");
    if (h is null) fatal("initPayloadHeader: null header");
    *h = PayloadHeader.init;
    h.maxCs = maxCs;
    h.done = done;
    h.plen = packedLen!(fn, withIteration);
    h.call = &callbackImpl!(fn, withIteration);
}

/// Packs `args` into `buf` for `fn` and returns the PayloadEntry.
/// `buf` must have room for at least `packedLen!(fn, withIteration)` ulongs.
/// With `withIteration = true`, pass only the packed parameters; the
/// trailing `ulong` iteration argument is supplied by the callback.
PayloadEntry payloadEntry(alias fn, uint maxCs = 1, uint done = 1,
        bool withIteration = false, Args...)(
    PayloadHeader* header, ulong[] buf, Args args) nothrow @nogc @system
    if (Args.length == packedParams!(fn, withIteration).length)
{
    static assert(validSignature!(fn, withIteration),
        "antfarm_templates: unsupported function signature");
    if (header is null) fatal("payloadEntry: null header");
    if (buf.length < packedLen!(fn, withIteration))
        fatal("payloadEntry: body buffer too small");
    initPayloadHeader!(fn, withIteration)(header, maxCs, done);
    packArgsImpl!(fn, withIteration, 0)(buf, args);
    return PayloadEntry(header, buf[0 .. packedLen!(fn, withIteration)]);
}

/// Packs `args` into `buf` for `fn` and returns the PayloadEntry with
/// runtime `maxCs` and `done`. Otherwise identical to `payloadEntry`.
PayloadEntry payloadEntryRuntime(alias fn, bool withIteration = false, Args...)(
    PayloadHeader* header, ulong[] buf, uint maxCs, uint done, Args args)
        nothrow @nogc @system
    if (Args.length == packedParams!(fn, withIteration).length)
{
    static assert(validSignature!(fn, withIteration),
        "antfarm_templates: unsupported function signature");
    if (header is null) fatal("payloadEntryRuntime: null header");
    if (buf.length < packedLen!(fn, withIteration))
        fatal("payloadEntryRuntime: body buffer too small");
    initPayloadHeader!(fn, withIteration)(header, maxCs, done);
    packArgsImpl!(fn, withIteration, 0)(buf, args);
    return PayloadEntry(header, buf[0 .. packedLen!(fn, withIteration)]);
}

/++
 + Adapts an input range of `fn`'s packed arguments into a payload-entry
 + input range for the payload-entry `write` overload. Each element becomes
 + one payload whose generated type-erased callback executes `fn` with that
 + element's parameters. The shared header and the packed body live inside
 + the range; `write` copies both into the ring before returning, so the
 + range need only outlive the `write` call.
 +
 + Accepted element forms:
 +   - a `std.typecons.Tuple` whose fields map onto `fn`'s packed parameters
 +     in declaration order (any arity);
 +   - for a single-parameter `fn`, the parameter value itself;
 +   - for a zero-parameter `fn`, the element is ignored (it only counts
 +     positions).
 +
 + The range advances only when its consumer pops it: `write` iterates its
 + own copies and returns how many payloads landed, so the producer pops
 + that many elements before the next `write` call.
 +/
struct PayloadArgRange(alias fn, uint maxCs, uint done, bool withIteration, AR)
    if (isInputRange!AR)
{
    static assert(validSignature!(fn, withIteration),
        "antfarm_templates: unsupported function signature");

    private AR args;
    private PayloadHeader hdr;
    private ulong[packedLen!(fn, withIteration)] bodyBuf;

    private this(AR a) nothrow @nogc @system
    {
        args = a;
        initPayloadHeader!(fn, withIteration)(&hdr, maxCs, done);
    }

    @property bool empty() nothrow @nogc @system { return args.empty; }

    @property PayloadEntry front() nothrow @nogc @system
    {
        static if (packedParams!(fn, withIteration).length == 0)
        {
            // No packed parameters; the element only counts positions.
        }
        else static if (is(Unqual!(ElementType!AR) == Tuple!Ts, Ts...))
        {
            packArgsImpl!(fn, withIteration)(bodyBuf, args.front.expand);
        }
        else
        {
            static assert(packedParams!(fn, withIteration).length == 1,
                "antfarm_templates: payloadRange elements must be a "
                ~ "std.typecons.Tuple matching " ~ fullyQualifiedName!fn
                ~ "'s packed parameters");
            packArgsImpl!(fn, withIteration)(bodyBuf, args.front);
        }
        return PayloadEntry(&hdr, bodyBuf[]);
    }

    void popFront() nothrow @nogc @system { args.popFront(); }
}

/// ditto
PayloadArgRange!(fn, maxCs, done, withIteration, AR)
        payloadRange(alias fn, uint maxCs = 1, uint done = 1,
                bool withIteration = false, AR)(AR args) nothrow @nogc @system
if (isInputRange!AR)
{
    return typeof(return)(args);
}

// ---------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------

private template packedParams(alias fn, bool withIteration)
{
    static if (withIteration)
        alias packedParams = Parameters!fn[0 .. $ - 1];
    else
        alias packedParams = Parameters!fn;
}

private template validSignature(alias fn, bool withIteration)
{
    static if (withIteration)
    {
        static assert(Parameters!fn.length > 0 && is(Parameters!fn[$ - 1] == ulong),
            "antfarm_templates: withIteration = true requires the function's last parameter to be ulong");
        static assert(__traits(getParameterStorageClasses, fn, Parameters!fn.length - 1).length == 0,
            "antfarm_templates: withIteration = true requires the trailing ulong iteration parameter to be plain by-value");
    }
    static assert(validParams!(fn, withIteration, 0),
        "antfarm_templates: unsupported function parameter");
    alias P = Parameters!fn;
    alias RT = ReturnType!fn;
    static assert(is(typeof(fn) : RT function(P) nothrow @nogc @system) ||
                  is(typeof(&fn) : RT function(P) nothrow @nogc @system),
        "antfarm_templates: function must be a function or function pointer callable as nothrow @nogc @system (delegates are not supported)");
    static if (!is(RT == void))
        static assert(is(RT : long),
            "antfarm_templates: function return type must be void or convertible to long");
    enum bool validSignature = true;
}

private template validParams(alias fn, bool withIteration, size_t i)
{
    alias P = packedParams!(fn, withIteration);
    static if (i < P.length)
    {
        static assert(__traits(getParameterStorageClasses, fn, i).length == 0,
            "antfarm_templates: parameters must be plain by-value (no ref/out/lazy/in/scope/return)");
        static assert(__traits(isPOD, P[i]),
            "antfarm_templates: parameter type must be POD");
        static assert(!hasUnsharedAliasing!(P[i]),
            "antfarm_templates: parameter type has unshared aliasing (mutable pointer/dynamic array); pass plain value types or immutable data such as string");
        static assert(P[i].alignof <= 8,
            "antfarm_templates: parameter type alignment > 8 is not supported by the ulong body packing");
        static assert(validParams!(fn, withIteration, i + 1),
            "antfarm_templates: unsupported function parameter type");
        enum bool validParams = true;
    }
    else
        enum bool validParams = true;
}

private long callbackImpl(alias fn, bool withIteration = false)(
    PayloadHeader* head, PayloadBody body, ulong iteration) nothrow @nogc @system
{
    static assert(validSignature!(fn, withIteration),
        "antfarm_templates: unsupported function signature");
    if (body.length < packedLen!(fn, withIteration))
        fatal("antfarm callback: body too short");
    static if (withIteration)
    {
        static if (is(ReturnType!fn == void))
        {
            fn(decodeArgs!(fn, withIteration)(body).expand, iteration);
            return 0;
        }
        else
        {
            return fn(decodeArgs!(fn, withIteration)(body).expand, iteration);
        }
    }
    else
    {
        static if (is(ReturnType!fn == void))
        {
            fn(decodeArgs!(fn, withIteration)(body).expand);
            return 0;
        }
        else
        {
            return fn(decodeArgs!(fn, withIteration)(body).expand);
        }
    }
}

private auto decodeArgs(alias fn, bool withIteration = false, size_t i = 0)(
    PayloadBody body) nothrow @nogc @system
{
    alias P = packedParams!(fn, withIteration);
    static if (i == P.length)
        return tuple();
    else
    {
        alias T = P[i];
        // Body words are ring slots. Load them raw; a plain `body[i]` is a
        // non-atomic read of the same object write() store-raws.
        enum size_t off = paramOffset!(fn, i, withIteration);
        enum size_t nwords = (T.sizeof + 7) / 8;
        ulong[nwords] words = void;
        foreach (w; 0 .. nwords)
            words[w] = atomicLoad!(MemoryOrder.raw)(
                *cast(shared ulong*)(body.ptr + off + w));
        T val = void;
        memcpy(cast(void*) &val, words.ptr, T.sizeof);
        return tuple(val, decodeArgs!(fn, withIteration, i + 1)(body).expand);
    }
}

private void packArgsImpl(alias fn, bool withIteration = false, size_t i = 0,
        Args...)(ulong[] buf, Args args) nothrow @nogc @system
    if (Args.length == packedParams!(fn, withIteration).length)
{
    alias P = packedParams!(fn, withIteration);
    static if (i < P.length)
    {
        alias T = P[i];
        T arg = cast(T) args[i];
        memcpy(&buf[paramOffset!(fn, i, withIteration)], cast(const void*) &arg, T.sizeof);
        packArgsImpl!(fn, withIteration, i + 1)(buf, args);
    }
}
