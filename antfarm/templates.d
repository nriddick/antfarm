/++
 + Ant Farm templates: generate type-erased callbacks and packed payload
 + entries from ordinary D functions.
 +
 + The function's parameters are serialized, in declaration order, into the
 + payload body as contiguous ulongs. Each parameter occupies
 + ceil(T.sizeof / 8) ulongs; the callback reconstructs the parameter by
 + copying raw-loaded words into the parameter type.
 +
 + Restrictions (enforced by static asserts where possible):
 +   - The function must be callable as nothrow @nogc @system.
 +   - Packed parameters must be plain by-value (no ref/out/lazy/in/scope/
 +     return), POD, with no unshared aliasing (mutable pointers/dynamic
 +     arrays are rejected; immutable references such as `string` or
 +     `immutable(int)*` are allowed and copied by reference), and with
 +     alignment <= 8.
 +   - The function must return void or a type implicitly convertible to long.
 +   - Each argument must implicitly convert to its parameter type. No cast
 +     is applied, so narrowing (`int` to `ubyte`) and mutable-to-immutable
 +     pointer conversions are compile errors.
 +
 + `antfarm.templates` publicly imports `antfarm`, so importing this
 + module also brings in `AntFarm`, `ConsumerView`, `Callback`,
 + `PayloadEntry`, `Token`, and `Tier`.
 +
 + PayloadEntry.call is the generated C-linkage callback.
 + The body slice aliases `buf[0 .. packedLen!fn]`; `write()` copies both
 + callback metadata and body into the ring before returning, so `buf` only
 + needs to stay alive until `write()` returns.
 +
 + Note: for reference-like parameters that pass the no-unshared-aliasing
 + rule (notably `string`), the body stores the reference, not the referred
 + data. The referred data must stay alive until the job executes.
 +/
module antfarm.templates;

public import antfarm;
import core.atomic;
import core.stdc.string : memcpy, memset;
import std.traits : Parameters, ReturnType, Unqual, fullyQualifiedName,
    hasUnsharedAliasing;
import std.range.primitives : ElementType, empty, front, hasLength,
    isForwardRange, isInputRange, isRandomAccessRange, hasSlicing, popFront, popFrontN, save;
import std.typecons : Tuple, tuple;

/// Packed body length, in ulongs, of `fn`'s packed parameter list.
template packedLen(alias fn)
{
    enum size_t packedLen = packedLenImpl!(packedParams!(fn));
}

private template packedLenImpl(T...)
{
    static if (T.length == 0)
        enum size_t packedLenImpl = 0;
    else
        enum size_t packedLenImpl = ((T[0].sizeof + 7) / 8) + packedLenImpl!(T[1 .. $]);
}

/// Offset, in ulongs, of packed parameter `i` inside the body.
template paramOffset(alias fn, size_t i)
{
    static if (i == 0)
        enum size_t paramOffset = 0;
    else
        enum size_t paramOffset =
            ((packedParams!(fn)[i - 1].sizeof + 7) / 8) +
            paramOffset!(fn, i - 1);
}

/// Returns the generated type-erased Callback for `fn`.
Callback callbackFor(alias fn)() nothrow @nogc @system
{
    static assert(validSignature!(fn),
        "antfarm.templates: unsupported function signature");
    return &callbackImpl!(fn);
}

/// Packs `args` into `buf` for `fn` and returns the PayloadEntry.
/// `buf` must have room for at least `packedLen!(fn)` ulongs.
PayloadEntry payloadEntry(alias fn, Args...)(
    ulong[] buf, Args args) nothrow @nogc @system
    if (Args.length == packedParams!(fn).length
        && argsConvert!(fn, Args))
{
    static assert(validSignature!(fn),
        "antfarm.templates: unsupported function signature");
    if (buf.length < packedLen!(fn))
        fatal("payloadEntry: body buffer too small");
    packArgsImpl!(fn, 0)(buf, args);
    return PayloadEntry(callbackFor!fn(), buf[0 .. packedLen!(fn)]);
}

/++
 + Adapts an input range of `fn`'s packed arguments into a payload-entry
 + range. When the argument source is a forward range, the result preserves
 + its checkpoints and can be passed to `write`. Each element becomes
 + one payload whose generated type-erased callback executes `fn` with that
 + element's parameters. The generated callback and packed body are supplied by
 + the range; `write` copies their metadata and body before returning, so the
 + range need only outlive the `write` call.
 +
 + Accepted element forms:
 +   - a `std.typecons.Tuple` whose fields map onto `fn`'s packed parameters
 +     in declaration order (any arity);
 +   - for a single-parameter `fn`, the parameter value itself;
 +   - for a zero-parameter `fn`, the element is ignored (it only counts
 +     positions).
 +
 + The range advances only when its consumer pops it: `write` uses saved
 + checkpoints and returns how many payloads landed, so the producer pops
 + that many elements before the next `write` call. Its common callback and
 + fixed packed width also select `write`'s uniform sizing path.
 +/
struct PayloadArgRangeImpl(alias fn, AR)
    if (isInputRange!AR)
{
    static assert(validSignature!(fn),
        "antfarm.templates: unsupported function signature");

    private AR args;
    private ulong[packedLen!(fn)] bodyBuf;

    private this(AR a) nothrow @nogc @system
    {
        args = a;
    }

    /// Uniform-layout protocol consumed by AntFarm.writeImpl. Every element
    /// has the same generated callback and compile-time packed body width.
    @property Callback commonPayloadCallback() nothrow @nogc @system
    {
        return callbackFor!fn();
    }

    /// The compile-time packed body width of every element.
    enum ulong fixedPayloadLength = packedLen!(fn);

    /// True when the argument source is exhausted.
    @property bool empty() nothrow @nogc @system { return args.empty; }

    /// The next payload entry, packed on demand from the current element.
    @property PayloadEntry front() nothrow @nogc @system
    {
        static if (packedParams!(fn).length == 0)
        {
            // No packed parameters; the element only counts positions.
        }
        else static if (is(Unqual!(ElementType!AR) == Tuple!Ts, Ts...))
        {
            packArgsImpl!(fn)(bodyBuf, args.front.expand);
        }
        else
        {
            static assert(packedParams!(fn).length == 1,
                "antfarm.templates: payloadRange elements must be a "
                ~ "std.typecons.Tuple matching " ~ fullyQualifiedName!fn
                ~ "'s packed parameters");
            packArgsImpl!(fn)(bodyBuf, args.front);
        }
        return PayloadEntry(commonPayloadCallback, bodyBuf[]);
    }

    /// Advance the argument source by one element.
    void popFront() nothrow @nogc @system { args.popFront(); }

    /// Checkpoint the argument source when it is a forward range.
    static if (isForwardRange!AR)
    {
        @property typeof(this) save() nothrow @nogc @system
        {
            typeof(this) result = this;
            result.args = args.save;
            return result;
        }
    }

    static if (hasLength!AR)
    {
        /// The argument source's remaining length.
        @property size_t length() nothrow @nogc @system
        {
            return cast(size_t) args.length;
        }
    }

    /// Pop up to `n` arguments; returns how many were popped.
    size_t popFrontN(size_t n) nothrow @nogc @system
    {
        return args.popFrontN(n);
    }
}

/// Explicit type spelling for the minimally packed argument range.
alias PayloadArgRange(alias fn, AR) = PayloadArgRangeImpl!(fn, AR);

/// One independently scheduled payload per argument element.
PayloadArgRange!(fn, AR) payloadRange(alias fn, AR)(AR args)
    nothrow @nogc @system if (isInputRange!AR)
{
    return typeof(return)(args);
}

/// Lazy grains over a finite, sliceable random-access argument range.
/// Each payload owns a copied subrange descriptor and executes its elements
/// sequentially. Referenced immutable/shared data must outlive execution.
/// The source descriptor must be POD, aligned to at most 8, with no unshared
/// mutable aliases. Unlike the one-argument payloadRange overload, arguments
/// are read during execution, not packed individually at publication.
struct PayloadGrainRange(alias fn, AR)
{
    private AR source;
    private size_t grain, position, count;
    private ulong[(AR.sizeof + 7) / 8] words;

    private this(AR input, size_t granularity) nothrow @nogc @system
    {
        static assert(validSignature!fn);
        static assert(__traits(isPOD, AR) && AR.alignof <= ulong.alignof &&
            !hasUnsharedAliasing!AR,
            "payload grains require a POD range without unshared mutable aliases");
        if (granularity == 0) fatal("payload grain must be positive");
        source = input;
        grain = granularity;
        immutable n = cast(size_t) source.length;
        count = n / grain + (n % grain != 0);
    }
    /// Copied descriptor width, independent of grain and final tail length.
    enum ulong fixedPayloadLength = (AR.sizeof + 7) / 8;
    /// Common metadata selects the Farm's uniform table path.
    @property Callback commonPayloadCallback() nothrow @nogc @system { return &invoke; }
    /// True when every grain has been popped.
    @property bool empty() const nothrow @nogc @safe { return position == count; }
    /// Remaining grains, not source elements.
    @property size_t length() const nothrow @nogc @safe { return count - position; }
    /// Independent checkpoint for sizing, publication and retry.
    @property typeof(this) save() nothrow @nogc @system { return this; }
    /// Next grain; descriptor storage is borrowed until this range advances.
    @property PayloadEntry front() nothrow @nogc @system
    {
        immutable first = position * grain;
        immutable remaining = cast(size_t) source.length - first;
        immutable last = first + (grain < remaining ? grain : remaining);
        AR block = source[first .. last];
        memcpy(words.ptr, &block, AR.sizeof);
        return PayloadEntry(&invoke, words[]);
    }
    /// Advance one grain.
    void popFront() nothrow @nogc @safe { assert(!empty); ++position; }
    /// Advance up to n admitted grains; returns the number advanced.
    size_t popFrontN(size_t n) nothrow @nogc @safe
    {
        immutable taken = n < length ? n : length;
        position += taken;
        return taken;
    }
    private static extern(C) void invoke(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
    {
        auto body = bodyPtr[0 .. bodyWords];
        if (body.length != fixedPayloadLength) fatal("payload grain body length mismatch");
        ulong[fixedPayloadLength] copy;
        foreach(i; 0 .. copy.length)
            copy[i] = atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*)(body.ptr + i));
        AR block = void;
        memcpy(&block, copy.ptr, AR.sizeof);
        foreach (i; 0 .. block.length)
        {
            static if (Parameters!fn.length == 0) fn();
            else static if (is(Unqual!(ElementType!AR) == Tuple!Ts, Ts...))
            {
                static assert(argsConvert!(fn, typeof(block[i].expand)));
                fn(block[i].expand);
            }
            else
            {
                static assert(argsConvert!(fn, ElementType!AR));
                fn(block[i]);
            }
        }
        return;
    }
}

/// Group up to grain argument elements into one payload. Grain must be
/// positive; the final payload handles the short tail. Publication counts and
/// popFrontN are measured in grains. Omit grain for minimal per-element packing.
auto payloadRange(alias fn, AR)(AR args, size_t grain) nothrow @nogc @system
    if (isRandomAccessRange!AR && hasLength!AR && hasSlicing!AR)
{
    return PayloadGrainRange!(fn, AR)(args, grain);
}

// ---------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------

private template packedParams(alias fn)
{
    alias packedParams = Parameters!fn;
}

/// True when every argument implicitly converts to the matching packed
/// parameter. Explicit casts are never applied: they would silently narrow
/// values or launder a mutable pointer into an `immutable` parameter.
template argsConvert(alias fn, Args...)
{
    alias P = packedParams!(fn);
    static if (Args.length != P.length)
        enum bool argsConvert = false;
    else
        enum bool argsConvert = argsConvertImpl!(0, P.length, P, Args);
}

private template argsConvertImpl(size_t i, size_t n, T...)
{
    static if (i == n)
        enum bool argsConvertImpl = true;
    else
        enum bool argsConvertImpl = is(T[n + i] : T[i])
            && argsConvertImpl!(i + 1, n, T);
}

private template validSignature(alias fn)
{
    static assert(validParams!(fn, 0),
        "antfarm.templates: unsupported function parameter");
    alias P = Parameters!fn;
    alias RT = ReturnType!fn;
    static assert(is(typeof(fn) : RT function(P) nothrow @nogc @system) ||
                  is(typeof(&fn) : RT function(P) nothrow @nogc @system),
        "antfarm.templates: function must be a function or function pointer callable as nothrow @nogc @system (delegates are not supported)");
    static if (!is(RT == void))
        static assert(is(RT : long),
            "antfarm.templates: function return type must be void or convertible to long");
    enum bool validSignature = true;
}

private template validParams(alias fn, size_t i)
{
    alias P = packedParams!(fn);
    static if (i < P.length)
    {
        static assert(__traits(getParameterStorageClasses, fn, i).length == 0,
            "antfarm.templates: parameters must be plain by-value (no ref/out/lazy/in/scope/return)");
        static assert(__traits(isPOD, P[i]),
            "antfarm.templates: parameter type must be POD");
        static assert(!hasUnsharedAliasing!(P[i]),
            "antfarm.templates: parameter type has unshared aliasing (mutable pointer/dynamic array); pass plain value types or immutable data such as string");
        static assert(P[i].alignof <= 8,
            "antfarm.templates: parameter type alignment > 8 is not supported by the ulong body packing");
        static assert(validParams!(fn, i + 1),
            "antfarm.templates: unsupported function parameter type");
        enum bool validParams = true;
    }
    else
        enum bool validParams = true;
}

private extern(C) void callbackImpl(alias fn)(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    auto body = bodyPtr[0 .. bodyWords];
    static assert(validSignature!(fn),
        "antfarm.templates: unsupported function signature");
    if (body.length < packedLen!(fn))
        fatal("antfarm callback: body too short");
    fn(decodeArgs!(fn)(body).expand);
}

private auto decodeArgs(alias fn, size_t i = 0)(
    PayloadBody body) nothrow @nogc @system
{
    alias P = packedParams!(fn);
    static if (i == P.length)
        return tuple();
    else
    {
        alias T = P[i];
        // Body words are ring slots. Load them raw; a plain `body[i]` is a
        // non-atomic read of the same object write() store-raws.
        enum size_t off = paramOffset!(fn, i);
        enum size_t nwords = (T.sizeof + 7) / 8;
        ulong[nwords] words = void;
        foreach (w; 0 .. nwords)
            words[w] = atomicLoad!(MemoryOrder.raw)(
                *cast(shared ulong*)(body.ptr + off + w));
        T val = void;
        memcpy(cast(void*) &val, words.ptr, T.sizeof);
        return tuple(val, decodeArgs!(fn, i + 1)(body).expand);
    }
}

private void packArgsImpl(alias fn, size_t i = 0,
        Args...)(ulong[] buf, Args args) nothrow @nogc @system
    if (Args.length == packedParams!(fn).length)
{
    alias P = packedParams!(fn);
    static if (i < P.length)
    {
        alias T = P[i];
        static assert(is(Args[i] : T),
            "antfarm.templates: argument " ~ i.stringof ~ " of type "
            ~ Args[i].stringof ~ " does not implicitly convert to "
            ~ fullyQualifiedName!fn ~ "'s parameter type " ~ T.stringof
            ~ " (no narrowing, and no mutable aliases into immutable/shared"
            ~ " parameters)");
        T arg = args[i];
        enum size_t off = paramOffset!(fn, i);
        enum size_t bytes = ((T.sizeof + 7) / 8) * ulong.sizeof;
        memset(&buf[off], 0, bytes);
        memcpy(&buf[off], cast(const void*) &arg, T.sizeof);
        packArgsImpl!(fn, i + 1)(buf, args);
    }
}
