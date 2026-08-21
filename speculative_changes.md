# Speculative change report: T21 and the antfarm_templates shim

Status: proposal only. No changes have been made in this file's scope.

## Context

The baseline tests (`antfarm_test.d`) and the torture suite (`review_torture/`)
were converted to build callbacks and payload entries through the shim
templates in `antfarm_templates.d`. All converted tests pass with DMD and LDC.

One exception remains: T21 in `review_torture/torture_tests.d`.

## The T21 problem

T21's `contentSumCb` deliberately validates the actual payload body length
before indexing the body:

```d
long contentSumCb(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    atomicFetchAdd(g_totalCalls, 1L);

    // Corrupted plen could make the body slice empty or huge; never index
    // before validating the length.
    if (b.length != T21_BODY_LEN)
    {
        atomicFetchAdd(g_contentBad, 1L);
        return 0;
    }

    immutable idx = cast(size_t) b[0];
    ...
}
```

The current shim templates only support functions whose parameters are packed
into contiguous `ulong`s and decoded back by type. A packed equivalent such
as:

```d
long contentSumCb(ulong[T21_BODY_LEN] body) nothrow @nogc @system
```

would not receive the actual body slice length. The generated callback only
checks `body.length < packedLen` before decoding, so the test would lose its
explicit "corrupted plen length" validation path. A shorter body would abort
inside the shim instead of incrementing `g_contentBad`; a longer body would
not be detected by the user function at all.

To preserve T21's current behavior we must either keep the raw type-erased
callback, or extend the shim so the user function can receive the actual
`PayloadBody` slice.

## Proposed shim extension: `withBody`

Add a `withBody` mode to `antfarm_templates.d`, parallel to the existing
`withIteration` mode. The function's last parameter must be `PayloadBody`;
the generated callback passes the actual body slice through unchanged.

### Proposed public API

```d
Callback callbackFor(alias fn, bool withIteration = false, bool withBody = false)();

void initPayloadHeader(alias fn, bool withIteration = false, bool withBody = false)(
    PayloadHeader* h, uint maxCs, uint done);

PayloadEntry payloadEntry(alias fn, uint maxCs = 1, uint done = 1,
    bool withIteration = false, bool withBody = false, Args...)(
    PayloadHeader* header, ulong[] buf, Args args);

PayloadEntry payloadEntryRuntime(alias fn, bool withIteration = false,
    bool withBody = false, Args...)(
    PayloadHeader* header, ulong[] buf, uint maxCs, uint done, Args args);
```

Existing call sites are unaffected because `withBody` defaults to `false`.

### Proposed internal changes

1. `packedParams` excludes the trailing `PayloadBody` when `withBody` is
   true, just as it excludes the trailing `ulong` when `withIteration` is
   true.

2. `validSignature` gains a `withBody` branch:
   - `withBody` and `withIteration` are mutually exclusive in the first
     version.
   - The function's last parameter must be exactly `PayloadBody`, plain
     by-value.
   - The function must have zero packed parameters. This keeps the first
     version unambiguous and is exactly what T21 needs.

   ```d
   static if (withBody)
   {
       static assert(!withIteration,
           "antfarm_templates: withBody and withIteration are mutually exclusive");
       static assert(Parameters!fn.length > 0 &&
                     is(Parameters!fn[$ - 1] == PayloadBody),
           "antfarm_templates: withBody = true requires the function's last parameter to be PayloadBody");
       static assert(__traits(getParameterStorageClasses, fn, Parameters!fn.length - 1).length == 0,
           "antfarm_templates: withBody = true requires the trailing PayloadBody parameter to be plain by-value");
       static assert(packedParams!(fn, withIteration, withBody).length == 0,
           "antfarm_templates: withBody = true currently supports only a raw PayloadBody parameter (no packed parameters)");
   }
   ```

3. `packedLen`, `paramOffset`, `decodeArgs`, and `packArgsImpl` thread
   `withBody` through so they operate only on `packedParams`.

4. `callbackImpl` branches on `withBody`:

   ```d
   static if (withBody)
   {
       static if (is(ReturnType!fn == void))
       {
           fn(body);
           return 0;
       }
       else
       {
           return fn(body);
       }
   }
   else
   {
       // existing plain / withIteration decode path unchanged
   }
   ```

5. `payloadEntry` and `payloadEntryRuntime` return the full caller buffer
   as the body when `withBody` is true, instead of `buf[0 .. packedLen]`:

   ```d
   return PayloadEntry(header, withBody ? buf : buf[0 .. packedLen!(fn, withIteration, withBody)]);
   ```

   Since `withBody` requires zero packed parameters, `packedLen` is `0`;
   returning `buf` preserves the caller's full body length (32 for T21).

### T21 after the shim extension

`contentSumCb` becomes shim-compatible without losing the length check:

```d
long contentSumCb(PayloadBody b) nothrow @nogc @system
{
    // Existing body unchanged: length check, per-payload count, content sum.
}
```

T21's payload construction becomes:

```d
foreach (i; 0 .. N)
{
    foreach (k; 0 .. BODY_LEN)
        bodies[i * BODY_LEN + k] = i + k;

    entries[i] = payloadEntry!(contentSumCb, 1, 1, false, true)(
        &headers[i], bodies[i * BODY_LEN .. i * BODY_LEN + BODY_LEN]);
}
```

This preserves:
- body length = `T21_BODY_LEN` (32),
- body contents `b[k] == idx + k`,
- the `b.length != T21_BODY_LEN` corrupted-plen check,
- exact per-payload and content-sum accounting,
- T21's geometry and `Wt`/laps behavior.

## Alternatives considered

1. **Keep T21 raw type-erased as it is now.**
   - No shim change, no test change.
   - Leaves one test outside the template conversion, which is why this
     report exists.

2. **Convert T21 to a packed `ulong[32]` parameter and drop the length
   check.**
   - Uses the current shim.
   - Changes test behavior: a corrupted short body would abort in the shim
     rather than incrementing `g_contentBad`; a corrupted long body would no
     longer be detected by the user function.

3. **Extend the shim with `withBody`.**
   - Preserves T21 exactly.
   - Small, defaulted, backward-compatible API addition.

## Verification plan for the `withBody` extension

- Re-run `dmd -g -O -inline antfarm.d antfarm_templates.d antfarm_test.d -of=antfarm_test && ./antfarm_test`.
- Re-run `make -C review_torture baseline`.
- Re-run `make -C review_torture run`.
- Re-run `make -C review_torture run-t19`.
- Run T21 specifically and confirm `T21 payload content multilap OK` with
  the same `Wt`/laps value as before (geometry unchanged).
