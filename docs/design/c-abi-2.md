# C ABI 2, a mockup

Status: mockup, 2026-11-04, on `2.2.1-progress`, before any code. This page
coalesces two things into one versioned surface: the review of what
[ABI 1](c-abi.md) covers against the D API index, and the
[Fiber sketch](fiber-c-abi.md). It is written as the design ABI 2 would
have, so that its shape can be judged before anything is built. ABI 1, its
six headers and `cabi/abi1.lock` are unchanged by it. The companion
[onboarding page](c-abi-2-onboarding.md) is the same proposal from the
side of the C programmer who meets it first.

## 1. Why a 2

ABI 1's rules are exact-size versioned option structures and a lock that
admits additions only. Three things on the list cannot be absorbed by
those rules:

- **Option structures need fields.** The plain host wants attachments, a
  wait deadline, the emit-round bound and host groups; the group host
  wants the caller's processor and the worker-token switch; both want a
  Fiber domain. Under the exact-size rule a new field is a new structure
  version, and ABI 1 rejects any size but its own.
- **A module-facing function table has to be designed once.** The Fiber
  sketch's first open item, a table or exports, is the shared-library
  question seen from one header. Answering it per header would give every
  header its own table and its own version.
- **One behaviour tightens.** The Fiber sketch decided that a D `Error`
  inside an export aborts through the fatal handler. The three existing
  wrappers fold it into `AF_RUNTIME_ERROR`. A lock of prototypes does not
  model a semantic change; a version does.

What ABI 2 is not: a redesign. Every ABI 1 symbol keeps its prototype and
its meaning except the one tightening above, and every ABI 1 program links
against the ABI 2 archives unchanged, because its option structures carry
their own version. ABI 2 is one archive pair that accepts both layouts.

## 2. Rules carried over

- Two archives: the BetterC core (Farm, actors, waves, private roots,
  fatal policy) and the druntime runtime (hosts, dispatcher, Fibers). One
  core instance per program.
- No D exception crosses a C frame. Operations return `af_status`; a
  `uint32_t` documented as yes/no is exactly 0 or 1; an outcome enum
  returns only its values.
- Handles are exclusive owning capabilities; identities (`af_actor_handle`,
  `af_fiber_handle` and their kin) are 16-byte copyable values that go
  stale rather than dangle.
- The status registry: every value in every header, never reused, each
  header with its range.
- Diagnostics are per calling thread through `af_last_error_message`; the
  fatal handler is process-wide and abort follows it.
- Wake-class calls need no druntime attachment; control calls need the
  control thread; services from other threads need attachment.
- Callbacks never throw, `longjmp` or retain their borrowed arguments.
- The lock admits additions only within a version. `cabi/abi2.lock` starts
  as `abi1.lock` plus ABI 2's rows.

## 3. Decisions

**Versioned structures negotiate.** Every options structure keeps
`struct_size` and `abi_version` as its first two words. ABI 2 accepts
`abi_version == 1` with the ABI 1 size and `abi_version == 2` with the ABI
2 size, and nothing else. `*_default` fills the ABI 2 layout. New fields
come after the ABI 1 `reserved` word, which stays zero, so an ABI 1 image
in memory is a valid prefix. Output structures that may grow
(`af_fiber_join_result`, `af_build_info`) are versioned the same way.

**`AF_ABI_VERSION` is 2, and there is a build query.** `af_abi_version`
returns 2. `af_build_info` reports the library version string, the D
compiler and its version, and a feature mask: whether the runtime archive
is linked, io_uring support, the mimalloc adapter, a ThreadSanitizer
build. A host logs it once; a module checks it before using a section of
the table.

**An `Error` aborts at every export.** The Fiber sketch's rule applies to
the hosts, the dispatcher and the group host as well: `catch (Error)` goes
to `fatal`, which runs the handler installed by `af_set_fatal_handler`,
prints and aborts. `AF_RUNTIME_ERROR` is what an `Exception` becomes, with
its message as the diagnostic. This is the one ABI 1 behaviour ABI 2
changes, and section 5 says what to audit.

**The plain host gains what the D host has.** `af_host` is the host most
C programs would use, and ABI 1 gives it step, wake, state and close but
not the rest of `FarmHost`. ABI 2 adds attachments (emit, drained and
finish callbacks driven by step; a private park attaches through a
convenience that transfers it to the host), a timed wait with the
worker-failure check, resolve and summary, host groups in its options, and
a Fiber domain served by step. The group host gains its own Fiber
attachment, the sketch's `af_group_start_fibers`, and the two options it
lacked.

**One table for every header.** `antfarm_api.h` declares `af_api`: a
`size` word, the ABI version, then one pointer per header section, each
section a structure of function pointers in header order, generated from
the lock so it cannot drift. `af_api_get` returns the library's table; a
resident hands it, or a copy with sections withheld as null, to a module
it loads. Within ABI 2 sections are append-only, and a module checks a
section's `size` before reading a member appended after its build. The
table is the only thing a dynamically loaded module needs: it imports
nothing from the executable, which is what Windows requires of a DLL.

**Cost hints are exports, and writes can derive them.** `af_cost_for_chunk`
and `af_root_cost_hint` mirror the D helpers. `AF_COST_AUTO` passed as
`avg_cost` to a core write derives the hint from the table's count and the
Farm's consumers, the way the D root publication does; ABI 1 callers who
pass 0 to 7 are unaffected.

**Queries the D side has and C lacked.** Actor runtime capacity and
quiescence, whether a Farm got large pages, the host's resolved processors
and its summary line.

**Garbage collection has a control surface for C residents.** A host that
embeds druntime from C has no handle on collections today. ABI 2 adds
collect, minimize, enable and disable, for attached threads outside
callbacks and bodies. A collection suspends attached threads, pool workers
included, for its duration and leaves unattached C threads running; a
frame-based host calls collect at a frame edge with the GC otherwise
disabled.

**Fibers come in as sketched, plus the plain host.** `antfarm_fibers.h` is
the Fiber sketch's surface with the decisions since (cancellation its own
channel, an `Error` aborting at the edge), with one row added:
`af_host_attach_fibers`. Statuses 128 to 143 stay its range.

**The loop stays out, with its room kept.** `ant_uv` has no C surface
until it has production use. ABI 2 reserves statuses 144 to 159 and a
table section for it, and names the meeting point: `af_uv_submit_and_wait`
delivering to an `af_fiber_event`, with the cancellation hold
`submitAndWait` has.

**Packaging and wrappers follow, not lead.** The archives stay static and
the one-core-instance rule stays; the table is the shared-library story
for modules, not for the library. The C++ wrappers (today the core Farm
and private roots) extend to actors, hosts and Fibers after the C surface
settles, and the table makes a wrapper's binding mechanical.

## 4. The surface

Additions only, per header, in the style of the existing declarations.
Sizes and offsets are asserted in the headers as today; this page does
not fix them.

### antfarm.h

```c
#define AF_ABI_VERSION 2u
#define AF_COST_AUTO UINT32_MAX     /* avg_cost: derive from count and consumers */
enum { AF_FEATURE_RUNTIME = 1u << 0, AF_FEATURE_IO_URING = 1u << 1,
       AF_FEATURE_MIMALLOC = 1u << 2, AF_FEATURE_TSAN = 1u << 3 };
typedef struct af_build_info {
    uint32_t struct_size, abi_version;   /* in: the caller's layout */
    const char *library_version;         /* borrowed, static: "2.2.1-dev" */
    const char *compiler;                /* "ldc2 1.42.0" or "dmd 2.112.1" */
    uint32_t features, reserved;
} af_build_info;
af_status af_build_info_get(af_build_info *);
/* Mirror antfarm's rootCostHintFor and avgCostForChunk. */
uint32_t af_cost_for_chunk(uint32_t chunk);
uint32_t af_root_cost_hint(size_t count, uint64_t consumers, uint32_t small_threshold);
uint32_t af_used_large_pages(const af_farm *);
```

`af_options` keeps its ABI 1 layout; `abi_version` 2 with the same size
is accepted so a program can be uniform about the version it writes.

### antfarm_actors.h and antfarm_private.h

```c
size_t af_actor_runtime_capacity(const af_actor_runtime *);
uint32_t af_actor_runtime_quiescent(const af_actor_runtime *); /* live and ready both zero */
```

The private header is unchanged. The convention it set, 24-bit failure
codes, is the one Fiber bodies use too.

### antfarm_runtime.h

```c
enum { AF_HOST_TIMED_OUT = 112, AF_HOST_WORKER_FAILED = 113 }; /* runtime range */
typedef struct af_host_options {
    /* ABI 1 fields through reserved, unchanged ... */
    uint64_t wait_timeout_ns;           /* af_host_wait_until default; AF_WAIT_FOREVER */
    uint32_t max_emit_rounds;           /* attachment rounds per step; 16 */
    uint32_t group_mode, group_index, farm_per_group;  /* AF_GROUP_* as the group host */
    uint64_t reserved2;
} af_host_options;
/* One emitter or service driven from every step, on the control thread.
   emit runs with the host's token once per emission round and returns
   items published; drained is the close precondition (NULL: always);
   finish runs after consumers stop, before runtimes, parks and the Farm
   go. Callbacks must return promptly and never wait for a worker. */
typedef struct af_attachment {
    uint32_t struct_size, abi_version;
    const char *name;
    size_t (*emit)(void *context, af_token *);
    uint32_t (*drained)(void *context);
    void (*finish)(void *context);
    void *context;
    uint64_t reserved;
} af_attachment;
af_status af_host_attach(af_host *, const af_attachment *);
/* Transfers the park: step pumps it with the host token, close requires
   it drained and destroys it. Never af_private_roots_destroy it after. */
af_status af_host_attach_park(af_host *, af_private_roots *);
typedef uint32_t (*af_host_predicate)(void *context);
/* while (!done(context)) step, with the worker-failure check and the
   deadline (AF_WAIT_FOREVER: the options' wait_timeout_ns). AF_HOST_TIMED_OUT
   leaves the host open; AF_HOST_WORKER_FAILED leaves only close/abandon. */
af_status af_host_wait_until(af_host *, af_host_predicate, void *context, uint64_t timeout_ns);
/* The worker CPU set the options would select; NULL/0 queries count. */
af_status af_host_resolve(const af_host_options *, af_processor *out, size_t capacity, size_t *count);
/* FarmHost.summary into caller storage; len is the full length. */
af_status af_host_summary(const af_host *, char *out, size_t capacity, size_t *len);
/* Attached threads, outside Farm callbacks, hooks and Fiber bodies. */
af_status af_druntime_gc_collect(void);
af_status af_druntime_gc_minimize(void);
af_status af_druntime_gc_enable(void);
af_status af_druntime_gc_disable(void);
```

`af_host_step` serves an attached Fiber domain and pumps attached parks
in the same emission rounds as attachments. `af_host_close` requires every
attachment drained, the Fiber domain drained and retired, and transferred
parks empty, and returns `AF_BUSY` with the first blocker named otherwise.

### antfarm_groups.h

```c
typedef struct af_group_options {
    /* ABI 1 fields through reserved, unchanged ... */
    af_processor caller_processor;      /* pin the caller; {0xFFFF, 0xFFFF} none */
    uint32_t worker_tokens, reserved2;  /* per-worker cached tokens; 1 */
    uint64_t reserved3;
} af_group_options;
```

Worker-side attachments stay internal to the group host. The one a
consumer has asked for is Fibers, and it is `af_group_start_fibers` in the
Fiber header. Other group attachments wait for a consumer.

### antfarm_forkjoin.h

Unchanged. The helper decision (map, scan, invoke, task groups, frame
allocation) stays with the roadmap; ABI 2 does not pre-empt it.

### antfarm_fibers.h

The [sketch's section 3](fiber-c-abi.md#3-the-surface), with:

```c
/* The domain af_host_step serves and retires. Spawn from the control
   thread or any attached thread; close requires it drained and retired. */
af_status af_host_attach_fibers(af_host *, const af_fiber_domain_options *, af_fiber_domain **out);
```

### antfarm_api.h

```c
typedef struct af_api_core {
    size_t size;                        /* bytes valid in this section */
    uint32_t (*abi_version)(void);
    void (*options_default)(af_options *);
    af_status (*create)(const af_options *, af_farm **);
    /* ... every antfarm.h function, in header order, generated from the lock */
} af_api_core;
/* af_api_actors, af_api_private, af_api_runtime, af_api_forkjoin,
   af_api_groups and af_api_fibers likewise; af_api_uv is reserved. */
typedef struct af_api {
    size_t size;
    uint32_t abi_version, reserved;
    const af_api_core *core;
    const af_api_actors *actors;
    const af_api_private *private_roots;
    const af_api_runtime *runtime;      /* NULL in a core-only program */
    const af_api_forkjoin *forkjoin;
    const af_api_groups *groups;
    const af_api_fibers *fibers;
    const void *uv;                     /* reserved for the loop */
    const void *reserved_sections[7];
} af_api;
/* The library's table: static, never freed. A resident may hand a module
   a copy with sections set to NULL to withhold them. */
const af_api *af_api_get(void);
```

The generator is `scripts/check-c-abi.py --write-api-table`, which reads
the lock's `PROTO` rows and writes the header and the D-side table; the
audit fails if either differs from the lock. A module built against an
older ABI 2 checks `size` on a section before using a member appended
later; a resident built against an older ABI 2 passes its shorter table,
and a newer module sees the smaller `size`.

### Status registry

| Range | Header | ABI 2 use |
|---|---|---|
| 0 to 10 | ABI 1 | unchanged |
| 64 to 79 | antfarm.h | none new |
| 80 to 95 | antfarm_actors.h | none new |
| 96 to 111 | antfarm_private.h | none new |
| 112 to 127 | antfarm_runtime.h | `AF_HOST_TIMED_OUT`, `AF_HOST_WORKER_FAILED` |
| 128 to 143 | antfarm_fibers.h | `AF_FIBER_CANCELLED`, `AF_FIBER_TIMED_OUT`, `AF_FIBER_STALE` |
| 144 to 159 | the loop | reserved |
| 160 to 175 | antfarm_api.h | reserved |

## 5. Migration from ABI 1

| ABI 1 program | Under ABI 2 |
|---|---|
| Links the ABI 1 archives | Links the ABI 2 archives; no source change required |
| Fills options with `*_default` and writes `abi_version = 1` | Still accepted with the ABI 1 size. `*_default` now writes 2 and the ABI 2 size, so a program that copies the default and asserts its own `sizeof` must rebuild against the ABI 2 headers |
| Passes `avg_cost` 0 to 7 | Unchanged; `AF_COST_AUTO` is opt-in |
| Relied on `AF_RUNTIME_ERROR` after a D `Error` in a runtime export | Now aborts through the fatal handler. Audit the handler: it may log and dump, and must not re-enter |
| Pumps a private park on the borrowed host Farm by hand | Still allowed; `af_host_attach_park` is the alternative |
| Ships the headers to a plugin and resolves exports | Can hand it `af_api_get()` instead; on Windows this removes the export requirement |

The lock: `cabi/abi2.lock` is `abi1.lock` plus the rows above, and the
audit runs both, so an ABI 1 prototype that drifts fails in either.

## 6. Validation

- Negotiation: every options structure in its ABI 1 image and size into
  the ABI 2 archive, and in its ABI 2 image; truncated, larger and
  mismatched versions rejected with `AF_INVALID`.
- Error policy: a death case per header, raised through a test seam, that
  reaches the fatal handler with its message and never a status.
- The plain host: attachments with emit, drained and finish; a transferred
  park drained and destroyed by close; `AF_BUSY` naming each blocker;
  `af_host_wait_until` timing out with the host open, and reporting a
  worker failure; resolve against the group host's resolve on the same
  options.
- Fibers: the sketch's section 8, plus the same cases through
  `af_host_attach_fibers` served by step.
- The table: a test module built as a shared object or DLL that imports
  nothing, receives the table and runs the core, host and Fiber cases
  through it; a shorter table from an older resident; a withheld section.
- GC: collect from the control thread and from an attached thread with
  pool workers parked and a native C thread running; collect inside a Farm
  callback or a Fiber body as a death case.
- Cost hints against the D helpers on the same inputs; `AF_COST_AUTO`
  producing the D root publication's hint.
- The declaration audit against `abi2.lock`, the generated table against
  the lock, both compilers, Linux and Windows, the TSan lane for the core.

## 7. Open

1. Section granularity of the table: one section per header, as above, or
   one flat table. Per header lets a resident withhold the runtime from a
   core-only module; flat is simpler to generate.
2. Whether ABI 1 archives keep shipping beside ABI 2, or negotiation makes
   the ABI 2 pair the only distribution. This page assumes the latter.
3. The GC surface under Fibers: a collection from a body suspends its own
   worker; the page makes it a death case, which the audit should prove.
4. Whether `af_host` and `af_group_host` merge, with host groups in the
   plain host's options making the group host a mode. That is an ABI 3
   question; ABI 2 keeps both.
5. The fork-join helpers, per the roadmap.
6. The Fiber sketch's remaining items: per-spawn completion, events before
   the loop, the failure object, tag scan versus list, DMD GC metadata.
