# Onboarding to C ABI 2

Status: in progress on `2.2.1-progress`. This page is written as the
onboarding guide ABI 2 ships with, so that the surface in
[c-abi-2.md](c-abi-2.md) can be judged from the side of the C programmer
who meets it first. Sections 2 to 5 and 7 to 11 describe calls that exist
and are tested (2026-11-04); the Fibers of section 6, and the Fiber
section of the table in section 7, remain proposals, and the text says so
where a call is ABI 1 today. The examples under `examples/` it refers to
are the ABI 1 ones.

## 1. Choose your archive

Ant Farm ships as two static archives built from one source revision with
one D compiler.

- **`antfarm_core`** has no D runtime. Farms, producers, views, public
  actors, waves and private roots live here, callable from any native
  thread with no initialisation. If your program brings its own threads
  and wants work tables and actors, stop here.
- **`antfarm_runtime`** adds druntime and with it the worker pool: the
  plain host, the group host, the fork-join dispatcher and Fibers. It
  needs `af_druntime_init` once, on the thread that will control hosts,
  and attachment for other threads that use runtime services.

One core instance per program: never embed a second copy in a plugin. A
plugin takes the function table of section 7 instead.

## 2. Build and link

Unchanged from ABI 1. Build the archives with the D compiler you will
link with:

```text
python3 scripts/build-c-abi.py --compiler ldc2 --runtime
```

Core-only programs link with the C toolchain. Runtime programs compile
their C with the C compiler and link with the D compiler's driver, which
supplies druntime:

```text
cc -std=c11 -Iinclude -c host.c -o host.o
ldc2 host.o .build/c-abi-ldc2-posix/libantfarm_runtime.a .build/c-abi-ldc2-posix/libantfarm_core.a -L-lpthread -L-lrt -L-latomic -of=host
```

On Windows, `cl /std:c11 /MT /c /Iinclude`, then `ldc2` with
`antfarm_runtime.lib`, `antfarm_core.lib`, `kernel32.lib` and
`advapi32.lib`. The first thing a program does is check what it linked:

```c
af_build_info info = { sizeof info, AF_ABI_VERSION };
if (af_build_info_get(&info) != AF_OK || af_abi_version() != 2) { /* mismatch */ }
printf("antfarm %s, %s, features %#x\n", info.library_version, info.compiler, info.features);
```

## 3. A first program on the core

ABI 1, unchanged: create a Farm, register a producer, write a table of
callbacks, consume it from a view, release in reverse. Read
[examples/c_core.c](../../examples/c_core.c); it prints `sum = 10`. Two
things are new in ABI 2, and both are optional:

- `AF_COST_AUTO` as the write's `avg_cost` derives the hint the D root
  publication would use; `af_cost_for_chunk` and `af_root_cost_hint` give
  the numbers if you would rather pass them yourself.
- `af_used_large_pages(farm)` says whether the ring got huge pages.

## 4. A host with workers

```c
#include "antfarm_runtime.h"

/* pending is decremented by the table hooks on worker threads. */
static uint32_t work_done(void *context) { return atomic_load((atomic_size_t *) context) == 0; }

int main(void) {
    if (af_druntime_init() != AF_OK) return 1;          /* this is now the control thread */
    af_host_options options;
    af_host_options_default(&options);                   /* struct_size and abi_version = 2 */
    options.workers = 2;
    options.wait_timeout_ns = 5ull * 1000 * 1000 * 1000;
    af_host *host;
    if (af_host_create(&options, &host) != AF_OK) {
        fprintf(stderr, "%s\n", af_last_error_message());
        return 1;
    }
    atomic_size_t pending;
    publish_work(host, &pending);                        /* af_host_register_producer, af_write, af_host_wake */
    af_status s = af_host_wait_until(host, work_done, &pending, AF_WAIT_FOREVER);
    if (s == AF_HOST_TIMED_OUT) { /* the host is still open: wait again, escalate or abandon */ }
    if (s == AF_HOST_WORKER_FAILED) { af_host_abandon(host); return 2; }
    while (af_host_close(host) == AF_BUSY) {             /* the message names the blocker */
        fprintf(stderr, "close: %s\n", af_last_error_message());
        repair_and_retry();
    }
    return af_druntime_shutdown() == AF_OK ? 0 : 3;
}
```

`af_host_wait_until` is `while (!done) step` with the deadline and the
worker-failure check; `af_host_step` remains for a loop you drive
yourself, and either serves whatever is attached to the host. Wake after
every accepted table and on idle polls, from any thread: a sleeping
consumer pins space the producer cannot reclaim.

### More than one LLC group

By default the host takes workers from one LLC group, the one with the
most eligible CPUs. To spread the workers over every group:

```c
options.group_mode = AF_GROUP_ALL;   /* AF_GROUP_AUTOMATIC, AF_GROUP_ALL, AF_GROUP_ONLY + group_index */
```

Every group's workers then consume the host's one Farm, and everything
this guide publishes reaches it. `farm_per_group` stays 0 on this host:
`af_host_create` returns `AF_INVALID` for 1, because the plain host's C
calls reach only one Farm and the other groups' Farms would get no work.
When you want one Farm per group, use the group host: `af_group_start` with
`AF_GROUP_ALL` (one Farm per group is its default), then `af_group_farm`,
`af_group_token` and `af_group_wake` with the group you mean, and a worker
body that consumes its own group's Farm. Private parks can live on any
group's Farm. Public actor runtimes cannot: they need an owned Farm, so
on a group host there is none. `af_host_resolve` and `af_group_resolve`
show the CPUs and groups a set of options would select, without starting
anything.

## 5. Actors and parks on the host

Host-owned public actors are ABI 1: `af_host_create_actors`, then kinds,
creation, send and wake, retire and reclaim, all from any native thread
except creation and destruction of the runtime itself. Read
[examples/c_actors.c](../../examples/c_actors.c) and
[c_host.c](../../examples/c_host.c).

A private park is created on the host's borrowed Farm as in ABI 1. New in
ABI 2, attach it and let the host drive it:

```c
af_private_roots *park;
af_private_roots_create(af_host_farm(host), &root_options, NULL, &park);
af_host_attach_park(host, park);     /* step pumps it; close drains and destroys it */
```

After attachment you spawn roots, take tracked outcomes and cancel as
before, but never pump or destroy the park yourself: both are fatal on a
transferred park. A park transfers once; a second `af_host_attach_park`
is `AF_INVALID`. Anything else that
needs the control thread's attention each step, a queue you drain into
tables or a service that must know when consumers stop, is an attachment:

```c
static size_t emit(void *context, af_token *token) {
    struct telemetry *t = context;     /* t->farm read before attaching */
    return af_write_bodies(t->farm, record, t->pending, t->count, token, AF_COST_AUTO, NULL);
}

state.farm = af_host_farm(host);       /* here, not inside emit */
af_attachment a = { sizeof a, AF_ABI_VERSION, "telemetry", emit, drained, finish, &state, 0 };
af_host_attach(host, &a);
```

`emit` runs with the host's token once per emission round and returns
how many items it published; zero does not mean drained. `drained` is
what close checks. `finish` runs after consumers stop and before the Farm
goes. None of the three may wait for a worker, and none may make an
`af_host_*` control call, `af_host_farm` included: the call is fatal
inside a runtime callback. So emit gets the token but not the Farm; read
the Farm once before attaching and keep it in the context, as above.

## 6. Fibers on the host

Fibers are cooperative tasks on D Fiber stacks, run by the pool's workers,
that wait without blocking a worker. A body is a C function returning
zero for completed or a 24-bit failure code; every wait is a call that
returns a status.

```c
#include "antfarm_fibers.h"

static uint32_t fetch_and_sum(void *context) {
    struct job *job = context;
    af_status s = af_fiber_event_wait(job->ready, 50ull * 1000 * 1000);
    if (s == AF_FIBER_CANCELLED) { release(job); return 0; }   /* outcome cancelled, acknowledged */
    if (s == AF_FIBER_TIMED_OUT) return ERR_TIMEOUT;           /* outcome failed, code ERR_TIMEOUT */
    job->sum = add_up(job);
    if (af_fiber_yield() == AF_FIBER_CANCELLED) { release(job); return 0; }
    return 0;
}

af_fiber_domain_options domain_options;
af_fiber_domain_options_default(&domain_options);
af_fiber_domain *domain;
af_host_attach_fibers(host, &domain_options, &domain);        /* step serves and retires it */

af_fiber_event_create(domain, 0, &job.ready);                  /* auto-reset */
af_fiber_spawn_options spawn;
af_fiber_spawn_options_default(&spawn);
spawn.tag = MODULE_TAG;
af_fiber_handle task;
af_fiber_spawn(domain, fetch_and_sum, &job, &spawn, &task);
af_fiber_event_set(job.ready);                                 /* any thread, no attachment */
af_host_wait_until(host, fiber_done, &task, AF_WAIT_FOREVER);  /* fiber_done: af_fiber_poll reports a terminal outcome */
```

Rules worth reading twice:

- A body may migrate between workers at every wait. Keep nothing
  thread-local across one: no `errno`, no `thread_local` address, no lock
  held.
- Cancellation is a status, not an error. Once a wait has returned
  `AF_FIBER_CANCELLED`, every later wait returns it at once; the body
  cleans up and returns. Its return value keeps its meaning: zero gives
  outcome cancelled and disposition acknowledged, a code gives failed
  with `cleanupFailed` and the code retained.
- A handle is a 16-byte value. Copy it freely; a stale one reports
  `AF_FIBER_STALE`.
- Set, post, signal, poll and the blocking join work from any native
  thread without attachment, like `af_host_wake`. Spawn and cancellation
  requests need an attached thread. Waits are legal only inside a body.
- Retirement runs on the control thread inside step. A body that never
  reaches a wait holds its task open; bound such work yourself.

## 7. A reloadable module

A program that hot-swaps code keeps a resident part, which owns the host
and the Fiber domain, and a module it loads and unloads. The module gets
the function table rather than importing from the executable, and every
Fiber it spawns carries its tag.

Resident, at load:

```c
const af_api *api = af_api_get();
module_init(api, domain, generation_tag);     /* the module's own entry point */
```

Module:

```c
static const af_api *api;
void module_init(const af_api *table, af_fiber_domain *domain, uint32_t tag) {
    if (table->abi_version != 2 || table->fibers == NULL) fail();
    if (table->fibers->size < sizeof(af_api_fibers)) fail();   /* older resident: check what you use */
    api = table;
    af_fiber_spawn_options spawn;
    api->fibers->spawn_options_default(&spawn);
    spawn.tag = tag;
    api->fibers->spawn(domain, tick, state, &spawn, NULL);
}
```

Resident, at reload:

1. Close its own gate on new spawns for the departing module.
2. `af_fiber_cancel_tagged(domain, tag, &requested)`: every live task with
   the tag wakes with `AF_FIBER_CANCELLED` at its wait or at its next
   boundary.
3. Keep stepping until `af_fiber_count_tagged(domain, tag, &n)` reports
   zero. Retirement clears each task's body and context, so zero means no
   pointer into the module remains anywhere in the domain, the recycle
   pool included.
4. Unload the module, load the new one, hand it the table and a new tag.

What the rule relies on: nothing resident holds a module pointer across
step 4. Per-spawn completion callbacks live in the same cleared record;
domain-level handlers are the resident's. Pointers into the module can
also reach the host through request data, actor states and hooks; those
are the resident's to drain the same way.

## 8. Which thread may call what

| Calls | Thread | Druntime attachment |
|---|---|---|
| Core: Farm, producers, views, actors, waves, private roots | any native thread | none |
| Wake class: host wake and state, group wake, Fiber set, post, signal, poll and join | any native thread | none |
| Runtime control: init and shutdown; host create, step, wait, attach, close, abandon; group start, stop, close; dispatcher start, stop | the control thread | it initialised druntime |
| Runtime services: root submission, for and reduce, Fiber spawn and cancellation requests | any thread | attach first, detach before exit |
| Fiber boundaries: yield, sleep, await, event and semaphore waits, join from a Fiber, cancel current | inside a body only | not applicable |
| GC control: collect, minimize, enable, disable | attached threads, outside callbacks and bodies | attached |

Callbacks of every kind must not throw, `longjmp`, block on a worker, or
keep their borrowed arguments.

## 9. When something fails

- A status other than `AF_OK` comes with a message from
  `af_last_error_message` on the thread that got it, and an OS error from
  `af_last_os_error` when one applies. Inside a Fiber body, read it before
  the next wait: the diagnostic is thread-local and the body may move.
- `AF_INVALID` is a contract you broke that the library could check.
  `AF_BUSY` is a precondition not yet met, usually on close; the message
  names the first blocker, and the call is safe to repeat.
- `AF_RUNTIME_ERROR` is a D `Exception` with its message. A D `Error`, an
  assertion or an exhausted allocation inside the library, never returns:
  it reaches the handler from `af_set_fatal_handler`, which may log and
  dump, and then the process aborts. The same holds for contract
  violations the library cannot report, such as a forged handle.
- `AF_HOST_WORKER_FAILED` from a wait leaves only close and abandon.
  `AF_HOST_TIMED_OUT` leaves the host open: wait again, escalate, or
  abandon.

## 10. Teardown, in order

1. Stop publishing: quiesce every producer and every thread that wakes.
2. Let work finish: wait until your own completion counts say so, then
   drain attachments and take every tracked outcome.
3. Fibers: stop spawning, cancel what should not finish, step until the
   domain reports drained and the count of unretired tasks is zero.
4. Actors: request retirement of every owner, drive until retired,
   reclaim.
5. Return what you borrowed: unregister extra producers, unsubscribe and
   destroy external views, destroy waves.
6. `af_host_close`, repeating on `AF_BUSY` after reading the message.
7. `af_druntime_shutdown`, which refuses while any host or attachment is
   still open.

`af_host_abandon` is the emergency exit: it stops workers, keeps the ring
and unfinished work, invalidates every associated handle and leaks the
rest on purpose.

## 11. Moving an ABI 1 program

1. Rebuild against the ABI 2 headers. Option structures filled by
   `*_default` now carry version 2; code that wrote `abi_version = 1`
   into a structure of ABI 1 size still works.
2. Check the fatal handler. A D `Error` in a runtime export used to come
   back as `AF_RUNTIME_ERROR`; it now aborts after the handler runs.
3. Nothing else changes unless you opt in: `AF_COST_AUTO`, attachments,
   `af_host_wait_until`, Fibers, the table.

## 12. Mistakes the first week brings

- Calling a Fiber wait from a plain thread, or a control call from a
  worker. Both are death cases on purpose.
- Forgetting to wake after a write, then waiting for a parked consumer to
  notice.
- Keeping `errno` or a thread-local pointer across a Fiber wait.
- Treating `AF_FIBER_CANCELLED` as a failure and returning a code, which
  records `cleanupFailed`. Return zero after cleanup.
- Destroying a park after attaching it, or pumping it beside the host.
- A resident that keeps a module's callback or context in its own tables
  past the unload. The count rule covers the domain, not the resident.
- Collecting garbage from inside a body or a callback.
