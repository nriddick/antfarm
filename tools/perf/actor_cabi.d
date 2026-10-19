// D comparison for actor_cabi.c: same erased byte-copy state and allocator.
// BetterC keeps this driver free of druntime and native-thread attachment.
module antfarm.actors.cabi_bench;
import antfarm.actors.actor;
import antfarm.actors.cabi : af_actor_allocator, af_actor_owner;
import antfarm.c.core : af_farm;
import core.stdc.stdlib : abort, malloc, free;

private struct Runtime { ActorRuntime* impl; af_actor_allocator allocator; }
private void* allocate(void* c, size_t n, size_t a) nothrow @nogc @system
{ auto r = cast(Runtime*) c; return r.allocator.allocate(r.allocator.context, n, a); }
private void deallocate(void* c, void* p, size_t n, size_t a) nothrow @nogc @system
{ auto r = cast(Runtime*) c; r.allocator.deallocate(r.allocator.context, p, n, a); }
private void handler(scope ref ActorErasedBorrow, scope ref ActorContext) nothrow @nogc @system {}

extern(C) nothrow @nogc @system:
void* bench_d_runtime(af_farm* farm, size_t capacity, af_actor_allocator allocator)
{
    auto r = cast(Runtime*) malloc(Runtime.sizeof);
    if (r is null) abort();
    r.allocator = allocator;
    r.impl = ActorRuntime.create(farm.impl, capacity, ActorAllocator(r, &allocate, &deallocate));
    if (r.impl is null) abort();
    return r;
}
void bench_d_create(void* runtime, const(void)* initial, size_t bytes,
        af_actor_owner* owners, size_t count)
{
    auto r = cast(Runtime*) runtime;
    foreach (i; 0 .. count)
    {
        auto owner = r.impl.erasedAdapter.createActor(cast(const(ubyte)*) initial + i * bytes, bytes, 64, &handler);
        if (!owner.valid) abort();
        auto h = owner.handle;
        owners[i] = af_actor_owner(h.slot_, h.generation_);
        // ActorErasedOwner has no implicit cleanup; ownership transfers to the
        // benchmark's caller storage and is reclaimed below, after handoff.
    }
}
void bench_d_reclaim(void* runtime, af_actor_owner* owners, size_t count)
{
    foreach (i; 0 .. count)
    {
        auto slot = cast(ActorSlot*) owners[i]._slot;
        auto generation = owners[i]._generation;
        if (requestActorRetire(slot, generation, ActorRetireMode.drain) != ActorRetireResult.requested ||
            reclaimActor(slot, generation) != ActorReclaimResult.reclaimed) abort();
        owners[i] = af_actor_owner.init;
    }
}
void bench_d_destroy(void* runtime)
{
    auto r = cast(Runtime*) runtime;
    r.impl.destroy();
    free(r);
}
