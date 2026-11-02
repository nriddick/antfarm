/+ dub.sdl:
    name "load_assets"
    targetType "executable"
    dependency "antfarm:ant_uv" path="../.."
    dependency "antfarm:forkjoin" path="../.."
    dependency "antfarm" path="../.."
    configuration "default" {
    }
    configuration "mimalloc" {
        subConfiguration "antfarm" "mimalloc-v3"
    }
+/
/// A resource manager at the start of every tick. The main thread drives
/// the ticks. At the start of each it runs the table's `step`: settle the
/// loads that finished, release what the previous tick asked for, and the
/// loads submitted since go out. Then the tick's work runs on the fork-join
/// workers, and the main thread waits for it. Nothing in the table changes
/// while that work runs, and an `unload` during the tick only queues, so
/// the work never reads a freed block. docs/guide/loading-files.md walks
/// through it.
///
///     dub run --single ant_uv/examples/load_assets.d
///
/// `--config=mimalloc` loads into mimalloc v3 instead of the C runtime. It
/// builds the pinned library from the third_party/mimalloc submodule, so it
/// needs the submodule, CMake and a C compiler.
module load_assets;

import ant_uv;
import antfarm.ring : RingElement, SlotRing;
import forkjoin : Dispatcher, forkJoinEach;
import core.thread : Thread;
import core.time : msecs;
import std.file : thisExePath;
import std.path : baseName;
import std.range : iota;
import std.stdio : writefln, writeln;
import std.string : indexOf, toStringz;

version (AntfarmMimallocV3)
{
    import antfarm.actors.mimalloc : mimallocV3ActorAllocator;
    enum allocatorName = "mimalloc v3";
}
else
    enum allocatorName = "the C runtime";

/// `releasing` is `ready` with a release queued: the bytes stay readable
/// until the next `step`.
enum AssetState { unloaded, loading, ready, releasing, failed, unloading }

/// One file-backed asset. Assets live in a fixed array, so the request
/// inside each keeps a stable address while the loop owns it.
struct Asset
{
    const(char)* path;  // NUL-terminated; the table keeps it alive
    AssetState state;
    UvRequest request;  // the loop's while loading or unloading
    UvBuffer file;      // the file while ready or releasing
}

/// Files loaded by the loop thread and collected by one owning thread, the
/// main thread here, at the start of each tick. Nothing in it blocks, and
/// only `step` frees.
final class AssetTable
{
    private UvLoop loop;
    private SlotRing!RingElement* completions; // the loop pushes; step pops
    private Asset[] assets;

    this(UvLoop loop, string[] paths)
    {
        this.loop = loop;
        completions = SlotRing!RingElement.create(64);
        assets = new Asset[paths.length];
        foreach (i, path; paths)
            assets[i].path = path.toStringz;
    }

    /// Start loading. False when the request ring is full: try next tick.
    bool load(size_t id)
    {
        auto a = &assets[id];
        if (a.state != AssetState.unloaded && a.state != AssetState.failed) return true;
        a.request = UvRequest.readFile(a.path); // open, read to the end, close
        a.request.user = id;                    // comes back as the element's tag
        a.request.completions = completions;    // deliver to this table
        final switch (loop.submit(&a.request))
        {
        case UvSubmit.accepted:
            a.state = AssetState.loading;
            return true;
        case UvSubmit.full:
            return false;
        case UvSubmit.closed, UvSubmit.invalid:
            a.state = AssetState.failed;
            return true;
        }
    }

    /// The tick's start: settle finished loads, then apply the releases
    /// asked for during the previous tick. Nothing else frees.
    void step()
    {
        RingElement e;
        while (completions.tryPop(e))
            settle(&assets[cast(size_t) e.tag]);
        foreach (ref a; assets)
            if (a.state == AssetState.releasing)
            {
                a.file.free(); // back to the allocator that made it
                a.state = AssetState.unloaded;
            }
    }

    // The popped element hands the request back: the loop no longer
    // touches it, and its outputs are visible here.
    private void settle(Asset* a)
    {
        if (a.state == AssetState.unloading)
        {
            a.request.freeBuffer(); // only if the read won the race
            a.state = AssetState.unloaded;
        }
        else if (a.request.succeeded)
        {
            a.file = a.request.takeBuffer();
            a.state = AssetState.ready;
        }
        else
            a.state = AssetState.failed;
    }

    AssetState state(size_t id) const nothrow @nogc { return assets[id].state; }

    const(char)[] error(size_t id) const nothrow @nogc { return assets[id].request.errorName; }

    /// The file's bytes while ready, else null. Valid until the `step`
    /// after `unload(id)`, so for the rest of the tick that unloads it.
    const(ubyte)[] bytes(size_t id) const nothrow @nogc
    {
        immutable s = assets[id].state;
        return s == AssetState.ready || s == AssetState.releasing ? assets[id].file.bytes : null;
    }

    /// Ask for the file to be released at the next `step`. A load still in
    /// flight is cancelled and finished by a later step, because the loop
    /// owns its request until then.
    void unload(size_t id)
    {
        auto a = &assets[id];
        final switch (a.state)
        {
        case AssetState.ready:
            a.state = AssetState.releasing;
            break;
        case AssetState.loading:
            loop.cancel(&a.request); // a hint: if it misses, step frees the result
            a.state = AssetState.unloading;
            break;
        case AssetState.unloaded, AssetState.failed, AssetState.releasing, AssetState.unloading:
            break;
        }
    }

    bool busy() const nothrow @nogc
    {
        foreach (ref a; assets)
            if (a.state == AssetState.loading || a.state == AssetState.unloading) return true;
        return false;
    }

    /// After `loop.close()`: every request is published by then, so settle
    /// what the ring could not take, and free everything.
    void shutdown()
    {
        step();
        foreach (ref a; assets)
            if ((a.state == AssetState.loading || a.state == AssetState.unloading)
                && a.request.finished)
                settle(&a);
        foreach (id; 0 .. assets.length)
            unload(id);
        step();
        completions.destroy();
    }
}

void main()
{
    // One loop thread for the application, and the fork-join workers for
    // the tick's work. A real program keeps the loop's CPU out of the
    // workers' set: UvLoopOptions.cpu, and Dispatcher.start(cpus) without it.
    UvLoopOptions options;
    version (AntfarmMimallocV3)
        options.allocator = mimallocV3ActorAllocator(); // every readFile, unless its request says otherwise
    auto loop = UvLoop.create(options);
    Dispatcher pool;
    pool.start();
    scope (exit) pool.stop();

    // This program's source, a system file, a missing file, and this
    // program's binary, which is large enough to still be loading when it
    // is unloaded.
    auto paths = [__FILE_FULL_PATH__, "/etc/os-release", "/no/such/file", thisExePath];
    auto table = new AssetTable(loop, paths);
    auto sums = new ulong[paths.length]; // one slot per asset: no two leaves share one

    writefln("loading %s files into %s", paths.length, allocatorName);
    foreach (id; 0 .. paths.length) table.load(id);
    table.unload(3); // changed our mind before it finished

    // The tick: the manager's step, then the tick's work on the workers,
    // which returns only when every leaf has run.
    void tick()
    {
        table.step();
        forkJoinEach(iota(paths.length), 1, (size_t id) nothrow @nogc @system {
            ulong sum;
            foreach (b; table.bytes(id)) sum += b;
            sums[id] = sum;
        });
    }

    size_t ticks;
    do
    {
        tick();
        ++ticks;
        Thread.sleep(1.msecs); // the rest of the frame
    }
    while (table.busy());
    writefln("settled after %s tick(s)", ticks);

    // Using the files, any number of ticks on.
    auto source = cast(const(char)[]) table.bytes(0);
    writefln("%s: %s bytes, first line %s", baseName(paths[0]), source.length,
        source[0 .. source.indexOf('\n')]);
    writefln("%s: %s bytes, byte sum %s", paths[1], table.bytes(1).length, sums[1]);
    writefln("%s: %s (%s)", paths[2], table.state(2), table.error(2));
    writefln("%s: %s", baseName(paths[3]), table.state(3));

    // An unload during a tick only queues: the bytes stay valid for the rest
    // of it, and the step at the start of the next tick frees them.
    table.unload(0);
    writefln("%s after unload: %s, %s bytes still readable", baseName(paths[0]), table.state(0),
        table.bytes(0).length);
    tick();
    writefln("%s after the next step: %s", baseName(paths[0]), table.state(0));

    auto report = loop.close();
    table.shutdown();
    writeln(report);
}
