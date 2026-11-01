/+ dub.sdl:
    name "load_assets"
    targetType "executable"
    dependency "antfarm:ant_uv" path="../.."
    dependency "antfarm" path="../.."
    configuration "default" {
    }
    configuration "mimalloc" {
        subConfiguration "antfarm" "mimalloc-v3"
    }
+/
/// Files loaded through the loop thread, collected by the main thread once
/// per frame, used on later frames, and unloaded, including one unloaded
/// while it is still loading. docs/guide/loading-files.md walks through it.
///
///     dub run --single ant_uv/examples/load_assets.d
///
/// `--config=mimalloc` loads into mimalloc v3 instead of the C runtime. It
/// builds the pinned library from the third_party/mimalloc submodule, so it
/// needs the submodule, CMake and a C compiler.
module load_assets;

import ant_uv;
import antfarm.ring : RingElement, SlotRing;
import core.thread : Thread;
import core.time : msecs;
import std.file : thisExePath;
import std.path : baseName;
import std.stdio : writefln, writeln;
import std.string : indexOf, toStringz;

version (AntfarmMimallocV3)
{
    import antfarm.actors.mimalloc : mimallocV3ActorAllocator;
    enum allocatorName = "mimalloc v3";
}
else
    enum allocatorName = "the C runtime";

enum AssetState { unloaded, loading, ready, failed, unloading }

/// One file-backed asset. Assets live in a fixed array, so the request
/// inside each keeps a stable address while the loop owns it.
struct Asset
{
    const(char)* path;  // NUL-terminated; the table keeps it alive
    AssetState state;
    UvRequest request;  // the loop's while loading or unloading
    UvBuffer file;      // the file while ready
}

/// Files loaded by the loop thread and collected by one owning thread, the
/// main thread here, once per tick. Nothing in it blocks.
final class AssetTable
{
    private UvLoop loop;
    private SlotRing!RingElement* completions; // the loop pushes; tick pops
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
        a.request = UvRequest.init;
        a.request.op = UvOp.readFile;          // open, read to the end, close
        a.request.path = a.path;
        a.request.user = id;                   // comes back as the element's tag
        a.request.completions = completions;   // deliver to this table
        version (AntfarmMimallocV3)
            a.request.allocator = mimallocV3ActorAllocator();
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

    /// Collect finished loads.
    void tick()
    {
        RingElement e;
        while (completions.tryPop(e))
            settle(&assets[cast(size_t) e.tag]);
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

    AssetState state(size_t id) { return assets[id].state; }

    const(char)[] error(size_t id) { return assets[id].request.errorName; }

    /// The file's bytes while ready, else null. Valid until `unload(id)`.
    const(ubyte)[] bytes(size_t id)
    {
        return assets[id].state == AssetState.ready ? assets[id].file.bytes : null;
    }

    /// Release the file. A load still in flight is cancelled and finished
    /// by a later tick, because the loop owns its request until then.
    void unload(size_t id)
    {
        auto a = &assets[id];
        final switch (a.state)
        {
        case AssetState.ready:
            a.file.free(); // back to the allocator that made it
            a.state = AssetState.unloaded;
            break;
        case AssetState.loading:
            loop.cancel(&a.request); // a hint: if it misses, tick frees the result
            a.state = AssetState.unloading;
            break;
        case AssetState.unloaded, AssetState.failed, AssetState.unloading:
            break;
        }
    }

    bool busy()
    {
        foreach (ref a; assets)
            if (a.state == AssetState.loading || a.state == AssetState.unloading) return true;
        return false;
    }

    /// After `loop.close()`: every request is published by then, so collect
    /// the completions, settle any the ring could not take, and free all.
    void shutdown()
    {
        tick();
        foreach (ref a; assets)
            if ((a.state == AssetState.loading || a.state == AssetState.unloading)
                && a.request.finished)
                settle(&a);
        foreach (id; 0 .. assets.length)
            unload(id);
        completions.destroy();
    }
}

void main()
{
    // One loop thread for the application. Pin it with options.cpu to a CPU
    // kept out of the worker set.
    auto loop = UvLoop.create();
    // This program's source, a system file, a missing file, and this
    // program's binary, which is large enough to still be loading when it
    // is unloaded.
    auto paths = [__FILE_FULL_PATH__, "/etc/os-release", "/no/such/file", thisExePath];
    auto table = new AssetTable(loop, paths);

    writefln("loading %s files into %s", paths.length, allocatorName);
    foreach (id; 0 .. paths.length) table.load(id);
    table.unload(3); // changed our mind before it finished

    // The game loop: collect, use what is ready, get on with the frame.
    size_t frames;
    while (table.busy())
    {
        table.tick();
        ++frames;
        Thread.sleep(1.msecs); // the rest of the frame
    }
    writefln("settled after %s frame(s)", frames);

    // Using the files later, any number of frames on.
    auto source = cast(const(char)[]) table.bytes(0);
    writefln("%s: %s bytes, first line %s", baseName(paths[0]), source.length,
        source[0 .. source.indexOf('\n')]);
    writefln("%s: %s bytes", paths[1], table.bytes(1).length);
    writefln("%s: %s (%s)", paths[2], table.state(2), table.error(2));
    writefln("%s: %s", baseName(paths[3]), table.state(3));

    // Unloading frees the bytes at once: nothing is in flight.
    table.unload(0);
    table.unload(1);

    auto report = loop.close();
    table.shutdown();
    writeln(report);
}
