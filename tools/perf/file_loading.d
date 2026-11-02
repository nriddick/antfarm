/// File loading through SDL3 AsyncIO and through ant_uv, from one thread that
/// plays a game's main thread: whole files of 16 KiB and 8 MiB, random 64 KiB
/// reads from one open 256 MiB file, and one 16 KiB file at a time, back to
/// back and after a millisecond idle. Up to 64 requests are outstanding. After
/// start-up nothing allocates from the GC: arrays, paths and output are C, the
/// measured paths are `@nogc`, and each process reports its GC collection count.
///
///     file_loading --setup DIR
///     file_loading --impl sdl|uv [--alloc crt|mi] [--aux N] [--io-uring] --dir DIR [--rounds R]
///
/// `--alloc mi` routes the file buffers through mimalloc v3: ant_uv's
/// `request.allocator`, and SDL's `SDL_SetMemoryFunctions`. Every buffer is
/// checked to come from the heap it should. `--io-uring` sets
/// `UvLoopOptions.ioUring` (Linux).
/// `file_loading_matrix.py` runs configurations interleaved and summarizes.
/// The record is docs/performance/file-loading-sdl3-2026-11-02.md.
module file_loading;

import ant_uv;
import ant_uv.binding : UV_FS_O_RDONLY;
import antfarm.actors.mimalloc : mimallocV3ActorAllocator;
import antfarm.ring : RingElement, SlotRing;
import core.lifetime : emplace;
import core.memory : GC;
import core.stdc.stdio : fclose, FILE, fflush, fopen, fprintf, fread, printf, snprintf, stderr, stdout;
import core.stdc.stdlib : atoi, calloc, exit, free, malloc, qsort;
import core.stdc.string : strcmp;
import core.thread : Thread;
import core.time : MonoTime, msecs;

extern (C) nothrow @nogc
{
    struct SDL_AsyncIO;
    struct SDL_AsyncIOQueue;
    struct SDL_AsyncIOOutcome
    {
        SDL_AsyncIO* asyncio;
        int type;
        int result;
        void* buffer;
        ulong offset;
        ulong bytes_requested;
        ulong bytes_transferred;
        void* userdata;
    }

    alias MallocFn = extern (C) void* function(size_t) nothrow @nogc;
    alias CallocFn = extern (C) void* function(size_t, size_t) nothrow @nogc;
    alias ReallocFn = extern (C) void* function(void*, size_t) nothrow @nogc;
    alias FreeFn = extern (C) void function(void*) nothrow @nogc;

    int SDL_GetVersion();
    const(char)* SDL_GetError();
    void SDL_free(void* mem);
    bool SDL_SetMemoryFunctions(MallocFn, CallocFn, ReallocFn, FreeFn);
    SDL_AsyncIO* SDL_AsyncIOFromFile(const(char)* file, const(char)* mode);
    bool SDL_ReadAsyncIO(SDL_AsyncIO* asyncio, void* ptr, ulong offset, ulong size,
        SDL_AsyncIOQueue* queue, void* userdata);
    bool SDL_CloseAsyncIO(SDL_AsyncIO* asyncio, bool flush, SDL_AsyncIOQueue* queue, void* userdata);
    SDL_AsyncIOQueue* SDL_CreateAsyncIOQueue();
    void SDL_DestroyAsyncIOQueue(SDL_AsyncIOQueue* queue);
    bool SDL_GetAsyncIOResult(SDL_AsyncIOQueue* queue, SDL_AsyncIOOutcome* outcome);
    bool SDL_LoadFileAsync(const(char)* file, SDL_AsyncIOQueue* queue, void* userdata);

    void* mi_malloc(size_t size);
    void* mi_calloc(size_t count, size_t size);
    void* mi_realloc(void* p, size_t size);
    void mi_free(void* p);
    bool mi_is_in_heap_region(const(void)* p);
}

enum int sdlRead = 0, sdlClose = 2, sdlComplete = 0;

enum size_t window = 64;              // requests outstanding at once
enum size_t smallCount = 2000, smallSize = 16 << 10;
enum size_t largeCount = 32, largeSize = 8 << 20;
enum size_t pakSize = 256 << 20, block = 64 << 10, pakReads = 4096;
enum size_t fileLoads = 2000, idleLoads = 500;
enum long workThresholdNs = 2_000;    // an empty SDL poll this slow did work
enum size_t pathMax = 512;

// Everything the measured paths call is C or @nogc.
nothrow @nogc
{

long ns(MonoTime a, MonoTime b) { return (b - a).total!"nsecs"; }

void fail(const(char)* what)
{
    fprintf(stderr, "FAIL: %s\n", what);
    fflush(stderr);
    exit(1);
}

void sdlFail(const(char)* what)
{
    fprintf(stderr, "FAIL: %s: %s\n", what, SDL_GetError());
    fflush(stderr);
    exit(1);
}

T* cnew(T)(size_t n)
{
    auto p = cast(T*) calloc(n, T.sizeof);
    if (p is null) fail("out of memory");
    return p;
}

ulong firstWord(const(void)* p) { return *cast(const(ulong)*) p; }

// ---- process CPU time ---------------------------------------------------------

version (Windows)
{
    struct FILETIME { uint lo, hi; }
    extern (Windows) int GetProcessTimes(void*, FILETIME*, FILETIME*, FILETIME*, FILETIME*);
    extern (Windows) void* GetCurrentProcess();
    double cpuSeconds()
    {
        FILETIME c, e, k, u;
        GetProcessTimes(GetCurrentProcess(), &c, &e, &k, &u);
        static ulong t(FILETIME f) { return (cast(ulong) f.hi << 32) | f.lo; }
        return (t(k) + t(u)) / 1e7;
    }
}
else
{
    import core.sys.posix.sys.resource : getrusage, rusage, RUSAGE_SELF;
    double cpuSeconds()
    {
        rusage r;
        getrusage(RUSAGE_SELF, &r);
        return r.ru_utime.tv_sec + r.ru_stime.tv_sec + (r.ru_utime.tv_usec + r.ru_stime.tv_usec) / 1e6;
    }
}

// ---- results --------------------------------------------------------------------

struct Totals
{
    long wallNs, submitNs, handleNs;
    size_t ops;
    ulong bytes;
}

__gshared char[64] implName;
__gshared double cpuMark;
__gshared MonoTime wallMark;

void startClock()
{
    cpuMark = cpuSeconds();
    wallMark = MonoTime.currTime;
}

double cpuPerWall()
{
    return (cpuSeconds() - cpuMark) / (ns(wallMark, MonoTime.currTime) / 1e9);
}

void report(const(char)* scenario, size_t round, ref const Totals t)
{
    immutable cpu = cpuPerWall();
    printf("RESULT impl=%s scenario=%s round=%d ops=%d bytes=%llu wall_ms=%.3f submit_us_per_op=%.3f "
        ~ "handle_us_per_op=%.3f mib_per_s=%.1f ops_per_s=%.0f cpu_per_wall=%.2f\n", implName.ptr,
        scenario, cast(int) round, cast(int) t.ops, t.bytes, t.wallNs / 1e6,
        t.submitNs / 1e3 / t.ops, t.handleNs / 1e3 / t.ops,
        t.bytes / (1024.0 * 1024.0) / (t.wallNs / 1e9), t.ops / (t.wallNs / 1e9), cpu);
    fflush(stdout);
}

extern (C) int compareLong(const(void)* a, const(void)* b)
{
    immutable x = *cast(const(long)*) a, y = *cast(const(long)*) b;
    return x < y ? -1 : x > y;
}

void reportLatency(const(char)* scenario, size_t round, long* samples, size_t n)
{
    immutable cpu = cpuPerWall();
    qsort(samples, n, long.sizeof, &compareLong);
    double q(double f) { return samples[cast(size_t)(f * (n - 1))] / 1e3; }
    printf("LATENCY impl=%s scenario=%s round=%d n=%d p50_us=%.2f p90_us=%.2f p99_us=%.2f "
        ~ "p999_us=%.2f max_us=%.2f cpu_per_wall=%.2f\n", implName.ptr, scenario, cast(int) round,
        cast(int) n, q(0.5), q(0.9), q(0.99), q(0.999), samples[n - 1] / 1e3, cpu);
    fflush(stdout);
}

// Every buffer must come from the heap the configuration names.
__gshared bool useMi;

void checkHeap(const(void)* p)
{
    if (p !is null && mi_is_in_heap_region(p) != useMi)
        fail(useMi ? "a file buffer did not come from mimalloc" : "a file buffer came from mimalloc");
}

const(char)** makePaths(const(char)* dir, const(char)* pattern, size_t count)
{
    auto text = cnew!char(count * pathMax);
    auto paths = cnew!(const(char)*)(count);
    foreach (i; 0 .. count)
    {
        snprintf(text + i * pathMax, pathMax, pattern, dir, cast(int) i);
        paths[i] = text + i * pathMax;
    }
    return paths;
}

// Read a file through the C runtime so the page cache holds it.
void warm(const(char)* path, ubyte* scratch, size_t scratchSize)
{
    auto f = fopen(path, "rb");
    if (f is null) fail("warm open");
    while (fread(scratch, 1, scratchSize, f) == scratchSize) {}
    fclose(f);
}

} // nothrow @nogc

// ---- ant_uv ---------------------------------------------------------------------

struct Uv
{
nothrow @nogc:
    UvLoop loop;
    SlotRing!RingElement* ring;
    ActorAllocator allocator;

    UvRequest* requests(size_t n)
    {
        auto reqs = cnew!UvRequest(n);
        foreach (i; 0 .. n) emplace(&reqs[i]);
        return reqs;
    }

    // Whole files into the loop's allocation, collected from the completion
    // ring, checked and freed.
    Totals whole(const(char)** paths, size_t n, size_t size)
    {
        auto reqs = requests(n);
        Totals t;
        size_t next, done, inflight;
        immutable t0 = MonoTime.currTime;
        while (done < n)
        {
            while (next < n && inflight < window)
            {
                auto r = &reqs[next];
                r.op = UvOp.readFile;
                r.path = paths[next];
                r.user = next;
                r.completions = ring;
                r.allocator = allocator;
                immutable a = MonoTime.currTime;
                immutable s = loop.submit(r);
                t.submitNs += ns(a, MonoTime.currTime);
                if (s == UvSubmit.full) break;
                if (s != UvSubmit.accepted) fail("uv whole submit");
                ++next;
                ++inflight;
            }
            immutable a = MonoTime.currTime;
            bool got;
            RingElement e;
            while (ring.tryPop(e))
            {
                got = true;
                auto r = &reqs[cast(size_t) e.tag];
                if (!r.succeeded || r.length != size || firstWord(r.buffer) != e.tag)
                    fail("uv whole read");
                checkHeap(r.buffer);
                auto file = r.takeBuffer();
                t.bytes += file.bytes.length;
                file.free();
                ++done;
                --inflight;
            }
            if (got) t.handleNs += ns(a, MonoTime.currTime);
        }
        t.wallNs = ns(t0, MonoTime.currTime);
        t.ops = n;
        free(reqs);
        return t;
    }

    int open(const(char)* path)
    {
        UvRequest r;
        r.op = UvOp.open;
        r.path = path;
        r.flags = UV_FS_O_RDONLY;
        if (loop.submit(&r) != UvSubmit.accepted) fail("uv open submit");
        while (!r.finished) {}
        if (r.result < 0) fail("uv open");
        return cast(int) r.result;
    }

    void close(int file)
    {
        UvRequest r;
        r.op = UvOp.close;
        r.file = file;
        if (loop.submit(&r) != UvSubmit.accepted) fail("uv close submit");
        while (!r.finished) {}
    }

    // Random 64 KiB reads from one open file into caller buffers.
    Totals pak(int file, const(size_t)* blocks, ubyte* slots)
    {
        immutable n = pakReads;
        auto reqs = requests(n);
        auto slotOf = cnew!size_t(n);
        size_t[window] freeSlots;
        size_t freeCount = window;
        foreach (s; 0 .. window) freeSlots[s] = window - 1 - s;
        Totals t;
        size_t next, done;
        immutable t0 = MonoTime.currTime;
        while (done < n)
        {
            while (next < n && freeCount)
            {
                immutable s = freeSlots[freeCount - 1];
                auto r = &reqs[next];
                r.op = UvOp.read;
                r.file = file;
                r.buffer = slots + s * block;
                r.length = block;
                r.offset = cast(long)(blocks[next] * block);
                r.user = next;
                r.completions = ring;
                immutable a = MonoTime.currTime;
                immutable st = loop.submit(r);
                t.submitNs += ns(a, MonoTime.currTime);
                if (st == UvSubmit.full) break;
                if (st != UvSubmit.accepted) fail("uv pak submit");
                --freeCount;
                slotOf[next] = s;
                ++next;
            }
            immutable a = MonoTime.currTime;
            bool got;
            RingElement e;
            while (ring.tryPop(e))
            {
                got = true;
                immutable i = cast(size_t) e.tag;
                auto r = &reqs[i];
                if (!r.done || r.result != block || firstWord(r.buffer) != blocks[i]) fail("uv pak read");
                t.bytes += block;
                freeSlots[freeCount++] = slotOf[i];
                ++done;
            }
            if (got) t.handleNs += ns(a, MonoTime.currTime);
        }
        t.wallNs = ns(t0, MonoTime.currTime);
        t.ops = n;
        free(slotOf);
        free(reqs);
        return t;
    }

    // One whole small file at a time, from submit until the bytes are usable;
    // with `idle`, after a millisecond's sleep, so the loop's threads are asleep.
    long* fileLatency(const(char)** paths, size_t pathCount, size_t count, bool idle)
    {
        auto samples = cnew!long(count);
        foreach (i; 0 .. count)
        {
            if (idle) Thread.sleep(1.msecs);
            UvRequest r;
            r.op = UvOp.readFile;
            r.path = paths[i % pathCount];
            r.allocator = allocator;
            immutable a = MonoTime.currTime;
            if (loop.submit(&r) != UvSubmit.accepted) fail("uv file submit");
            while (!r.finished) {}
            samples[i] = ns(a, MonoTime.currTime);
            if (!r.succeeded || firstWord(r.buffer) != i % pathCount) fail("uv file read");
            checkHeap(r.buffer);
            r.freeBuffer();
        }
        return samples;
    }
}

// ---- SDL3 AsyncIO ---------------------------------------------------------------

struct Sdl
{
nothrow @nogc:
    SDL_AsyncIOQueue* queue;

    void renew()
    {
        if (queue !is null) SDL_DestroyAsyncIOQueue(queue); // waits for one-shot closes
        queue = SDL_CreateAsyncIOQueue();
        if (queue is null) sdlFail("SDL_CreateAsyncIOQueue");
    }

    // Poll once: true when an outcome was returned. A poll that returned
    // nothing but took longer than the threshold did work (it took a
    // one-shot close, whose outcome SDL keeps), and is counted too.
    bool poll(ref SDL_AsyncIOOutcome o, ref Totals t)
    {
        immutable a = MonoTime.currTime;
        immutable got = SDL_GetAsyncIOResult(queue, &o);
        immutable d = ns(a, MonoTime.currTime);
        if (got || d > workThresholdNs) t.handleNs += d;
        return got;
    }

    Totals whole(const(char)** paths, size_t n, size_t size)
    {
        Totals t;
        size_t next, done, inflight;
        immutable t0 = MonoTime.currTime;
        while (done < n)
        {
            while (next < n && inflight < window)
            {
                immutable a = MonoTime.currTime;
                immutable ok = SDL_LoadFileAsync(paths[next], queue, cast(void*) next);
                t.submitNs += ns(a, MonoTime.currTime);
                if (!ok) sdlFail("SDL_LoadFileAsync");
                ++next;
                ++inflight;
            }
            SDL_AsyncIOOutcome o;
            while (poll(o, t))
            {
                immutable a = MonoTime.currTime;
                immutable i = cast(size_t) o.userdata;
                if (o.type != sdlRead || o.result != sdlComplete || o.bytes_transferred != size
                        || firstWord(o.buffer) != i)
                    fail("SDL whole read");
                checkHeap(o.buffer);
                t.bytes += o.bytes_transferred;
                SDL_free(o.buffer);
                t.handleNs += ns(a, MonoTime.currTime);
                ++done;
                --inflight;
            }
        }
        t.wallNs = ns(t0, MonoTime.currTime);
        t.ops = n;
        renew();
        return t;
    }

    SDL_AsyncIO* open(const(char)* path)
    {
        auto f = SDL_AsyncIOFromFile(path, "r");
        if (f is null) sdlFail("SDL_AsyncIOFromFile");
        return f;
    }

    void close(SDL_AsyncIO* f)
    {
        if (!SDL_CloseAsyncIO(f, false, queue, null)) sdlFail("SDL_CloseAsyncIO");
        SDL_AsyncIOOutcome o;
        while (!SDL_GetAsyncIOResult(queue, &o) || o.type != sdlClose) {}
    }

    Totals pak(SDL_AsyncIO* f, const(size_t)* blocks, ubyte* slots)
    {
        immutable n = pakReads;
        auto slotOf = cnew!size_t(n);
        size_t[window] freeSlots;
        size_t freeCount = window;
        foreach (s; 0 .. window) freeSlots[s] = window - 1 - s;
        Totals t;
        size_t next, done;
        immutable t0 = MonoTime.currTime;
        while (done < n)
        {
            while (next < n && freeCount)
            {
                immutable s = freeSlots[freeCount - 1];
                immutable a = MonoTime.currTime;
                immutable ok = SDL_ReadAsyncIO(f, slots + s * block, blocks[next] * block, block, queue,
                    cast(void*) next);
                t.submitNs += ns(a, MonoTime.currTime);
                if (!ok) sdlFail("SDL_ReadAsyncIO");
                --freeCount;
                slotOf[next] = s;
                ++next;
            }
            SDL_AsyncIOOutcome o;
            while (poll(o, t))
            {
                immutable i = cast(size_t) o.userdata;
                if (o.type != sdlRead || o.result != sdlComplete || o.bytes_transferred != block
                        || firstWord(o.buffer) != blocks[i])
                    fail("SDL pak read");
                t.bytes += block;
                freeSlots[freeCount++] = slotOf[i];
                ++done;
            }
        }
        t.wallNs = ns(t0, MonoTime.currTime);
        t.ops = n;
        free(slotOf);
        return t;
    }

    long* fileLatency(const(char)** paths, size_t pathCount, size_t count, bool idle)
    {
        auto samples = cnew!long(count);
        foreach (i; 0 .. count)
        {
            if (idle) Thread.sleep(1.msecs);
            immutable a = MonoTime.currTime;
            if (!SDL_LoadFileAsync(paths[i % pathCount], queue, null)) sdlFail("SDL file submit");
            SDL_AsyncIOOutcome o;
            while (!SDL_GetAsyncIOResult(queue, &o) || o.type != sdlRead) {}
            samples[i] = ns(a, MonoTime.currTime);
            if (o.result != sdlComplete || firstWord(o.buffer) != i % pathCount) fail("SDL file read");
            checkHeap(o.buffer);
            SDL_free(o.buffer);
        }
        renew();
        return samples;
    }
}

// ---- data -------------------------------------------------------------------------

void setup(const(char)* dir)
{
    import std.file : mkdirRecurse, write;
    import std.path : buildPath;
    import std.string : fromStringz;

    auto d = dir.fromStringz.idup;
    mkdirRecurse(buildPath(d, "small"));
    mkdirRecurse(buildPath(d, "large"));
    auto b = cast(ubyte*) malloc(pakSize);
    void fill(size_t size, ulong id)
    {
        foreach (k; 0 .. size) b[k] = cast(ubyte)(k * 31 + id);
        *cast(ulong*) b = id;
    }
    char[pathMax] p;
    foreach (i; 0 .. smallCount)
    {
        fill(smallSize, i);
        snprintf(p.ptr, pathMax, "%s/small/f%04d.bin", dir, cast(int) i);
        write(p.ptr.fromStringz, b[0 .. smallSize]);
    }
    foreach (i; 0 .. largeCount)
    {
        fill(largeSize, i);
        snprintf(p.ptr, pathMax, "%s/large/g%02d.bin", dir, cast(int) i);
        write(p.ptr.fromStringz, b[0 .. largeSize]);
    }
    fill(pakSize, 0);
    foreach (k; 0 .. pakSize / block) *cast(ulong*)(b + k * block) = k;
    snprintf(p.ptr, pathMax, "%s/pak.bin", dir);
    write(p.ptr.fromStringz, b[0 .. pakSize]);
    printf("wrote %d small, %d large and a %d MiB pak under %s\n", cast(int) smallCount,
        cast(int) largeCount, cast(int)(pakSize >> 20), dir);
}

// ---- driver -------------------------------------------------------------------------

void main(string[] args) @system
{
    const(char)* impl, dir, setupDir, alloc = "crt";
    uint aux, rounds = 3;
    bool ioUring;
    // druntime's arguments are not NUL-terminated on Windows: copy each value.
    static const(char)* cstr(string s)
    {
        import core.stdc.string : memcpy;
        auto p = cnew!char(s.length + 1);
        memcpy(p, s.ptr, s.length);
        return p;
    }
    for (size_t i = 1; i < args.length; ++i)
    {
        immutable a = args[i];
        const(char)* next() { if (i + 1 >= args.length) fail("missing value"); return cstr(args[++i]); }
        if (a == "--setup") setupDir = next();
        else if (a == "--impl") impl = next();
        else if (a == "--alloc") alloc = next();
        else if (a == "--aux") aux = atoi(next());
        else if (a == "--dir") dir = next();
        else if (a == "--rounds") rounds = atoi(next());
        else if (a == "--io-uring") ioUring = true;
        else fail("unknown argument");
    }
    if (setupDir !is null) { setup(setupDir); return; }
    immutable sdlImpl = impl !is null && strcmp(impl, "sdl") == 0;
    if (!sdlImpl && (impl is null || strcmp(impl, "uv") != 0)) fail("--impl sdl|uv");
    if (strcmp(alloc, "mi") == 0) useMi = true;
    else if (strcmp(alloc, "crt") != 0) fail("--alloc crt|mi");
    if (dir is null) fail("--dir DIR");

    // SDL's allocator must be replaced before SDL allocates anything.
    if (sdlImpl && useMi && !SDL_SetMemoryFunctions(&mi_malloc, &mi_calloc, &mi_realloc, &mi_free))
        sdlFail("SDL_SetMemoryFunctions");

    auto small = makePaths(dir, "%s/small/f%04d.bin", smallCount);
    auto large = makePaths(dir, "%s/large/g%02d.bin", largeCount);
    char[pathMax] pakPath;
    snprintf(pakPath.ptr, pathMax, "%s/pak.bin", dir);
    {
        enum scratchSize = 8 << 20;
        auto scratch = cnew!ubyte(scratchSize);
        foreach (i; 0 .. smallCount) warm(small[i], scratch, scratchSize);
        foreach (i; 0 .. largeCount) warm(large[i], scratch, scratchSize);
        warm(pakPath.ptr, scratch, scratchSize);
        free(scratch);
    }
    // The same random blocks for every configuration (xorshift, fixed seed).
    auto blocks = cnew!size_t(pakReads);
    ulong x = 42;
    foreach (i; 0 .. pakReads)
    {
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        blocks[i] = cast(size_t)(x % (pakSize / block));
    }
    auto slots = cnew!ubyte(window * block);

    Uv uv;
    Sdl sdl;
    if (sdlImpl)
    {
        immutable v = SDL_GetVersion();
        sdl.renew();
        snprintf(implName.ptr, implName.length, "sdl3-%s", alloc);
        fprintf(stderr, "SDL %d.%d.%d\n", v / 1_000_000, v / 1000 % 1000, v % 1000);
    }
    else
    {
        UvLoopOptions o;
        o.auxThreads = aux;
        o.ioUring = ioUring;
        uv.loop = UvLoop.create(o);
        if (ioUring && !uv.loop.ioUring) fail("--io-uring did not take effect");
        uv.ring = SlotRing!RingElement.create(1024);
        if (useMi) uv.allocator = mimallocV3ActorAllocator();
        snprintf(implName.ptr, implName.length, "ant_uv-aux%d-%s%s",
            cast(int) uv.loop.auxThreads.length, alloc, ioUring ? "-io_uring".ptr : "".ptr);
    }

    // Round 0 warms up and is not reported.
    foreach (round; 0 .. rounds + 1)
    {
        void emit(const(char)* s, Totals t) { if (round) report(s, round, t); }
        void emitLatency(const(char)* s, long* l, size_t n)
        {
            if (round) reportLatency(s, round, l, n);
            free(l);
        }
        immutable loads = round ? fileLoads : 200, idles = round ? idleLoads : 20;
        if (sdlImpl)
        {
            startClock(); emit("whole-16KiB", sdl.whole(small, smallCount, smallSize));
            startClock(); emit("whole-8MiB", sdl.whole(large, largeCount, largeSize));
            auto f = sdl.open(pakPath.ptr);
            startClock(); emit("pak-64KiB", sdl.pak(f, blocks, slots));
            sdl.close(f);
            startClock(); emitLatency("file-16KiB", sdl.fileLatency(small, smallCount, loads, false), loads);
            startClock(); emitLatency("file-16KiB-idle", sdl.fileLatency(small, smallCount, idles, true), idles);
        }
        else
        {
            startClock(); emit("whole-16KiB", uv.whole(small, smallCount, smallSize));
            startClock(); emit("whole-8MiB", uv.whole(large, largeCount, largeSize));
            immutable f = uv.open(pakPath.ptr);
            startClock(); emit("pak-64KiB", uv.pak(f, blocks, slots));
            uv.close(f);
            startClock(); emitLatency("file-16KiB", uv.fileLatency(small, smallCount, loads, false), loads);
            startClock(); emitLatency("file-16KiB-idle", uv.fileLatency(small, smallCount, idles, true), idles);
        }
    }
    if (sdlImpl) SDL_DestroyAsyncIOQueue(sdl.queue);
    printf("GC impl=%s collections=%llu\n", implName.ptr, cast(ulong) GC.profileStats().numCollections);
    fflush(stdout);
    if (!sdlImpl)
    {
        uv.loop.close();
        uv.ring.destroy();
    }
}
