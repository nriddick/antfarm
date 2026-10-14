module life.headless;

import life.backend;
import antfarm.actors : PrivateOutcomeStatus;
import farmhost : FarmHost;
import std.conv : to;
import std.exception : enforce;
import std.stdio : writefln, writeln;

// Deliberately simple reference: signed offsets and modulo wraparound.
private ubyte[] reference(const(ubyte)[] input, uint width, uint height)
{
    auto output = new ubyte[input.length];
    foreach (y; 0 .. height)
        foreach (x; 0 .. width)
        {
            uint neighbours;
            foreach (dy; -1 .. 2)
                foreach (dx; -1 .. 2)
                    if (dx != 0 || dy != 0)
                    {
                        auto nx = (cast(long) x + dx + width) % width;
                        auto ny = (cast(long) y + dy + height) % height;
                        neighbours += input[cast(size_t) (ny * width + nx)];
                    }
            auto i = cast(size_t) y * width + x;
            output[i] = cast(ubyte) (neighbours == 3 || (input[i] && neighbours == 2));
        }
    return output;
}

private void patterns(uint workers)
{
    // One host serves both simulators; declared first, it closes last.
    auto host = new FarmHost(workers);
    scope (exit) host.closeOrAbandon();
    auto life = new Life(host, 8, 8);
    scope(exit) life.close();
    life.setCell(2, 3, true);
    life.setCell(3, 3, true);
    life.setCell(4, 3, true);
    auto horizontal = life.current.dup;
    auto vertical = new ubyte[64];
    foreach (y; 2 .. 5) vertical[y * 8 + 3] = 1;
    life.step();
    enforce(life.current == vertical && life.generation == 1, "blinker generation 1");
    life.step();
    enforce(life.current == horizontal && life.generation == 2, "blinker generation 2");
    life.clear();
    foreach (y; 3 .. 5)
        foreach (x; 3 .. 5) life.setCell(x, y, true);
    auto block = life.current.dup;
    life.step(12);
    enforce(life.current == block && life.generation == 12, "block still life");
    life.clear();
    // Start across both torus edges; every four generations translate (1,1).
    uint[2][5] glider = [[1u, 0u], [2u, 1u], [0u, 2u], [1u, 2u], [2u, 2u]];
    foreach (cell; glider) life.setCell((cell[0] + 6) % 8, (cell[1] + 6) % 8, true);
    foreach (period; 1 .. 6)
    {
        life.step(4);
        auto expected = new ubyte[64];
        foreach (cell; glider)
            expected[((cell[1] + 6 + period) % 8) * 8 + (cell[0] + 6 + period) % 8] = 1;
        enforce(life.current == expected && life.generation == period * 4,
            "glider torus translation");
    }
    life.randomize(123456);
    auto expected = life.current.dup;
    foreach (generation; 1 .. 25)
    {
        expected = reference(expected, 8, 8);
        life.step();
        enforce(life.current == expected && life.generation == generation,
            "seeded reference generation mismatch");
    }
    life.begin(1_000_000);
    life.progress();
    life.cancel();
    while (life.busy) life.progress();
    enforce(life.lastStatus == PrivateOutcomeStatus.cancelled, "cooperative cancellation");
    // A cancelled partial wave must not become the current projection.
    foreach (_; 24 .. life.generation) expected = reference(expected, 8, 8);
    enforce(life.current == expected, "cancelled wave projection");
    life.step();
    enforce(life.current == reference(expected, 8, 8), "restart after cancellation");
    auto before = life.current.dup;
    auto generation = life.generation;
    life.step(0);
    enforce(life.current == before && life.generation == generation, "zero-generation request");

    // Degenerate tori count the eight wrapped neighbour positions separately.
    auto tiny = new Life(host, 1, 2);
    scope(exit) tiny.close();
    tiny.setCell(0, 0, true);
    auto small = tiny.current.dup;
    foreach (_; 0 .. 4)
    {
        small = reference(small, 1, 2);
        tiny.step();
        enforce(tiny.current == small, "small torus reference");
    }
    writeln("Life PASS: blinker, block, toroidal glider, seeded reference, cancel/restart; workers=", workers);
}

void main(string[] args)
{
    auto workers = args.length > 1 ? to!uint(args[1]) : 0;
    if (args.length > 2 && args[2] == "--bench")
    {
        enforce(args.length == 6, "usage: life_headless WORKERS --bench WIDTH HEIGHT GENERATIONS");
        auto host = new FarmHost(workers);
        scope (exit) host.closeOrAbandon();
        auto life = new Life(host, to!uint(args[3]), to!uint(args[4]));
        scope(exit) life.close();
        life.randomize(123456);
        life.step(to!ulong(args[5]));
        writefln("workers=%s generations=%s gen/s=%.3f cells/s=%.3f; %s", workers,
            life.generation, life.generationsPerSecond, life.cellsPerSecond,
            host.summary());
    }
    else patterns(workers);
}
