module life.backend;

import antfarm;
import actors;
import example_host;
import core.time : MonoTime;
import std.exception : enforce;
import std.random : Random, uniform;

private struct Board
{
    uint width, height;
    ubyte[][2] projections;
    uint current;
    ulong generation;
}

private struct Cell
{
    uint x, y;
    Board* board;
}

private void stepCell(scope ref PrivateBorrow!Cell actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    auto cell = &actor.value();
    auto board = cell.board;
    auto input = board.projections[board.current];
    auto output = board.projections[1 - board.current];
    immutable left = cell.x == 0 ? board.width - 1 : cell.x - 1;
    immutable right = cell.x + 1 == board.width ? 0 : cell.x + 1;
    immutable up = cell.y == 0 ? board.height - 1 : cell.y - 1;
    immutable down = cell.y + 1 == board.height ? 0 : cell.y + 1;
    immutable index = cast(size_t) cell.y * board.width + cell.x;
    immutable neighbours = input[cast(size_t) up * board.width + left]
        + input[cast(size_t) up * board.width + cell.x]
        + input[cast(size_t) up * board.width + right]
        + input[cast(size_t) cell.y * board.width + left]
        + input[cast(size_t) cell.y * board.width + right]
        + input[cast(size_t) down * board.width + left]
        + input[cast(size_t) down * board.width + cell.x]
        + input[cast(size_t) down * board.width + right];
    // Every member writes only its own next-generation slot. Nobody writes
    // input during a wave; all cross-cell reads use the previous projection.
    output[index] = cast(ubyte) (neighbours == 3 || (input[index] && neighbours == 2));
}

private struct Run
{
    Board* board;
    ulong remaining;
    PrivateChildPark!Cell* cells;
}

private PrivateRootDisposition runWave(scope ref PrivateBorrow!Run actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    auto run = &actor.value();
    if (run.cells !is null)
    {
        // A cancelled wave may have written only a prefix: discard it.
        if (context.cancelled || run.cells.outcome(context).cancelled)
            return PrivateRootDisposition.finish;
        // Callback writes -> Shc/Tprogress join -> wave credits -> root
        // waitChildren resume -> next start/publication. This orders all
        // previous writes before peers read them in the next wave.
        run.board.current = 1 - run.board.current;
        ++run.board.generation;
        --run.remaining;
    }
    if (context.cancelled || run.remaining == 0)
        return PrivateRootDisposition.finish;
    if (run.cells is null)
    {
        run.cells = context.createChildren!Cell(
            cast(size_t) run.board.width * run.board.height);
        if (run.cells is null) fatal("Life cell cohort allocation failed");
        foreach (y; 0 .. run.board.height)
            foreach (x; 0 .. run.board.width)
                if (run.cells.spawn(context, Cell(x, y, run.board))
                        != PrivateSpawnResult.spawned)
                    fatal("Life cell spawn failed");
    }
    if (run.cells.start!stepCell(context) != PrivateStartResult.started)
        fatal("Life wave rejected");
    return PrivateRootDisposition.waitChildren;
}

// Each request owns one cohort. App-owned buffers outlive all cohorts and
// remain rooted by this simulator until the tracked root has drained.
final class Life
{
    private Board* board;
    private ExampleHost host;
    private Token token;
    private PrivateRootPark!(Run, runWave)* park;
    private PrivateRootHandle root;
    private MonoTime started;
    private ulong initialGeneration;
    private double elapsed;
    private ulong timedGenerations;
    private bool running;
    immutable uint workers;
    PrivateOutcomeStatus lastStatus;

    this(uint width, uint height, uint workerCount = 0)
    {
        enforce(width > 0 && height > 0
            && cast(ulong) width * height <= uint.max, "invalid Life board dimensions");
        workers = workerCount;
        board = new Board;
        board.width = width;
        board.height = height;
        foreach (ref projection; board.projections)
            projection = new ubyte[cast(size_t) width * height];
        host = new ExampleHost(workers);
        token = host.farm.registerProducer(Tier.small);
        if (!token.valid) fatal("Life producer registration failed");
        PrivateRootOptions options;
        options.tracking = true;
        park = PrivateRootPark!(Run, runWave).create(host.farm, 1, options);
        if (park is null) fatal("Life root park allocation failed");
    }

    @property bool busy() const { return running; }
    @property uint width() const { return board.width; }
    @property uint height() const { return board.height; }
    @property ulong generation() const
    {
        enforce(!running, "Life generation is available after drain");
        return board.generation;
    }
    @property const(ubyte)[] current() const
    {
        enforce(!running, "Life projections are available after drain");
        return board.projections[board.current];
    }
    @property double generationsPerSecond() const
    {
        return elapsed == 0 ? 0 : timedGenerations / elapsed;
    }
    @property double cellsPerSecond() const
    {
        return generationsPerSecond * width * height;
    }

    void begin(ulong generations)
    {
        enforce(park !is null && !running, "Life request requires an idle simulator");
        if (generations == 0) return;
        host.resetDeadline();
        started = MonoTime.currTime;
        initialGeneration = board.generation;
        if (park.spawn(Run(board, generations), root) != PrivateSpawnResult.spawned)
            fatal("Life root spawn failed");
        running = true;
    }

    // UI thread pumps and takes outcomes; workers only run callbacks.
    void progress()
    {
        if (!running) return;
        park.pump(token);
        host.progress();
        PrivateRootOutcome outcome;
        if (park.takeOutcome(outcome))
        {
            lastStatus = outcome.status;
            if (lastStatus == PrivateOutcomeStatus.failed)
                fatal("Life root failed");
            elapsed += (MonoTime.currTime - started).total!"nsecs" / 1e9;
            timedGenerations += board.generation - initialGeneration;
            running = false;
        }
    }

    void step(ulong generations = 1)
    {
        begin(generations);
        while (busy) progress();
    }

    void cancel()
    {
        if (running) root.requestCancel();
    }

    void setCell(uint x, uint y, bool alive)
    {
        enforce(!running && x < width && y < height, "Life edit requires an idle valid cell");
        board.projections[board.current][cast(size_t) y * width + x] = cast(ubyte) alive;
    }

    void clear()
    {
        enforce(!running, "Life clear requires an idle simulator");
        board.current = 0;
        board.projections[board.current][] = 0;
        board.generation = 0;
        elapsed = 0;
        timedGenerations = 0;
    }

    void randomize(uint seed)
    {
        clear();
        auto random = Random(seed);
        foreach (ref cell; board.projections[board.current])
            cell = cast(ubyte) uniform(0, 2, random);
    }

    void close()
    {
        if (park is null) return;
        cancel();
        while (busy) progress();
        // Drain includes takeOutcome, then join/unsubscribe, destroy park,
        // unregister producer, destroy Farm. Buffers remain alive throughout.
        host.stopConsumers();
        park.destroy();
        park = null;
        host.farm.unregisterProducer(token);
        host.farm.destroy();
    }
}
