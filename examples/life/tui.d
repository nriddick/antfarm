module life.tui;

import life.backend;
import life.termbox;
import threadpool.host : FarmHost;
import core.stdc.errno : EINTR;
import core.time : MonoTime, msecs;
import std.algorithm : max, min;
import std.conv : to;
import std.exception : enforce;
import std.format : format;
import std.random : unpredictableSeed;

private void render(Life life, uint x, uint y, bool paused, uint speed)
{
    enforce(tb_clear() == 0, "termbox clear failed");
    auto cells = life.current;
    // Startup dimensions stay fixed. Resize clips the board to the viewport.
    immutable visibleWidth = min(cast(int) life.width, tb_width());
    immutable visibleHeight = max(0, min(cast(int) life.height, tb_height() - 2));
    foreach (row; 0 .. visibleHeight)
        foreach (column; 0 .. visibleWidth)
            enforce(tb_set_cell(column, row,
                cells[cast(size_t) row * life.width + column] ? '#' : ' ',
                TB_DEFAULT, TB_DEFAULT) == 0, "termbox cell failed");
    auto status = format("gen %s | %.1f gen/s | %.0f cells/s | workers %s | %s | speed %s",
        life.generation, life.generationsPerSecond, life.cellsPerSecond,
        life.host.plan.workers, paused ? "paused" : "running", speed);
    auto help = "space pause  n step  r random  c clear  g glider  arrows move  enter toggle  +/- speed  q quit";
    foreach (row, line; [status, help])
        foreach (column, ch; line)
            if (column < tb_width() && tb_height() >= 2)
                enforce(tb_set_cell(cast(int) column, tb_height() - 2 + cast(int) row,
                    ch, TB_REVERSE, TB_DEFAULT) == 0, "termbox status failed");
    enforce(tb_set_cursor(x < visibleWidth && y < visibleHeight ? cast(int) x : -1,
        x < visibleWidth && y < visibleHeight ? cast(int) y : -1) == 0,
        "termbox cursor failed");
    enforce(tb_present() == 0, "termbox present failed");
}

void main(string[] args)
{
    auto workers = args.length > 1 ? to!uint(args[1]) : 2;
    // Restore on ordinary exit and D exceptions. Runtime aborts/signals are
    // outside scope(exit); see the README's terminal recovery limitation.
    scope(exit) tb_shutdown();
    enforce(tb_init() == 0, "termbox initialization failed (a POSIX tty is required)");
    enforce(tb_width() > 0 && tb_height() > 2, "terminal must have at least three rows");
    // No wait deadline: the UI steps the host in its frame loop. Declared
    // before the simulator, the host closes after it.
    auto host = new FarmHost(workers);
    scope (exit) host.close();
    scope (failure) host.abandon();
    auto life = new Life(host, cast(uint) tb_width(), cast(uint) (tb_height() - 2));
    scope(exit) life.close();
    life.randomize(unpredictableSeed);
    uint x, y;
    uint speed = 10;
    bool paused;
    bool singleStep;
    bool quit;
    TbEvent[] pending;
    auto nextStep = MonoTime.currTime;
    auto nextPoll = MonoTime.currTime;
    while (!quit)
    {
        if (life.busy)
        {
            life.progress();
            if (life.busy && MonoTime.currTime < nextPoll) continue;
        }
        if (!life.busy)
        {
            // Events captured during a wave are applied only after its drain.
            foreach (event; pending)
            {
                switch (event.key)
                {
                    case TB_KEY_ARROW_UP: y = y == 0 ? life.height - 1 : y - 1; break;
                    case TB_KEY_ARROW_DOWN: y = (y + 1) % life.height; break;
                    case TB_KEY_ARROW_LEFT: x = x == 0 ? life.width - 1 : x - 1; break;
                    case TB_KEY_ARROW_RIGHT: x = (x + 1) % life.width; break;
                    case TB_KEY_ENTER:
                        life.setCell(x, y, !life.current[cast(size_t) y * life.width + x]);
                        break;
                    default: break;
                }
                switch (event.ch)
                {
                    case ' ': paused = !paused; break;
                    case 'n': paused = true; singleStep = true; break;
                    case 'r': life.randomize(unpredictableSeed); break;
                    case 'c': life.clear(); break;
                    case 'g':
                        uint[2][5] glider = [[1u, 0u], [2u, 1u], [0u, 2u], [1u, 2u], [2u, 2u]];
                        foreach (cell; glider)
                            life.setCell((x + cell[0]) % life.width,
                                (y + cell[1]) % life.height, true);
                        break;
                    case '+': case '=': speed = min(240u, speed + 1); break;
                    case '-': speed = max(1u, speed - 1); break;
                    default: break;
                }
            }
            pending.length = 0;
            render(life, x, y, paused, speed);
            if (singleStep || (!paused && MonoTime.currTime >= nextStep))
            {
                // One generation per request bounds input latency at a wave.
                life.begin(1);
                singleStep = false;
                nextStep = MonoTime.currTime + (1000 / speed).msecs;
            }
        }
        TbEvent event;
        auto result = tb_peek_event(&event, life.busy ? 0 : 20);
        nextPoll = MonoTime.currTime + 10.msecs;
        if (result == TB_ERR_NO_EVENT || (result == TB_ERR_POLL && tb_last_errno() == EINTR))
            continue;
        enforce(result == 0, "termbox event failed");
        if (event.type != TB_EVENT_KEY) continue;
        if (event.ch == 'q' || event.key == TB_KEY_CTRL_C)
        {
            life.cancel();
            quit = true;
        }
        else pending ~= event;
    }
}
