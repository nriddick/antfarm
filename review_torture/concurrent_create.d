module concurrent_create;

import antfarm;
import core.atomic;
import core.thread;
import core.stdc.stdio : puts;

__gshared shared(uint) ready, go;

void construct()
{
    atomicFetchAdd(ready, 1u);
    while (!atomicLoad(go)) Thread.yield();
    foreach (_; 0 .. 100)
    {
        auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 256);
        auto memory = cast(ulong*) farm.buf;
        auto words = farm.bufBytes / ulong.sizeof;
        memory[0] = 0x123456789ABCDEF0UL;
        assert(memory[words] == memory[0]);
        memory[2 * words - 1] = 0xFEDCBA9876543210UL;
        assert(memory[words - 1] == memory[2 * words - 1]);
        farm.destroy();
    }
}

// Standalone so the first mapping resolver call occurs inside the race.
void main()
{
    Thread[8] threads;
    foreach (ref t; threads) { t = new Thread(&construct); t.start(); }
    while (atomicLoad(ready) != threads.length) Thread.yield();
    atomicStore(go, 1u);
    foreach (t; threads) t.join();
    puts("800 concurrent creates: both mapping aliases verified");
}
