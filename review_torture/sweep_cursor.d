/++
 + Oldest-epoch re-walk cursor (SPEC.md section 8, "Deadlock freedom and the
 + idle re-walk"). Consumer A parks inside the primary call of an MT payload
 + in the last table of epoch E, so E cannot confirm. Consumer B has drained
 + every other table of E, moved its position to E + 1 and idles at the
 + frontier with E held as oldest. Each idle poll of B re-walks E; with the
 + cursor, polls after the first resume at the parked table instead of
 + re-walking every table of E.
 +
 +   make -C review_torture run
 +/
module sweep_cursor;

import antfarm.templates;
import torture_common;
import core.atomic;
import core.thread;
import core.time;
import core.stdc.stdio;

version (AntfarmSweepStats) {} else
    static assert(false, "build with AntfarmSweepStats");

enum ulong ST_TAG = 0;
enum ulong MT_TAG = 1;
enum size_t MT_BODY = 1500;
enum POLLS = 16;

bool isParker; // thread-local: set only on consumer A's thread
__gshared shared(int) parked, release, stopA;
__gshared shared(long) mtCalls, stCalls;

long sweepCb(PayloadHeader*, PayloadBody body, ulong) nothrow @nogc @system
{
    if (body[0] == MT_TAG)
    {
        if (isParker)
        {
            atomicStore!(MemoryOrder.rel)(parked, 1);
            while (atomicLoad!(MemoryOrder.acq)(release) == 0)
                Thread.yield();
        }
        atomicFetchAdd!(MemoryOrder.raw)(mtCalls, 1L);
    }
    else
        atomicFetchAdd!(MemoryOrder.raw)(stCalls, 1L);
    return 1;
}

void waitFor(shared(int)* flag, const(char)[] what)
{
    immutable deadline = MonoTime.currTime + 30.seconds;
    while (atomicLoad!(MemoryOrder.acq)(*flag) == 0)
    {
        check(MonoTime.currTime < deadline, what);
        Thread.yield();
    }
}

ulong writeOne(AntFarm* f, ref Token tok, PayloadEntry* e)
{
    immutable start = atomicLoad!(MemoryOrder.raw)(f.Wt);
    immutable deadline = MonoTime.currTime + 30.seconds;
    while (f.write(e[0 .. 1], tok) != 1)
    {
        check(MonoTime.currTime < deadline, "write stalled");
        Thread.yield();
    }
    return start;
}

void drain(ref ConsumerView v)
{
    while (v.consumeNext()) {}
}

void main()
{
    auto f = AntFarm.create(2, 8, 2);
    auto tok = f.registerProducer(Tier.bulk);
    check(tok.valid, "reg");

    PayloadHeader stHead, mtHead;
    stHead.maxCs = 1;
    stHead.done = 1;
    stHead.call = &sweepCb;
    mtHead.maxCs = 2;
    mtHead.done = 2;
    mtHead.call = &sweepCb;
    ulong[2] stBody = [ST_TAG, 0];
    ulong[MT_BODY] mtBody;
    mtBody[0] = MT_TAG;
    auto st = PayloadEntry(&stHead, stBody[]);
    auto mt = PayloadEntry(&mtHead, mtBody[]);

    ConsumerView b;
    immutable e = b.subscribe(f);
    check(e >= 0, "B subscribe");
    immutable boundary = (cast(ulong) e + 1) << f.segShift;

    shared int aReady;
    auto a = new Thread({
        isParker = true;
        ConsumerView v;
        check(v.subscribe(f) == e, "A subscribe");
        atomicStore!(MemoryOrder.rel)(aReady, 1);
        while (atomicLoad!(MemoryOrder.acq)(stopA) == 0)
            if (!v.consumeNext())
                Thread.yield();
        v.unsubscribe();
    });
    a.start();
    waitFor(&aReady, "A subscribe timeout");

    // Fill E with small ST tables, then one MT table that starts in E and
    // runs past its boundary, so it is the last table of E.
    long nst;
    while (boundary - atomicLoad!(MemoryOrder.raw)(f.Wt) > MT_BODY / 2)
    {
        writeOne(f, tok, &st);
        ++nst;
        drain(b);
    }
    immutable tablesInE = nst + 1;
    // A takes the MT table's lone shard-0 run, on its primary path or, if
    // its shard index skips the small table, in its position re-walk.
    immutable mtSeq = writeOne(f, tok, &mt);
    check(mtSeq < boundary && atomicLoad!(MemoryOrder.raw)(f.Wt) > boundary,
        "MT table straddles the boundary of E");
    waitFor(&parked, "A park timeout");

    // B passes the MT table (running its other iteration) into E + 1.
    foreach (_; 0 .. 4)
    {
        writeOne(f, tok, &st);
        ++nst;
    }
    // B's re-walks of E start here: until now B's oldest was its position.
    ulong[POLLS] perPoll;
    ulong before = b.oldSweepTables;
    drain(b); // ends with B's first idle poll
    check(b.oldestEi == e && b.newestEi == e + 1, "B holds E as oldest");
    perPoll[0] = b.oldSweepTables - before;
    foreach (i; 1 .. POLLS)
    {
        before = b.oldSweepTables;
        check(!b.consumeNext(), "B idles at the frontier");
        perPoll[i] = b.oldSweepTables - before;
    }
    printf("T27 tables in E=%lld; tables re-walked per idle poll:", tablesInE);
    foreach (n; perPoll)
        printf(" %llu", n);
    printf("\n");
    fflush(stdout);
    check(b.oldestEi == e, "E still held while A is parked");
    check(perPoll[0] >= 100, "first poll walks the segment");
    foreach (n; perPoll[1 .. $])
        check(n <= 1, "later idle polls resume at the parked table");

    atomicStore!(MemoryOrder.rel)(release, 1);
    immutable deadline = MonoTime.currTime + 30.seconds;
    while (b.oldestEi == e)
    {
        check(MonoTime.currTime < deadline, "E released after A returns");
        b.consumeNext();
        Thread.yield();
    }
    atomicStore!(MemoryOrder.rel)(stopA, 1);
    a.join();
    b.unsubscribe();
    check(atomicLoad(stCalls) == nst, "ST calls exact");
    check(atomicLoad(mtCalls) == 2, "MT calls exact");
    f.unregisterProducer(tok);
    f.destroy();
    say("T27 oldest-epoch re-walk cursor OK");
}
