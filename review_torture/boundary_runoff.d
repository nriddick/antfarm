// Deterministic regression for the pre-d6b2eff unchecked equality boundary.
// One producer, one consumer pinned at epoch zero, exact quota-sized tables.
// The old space >= Exmax gate admitted a write ending exactly at a boundary,
// then reused the next segment's metadata without ever needing an Rt sweep.
module boundary_runoff;

import antfarm;
import core.atomic;
import core.stdc.stdio;

long unusedCallback(PayloadHeader*, PayloadBody, ulong) nothrow @nogc @system
{
    fatal("parked consumer unexpectedly invoked");
    return 0;
}

void main(string[] args)
{
    setvbuf(stdout, null, _IONBF, 0);
    auto f = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 512);
    ConsumerView held;
    immutable pulse = args.length > 1 && args[1] == "pulse";
    if (!pulse && held.subscribe(f) < 0) fatal("boundary test subscribe");
    printf("boundary runoff mode=%s\n", pulse ? "Sub0".ptr : "live-root".ptr);
    auto token = f.registerProducer(Tier.small);
    PayloadHeader header;
    header.maxCs = header.done = 1;
    header.call = &unusedCallback;
    // Cf=SqCs=1, ST singleton overhead=55 words, body=457 => Tsize=512.
    ulong[457] body;
    foreach (i; 0 .. body.length) body[i] = i;
    PayloadEntry[1] entries = [PayloadEntry(&header, body[])];
    ulong sent;
    while (sent <= f.Ln / 512)
    {
        body[0] = sent; // distinct first body word detects actual wrap overwrite
        immutable result = f.write(entries[], token);
        if (result == 0) break;
        ++sent;
        immutable wt = atomicLoad(f.Wt);
        if (wt != sent * 512) fatal("unexpected regression table geometry");
        if (wt % f.segCap == 0)
            printf("boundary Wt=%llu epoch=%llu heldRt=%llx heldEs=%lld\n",
                wt, wt / f.segCap, atomicLoad(f.Rt[0][0]), atomicLoad(f.stats[0].es));
        if (wt >= f.Ln)
        {
            version (ObserveUncheckedOverwrite)
            {
                // Negative control only: compile the frozen old implementation
                // without allWriteCheck, and observe the next table replacing
                // the pinned epoch's sentinel and body before aborting.
                if (wt > f.Ln)
                {
                    immutable poff = atomicLoad(f.buf[8]);
                    immutable body0 = atomicLoad(f.buf[poff + 16]);
                    printf("old overwrite: heldRt=%llx heldEs=%lld sentinel=%llu expected=%llu body0=%llu expectedBody0=0\n",
                        atomicLoad(f.Rt[0][0]), atomicLoad(f.stats[0].es),
                        atomicLoad(f.buf[0]), sentinelOf(0), body0);
                    if (atomicLoad(f.buf[0]) == sentinelOf(0) || body0 == 0)
                        fatal("old overwrite oracle did not observe corruption");
                    fatal("old producer overwrote a pinned consumer's sentinel and body");
                }
            }
            else
                fatal("producer reached pinned epoch zero's next lap");
        }
    }
    immutable stop = atomicLoad(f.Wt);
    foreach (i; 0 .. 10_000)
        if (f.write(entries[], token) != 0 || atomicLoad(f.Wt) != stop)
            fatal("boundary retry renewed into a held epoch");
    if (stop >= f.Ln || atomicLoad(f.stats[0].es) != 0 || atomicLoad(f.Rt[0][0]) == 0)
        fatal("held epoch zero lost protection");
    printf("boundary runoff PASS sent=%llu Wt=%llu rejected=10000 heldEs=0 largePages=%d\n",
        sent, stop, f.usedLargePages);
    held.unsubscribe();
    f.unregisterProducer(token);
    f.destroy();
}
