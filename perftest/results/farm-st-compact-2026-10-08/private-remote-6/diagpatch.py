#!/usr/bin/env python3
"""DIAGNOSTIC ONLY (never timed for the gate): per-consumer claim/entry/idle
counters and per-table emitter counters for private-remote. Applied
identically to the base and candidate trees.  usage: diagpatch.py SRCDIR"""
import sys
d = sys.argv[1]

def sub(path, old, new, count=1):
    s = open(path).read()
    assert s.count(old) >= 1, (path, old)
    s = s.replace(old, new, count)
    open(path, 'w').write(s)

# antfarm.d: thread-local claim counters at the shard claim site.
sub(f'{d}/antfarm.d', 'struct ConsumerView\n{',
    'public ulong diagClaims, diagClaimEntries; // DIAG thread-local\n'
    'struct ConsumerView\n{')
sub(f'{d}/antfarm.d',
    '            immutable runlen = chunk < shlen - x * chunk ? chunk : shlen - x * chunk;\n',
    '            immutable runlen = chunk < shlen - x * chunk ? chunk : shlen - x * chunk;\n'
    '            ++diagClaims; diagClaimEntries += runlen;\n')

# private_actor.d: per-table counters on the emitter, plus in-flight batches.
p = f'{d}/actors/private_actor.d'
sub(p, 'module actors.private_actor;\n',
    'module actors.private_actor;\n'
    'public __gshared ulong diagTables, diagTableEntries, diagPumps, diagNoSpare, diagInflightSum, diagShort;\n')
sub(p, '''        service_.pump(token);
        if (spare_ is null || ready_ is null) return 0;
        auto batch = spare_;''', '''        service_.pump(token);
        ++diagPumps;
        if (spare_ is null && ready_ !is null) ++diagNoSpare;
        if (spare_ is null || ready_ is null) return 0;
        { size_t sp; for (auto b = spare_; b !is null; b = b.next) ++sp; diagInflightSum += batchCount_ - sp; }
        auto batch = spare_;''')
sub(p, '''            &batch.hook, token, avgCost);
        foreach (entry; batch.entries[n .. count])''', '''            &batch.hook, token, avgCost);
        if (n) { ++diagTables; diagTableEntries += n; if (n < count) ++diagShort; }
        foreach (entry; batch.entries[n .. count])''')

# actor_churn.d: per-consumer report at thread exit (idle polls/ticks).
c = f'{d}/perftest/actor_churn.d'
sub(c, '''        while (atomicLoad!(MemoryOrder.acq)(stop) == 0)
            if (!view.consumeNext()) pause();
        while (view.consumeNext()) {}
    }''', '''        import antfarm : diagClaims, diagClaimEntries;
        import core.stdc.stdio : printf;
        ulong idle, busyT, idleT, polls;
        static ulong tsc() nothrow @nogc { uint lo, hi; asm nothrow @nogc { rdtsc; mov lo, EAX; mov hi, EDX; } return (cast(ulong) hi << 32) | lo; }
        auto t0 = tsc();
        while (atomicLoad!(MemoryOrder.acq)(stop) == 0)
        {
            immutable a = tsc();
            immutable got = view.consumeNext();
            immutable b = tsc();
            ++polls;
            if (got) busyT += b - a; else { ++idle; pause(); idleT += tsc() - a; }
        }
        while (view.consumeNext()) {}
        printf("diag_consumer cpu=%d claims=%llu entries=%llu entries_per_claim=%.2f polls=%llu idle_polls=%llu busy_frac=%.4f\\n",
            cpu, diagClaims, diagClaimEntries, diagClaims ? cast(double) diagClaimEntries / diagClaims : 0.0,
            polls, idle, cast(double) busyT / (tsc() - t0));
    }''')
sub(c, '''    immutable actorCycles = cast(double) count * rounds;
    writefln("mode=%s actors=%s consumers=%s batch=%s warmups=%s state=%s stateAlign=%s allocator=%s ringMiB=8 hugePages=%s cpus=%s",''',
    '''    immutable actorCycles = cast(double) count * rounds;
    {
        import actors.private_actor : diagTables, diagTableEntries, diagPumps, diagNoSpare, diagInflightSum, diagShort;
        writefln("diag_emitter tables=%s entries_per_table=%.2f short_tables=%s pumps=%s pumps_no_spare=%s mean_inflight_batches=%.2f tables_per_cycle=%.2f cycles_total=%s",
            diagTables, cast(double) diagTableEntries / diagTables, diagShort, diagPumps, diagNoSpare,
            cast(double) diagInflightSum / diagTables, cast(double) diagTables / cycle, cycle);
    }
    writefln("mode=%s actors=%s consumers=%s batch=%s warmups=%s state=%s stateAlign=%s allocator=%s ringMiB=8 hugePages=%s cpus=%s",''')
print('patched', d)
