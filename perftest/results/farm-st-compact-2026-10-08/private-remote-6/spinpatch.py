#!/usr/bin/env python3
"""DIAGNOSTIC ONLY (never timed for the gate): rdtsc spin controls.
  ANTFARM_DIAG_WRITER_TICKS=t    spin t ticks per entry after a compact
                                 table's records are emitted (candidate only)
  ANTFARM_DIAG_CONSUMER_TICKS=t  spin t ticks after each ST callback in a
                                 claimed run (compact runCompact or the
                                 full-layout enterPayload loop)
Both default to 0; the spin code is then inert but present, so ticks=0 is
also a code-layout control.  usage: spinpatch.py SRCDIR"""
import sys
p = sys.argv[1] + '/antfarm.d'
s = open(p).read()
compact = 'void runCompact(' in s

TSC = ('static ulong diagTsc() nothrow @nogc { uint lo, hi; asm nothrow @nogc '
       '{ rdtsc; mov lo, EAX; mov hi, EDX; } return (cast(ulong) hi << 32) | lo; }\n')

def sub(old, new):
    global s
    assert s.count(old) == 1, old
    s = s.replace(old, new)

sub('struct ConsumerView\n{', '''public __gshared ulong diagWriterTicks, diagConsumerTicks; // DIAG
''' + TSC + '''shared static this()
{
    import core.stdc.stdlib : getenv, strtoul;
    if (auto v = getenv("ANTFARM_DIAG_WRITER_TICKS")) diagWriterTicks = strtoul(v, null, 10);
    if (auto v = getenv("ANTFARM_DIAG_CONSUMER_TICKS")) diagConsumerTicks = strtoul(v, null, 10);
}
struct ConsumerView
{''')

SPIN = '''if (diagConsumerTicks)
            {
                immutable until = diagTsc() + diagConsumerTicks;
                while (diagTsc() < until) {}
            }
'''
if compact:
    sub('''                po += PCOMPACT_LEN + pe.body.length;
                ++i;
                emit.popFront();
            }
''', '''                po += PCOMPACT_LEN + pe.body.length;
                ++i;
                emit.popFront();
            }
            if (diagWriterTicks)
            {
                immutable until = diagTsc() + diagWriterTicks * n;
                while (diagTsc() < until) {}
            }
''')
    sub('''            head.call(&head, body_, 0);
''', '''            head.call(&head, body_, 0);
            ''' + SPIN)
else:
    sub('''                enterPayload(bp, tseqIdx + poff, false);
            }''', '''                enterPayload(bp, tseqIdx + poff, false);
                ''' + SPIN.replace('\n            ', '\n                ') + '''            }''')
open(p, 'w').write(s)
print('patched', p, 'compact' if compact else 'full')
