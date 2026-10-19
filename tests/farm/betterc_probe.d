// Native C linkage/lifetime probe; not a public export.
module betterc_probe;
import antfarm;
extern(C) int antfarm_test_c(Callback call, TableCompletionHook* hook) nothrow @nogc @system
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 256, DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    if (view.subscribe(farm) < 0) return 1;
    scope(exit) view.unsubscribe();
    ulong[3] words = [2, 3, 5];
    PayloadBody[3] bodies = [words[], words[0 .. 1], words[0 .. 0]];
    PayloadEntry[3] entries;
    foreach (i; 0 .. entries.length) entries[i] = PayloadEntry(call, bodies[i]);
    if (farm.write(entries[], token, 7, hook) != 3) return 2;
    while (view.consumeNext()) {}
    if (farm.write(call, bodies[], token, 7, hook) != 3) return 3;
    while (view.consumeNext()) {}
    PayloadBody[3] fixed = [words[0 .. 1], words[0 .. 1], words[0 .. 1]];
    if (farm.write(call, fixed[], 1, token, 7, hook) != 3) return 4;
    while (view.consumeNext()) {}
    return 0;
}
