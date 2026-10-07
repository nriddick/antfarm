#!/usr/bin/env bash
# Gate (a) precondition: code that never instantiates cohortSlab must compile
# to the base's code layout. Builds base c3e7521 and candidate RF2 runtime with
# the pre-slab benchmark 8143463 using perf_gate.py's exact command, then
# compares .text size, the hot Farm symbols' addresses/alignment and a
# normalized disassembly. Usage: text-identity.sh CANDIDATE_SHA > text-identity.txt
set -euo pipefail
candidate=$1
repo=$(cd "$(dirname "$0")/../../.." && pwd)
archive=$repo/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a
out=$(mktemp -d /tmp/rf2-textid-XXXXXX)
for pair in "base c3e7521" "candidate $candidate"; do
    set -- $pair
    w=$out/$1-src
    git -C "$repo" worktree add -q --detach "$w" "$2"
    git -C "$repo" show 8143463:perftest/actor_churn.d > "$w/perftest/actor_churn.d"
    (cd "$w" && ldc2 -O2 -release -d-version=AntfarmMimallocV3 -i -I. -Ithreadpool/source \
        perftest/actor_churn.d antfarm.d antfarm_allocation.d actors/package.d actors/actor.d \
        actors/wave.d actors/private_actor.d actors/mimalloc.d "$archive" \
        -L-lpthread -L-lrt -L-latomic -of="$out/$1-churn")
    git -C "$repo" worktree remove --force "$w"
done
echo "base c3e7521 vs candidate $candidate, benchmark 8143463 (no cohortSlab instantiation)"
echo "ldc2: $(ldc2 --version | head -1)"
for v in base candidate; do
    echo "== $v sha256 $(sha256sum "$out/$v-churn" | cut -c1-64)"
    size -A "$out/$v-churn" | grep -E '^\.text|^\.rodata'
    nm -S --defined-only "$out/$v-churn" \
        | grep -E 'writeImplTSQBf__T21PayloadBroadcastRangeTS6actors13private_actor11ChildBodies|ConsumerView12processShard|runChildS_D11actor_churn13subsetOperate' \
        | while read -r a s t n; do printf '%s size=%d mod64=%d %.60s\n' "$a" $((16#$s)) $((16#$a % 64)) "$n"; done
done
for v in base candidate; do
    objdump -d --no-show-raw-insn -j .text "$out/$v-churn" | tail -n +3 \
        | sed -E 's/^ *[0-9a-f]+:\s*//; s/0x[0-9a-f]+/HEX/g; s/# [0-9a-f]+ <[^>]*>/# TARGET/; s/[0-9a-f]{5,}/ADDR/g; s/<[^>]*>/<SYM>/g' > "$out/$v.dis"
done
echo "normalized .text disassembly lines: $(wc -l < "$out/base.dis") base, $(wc -l < "$out/candidate.dis") candidate"
echo "differing normalized instruction lines: $(diff "$out/base.dis" "$out/candidate.dis" | grep -c '^[<>]' || true)"
echo "(addresses, immediates, symbol names and rip-relative targets normalized; .rodata differs only by longer TypeInfo names placed after .text)"
