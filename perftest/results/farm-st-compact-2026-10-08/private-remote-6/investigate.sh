#!/usr/bin/env bash
# private-remote/6 investigation (2026-10-08). Diagnostic builds and runs,
# serial on a quiet host; none of these numbers are gate timings.
#   base  cf2aa91 (local main: Wt reservation fix)
#   cand  5e22442 (compact ST runtime; antfarm.d/actors identical at HEAD)
# Both use 2f853e5's perftest/actor_churn.d, as the gate does.
# usage: investigate.sh [step...]   steps: build stat cas diag quota matched writer consumer
# MIMALLOC_SRC: pinned v3.5.3 source tree (the worktree's submodule is not
# checked out, so the main checkout's copy is the default).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../../.." && pwd)
WORK=${WORK:-/tmp/pr6-investigate}
MIMALLOC_SRC=${MIMALLOC_SRC:-/home/nathan-riddick/pi_ground/antfarm/third_party/mimalloc}
MI=$REPO/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a
BENCH_REF=2f853e5
CPUS=0,1,2,3,4,5,6
steps=${*:-build stat cas diag quota matched writer consumer}

waitquiet() { while ! python3 -c "import os,sys; sys.exit(0 if os.getloadavg()[0] < 0.8 else 1)"; do sleep 20; done; }

build() { # build SRCDIR OUT [ARCHIVE]
    (cd "$1" && ldc2 -O2 -release -d-version=AntfarmMimallocV3 -i -I. -Ithreadpool/source \
        perftest/actor_churn.d antfarm.d antfarm_allocation.d actors/package.d actors/actor.d \
        actors/wave.d actors/private_actor.d actors/mimalloc.d "${3:-$MI}" \
        -L-lpthread -L-lrt -L-latomic -of="$2")
}

tree() { # tree NAME REF [patch...]
    local d=$WORK/src-$1; rm -rf "$d"; mkdir -p "$d"
    git -C "$REPO" archive "$2" | tar -x -C "$d"
    git -C "$REPO" show $BENCH_REF:perftest/actor_churn.d > "$d/perftest/actor_churn.d"
    shift 2; for p in "$@"; do python3 "$HERE/$p" "$d" > /dev/null; done
}

churn() { # churn BIN QUOTA [env...]  (private-remote/6, gate geometry otherwise)
    local bin=$1 q=$2; shift 2
    env ANTFARM_HUGE_PAGES=0 "$@" "$WORK/bin/$bin" private-remote 16384 3 6 256 5 mimalloc $CPUS small "$q"
}
rate() { grep -o 'Mactor_cycles/s=[0-9.]*' | cut -d= -f2; }

if [[ " $steps " == *" build "* ]]; then
    mkdir -p "$WORK/bin" "$WORK/mi-diag"
    # Counting-only mimalloc: plain cc build of static.c with the diag patch.
    rm -rf "$WORK/mimalloc"; cp -r "$MIMALLOC_SRC" "$WORK/mimalloc"
    patch -d "$WORK/mimalloc" -p1 < "$HERE/mimalloc-diag.patch"
    cc -O3 -DNDEBUG -DMI_STATIC_LIB -I"$WORK/mimalloc/include" -fvisibility=hidden \
        -ftls-model=initial-exec -fno-builtin-malloc -w -c "$WORK/mimalloc/src/static.c" -o "$WORK/mi-diag/static.o"
    ar rcs "$WORK/mi-diag/libmimalloc.a" "$WORK/mi-diag/static.o"
    for side in base:cf2aa91 cand:5e22442; do
        n=${side%%:*} r=${side##*:}
        tree $n $r; build "$WORK/src-$n" "$WORK/bin/$n"
        build "$WORK/src-$n" "$WORK/bin/$n-midiag" "$WORK/mi-diag/libmimalloc.a"
        tree $n-diag $r diagpatch.py; build "$WORK/src-$n-diag" "$WORK/bin/$n-diag"
        tree $n-spin $r spinpatch.py; build "$WORK/src-$n-spin" "$WORK/bin/$n-spin"
        build "$WORK/src-$n-spin" "$WORK/bin/$n-spin-midiag" "$WORK/mi-diag/libmimalloc.a"
    done
fi

# Configurations: gate quota 16384 words (3.5 full-layout or 20.3 compact
# 256-entry tables), and the "matched" quotas that give the other layout the
# same tables per quota (16384 * 806/4646 = 2842; 16384 * 4646/806 = 94440).
CONFIGS=("base 16384" "cand 16384" "cand 2842" "base 94440")

if [[ " $steps " == *" stat "* ]]; then
    waitquiet; out=$HERE/stat.txt; : > "$out"
    for rep in 1 2 3; do for c in "${CONFIGS[@]}"; do read -r b q <<<"$c"
        o=$(ANTFARM_HUGE_PAGES=0 perf stat -x, -e cycles:u,instructions:u,r0243:u,r0860:u -o "$WORK/st.csv" -- \
            "$WORK/bin/$b" private-remote 16384 3 6 256 5 mimalloc $CPUS small "$q")
        python3 - "$b" "$q" "$o" "$WORK/st.csv" >> "$out" <<'PY'
import sys,re,csv
b,q,o,f=sys.argv[1:5]
u=int(re.search(r'verified_actor_cycles=(\d+)',o)[1]); r=re.search(r'Mactor_cycles/s=([\d.]+)',o)[1]
v={row[2]:float(row[0]) for row in csv.reader(open(f)) if len(row)>3 and row[0].replace('.','').isdigit()}
print(f"{b} q={q} rate={r} cycles/u={v['cycles:u']/u:.1f} instr/u={v['instructions:u']/u:.1f} ipc={v['instructions:u']/v['cycles:u']:.3f} r0243/u={v['r0243:u']/u:.3f} r0860/u={v['r0860:u']/u:.3f}")
PY
    done; done; cat "$out"
fi

midiag() { # midiag LABEL BIN QUOTA [env...]
    local label=$1 bin=$2 q=$3; shift 3
    local o; o=$(churn "$bin" "$q" "$@")
    python3 - "$label" "$q" "$o" <<'PY'
import sys,re
l,q,o=sys.argv[1:4]
u=int(re.search(r'verified_actor_cycles=(\d+)',o)[1]); r=re.search(r'Mactor_cycles/s=([\d.]+)',o)[1]
m={k:int(v) for k,v in re.findall(r'(mt_frees|cas_attempts|newly_owned|empty_pushes)=(\d+)',o)}
print(f"{l} q={q} rate={r} mt_frees/u={m['mt_frees']/u:.3f} cas_attempts/free={m['cas_attempts']/m['mt_frees']:.3f} newly_owned/u={m['newly_owned']/u:.4f}")
PY
}

if [[ " $steps " == *" cas "* ]]; then
    waitquiet; out=$HERE/cas.txt; : > "$out"
    for rep in 1 2 3; do for c in "${CONFIGS[@]}"; do read -r b q <<<"$c"
        midiag "$b" "$b-midiag" "$q" >> "$out"
    done; done; cat "$out"
fi

if [[ " $steps " == *" diag "* ]]; then
    waitquiet; out=$HERE/diag.txt; : > "$out"
    for c in "${CONFIGS[@]}" "base 14000"; do read -r b q <<<"$c"
        echo "== $b q=$q" >> "$out"
        churn "$b-diag" "$q" | grep -E 'diag_|Mactor' | sort >> "$out"
    done; cat "$out"
fi

if [[ " $steps " == *" quota "* ]]; then
    waitquiet; out=$HERE/quota.txt; : > "$out"
    for rep in 1 2 3; do
        for q in 9400 14000 16384 18584 28000 47000 94440; do echo "base q=$q $(churn base $q | rate)" >> "$out"; done
        for q in 1700 2418 2842 3224 4100 8100 12000 16384; do echo "cand q=$q $(churn cand $q | rate)" >> "$out"; done
    done; sort -s -k1,1 -k2,2V "$out"
fi

if [[ " $steps " == *" matched "* ]]; then
    waitquiet; out=$HERE/matched.txt; : > "$out"
    for rep in $(seq 10); do for c in "${CONFIGS[@]}"; do read -r b q <<<"$c"
        echo "$rep $b q=$q $(churn "$b" "$q" | rate)" >> "$out"
    done; done; cat "$out"
fi

if [[ " $steps " == *" writer "* ]]; then
    waitquiet; out=$HERE/writer-slowdown.txt; : > "$out"
    for rep in 1 2 3; do
        echo "base ticks=- $(churn base 16384 | rate)" >> "$out"
        for t in 0 8 16 32 64; do echo "cand-spin writer_ticks=$t $(churn cand-spin 16384 ANTFARM_DIAG_WRITER_TICKS=$t | rate)" >> "$out"; done
    done; cat "$out"
fi

if [[ " $steps " == *" consumer "* ]]; then
    waitquiet; out=$HERE/consumer-delay.txt; : > "$out"
    for rep in 1 2; do for b in base cand; do for t in 0 10 20 40 80; do
        echo "$b-spin consumer_ticks=$t $(churn $b-spin 16384 ANTFARM_DIAG_CONSUMER_TICKS=$t | rate)" >> "$out"
    done; done; done
    for b in base cand; do for t in 0 10 40; do
        midiag "$b-spin-midiag consumer_ticks=$t" "$b-spin-midiag" 16384 ANTFARM_DIAG_CONSUMER_TICKS=$t >> "$out"
    done; done; cat "$out"
fi
