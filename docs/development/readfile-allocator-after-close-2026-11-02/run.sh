#!/usr/bin/env bash
# Reproduces the report in this directory. Builds the FUSE stub, the probe
# against this checkout's ant_uv, and the probe again against a copy of
# loop.d with a stop check before the allocation; runs the stall cases under
# the stub, then the zero-abandonAfter sweep on local files.
#
#   docs/development/readfile-allocator-after-close-2026-11-02/run.sh [work-dir]
#
# Needs Linux, LDC, libuv (found by pkg-config), libfuse 3 with fusermount3,
# and a C compiler. Writes stall.txt, zero.txt and spans.txt to the work
# directory, a new one under $TMPDIR (or /tmp) unless given. REPS (3) sets the
# runs of each stall case and RUNS (30) the processes per sweep line.
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
REPS=${REPS:-3}
RUNS=${RUNS:-30}
W=${1:-$(mktemp -d "${TMPDIR:-/tmp}/antuv-alloc-after-close.XXXXXX")}
mkdir -p "$W/mnt" "$W/check"
W=$(cd "$W" && pwd)
cd "$W"

{
  echo "tree: $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet HEAD -- ant_uv antfarm || echo ' (ant_uv or antfarm modified)')"
  echo "host: $(uname -r); $(ldc2 --version | head -1); libuv $(pkg-config --modversion libuv); fuse3 $(pkg-config --modversion fuse3); $(ldd --version | head -1)"
  echo "work directory: $(df --output=fstype "$W" | tail -1)"
} > env.txt

DFLAGS=(-g -O1 -i -I"$ROOT" -I"$ROOT/threadpool/source" -I"$ROOT/ant_fibers/source")
# shellcheck disable=SC2046 # pkg-config's flags are words
cc -O1 -Wall -o slowfs "$HERE/slowfs.c" $(pkg-config --cflags --libs fuse3)
ldc2 "${DFLAGS[@]}" -I"$ROOT/ant_uv/source" "$HERE/probe.d" -L-luv -of=probe -od=obj
# The check under test: stop before allocating, as the job does before each read.
rm -rf check/source
cp -r "$ROOT/ant_uv/source" check/
sed -i '/UV_S_IFDIR) return closeFile(UV_EISDIR);/a\        if (stopped()) return closeFile(UV_ECANCELED); // the check under test' \
  check/source/ant_uv/loop.d
[ "$(grep -c 'the check under test' check/source/ant_uv/loop.d)" = 1 ] \
  || { echo "could not place the check in loop.d"; exit 1; }
ldc2 "${DFLAGS[@]}" -I"$W/check/source" "$HERE/probe.d" -L-luv -of=probe-check -od=obj-check
head -c 16384 /dev/urandom > local16k.bin
head -c 8388608 /dev/urandom > local8m.bin

# The stall cases, under a FUSE file system that stalls open or fstat 1.5 s.
LOG="$W/slowfs.log"
: > "$LOG"
SLOWFS_DELAY_MS=1500 ./slowfs -f -o fsname=slowfs,auto_unmount "$W/mnt" 2>> "$LOG" &
FS=$!
# Unmounting ends the daemon, so the kill and wait may find nothing.
trap 'fusermount3 -u "$W/mnt" 2>/dev/null || true; kill "$FS" 2>/dev/null || true; wait "$FS" 2>/dev/null || true' EXIT
for _ in $(seq 100); do [ -e "$W/mnt/fast.bin" ] && break; sleep 0.05; done
[ -e "$W/mnt/fast.bin" ] || { echo "the FUSE mount failed"; cat "$LOG"; exit 1; }

stall() {
  local before
  before=$(wc -l < "$LOG")
  echo "--- $*"
  nice -n 10 "./$1" stall "${@:2}"
  tail -n +"$((before + 1))" "$LOG" | sed 's/^/    /'
}
{
  cat env.txt
  for _ in $(seq "$REPS"); do
    for bin in probe probe-check; do
      stall "$bin" "$W/mnt/slow-fstat.bin"
      stall "$bin" "$W/mnt/slow-open.bin"
      stall "$bin" "$W/local16k.bin" blocking
    done
  done
} | tee stall.txt

# abandonAfter zero: 64 local reads, closed after a delay; RUNS runs per line.
{
  cat env.txt
  for bin in probe probe-check; do
    for f in local16k.bin local8m.bin; do
      for delay in 0 20 50 100 300 1000; do
        for _ in $(seq "$RUNS"); do nice -n 10 "./$bin" zero "$W/$f" 64 "$delay"; done \
          | awk -v b="$bin" -v f="$f" -v d="$delay" '
            { for (i = 2; i <= NF; i++) { split($i, kv, "="); v[kv[1]] = kv[2] + 0 }
              for (k in v) if (k != "delay_us" && k != "close_us") s[k] += v[k]
              if (n == 0 || v["close_us"] < lo) lo = v["close_us"]
              if (v["close_us"] > hi) hi = v["close_us"]
              n++ }
            END { printf "%-11s %-12s delay %4d us, %d runs: close took %.0f-%.0f us; abandoned %d, allocate %d, started after close returned %d, running when it returned %d, deallocate %d\n",
                    b, f, d, n, lo, hi, s["abandoned"], s["allocate"], s["started_after_close"],
                    s["running_at_close"], s["deallocate"] }'
      done
    done
  done
} | tee zero.txt

# The calls still running when close returned, one run at a time.
{
  cat env.txt
  for f in local16k.bin local8m.bin; do
    for _ in $(seq "$REPS"); do echo "--- $f"; nice -n 10 ./probe zero "$W/$f" 64 0 spans; done
  done
} | tee spans.txt
echo "outputs in $W after ${SECONDS} s"
