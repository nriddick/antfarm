#!/usr/bin/env bash
# Reproduces the lost SQPOLL wakeup in libuv 1.53.0 and shows the fence
# removes it: builds libuv 1.53.0 from its release archive twice, as released and
# with the patch in this directory applied, then runs COPIES copies of
# sqpoll_lost_wakeup.c against each for ITERATIONS iterations.
#
#   docs/development/libuv-sqpoll-wakeup-2026-11-04/run.sh [work-dir]
#
# Needs Linux 5.10.186 or later, a C compiler, CMake, and the 1.53.0 release
# archive, downloaded into the work directory when it is missing and checked
# against its SHA-256. COPIES (8) and ITERATIONS (20000) scale the run; at 9
# to 12 ms per iteration a full run is about four minutes per build.
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
W=${1:-$(mktemp -d "${TMPDIR:-/tmp}/libuv-sqpoll-wakeup.XXXXXX")}
COPIES=${COPIES:-8}
ITERATIONS=${ITERATIONS:-20000}
mkdir -p "$W"
W=$(cd "$W" && pwd)
cd "$W"

# The 1.53.0 release archive, as the record's measurements used it.
ARCHIVE=$W/libuv-v1.53.0.tar.gz
[ -f "$ARCHIVE" ] || curl -fsSL -o "$ARCHIVE" https://dist.libuv.org/dist/v1.53.0/libuv-v1.53.0.tar.gz
echo "cb0d6dd2128d5a95bd242c6cc982a24fe608fa93da57b6b4ec763b0018c53e64  $ARCHIVE" | sha256sum -c
for v in released fenced; do
  rm -rf "$v"; mkdir "$v"
  tar -xzf "$ARCHIVE" -C "$v" --strip-components=1
  if [ "$v" = fenced ]; then (cd "$v" && patch -p1 < "$HERE/libuv-1.53.0-sqpoll-wakeup-fence.patch")
  fi
  cmake -S "$v" -B "$v/build" -DCMAKE_BUILD_TYPE=Release -DLIBUV_BUILD_SHARED=ON \
    -DLIBUV_BUILD_TESTS=OFF -DLIBUV_BUILD_BENCH=OFF -DBUILD_TESTING=OFF > "$v/cmake.log"
  cmake --build "$v/build" --target uv --parallel > "$v/build.log"
  cc -O1 -Wall -I "$v/include" "$HERE/sqpoll_lost_wakeup.c" -L "$v/build" -luv -o "repro-$v"
done
{
  echo "host: $(uname -r); $(cc --version | head -1); $COPIES copies, $ITERATIONS iterations each"
  for v in released fenced; do
    echo "=== libuv 1.53.0 $v"
    for i in $(seq "$COPIES"); do
      LD_LIBRARY_PATH=$W/$v/build nice -n 10 "./repro-$v" "$ITERATIONS" 9000 12000 > "$v-copy-$i.log" 2>&1 &
    done
    wait
    for i in $(seq "$COPIES"); do
      echo "copy $i: $(grep -E 'lost wakeup|every stat completed|still incomplete|no io_uring|not accepted' "$v-copy-$i.log" | tail -1)"
    done
  done
  echo "done after ${SECONDS} s"
} | tee results.txt
echo "outputs in $W"
