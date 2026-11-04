#!/usr/bin/env bash
# Reproduces the lost SQPOLL wakeup in libuv 1.53.0 and shows the fence
# removes it: builds libuv from the pinned archive twice, as released and
# with the patch in this directory applied, then runs COPIES copies of
# sqpoll_lost_wakeup.c against each for ITERATIONS iterations.
#
#   docs/development/libuv-sqpoll-wakeup-2026-11-04/run.sh [work-dir]
#
# Needs Linux 5.10.186 or later, a C compiler, CMake, and the archive, which
# scripts/build-libuv.py downloads into .build/libuv/ when it is missing.
# COPIES (8) and ITERATIONS (20000) scale the run; at 9 to 12 ms per
# iteration a full run is about four minutes per build.
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
W=${1:-$(mktemp -d "${TMPDIR:-/tmp}/libuv-sqpoll-wakeup.XXXXXX")}
COPIES=${COPIES:-8}
ITERATIONS=${ITERATIONS:-20000}
mkdir -p "$W"
W=$(cd "$W" && pwd)
cd "$W"

# The archive the pin uses, fetched and checked by the pin script.
python3 "$ROOT/scripts/build-libuv.py" > /dev/null
ARCHIVE=$ROOT/.build/libuv/$(ls "$ROOT/.build/libuv" | grep -E '^libuv-v.*\.tar\.gz$' | head -1)
echo "archive: $ARCHIVE"
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
