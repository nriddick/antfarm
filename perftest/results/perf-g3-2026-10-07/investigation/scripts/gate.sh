#!/usr/bin/env bash
# gate.sh NAME args... : wait for 1-min load <= 1, then run perf_gate into results/NAME.
set -u
name=$1; shift
cd /home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree
out=perftest/results/perf-g3-2026-10-07/$name
until awk '{exit !($1 <= 1.0)}' /proc/loadavg; do sleep 15; done
python3 perftest/perf_gate.py --archive /home/nathan-riddick/pi_ground/antfarm/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a --benchmark-ref daad3d9 --output "$out" "$@" > "$out.console.log" 2>&1
rc=$?; echo $rc > "$out.exit-status"; mkdir -p /tmp/g3/done; echo "exit=$rc" >> /tmp/g3/done/$name.done
