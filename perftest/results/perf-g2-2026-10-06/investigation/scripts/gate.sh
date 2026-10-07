#!/usr/bin/env bash
# gate.sh NAME args... : wait for 1-min load <= 1, then run perf_gate into results/NAME.
set -u
name=$1; shift
cd /home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree
out=perftest/results/perf-g2-2026-10-06/$name
until awk '{exit !($1 <= 1.0)}' /proc/loadavg; do sleep 15; done
python3 perftest/perf_gate.py --archive /home/nathan-riddick/pi_ground/antfarm/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a --benchmark-ref 7ee7916 --output "$out" "$@" > "$out.console.log" 2>&1
rc=$?; echo $rc > "$out.exit-status"; echo "exit=$rc" >> /tmp/g2/$name.done
