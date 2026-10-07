#!/usr/bin/env bash
# screen2.sh NAME BIN_B PAIRS ALLOC "cells..." : informal alternating A/B of base vs BIN_B.
set -u; name=$1; B=$2; pairs=$3; export AB_ALLOC=$4; shift 4; A=/tmp/g3/bin/churn-base; cd /tmp/g3
until awk '{exit !($1 <= 1.0)}' /proc/loadavg; do sleep 10; done
for spec in "$@"; do set -- $spec; python3 scripts/ab.py $1 $2 $3 $pairs $A $B | tail -1; done > ab/$name.txt 2>&1
