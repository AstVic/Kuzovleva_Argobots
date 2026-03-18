#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR"

resolve_argobots_flags() {
    if command -v pkg-config >/dev/null 2>&1 && pkg-config --exists argobots; then
        ABT_CFLAGS="$(pkg-config --cflags argobots)"
        ABT_LIBS="$(pkg-config --libs argobots) -lm -lpthread"
        return
    fi

    local candidates=()
    if [ -n "${ARGOBOTS_INSTALL_DIR:-}" ]; then
        candidates+=("$ARGOBOTS_INSTALL_DIR")
    fi
    candidates+=("$HOME/local/argobots" "$HOME/argobots-install" "/usr/local" "/usr")

    for dir in "${candidates[@]}"; do
        if [ -f "$dir/include/abt.h" ] && [ -d "$dir/lib" ]; then
            ABT_CFLAGS="-I$dir/include"
            ABT_LIBS="-L$dir/lib -labt -lm -lpthread"
            return
        fi
    done

    echo "Argobots not found (abt.h/libabt)." >&2
    exit 1
}

resolve_argobots_flags

gcc -O3 -Wall -Wextra $ABT_CFLAGS \
    -o jac3d \
    jac3d.c abt_reduction.c \
    ../argobots_framework/examples/workstealing_scheduler/abt_workstealing_scheduler.c \
    ../argobots_framework/examples/workstealing_scheduler/abt_workstealing_scheduler_cost_aware.c \
    $ABT_LIBS

mkdir -p results_scheduler_compare
RESULTS="results_scheduler_compare/benchmark_results.txt"
echo "Argobots Jacobi-3D Scheduler Comparison" > "$RESULTS"
echo "=======================================" >> "$RESULTS"
echo "scheduler,xstreams,threads,time_seconds,real_time_nanos,verification" > results_scheduler_compare/summary.csv

XSTREAMS=(1 2 4 8)
THREADS=(1 2 4 8 16)
NUM_RUNS=2

for scheduler in old new; do
  for xstreams in "${XSTREAMS[@]}"; do
    for threads in "${THREADS[@]}"; do
      total_time=0
      total_nanos=0
      verification="SUCCESSFUL"

      for run in $(seq 1 $NUM_RUNS); do
        OUTPUT_FILE="results_scheduler_compare/jac3d_${scheduler}_x${xstreams}_t${threads}_run${run}.txt"
        ABT_WS_SCHEDULER=$scheduler ./jac3d $xstreams $threads > "$OUTPUT_FILE" 2>&1
        TIME=$(grep "Time in seconds" "$OUTPUT_FILE" | awk '{print $NF}')
        TIME_NANOS=$(grep "Real time" "$OUTPUT_FILE" | awk '{print $NF}')
        VERIFICATION=$(grep "Verification" "$OUTPUT_FILE" | awk '{print $NF}')
        total_time=$(echo "$total_time + $TIME" | bc)
        total_nanos=$(echo "$total_nanos + $TIME_NANOS" | bc)
        if [ "$VERIFICATION" = "UNSUCCESSFUL" ]; then
          verification="UNSUCCESSFUL"
        fi
      done

      mean_time=$(echo "$total_time / $NUM_RUNS" | bc -l)
      mean_nanos=$(echo "$total_nanos / $NUM_RUNS" | bc -l)
      echo "$scheduler,$xstreams,$threads,$mean_time,$mean_nanos,$verification" >> results_scheduler_compare/summary.csv
      echo "$scheduler x=$xstreams t=$threads time=$mean_time verification=$verification" >> "$RESULTS"
    done
  done
done

echo "Done. Results in results_scheduler_compare/"