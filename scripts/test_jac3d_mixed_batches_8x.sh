#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$SCRIPT_DIR"

require_command() {
    local cmd="$1"
    local help_msg="$2"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Missing required command: $cmd. $help_msg" >&2
        exit 1
    fi
}

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

resolve_scheduler_sources() {
    local scheduler_root="${ABT_SCHEDULER_DIR:-$REPO_ROOT/workstealing_scheduler}"
    SCHEDULER_OLD_SRC="$scheduler_root/abt_workstealing_scheduler.c"
    SCHEDULER_NEW_SRC="$scheduler_root/abt_workstealing_scheduler_cost_aware.c"

    if [ ! -f "$SCHEDULER_OLD_SRC" ] || [ ! -f "$SCHEDULER_NEW_SRC" ]; then
        echo "Scheduler sources not found. Set ABT_SCHEDULER_DIR or verify repository layout." >&2
        exit 1
    fi
}

build_jac3d_binary() {
    local output_bin="$1"
    local grid_size="$2"

    gcc -O3 -Wall -Wextra $ABT_CFLAGS \
        -I"$JAC3D_SRC_DIR" \
        -I"$(dirname "$SCHEDULER_OLD_SRC")" \
        "-DL=$grid_size" \
        -o "$output_bin" \
        "$JAC3D_SRC_DIR/jac3d.c" "$JAC3D_SRC_DIR/abt_reduction.c" \
        "$SCHEDULER_OLD_SRC" \
        "$SCHEDULER_NEW_SRC" \
        $ABT_LIBS
}

require_command gcc "Install GCC or Clang-compatible gcc."
require_command bc "Install bc for floating-point aggregation."
resolve_argobots_flags
resolve_scheduler_sources

JAC3D_SRC_DIR="${JAC3D_SRC_DIR:-$REPO_ROOT/jac3d_argobots}"
if [ ! -f "$JAC3D_SRC_DIR/jac3d.c" ] || [ ! -f "$JAC3D_SRC_DIR/abt_reduction.c" ]; then
    echo "Jacobi-3D sources not found. Set JAC3D_SRC_DIR or verify repository layout." >&2
    exit 1
fi

XSTREAMS="${XSTREAMS:-8}"
THREADS_STR="${THREADS_LIST:-6 10 12 14}"
read -r -a THREADS <<< "$THREADS_STR"
MIXED_TASK_SIZES=(320 384 448 512 576 640 768 896)
MIXED_TASK_BATCHES="${MIXED_TASK_BATCHES:-1}"

mkdir -p results_scheduler_compare
RESULTS="results_scheduler_compare/benchmark_mixed_8x.txt"
SUMMARY="results_scheduler_compare/summary_mixed_8x.csv"
echo "scheduler,xstreams,threads,batch,total_time_seconds,total_real_time_nanos,total_steals_count,verification" > "$SUMMARY"
echo "8-way mixed Jacobi benchmark (sizes: ${MIXED_TASK_SIZES[*]})" > "$RESULTS"
echo "xstreams=$XSTREAMS, threads_list=${THREADS[*]}, batches=$MIXED_TASK_BATCHES" >> "$RESULTS"

echo "Building binaries for sizes: ${MIXED_TASK_SIZES[*]}"
for size in "${MIXED_TASK_SIZES[@]}"; do
    build_jac3d_binary "jac3d_L${size}" "$size"
done

for scheduler in old new; do
    echo "" >> "$RESULTS"
    echo "Scheduler: $scheduler" >> "$RESULTS"
    echo "----------------------" >> "$RESULTS"

    for threads in "${THREADS[@]}"; do
        for batch in $(seq 1 "$MIXED_TASK_BATCHES"); do
            pids=()
            output_files=()
            verification="SUCCESSFUL"
            total_time=0
            total_nanos=0
            total_steals=0

            for size in "${MIXED_TASK_SIZES[@]}"; do
                output_file="results_scheduler_compare/jac3d_${scheduler}_8x_L${size}_x${XSTREAMS}_t${threads}_batch${batch}.txt"
                ABT_WS_SCHEDULER=$scheduler "./jac3d_L${size}" "$XSTREAMS" "$threads" > "$output_file" 2>&1 &
                pids+=("$!")
                output_files+=("$output_file")
            done

            for pid in "${pids[@]}"; do
                wait "$pid"
            done

            for output_file in "${output_files[@]}"; do
                TIME=$(grep "Time in seconds" "$output_file" | awk '{print $NF}')
                TIME_NANOS=$(grep "Real time" "$output_file" | awk '{print $NF}')
                VERIFICATION=$(grep "Verification" "$output_file" | awk '{print $NF}')
                STEALS_COUNT=$(grep "Steals count" "$output_file" | awk '{print $NF}')
                total_time=$(echo "$total_time + $TIME" | bc -l)
                total_nanos=$(echo "$total_nanos + $TIME_NANOS" | bc)
                total_steals=$(echo "$total_steals + $STEALS_COUNT" | bc)
                if [ "$VERIFICATION" = "UNSUCCESSFUL" ]; then
                    verification="UNSUCCESSFUL"
                fi
            done

            echo "$scheduler,$XSTREAMS,$threads,$batch,$total_time,$total_nanos,$total_steals,$verification" >> "$SUMMARY"
            echo "$scheduler x=$XSTREAMS t=$threads batch=$batch parallel_tasks=8 sizes=${MIXED_TASK_SIZES[*]} total_time=$total_time total_steals=$total_steals verification=$verification" >> "$RESULTS"
        done
    done
done

echo "Done. Results:"
echo "  - $SUMMARY"
echo "  - $RESULTS"
