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
    local third_party_root="$REPO_ROOT/third_party/argobots"

    if [ -n "${ARGOBOTS_INSTALL_DIR:-}" ] && [ -f "$ARGOBOTS_INSTALL_DIR/include/abt.h" ] && [ -d "$ARGOBOTS_INSTALL_DIR/lib" ]; then
        ABT_CFLAGS="-I$ARGOBOTS_INSTALL_DIR/include"
        ABT_LIBS="-L$ARGOBOTS_INSTALL_DIR/lib -labt -lm -lpthread"
        return
    fi

    if [ -f "$third_party_root/argobots-prefix/include/abt.h" ] && [ -d "$third_party_root/argobots-prefix/lib" ]; then
        ABT_CFLAGS="-I$third_party_root/argobots-prefix/include"
        ABT_LIBS="-L$third_party_root/argobots-prefix/lib -labt -lm -lpthread"
        return
    fi

    if [ -f "$third_party_root/src/include/abt.h" ] && [ -d "$third_party_root/src/.libs" ]; then
        ABT_CFLAGS="-I$third_party_root/src/include"
        ABT_LIBS="-L$third_party_root/src/.libs -labt -lm -lpthread"
        return
    fi

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

    echo "Argobots not found." >&2
    echo "Expected a build in third_party/argobots (for example third_party/argobots/src/.libs and src/include, or argobots-prefix)." >&2
    echo "You can also set ARGOBOTS_INSTALL_DIR to a built/install directory." >&2
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

build_binary() {
    gcc -O3 -Wall -Wextra $ABT_CFLAGS \
        -I"$REPO_ROOT/jac3d_argobots" \
        -I"$(dirname "$SCHEDULER_OLD_SRC")" \
        -o jac3d_multi_runtime \
        "$REPO_ROOT/jac3d_argobots/jac3d_multi_runtime.c" \
        "$SCHEDULER_OLD_SRC" \
        "$SCHEDULER_NEW_SRC" \
        $ABT_LIBS
}

require_command gcc "Install GCC or Clang-compatible gcc."
resolve_argobots_flags
resolve_scheduler_sources

XSTREAMS=(1 2 4 8)
THREADS=(1 2 4 8 16)
TASK_SIZES_STR="${TASK_SIZES_8X:-352 320 288 256 224 192 160 128}"
read -r -a TASK_SIZES <<< "$TASK_SIZES_STR"

if [ "${#TASK_SIZES[@]}" -ne 8 ]; then
    echo "Expected exactly 8 task sizes, got ${#TASK_SIZES[@]} (${TASK_SIZES[*]})." >&2
    exit 1
fi

mkdir -p results_scheduler_compare
RESULTS="results_scheduler_compare/benchmark_single_runtime_8x.txt"
SUMMARY="results_scheduler_compare/summary_single_runtime_8x.csv"
echo "scheduler,xstreams,threads,task_count,total_time_seconds,total_real_time_nanos,total_steal_operations,total_stolen_tasks,verification" > "$SUMMARY"
echo "Single-runtime 8-task Jacobi benchmark" > "$RESULTS"
echo "task_sizes=${TASK_SIZES[*]}" >> "$RESULTS"

build_binary

for scheduler in old new; do
    echo "" >> "$RESULTS"
    echo "Scheduler: $scheduler" >> "$RESULTS"
    echo "----------------------" >> "$RESULTS"

    for xstreams in "${XSTREAMS[@]}"; do
        for threads in "${THREADS[@]}"; do
            output_file="results_scheduler_compare/jac3d_${scheduler}_single_runtime_8x_x${xstreams}_t${threads}.txt"
            ABT_WS_SCHEDULER=$scheduler ./jac3d_multi_runtime "$xstreams" "$threads" "${TASK_SIZES[@]}" > "$output_file" 2>&1

            total_time=$(grep "Time in seconds" "$output_file" | awk '{print $NF}')
            total_nanos=$(grep "Real time" "$output_file" | awk '{print $NF}')
            verification=$(grep "Verification" "$output_file" | awk '{print $NF}')
            total_steal_ops=$(grep "Steal operations" "$output_file" | awk '{print $NF}')
            total_stolen_tasks=$(grep "Stolen tasks" "$output_file" | awk '{print $NF}')

            echo "$scheduler,$xstreams,$threads,8,$total_time,$total_nanos,$total_steal_ops,$total_stolen_tasks,$verification" >> "$SUMMARY"
            echo "$scheduler x=$xstreams t=$threads tasks=8 sizes=${TASK_SIZES[*]} total_time=$total_time total_steal_ops=$total_steal_ops total_stolen_tasks=$total_stolen_tasks verification=$verification" >> "$RESULTS"
        done
    done
done

echo "Done. Results:"
echo "  - $SUMMARY"
echo "  - $RESULTS"
