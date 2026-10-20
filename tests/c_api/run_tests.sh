#!/bin/bash
set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCHED="$REPO_ROOT/workstealing_scheduler"
BUILD="$SCRIPT_DIR/build"
RESULTS="$REPO_ROOT/tests_results/c_api"
THREADS="${THREADS:-1 2 4 8}"

resolve_argobots() {
    local candidates=()
    if [ -n "${ARGOBOTS_INSTALL_DIR:-}" ]; then
        candidates+=("$ARGOBOTS_INSTALL_DIR")
    fi
    candidates+=("$REPO_ROOT/third_party/argobots-prefix" "$HOME/local/argobots" \
                 "$HOME/argobots-install" "/usr/local" "/usr")
    for dir in "${candidates[@]}"; do
        if [ -f "$dir/include/abt.h" ] && [ -d "$dir/lib" ]; then
            ABT_DIR="$dir"
            return
        fi
    done
    echo "Argobots не найден. Задайте ARGOBOTS_INSTALL_DIR." >&2
    exit 1
}

resolve_argobots
mkdir -p "$BUILD" "$RESULTS"
gcc -O2 -Wall -Wextra -I"$SCHED" -I"$ABT_DIR/include" -o "$BUILD/test_c_api" \
    "$SCRIPT_DIR/test_c_api.c" "$SCHED/ws_parallel.c" "$SCHED/ws_runtime.c" "$SCHED/ws_task.c" \
    "$SCHED/abt_workstealing_scheduler.c" "$SCHED/abt_workstealing_scheduler_cost_aware.c" \
    -L"$ABT_DIR/lib" -Wl,-rpath,"$ABT_DIR/lib" -labt -lm -lpthread

status=0
run() {
    local label="$1" p="$2"
    shift 2
    local log="$RESULTS/${label}_p${p}.txt"
    if env WS_NUM_THREADS="$p" "$@" "$BUILD/test_c_api" > "$log" 2>&1; then
        printf "%-16s p=%-3s PASSED\n" "$label" "$p"
    else
        printf "%-16s p=%-3s FAILED\n" "$label" "$p"
        status=1
    fi
}

for p in $THREADS; do
    run default "$p" ABT_WS_SCHEDULER=default
    run randws "$p" ABT_WS_SCHEDULER=randws
    run old "$p" ABT_WS_SCHEDULER=old
    run new "$p" ABT_WS_SCHEDULER=new
    run new_iters "$p" ABT_WS_SCHEDULER=new WS_COST=iters
    run new_fixed "$p" ABT_WS_SCHEDULER=new WS_GRAIN=fixed
done

echo "Результаты: $RESULTS"
exit $status
