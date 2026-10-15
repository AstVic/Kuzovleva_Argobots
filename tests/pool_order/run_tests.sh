#!/bin/bash
set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCHED="$REPO_ROOT/workstealing_scheduler"
BUILD="$SCRIPT_DIR/build"
RESULTS="$REPO_ROOT/tests_results/pool_order"

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
gcc -O2 -Wall -Wextra -I"$SCHED" -I"$ABT_DIR/include" -o "$BUILD/test_pool_order" \
    "$SCRIPT_DIR/test_pool_order.c" "$SCHED/ws_runtime.c" "$SCHED/ws_task.c" \
    "$SCHED/abt_workstealing_scheduler.c" "$SCHED/abt_workstealing_scheduler_cost_aware.c" \
    -L"$ABT_DIR/lib" -Wl,-rpath,"$ABT_DIR/lib" -labt -lm -lpthread

status=0
run() {
    local label="$1" arg="$2"
    shift 2
    local log="$RESULTS/${label}.txt"
    if env "$@" "$BUILD/test_pool_order" "$arg" > "$log" 2>&1; then
        printf "%-20s PASSED\n" "$label"
    else
        printf "%-20s FAILED\n" "$label"
        status=1
    fi
}

run pools pools ABT_WS_SCHEDULER=default
run randws lifo ABT_WS_SCHEDULER=randws
run randws_fifo fifo ABT_WS_SCHEDULER=randws_fifo
run old_fifo fifo ABT_WS_SCHEDULER=old WS_POOL=fifo
run old_randws lifo ABT_WS_SCHEDULER=old WS_POOL=randws
run new_fifo fifo ABT_WS_SCHEDULER=new WS_POOL=fifo
run new_randws lifo ABT_WS_SCHEDULER=new WS_POOL=randws

echo "Результаты: $RESULTS"
exit $status
