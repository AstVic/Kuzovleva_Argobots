#!/bin/bash
# Сравнение планировщиков на Jacobi-3D с последовательным эталоном.
#
# Методика:
#   * основная величина времени — CLOCK_MONOTONIC ("Mono time"), а не clock():
#     clock() складывает процессорное время всех потоков и растёт с числом ES;
#   * каждая конфигурация повторяется NUM_RUNS раз, берётся медиана;
#   * режимы планировщика чередуются ВНУТРИ одного повтора, чтобы прогрев
#     машины (особенно ноутбука с пассивным охлаждением) не превратился в
#     разницу между режимами;
#   * перед измерениями делается прогревочный прогон, он не учитывается;
#   * speedup = медиана последовательной программы / медиана параллельной.
#
# Переменные окружения: XSTREAMS, CHUNKS, SCHEDULERS, NUM_RUNS, GRID_SIZE,
#   COOLDOWN, ARGOBOTS_INSTALL_DIR, ABT_SCHEDULER_DIR.
set -euo pipefail

# Числа в отчётах и CSV печатаются только с точкой: под русской локалью awk и bc
# выдают "10,85", и строка CSV перестаёт соответствовать заголовку.
export LC_ALL=C

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
    candidates+=("$REPO_ROOT/third_party/argobots-prefix" "$HOME/local/argobots" \
                 "$HOME/argobots-install" "/usr/local" "/usr")

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
    SCHEDULER_TASK_SRC="$scheduler_root/ws_task.c"

    for f in "$SCHEDULER_OLD_SRC" "$SCHEDULER_NEW_SRC" "$SCHEDULER_TASK_SRC"; do
        if [ ! -f "$f" ]; then
            echo "Scheduler source not found: $f" >&2
            echo "Set ABT_SCHEDULER_DIR or verify repository layout." >&2
            exit 1
        fi
    done
}

require_command gcc "Install GCC or Clang-compatible gcc."
resolve_argobots_flags
resolve_scheduler_sources

JAC3D_SRC_DIR="${JAC3D_SRC_DIR:-$REPO_ROOT/jac3d_argobots}"
SEQ_SRC="${SEQ_SRC:-$REPO_ROOT/reference/jac3d_seq.c}"
if [ ! -f "$JAC3D_SRC_DIR/jac3d.c" ] || [ ! -f "$JAC3D_SRC_DIR/abt_reduction.c" ]; then
    echo "Jacobi-3D sources not found. Set JAC3D_SRC_DIR or verify repository layout." >&2
    exit 1
fi

GRID_SIZE="${GRID_SIZE:-384}"
XSTREAMS="${XSTREAMS:-1 2 4 8}"
CHUNKS="${CHUNKS:-32 64 80}"
SCHEDULERS="${SCHEDULERS:-default old new}"
NUM_RUNS="${NUM_RUNS:-3}"
COOLDOWN="${COOLDOWN:-2}"

build_jac3d_binary() {
    # shellcheck disable=SC2086
    gcc -O3 -Wall -Wextra $ABT_CFLAGS \
        -I"$JAC3D_SRC_DIR" -I"$(dirname "$SCHEDULER_OLD_SRC")" \
        "-DL=$GRID_SIZE" -o "$1" \
        "$JAC3D_SRC_DIR/jac3d.c" "$JAC3D_SRC_DIR/abt_reduction.c" \
        "$SCHEDULER_OLD_SRC" "$SCHEDULER_NEW_SRC" "$SCHEDULER_TASK_SRC" \
        $ABT_LIBS
}

build_seq_binary() {
    if [ ! -f "$SEQ_SRC" ]; then
        echo "Последовательный эталон не найден: $SEQ_SRC" >&2
        exit 1
    fi
    gcc -O3 -Wall -Wextra "-DL=$GRID_SIZE" -o "$1" "$SEQ_SRC" -lm
}

echo "== сборка =="
build_jac3d_binary jac3d
build_seq_binary jac3d_seq

mkdir -p results_scheduler_compare
RESULTS="results_scheduler_compare/benchmark_results.txt"
SUMMARY="results_scheduler_compare/summary.csv"
RUNS_CSV="results_scheduler_compare/runs.csv"
ENVFILE="results_scheduler_compare/environment.txt"

{
    echo "date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "host: $(uname -srm)"
    if command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu >/dev/null 2>&1; then
        echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo 'Apple Silicon')"
        echo "cores total: $(sysctl -n hw.ncpu)"
        echo "cores performance: $(sysctl -n hw.perflevel0.logicalcpu 2>/dev/null || echo n/a)"
        echo "cores efficiency: $(sysctl -n hw.perflevel1.logicalcpu 2>/dev/null || echo n/a)"
        echo "power: $(pmset -g batt 2>/dev/null | head -1)"
    else
        echo "cores: $( (command -v nproc >/dev/null && nproc) || echo n/a )"
    fi
    echo "compiler: $(gcc --version | head -1)"
    echo "argobots: $ABT_CFLAGS"
    echo "git: $(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo n/a) $(cd "$REPO_ROOT" && git rev-parse --abbrev-ref HEAD 2>/dev/null || echo n/a)"
    echo "params: GRID_SIZE=$GRID_SIZE XSTREAMS='$XSTREAMS' CHUNKS='$CHUNKS' SCHEDULERS='$SCHEDULERS' NUM_RUNS=$NUM_RUNS COOLDOWN=$COOLDOWN"
} > "$ENVFILE"
sed 's/^/  /' "$ENVFILE"

echo "scheduler,grid_size,xstreams,chunks,run,order,unix_time,mono_time_nanos,cpu_time_seconds,steal_operations,stolen_tasks,verification" > "$RUNS_CSV"

record_run() {   # record_run <scheduler> <xstreams> <chunks> <run> <order> <вывод программы>
    local sched="$1" x="$2" c="$3" run="$4" order="$5" out="$6"
    local mono cpu ops tasks ver
    mono=$(echo "$out" | awk '/Mono time/{print $NF}')
    cpu=$(echo "$out" | awk '/Time in seconds/{print $NF}')
    ops=$(echo "$out" | awk '/Steal operations/{print $NF}')
    tasks=$(echo "$out" | awk '/Stolen tasks/{print $NF}')
    ver=$(echo "$out" | awk '/Verification/{print $NF}')
    echo "$sched,$GRID_SIZE,$x,$c,$run,$order,$(date +%s),$mono,$cpu,$ops,$tasks,$ver" >> "$RUNS_CSV"
}

echo "== прогрев (не учитывается) =="
ABT_WS_SCHEDULER=new ./jac3d 2 "$(echo "$CHUNKS" | awk '{print $1}')" > /dev/null
./jac3d_seq > /dev/null
sleep "$COOLDOWN"

echo "== последовательный эталон =="
for run in $(seq 1 "$NUM_RUNS"); do
    out=$(./jac3d_seq)
    record_run sequential 1 1 "$run" 0 "$out"
    sleep "$COOLDOWN"
done
echo "  готово: $NUM_RUNS прогонов"

echo "== параллельные прогоны =="
order=0
for run in $(seq 1 "$NUM_RUNS"); do
    for x in $XSTREAMS; do
        for c in $CHUNKS; do
            for sched in $SCHEDULERS; do
                order=$((order + 1))
                out=$(ABT_WS_SCHEDULER=$sched ./jac3d "$x" "$c")
                record_run "$sched" "$x" "$c" "$run" "$order" "$out"
                sleep "$COOLDOWN"
            done
        done
    done
    echo "  повтор $run из $NUM_RUNS готов"
done

median() {
    sort -n | awk '{a[NR]=$1} END{ if (NR==0) print 0;
        else if (NR%2) printf "%.0f\n", a[(NR+1)/2];
        else printf "%.0f\n", (a[NR/2]+a[NR/2+1])/2 }'
}

seq_median=$(awk -F, '$1=="sequential"{print $8}' "$RUNS_CSV" | median)

echo "scheduler,grid_size,xstreams,chunks,runs,median_mono_nanos,median_mono_seconds,speedup_vs_sequential,parallel_efficiency,median_steal_operations,median_stolen_tasks,verification" > "$SUMMARY"
{
    echo "Argobots Jacobi-3D: сравнение планировщиков с последовательным эталоном"
    echo "======================================================================"
    cat "$ENVFILE"
    echo
    printf "%-12s %-9s %-7s %14s %10s %12s\n" "scheduler" "xstreams" "chunks" "median, s" "speedup" "efficiency"
    printf "%-12s %-9s %-7s %14.3f %10s %12s\n" "sequential" "1" "-" \
        "$(awk -v v="$seq_median" 'BEGIN{print v/1e9}')" "1.000" "-"
    for x in $XSTREAMS; do
        for c in $CHUNKS; do
            for sched in $SCHEDULERS; do
                med=$(awk -F, -v s="$sched" -v x="$x" -v c="$c" \
                    '$1==s && $3==x && $4==c {print $8}' "$RUNS_CSV" | median)
                ops=$(awk -F, -v s="$sched" -v x="$x" -v c="$c" \
                    '$1==s && $3==x && $4==c {print $10}' "$RUNS_CSV" | median)
                tasks=$(awk -F, -v s="$sched" -v x="$x" -v c="$c" \
                    '$1==s && $3==x && $4==c {print $11}' "$RUNS_CSV" | median)
                ver=$(awk -F, -v s="$sched" -v x="$x" -v c="$c" \
                    '$1==s && $3==x && $4==c {print $12}' "$RUNS_CSV" | sort -u | tr '\n' '/' | sed 's|/$||')
                speedup=$(awk -v a="$seq_median" -v b="$med" 'BEGIN{ if (b+0>0) printf "%.3f", a/b; else print "n/a" }')
                eff=$(awk -v s="$speedup" -v x="$x" 'BEGIN{ if (s+0>0) printf "%.3f", s/x; else print "n/a" }')
                secs=$(awk -v v="$med" 'BEGIN{print v/1e9}')
                echo "$sched,$GRID_SIZE,$x,$c,$NUM_RUNS,$med,$secs,$speedup,$eff,$ops,$tasks,$ver" >> "$SUMMARY"
                printf "%-12s %-9s %-7s %14.3f %10s %12s\n" "$sched" "$x" "$c" "$secs" "$speedup" "$eff"
            done
        done
    done
} | tee "$RESULTS"

echo
echo "Готово."
echo "  сырые прогоны : $RUNS_CSV"
echo "  сводка        : $SUMMARY"
echo "  отчёт         : $RESULTS"
