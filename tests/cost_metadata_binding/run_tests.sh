#!/bin/bash
# Прогон тестов фичи cost_metadata_binding: оценка стоимости хранится вместе
# с ULT вместо отдельной очереди оценок.
#
# Результаты складываются в tests_results/cost_metadata_binding/.
#
# Значения по умолчанию рассчитаны на MacBook Air M4 (10 ядер: 4 P + 6 E,
# пассивное охлаждение). Замеры чередуют старый и новый механизм внутри одного
# повтора, чтобы медленный нагрев корпуса не превратился в разницу между ними.
#
# Переменные окружения:
#   JAC_L, JAC_CHUNKS, JAC_XSTREAMS, RUNS   — A/B на Jacobi
#   MULTI_SIZES, MULTI_XSTREAMS, MULTI_RUNS — A/B на multi-runtime
#   INV_XSTREAMS, INV_PRODUCERS, INV_TASKS  — тест инварианта
#   COOLDOWN                                — пауза между прогонами, с
#   ARGOBOTS_INSTALL_DIR                    — где искать Argobots
set -euo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
RESULTS="$REPO_ROOT/tests_results/cost_metadata_binding"
BUILD="$SCRIPT_DIR/build"
SCHED="$REPO_ROOT/workstealing_scheduler"
JAC="$REPO_ROOT/jac3d_argobots"

JAC_L="${JAC_L:-384}"
JAC_CHUNKS="${JAC_CHUNKS:-16 32 64 80 128}"
JAC_XSTREAMS="${JAC_XSTREAMS:-1 2 4 8 10}"
RUNS="${RUNS:-3}"
MULTI_SIZES="${MULTI_SIZES:-256 224 192 160}"
MULTI_XSTREAMS="${MULTI_XSTREAMS:-4 8}"
MULTI_RUNS="${MULTI_RUNS:-3}"
INV_XSTREAMS="${INV_XSTREAMS:-2 4 8}"
INV_PRODUCERS="${INV_PRODUCERS:-1 4}"
INV_TASKS="${INV_TASKS:-8000}"
COOLDOWN="${COOLDOWN:-2}"

mkdir -p "$RESULTS" "$BUILD"

resolve_argobots_flags() {
    if [ -n "${ARGOBOTS_INSTALL_DIR:-}" ]; then
        ABT_CFLAGS="-I$ARGOBOTS_INSTALL_DIR/include"
        ABT_LIBS="-L$ARGOBOTS_INSTALL_DIR/lib -labt -lm -lpthread"
        return
    fi
    if command -v pkg-config >/dev/null 2>&1 && pkg-config --exists argobots; then
        ABT_CFLAGS="$(pkg-config --cflags argobots)"
        ABT_LIBS="$(pkg-config --libs argobots) -lm -lpthread"
        return
    fi
    for dir in "$REPO_ROOT/third_party/argobots-prefix" "$REPO_ROOT/third_party/argobots"; do
        if [ -f "$dir/include/abt.h" ]; then
            ABT_CFLAGS="-I$dir/include"
            ABT_LIBS="-L$dir/lib -labt -lm -lpthread"
            return
        fi
    done
    echo "Argobots не найден. Задайте ARGOBOTS_INSTALL_DIR." >&2
    exit 1
}
resolve_argobots_flags

CC="${CC:-gcc}"
COMMON="-O2 -Wall -Wextra $ABT_CFLAGS -I$SCHED -I$JAC"
SCHED_SRC="$SCHED/abt_workstealing_scheduler.c $SCHED/abt_workstealing_scheduler_cost_aware.c $SCHED/ws_task.c"

echo "== сборка =="
# shellcheck disable=SC2086
$CC $COMMON -DWS_DEBUG_COST_CHECK -o "$BUILD/test_cost_binding_new" \
    "$SCRIPT_DIR/test_cost_binding.c" $SCHED_SRC $ABT_LIBS
# shellcheck disable=SC2086
$CC $COMMON -DWS_DEBUG_COST_CHECK -DWS_LEGACY_EST_FIFO -o "$BUILD/test_cost_binding_legacy" \
    "$SCRIPT_DIR/test_cost_binding.c" $SCHED_SRC $ABT_LIBS
# shellcheck disable=SC2086
$CC $COMMON -DL=$JAC_L -o "$BUILD/jac3d_new" "$JAC/jac3d.c" "$JAC/abt_reduction.c" $SCHED_SRC $ABT_LIBS
# shellcheck disable=SC2086
$CC $COMMON -DL=$JAC_L -DWS_LEGACY_EST_FIFO -o "$BUILD/jac3d_legacy" "$JAC/jac3d.c" "$JAC/abt_reduction.c" $SCHED_SRC $ABT_LIBS
# shellcheck disable=SC2086
$CC $COMMON -o "$BUILD/multi_new" "$JAC/jac3d_multi_runtime.c" $SCHED_SRC $ABT_LIBS
# shellcheck disable=SC2086
$CC $COMMON -DWS_LEGACY_EST_FIFO -o "$BUILD/multi_legacy" "$JAC/jac3d_multi_runtime.c" $SCHED_SRC $ABT_LIBS

ABT_LIBDIR=$(printf '%s\n' $ABT_LIBS | sed -n 's/^-L//p' | head -1)
if [ -n "$ABT_LIBDIR" ]; then
    export LD_LIBRARY_PATH="$ABT_LIBDIR:${LD_LIBRARY_PATH:-}"
    export DYLD_LIBRARY_PATH="$ABT_LIBDIR:${DYLD_LIBRARY_PATH:-}"
fi

{
    echo "date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "host: $(uname -srm)"
    if command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu >/dev/null 2>&1; then
        echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo 'Apple Silicon')"
        echo "cores total: $(sysctl -n hw.ncpu)"
        echo "cores performance: $(sysctl -n hw.perflevel0.logicalcpu 2>/dev/null || echo n/a)"
        echo "cores efficiency: $(sysctl -n hw.perflevel1.logicalcpu 2>/dev/null || echo n/a)"
        echo "memory GB: $(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 ))"
        echo "power: $(pmset -g batt 2>/dev/null | head -1)"
        echo "low power mode: $(pmset -g 2>/dev/null | awk '/lowpowermode/{print $2}')"
    else
        echo "cores: $( (command -v nproc >/dev/null && nproc) || echo n/a )"
    fi
    echo "compiler: $($CC --version | head -1)"
    echo "argobots: $ABT_CFLAGS"
    echo "git: $(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo n/a) $(cd "$REPO_ROOT" && git rev-parse --abbrev-ref HEAD 2>/dev/null || echo n/a)"
    echo "params: JAC_L=$JAC_L JAC_CHUNKS='$JAC_CHUNKS' JAC_XSTREAMS='$JAC_XSTREAMS' RUNS=$RUNS"
    echo "params: MULTI_SIZES='$MULTI_SIZES' MULTI_XSTREAMS='$MULTI_XSTREAMS' MULTI_RUNS=$MULTI_RUNS"
    echo "params: INV_XSTREAMS='$INV_XSTREAMS' INV_PRODUCERS='$INV_PRODUCERS' INV_TASKS=$INV_TASKS COOLDOWN=$COOLDOWN"
} > "$RESULTS/environment.txt"
cat "$RESULTS/environment.txt" | sed 's/^/  /'

echo "== тест 1: инвариант ULT <-> оценка =="
echo "binding,xstreams,producers,tasks,barrier,dispatches,wrong_estimate,missing_estimate,foreign_took_estimate,queued_count_left,queued_count_worst_pool,queued_estimated_left,result" \
    > "$RESULTS/invariant_summary.csv"
for binding in new legacy; do
    for x in $INV_XSTREAMS; do
        for producers in $INV_PRODUCERS; do
            for barrier in no yes; do
                args=("$x" "$producers" "$INV_TASKS")
                [ "$barrier" = yes ] && args+=(--barrier)
                out="$RESULTS/invariant_${binding}_x${x}_p${producers}_barrier_${barrier}.txt"
                set +e
                "$BUILD/test_cost_binding_${binding}" "${args[@]}" > "$out" 2>&1
                rc=$?
                set -e
                get() { grep "^$1" "$out" | sed -E 's/^[^:]*: *(-?[0-9]+).*/\1/'; }
                worst() { grep "^$1" "$out" | sed -E 's/.*: *(-?[0-9]+)\)?$/\1/'; }
                tasks=$(grep -o 'tasks=[0-9]*' "$out" | head -1 | cut -d= -f2)
                verdict=$(grep -o 'PASSED\|FAILED' "$out" | tail -1)
                echo "$binding,$x,$producers,$tasks,$barrier,$(get 'ULT dispatches'),$(get 'wrong estimate'),$(get 'missing estimate'),$(get 'foreign took'),$(get 'queued_count left'),$(worst 'queued_count left'),$(get 'queued_estimated'),$verdict" \
                    >> "$RESULTS/invariant_summary.csv"
                echo "  $binding x=$x producers=$producers barrier=$barrier -> $verdict (rc=$rc)"
            done
        done
    done
done

echo "== тест 2: результаты вычислений не изменились =="
{
    echo "multi-runtime, sizes: $MULTI_SIZES"
    for mode in default old new new_legacy; do
        bin="$BUILD/multi_new"; env_mode="$mode"
        if [ "$mode" = new_legacy ]; then bin="$BUILD/multi_legacy"; env_mode=new; fi
        echo "--- ABT_WS_SCHEDULER=$env_mode binary=$(basename "$bin")"
        # shellcheck disable=SC2086
        ABT_WS_SCHEDULER=$env_mode "$bin" 4 $MULTI_SIZES | grep -E "problem\[|Verification"
    done
} > "$RESULTS/equivalence_multi.txt" 2>&1
unique=$(grep "problem\[" "$RESULTS/equivalence_multi.txt" | sed 's/.*final_eps/final_eps/' | sort -u | wc -l | tr -d ' ')
nproblems=$(printf '%s\n' $MULTI_SIZES | wc -l | tr -d ' ')
echo "  различных результатов: $unique (ожидается $nproblems — по одному на задачу)"

echo "== тест 3: верификация Jacobi во всех режимах =="
{
    echo "jac3d L=$JAC_L"
    if [ "$JAC_L" != "384" ]; then
        echo "ВНИМАНИЕ: встроенная проверка Jacobi рассчитана на L=384;"
        echo "при другом размере сетки Verification всегда UNSUCCESSFUL."
    fi
    for mode in default old new new_legacy; do
        bin="$BUILD/jac3d_new"; env_mode="$mode"
        if [ "$mode" = new_legacy ]; then bin="$BUILD/jac3d_legacy"; env_mode=new; fi
        line=$(ABT_WS_SCHEDULER=$env_mode "$bin" 4 80 | grep -E "Verification|Mono time" | tr '\n' ' ')
        echo "$mode: $line"
    done
} > "$RESULTS/verification_jac3d.txt" 2>&1
sed 's/^/  /' "$RESULTS/verification_jac3d.txt"

# медиана столбца чисел со стандартного ввода
median() {
    sort -n | awk '{a[NR]=$1} END{ if (NR==0) print "n/a";
        else if (NR%2) printf "%.0f\n", a[(NR+1)/2];
        else printf "%.0f\n", (a[NR/2]+a[NR/2+1])/2 }'
}

nx=$(printf '%s\n' $JAC_XSTREAMS | wc -l | tr -d ' ')
nc=$(printf '%s\n' $JAC_CHUNKS | wc -l | tr -d ' ')
total=$((nx * nc * 2 * RUNS))
echo "== тест 4: A/B на Jacobi: $nx x $nc конфигураций, 2 механизма, $RUNS повторов = $total прогонов =="
AB="$RESULTS/ab_timing.csv"
echo "binding,xstreams,chunks,run,order,unix_time,mono_time_nanos,steal_operations,stolen_tasks,verification" > "$AB"
order=0
for run in $(seq 1 "$RUNS"); do
    for x in $JAC_XSTREAMS; do
        for c in $JAC_CHUNKS; do
            # оба механизма подряд внутри одного повтора: нагрев действует на них одинаково
            for binding in legacy new; do
                order=$((order + 1))
                out=$(ABT_WS_SCHEDULER=new "$BUILD/jac3d_$binding" "$x" "$c")
                echo "$binding,$x,$c,$run,$order,$(date +%s),$(echo "$out" | awk '/Mono time/{print $NF}'),$(echo "$out" | awk '/Steal operations/{print $NF}'),$(echo "$out" | awk '/Stolen tasks/{print $NF}'),$(echo "$out" | awk '/Verification/{print $NF}')" >> "$AB"
                sleep "$COOLDOWN"
            done
        done
    done
    echo "  повтор $run из $RUNS готов"
done

echo "== тест 5: A/B на multi-runtime (неоднородные задачи) =="
ABM="$RESULTS/ab_multi.csv"
echo "binding,xstreams,sizes,run,order,unix_time,mono_time_nanos,steal_operations,stolen_tasks,verification" > "$ABM"
order=0
for run in $(seq 1 "$MULTI_RUNS"); do
    for x in $MULTI_XSTREAMS; do
        for binding in legacy new; do
            order=$((order + 1))
            # shellcheck disable=SC2086
            out=$(ABT_WS_SCHEDULER=new "$BUILD/multi_$binding" "$x" $MULTI_SIZES)
            echo "$binding,$x,\"$MULTI_SIZES\",$run,$order,$(date +%s),$(echo "$out" | awk '/Mono time/{print $NF}'),$(echo "$out" | awk '/Steal operations/{print $NF}'),$(echo "$out" | awk '/Stolen tasks/{print $NF}'),$(echo "$out" | awk '/Verification/{print $NF}')" >> "$ABM"
            sleep "$COOLDOWN"
        done
    done
    echo "  повтор $run из $MULTI_RUNS готов"
done

echo "== медианы =="
{
    echo "Jacobi L=$JAC_L (время в миллисекундах, медиана по $RUNS прогонам)"
    printf "%-8s %-9s %-7s %12s %12s %10s\n" "xstreams" "chunks" "" "legacy" "new" "new/legacy"
    for x in $JAC_XSTREAMS; do
        for c in $JAC_CHUNKS; do
            l=$(awk -F, -v x="$x" -v c="$c" '$1=="legacy" && $2==x && $3==c {print $7}' "$AB" | median)
            n=$(awk -F, -v x="$x" -v c="$c" '$1=="new" && $2==x && $3==c {print $7}' "$AB" | median)
            ratio=$(awk -v a="$l" -v b="$n" 'BEGIN{ if (a+0>0) printf "%.3f", b/a; else print "n/a" }')
            printf "%-8s %-9s %-7s %12.1f %12.1f %10s\n" "$x" "$c" "" "$(awk -v v="$l" 'BEGIN{print v/1e6}')" "$(awk -v v="$n" 'BEGIN{print v/1e6}')" "$ratio"
        done
    done
    echo
    echo "multi-runtime sizes=$MULTI_SIZES (медиана по $MULTI_RUNS прогонам)"
    printf "%-8s %12s %12s %10s\n" "xstreams" "legacy" "new" "new/legacy"
    for x in $MULTI_XSTREAMS; do
        l=$(awk -F, -v x="$x" '$1=="legacy" && $2==x {print $7}' "$ABM" | median)
        n=$(awk -F, -v x="$x" '$1=="new" && $2==x {print $7}' "$ABM" | median)
        ratio=$(awk -v a="$l" -v b="$n" 'BEGIN{ if (a+0>0) printf "%.3f", b/a; else print "n/a" }')
        printf "%-8s %12.1f %12.1f %10s\n" "$x" "$(awk -v v="$l" 'BEGIN{print v/1e6}')" "$(awk -v v="$n" 'BEGIN{print v/1e6}')" "$ratio"
    done
} | tee "$RESULTS/ab_timing_medians.txt"

echo
echo "Готово. Результаты: $RESULTS"
echo "Перед серьёзным замером: ноутбук в розетке, Low Power Mode выключен,"
echo "тяжёлые программы закрыты. Прогоны чередуют legacy и new, но нагрев"
echo "всё равно стоит учитывать: смотрите столбец unix_time в CSV."
