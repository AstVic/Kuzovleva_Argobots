#!/bin/bash
# Сравнение планировщиков на C-программах из benchmarks/: NPB CG и NPB BT-MZ.
# Режимы запускаются по кругу PASSES раз со сдвигом порядка,
# каждый запуск проверяется встроенной верификацией программы.
#
# Переменные окружения:
#   PROGRAMS  - cg bt-mz
#   MODES     - seq omp default randws randws_fifo old new; вариант режима -
#               метка:режим:ПЕРЕМЕННАЯ=значение,..., например omp_dyn:omp:OMP_SCHEDULE=dynamic
#   THREADS   - число потоков (по умолчанию все ядра)
#   CG_CLASS, BT_CLASS - класс задач NPB (S W A B C)
#   PASSES    - число проходов по всем режимам
set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD="$REPO_ROOT/benchmarks/build"

detect_cpus() {
    if command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu >/dev/null 2>&1; then
        sysctl -n hw.ncpu
    else
        nproc
    fi
}

PROGRAMS="${PROGRAMS:-cg bt-mz}"
MODES="${MODES:-seq omp randws old new}"
THREADS="${THREADS:-$(detect_cpus)}"
CG_CLASS="${CG_CLASS:-B}"
BT_CLASS="${BT_CLASS:-A}"
PASSES="${PASSES:-3}"

CG_CLASSES="$CG_CLASS" BT_CLASSES="$BT_CLASS" "$REPO_ROOT/benchmarks/build.sh"

OUT="$REPO_ROOT/tests_results/c_benchmarks/$(date -u +%Y%m%d_%H%M%S)"
mkdir -p "$OUT/logs"
RUNS_CSV="$OUT/runs.csv"
{
    echo "date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "host: $(uname -srm)"
    if command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu >/dev/null 2>&1; then
        echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo n/a)"
        echo "power: $(pmset -g batt 2>/dev/null | head -1)"
    else
        echo "cores: $(nproc)"
    fi
    echo "git: $(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo n/a) $(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo n/a)"
    echo "params: PROGRAMS='$PROGRAMS' MODES='$MODES' THREADS=$THREADS CG_CLASS=$CG_CLASS BT_CLASS=$BT_CLASS PASSES=$PASSES"
} | tee "$OUT/environment.txt"
echo "program,class,mode,threads,pass,seconds,status" > "$RUNS_CSV"

spec_label() { echo "${1%%:*}"; }
spec_mode() {
    local rest="${1#*:}"
    if [ "$rest" = "$1" ]; then echo "$1"; else echo "${rest%%:*}"; fi
}
spec_env() {
    local rest="${1#*:}"
    if [ "$rest" = "$1" ]; then return; fi
    rest="${rest#*:}"
    if [ "$rest" = "${1#*:}" ]; then return; fi
    echo "$rest" | tr ',' ' '
}

binary_for() {
    local program="$1" mode="$2" variant
    case "$mode" in
        seq) variant=seq ;;
        omp) variant=omp ;;
        *) variant=ws ;;
    esac
    case "$program" in
        cg) echo "$BUILD/cg/$CG_CLASS/cg.$variant" ;;
        bt-mz) echo "$BUILD/bt-mz/$BT_CLASS/bt-mz.$variant" ;;
    esac
}

class_for() {
    case "$1" in
        cg) echo "$CG_CLASS" ;;
        bt-mz) echo "$BT_CLASS" ;;
        *) echo "-" ;;
    esac
}

run_one() {
    local program="$1" spec="$2" pass="$3"
    local label mode bin log stats envs=() seconds status
    label=$(spec_label "$spec")
    mode=$(spec_mode "$spec")
    bin=$(binary_for "$program" "$mode")
    log="$OUT/logs/${program}_${label}_p${pass}.txt"
    stats="$OUT/logs/${program}_${label}_p${pass}.stats"
    case "$mode" in
        seq) ;;
        omp) envs=(OMP_NUM_THREADS="$THREADS") ;;
        *) envs=(ABT_WS_SCHEDULER="$mode" WS_NUM_THREADS="$THREADS" WS_STATS="$stats") ;;
    esac
    for v in $(spec_env "$spec"); do envs+=("$v"); done
    if [ ! -x "$bin" ]; then
        echo "$program,$(class_for "$program"),$label,$THREADS,$pass,,MISSING" >> "$RUNS_CSV"
        return
    fi
    if (cd "$OUT" && env ${envs[@]+"${envs[@]}"} "$bin") > "$log" 2>&1; then
        status=OK
    else
        status=FAILED
    fi
    if ! grep -qiE "verification (being performed for class [A-Z] *)?successful|verification +successful|verification += +SUCCESSFUL|^ *VERIFICATION SUCCESSFUL" "$log"; then
        status=FAILED
    fi
    seconds=$(awk -F= '/Real time \(nanos\)/{gsub(/ /,"",$2); printf "%.6f", $2 / 1e9; exit}' "$log")
    echo "$program,$(class_for "$program"),$label,$THREADS,$pass,$seconds,$status" >> "$RUNS_CSV"
    printf "  %-3s %-10s %-14s %10s %s\n" "$pass" "$program" "$label" "${seconds:-?}" "$status"
}

read -r -a MODE_LIST <<< "$MODES"
NM=${#MODE_LIST[@]}
for program in $PROGRAMS; do
    echo "== $program =="
    for pass in $(seq 1 "$PASSES"); do
        for k in $(seq 0 $((NM - 1))); do
            run_one "$program" "${MODE_LIST[$(( (k + pass - 1) % NM ))]}" "$pass"
        done
    done
done

echo
echo "Готово."
echo "  прогоны : $RUNS_CSV"
echo "  логи    : $OUT/logs"
