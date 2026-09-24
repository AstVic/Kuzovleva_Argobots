#!/bin/bash
# Проверка корректности Argobots-плагина parlaylib во всех режимах планировщика.
# Требует предварительного запуска scripts/setup_pbbsbench.sh.
#
# Переменные окружения: MODES, THREADS, PBBS_DIR.
set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PBBS_DIR="${PBBS_DIR:-$REPO_ROOT/third_party/pbbsbench}"
CONFIG="$PBBS_DIR/common/argobotsConfig"
BUILD="$SCRIPT_DIR/build"
RESULTS="$REPO_ROOT/tests_results/parlay_plugin"

MODES="${MODES:-default randws old new}"
THREADS="${THREADS:-1 2 4 8}"

if [ ! -f "$CONFIG" ]; then
    echo "Не найден $CONFIG. Сначала запустите scripts/setup_pbbsbench.sh." >&2
    exit 1
fi
ABT_DIR=$(awk -F' = ' '$1=="ABT_DIR"{print $2}' "$CONFIG")
WSRT_LIB=$(awk -F' = ' '$1=="WSRT_LIB"{print $2}' "$CONFIG")

mkdir -p "$BUILD" "$RESULTS"
g++ -O2 -std=c++17 -Wall -Wextra -DPARLAY_ARGOBOTS \
    -I"$PBBS_DIR/parlaylib/include" -I"$REPO_ROOT" -I"$REPO_ROOT/workstealing_scheduler" \
    -I"$ABT_DIR/include" \
    -o "$BUILD/test_parlay_plugin" "$SCRIPT_DIR/test_parlay_plugin.cpp" \
    -L"$WSRT_LIB" -Wl,-rpath,"$WSRT_LIB" -lwsrt \
    -L"$ABT_DIR/lib" -Wl,-rpath,"$ABT_DIR/lib" -labt -pthread

SUMMARY="$RESULTS/summary.csv"
echo "mode,threads,result,seconds" > "$SUMMARY"
status=0
for mode in $MODES; do
    for p in $THREADS; do
        log="$RESULTS/${mode}_p${p}.txt"
        start=$(date +%s)
        if ABT_WS_SCHEDULER=$mode PARLAY_NUM_THREADS=$p "$BUILD/test_parlay_plugin" > "$log" 2>&1; then
            result=PASSED
        else
            result=FAILED
            status=1
        fi
        secs=$(( $(date +%s) - start ))
        echo "$mode,$p,$result,$secs" >> "$SUMMARY"
        printf "%-8s p=%-3s %s\n" "$mode" "$p" "$result"
    done
done

echo "Результаты: $RESULTS"
exit $status
