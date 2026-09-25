#!/bin/bash
# Сравнение планировщиков на наборе pbbsbench.
#
# Для каждой задачи из runall (основные параллельные реализации) бенчмарк
# собирается в трёх вариантах: родной планировщик parlay, OpenMP и Argobots,
# затем запускается во всех режимах подряд, чтобы нагрев машины не превращался
# в разницу между режимами. Результат каждого запуска сверяется программой
# *Check из pbbsbench.
#
# Переменные окружения:
#   MODES    - режимы: homegrown openmp default randws old new;
#              вариант режима задаётся как метка:режим:ПЕРЕМЕННАЯ=значение,...,
#              например new_base:new:WS_FALLBACK_STEAL_ONE=0
#   THREADS  - число рабочих потоков (по умолчанию все ядра)
#   SIZE     - small | full
#   ONLY     - список реализаций через пробел, например "histogram/parallel"
#   ROUNDS   - повторов на один вход
#   PBBS_DIR
set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PBBS_DIR="${PBBS_DIR:-$REPO_ROOT/third_party/pbbsbench}"

detect_cpus() {
    if command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu >/dev/null 2>&1; then
        sysctl -n hw.ncpu
    else
        nproc
    fi
}

MODES="${MODES:-homegrown openmp randws old new}"
THREADS="${THREADS:-$(detect_cpus)}"
SIZE="${SIZE:-small}"
ROUNDS="${ROUNDS:-3}"

if [ ! -f "$PBBS_DIR/common/argobotsConfig" ]; then
    echo "pbbsbench не подготовлен. Сначала запустите scripts/setup_pbbsbench.sh." >&2
    exit 1
fi

if [ -n "${ONLY:-}" ]; then
    BENCHES="$ONLY"
else
    BENCHES=$(python3 - "$PBBS_DIR/runall" <<'PY'
import ast, re, sys
src = open(sys.argv[1]).read()
body = src[src.index("tests = ["):]
body = body[:body.index("\n]\n") + 3]
tests = ast.literal_eval(re.sub(r"#.*", "", body.split("=", 1)[1]))
print(" ".join(t[0] for t in tests if t[1] and int(t[2]) == 0))
PY
)
fi

if [ "$SIZE" = "full" ]; then INPUTS=./testInputs; else INPUTS=./testInputs_small; fi

OUT="$REPO_ROOT/tests_results/pbbsbench/$(date -u +%Y%m%d_%H%M%S)"
mkdir -p "$OUT/logs"
RUNS_CSV="$OUT/runs.csv"
ENVFILE="$OUT/environment.txt"

{
    echo "date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "host: $(uname -srm)"
    if command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu >/dev/null 2>&1; then
        echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo n/a)"
        echo "cores total: $(sysctl -n hw.ncpu)"
        echo "cores performance: $(sysctl -n hw.perflevel0.logicalcpu 2>/dev/null || echo n/a)"
        echo "cores efficiency: $(sysctl -n hw.perflevel1.logicalcpu 2>/dev/null || echo n/a)"
        echo "power: $(pmset -g batt 2>/dev/null | head -1)"
    else
        echo "cores: $(nproc)"
    fi
    echo "compiler: $(g++ --version | head -1)"
    echo "git: $(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo n/a) $(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo n/a)"
    echo "pbbsbench: $(git -C "$PBBS_DIR" rev-parse --short HEAD 2>/dev/null || echo n/a)"
    echo "params: MODES='$MODES' THREADS=$THREADS SIZE=$SIZE ROUNDS=$ROUNDS"
    echo "benchmarks: $BENCHES"
} > "$ENVFILE"
sed 's/^/  /' "$ENVFILE"

echo "mode,threads,benchmark,input,geomean_seconds,times,status" > "$RUNS_CSV"

spec_label() { echo "${1%%:*}"; }

spec_mode() {
    local rest="${1#*:}"
    if [ "$rest" = "$1" ]; then echo "$1"; else echo "${rest%%:*}"; fi
}

spec_env() {
    local rest="${1#*:}"
    [ "$rest" = "$1" ] && return
    local vars="${rest#*:}"
    [ "$vars" = "$rest" ] && return
    echo "$vars" | tr ',' ' '
}

family_of() {
    case "$(spec_mode "$1")" in
        homegrown) echo homegrown ;;
        openmp) echo openmp ;;
        *) echo argobots ;;
    esac
}

build() {   # build <каталог бенчмарка> <семейство>
    local dir="$1" family="$2"
    case "$family" in
        homegrown) (cd "$dir" && make -s cleanall >/dev/null 2>&1; make -s) ;;
        openmp)    (cd "$dir" && make -s cleanall >/dev/null 2>&1; OPENMP=1 make -s) ;;
        argobots)  (cd "$dir" && make -s cleanall >/dev/null 2>&1; ARGOBOTS=1 make -s) ;;
    esac
}

run_mode() {   # run_mode <каталог> <бенчмарк> <режим>
    local dir="$1" bench="$2" spec="$3" log status=OK mode label
    mode=$(spec_mode "$spec")
    label=$(spec_label "$spec")
    log="$OUT/logs/${bench//\//_}_${label}.txt"
    local envs=()
    case "$mode" in
        homegrown) ;;
        openmp) envs=(OPENMP=1) ;;
        *) envs=(ABT_WS_SCHEDULER="$mode") ;;
    esac
    for v in $(spec_env "$spec"); do envs+=("$v"); done
    if ! (cd "$dir" && env ${envs[@]+"${envs[@]}"} "$INPUTS" -r "$ROUNDS" -p "$THREADS" -k) > "$log" 2>&1 \
        || grep -q "TERMINATED ABNORMALLY" "$log"; then
        status=FAILED
    fi
    python3 - "$log" "$label" "$THREADS" "$bench" "$status" >> "$RUNS_CSV" <<'PY'
import re, sys
log, mode, p, bench, status = sys.argv[1:]
for line in open(log):
    m = re.match(r"(\S+) : .* : (.*), geomean = ([0-9.]+)", line)
    if m:
        times = m.group(2).replace("'", "").replace(",", "").split()
        print(f"{mode},{p},{bench},{m.group(1)},{m.group(3)},{' '.join(times)},{status}")
PY
    printf "  %-10s %s\n" "$label" "$status"
}

for bench in $BENCHES; do
    dir="$PBBS_DIR/benchmarks/$bench"
    echo "== $bench =="
    for family in homegrown openmp argobots; do
        modes=()
        for mode in $MODES; do
            [ "$(family_of "$mode")" = "$family" ] && modes+=("$mode")
        done
        [ ${#modes[@]} -eq 0 ] && continue
        if ! build "$dir" "$family" > "$OUT/logs/${bench//\//_}_build_${family}.txt" 2>&1; then
            for mode in "${modes[@]}"; do
                echo "$(spec_label "$mode"),$THREADS,$bench,-,,,BUILD_FAILED" >> "$RUNS_CSV"
                printf "  %-10s BUILD_FAILED\n" "$(spec_label "$mode")"
            done
            continue
        fi
        for mode in "${modes[@]}"; do
            run_mode "$dir" "$bench" "$mode"
        done
    done
done

echo
echo "Готово."
echo "  окружение : $ENVFILE"
echo "  прогоны   : $RUNS_CSV"
echo "  логи      : $OUT/logs"
