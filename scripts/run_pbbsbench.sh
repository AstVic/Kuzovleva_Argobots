#!/bin/bash
# Сравнение планировщиков на наборе pbbsbench.
#
# Для каждой задачи из runall (основные параллельные реализации) бенчмарк
# собирается во всех нужных вариантах, после чего режимы запускаются по кругу
# PASSES раз с поворотом порядка, чтобы дрейф машины (нагрев, фоновая нагрузка)
# не превращался в разницу между режимами. Результат каждого запуска сверяется
# программой *Check из pbbsbench.
#
# Переменные окружения:
#   MODES    - режимы: sequential homegrown openmp default randws randws_fifo old new;
#              вариант режима задаётся как метка:режим:ПЕРЕМЕННАЯ=значение,...,
#              например new_base:new:WS_FALLBACK_STEAL_ONE=0
#   THREADS  - число рабочих потоков (по умолчанию все ядра)
#   SIZE     - small | full
#   ONLY     - список реализаций через пробел, например "histogram/parallel"
#   ROUNDS   - повторов на один вход внутри одного запуска
#   PASSES   - число проходов по всем режимам
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
PASSES="${PASSES:-3}"

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
    echo "params: MODES='$MODES' THREADS=$THREADS SIZE=$SIZE ROUNDS=$ROUNDS PASSES=$PASSES"
    echo "benchmarks: $BENCHES"
} > "$ENVFILE"
sed 's/^/  /' "$ENVFILE"

echo "mode,threads,benchmark,input,pass,geomean_seconds,times,status" > "$RUNS_CSV"

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
        sequential) echo sequential ;;
        homegrown) echo homegrown ;;
        openmp) echo openmp ;;
        *) echo argobots ;;
    esac
}

build() {   # build <каталог бенчмарка> <семейство>
    local dir="$1" family="$2"
    case "$family" in
        sequential) (cd "$dir" && make -s cleanall >/dev/null 2>&1; SEQUENTIAL=1 make -s) ;;
        homegrown) (cd "$dir" && make -s cleanall >/dev/null 2>&1; make -s) ;;
        openmp)    (cd "$dir" && make -s cleanall >/dev/null 2>&1; OPENMP=1 make -s) ;;
        argobots)  (cd "$dir" && make -s cleanall >/dev/null 2>&1; ARGOBOTS=1 make -s) ;;
    esac
}

save_binaries() {   # save_binaries <каталог> <семейство> <метка времени>
    local dir="$1" family="$2" stamp="$3" f
    mkdir -p "$dir/.bin_$family"
    find "$dir" -maxdepth 1 -type f -perm -u+x -newer "$stamp" | while read -r f; do
        cp "$f" "$dir/.bin_$family/"
    done
    [ -n "$(ls -A "$dir/.bin_$family")" ]
}

install_binaries() {   # install_binaries <каталог> <семейство>
    local dir="$1" family="$2" f name
    for f in "$dir/.bin_$family"/*; do
        name=$(basename "$f")
        cp "$f" "$dir/.$name.tmp"
        mv -f "$dir/.$name.tmp" "$dir/$name"
    done
}

run_mode() {   # run_mode <каталог> <бенчмарк> <режим> <проход>
    local dir="$1" bench="$2" spec="$3" pass="$4" log status=OK mode label
    mode=$(spec_mode "$spec")
    label=$(spec_label "$spec")
    log="$OUT/logs/${bench//\//_}_${label}_p${pass}.txt"
    local stats="$OUT/logs/${bench//\//_}_${label}_p${pass}.stats"
    local envs=()
    case "$mode" in
        sequential|homegrown) ;;
        openmp) envs=(OPENMP=1) ;;
        *) envs=(ABT_WS_SCHEDULER="$mode" WS_STATS="$stats") ;;
    esac
    for v in $(spec_env "$spec"); do envs+=("$v"); done
    install_binaries "$dir" "$(family_of "$spec")"
    if ! (cd "$dir" && env ${envs[@]+"${envs[@]}"} "$INPUTS" -r "$ROUNDS" -p "$THREADS" -k) > "$log" 2>&1 \
        || grep -q "TERMINATED ABNORMALLY" "$log"; then
        status=FAILED
    fi
    python3 - "$log" "$label" "$THREADS" "$bench" "$pass" "$status" >> "$RUNS_CSV" <<'PY'
import re, sys
log, mode, p, bench, npass, status = sys.argv[1:]
rows = 0
for line in open(log):
    m = re.match(r"(\S+) : .* : (.*), geomean = ([0-9.]+)", line)
    if m:
        times = m.group(2).replace("'", "").replace(",", "").split()
        print(f"{mode},{p},{bench},{m.group(1)},{npass},{m.group(3)},{' '.join(times)},{status}")
        rows += 1
if rows == 0:
    print(f"{mode},{p},{bench},-,{npass},,,FAILED")
PY
    printf "  %-3s %-16s %s\n" "$pass" "$label" "$status"
}

for bench in $BENCHES; do
    dir="$PBBS_DIR/benchmarks/$bench"
    echo "== $bench =="
    rm -rf "$dir"/.bin_*
    ready=()
    for family in sequential homegrown openmp argobots; do
        modes=()
        for mode in $MODES; do
            [ "$(family_of "$mode")" = "$family" ] && modes+=("$mode")
        done
        [ ${#modes[@]} -eq 0 ] && continue
        stamp="$OUT/logs/.stamp"
        touch "$stamp"
        sleep 1
        if ! build "$dir" "$family" > "$OUT/logs/${bench//\//_}_build_${family}.txt" 2>&1 \
            || ! save_binaries "$dir" "$family" "$stamp"; then
            for mode in "${modes[@]}"; do
                echo "$(spec_label "$mode"),$THREADS,$bench,-,,,,BUILD_FAILED" >> "$RUNS_CSV"
                printf "  %-3s %-16s BUILD_FAILED\n" "-" "$(spec_label "$mode")"
            done
            continue
        fi
        ready+=("${modes[@]}")
    done
    n=${#ready[@]}
    [ "$n" -eq 0 ] && continue
    for ((pass = 1; pass <= PASSES; pass++)); do
        for ((i = 0; i < n; i++)); do
            run_mode "$dir" "$bench" "${ready[$(( (i + pass - 1) % n ))]}" "$pass"
        done
    done
    rm -rf "$dir"/.bin_*
done

echo
echo "Готово."
echo "  окружение : $ENVFILE"
echo "  прогоны   : $RUNS_CSV"
echo "  логи      : $OUT/logs"
