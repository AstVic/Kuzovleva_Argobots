#!/bin/bash
# Сборка программ из benchmarks/ во всех вариантах:
#   seq - исходная последовательная версия,
#   omp  - исходная OpenMP-версия,
#   bolt - та же OpenMP-версия с библиотекой BOLT (OpenMP поверх Argobots) вместо libgomp,
#   llvm - та же OpenMP-версия с библиотекой OpenMP из LLVM на потоках ОС (BOLT без Argobots),
#   ws   - версия на API рантайма (ws_parallel.h).
#
# Переменные окружения: CG_CLASSES, BT_CLASSES, ARGOBOTS_INSTALL_DIR, BOLT_INSTALL_DIR,
# LLVM_OMP_INSTALL_DIR, CC, CLANG.
# Варианты bolt и llvm собираются clang с -fnoopenmp-use-tls, если найдены clang и библиотека
# (BOLT_INSTALL_DIR или ~/bolt-install, LLVM_OMP_INSTALL_DIR или ~/llvm-omp-install): потоки
# OpenMP в BOLT - это ULT, они переходят между потоками ОС, поэтому threadprivate-переменные
# должен хранить рантайм OpenMP, а не память потока ОС.
# Результат: benchmarks/build/<программа>/<вариант>/...
set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SCHED="$REPO_ROOT/workstealing_scheduler"
BUILD="$SCRIPT_DIR/build"
CG_CLASSES="${CG_CLASSES:-S W A B C}"
BT_CLASSES="${BT_CLASSES:-S W A B}"
CC="${CC:-cc}"
CFLAGS="-std=gnu11 -O3 -ffp-contract=off -w"

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

resolve_openmp() {
    if [ "$(uname -s)" = "Darwin" ]; then
        local libomp
        libomp="$(brew --prefix libomp 2>/dev/null || true)"
        if [ -z "$libomp" ] || [ ! -d "$libomp/lib" ]; then
            echo "libomp не найден (brew install libomp): OpenMP-варианты пропускаются" >&2
            OMP_CFLAGS=""
            return
        fi
        OMP_CFLAGS="-Xpreprocessor -fopenmp -I$libomp/include"
        OMP_LIBS="-L$libomp/lib -Wl,-rpath,$libomp/lib -lomp"
    else
        OMP_CFLAGS="-fopenmp"
        OMP_LIBS="-fopenmp"
    fi
}

find_omp_lib() {
    local dir
    for dir in "$@"; do
        if [ -n "$dir" ] && [ -f "$dir/lib/libomp.so" ]; then
            echo "$dir"
            return
        fi
    done
}

resolve_bolt() {
    CLANG="${CLANG:-$(command -v clang || true)}"
    BOLT_DIR="$(find_omp_lib "${BOLT_INSTALL_DIR:-}" "$HOME/bolt-install")"
    LLVM_OMP_DIR="$(find_omp_lib "${LLVM_OMP_INSTALL_DIR:-}" "$HOME/llvm-omp-install")"
    if [ -z "$CLANG" ] && { [ -n "$BOLT_DIR" ] || [ -n "$LLVM_OMP_DIR" ]; }; then
        echo "clang не найден: варианты bolt и llvm пропускаются" >&2
        BOLT_DIR=""
        LLVM_OMP_DIR=""
    fi
}

resolve_argobots
resolve_openmp
resolve_bolt
WS_SRC=("$SCHED/ws_parallel.c" "$SCHED/ws_runtime.c" "$SCHED/ws_task.c"
        "$SCHED/abt_workstealing_scheduler.c" "$SCHED/abt_workstealing_scheduler_cost_aware.c")
WS_CFLAGS="-I$SCHED -I$ABT_DIR/include"
WS_LIBS="-L$ABT_DIR/lib -Wl,-rpath,$ABT_DIR/lib -labt -lpthread"

build_variant() {
    local variant="$1" out="$2"
    shift 2
    case "$variant" in
        seq) $CC $CFLAGS "$@" -o "$out" -lm ;;
        omp)
            if [ -z "$OMP_CFLAGS" ]; then return; fi
            # shellcheck disable=SC2086
            $CC $CFLAGS $OMP_CFLAGS "$@" -o "$out" $OMP_LIBS -lm ;;
        bolt|llvm)
            local libdir="$BOLT_DIR"
            if [ "$variant" = "llvm" ]; then libdir="$LLVM_OMP_DIR"; fi
            if [ -z "$libdir" ]; then return; fi
            "$CLANG" $CFLAGS -fopenmp -fnoopenmp-use-tls -I"$libdir/include" "$@" -o "$out" \
                -L"$libdir/lib" -Wl,--disable-new-dtags,-rpath,"$libdir/lib" -lomp -lm ;;
        ws)
            # shellcheck disable=SC2086
            $CC $CFLAGS $WS_CFLAGS "$@" "${WS_SRC[@]}" -o "$out" $WS_LIBS -lm ;;
    esac
}

build_cg() {
    local dir="$SCRIPT_DIR/cg" class out
    for class in $CG_CLASSES; do
        out="$BUILD/cg/$class"
        mkdir -p "$out"
        cp "$dir/params/npbparams_$class.h" "$out/npbparams.h"
        for variant in seq omp bolt llvm ws; do
            local extra=() srcv="$variant"
            if [ "$variant" = "bolt" ] || [ "$variant" = "llvm" ]; then srcv=omp; fi
            if [ "$srcv" != "omp" ]; then extra=(-include "$dir/common/omp_serial.h"); fi
            build_variant "$variant" "$out/cg.$variant" ${extra[@]+"${extra[@]}"} -I"$out" -I"$dir" -I"$dir/common" \
                "$dir/$srcv/cg.c" "$dir/common/c_timers.c" "$dir/common/print_results.c" \
                "$dir/common/randdp.c" "$dir/common/wtime.c"
        done
        echo "cg $class: $(ls "$out" | grep -v '\.h$' | tr '\n' ' ')"
    done
}

build_bt() {
    local dir="$SCRIPT_DIR/bt-mz" class out src variant files
    for class in $BT_CLASSES; do
        out="$BUILD/bt-mz/$class"
        mkdir -p "$out"
        cp "$dir/params/npbparams_$class.h" "$out/npbparams.h"
        for variant in seq omp bolt llvm ws; do
            files=()
            src="$dir/original"
            for f in "$src"/*.c; do
                if [ "$variant" = "ws" ] && [ -f "$dir/ws/$(basename "$f")" ]; then
                    files+=("$dir/ws/$(basename "$f")")
                else
                    files+=("$f")
                fi
            done
            local extra=()
            case "$variant" in omp|bolt|llvm) ;; *) extra=(-I"$dir/serial") ;; esac
            build_variant "$variant" "$out/bt-mz.$variant" ${extra[@]+"${extra[@]}"} -I"$out" -I"$src" -I"$dir/common" \
                "${files[@]}" "$dir/common/c_print_results.c" "$dir/common/c_timers.c" \
                "$dir/common/wtime.c"
        done
        echo "bt-mz $class: $(ls "$out" | grep -v '\.h$' | tr '\n' ' ')"
    done
}

build_hetero() {
    local src="$SCRIPT_DIR/hetero/hetero.c" out="$BUILD/hetero"
    mkdir -p "$out"
    build_variant seq "$out/hetero.seq" "$src"
    build_variant omp "$out/hetero.omp" "$src"
    build_variant bolt "$out/hetero.bolt" "$src"
    build_variant llvm "$out/hetero.llvm" "$src"
    build_variant ws "$out/hetero.ws" -DHETERO_WS "$src"
    echo "hetero: $(ls "$out" | tr '\n' ' ')"
}

build_cg
if [ -d "$SCRIPT_DIR/bt-mz" ]; then build_bt; fi
build_hetero
