#!/bin/bash
# Загружает pbbsbench на зафиксированной ревизии, подключает к нему
# Argobots-плагин parlaylib и собирает библиотеку рантайма.
#
# Переменные окружения: PBBS_DIR, ARGOBOTS_INSTALL_DIR.
set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PBBS_URL="https://github.com/cmuparlay/pbbsbench.git"
PBBS_COMMIT="3932578095bb73a2b5dbf8102ed5ebe3f5041b19"
PBBS_DIR="${PBBS_DIR:-$REPO_ROOT/third_party/pbbsbench}"
WSRT_BUILD="$REPO_ROOT/build/wsrt"

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

fetch_pbbsbench() {
    if [ -d "$PBBS_DIR/.git" ]; then
        echo "pbbsbench уже загружен: $PBBS_DIR"
        return
    fi
    echo "== загрузка pbbsbench ${PBBS_COMMIT:0:7} =="
    mkdir -p "$PBBS_DIR"
    git -C "$PBBS_DIR" init -q
    git -C "$PBBS_DIR" remote add origin "$PBBS_URL"
    if ! git -C "$PBBS_DIR" fetch -q --depth 1 origin "$PBBS_COMMIT"; then
        git -C "$PBBS_DIR" fetch -q origin
    fi
    git -C "$PBBS_DIR" checkout -q "$PBBS_COMMIT"
    git -C "$PBBS_DIR" submodule update -q --init --recursive
}

apply_patch() {   # apply_patch <каталог> <патч>
    local dir="$1" patch="$2"
    if git -C "$dir" apply --reverse --check "$patch" 2>/dev/null; then
        echo "патч уже применён: $(basename "$patch")"
    else
        git -C "$dir" checkout -q -- .
        git -C "$dir" apply "$patch"
        echo "патч применён: $(basename "$patch")"
    fi
}

build_runtime() {
    echo "== сборка libwsrt =="
    local srcs=() lib
    for src in abt_workstealing_scheduler abt_workstealing_scheduler_cost_aware ws_task ws_runtime; do
        srcs+=("$REPO_ROOT/workstealing_scheduler/$src.c")
    done
    mkdir -p "$WSRT_BUILD"
    if [ "$(uname -s)" = "Darwin" ]; then
        lib="$WSRT_BUILD/libwsrt.dylib"
        gcc -O3 -Wall -Wextra -dynamiclib -install_name @rpath/libwsrt.dylib \
            -I"$ABT_DIR/include" -o "$lib" "${srcs[@]}" -L"$ABT_DIR/lib" -labt
    else
        lib="$WSRT_BUILD/libwsrt.so"
        gcc -O3 -Wall -Wextra -shared -fPIC \
            -I"$ABT_DIR/include" -o "$lib" "${srcs[@]}" \
            -L"$ABT_DIR/lib" -Wl,-rpath,"$ABT_DIR/lib" -labt
    fi
    WSRT_LIB_FILE="$lib"
}

write_config() {
    cat > "$PBBS_DIR/common/argobotsConfig" <<EOF
WSRT_ROOT = $REPO_ROOT
WSRT_LIB = $WSRT_BUILD
ABT_DIR = $ABT_DIR
EOF
}

resolve_argobots
fetch_pbbsbench
apply_patch "$PBBS_DIR" "$REPO_ROOT/patches/pbbsbench.patch"
apply_patch "$PBBS_DIR/parlaylib" "$REPO_ROOT/patches/parlaylib.patch"
build_runtime
write_config

echo
echo "Готово."
echo "  pbbsbench : $PBBS_DIR"
echo "  argobots  : $ABT_DIR"
echo "  libwsrt   : $WSRT_LIB_FILE"
