#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build}"
BUILD_TYPE="${BUILD_TYPE:-Debug}"

# On macOS, default to Apple's clang. Homebrew LLVM's libc++ and the OS
# unwinder disagree, so exceptions are silently swallowed (see CMakeLists.txt).
# Respects caller-supplied CC/CXX if either is already set.
if [[ "$(uname -s)" == "Darwin" && -z "${CC:-}" && -z "${CXX:-}" ]]; then
  export CC=/usr/bin/clang
  export CXX=/usr/bin/clang++
fi

if command -v ninja >/dev/null 2>&1; then
  GENERATOR="${GENERATOR:-Ninja}"
else
  GENERATOR="${GENERATOR:-Unix Makefiles}"
fi

DEPS_DIR="$PROJECT_ROOT/.deps"

generator_mismatch() {
  local cache="$1"
  [[ -f "$cache" ]] || return 1
  local existing
  existing="$(grep -E '^CMAKE_GENERATOR:' "$cache" | cut -d= -f2- || true)"
  [[ -n "$existing" && "$existing" != "$GENERATOR" ]]
}

if generator_mismatch "$BUILD_DIR/CMakeCache.txt"; then
  echo "Generator changed in $BUILD_DIR; cleaning"
  rm -rf "$BUILD_DIR"
fi

if [[ -d "$DEPS_DIR" ]]; then
  for cache in "$DEPS_DIR"/*-subbuild/CMakeCache.txt; do
    if generator_mismatch "$cache"; then
      echo "Generator changed in FetchContent cache; cleaning $DEPS_DIR"
      rm -rf "$DEPS_DIR"
      break
    fi
  done
fi

cmake -S "$PROJECT_ROOT" -B "$BUILD_DIR" -G "$GENERATOR" -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
cmake --build "$BUILD_DIR" -j
