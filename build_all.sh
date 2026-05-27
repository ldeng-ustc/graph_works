#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
JOBS="$(nproc)"

# Build Aster
pushd "$SCRIPT_DIR/Aster" >/dev/null
cmake -S . -B build
cmake --build build --parallel "$JOBS"
popd >/dev/null

# Build BACH
pushd "$SCRIPT_DIR/BACH" >/dev/null
cmake -S . -B build
cmake --build build --parallel "$JOBS"
popd >/dev/null

# Build GraphOne
pushd "$SCRIPT_DIR/GraphOne" >/dev/null
cmake -S . -B build
cmake --build build --parallel "$JOBS"
popd >/dev/null

# Build XPGraph
pushd "$SCRIPT_DIR/XPGraph" >/dev/null
make -j"$JOBS"
popd >/dev/null

# Build LSGraph
pushd "$SCRIPT_DIR/LSGraph" >/dev/null
make -j"$JOBS"
popd >/dev/null

# Build LSMGraph
pushd "$SCRIPT_DIR/LSMGraph" >/dev/null
cmake -S . -B build
cmake --build build --parallel "$JOBS"
popd >/dev/null

# Build LiveGraph
pushd "$SCRIPT_DIR/LiveGraph" >/dev/null
cmake -S . -B build
cmake --build build --parallel "$JOBS"
popd >/dev/null
