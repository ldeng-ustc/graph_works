#!/bin/bash

# Build GraphOne
pushd GraphOne
cmake -S . -B build
cmake --build build
popd

# Build XPGraph
pushd XPGraph
make
popd

# Build LSGraph
pushd LSGraph
make
popd