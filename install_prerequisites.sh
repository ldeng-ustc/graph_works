#!/bin/bash

set -euo pipefail

sudo apt update
sudo apt install -y \
  libdouble-conversion-dev \
  libfmt-dev \
  libfast-float-dev \
  libgflags-dev \
  libgoogle-glog-dev \
  liburing-dev \
  liblz4-dev \
  libsnappy-dev \
  libsodium-dev \
  libunwind-dev \
  libaio-dev
