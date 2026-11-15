#!/bin/sh
# 本机 Linux 验证：在 Ubuntu 项目根目录执行 sh scripts/validate_on_ubuntu.sh。
# 不改变系统软件源、不安装软件；缺依赖时由 CMake 明确报告。
set -eu
cmake -S . -B build-host -DBUILD_TESTING=ON -DSTATIC_CXX_RUNTIME=OFF
cmake --build build-host -j4
ctest --test-dir build-host --output-on-failure
