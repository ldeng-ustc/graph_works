# BACH 和 LiveGraph 依赖说明

本文档记录当前工作区里 `LiveGraph/` 和 `BACH/` 在本机上的实际构建阻塞点，以及建议的安装方式。

## 当前已确认的阻塞点

### LiveGraph

- 早期失败点是 `find_package(TBB REQUIRED)`
- 安装 `libtbb-dev` 之后，`TBB` 已可发现
- 但 `LiveGraph` 自带的 `cmake/FindTBB.cmake` 较老，会继续尝试读取旧版头文件 `tbb/tbb_stddef.h`
- 在 Debian bookworm / oneTBB 环境下，可通过给 CMake 传入 `-DTBB_VERSION=2021` 绕过这一步

### BACH

- 最早失败点是：
  - `include/dynamic_bitset` 不存在
  - `include/folly` 不存在
- 这两个目录来自 git submodule
- 在补齐 submodule 后，当前新的失败点是 Folly 依赖链里的 `FastFloat`

## 建议先安装的系统依赖

先更新软件源：

```bash
sudo apt update
```

### LiveGraph 建议依赖

最小建议：

```bash
sudo apt install -y build-essential cmake gcc g++ libtbb-dev
```

说明：

- `build-essential` 提供基础编译工具链
- `cmake` 用于配置构建
- `libtbb-dev` 用于满足 `find_package(TBB REQUIRED)`
- OpenMP 在 GCC 环境下一般由 `gcc/g++` 自带支持

如果你希望显式安装 GCC 12，也可以用：

```bash
sudo apt install -y gcc-12 g++-12
```

### BACH 建议依赖

`BACH/README.md` 明确写了需要 `G++12` 和 `Folly`。结合 Folly 常见构建依赖，建议先装这一组：

```bash
sudo apt install -y \
  build-essential cmake git gcc-12 g++-12 \
  libboost-all-dev libdouble-conversion-dev libevent-dev \
  libgflags-dev libgoogle-glog-dev libssl-dev zlib1g-dev \
  libfmt-dev libunwind-dev libfast-float-dev \
  libsnappy-dev liblz4-dev libzstd-dev libbz2-dev \
  libdwarf-dev binutils-dev libaio-dev liburing-dev libsodium-dev
```

说明：

- `git` 用来拉取 submodule
- `gcc-12 g++-12` 对齐 `BACH` 当前 `CMakeLists.txt` 中硬编码的编译器设置
- `libboost-all-dev`、`libdouble-conversion-dev`、`libevent-dev`、`libgflags-dev`、`libgoogle-glog-dev`、`libssl-dev`、`zlib1g-dev` 是 Folly 常见依赖
- `libfmt-dev`、`libunwind-dev` 在很多 Folly 版本中也经常需要，建议一起安装
- `libfast-float-dev` 是本机当前已经实际触发的缺失依赖
- `libsnappy-dev`、`liblz4-dev`、`libzstd-dev`、`libbz2-dev`、`libdwarf-dev`、`binutils-dev`、`libaio-dev`、`liburing-dev`、`libsodium-dev` 是当前这版 Folly CMake 里继续会探测到的依赖，建议一次补齐

注意：

- `BACH` 当前仓库不是通过系统包链接 Folly，而是通过 `include/folly` 子模块直接 `add_subdirectory`
- 所以系统包解决的是 Folly 的外围依赖，不会替代 git submodule 本身

## BACH 还需要补的 submodule

进入 `BACH/` 后执行：

```bash
git submodule update --init --recursive
```

这一步会补齐：

- `include/folly`
- `include/dynamic_bitset`

如果网络环境有问题，也可以单独检查：

```bash
git submodule status
```

## 建议的安装与构建顺序

### 1. 先装系统依赖

```bash
sudo apt update
sudo apt install -y build-essential cmake git gcc-12 g++-12 \
  libtbb-dev libboost-all-dev libdouble-conversion-dev libevent-dev \
  libgflags-dev libgoogle-glog-dev libssl-dev zlib1g-dev \
  libfmt-dev libunwind-dev
```

### 2. 初始化 BACH submodule

```bash
cd /home/denglong.1997/graph_works/BACH
git submodule update --init --recursive
```

### 3. 为 BACH 选择兼容的 folly 版本

当前机器上，较新的 `folly` 会触发：

- `fast_float::chars_format::allow_leading_plus`
- 更新版 `liburing` 的 API 需求

为了避免修改依赖源码，当前验证可用的做法是把 `folly` 切到：

```bash
cd /home/denglong.1997/graph_works/BACH
git -C include/folly switch --detach v2024.10.28.00
```

### 4. 重新配置 LiveGraph

```bash
cd /home/denglong.1997/graph_works/LiveGraph
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DTBB_VERSION=2021
cmake --build build -j
ctest --test-dir build --output-on-failure
```

### 5. 重新配置 BACH

```bash
cd /home/denglong.1997/graph_works/BACH
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
  -DBoost_NO_BOOST_CMAKE=ON \
  -DBOOST_ROOT=/usr \
  -DBoost_INCLUDE_DIR=/usr/include \
  -DBoost_LIBRARY_DIR_RELEASE=/usr/lib/x86_64-linux-gnu \
  -DBoost_LIBRARY_DIR_DEBUG=/usr/lib/x86_64-linux-gnu
cmake --build build -j
```

## 我当前观察到的实际情况

本机软件源里已经能查到以下包，说明用 `apt install` 理论上是可行的：

- `libtbb-dev`
- `libgoogle-glog-dev`
- `libgflags-dev`
- `libdouble-conversion-dev`
- `libevent-dev`
- `libboost-all-dev`
- `libssl-dev`
- `zlib1g-dev`
- `libfast-float-dev`
- `libsnappy-dev`
- `liblz4-dev`
- `libzstd-dev`
- `libbz2-dev`
- `libdwarf-dev`
- `binutils-dev`
- `libaio-dev`
- `liburing-dev`
- `libsodium-dev`

## 当前进展

- `LiveGraph` 已在本机成功完成配置、编译，并通过全部测试
- `LiveGraph` 当前可用的配置命令是：

```bash
cd /home/denglong.1997/graph_works/LiveGraph
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DTBB_VERSION=2021
cmake --build build -j
ctest --test-dir build --output-on-failure
```

- `BACH` 当前还未完成配置，最新阻塞点是 `FastFloat`
## 当前结果

- `LiveGraph` 已成功构建，并通过 `ctest`
- `BACH` 已成功构建出共享库 `libbach.so`
- `BACH` 还需要额外指定：
  - `folly` 版本：`v2024.10.28.00`
  - Boost CMake 参数：强制使用 `/usr` 下的系统 Boost，绕过 `/usr/local` 中的 `Boost 1.90` 配置

## 新增 benchmark 可执行文件

### LiveGraph

新增文件：

- `LiveGraph/examples/livegraph_bin32_bench.cpp`

构建后可执行文件：

- `LiveGraph/build/examples/livegraph_bin32_bench`

示例运行：

```bash
cd /home/denglong.1997/graph_works/LiveGraph
./build/examples/livegraph_bin32_bench \
  -f /home/denglong.1997/graph_works/data/smoke/tiny.bin32 \
  --storage /home/denglong.1997/graph_works/data/livegraph-bench-smoke \
  --reset --bfs-rounds 3 --pr-iters 5
```

### BACH

新增文件：

- `BACH/bach_bin32_bench.cpp`

构建后可执行文件：

- `BACH/build/bach_bin32_bench`

示例运行：

```bash
cd /home/denglong.1997/graph_works/BACH
./build/bach_bin32_bench \
  -f /home/denglong.1997/graph_works/data/smoke/tiny.bin32 \
  --storage /home/denglong.1997/graph_works/data/bach-bench-smoke \
  --reset --bfs-rounds 3 --pr-iters 5
```

### benchmark 功能

两个新程序都支持：

- 读取 `bin32` 图文件
- 将图导入各自系统的外存目录
- 运行 `BFS`
- 运行 `PageRank`
- 运行 `CC`
- 输出统一的 `[EXPOUT]` 结果字段

## smoke test

我已经用一个 8 条边的小图验证过：

- `LiveGraph` smoke test 成功
- `BACH` smoke test 成功
- 两者得到一致结果：
  - `BFS_Checksum = 18`
  - `PR_Sum = 0.80343750`
  - `CC_Components = 2`

## 后续计划

等依赖安装完成后，我会按你的要求：

1. 先继续跑通 `LiveGraph`
2. 再处理 `BACH`
3. 尽量为两者补一个和 `LSGraph-Bench.cc` / `XPGraph` 类似的 benchmark 可执行入口：
   - 读入 bin32 图
   - 运行 BFS
   - 运行 PR
   - 运行 CC
