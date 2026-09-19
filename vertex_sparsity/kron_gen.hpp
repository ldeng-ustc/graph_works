// 顶点稀疏性实验（M1）的共享工具头文件。
//
// 作用：在内存里用 Kronecker 生成器直接产生固定条数的边（默认 1 亿），
// 顶点范围由 logN 决定。所有边只放在内存的数组里，不写外存，避免干扰
// 机器上正在跑的其它实验。各系统的 bench 引用本头文件拿到同一份边集，
// 再各自转成自己需要的输入格式。
//
// 另外提供读取进程常驻内存（RSS）的辅助函数，用来观测数据结构的内存开销。

#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <unistd.h>
#include <vector>

// 生成器是 C 代码，用 extern "C" 引入。
extern "C" {
#include "graph_generator.h"
#include "utils.h"
}

namespace vsparsity {

// 机器的 core_pattern 会把大进程的 core 写到共享盘。容量边界实验可能因
// 内存映射失败退出，因此在 bench 入口同时关闭 dumpable 和 core size。
inline void disable_core_dumps() {
  prctl(PR_SET_DUMPABLE, 0, 0, 0, 0);
  rlimit core_limit{0, 0};
  setrlimit(RLIMIT_CORE, &core_limit);
}

// 一条边只保留源点和目标点。logN <= 30 时顶点号 < 2^31，用 uint32_t 足够。
struct SimpleEdge {
  uint32_t src;
  uint32_t dst;
};

// 生成 num_edges 条边，顶点范围是 [0, 2^logN)。
// 结果写入 out（函数内部 resize）。生成阶段用生成器自带的并行，
// 这部分不计入被测系统的 ingest 时间。
inline void gen_kron_edges(int logN, int64_t num_edges,
                           std::vector<SimpleEdge>& out, uint64_t seed1 = 1,
                           uint64_t seed2 = 2) {
  uint_fast32_t seed[5];
  make_mrg_seed(seed1, seed2, seed);

  out.resize(static_cast<size_t>(num_edges));

  // 分块生成，每块最多 2^26 条边，控制临时缓冲的峰值内存。
  const int64_t block_size = 1LL << 26;
  std::vector<packed_edge> buf(static_cast<size_t>(block_size));

  for (int64_t start = 0; start < num_edges; start += block_size) {
    int64_t end = start + block_size;
    if (end > num_edges) end = num_edges;
    generate_kronecker_range(seed, logN, start, end, buf.data());
    for (int64_t i = start; i < end; i++) {
      const packed_edge* p = &buf[static_cast<size_t>(i - start)];
      out[static_cast<size_t>(i)].src =
          static_cast<uint32_t>(get_v0_from_edge(p));
      out[static_cast<size_t>(i)].dst =
          static_cast<uint32_t>(get_v1_from_edge(p));
    }
  }
}

// 返回当前进程常驻内存字节数（RSS），读 /proc/self/statm 的第二个字段。
inline size_t get_rss_bytes() {
  FILE* f = fopen("/proc/self/statm", "r");
  if (!f) return 0;
  long size_pages = 0, resident_pages = 0;
  if (fscanf(f, "%ld %ld", &size_pages, &resident_pages) != 2) {
    fclose(f);
    return 0;
  }
  fclose(f);
  return static_cast<size_t>(resident_pages) *
         static_cast<size_t>(sysconf(_SC_PAGESIZE));
}

// 返回进程历史峰值常驻内存（VmHWM），读 /proc/self/status。
inline size_t get_peak_rss_bytes() {
  FILE* f = fopen("/proc/self/status", "r");
  if (!f) return 0;
  char line[256];
  size_t kb = 0;
  while (fgets(line, sizeof(line), f)) {
    if (sscanf(line, "VmHWM: %zu kB", &kb) == 1) break;
  }
  fclose(f);
  return kb * 1024;
}

}  // namespace vsparsity
