// LSGraph 顶点稀疏性 bench。
//
// 目的：固定边数（默认 1 亿），逐步增大顶点范围（logN），观测 LSGraph 这种
// 纯内存图结构的 ingest 吞吐和内存开销随顶点数的变化。LSGraph 在构造时按
// 顶点数分配 per 顶点的块，因此顶点越多、空置块越多，内存随 |V| 线性增长。
//
// 边在内存里用 Kronecker 生成器直接产生，不读写外存。只做 ingest，不跑算法。
// 单线程运行（运行时设 CILK_NWORKERS=1）。

#define ENABLE_LOCK 1
#define WEIGHTED 0
#define VERIFY 0

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <tuple>
#include <vector>

#include "LSGraph.h"
#include "parallel.h"
#include "parse_command_line.h"

#include "kron_gen.hpp"

using namespace graphstore;

#define EXPOUT "[EXPOUT]"

int main(int argc, char** argv) {
  vsparsity::disable_core_dumps();
  commandLine P(argc, argv, "./lsgraph_vsparsity -n logN -e num_edges -bs batch");
  int logN = P.getOptionLongValue("-n", 23);
  int64_t num_edges = P.getOptionLongValue("-e", 100000000);
  size_t batch_size = P.getOptionLongValue("-bs", 4096);
  uint32_t num_nodes = (logN >= 31) ? 0xFFFFFFFFu : (1u << logN);

  printf("Num workers: %ld\n", getWorkers());
  printf("logN=%d num_vertices=%u num_edges=%ld batch=%zu\n", logN, num_nodes,
         (long)num_edges, batch_size);

  // 1) 内存生成边集。
  std::vector<vsparsity::SimpleEdge> edges;
  vsparsity::gen_kron_edges(logN, num_edges, edges);
  size_t rss_after_gen = vsparsity::get_rss_bytes();
  printf("Generated %zu edges, rss=%zuMB\n", edges.size(), rss_after_gen >> 20);

  // 2) 构造空图并 ingest，按 batch 逐批插入。载入时间从图结构初始化开始，
  // 因为顶点稀疏性的主要代价正是按顶点分配和触碰元数据。
  auto ts_begin = std::chrono::high_resolution_clock::now();
  LSGraph g(num_nodes);
  size_t rss_after_alloc = vsparsity::get_rss_bytes();

  using Sequence = parlay::sequence<std::tuple<uint32_t, uint32_t>>;
  size_t batchs = (edges.size() + batch_size - 1) / batch_size;

  for (size_t b = 0; b < batchs; b++) {
    size_t start = b * batch_size;
    size_t sz = std::min(batch_size, edges.size() - start);
    Sequence batch(sz);
    for (size_t j = 0; j < sz; j++) {
      batch[j] = std::make_tuple(edges[start + j].src, edges[start + j].dst);
    }
    g.add_edge_batch_sort(batch, batch.size(), num_nodes);
  }
  auto ts_end = std::chrono::high_resolution_clock::now();

  size_t rss_after_ingest = vsparsity::get_rss_bytes();
  size_t peak_rss = vsparsity::get_peak_rss_bytes();
  double t_ingest = std::chrono::duration<double>(ts_end - ts_begin).count();
  double mtps = num_edges / t_ingest / 1e6;

  // 结构内存 = ingest 后 RSS 减去生成的边集占用（用生成后、建图前的 RSS 近似）。
  long struct_bytes = (long)rss_after_ingest - (long)rss_after_gen;
  if (struct_bytes < 0) struct_bytes = 0;

  printf(EXPOUT "System: LSGraph\n");
  printf(EXPOUT "logN: %d\n", logN);
  printf(EXPOUT "Vertex count: %u\n", num_nodes);
  printf(EXPOUT "Edge count: %ld\n", (long)num_edges);
  printf(EXPOUT "Ingest: %.4f\n", t_ingest);
  printf(EXPOUT "Throughput_MEPS: %.4f\n", mtps);
  printf(EXPOUT "RSS_after_gen: %zu\n", rss_after_gen);
  printf(EXPOUT "RSS_after_alloc: %zu\n", rss_after_alloc);
  printf(EXPOUT "RSS_after_ingest: %zu\n", rss_after_ingest);
  printf(EXPOUT "RSS_peak: %zu\n", peak_rss);
  printf(EXPOUT "RSS_struct: %ld\n", struct_bytes);
  return 0;
}
