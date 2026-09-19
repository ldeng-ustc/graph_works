// BACH 顶点稀疏性 bench（禁 flush）。
//
// 目的：固定边数（默认 1 亿），逐步增大顶点范围（logN），观测 BACH 的内存
// 部分（memtable）在只做 ingest 时，吞吐和内存随顶点数的变化。BACH 按顶点
// 分组管理边索引，顶点数增大会带来更多分组和 per 顶点结构，内存随 |V| 增长。
//
// 为了测出纯内存 ingest 的时间，把触发 flush 的阈值调到极大，让全部边都留在
// 内存里。边在内存里用 Kronecker 生成器直接产生，不读外存。单线程 ingest。

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "BACH/BACH.h"

#include "kron_gen.hpp"

namespace fs = std::filesystem;

#define EXPOUT "[EXPOUT]"

namespace {

struct Args {
  int logN = 23;
  int64_t num_edges = 100000000;
  std::string storage_dir = "./output/bach/vsparsity";
};

Args parse_args(int argc, char** argv) {
  Args a;
  for (int i = 1; i < argc; ++i) {
    std::string s = argv[i];
    if (s == "-n" && i + 1 < argc) a.logN = std::atoi(argv[++i]);
    else if (s == "-e" && i + 1 < argc) a.num_edges = std::atoll(argv[++i]);
    else if (s == "--storage" && i + 1 < argc) a.storage_dir = argv[++i];
  }
  return a;
}

}  // namespace

int main(int argc, char** argv) {
  vsparsity::disable_core_dumps();
  setvbuf(stdout, nullptr, _IONBF, 0);
  Args args = parse_args(argc, argv);
  uint32_t num_nodes = (args.logN >= 31) ? 0xFFFFFFFFu : (1u << args.logN);
  printf("logN=%d num_vertices=%u num_edges=%ld\n", args.logN, num_nodes,
         (long)args.num_edges);

  // 每次运行清空存储目录，避免旧数据干扰。
  if (fs::exists(args.storage_dir)) fs::remove_all(args.storage_dir);
  fs::create_directories(args.storage_dir);

  // 1) 内存生成边集。
  std::vector<vsparsity::SimpleEdge> edges;
  vsparsity::gen_kron_edges(args.logN, args.num_edges, edges);
  size_t rss_after_gen = vsparsity::get_rss_bytes();
  printf("Generated %zu edges, rss=%zuMB\n", edges.size(), rss_after_gen >> 20);

  // 2) 打开 BACH，把 flush 相关阈值调到极大以禁用 flush。ingest 由下面的
  // 串行循环保证单线程，这里的线程数选项只影响内部 epoch 表和后台线程，
  // 保留默认值即可（设为 1 会让内部并发表尺寸退化为 0）。
  auto options = std::make_shared<BACH::Options>();
  options->STORAGE_DIR = args.storage_dir;
  options->MEM_TABLE_MAX_SIZE = (size_t)1 << 60;
  options->LEVEL_0_MAX_SIZE = (size_t)1 << 60;
  options->VERTEX_PROPERTY_MAX_SIZE = (size_t)1 << 60;

  // 载入时间从图结构初始化开始，包含按顶点建立元数据的成本。
  auto ts_begin = std::chrono::high_resolution_clock::now();
  auto db = std::make_unique<BACH::DB>(options);
  BACH::label_t vertex_label = db->AddVertexLabel("v");
  BACH::label_t edge_label = db->AddEdgeLabel("e", "v", "v");

  // 3) 建立顶点。BACH 顶点号连续，逐个 AddVertex。
  auto ts_vertices_begin = std::chrono::high_resolution_clock::now();
  {
    auto txn = db->BeginTransaction();
    for (uint32_t v = 0; v < num_nodes; ++v) {
      txn.AddVertex(vertex_label);
    }
  }
  auto ts_vertices_end = std::chrono::high_resolution_clock::now();
  size_t rss_after_vertices = vsparsity::get_rss_bytes();

  // 4) 单线程 ingest 全部边。
  auto ts_edges_begin = std::chrono::high_resolution_clock::now();
  {
    auto txn = db->BeginTransaction();
    for (size_t i = 0; i < edges.size(); ++i) {
      txn.PutEdge(edges[i].src, edges[i].dst, edge_label, 1.0);
    }
  }
  auto ts_edges_end = std::chrono::high_resolution_clock::now();
  auto ts_end = std::chrono::high_resolution_clock::now();

  size_t rss_after_ingest = vsparsity::get_rss_bytes();
  size_t peak_rss = vsparsity::get_peak_rss_bytes();
  double t_ingest = std::chrono::duration<double>(ts_end - ts_begin).count();
  double t_vertices = std::chrono::duration<double>(ts_vertices_end - ts_vertices_begin).count();
  double t_edges = std::chrono::duration<double>(ts_edges_end - ts_edges_begin).count();
  double mtps = args.num_edges / t_ingest / 1e6;
  double edge_mtps = args.num_edges / t_edges / 1e6;

  long struct_bytes = (long)rss_after_ingest - (long)rss_after_gen;
  if (struct_bytes < 0) struct_bytes = 0;

  printf(EXPOUT "System: BACH\n");
  printf(EXPOUT "logN: %d\n", args.logN);
  printf(EXPOUT "Vertex count: %u\n", num_nodes);
  printf(EXPOUT "Edge count: %ld\n", (long)args.num_edges);
  printf(EXPOUT "Ingest: %.4f\n", t_ingest);
  printf(EXPOUT "VertexBuild: %.4f\n", t_vertices);
  printf(EXPOUT "EdgeInsert: %.4f\n", t_edges);
  printf(EXPOUT "Throughput_MEPS: %.4f\n", mtps);
  printf(EXPOUT "EdgeOnlyThroughput_MEPS: %.4f\n", edge_mtps);
  printf(EXPOUT "RSS_after_gen: %zu\n", rss_after_gen);
  printf(EXPOUT "RSS_after_alloc: %zu\n", rss_after_vertices);
  printf(EXPOUT "RSS_after_ingest: %zu\n", rss_after_ingest);
  printf(EXPOUT "RSS_peak: %zu\n", peak_rss);
  printf(EXPOUT "RSS_struct: %ld\n", struct_bytes);

  // 提前退出，跳过 BACH 析构里的落盘，避免写外存。先刷新 stdout。
  fflush(stdout);
  std::_Exit(0);
  return 0;
}
