// LSMGraph 顶点稀疏性 bench（禁 flush）。
//
// 目的：固定边数（默认 1 亿），逐步增大顶点范围（logN），观测 LSMGraph 内存
// 部分（memtable + per 顶点索引）在只做 ingest 时，吞吐和内存随顶点数的变化。
// LSMGraph 在构造与建顶点时按顶点数分配 per 顶点的锁、层级、多级索引，顶点数
// 增大会让这些结构线性膨胀，即使边数不变。
//
// 为了测出纯内存 ingest 的时间，用带 LSMG_VSPARSITY_NO_FLUSH 宏的独立 build 把
// memtable 容量抬到能装下全部边，让 ingest 全程不触发 flush。边在内存里用
// Kronecker 生成器直接产生，不读外存。单线程 ingest。

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "common/flags.h"
#include "lsmgraph.h"

#include "kron_gen.hpp"

namespace fs = std::filesystem;

#define EXPOUT "[EXPOUT]"

namespace {

struct Args {
  int logN = 23;
  int64_t num_edges = 100000000;
  std::string storage_dir = "/data01/graph_output/vsparsity/lsmgraph";
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

// 配置 LSMGraph 的 gflags。关掉加载旧数据，只保留一个 memtable，单线程后台。
void configure_flags(const std::string& storage_dir) {
  FLAGS_LOAD_OLD_DATA = false;
  FLAGS_OPEN_SSTDATA_CACHE = true;
  FLAGS_directed = true;
  FLAGS_shuffle_edge = false;
  FLAGS_support_mulversion = true;
  FLAGS_db_path = storage_dir;
  FLAGS_thread_num = 1;
  FLAGS_memtable_num = 1;
  FLAGS_max_subcompactions = 1;
  FLAGS_mmap_path = (fs::path(storage_dir) / "mmap" / "block.mmap").string();
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

  // 2) 打开图。open 内部会把 max_vertex_num 加 10 再构造。
  // 载入时间从图结构初始化开始，包含多级索引和顶点元数据的建立成本。
  auto ts_begin = std::chrono::high_resolution_clock::now();
  configure_flags(args.storage_dir);
  fs::create_directories(fs::path(FLAGS_mmap_path).parent_path());
  lsmg::LSMGraph* graph = nullptr;
  lsmg::LSMGraph::open(args.storage_dir, num_nodes, &graph);

  // 3) 建立顶点。顶点号连续返回，逐个建。这一步会触碰 per 顶点结构。
  auto ts_vertices_begin = std::chrono::high_resolution_clock::now();
  for (uint32_t v = 0; v < num_nodes; ++v) {
    if (graph->new_vertex() != v) {
      throw std::runtime_error("LSMGraph returned non-contiguous vertex ids");
    }
    graph->put_vertex(v, std::string_view());
  }
  auto ts_vertices_end = std::chrono::high_resolution_clock::now();
  size_t rss_after_alloc = vsparsity::get_rss_bytes();

  // 4) 单线程 ingest 全部边。
  auto ts_edges_begin = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < edges.size(); ++i) {
    graph->put_edge(edges[i].src, edges[i].dst, std::string());
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

  printf(EXPOUT "System: LSMGraph\n");
  printf(EXPOUT "logN: %d\n", args.logN);
  printf(EXPOUT "Vertex count: %u\n", num_nodes);
  printf(EXPOUT "Edge count: %ld\n", (long)args.num_edges);
  printf(EXPOUT "Ingest: %.4f\n", t_ingest);
  printf(EXPOUT "VertexBuild: %.4f\n", t_vertices);
  printf(EXPOUT "EdgeInsert: %.4f\n", t_edges);
  printf(EXPOUT "Throughput_MEPS: %.4f\n", mtps);
  printf(EXPOUT "EdgeOnlyThroughput_MEPS: %.4f\n", edge_mtps);
  printf(EXPOUT "RSS_after_gen: %zu\n", rss_after_gen);
  printf(EXPOUT "RSS_after_alloc: %zu\n", rss_after_alloc);
  printf(EXPOUT "RSS_after_ingest: %zu\n", rss_after_ingest);
  printf(EXPOUT "RSS_peak: %zu\n", peak_rss);
  printf(EXPOUT "RSS_struct: %ld\n", struct_bytes);

  // 提前退出，跳过 LSMGraph 析构里的落盘与 mmap 同步，避免写外存。
  fflush(stdout);
  std::_Exit(0);
  return 0;
}
