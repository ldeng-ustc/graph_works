// XPGraph 顶点稀疏性 bench（DRAM-only，禁 flush）。
//
// 目的：固定边数（正式实验为 1 亿），逐步增大顶点范围（logN），观测
// XPGraph-D 的内存图结构在载入时，吞吐和内存随顶点数的变化。
//
// 边在内存里用 Kronecker 生成器直接产生，不读写外存。调用 add_edge 的输入
// 线程只有一个，XPGraph 内部的分类与缓冲线程也设为一个。载入完成后等待 edge
// log 中的所有边进入 DRAM 顶点缓冲，但不调用 flush_all_vbufs，因此不会把顶点
// 缓冲归档到外存。计时包含 XPGraph 初始化和全部边进入内存图结构的时间。

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "libxpgraph.h"

#include "kron_gen.hpp"

namespace fs = std::filesystem;

#define EXPOUT "[EXPOUT]"

namespace {

struct Args {
  int logN = 23;
  int64_t num_edges = 100000000;
  int elog_shift = 27;
  int vbuf_pool_gib = 16;
  int pblk_pool_gib = 16;
  std::string storage_dir = "/dev/shm/scattergraph-vsparsity/xpgraph";
};

Args parse_args(int argc, char** argv) {
  Args args;
  for (int i = 1; i < argc; ++i) {
    std::string option = argv[i];
    if (option == "-n" && i + 1 < argc) {
      args.logN = std::atoi(argv[++i]);
    } else if (option == "-e" && i + 1 < argc) {
      args.num_edges = std::atoll(argv[++i]);
    } else if (option == "--elog-shift" && i + 1 < argc) {
      args.elog_shift = std::atoi(argv[++i]);
    } else if (option == "--vbuf-pool-gib" && i + 1 < argc) {
      args.vbuf_pool_gib = std::atoi(argv[++i]);
    } else if (option == "--pblk-pool-gib" && i + 1 < argc) {
      args.pblk_pool_gib = std::atoi(argv[++i]);
    } else if (option == "--storage" && i + 1 < argc) {
      args.storage_dir = argv[++i];
    }
  }
  return args;
}

std::vector<std::string> make_xpgraph_args(const Args& args,
                                           uint32_t num_vertices) {
  fs::path storage = fs::absolute(args.storage_dir);
  std::string storage_path = storage.string();
  if (storage_path.back() != '/') storage_path.push_back('/');

  return {
      "xpgraph_vsparsity",
      "-v",
      std::to_string(num_vertices),
      "-e",
      std::to_string(args.num_edges),
      "-t",
      "1",
      "-s",
      "1",
      "-numa",
      "0",
      "-p0",
      storage_path,
      "-p1",
      storage_path,
      "--elogsize",
      std::to_string(args.elog_shift),
      "--mempool",
      "1",
      "--membulk",
      "16",
      "--vbuf_pool",
      std::to_string(args.vbuf_pool_gib),
      "--pblk_pools",
      std::to_string(args.pblk_pool_gib),
      "--dram_only",
      "1",
      "--persist",
      "0",
  };
}

}  // namespace

int main(int argc, char** argv) {
  vsparsity::disable_core_dumps();
  setvbuf(stdout, nullptr, _IONBF, 0);

  Args args = parse_args(argc, argv);
  if (args.logN < 1 || args.logN > 30 || args.num_edges <= 0) {
    std::fprintf(stderr, "invalid arguments: logN=%d edges=%ld\n", args.logN,
                 static_cast<long>(args.num_edges));
    return 2;
  }
  if (args.elog_shift < 23 ||
      (1ULL << args.elog_shift) < static_cast<uint64_t>(args.num_edges)) {
    std::fprintf(stderr,
                 "edge log is too small: shift=%d capacity=%llu edges=%ld\n",
                 args.elog_shift, 1ULL << args.elog_shift,
                 static_cast<long>(args.num_edges));
    return 2;
  }

  uint32_t num_vertices = 1u << args.logN;
  std::printf("logN=%d num_vertices=%u num_edges=%ld\n", args.logN,
              num_vertices, static_cast<long>(args.num_edges));

  if (fs::exists(args.storage_dir)) fs::remove_all(args.storage_dir);
  fs::create_directories(args.storage_dir);

  std::vector<vsparsity::SimpleEdge> edges;
  vsparsity::gen_kron_edges(args.logN, args.num_edges, edges);
  size_t rss_after_gen = vsparsity::get_rss_bytes();
  std::printf("Generated %zu edges, rss=%zuMB\n", edges.size(),
              rss_after_gen >> 20);

  std::vector<std::string> xp_arg_strings =
      make_xpgraph_args(args, num_vertices);
  std::vector<const char*> xp_argv;
  xp_argv.reserve(xp_arg_strings.size());
  for (const std::string& value : xp_arg_strings) {
    xp_argv.push_back(value.c_str());
  }

  auto ts_begin = std::chrono::high_resolution_clock::now();
  auto graph = std::make_unique<XPGraph>(
      static_cast<int>(xp_argv.size()), xp_argv.data());
  size_t rss_after_alloc = vsparsity::get_rss_bytes();

  for (const auto& edge : edges) {
    graph->add_edge(edge.src, edge.dst);
  }
  graph->buffer_all_edges();
  auto ts_end = std::chrono::high_resolution_clock::now();

  size_t rss_after_ingest = vsparsity::get_rss_bytes();
  size_t peak_rss = vsparsity::get_peak_rss_bytes();
  double ingest_seconds =
      std::chrono::duration<double>(ts_end - ts_begin).count();
  double throughput_meps = args.num_edges / ingest_seconds / 1e6;

  long struct_bytes =
      static_cast<long>(rss_after_ingest) - static_cast<long>(rss_after_gen);
  if (struct_bytes < 0) struct_bytes = 0;

  std::printf(EXPOUT "System: XPGraph\n");
  std::printf(EXPOUT "logN: %d\n", args.logN);
  std::printf(EXPOUT "Vertex count: %u\n", num_vertices);
  std::printf(EXPOUT "Edge count: %ld\n",
              static_cast<long>(args.num_edges));
  std::printf(EXPOUT "Ingest: %.4f\n", ingest_seconds);
  std::printf(EXPOUT "Throughput_MEPS: %.4f\n", throughput_meps);
  std::printf(EXPOUT "RSS_after_gen: %zu\n", rss_after_gen);
  std::printf(EXPOUT "RSS_after_alloc: %zu\n", rss_after_alloc);
  std::printf(EXPOUT "RSS_after_ingest: %zu\n", rss_after_ingest);
  std::printf(EXPOUT "RSS_peak: %zu\n", peak_rss);
  std::printf(EXPOUT "RSS_struct: %ld\n", struct_bytes);

  // 跳过析构中的统计报告和目录递归删除。DRAM-only 模式没有外存数据需要同步。
  std::fflush(stdout);
  std::_Exit(0);
}
