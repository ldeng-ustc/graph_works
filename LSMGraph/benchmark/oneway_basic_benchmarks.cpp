#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <unistd.h>

#define EXPOUT "[EXPOUT]"

#include "common/flags.h"
#include "lsmgraph.h"
#include "../../oneway_basic_benchmarks/oneway_graph_benchmarks.hpp"

namespace fs = std::filesystem;

namespace {

struct Edge32 {
  uint32_t src;
  uint32_t dst;
};

static_assert(sizeof(Edge32) == 2 * sizeof(uint32_t), "Edge32 must be 8 bytes");

constexpr size_t kDefaultChunkBytes = 16ull << 20;
constexpr size_t kDefaultChunkEdges = kDefaultChunkBytes / sizeof(Edge32);

struct Options {
  std::string input_path;
  std::string storage_dir;
  std::string dataset_name;
  size_t num_vertices = 0;
  size_t num_threads = 16;
  size_t algo_threads = 0;
  size_t memtable_num = 2;
  size_t bfs_rounds = 1;
  size_t pr_iterations = 1;
  double pr_epsilon = 0.0;
  size_t cc_rounds = 1;
  size_t max_batch = 0;
  bool reset = false;
  bool skip_analytics = false;
};

double now_seconds() {
  using Clock = std::chrono::steady_clock;
  static const auto begin = Clock::now();
  return std::chrono::duration<double>(Clock::now() - begin).count();
}

long get_rss_bytes() {
  std::ifstream stat_file("/proc/self/statm");
  long total_pages = 0;
  long resident_pages = 0;
  if (!(stat_file >> total_pages >> resident_pages)) {
    return 0;
  }
  return resident_pages * sysconf(_SC_PAGESIZE);
}

std::string require_value(const std::string& flag, int& index, int argc, char** argv) {
  if (index + 1 >= argc) {
    throw std::runtime_error("missing value for " + flag);
  }
  return argv[++index];
}

void print_usage(const char* argv0) {
  std::cerr
      << "Usage: " << argv0 << " -f <graph.bin32> --num-vertices <n> --storage <dir> [options]\n"
      << "Single-direction benchmark: stores each input (src,dst) exactly once.\n"
      << "Algorithms: forward BFS, push PageRank, and weak CC over the one stored edge set.\n"
      << "Options:\n"
      << "  --dataset <name>             Dataset name for LSMGraph metadata\n"
      << "  --threads <n>                Ingest workers, default 16\n"
      << "  --algo-threads <n>           BFS/PR/CC workers, default --threads\n"
      << "  --memtable-num <n>           MemGraph count, default 2\n"
      << "  --bfs-rounds <n>             BFS rounds, default 1\n"
      << "  --pr-iterations <n>          PageRank iterations, default 1\n"
      << "  --pr-epsilon <x>             PageRank early stop epsilon, default 0\n"
      << "  --cc-rounds <n>              CC rounds, default 1\n"
      << "  --max-batch <n>              Stop after N 16MiB batches, default 0 (all)\n"
      << "  --skip-analytics             Skip BFS/PR/CC\n"
      << "  --reset                      Remove storage before ingest\n";
}

Options parse_options(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "-f" || arg == "--input") {
      options.input_path = require_value(arg, index, argc, argv);
    } else if (arg == "--num-vertices") {
      options.num_vertices = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--storage") {
      options.storage_dir = require_value(arg, index, argc, argv);
    } else if (arg == "--dataset") {
      options.dataset_name = require_value(arg, index, argc, argv);
    } else if (arg == "--threads") {
      options.num_threads = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--algo-threads") {
      options.algo_threads = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--memtable-num") {
      options.memtable_num = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--bfs-rounds") {
      options.bfs_rounds = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--pr-iterations") {
      options.pr_iterations = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--pr-epsilon") {
      options.pr_epsilon = std::stod(require_value(arg, index, argc, argv));
    } else if (arg == "--cc-rounds") {
      options.cc_rounds = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--max-batch") {
      options.max_batch = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--skip-analytics") {
      options.skip_analytics = true;
    } else if (arg == "--reset") {
      options.reset = true;
    } else if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      std::exit(0);
    } else {
      throw std::runtime_error("unknown argument: " + arg);
    }
  }

  if (options.input_path.empty() || options.storage_dir.empty() || options.num_vertices == 0) {
    throw std::runtime_error("-f, --storage, and --num-vertices are required");
  }
  if (options.num_threads == 0 || options.memtable_num == 0) {
    throw std::runtime_error("thread and memtable counts must be greater than zero");
  }
  if (options.algo_threads == 0) {
    options.algo_threads = options.num_threads;
  }
  if (options.dataset_name.empty()) {
    options.dataset_name = fs::path(options.input_path).stem().string();
  }
  return options;
}

void prepare_storage_dir(const fs::path& storage_dir, bool reset) {
  if (fs::exists(storage_dir)) {
    if (!reset) {
      throw std::runtime_error("storage already exists; pass --reset: " + storage_dir.string());
    }
    fs::remove_all(storage_dir);
  }
  fs::create_directories(storage_dir);
}

void configure_lsmgraph_flags(const Options& options) {
  FLAGS_LOAD_OLD_DATA = false;
  FLAGS_OPEN_SSTDATA_CACHE = true;
  FLAGS_directed = true;
  FLAGS_shuffle_edge = false;
  FLAGS_support_mulversion = true;
  FLAGS_db_path = options.storage_dir;
  FLAGS_dataset_path = options.input_path;
  FLAGS_dataset_name = options.dataset_name;
  FLAGS_thread_num = static_cast<uint32_t>(options.algo_threads);
  FLAGS_memtable_num = static_cast<uint32_t>(options.memtable_num);
  FLAGS_max_subcompactions = 8;
  FLAGS_mmap_path = (fs::path(options.storage_dir) / "mmap" / "block.mmap").string();
}

lsmg::LSMGraph* open_graph(const Options& options) {
  configure_lsmgraph_flags(options);
  fs::create_directories(options.storage_dir);
  fs::create_directories(fs::path(FLAGS_mmap_path).parent_path());
  lsmg::LSMGraph* graph = nullptr;
  lsmg::LSMGraph::open(options.storage_dir, options.num_vertices, &graph);
  return graph;
}

void create_vertices(lsmg::LSMGraph& graph, uint32_t num_vertices) {
  const double begin = now_seconds();
  for (uint32_t vertex = 0; vertex < num_vertices; ++vertex) {
    if (graph.new_vertex() != vertex) {
      throw std::runtime_error("LSMGraph returned non-contiguous vertex ids");
    }
    graph.put_vertex(vertex, std::string_view());
  }
  std::printf("[ingest] vertices=%u time=%.4f\n", num_vertices, now_seconds() - begin);
}

size_t ingest_edges(lsmg::LSMGraph& graph,
                    oneway_bench::GraphMetadata& metadata,
                    const Options& options) {
  std::FILE* input = std::fopen(options.input_path.c_str(), "rb");
  if (!input) {
    throw std::runtime_error("failed to open input: " + options.input_path);
  }

  std::fseek(input, 0, SEEK_END);
  const long file_bytes = std::ftell(input);
  std::fseek(input, 0, SEEK_SET);
  if (file_bytes < 0 || file_bytes % static_cast<long>(sizeof(Edge32)) != 0) {
    std::fclose(input);
    throw std::runtime_error("input is not aligned bin32 edges");
  }
  const size_t total_edges = static_cast<size_t>(file_bytes) / sizeof(Edge32);
  const size_t total_batches = (total_edges + kDefaultChunkEdges - 1) / kDefaultChunkEdges;

  std::vector<Edge32> chunk(kDefaultChunkEdges);
  size_t loaded_edges = 0;
  size_t batch = 0;
  const double ingest_begin = now_seconds();
  while (true) {
    const size_t count = std::fread(chunk.data(), sizeof(Edge32), kDefaultChunkEdges, input);
    if (count == 0) {
      break;
    }

    for (size_t index = 0; index < count; ++index) {
      const Edge32& edge = chunk[index];
      oneway_bench::validate_edge_endpoints(
          edge.src, edge.dst, metadata.num_vertices, loaded_edges + index);
      ++metadata.out_degree[edge.src];
    }

    const double batch_begin = now_seconds();
#pragma omp parallel for num_threads(static_cast<int>(options.num_threads)) schedule(dynamic, 65536)
    for (uint64_t index = 0; index < count; ++index) {
      const Edge32& edge = chunk[static_cast<size_t>(index)];
      graph.put_edge(edge.src, edge.dst, std::string());
    }

    if (!metadata.has_active_vertex) {
      metadata.first_active_vertex = chunk.front().src;
      metadata.has_active_vertex = true;
    }
    loaded_edges += count;
    ++batch;
    const double batch_seconds = now_seconds() - batch_begin;
    const double elapsed = now_seconds() - ingest_begin;
    std::printf("[ingest] direction=oneway batch=%zu/%zu edges=%zu progress=%.2f%% "
                "batch_time=%.4f elapsed=%.4f rate=%.4fM edges/s\n",
                batch,
                total_batches,
                count,
                total_edges == 0 ? 100.0 : 100.0 * static_cast<double>(loaded_edges) / total_edges,
                batch_seconds,
                elapsed,
                batch_seconds == 0.0 ? 0.0 : static_cast<double>(count) / batch_seconds / 1e6);
    if (count < kDefaultChunkEdges || (options.max_batch != 0 && batch >= options.max_batch)) {
      break;
    }
  }

  const double wait_begin = now_seconds();
  graph.WaitForBackgroundWork();
  std::printf("[ingest] direction=oneway final_background_wait=%.4f\n",
              now_seconds() - wait_begin);
  std::fclose(input);
  metadata.total_edges = loaded_edges;
  return loaded_edges;
}

auto make_out_neighbors(lsmg::LSMGraph& graph) {
  return [&graph] {
    return [&graph](uint32_t src, auto&& callback) {
      auto iterator = graph.get_edges(src);
      for (; iterator.valid(); iterator.next()) {
        if (!callback(static_cast<uint32_t>(iterator.dst_id()))) {
          break;
        }
      }
    };
  };
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const double benchmark_begin = now_seconds();
    const Options options = parse_options(argc, argv);
    std::printf("[config] benchmark=oneway_basic_benchmarks system=LSMGraph input=%s storage=%s "
                "vertices=%zu threads=%zu algo_threads=%zu memtable_num=%zu\n",
                options.input_path.c_str(),
                options.storage_dir.c_str(),
                options.num_vertices,
                options.num_threads,
                options.algo_threads,
                options.memtable_num);
    prepare_storage_dir(options.storage_dir, options.reset);

    const double load_begin = now_seconds();
    oneway_bench::GraphMetadata metadata;
    metadata.num_vertices = static_cast<uint32_t>(options.num_vertices);
    metadata.out_degree.assign(metadata.num_vertices, 0);
    const double load_seconds = now_seconds() - load_begin;

    lsmg::LSMGraph* graph = open_graph(options);
    create_vertices(*graph, metadata.num_vertices);
    const double ingest_begin = now_seconds();
    const size_t loaded_edges = ingest_edges(*graph, metadata, options);
    const double ingest_seconds = now_seconds() - ingest_begin;
    const long rss_ingest = get_rss_bytes();

    oneway_bench::BenchmarkResult result;
    if (!options.skip_analytics) {
      oneway_bench::BenchmarkConfig config;
      config.algo_threads = options.algo_threads;
      config.bfs_rounds = options.bfs_rounds;
      config.pr_iterations = options.pr_iterations;
      config.pr_epsilon = options.pr_epsilon;
      config.cc_rounds = options.cc_rounds;
      result = oneway_bench::run_oneway_graph_benchmarks(
          metadata, config, make_out_neighbors(*graph));
    }

    const double total_seconds = now_seconds() - benchmark_begin;
    std::printf(EXPOUT "Load: %.4f\n", load_seconds);
    std::printf(EXPOUT "Ingest: %.4f\n", ingest_seconds);
    std::printf(EXPOUT "BFS: %.4f\n", result.bfs_seconds);
    std::printf(EXPOUT "PR: %.4f\n", result.pr_seconds);
    std::printf(EXPOUT "CC: %.4f\n", result.cc_seconds);
    std::printf(EXPOUT "RSS_Ingest: %ld\n", rss_ingest);
    std::printf("[result] direction=oneway bfs_checksum=%llu pr_sum=%.8f cc_components=%zu "
                "total=%.4f ingest_bw=%.4fM edges/s edges=%zu\n",
                static_cast<unsigned long long>(result.bfs_checksum),
                result.pr_sum,
                result.cc_components,
                total_seconds,
                ingest_seconds == 0.0 ? 0.0 : static_cast<double>(loaded_edges) / ingest_seconds / 1e6,
                loaded_edges);

    delete graph;
    return 0;
  } catch (const std::exception& exception) {
    std::cerr << "error: " << exception.what() << '\n';
    return 1;
  }
}
