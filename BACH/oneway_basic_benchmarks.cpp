#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "BACH/BACH.h"
#include "../oneway_basic_benchmarks/oneway_graph_benchmarks.hpp"

namespace fs = std::filesystem;

#define EXPOUT "[EXPOUT]"

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
  size_t num_vertices = 0;
  size_t ingest_batch_edges = kDefaultChunkEdges;
  size_t num_threads = 16;
  size_t algo_threads = 0;
  size_t bfs_rounds = 1;
  size_t pr_iterations = 1;
  double pr_epsilon = 0.0;
  size_t cc_rounds = 1;
  size_t max_batch = 0;
  bool reset = false;
  bool skip_analytics = false;
};

long get_rss_bytes() {
  std::ifstream stat_file("/proc/self/statm");
  long total_pages = 0;
  long resident_pages = 0;
  if (!(stat_file >> total_pages >> resident_pages)) {
    return 0;
  }
  return resident_pages * sysconf(_SC_PAGESIZE);
}

double now_seconds() {
  using Clock = std::chrono::steady_clock;
  static const auto begin = Clock::now();
  return std::chrono::duration<double>(Clock::now() - begin).count();
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
      << "  --threads <n>                 Ingest workers, default 16\n"
      << "  --algo-threads <n>            BFS/PR/CC workers, default --threads\n"
      << "  --ingest-batch-edges <n>      Edges per batch, default 2097152\n"
      << "  --bfs-rounds <n>              BFS rounds, default 1\n"
      << "  --pr-iters <n>                PageRank iterations, default 1\n"
      << "  --pr-epsilon <x>              PageRank early stop epsilon, default 0\n"
      << "  --cc-rounds <n>               CC rounds, default 1\n"
      << "  --max-batch <n>               Stop after N batches, default 0 (all)\n"
      << "  --skip-analytics              Skip BFS/PR/CC\n"
      << "  --reset                       Remove storage before ingest\n";
}

Options parse_options(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "-f" || arg == "--file") {
      options.input_path = require_value(arg, index, argc, argv);
    } else if (arg == "--num-vertices") {
      options.num_vertices = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--storage") {
      options.storage_dir = require_value(arg, index, argc, argv);
    } else if (arg == "--threads") {
      options.num_threads = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--algo-threads") {
      options.algo_threads = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--ingest-batch-edges") {
      options.ingest_batch_edges = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--bfs-rounds") {
      options.bfs_rounds = std::stoull(require_value(arg, index, argc, argv));
    } else if (arg == "--pr-iters") {
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
  if (options.num_threads == 0 || options.ingest_batch_edges == 0) {
    throw std::runtime_error("thread and batch counts must be greater than zero");
  }
  if (options.algo_threads == 0) {
    options.algo_threads = options.num_threads;
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

struct BACHDb {
  std::shared_ptr<BACH::Options> options;
  std::unique_ptr<BACH::DB> db;
  BACH::label_t vertex_label = 0;
  BACH::label_t edge_label = 0;
};

BACHDb open_db(const Options& options) {
  BACHDb handle;
  handle.options = std::make_shared<BACH::Options>();
  handle.options->STORAGE_DIR = options.storage_dir;
  handle.options->NUM_OF_COMPACTION_THREAD = options.num_threads;
  handle.options->MAX_WORKER_THREAD =
      std::max<size_t>(handle.options->MAX_WORKER_THREAD, options.num_threads);
  handle.db = std::make_unique<BACH::DB>(handle.options);
  handle.vertex_label = handle.db->AddVertexLabel("v");
  handle.edge_label = handle.db->AddEdgeLabel("e", "v", "v");
  return handle;
}

void create_vertices(BACH::DB& db, uint32_t num_vertices, BACH::label_t vertex_label) {
  const double begin = now_seconds();
  auto transaction = db.BeginTransaction();
  for (uint32_t vertex = 0; vertex < num_vertices; ++vertex) {
    if (transaction.AddVertex(vertex_label) != vertex) {
      throw std::runtime_error("BACH returned non-contiguous vertex ids");
    }
  }
  std::printf("[ingest] vertices=%u time=%.4f\n", num_vertices, now_seconds() - begin);
}

size_t ingest_edges(BACH::DB& db,
                    oneway_bench::GraphMetadata& graph,
                    const Options& options,
                    BACH::label_t edge_label) {
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
  const size_t total_batches =
      (total_edges + options.ingest_batch_edges - 1) / options.ingest_batch_edges;

  std::vector<Edge32> chunk(options.ingest_batch_edges);
  size_t loaded_edges = 0;
  size_t batch = 0;
  const double ingest_begin = now_seconds();
  while (true) {
    const size_t count = std::fread(
        chunk.data(), sizeof(Edge32), options.ingest_batch_edges, input);
    if (count == 0) {
      break;
    }

    for (size_t index = 0; index < count; ++index) {
      const Edge32& edge = chunk[index];
      oneway_bench::validate_edge_endpoints(
          edge.src, edge.dst, graph.num_vertices, loaded_edges + index);
      ++graph.out_degree[edge.src];
    }

    std::atomic<bool> failed(false);
    std::exception_ptr exception;
    std::mutex exception_mutex;
    const size_t threads = oneway_bench::clamp_thread_count(options.num_threads, count);
    const double batch_begin = now_seconds();
#pragma omp parallel num_threads(static_cast<int>(threads))
    {
      try {
        auto transaction = db.BeginTransaction();
#pragma omp for schedule(static)
        for (int64_t index = 0; index < static_cast<int64_t>(count); ++index) {
          const Edge32& edge = chunk[static_cast<size_t>(index)];
          transaction.PutEdge(edge.src, edge.dst, edge_label, 1.0);
        }
      } catch (...) {
        failed.store(true, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(exception_mutex);
        if (!exception) {
          exception = std::current_exception();
        }
      }
    }
    if (failed.load(std::memory_order_relaxed)) {
      std::fclose(input);
      std::rethrow_exception(exception);
    }

    if (!graph.has_active_vertex) {
      graph.first_active_vertex = chunk.front().src;
      graph.has_active_vertex = true;
    }
    loaded_edges += count;
    ++batch;
    const double elapsed = now_seconds() - ingest_begin;
    const double batch_seconds = now_seconds() - batch_begin;
    std::printf("[ingest] direction=oneway batch=%zu/%zu edges=%zu progress=%.2f%% "
                "batch_time=%.4f elapsed=%.4f rate=%.4fM edges/s\n",
                batch,
                total_batches,
                count,
                total_edges == 0 ? 100.0 : 100.0 * static_cast<double>(loaded_edges) / total_edges,
                batch_seconds,
                elapsed,
                batch_seconds == 0.0 ? 0.0 : static_cast<double>(count) / batch_seconds / 1e6);
    if (count < options.ingest_batch_edges ||
        (options.max_batch != 0 && batch >= options.max_batch)) {
      break;
    }
  }
  std::fclose(input);
  graph.total_edges = loaded_edges;
  return loaded_edges;
}

class BACHOutNeighborOracle {
 public:
  BACHOutNeighborOracle(BACH::DB& db, BACH::label_t edge_label, size_t threads)
      : edge_label_(edge_label) {
    transactions_.reserve(threads);
    for (size_t thread = 0; thread < threads; ++thread) {
      transactions_.push_back(std::make_shared<BACH::Transaction>(db.BeginReadOnlyTransaction()));
    }
  }

  auto make_out_neighbors() {
    return [this](uint32_t src, auto&& callback) {
      const size_t tid = std::min(
          static_cast<size_t>(omp_get_thread_num()), transactions_.size() - 1);
      const auto edges = transactions_[tid]->GetEdges(src, edge_label_);
      for (const auto& [dst, property] : *edges) {
        (void)property;
        if (!callback(static_cast<uint32_t>(dst))) {
          break;
        }
      }
    };
  }

 private:
  BACH::label_t edge_label_;
  std::vector<std::shared_ptr<BACH::Transaction>> transactions_;
};

}  // namespace

int main(int argc, char** argv) {
  try {
    const double benchmark_begin = now_seconds();
    const Options options = parse_options(argc, argv);
    std::printf("[config] benchmark=oneway_basic_benchmarks system=BACH input=%s storage=%s "
                "vertices=%zu threads=%zu algo_threads=%zu batch_edges=%zu\n",
                options.input_path.c_str(),
                options.storage_dir.c_str(),
                options.num_vertices,
                options.num_threads,
                options.algo_threads,
                options.ingest_batch_edges);
    prepare_storage_dir(options.storage_dir, options.reset);

    const double load_begin = now_seconds();
    oneway_bench::GraphMetadata graph;
    graph.num_vertices = static_cast<uint32_t>(options.num_vertices);
    graph.out_degree.assign(graph.num_vertices, 0);
    const double load_seconds = now_seconds() - load_begin;

    BACHDb handle = open_db(options);
    create_vertices(*handle.db, graph.num_vertices, handle.vertex_label);
    const double ingest_begin = now_seconds();
    const size_t loaded_edges = ingest_edges(*handle.db, graph, options, handle.edge_label);
    const double ingest_seconds = now_seconds() - ingest_begin;
    const long rss_ingest = get_rss_bytes();

    oneway_bench::BenchmarkResult result;
    if (!options.skip_analytics) {
      BACHOutNeighborOracle oracle(*handle.db, handle.edge_label, options.algo_threads);
      oneway_bench::BenchmarkConfig config;
      config.algo_threads = options.algo_threads;
      config.bfs_rounds = options.bfs_rounds;
      config.pr_iterations = options.pr_iterations;
      config.pr_epsilon = options.pr_epsilon;
      config.cc_rounds = options.cc_rounds;
      result = oneway_bench::run_oneway_graph_benchmarks(
          graph, config, [&oracle] { return oracle.make_out_neighbors(); });
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
    return 0;
  } catch (const std::exception& exception) {
    std::cerr << "error: " << exception.what() << '\n';
    return 1;
  }
}
