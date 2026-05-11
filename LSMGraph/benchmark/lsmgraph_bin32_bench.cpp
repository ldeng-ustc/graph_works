#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <unistd.h>

#define EXPOUT "[EXPOUT]"

#include "common/flags.h"
#include "graph_benchmarks.hpp"
#include "lsmgraph.h"

namespace fs = std::filesystem;

namespace {

struct Options {
  std::string input_path;
  std::string storage_dir;
  std::string dataset_name;
  size_t num_threads = 1;
  size_t algo_threads = 1;
  size_t memtable_num = 2;
  size_t bfs_rounds = 20;
  size_t pr_iterations = 10;
  size_t cc_rounds = 10;
  size_t cc_neighbor_rounds = 2;
  double pr_epsilon = 0.0;
  bool reset = false;
};

constexpr size_t kDefaultIngestBatchEdges = 1ull << 20;

struct StoragePaths {
  std::string out_storage_dir;
  std::string in_storage_dir;
};

double now_seconds() {
  using clock = std::chrono::steady_clock;
  static const auto start = clock::now();
  return std::chrono::duration<double>(clock::now() - start).count();
}

long get_rss_bytes() {
  std::ifstream stat_file("/proc/self/statm");
  if (!stat_file) {
    return 0;
  }
  long total_pages = 0;
  long resident_pages = 0;
  stat_file >> total_pages >> resident_pages;
  const long page_size = sysconf(_SC_PAGESIZE);
  return resident_pages * page_size;
}

std::string require_value(const std::string& flag, int& index, int argc, char** argv) {
  if (index + 1 >= argc) {
    throw std::runtime_error("missing value for " + flag);
  }
  return argv[++index];
}

void print_usage(const char* argv0) {
  std::fprintf(stderr,
               "Usage: %s -f <bin32-file> --storage <dir> [options]\n"
               "Options:\n"
               "  -f, --input <path>       Input bin32 file\n"
               "  --storage <dir>          Database directory\n"
               "  --dataset <name>         Dataset name for output\n"
               "  --threads <n>            Ingest threads, default 1\n"
               "  --algo-threads <n>       Algorithm threads, default 1\n"
               "  --memtable-num <n>       Memtable count, default 2\n"
               "  --bfs-rounds <n>         BFS rounds, default 20\n"
               "  --pr-iterations <n>      PageRank iterations, default 10\n"
               "  --cc-rounds <n>          CC rounds, default 10\n"
               "  --cc-neighbor-rounds <n> CC sampled neighbor rounds, default 2\n"
               "  --pr-epsilon <x>         PageRank early-stop epsilon, default 0\n"
               "  --reset                  Remove storage dir before ingest\n",
               argv0);
}

Options parse_options(int argc, char** argv) {
  Options opts;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "-f" || arg == "--input") {
      opts.input_path = require_value(arg, i, argc, argv);
    } else if (arg == "--storage") {
      opts.storage_dir = require_value(arg, i, argc, argv);
    } else if (arg == "--dataset") {
      opts.dataset_name = require_value(arg, i, argc, argv);
    } else if (arg == "--threads") {
      opts.num_threads = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--algo-threads") {
      opts.algo_threads = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--memtable-num") {
      opts.memtable_num = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--bfs-rounds") {
      opts.bfs_rounds = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--pr-iterations") {
      opts.pr_iterations = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--cc-rounds") {
      opts.cc_rounds = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--cc-neighbor-rounds") {
      opts.cc_neighbor_rounds = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--pr-epsilon") {
      opts.pr_epsilon = std::stod(require_value(arg, i, argc, argv));
    } else if (arg == "--reset") {
      opts.reset = true;
    } else if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      std::exit(0);
    } else {
      throw std::runtime_error("unknown argument: " + arg);
    }
  }

  if (opts.input_path.empty()) {
    throw std::runtime_error("input file is required");
  }
  if (opts.storage_dir.empty()) {
    throw std::runtime_error("storage directory is required");
  }
  if (opts.num_threads == 0 || opts.algo_threads == 0 || opts.memtable_num == 0) {
    throw std::runtime_error("thread counts and memtable count must be > 0");
  }
  if (opts.dataset_name.empty()) {
    opts.dataset_name = fs::path(opts.input_path).stem().string();
  }
  return opts;
}

void log_config(const Options& opts) {
  std::printf("[config] dataset=%s\n", opts.input_path.c_str());
  std::printf("[config] storage=%s\n", opts.storage_dir.c_str());
  std::printf("[config] batch_size=%zu threads=%zu algo_threads=%zu bfs_rounds=%zu pr_iters=%zu pr_epsilon=%.6f cc_rounds=%zu cc_neighbor_rounds=%zu memtable_num=%zu\n",
              kDefaultIngestBatchEdges,
              opts.num_threads,
              opts.algo_threads,
              opts.bfs_rounds,
              opts.pr_iterations,
              opts.pr_epsilon,
              opts.cc_rounds,
              opts.cc_neighbor_rounds,
              opts.memtable_num);
}

StoragePaths make_storage_paths(const Options& opts) {
  StoragePaths paths;
  paths.out_storage_dir = (fs::path(opts.storage_dir) / "out").string();
  paths.in_storage_dir = (fs::path(opts.storage_dir) / "in").string();
  return paths;
}

lsmg_bench::LoadedGraph load_bin32_graph(const std::string& path) {
  lsmg_bench::LoadedGraph graph;
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    throw std::runtime_error("failed to open input file: " + path);
  }

  file.seekg(0, std::ios::end);
  const std::streamoff file_size = file.tellg();
  file.seekg(0, std::ios::beg);
  if (file_size < 0 || (file_size % (2 * static_cast<std::streamoff>(sizeof(uint32_t)))) != 0) {
    throw std::runtime_error("input file size is not aligned to bin32 edge pairs");
  }

  const uint64_t edge_count = static_cast<uint64_t>(file_size) / (2 * sizeof(uint32_t));
  graph.edges.reserve(edge_count);

  uint32_t max_vertex = 0;
  lsmg_bench::LoadedGraph::EdgePair edge;
  while (file.read(reinterpret_cast<char*>(&edge), sizeof(edge))) {
    graph.edges.push_back(edge);
    max_vertex = std::max(max_vertex, std::max(edge.src, edge.dst));
  }

  graph.num_vertices = graph.edges.empty() ? 0 : static_cast<uint64_t>(max_vertex) + 1;
  graph.out_degree.assign(graph.num_vertices, 0);
  graph.in_degree.assign(graph.num_vertices, 0);
  for (const auto& e : graph.edges) {
    graph.out_degree[e.src]++;
    graph.in_degree[e.dst]++;
  }

  return graph;
}

void configure_lsmgraph_flags(const Options& opts, const std::string& storage_dir) {
  FLAGS_LOAD_OLD_DATA = false;
  FLAGS_OPEN_SSTDATA_CACHE = true;
  FLAGS_directed = true;
  FLAGS_shuffle_edge = false;
  FLAGS_support_mulversion = true;
  FLAGS_db_path = storage_dir;
  FLAGS_dataset_path = opts.input_path;
  FLAGS_dataset_name = opts.dataset_name;
  FLAGS_thread_num = static_cast<uint32_t>(opts.algo_threads);
  FLAGS_memtable_num = static_cast<uint32_t>(opts.memtable_num);
  FLAGS_mmap_path = (fs::path(storage_dir) / "mmap" / "block.mmap").string();
}

lsmg::LSMGraph* open_graph(const Options& opts, const std::string& storage_dir, uint32_t num_vertices, const char* tag) {
  configure_lsmgraph_flags(opts, storage_dir);
  fs::create_directories(storage_dir);
  fs::create_directories(fs::path(FLAGS_mmap_path).parent_path());
  lsmg::LSMGraph* db = nullptr;
  std::printf("[db] opening %s storage=%s\n", tag, storage_dir.c_str());
  lsmg::LSMGraph::open(storage_dir, num_vertices, &db);
  return db;
}

void wait_for_background_work(lsmg::LSMGraph& db,
                              const Options& opts,
                              const std::string& storage_dir,
                              const char* graph_tag,
                              size_t batch,
                              size_t total_batches) {
  configure_lsmgraph_flags(opts, storage_dir);
  const double wait_begin = now_seconds();
  db.AwaitWrite();
  while (db.GetCompactionState()) {
    std::this_thread::sleep_for(std::chrono::microseconds(20));
  }
  const double wait_seconds = now_seconds() - wait_begin;
  std::printf("[ingest] graph=%s batch=%zu/%zu background_wait=%.4f\n",
              graph_tag,
              batch + 1,
              total_batches,
              wait_seconds);
}

void create_vertices(lsmg::LSMGraph& db,
                     uint32_t num_vertices,
                     const char* tag) {
  const double vertex_begin = now_seconds();
  std::printf("[ingest] creating %u vertices graph=%s\n", num_vertices, tag);
  for (uint64_t v = 0; v < num_vertices; ++v) {
    const auto new_vertex = db.new_vertex();
    if (new_vertex != v) {
      throw std::runtime_error("LSMGraph new_vertex returned unexpected vertex id");
    }
    db.put_vertex(new_vertex, std::string_view());
  }
  std::printf("[ingest] vertices done graph=%s time=%.4f\n", tag, now_seconds() - vertex_begin);
}

void ingest_single_graph(lsmg::LSMGraph& db,
                         const lsmg_bench::LoadedGraph& graph,
                         const Options& opts,
                         const std::string& storage_dir,
                         bool reverse_edges,
                         const char* graph_tag) {
  std::printf("[ingest] loading %zu edges graph=%s reverse=%d with batch_size=%zu\n",
              graph.edges.size(),
              graph_tag,
              reverse_edges ? 1 : 0,
              kDefaultIngestBatchEdges);
  const size_t total_batches =
      graph.edges.empty() ? 0 : ((graph.edges.size() + kDefaultIngestBatchEdges - 1) / kDefaultIngestBatchEdges);
  const double ingest_begin = now_seconds();
  for (size_t batch = 0; batch < total_batches; ++batch) {
    const size_t begin = batch * kDefaultIngestBatchEdges;
    const size_t end = std::min(begin + kDefaultIngestBatchEdges, graph.edges.size());

    configure_lsmgraph_flags(opts, storage_dir);
    const double batch_begin = now_seconds();
#pragma omp parallel for num_threads(opts.num_threads) schedule(dynamic, 65536)
    for (uint64_t i = begin; i < end; ++i) {
      const auto& e = graph.edges[i];
      if (reverse_edges) {
        db.put_edge(e.dst, e.src, std::string());
      } else {
        db.put_edge(e.src, e.dst, std::string());
      }
    }
    wait_for_background_work(db, opts, storage_dir, graph_tag, batch, total_batches);

    const double batch_seconds = now_seconds() - batch_begin;
    const double elapsed = now_seconds() - ingest_begin;
    const double edge_count = static_cast<double>(end - begin);
    const double rate = batch_seconds > 0.0 ? edge_count / batch_seconds / 1e6 : 0.0;
    const double progress = graph.edges.empty() ? 100.0 : (100.0 * static_cast<double>(end) / graph.edges.size());
    std::printf("[ingest] graph=%s batch=%zu/%zu edges=[%zu,%zu) threads=%zu progress=%.2f%% batch_time=%.4f elapsed=%.4f rate=%.4fM edges/s\n",
                graph_tag,
                batch + 1,
                total_batches,
                begin,
                end,
                opts.num_threads,
                progress,
                batch_seconds,
                elapsed,
                rate);
  }
}

void print_summary(const std::string& dataset_name,
                   double load_seconds,
                   double ingest_seconds,
                   long rss_ingest,
                   const lsmg_bench::BenchmarkResult& result,
                   long rss_bfs,
                   size_t edge_count,
                   double total_seconds) {
  const double ingest_bandwidth =
      ingest_seconds > 0.0 ? static_cast<double>(edge_count) / ingest_seconds / 1e6 : 0.0;
  std::printf(EXPOUT "Load: %.4f\n", load_seconds);
  std::printf(EXPOUT "Ingest: %.4f\n", ingest_seconds);
  std::printf(EXPOUT "BFS: %.4f\n", result.bfs_seconds);
  std::printf(EXPOUT "PR: %.4f\n", result.pr_seconds);
  std::printf(EXPOUT "CC: %.4f\n", result.cc_seconds);
  std::printf(EXPOUT "RSS_Ingest: %ld\n", rss_ingest);
  std::printf(EXPOUT "RSS_BFS: %ld\n", rss_bfs);
  std::printf("[result] bfs_checksum=%llu pr_sum=%.8f cc_components=%llu total=%.4f ingest_bw=%.4fM edges/s dataset=%s\n",
              static_cast<unsigned long long>(result.bfs_checksum),
              result.pr_sum,
              static_cast<unsigned long long>(result.cc_components),
              total_seconds,
              ingest_bandwidth,
              dataset_name.c_str());
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const double bench_begin = now_seconds();
    const Options opts = parse_options(argc, argv);
    log_config(opts);
    const StoragePaths storage_paths = make_storage_paths(opts);

    if (opts.reset && fs::exists(opts.storage_dir)) {
      fs::remove_all(opts.storage_dir);
    }
    fs::create_directories(opts.storage_dir);

    const double load_begin = now_seconds();
    std::printf("[load] reading bin32 graph\n");
    lsmg_bench::LoadedGraph graph = load_bin32_graph(opts.input_path);
    const double load_seconds = now_seconds() - load_begin;
    std::printf("[load] done vertices=%llu edges=%zu time=%.4f\n",
                static_cast<unsigned long long>(graph.num_vertices),
                graph.edges.size(),
                load_seconds);

    lsmg::LSMGraph* out_db = open_graph(opts, storage_paths.out_storage_dir, graph.num_vertices, "out-graph");
    lsmg::LSMGraph* in_db = open_graph(opts, storage_paths.in_storage_dir, graph.num_vertices, "in-graph");

    create_vertices(*out_db, graph.num_vertices, "out");
    create_vertices(*in_db, graph.num_vertices, "in");

    const double ingest_begin = now_seconds();
    ingest_single_graph(*out_db, graph, opts, storage_paths.out_storage_dir, false, "out");
    ingest_single_graph(*in_db, graph, opts, storage_paths.in_storage_dir, true, "in");
    const double ingest_seconds = now_seconds() - ingest_begin;
    const long rss_ingest = get_rss_bytes();

    lsmg_bench::BenchmarkConfig config;
    config.algo_threads = opts.algo_threads;
    config.bfs_rounds = opts.bfs_rounds;
    config.pr_iterations = opts.pr_iterations;
    config.cc_rounds = opts.cc_rounds;
    config.cc_neighbor_rounds = opts.cc_neighbor_rounds;
    config.pr_epsilon = opts.pr_epsilon;

    const lsmg_bench::BenchmarkResult result =
        lsmg_bench::run_lsmgraph_graph_benchmarks_gapbs(*out_db, *in_db, graph, config);
    const double total_seconds = now_seconds() - bench_begin;
    print_summary(opts.dataset_name,
                  load_seconds,
                  ingest_seconds,
                  rss_ingest,
                  result,
                  result.rss_bfs,
                  graph.edges.size(),
                  total_seconds);

    delete in_db;
    delete out_db;
    return 0;
  } catch (const std::exception& ex) {
    std::fprintf(stderr, "error: %s\n", ex.what());
    return 1;
  }
}
