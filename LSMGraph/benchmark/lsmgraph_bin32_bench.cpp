#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
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
#include "../../../scatter_graph/script/query_bench.hpp"

namespace fs = std::filesystem;

namespace {

struct Options {
  std::string input_path;
  std::string storage_dir;
  std::string dataset_name;
  size_t num_vertices = 0;  // required, passed from driver via --num-vertices
  size_t num_threads = 1;
  size_t algo_threads = 1;
  size_t memtable_num = 2;
  size_t bfs_rounds = 20;
  size_t pr_iterations = 10;
  size_t cc_rounds = 10;
  size_t cc_neighbor_rounds = 2;
  double pr_epsilon = 0.0;
  bool reset = false;
  bool skip_analytics = false;
  size_t max_batch = 0;  // 0 = no limit; otherwise stop after this many 16MiB batches
  query_bench::QueryOptions query;
};

// Streaming chunk: 16 MiB (8 bytes per edge → 2M edges per chunk).
constexpr size_t kStreamChunkBytes = 16ull << 20;
constexpr size_t kStreamChunkEdges = kStreamChunkBytes / (2 * sizeof(uint32_t));
constexpr size_t kDefaultIngestBatchEdges = kStreamChunkEdges;

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
               "  --skip-analytics         Skip BFS/PR/CC after query phase\n"
               "  --reset                  Remove storage dir before ingest\n"
               "  --max-batch <n>          Stop ingest after N 16MiB batches (0=no limit)\n"
               "  --query-pool-size <n>    Point-query vertex pool size, default 10000\n"
               "  --query-uniform-count <n> Point-Uniform query count, default 1000000\n"
               "  --query-skew-count <n>   Point-Skew query count, default 1000000\n"
               "  --query-zipf-alpha <x>   Point-Skew Zipf alpha, default 1.2\n"
               "  --query-pool-seed <n>    Query pool RNG seed, default 42\n"
               "  --query-seed-base <n>    Query stream seed base, default 43\n",
               argv0);
}

Options parse_options(int argc, char** argv) {
  Options opts;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "-f" || arg == "--input") {
      opts.input_path = require_value(arg, i, argc, argv);
    } else if (arg == "--num-vertices") {
      opts.num_vertices = std::stoull(require_value(arg, i, argc, argv));
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
    } else if (arg == "--skip-analytics") {
      opts.skip_analytics = true;
    } else if (arg == "--max-batch") {
      opts.max_batch = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--query-pool-size") {
      opts.query.pool_size = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--query-uniform-count") {
      opts.query.uniform_count = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--query-skew-count") {
      opts.query.skew_count = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--query-zipf-alpha") {
      opts.query.zipf_alpha = std::stod(require_value(arg, i, argc, argv));
    } else if (arg == "--query-pool-seed") {
      opts.query.pool_seed = std::stoull(require_value(arg, i, argc, argv));
    } else if (arg == "--query-seed-base") {
      opts.query.seed_base = std::stoull(require_value(arg, i, argc, argv));
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
  if (opts.num_vertices == 0) {
    throw std::runtime_error("--num-vertices is required (from datasets.py)");
  }
  if (opts.num_threads == 0 || opts.algo_threads == 0 || opts.memtable_num == 0) {
    throw std::runtime_error("thread counts and memtable count must be > 0");
  }
  if (opts.dataset_name.empty()) {
    opts.dataset_name = fs::path(opts.input_path).stem().string();
  }
  opts.query.threads = opts.algo_threads;
  return opts;
}

void log_config(const Options& opts) {
  std::printf("[config] dataset=%s\n", opts.input_path.c_str());
  std::printf("[config] storage=%s\n", opts.storage_dir.c_str());
  std::printf("[config] batch_size=%zu threads=%zu algo_threads=%zu bfs_rounds=%zu pr_iters=%zu pr_epsilon=%.6f cc_rounds=%zu cc_neighbor_rounds=%zu memtable_num=%zu max_subcompactions=8\n",
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

lsmg_bench::LoadedGraph make_empty_loaded_graph(uint64_t num_vertices) {
  lsmg_bench::LoadedGraph graph;
  graph.num_vertices = num_vertices;
  graph.out_degree.assign(num_vertices, 0);
  graph.in_degree.assign(num_vertices, 0);
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
  FLAGS_max_subcompactions = 8;
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

size_t stream_ingest_graph(lsmg::LSMGraph& db,
                           lsmg_bench::LoadedGraph& graph,
                           const Options& opts,
                           const std::string& storage_dir,
                           bool reverse_edges,
                           bool tally_degrees,
                           const char* graph_tag) {
  configure_lsmgraph_flags(opts, storage_dir);
  std::printf("[ingest] streaming edges graph=%s reverse=%d from %s with chunk_edges=%zu (%zu MiB)\n",
              graph_tag,
              reverse_edges ? 1 : 0,
              opts.input_path.c_str(),
              kStreamChunkEdges,
              kStreamChunkBytes >> 20);

  std::FILE* fp = std::fopen(opts.input_path.c_str(), "rb");
  if (!fp) {
    throw std::runtime_error("failed to open input file: " + opts.input_path);
  }

  std::fseek(fp, 0, SEEK_END);
  const long file_bytes = std::ftell(fp);
  std::fseek(fp, 0, SEEK_SET);
  const size_t edge_size = 2 * sizeof(uint32_t);
  if (file_bytes < 0 || file_bytes % static_cast<long>(edge_size) != 0) {
    std::fclose(fp);
    throw std::runtime_error("input file size is not aligned to bin32 edge pairs");
  }
  const size_t total_edges = static_cast<size_t>(file_bytes) / edge_size;
  const size_t total_batches =
      total_edges == 0 ? 0 : (total_edges + kStreamChunkEdges - 1) / kStreamChunkEdges;

  std::vector<lsmg_bench::LoadedGraph::EdgePair> chunk(kStreamChunkEdges);
  size_t cursor = 0;
  size_t batch_id = 0;
  const double ingest_begin = now_seconds();
  while (true) {
    const size_t got = std::fread(chunk.data(), edge_size, kStreamChunkEdges, fp);
    if (got == 0) break;

    const double batch_begin = now_seconds();

#pragma omp parallel for num_threads(opts.num_threads) schedule(dynamic, 65536)
    for (uint64_t i = 0; i < got; ++i) {
      const auto& e = chunk[i];
      if (reverse_edges) {
        db.put_edge(e.dst, e.src, std::string());
      } else {
        db.put_edge(e.src, e.dst, std::string());
      }
    }

    if (tally_degrees) {
      for (size_t i = 0; i < got; ++i) {
        const auto& e = chunk[i];
        if (e.src < graph.num_vertices) graph.out_degree[e.src]++;
        if (e.dst < graph.num_vertices) graph.in_degree[e.dst]++;
      }
    }

    cursor += got;
    ++batch_id;
    const double batch_seconds = now_seconds() - batch_begin;
    const double elapsed = now_seconds() - ingest_begin;
    const double rate = batch_seconds > 0.0 ? static_cast<double>(got) / batch_seconds / 1e6 : 0.0;
    const double progress = total_edges == 0 ? 100.0
                                              : 100.0 * static_cast<double>(cursor) /
                                                    static_cast<double>(total_edges);
    std::printf("[ingest] graph=%s batch=%zu/%zu edges=%zu cursor=%zu threads=%zu progress=%.2f%% batch_time=%.4f elapsed=%.4f rate=%.4fM edges/s\n",
                graph_tag, batch_id, total_batches, got, cursor, opts.num_threads,
                progress, batch_seconds, elapsed, rate);

    if (got < kStreamChunkEdges) break;  // EOF reached
    if (opts.max_batch != 0 && batch_id >= opts.max_batch) {
      std::printf("[ingest] reached --max-batch=%zu, stopping early\n", opts.max_batch);
      break;
    }
  }

  const double wait_begin = now_seconds();
  db.WaitForBackgroundWork();
  std::printf("[ingest] graph=%s final_background_wait=%.4f\n",
              graph_tag,
              now_seconds() - wait_begin);

  std::fclose(fp);
  return cursor;
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
    // Driver supplied num_vertices via --num-vertices, no input scan needed.
    lsmg_bench::LoadedGraph graph = make_empty_loaded_graph(opts.num_vertices);
    const double load_seconds = now_seconds() - load_begin;
    std::printf("[load] vertices=%llu (from --num-vertices) time=%.4f\n",
                static_cast<unsigned long long>(graph.num_vertices),
                load_seconds);

    lsmg::LSMGraph* out_db = open_graph(opts, storage_paths.out_storage_dir, graph.num_vertices, "out-graph");
    lsmg::LSMGraph* in_db = open_graph(opts, storage_paths.in_storage_dir, graph.num_vertices, "in-graph");

    create_vertices(*out_db, graph.num_vertices, "out");
    create_vertices(*in_db, graph.num_vertices, "in");

    const double ingest_begin = now_seconds();
    const size_t total_ingested = stream_ingest_graph(
        *out_db, graph, opts, storage_paths.out_storage_dir,
        false, true, "out");
    const size_t total_reverse_ingested = stream_ingest_graph(
        *in_db, graph, opts, storage_paths.in_storage_dir,
        true, false, "in");
    if (total_reverse_ingested != total_ingested) {
      throw std::runtime_error("out/in ingest edge counts differ");
    }
    const double ingest_seconds = now_seconds() - ingest_begin;
    const long rss_ingest = get_rss_bytes();

    lsmg_bench::LSMGraphNeighborOracle query_oracle(*out_db, *in_db);
    auto query_walker = [&](uint32_t v) -> uint64_t {
      uint64_t degree = 0;
      query_oracle.for_out_neighbors(static_cast<size_t>(omp_get_thread_num()), v,
                                     [&](uint32_t /*dst*/) {
                                       ++degree;
                                       return true;
                                     });
      return degree;
    };
    query_bench::run_query_phase(graph.num_vertices, query_walker, opts.query, std::cout);

    lsmg_bench::BenchmarkConfig config;
    config.algo_threads = opts.algo_threads;
    config.bfs_rounds = opts.bfs_rounds;
    config.pr_iterations = opts.pr_iterations;
    config.cc_rounds = opts.cc_rounds;
    config.cc_neighbor_rounds = opts.cc_neighbor_rounds;
    config.pr_epsilon = opts.pr_epsilon;

    lsmg_bench::BenchmarkResult result;
    if (!opts.skip_analytics) {
      result = lsmg_bench::run_lsmgraph_graph_benchmarks_gapbs(
          *out_db, *in_db, graph, config);
    }
    const double total_seconds = now_seconds() - bench_begin;
    print_summary(opts.dataset_name,
                  load_seconds,
                  ingest_seconds,
                  rss_ingest,
                  result,
                  result.rss_bfs,
                  total_ingested,
                  total_seconds);

    delete in_db;
    delete out_db;
    return 0;
  } catch (const std::exception& ex) {
    std::fprintf(stderr, "error: %s\n", ex.what());
    return 1;
  }
}
