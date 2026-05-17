#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <unistd.h>

#include <gflags/gflags.h>

#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/graph.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

#ifdef _OPENMP
#include <omp.h>
#else
inline int omp_get_thread_num() { return 0; }
#endif

#ifndef EXPOUT
#define EXPOUT "[EXPOUT]"
#endif

namespace fs = std::filesystem;
namespace rdb = ROCKSDB_NAMESPACE;

struct Edge32 {
  uint32_t src;
  uint32_t dst;
};

static_assert(sizeof(Edge32) == sizeof(uint32_t) * 2, "Edge32 must be 8 bytes");

struct LoadedGraph {
  std::vector<Edge32> edges;
  uint32_t num_vertices = 0;
};

struct BenchmarkResult {
  double bfs_seconds = 0.0;
  size_t bfs_checksum = 0;
  long rss_bfs = 0;
  double pr_seconds = 0.0;
  double pr_sum = 0.0;
  double cc_seconds = 0.0;
  size_t cc_components = 0;
};

DEFINE_string(file, "", "Input graph in bin32 format");
DEFINE_string(storage, "", "Storage directory for the RocksGraph database");
DEFINE_string(dataset, "", "Dataset name shown in logs");
DEFINE_uint64(threads,
              std::max<uint64_t>(1, std::thread::hardware_concurrency()),
              "RocksDB/background worker thread count");
DEFINE_uint64(algo_threads, 0, "Algorithm worker threads, defaults to --threads");
DEFINE_uint64(ingest_batch_edges, 1 << 20, "Edge count per AddEdgeBatch ingest call");
DEFINE_uint64(bfs_rounds, 20, "BFS rounds");
DEFINE_uint64(pr_iterations, 10, "PageRank iterations");
DEFINE_double(pr_epsilon, 0.0, "PageRank early-stop epsilon");
DEFINE_uint64(cc_rounds, 10, "Connected-components rounds");
DEFINE_uint64(cc_neighbor_rounds, 2, "Connected-components sampled neighbor rounds");
DEFINE_int32(update_policy, EDGE_UPDATE_ADAPTIVE, "Aster edge update policy");
DEFINE_int32(encoding_type, ENCODING_TYPE_NONE, "Aster edge encoding type");
DEFINE_bool(enable_bloom_filter, true, "Enable bloom filter");
DEFINE_bool(direct_io, false, "Enable direct IO for reads and flush/compaction");
DEFINE_bool(reset, false, "Remove existing storage directory before ingest");
DEFINE_bool(print_rocksdb_stats, false, "Print RocksDB internal stats at the end");
DEFINE_uint64(write_buffer_mb, 64, "RocksDB write buffer size in MB");
DEFINE_uint64(max_write_buffer_number, 4, "RocksDB max write buffer number");
DEFINE_uint64(block_cache_mb, 64, "Block cache size in MB");
DEFINE_uint64(max_subcompactions, 0, "RocksDB max subcompactions, 0 => auto");
DEFINE_int64(debug_vertex, -1, "Print one vertex's out/in neighbors and exit after ingest");

inline double seconds_since(const std::chrono::steady_clock::time_point& begin) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
}

inline long get_rss() {
  std::ifstream stat_file("/proc/self/stat");
  std::string line;
  std::getline(stat_file, line);
  std::istringstream iss(line);
  std::string token;
  for (int i = 0; i < 23; ++i) {
    iss >> token;
  }
  iss >> token;
  return std::stol(token) * sysconf(_SC_PAGESIZE);
}

inline size_t clamp_thread_count(size_t requested_threads, size_t work_items) {
  if (work_items == 0) {
    return 1;
  }
  return std::max<size_t>(1, std::min(requested_threads, work_items));
}

inline bool compare_and_swap(uint32_t& x, uint32_t old_val, uint32_t new_val) {
  return __sync_bool_compare_and_swap(&x, old_val, new_val);
}

void validate_flags() {
  if (FLAGS_file.empty()) {
    throw std::runtime_error("Please specify --file=<graph.bin32>");
  }
  if (FLAGS_threads == 0) {
    throw std::runtime_error("--threads must be > 0");
  }
  if (FLAGS_algo_threads == 0) {
    FLAGS_algo_threads = FLAGS_threads;
  }
  if (FLAGS_ingest_batch_edges == 0) {
    throw std::runtime_error("--ingest_batch_edges must be > 0");
  }
  if (FLAGS_debug_vertex < -1) {
    throw std::runtime_error("--debug_vertex must be >= 0");
  }
}

std::string dataset_name_from_path(const std::string& path) {
  fs::path p(path);
  return p.stem().empty() ? "dataset" : p.stem().string();
}

std::string resolve_dataset_name() {
  return FLAGS_dataset.empty() ? dataset_name_from_path(FLAGS_file) : FLAGS_dataset;
}

std::string resolve_storage_dir() {
  if (!FLAGS_storage.empty()) {
    return FLAGS_storage;
  }
  return (fs::path("/tmp") / ("aster-bench-" + resolve_dataset_name())).string();
}

LoadedGraph load_bin32_graph(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    throw std::runtime_error("Failed to open input file: " + path);
  }

  in.seekg(0, std::ios::end);
  std::streamsize bytes = in.tellg();
  in.seekg(0, std::ios::beg);
  if (bytes < 0 || bytes % static_cast<std::streamsize>(sizeof(Edge32)) != 0) {
    throw std::runtime_error("Input file is not a valid bin32 edge list");
  }

  LoadedGraph graph;
  graph.edges.resize(static_cast<size_t>(bytes / static_cast<std::streamsize>(sizeof(Edge32))));
  if (!graph.edges.empty()) {
    in.read(reinterpret_cast<char*>(graph.edges.data()), bytes);
    if (!in) {
      throw std::runtime_error("Failed to read all edges from input file");
    }
  }

  uint32_t max_vertex = 0;
  for (const auto& edge : graph.edges) {
    max_vertex = std::max(max_vertex, std::max(edge.src, edge.dst));
  }
  graph.num_vertices = graph.edges.empty() ? 0 : (max_vertex + 1);
  return graph;
}

void prepare_storage_dir(const fs::path& storage_dir, bool reset) {
  if (fs::exists(storage_dir)) {
    const bool non_empty = fs::is_directory(storage_dir) &&
                           fs::directory_iterator(storage_dir) != fs::directory_iterator();
    if (non_empty) {
      if (!reset) {
        throw std::runtime_error(
            "Storage directory is not empty: " + storage_dir.string() +
            ". Use --reset to remove it first.");
      }
      fs::remove_all(storage_dir);
    } else if (!fs::is_directory(storage_dir)) {
      if (!reset) {
        throw std::runtime_error("Storage path exists and is not a directory: " +
                                 storage_dir.string());
      }
      fs::remove(storage_dir);
    }
  }
  fs::create_directories(storage_dir);
}

void log_config(const std::string& storage_dir) {
  std::printf("[config] dataset=%s\n", resolve_dataset_name().c_str());
  std::printf("[config] file=%s\n", FLAGS_file.c_str());
  std::printf("[config] storage=%s\n", storage_dir.c_str());
  std::printf(
      "[config] threads=%llu algo_threads=%llu ingest_batch_edges=%llu update_policy=%d "
      "encoding_type=%d bfs_rounds=%llu pr_iterations=%llu pr_epsilon=%.6f cc_rounds=%llu "
      "cc_neighbor_rounds=%llu bloom=%d direct_io=%d write_buffer_mb=%llu "
      "max_write_buffer_number=%llu block_cache_mb=%llu max_subcompactions=%llu\n",
      static_cast<unsigned long long>(FLAGS_threads),
      static_cast<unsigned long long>(FLAGS_algo_threads),
      static_cast<unsigned long long>(FLAGS_ingest_batch_edges),
      FLAGS_update_policy,
      FLAGS_encoding_type,
      static_cast<unsigned long long>(FLAGS_bfs_rounds),
      static_cast<unsigned long long>(FLAGS_pr_iterations),
      FLAGS_pr_epsilon,
      static_cast<unsigned long long>(FLAGS_cc_rounds),
      static_cast<unsigned long long>(FLAGS_cc_neighbor_rounds),
      FLAGS_enable_bloom_filter ? 1 : 0,
      FLAGS_direct_io ? 1 : 0,
      static_cast<unsigned long long>(FLAGS_write_buffer_mb),
      static_cast<unsigned long long>(FLAGS_max_write_buffer_number),
      static_cast<unsigned long long>(FLAGS_block_cache_mb),
      static_cast<unsigned long long>(FLAGS_max_subcompactions));
}

rdb::Options build_options() {
  rdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.level_compaction_dynamic_level_bytes = false;
  options.statistics = rdb::CreateDBStatistics();
  options.write_buffer_size = static_cast<size_t>(FLAGS_write_buffer_mb) << 20;
  options.max_write_buffer_number = static_cast<int>(FLAGS_max_write_buffer_number);
  options.max_subcompactions = static_cast<uint32_t>(
      FLAGS_max_subcompactions == 0 ? std::min<uint64_t>(FLAGS_threads, 8) : FLAGS_max_subcompactions);
  options.IncreaseParallelism(static_cast<int>(FLAGS_threads));
  options.max_background_jobs = static_cast<int>(FLAGS_threads);
  if (FLAGS_direct_io) {
    options.use_direct_io_for_flush_and_compaction = true;
    options.use_direct_reads = true;
  }

  rdb::BlockBasedTableOptions table_options;
  table_options.block_cache =
      rdb::NewLRUCache(static_cast<size_t>(FLAGS_block_cache_mb) << 20);
  if (FLAGS_enable_bloom_filter) {
    table_options.filter_policy.reset(rdb::NewBloomFilterPolicy(10, false));
  }
  options.table_factory.reset(rdb::NewBlockBasedTableFactory(table_options));
  return options;
}

void init_vertices(rdb::RocksGraph& graph, uint32_t num_vertices) {
  const auto begin = std::chrono::steady_clock::now();
  if (FLAGS_update_policy == EDGE_UPDATE_EAGER) {
    graph.n = num_vertices;
    std::printf("[ingest] using eager mode, preset vertex_count=%u time=%.4f\n",
                num_vertices,
                seconds_since(begin));
    return;
  }

  std::printf("[ingest] creating %u vertices\n", num_vertices);
  for (uint32_t v = 0; v < num_vertices; ++v) {
    const auto status = graph.AddVertex(static_cast<rdb::node_id_t>(v));
    if (!status.ok()) {
      throw std::runtime_error("AddVertex failed: " + status.ToString());
    }
  }
  std::printf("[ingest] vertices done time=%.4f\n", seconds_since(begin));
}

void ingest_edges(rdb::RocksGraph& graph, const LoadedGraph& loaded) {
  const size_t total_batches = loaded.edges.empty()
                                   ? 0
                                   : ((loaded.edges.size() + FLAGS_ingest_batch_edges - 1) /
                                      FLAGS_ingest_batch_edges);
  std::printf("[ingest] loading %zu edges with batch_size=%llu\n",
              loaded.edges.size(),
              static_cast<unsigned long long>(FLAGS_ingest_batch_edges));
  const auto ingest_begin = std::chrono::steady_clock::now();
  for (size_t batch = 0; batch < total_batches; ++batch) {
    const size_t begin = batch * FLAGS_ingest_batch_edges;
    const size_t end = std::min(begin + static_cast<size_t>(FLAGS_ingest_batch_edges),
                                loaded.edges.size());
    std::vector<std::pair<rdb::node_id_t, rdb::node_id_t>> batch_edges;
    batch_edges.reserve(end - begin);
    for (size_t i = begin; i < end; ++i) {
      batch_edges.emplace_back(static_cast<rdb::node_id_t>(loaded.edges[i].src),
                               static_cast<rdb::node_id_t>(loaded.edges[i].dst));
    }
    const auto batch_begin = std::chrono::steady_clock::now();
    const auto status = graph.AddEdgeBatch(batch_edges);
    if (!status.ok()) {
      throw std::runtime_error("AddEdgeBatch failed: " + status.ToString());
    }
    const double batch_seconds = seconds_since(batch_begin);
    const double elapsed = seconds_since(ingest_begin);
    const double rate =
        batch_seconds > 0.0 ? static_cast<double>(batch_edges.size()) / batch_seconds / 1e6 : 0.0;
    const double progress =
        loaded.edges.empty() ? 100.0 : (100.0 * static_cast<double>(end) / loaded.edges.size());
    std::printf(
        "[ingest] batch=%zu/%zu edges=[%zu,%zu) progress=%.2f%% batch_time=%.4f elapsed=%.4f "
        "rate=%.4fM edges/s\n",
        batch + 1,
        total_batches,
        begin,
        end,
        progress,
        batch_seconds,
        elapsed,
        rate);
  }
}

class AsterNeighborOracle {
 public:
  explicit AsterNeighborOracle(rdb::RocksGraph& graph) : graph_(graph) {}

  template <typename Fn>
  void for_out_neighbors(size_t tid, uint32_t src, Fn&& fn) const {
    (void)tid;
    rdb::Edges edges;
    const auto status = graph_.GetAllEdges(static_cast<rdb::node_id_t>(src), &edges);
    if (!status.ok()) {
      return;
    }
    for (uint32_t i = 0; i < edges.num_edges_out; ++i) {
      if (!fn(static_cast<uint32_t>(edges.nxts_out[i].nxt))) {
        break;
      }
    }
    free_edges(&edges);
  }

  template <typename Fn>
  void for_in_neighbors(size_t tid, uint32_t dst, Fn&& fn) const {
    (void)tid;
    rdb::Edges edges;
    const auto status = graph_.GetAllEdges(static_cast<rdb::node_id_t>(dst), &edges);
    if (!status.ok()) {
      return;
    }
    for (uint32_t i = 0; i < edges.num_edges_in; ++i) {
      if (!fn(static_cast<uint32_t>(edges.nxts_in[i].nxt))) {
        break;
      }
    }
    free_edges(&edges);
  }

 private:
  rdb::RocksGraph& graph_;
};

std::vector<uint32_t> collect_actual_out_degree(rdb::RocksGraph& graph, uint32_t num_vertices) {
  std::vector<uint32_t> out_degree(num_vertices, 0);
  const int threads =
      static_cast<int>(clamp_thread_count(static_cast<size_t>(FLAGS_algo_threads), num_vertices));
#pragma omp parallel for num_threads(threads) schedule(dynamic, 16384)
  for (int64_t v = 0; v < static_cast<int64_t>(num_vertices); ++v) {
    rdb::Edges edges;
    const auto status = graph.GetAllEdges(static_cast<rdb::node_id_t>(v), &edges);
    if (status.ok()) {
      out_degree[static_cast<size_t>(v)] = edges.num_edges_out;
      free_edges(&edges);
    }
  }
  return out_degree;
}

template <typename ForOutNeighbors, typename ForInNeighbors>
size_t run_gapbs_bfs(uint32_t num_vertices,
                     uint32_t source,
                     size_t num_threads,
                     ForOutNeighbors&& for_out_neighbors,
                     ForInNeighbors&& for_in_neighbors) {
  if (num_vertices == 0) {
    return 0;
  }

  std::vector<uint32_t> status(num_vertices, 0);
  uint32_t level = 1;
  bool top_down = true;
  size_t frontier = 0;
  size_t visited_count = 1;
  status[source] = level;

  do {
    frontier = 0;
    const char* mode = top_down ? "top-down" : "bottom-up";
    const int threads = static_cast<int>(clamp_thread_count(num_threads, num_vertices));
    if (top_down) {
#pragma omp parallel for num_threads(threads) schedule(dynamic, 16384) reduction(+:frontier)
      for (int64_t v = 0; v < static_cast<int64_t>(num_vertices); ++v) {
        if (status[static_cast<size_t>(v)] != level) {
          continue;
        }
        const size_t tid = static_cast<size_t>(omp_get_thread_num());
        for_out_neighbors(tid, static_cast<uint32_t>(v), [&](uint32_t dst) {
          if (dst < num_vertices &&
              compare_and_swap(status[static_cast<size_t>(dst)], 0, level + 1)) {
            ++frontier;
          }
          return true;
        });
      }
    } else {
#pragma omp parallel for num_threads(threads) schedule(dynamic, 16384) reduction(+:frontier)
      for (int64_t v = 0; v < static_cast<int64_t>(num_vertices); ++v) {
        if (status[static_cast<size_t>(v)] != 0) {
          continue;
        }
        const size_t tid = static_cast<size_t>(omp_get_thread_num());
        for_in_neighbors(tid, static_cast<uint32_t>(v), [&](uint32_t src) {
          if (src < num_vertices && status[static_cast<size_t>(src)] == level &&
              compare_and_swap(status[static_cast<size_t>(v)], 0, level + 1)) {
            ++frontier;
            return false;
          }
          return status[static_cast<size_t>(v)] == 0;
        });
      }
    }
    visited_count += frontier;
    std::printf("[bfs] level=%u mode=%s new_frontier=%zu visited=%zu\n",
                level,
                mode,
                frontier,
                visited_count);
    top_down = !(frontier >= static_cast<size_t>(0.002 * static_cast<double>(num_vertices)));
    ++level;
  } while (frontier);

  return visited_count;
}

template <typename ForInNeighbors>
double run_gapbs_pr(uint32_t num_vertices,
                    const std::vector<uint32_t>& out_degree,
                    size_t num_threads,
                    size_t iterations,
                    double epsilon,
                    ForInNeighbors&& for_in_neighbors) {
  if (num_vertices == 0) {
    return 0.0;
  }

  using ScoreT = float;
  const ScoreT kDamp = 0.85f;
  const ScoreT init_score = 1.0f / static_cast<ScoreT>(num_vertices);
  const ScoreT base_score = (1.0f - kDamp) / static_cast<ScoreT>(num_vertices);

  std::vector<ScoreT> scores(num_vertices, init_score);
  std::vector<ScoreT> outgoing_contrib(num_vertices, 0.0f);
  const int threads = static_cast<int>(clamp_thread_count(num_threads, num_vertices));
#pragma omp parallel for num_threads(threads) schedule(static)
  for (int64_t n = 0; n < static_cast<int64_t>(num_vertices); ++n) {
    outgoing_contrib[static_cast<size_t>(n)] =
        out_degree[static_cast<size_t>(n)] == 0
            ? 0.0f
            : init_score / static_cast<ScoreT>(out_degree[static_cast<size_t>(n)]);
  }

  for (size_t iter = 0; iter < iterations; ++iter) {
    const auto iter_begin = std::chrono::steady_clock::now();
    std::vector<ScoreT> prev_outgoing_contrib = outgoing_contrib;
    double error = 0.0;
#pragma omp parallel for num_threads(threads) schedule(dynamic, 16384) reduction(+:error)
    for (int64_t u = 0; u < static_cast<int64_t>(num_vertices); ++u) {
      ScoreT incoming_total = 0.0f;
      bool has_incoming = false;
      const size_t tid = static_cast<size_t>(omp_get_thread_num());
      for_in_neighbors(tid, static_cast<uint32_t>(u), [&](uint32_t src) {
        if (src < num_vertices) {
          has_incoming = true;
          incoming_total += prev_outgoing_contrib[static_cast<size_t>(src)];
        }
        return true;
      });
      if (!has_incoming) {
        continue;
      }
      const ScoreT old_score = scores[static_cast<size_t>(u)];
      scores[static_cast<size_t>(u)] = base_score + kDamp * incoming_total;
      error += std::fabs(scores[static_cast<size_t>(u)] - old_score);
      outgoing_contrib[static_cast<size_t>(u)] =
          out_degree[static_cast<size_t>(u)] == 0
              ? 0.0f
              : scores[static_cast<size_t>(u)] /
                    static_cast<ScoreT>(out_degree[static_cast<size_t>(u)]);
    }
    if (error < epsilon) {
      std::printf("[pr] iter=%zu error=%.6f time=%.4f early-stop=1\n",
                  iter + 1,
                  error,
                  seconds_since(iter_begin));
      break;
    }
    std::printf("[pr] iter=%zu error=%.6f time=%.4f\n",
                iter + 1,
                error,
                seconds_since(iter_begin));
  }

  double sum = 0.0;
  for (ScoreT score : scores) {
    sum += score;
  }
  return sum;
}

inline void link_components(uint32_t u, uint32_t v, uint32_t* comp) {
  uint32_t p1 = comp[u];
  uint32_t p2 = comp[v];
  while (p1 != p2) {
    const uint32_t high = p1 > p2 ? p1 : p2;
    const uint32_t low = p1 + (p2 - high);
    const uint32_t p_high = comp[high];
    if ((p_high == low) || (p_high == high && compare_and_swap(comp[high], high, low))) {
      break;
    }
    p1 = comp[comp[high]];
    p2 = comp[low];
  }
}

inline void compress_components(uint32_t num_vertices, uint32_t* comp, size_t num_threads) {
  const int threads = static_cast<int>(clamp_thread_count(num_threads, num_vertices));
#pragma omp parallel for num_threads(threads) schedule(dynamic, 16384)
  for (int64_t n = 0; n < static_cast<int64_t>(num_vertices); ++n) {
    while (comp[static_cast<size_t>(n)] != comp[comp[static_cast<size_t>(n)]]) {
      comp[static_cast<size_t>(n)] = comp[comp[static_cast<size_t>(n)]];
    }
  }
}

inline uint32_t sample_frequent_element(uint32_t* comp,
                                        uint32_t num_vertices,
                                        size_t num_samples = 1024) {
  std::unordered_map<uint32_t, int> sample_counts(32);
  std::mt19937 gen;
  std::uniform_int_distribution<uint32_t> distribution(0, num_vertices - 1);
  for (size_t i = 0; i < num_samples; ++i) {
    const uint32_t n = distribution(gen);
    sample_counts[comp[n]]++;
  }
  const auto most_frequent = std::max_element(
      sample_counts.begin(),
      sample_counts.end(),
      [](const auto& a, const auto& b) { return a.second < b.second; });
  const float frac_of_graph =
      static_cast<float>(most_frequent->second) / static_cast<float>(num_samples);
  std::printf("[cc] skipping largest intermediate component id=%u approx=%.2f%%\n",
              most_frequent->first,
              frac_of_graph * 100.0f);
  return most_frequent->first;
}

template <typename KeyT, typename ValT>
inline std::vector<std::pair<ValT, KeyT>> top_k_components(
    const std::vector<std::pair<KeyT, ValT>>& to_sort, size_t k) {
  std::vector<std::pair<ValT, KeyT>> top_k;
  ValT min_so_far = 0;
  for (const auto& kvp : to_sort) {
    if ((top_k.size() < k) || (kvp.second > min_so_far)) {
      top_k.push_back(std::make_pair(kvp.second, kvp.first));
      std::sort(top_k.begin(), top_k.end(), std::greater<std::pair<ValT, KeyT>>());
      if (top_k.size() > k) {
        top_k.resize(k);
      }
      min_so_far = top_k.back().first;
    }
  }
  return top_k;
}

inline void print_component_stats(const uint32_t* comp, uint32_t num_vertices) {
  std::unordered_map<uint32_t, uint32_t> count;
  uint32_t largest_size = 0;
  for (uint32_t i = 0; i < num_vertices; ++i) {
    const uint32_t size = ++count[comp[i]];
    largest_size = std::max(largest_size, size);
  }

  const float largest_ratio =
      num_vertices == 0 ? 0.0f : static_cast<float>(largest_size) / static_cast<float>(num_vertices);
  std::printf("[cc] largest_component=%u largest_component_ratio=%.6f\n",
              largest_size,
              largest_ratio);

  std::vector<std::pair<uint32_t, uint32_t>> count_vector;
  count_vector.reserve(count.size());
  for (const auto& kvp : count) {
    count_vector.push_back(kvp);
  }
  const int k = std::min<int>(5, static_cast<int>(count_vector.size()));
  const auto top_k = top_k_components(count_vector, static_cast<size_t>(k));
  std::printf("[cc] %d biggest clusters\n", k);
  for (const auto& kvp : top_k) {
    std::printf("[cc] %u:%u\n", kvp.second, kvp.first);
  }
  std::printf("[cc] components=%zu\n", count.size());
}

template <typename ForOutNeighbors, typename ForInNeighbors>
size_t run_gapbs_cc(uint32_t num_vertices,
                    size_t neighbor_rounds,
                    size_t num_threads,
                    ForOutNeighbors&& for_out_neighbors,
                    ForInNeighbors&& for_in_neighbors) {
  if (num_vertices == 0) {
    return 0;
  }

  std::vector<uint32_t> comp(num_vertices);
  const int threads = static_cast<int>(clamp_thread_count(num_threads, num_vertices));
#pragma omp parallel for num_threads(threads) schedule(static)
  for (int64_t v = 0; v < static_cast<int64_t>(num_vertices); ++v) {
    comp[static_cast<size_t>(v)] = static_cast<uint32_t>(v);
  }

  const auto sample_begin = std::chrono::steady_clock::now();
#pragma omp parallel for num_threads(threads) schedule(dynamic, 16384)
  for (int64_t u = 0; u < static_cast<int64_t>(num_vertices); ++u) {
    size_t d = 0;
    const size_t tid = static_cast<size_t>(omp_get_thread_num());
    for_out_neighbors(tid, static_cast<uint32_t>(u), [&](uint32_t v) {
      if (d < neighbor_rounds && v < num_vertices) {
        link_components(static_cast<uint32_t>(u), v, comp.data());
        ++d;
        return d < neighbor_rounds;
      }
      return false;
    });
  }
  compress_components(num_vertices, comp.data(), num_threads);

  const uint32_t largest_component = sample_frequent_element(comp.data(), num_vertices);
  std::printf("[cc] sample+compress time=%.4f largest_component=%u\n",
              seconds_since(sample_begin),
              largest_component);

  const auto final_begin = std::chrono::steady_clock::now();
#pragma omp parallel for num_threads(threads) schedule(dynamic, 16384)
  for (int64_t u = 0; u < static_cast<int64_t>(num_vertices); ++u) {
    if (comp[static_cast<size_t>(u)] == largest_component) {
      continue;
    }
    size_t d = 0;
    const size_t tid = static_cast<size_t>(omp_get_thread_num());
    for_out_neighbors(tid, static_cast<uint32_t>(u), [&](uint32_t v) {
      if (d > neighbor_rounds && v < num_vertices) {
        link_components(static_cast<uint32_t>(u), v, comp.data());
      }
      ++d;
      return true;
    });

    d = 0;
    for_in_neighbors(tid, static_cast<uint32_t>(u), [&](uint32_t v) {
      if (d > neighbor_rounds && v < num_vertices) {
        link_components(static_cast<uint32_t>(u), v, comp.data());
      }
      ++d;
      return true;
    });
  }
  compress_components(num_vertices, comp.data(), num_threads);
  std::printf("[cc] final-link+compress time=%.4f\n", seconds_since(final_begin));
  print_component_stats(comp.data(), num_vertices);

  size_t components = 0;
  for (uint32_t v = 0; v < num_vertices; ++v) {
    if (comp[v] == v) {
      ++components;
    }
  }
  return components;
}

BenchmarkResult run_benchmarks(rdb::RocksGraph& graph, const LoadedGraph& loaded) {
  BenchmarkResult result;
  AsterNeighborOracle oracle(graph);
  const std::vector<uint32_t> out_degree = collect_actual_out_degree(graph, loaded.num_vertices);

  for (size_t round = 0; round < FLAGS_bfs_rounds; ++round) {
    const uint32_t source =
        loaded.num_vertices == 0 ? 0 : static_cast<uint32_t>(round % loaded.num_vertices);
    const auto round_begin = std::chrono::steady_clock::now();
    std::printf("[bfs] round=%zu source=%u start\n", round, source);
    const size_t visited = run_gapbs_bfs(
        loaded.num_vertices,
        source,
        static_cast<size_t>(FLAGS_algo_threads),
        [&](size_t tid, uint32_t src, auto&& fn) { oracle.for_out_neighbors(tid, src, fn); },
        [&](size_t tid, uint32_t dst, auto&& fn) { oracle.for_in_neighbors(tid, dst, fn); });
    result.bfs_checksum += visited;
    const double round_seconds = seconds_since(round_begin);
    result.bfs_seconds += round_seconds;
    std::printf("[bfs] round=%zu source=%u visited=%zu time=%.4f\n",
                round,
                source,
                visited,
                round_seconds);
  }
  result.rss_bfs = get_rss();

  const auto pr_begin = std::chrono::steady_clock::now();
  std::printf("[pr] start iterations=%llu epsilon=%.6f\n",
              static_cast<unsigned long long>(FLAGS_pr_iterations),
              FLAGS_pr_epsilon);
  result.pr_sum = run_gapbs_pr(
      loaded.num_vertices,
      out_degree,
      static_cast<size_t>(FLAGS_algo_threads),
      static_cast<size_t>(FLAGS_pr_iterations),
      FLAGS_pr_epsilon,
      [&](size_t tid, uint32_t dst, auto&& fn) { oracle.for_in_neighbors(tid, dst, fn); });
  result.pr_seconds = seconds_since(pr_begin);
  std::printf("[pr] done sum=%.8f time=%.4f\n", result.pr_sum, result.pr_seconds);

  const auto cc_begin = std::chrono::steady_clock::now();
  for (size_t round = 0; round < FLAGS_cc_rounds; ++round) {
    const auto round_begin = std::chrono::steady_clock::now();
    std::printf("[cc] round=%zu start neighbor_rounds=%llu\n",
                round,
                static_cast<unsigned long long>(FLAGS_cc_neighbor_rounds));
    result.cc_components = run_gapbs_cc(
        loaded.num_vertices,
        static_cast<size_t>(FLAGS_cc_neighbor_rounds),
        static_cast<size_t>(FLAGS_algo_threads),
        [&](size_t tid, uint32_t src, auto&& fn) { oracle.for_out_neighbors(tid, src, fn); },
        [&](size_t tid, uint32_t dst, auto&& fn) { oracle.for_in_neighbors(tid, dst, fn); });
    std::printf("[cc] round=%zu done components=%zu time=%.4f\n",
                round,
                result.cc_components,
                seconds_since(round_begin));
  }
  result.cc_seconds = seconds_since(cc_begin);
  return result;
}

void print_debug_vertex(rdb::RocksGraph& graph, uint32_t vertex) {
  rdb::Edges edges;
  const auto status = graph.GetAllEdges(static_cast<rdb::node_id_t>(vertex), &edges);
  if (!status.ok()) {
    std::printf("[debug] vertex=%u status=%s\n", vertex, status.ToString().c_str());
    return;
  }
  std::vector<uint32_t> out_neighbors;
  std::vector<uint32_t> in_neighbors;
  out_neighbors.reserve(edges.num_edges_out);
  in_neighbors.reserve(edges.num_edges_in);
  for (uint32_t i = 0; i < edges.num_edges_out; ++i) {
    out_neighbors.push_back(static_cast<uint32_t>(edges.nxts_out[i].nxt));
  }
  for (uint32_t i = 0; i < edges.num_edges_in; ++i) {
    in_neighbors.push_back(static_cast<uint32_t>(edges.nxts_in[i].nxt));
  }
  free_edges(&edges);
  std::sort(out_neighbors.begin(), out_neighbors.end());
  std::sort(in_neighbors.begin(), in_neighbors.end());
  std::printf("[debug] vertex=%u out_degree=%zu in_degree=%zu\n",
              vertex,
              out_neighbors.size(),
              in_neighbors.size());
  std::printf("[debug] out_neighbors_sorted=");
  for (size_t i = 0; i < out_neighbors.size(); ++i) {
    std::printf("%s%u", i == 0 ? "" : ",", out_neighbors[i]);
  }
  std::printf("\n");
  std::printf("[debug] in_neighbors_sorted=");
  for (size_t i = 0; i < in_neighbors.size(); ++i) {
    std::printf("%s%u", i == 0 ? "" : ",", in_neighbors[i]);
  }
  std::printf("\n");
}

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Aster bin32 benchmark aligned with GraphOne/BACH/LiveGraph");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  try {
    validate_flags();
    const std::string storage_dir = resolve_storage_dir();
    prepare_storage_dir(storage_dir, FLAGS_reset);
    log_config(storage_dir);

    std::printf("[load] reading bin32 graph\n");
    const auto load_begin = std::chrono::steady_clock::now();
    const LoadedGraph loaded = load_bin32_graph(FLAGS_file);
    const double load_seconds = seconds_since(load_begin);
    std::printf("[load] done vertices=%u edges=%zu time=%.4f\n",
                loaded.num_vertices,
                loaded.edges.size(),
                load_seconds);

    std::printf("[db] opening RocksGraph\n");
    auto options = build_options();
    rdb::RocksGraph graph(
        options,
        FLAGS_update_policy,
        FLAGS_encoding_type,
        false,
        storage_dir);
    std::printf("[db] ready background_jobs=%d max_subcompactions=%u write_buffer_size=%zu\n",
                options.max_background_jobs,
                options.max_subcompactions,
                options.write_buffer_size);

    init_vertices(graph, loaded.num_vertices);
    const long rss_ingest_begin = get_rss();
    const auto ingest_begin = std::chrono::steady_clock::now();
    ingest_edges(graph, loaded);
    const double ingest_seconds = seconds_since(ingest_begin);
    const long rss_ingest = get_rss();
    std::printf("[ingest] done edges=%zu time=%.4f rss_delta=%ld\n",
                loaded.edges.size(),
                ingest_seconds,
                rss_ingest - rss_ingest_begin);

    if (FLAGS_debug_vertex >= 0) {
      print_debug_vertex(graph, static_cast<uint32_t>(FLAGS_debug_vertex));
      return 0;
    }

    BenchmarkResult result = run_benchmarks(graph, loaded);
    std::printf("[result] bfs_checksum=%zu pr_sum=%.8f cc_components=%zu\n",
                result.bfs_checksum,
                result.pr_sum,
                result.cc_components);

    if (FLAGS_print_rocksdb_stats) {
      std::string stats;
      graph.GetRocksDBStats(stats);
      std::printf("[result] rocksdb.stats\n%s\n", stats.c_str());
      if (options.statistics) {
        std::printf("[result] statistics\n%s\n", options.statistics->ToString().c_str());
      }
    }

    std::printf("%sLoad: %.4f\n", EXPOUT, load_seconds);
    std::printf("%sIngest: %.4f\n", EXPOUT, ingest_seconds);
    std::printf("%sBFS: %.4f\n", EXPOUT, result.bfs_seconds);
    std::printf("%sPR: %.4f\n", EXPOUT, result.pr_seconds);
    std::printf("%sCC: %.4f\n", EXPOUT, result.cc_seconds);
    std::printf("%sRSS_Ingest: %ld\n", EXPOUT, rss_ingest);
    std::printf("%sRSS_BFS: %ld\n", EXPOUT, result.rss_bfs);
    return 0;
  } catch (const std::exception& ex) {
    std::fprintf(stderr, "Error: %s\n", ex.what());
    return 1;
  }
}
