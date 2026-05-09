#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <numeric>
#include <omp.h>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <vector>

#include "BACH/BACH.h"

namespace fs = std::filesystem;

#ifndef EXPOUT
#define EXPOUT "[EXPOUT]"
#endif

struct Edge32 {
    uint32_t src;
    uint32_t dst;
};

static_assert(sizeof(Edge32) == sizeof(uint32_t) * 2, "Edge32 must be 8 bytes");

struct Options {
    std::string input_path;
    std::string storage_dir;
    size_t ingest_batch_edges = 1 << 20;
    size_t num_threads =
        std::max<size_t>(1, std::thread::hardware_concurrency() == 0
                                ? 1
                                : std::thread::hardware_concurrency());
    size_t algo_threads = 0;
    size_t pr_iterations = 10;
    double pr_epsilon = 0.0;
    size_t bfs_rounds = 20;
    size_t cc_neighbor_rounds = 2;
    bool reset = false;
    bool compact = false;
};

struct LoadedGraph {
    std::vector<Edge32> edges;
    std::vector<uint32_t> out_degree;
    std::vector<uint32_t> in_degree;
    uint32_t num_vertices = 0;
};

void print_usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0 << " -f <graph.bin32> [options]\n"
        << "Options:\n"
        << "  --storage <dir>       Storage root, default ../data/bach-bench/<dataset>\n"
        << "  --ingest-batch-edges <n>  Edge count per ingest transaction, default 1048576\n"
        << "  --threads <n>         Worker threads for ingest, default hardware concurrency\n"
        << "  --algo-threads <n>    Worker threads for BFS/PR/CC, default same as --threads\n"
        << "  --pr-iters <n>        PageRank iterations, default 10\n"
        << "  --pr-epsilon <x>      PageRank early-stop epsilon, default 0\n"
        << "  --bfs-rounds <n>      BFS rounds, default 20\n"
        << "  --cc-neighbor-rounds <n>  CC sampled neighbor rounds, default 2\n"
        << "  --compact             Run DB::CompactAll() after ingest\n"
        << "  --reset               Remove existing storage dir before ingest\n";
}

Options parse_args(int argc, char* argv[]) {
    Options opts;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto require_value = [&](const char* name) -> std::string {
            if (i + 1 >= argc) {
                throw std::runtime_error(std::string("Missing value for ") + name);
            }
            return argv[++i];
        };

        if (arg == "-f" || arg == "--file") {
            opts.input_path = require_value(arg.c_str());
        } else if (arg == "--storage") {
            opts.storage_dir = require_value("--storage");
        } else if (arg == "--ingest-batch-edges") {
            opts.ingest_batch_edges = std::stoull(require_value("--ingest-batch-edges"));
        } else if (arg == "--threads") {
            opts.num_threads = std::stoull(require_value("--threads"));
        } else if (arg == "--algo-threads") {
            opts.algo_threads = std::stoull(require_value("--algo-threads"));
        } else if (arg == "--pr-iters") {
            opts.pr_iterations = std::stoull(require_value("--pr-iters"));
        } else if (arg == "--pr-epsilon") {
            opts.pr_epsilon = std::stod(require_value("--pr-epsilon"));
        } else if (arg == "--bfs-rounds") {
            opts.bfs_rounds = std::stoull(require_value("--bfs-rounds"));
        } else if (arg == "--cc-neighbor-rounds") {
            opts.cc_neighbor_rounds = std::stoull(require_value("--cc-neighbor-rounds"));
        } else if (arg == "--reset") {
            opts.reset = true;
        } else if (arg == "--compact") {
            opts.compact = true;
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            std::exit(0);
        } else {
            throw std::runtime_error("Unknown argument: " + arg);
        }
    }

    if (opts.input_path.empty()) {
        throw std::runtime_error("Please specify -f <graph.bin32>");
    }
    if (opts.algo_threads == 0) {
        opts.algo_threads = opts.num_threads;
    }
    return opts;
}

double seconds_since(const std::chrono::steady_clock::time_point& begin) {
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
}

long get_rss() {
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

size_t clamp_thread_count(size_t requested_threads, size_t work_items) {
    if (work_items == 0) {
        return 1;
    }
    return std::max<size_t>(1, std::min(requested_threads, work_items));
}

std::string dataset_name_from_path(const std::string& path) {
    fs::path p(path);
    return p.stem().empty() ? "dataset" : p.stem().string();
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

    size_t edge_count = static_cast<size_t>(bytes / static_cast<std::streamsize>(sizeof(Edge32)));
    LoadedGraph graph;
    graph.edges.resize(edge_count);
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
    graph.out_degree.assign(graph.num_vertices, 0);
    graph.in_degree.assign(graph.num_vertices, 0);
    for (const auto& edge : graph.edges) {
        ++graph.out_degree[edge.src];
        ++graph.in_degree[edge.dst];
    }
    return graph;
}

void prepare_storage_dir(const fs::path& storage_dir, bool reset) {
    if (fs::exists(storage_dir)) {
        bool non_empty = fs::is_directory(storage_dir) &&
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
                throw std::runtime_error(
                    "Storage path already exists and is not a directory: " + storage_dir.string());
            }
            fs::remove(storage_dir);
        }
    }
    fs::create_directories(storage_dir);
}

uint32_t select_bfs_source(size_t round, const std::vector<uint32_t>& out_degree) {
    if (out_degree.empty()) {
        return 0;
    }
    size_t start = round % out_degree.size();
    for (size_t i = 0; i < out_degree.size(); ++i) {
        size_t idx = (start + i) % out_degree.size();
        if (out_degree[idx] > 0) {
            return static_cast<uint32_t>(idx);
        }
    }
    return static_cast<uint32_t>(start);
}

bool compare_and_swap(uint32_t& x, uint32_t old_val, uint32_t new_val) {
    return __sync_bool_compare_and_swap(&x, old_val, new_val);
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
        int threads = static_cast<int>(clamp_thread_count(num_threads, num_vertices));
        if (top_down) {
            #pragma omp parallel for num_threads(threads) schedule(dynamic, 16384) reduction(+:frontier)
            for (int64_t v = 0; v < static_cast<int64_t>(num_vertices); ++v) {
                if (status[static_cast<size_t>(v)] != level) {
                    continue;
                }
                size_t tid = static_cast<size_t>(omp_get_thread_num());
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
                size_t tid = static_cast<size_t>(omp_get_thread_num());
                for_in_neighbors(tid, static_cast<uint32_t>(v), [&](uint32_t src) {
                    if (src < num_vertices &&
                        status[static_cast<size_t>(src)] == level &&
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
    int threads = static_cast<int>(clamp_thread_count(num_threads, num_vertices));
    #pragma omp parallel for num_threads(threads) schedule(static)
    for (int64_t n = 0; n < static_cast<int64_t>(num_vertices); ++n) {
        outgoing_contrib[static_cast<size_t>(n)] =
            out_degree[static_cast<size_t>(n)] == 0
                ? 0.0f
                : init_score / static_cast<ScoreT>(out_degree[static_cast<size_t>(n)]);
    }

    for (size_t iter = 0; iter < iterations; ++iter) {
        auto iter_begin = std::chrono::steady_clock::now();
        std::vector<ScoreT> prev_outgoing_contrib = outgoing_contrib;
        double error = 0.0;
        #pragma omp parallel for num_threads(threads) schedule(dynamic, 16384) reduction(+:error)
        for (int64_t u = 0; u < static_cast<int64_t>(num_vertices); ++u) {
            ScoreT incoming_total = 0.0f;
            bool has_incoming = false;
            size_t tid = static_cast<size_t>(omp_get_thread_num());
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
            ScoreT old_score = scores[static_cast<size_t>(u)];
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
        std::printf("[pr] iter=%zu error=%.6f time=%.4f\n", iter + 1, error, seconds_since(iter_begin));
    }

    double sum = 0.0;
    for (ScoreT score : scores) {
        sum += score;
    }
    return sum;
}

void link_components(uint32_t u, uint32_t v, uint32_t* comp) {
    uint32_t p1 = comp[u];
    uint32_t p2 = comp[v];
    while (p1 != p2) {
        uint32_t high = p1 > p2 ? p1 : p2;
        uint32_t low = p1 + (p2 - high);
        uint32_t p_high = comp[high];
        if ((p_high == low) || (p_high == high && compare_and_swap(comp[high], high, low))) {
            break;
        }
        p1 = comp[comp[high]];
        p2 = comp[low];
    }
}

void compress_components(uint32_t num_vertices, uint32_t* comp, size_t num_threads) {
    int threads = static_cast<int>(clamp_thread_count(num_threads, num_vertices));
    #pragma omp parallel for num_threads(threads) schedule(dynamic, 16384)
    for (int64_t n = 0; n < static_cast<int64_t>(num_vertices); ++n) {
        while (comp[static_cast<size_t>(n)] != comp[comp[static_cast<size_t>(n)]]) {
            comp[static_cast<size_t>(n)] = comp[comp[static_cast<size_t>(n)]];
        }
    }
}

uint32_t sample_frequent_element(uint32_t* comp, uint32_t num_vertices, size_t num_samples = 1024) {
    std::unordered_map<uint32_t, int> sample_counts(32);
    std::mt19937 gen;
    std::uniform_int_distribution<uint32_t> distribution(0, num_vertices - 1);
    for (size_t i = 0; i < num_samples; ++i) {
        uint32_t n = distribution(gen);
        sample_counts[comp[n]]++;
    }
    auto most_frequent = std::max_element(
        sample_counts.begin(),
        sample_counts.end(),
        [](const auto& a, const auto& b) { return a.second < b.second; });
    return most_frequent->first;
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
    int threads = static_cast<int>(clamp_thread_count(num_threads, num_vertices));
    #pragma omp parallel for num_threads(threads) schedule(static)
    for (int64_t v = 0; v < static_cast<int64_t>(num_vertices); ++v) {
        comp[static_cast<size_t>(v)] = static_cast<uint32_t>(v);
    }

    auto sample_begin = std::chrono::steady_clock::now();
    #pragma omp parallel for num_threads(threads) schedule(dynamic, 16384)
    for (int64_t u = 0; u < static_cast<int64_t>(num_vertices); ++u) {
        size_t d = 0;
        size_t tid = static_cast<size_t>(omp_get_thread_num());
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

    uint32_t largest_component = sample_frequent_element(comp.data(), num_vertices);
    std::printf("[cc] sample+compress time=%.4f largest_component=%u\n",
                seconds_since(sample_begin),
                largest_component);
    auto final_begin = std::chrono::steady_clock::now();
    #pragma omp parallel for num_threads(threads) schedule(dynamic, 16384)
    for (int64_t u = 0; u < static_cast<int64_t>(num_vertices); ++u) {
        if (comp[static_cast<size_t>(u)] == largest_component) {
            continue;
        }

        size_t d = 0;
        size_t tid = static_cast<size_t>(omp_get_thread_num());
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

    size_t components = 0;
    for (uint32_t v = 0; v < num_vertices; ++v) {
        if (comp[v] == v) {
            ++components;
        }
    }
    return components;
}

int main(int argc, char* argv[]) {
    try {
        const auto bench_begin = std::chrono::steady_clock::now();
        Options opts = parse_args(argc, argv);
        if (opts.storage_dir.empty()) {
            opts.storage_dir = "../data/bach-bench/" + dataset_name_from_path(opts.input_path);
        }
        if (opts.ingest_batch_edges == 0) {
            throw std::runtime_error("--ingest-batch-edges must be greater than 0");
        }
        if (opts.num_threads == 0) {
            throw std::runtime_error("--threads must be greater than 0");
        }
        if (opts.algo_threads == 0) {
            throw std::runtime_error("--algo-threads must be greater than 0");
        }

        std::printf("[config] dataset=%s\n", opts.input_path.c_str());
        std::printf("[config] storage=%s\n", opts.storage_dir.c_str());
        std::printf("[config] ingest_batch_edges=%zu threads=%zu algo_threads=%zu bfs_rounds=%zu pr_iters=%zu pr_epsilon=%.6f cc_neighbor_rounds=%zu compact=%d\n",
                    opts.ingest_batch_edges,
                    opts.num_threads,
                    opts.algo_threads,
                    opts.bfs_rounds,
                    opts.pr_iterations,
                    opts.pr_epsilon,
                    opts.cc_neighbor_rounds,
                    opts.compact ? 1 : 0);

        auto ts_load_begin = std::chrono::steady_clock::now();
        std::printf("[load] reading bin32 graph\n");
        LoadedGraph loaded = load_bin32_graph(opts.input_path);
        auto ts_load_end = std::chrono::steady_clock::now();
        std::printf("[load] done vertices=%u edges=%zu time=%.4f\n",
                    loaded.num_vertices,
                    loaded.edges.size(),
                    std::chrono::duration<double>(ts_load_end - ts_load_begin).count());

        prepare_storage_dir(opts.storage_dir, opts.reset);

        std::printf("[db] opening storage and creating labels\n");
        auto db_options = std::make_shared<BACH::Options>();
        db_options->STORAGE_DIR = opts.storage_dir;
        db_options->NUM_OF_COMPACTION_THREAD = opts.num_threads;
        db_options->MAX_WORKER_THREAD = std::max<size_t>(db_options->MAX_WORKER_THREAD, opts.num_threads);
        BACH::DB db(db_options);
        BACH::label_t vertex_label = db.AddVertexLabel("v");
        BACH::label_t edge_out_label = db.AddEdgeLabel("e_out", "v", "v");
        BACH::label_t edge_in_label = db.AddEdgeLabel("e_in", "v", "v");
        std::printf("[db] labels ready vertex_label=%u edge_out_label=%u edge_in_label=%u compaction_threads=%zu\n",
                    vertex_label,
                    edge_out_label,
                    edge_in_label,
                    db_options->NUM_OF_COMPACTION_THREAD);

        auto ts_ingest_begin = std::chrono::steady_clock::now();
        std::printf("[ingest] creating %u vertices\n", loaded.num_vertices);
        auto vertex_begin = std::chrono::steady_clock::now();
        {
            auto tx = db.BeginTransaction();
            for (uint32_t i = 0; i < loaded.num_vertices; ++i) {
                BACH::vertex_t vertex_id = tx.AddVertex(vertex_label);
                if (vertex_id != i) {
                    throw std::runtime_error("Vertex id allocation is not contiguous as expected");
                }
            }
        }
        std::printf("[ingest] vertices done time=%.4f\n", seconds_since(vertex_begin));
        std::printf("[ingest] loading %zu edges with batch_size=%zu\n",
                    loaded.edges.size(),
                    opts.ingest_batch_edges);
        size_t batch_id = 0;
        size_t total_batches =
            (loaded.edges.size() + opts.ingest_batch_edges - 1) / opts.ingest_batch_edges;
        for (size_t begin = 0; begin < loaded.edges.size(); begin += opts.ingest_batch_edges) {
            size_t end = std::min(begin + opts.ingest_batch_edges, loaded.edges.size());
            auto batch_begin = std::chrono::steady_clock::now();
            size_t batch_edges = end - begin;
            size_t batch_threads = clamp_thread_count(opts.num_threads, batch_edges);
            std::atomic<bool> ingest_failed(false);
            std::exception_ptr ingest_exception;
            std::mutex ingest_exception_mutex;
            #pragma omp parallel num_threads(static_cast<int>(batch_threads))
            {
                if (ingest_failed.load(std::memory_order_relaxed)) {
                    goto ingest_parallel_end;
                }
                try {
                    auto tx = db.BeginTransaction();
                    #pragma omp for schedule(static)
                    for (int64_t i = static_cast<int64_t>(begin); i < static_cast<int64_t>(end); ++i) {
                        const auto& edge = loaded.edges[static_cast<size_t>(i)];
                        tx.PutEdge(edge.src, edge.dst, edge_out_label, 1.0);
                        tx.PutEdge(edge.dst, edge.src, edge_in_label, 1.0);
                    }
                } catch (...) {
                    ingest_failed.store(true, std::memory_order_relaxed);
                    std::lock_guard<std::mutex> lock(ingest_exception_mutex);
                    if (!ingest_exception) {
                        ingest_exception = std::current_exception();
                    }
                }
ingest_parallel_end:
                ;
            }
            if (ingest_exception) {
                std::rethrow_exception(ingest_exception);
            }
            ++batch_id;
            double elapsed = seconds_since(ts_ingest_begin);
            double pct = loaded.edges.empty()
                             ? 100.0
                             : 100.0 * static_cast<double>(end) / static_cast<double>(loaded.edges.size());
            double batch_seconds = seconds_since(batch_begin);
            double ingest_mteps =
                elapsed > 0.0 ? static_cast<double>(end) / elapsed / 1e6 : 0.0;
            std::printf("[ingest] batch=%zu/%zu edges=[%zu,%zu) threads=%zu progress=%.2f%% batch_time=%.4f elapsed=%.4f rate=%.4fM edges/s\n",
                        batch_id,
                        total_batches,
                        begin,
                        end,
                        batch_threads,
                        pct,
                        batch_seconds,
                        elapsed,
                        ingest_mteps);
        }
        if (opts.compact) {
            auto compact_begin = std::chrono::steady_clock::now();
            std::printf("[compact] start\n");
            db.CompactAll();
            std::printf("[compact] done time=%.4f\n", seconds_since(compact_begin));
        }
        auto ts_ingest_end = std::chrono::steady_clock::now();
        long rss_ingest = get_rss();

        double bfs_seconds = 0.0;
        size_t bfs_checksum = 0;
        for (size_t round = 0; round < opts.bfs_rounds; ++round) {
            uint32_t source = select_bfs_source(round, loaded.out_degree);
            auto t0 = std::chrono::steady_clock::now();
            std::printf("[bfs] round=%zu source=%u start\n", round, source);
            size_t bfs_threads = clamp_thread_count(opts.algo_threads, loaded.num_vertices);
            std::vector<std::shared_ptr<BACH::Transaction>> bfs_out_txs;
            std::vector<std::shared_ptr<BACH::Transaction>> bfs_in_txs;
            bfs_out_txs.reserve(bfs_threads);
            bfs_in_txs.reserve(bfs_threads);
            for (size_t tid = 0; tid < bfs_threads; ++tid) {
                bfs_out_txs.push_back(std::make_shared<BACH::Transaction>(db.BeginReadOnlyTransaction()));
                bfs_in_txs.push_back(std::make_shared<BACH::Transaction>(db.BeginReadOnlyTransaction()));
            }
            auto with_out_neighbors = [&](size_t tid, uint32_t src, auto&& fn) {
                auto edges = bfs_out_txs[tid]->GetEdges(src, edge_out_label);
                for (const auto& [dst, property] : *edges) {
                    (void)property;
                    if (!fn(dst)) {
                        break;
                    }
                }
            };
            auto with_in_neighbors = [&](size_t tid, uint32_t dst, auto&& fn) {
                auto edges = bfs_in_txs[tid]->GetEdges(dst, edge_in_label);
                for (const auto& [src, property] : *edges) {
                    (void)property;
                    if (!fn(src)) {
                        break;
                    }
                }
            };
            size_t visited = run_gapbs_bfs(
                loaded.num_vertices, source, opts.algo_threads, with_out_neighbors, with_in_neighbors);
            bfs_checksum += visited;
            auto t1 = std::chrono::steady_clock::now();
            double round_seconds = std::chrono::duration<double>(t1 - t0).count();
            bfs_seconds += round_seconds;
            std::printf("[bfs] round=%zu source=%u visited=%zu time=%.4f\n",
                        round,
                        source,
                        visited,
                        round_seconds);
        }
        long rss_bfs = get_rss();

        auto pr_t0 = std::chrono::steady_clock::now();
        std::printf("[pr] start iterations=%zu epsilon=%.6f\n", opts.pr_iterations, opts.pr_epsilon);
        size_t pr_threads = clamp_thread_count(opts.algo_threads, loaded.num_vertices);
        std::vector<std::shared_ptr<BACH::Transaction>> pr_in_txs;
        pr_in_txs.reserve(pr_threads);
        for (size_t tid = 0; tid < pr_threads; ++tid) {
            pr_in_txs.push_back(std::make_shared<BACH::Transaction>(db.BeginReadOnlyTransaction()));
        }
        auto with_in_neighbors = [&](size_t tid, uint32_t dst, auto&& fn) {
            auto edges = pr_in_txs[tid]->GetEdges(dst, edge_in_label);
            for (const auto& [src, property] : *edges) {
                (void)property;
                if (!fn(src)) {
                    break;
                }
            }
        };
        double pr_sum = run_gapbs_pr(
            loaded.num_vertices,
            loaded.out_degree,
            opts.algo_threads,
            opts.pr_iterations,
            opts.pr_epsilon,
            with_in_neighbors);
        auto pr_t1 = std::chrono::steady_clock::now();
        double pr_seconds = std::chrono::duration<double>(pr_t1 - pr_t0).count();
        std::printf("[pr] done sum=%.8f time=%.4f\n", pr_sum, pr_seconds);

        auto cc_t0 = std::chrono::steady_clock::now();
        std::printf("[cc] start neighbor_rounds=%zu\n", opts.cc_neighbor_rounds);
        size_t cc_threads = clamp_thread_count(opts.algo_threads, loaded.num_vertices);
        std::vector<std::shared_ptr<BACH::Transaction>> cc_out_txs;
        std::vector<std::shared_ptr<BACH::Transaction>> cc_in_txs;
        cc_out_txs.reserve(cc_threads);
        cc_in_txs.reserve(cc_threads);
        for (size_t tid = 0; tid < cc_threads; ++tid) {
            cc_out_txs.push_back(std::make_shared<BACH::Transaction>(db.BeginReadOnlyTransaction()));
            cc_in_txs.push_back(std::make_shared<BACH::Transaction>(db.BeginReadOnlyTransaction()));
        }
        auto cc_with_out_neighbors = [&](size_t tid, uint32_t src, auto&& fn) {
            auto edges = cc_out_txs[tid]->GetEdges(src, edge_out_label);
            for (const auto& [dst, property] : *edges) {
                (void)property;
                if (!fn(dst)) {
                    break;
                }
            }
        };
        auto cc_with_in_neighbors = [&](size_t tid, uint32_t dst, auto&& fn) {
            auto edges = cc_in_txs[tid]->GetEdges(dst, edge_in_label);
            for (const auto& [src, property] : *edges) {
                (void)property;
                if (!fn(src)) {
                    break;
                }
            }
        };
        size_t cc_components = run_gapbs_cc(
            loaded.num_vertices,
            opts.cc_neighbor_rounds,
            opts.algo_threads,
            cc_with_out_neighbors,
            cc_with_in_neighbors);
        auto cc_t1 = std::chrono::steady_clock::now();
        double cc_seconds = std::chrono::duration<double>(cc_t1 - cc_t0).count();
        std::printf("[cc] done components=%zu time=%.4f\n", cc_components, cc_seconds);

        double load_seconds = std::chrono::duration<double>(ts_load_end - ts_load_begin).count();
        double ingest_seconds = std::chrono::duration<double>(ts_ingest_end - ts_ingest_begin).count();
        double total_seconds =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - bench_begin).count();
        double ingest_bandwidth =
            ingest_seconds > 0.0 ? static_cast<double>(loaded.edges.size()) / ingest_seconds / 1e6 : 0.0;

        std::printf(EXPOUT "Load: %.4f\n", load_seconds);
        std::printf(EXPOUT "Ingest: %.4f\n", ingest_seconds);
        std::printf(EXPOUT "BFS: %.4f\n", bfs_seconds);
        std::printf(EXPOUT "PR: %.4f\n", pr_seconds);
        std::printf(EXPOUT "CC: %.4f\n", cc_seconds);
        std::printf(EXPOUT "RSS_Ingest: %ld\n", rss_ingest);
        std::printf(EXPOUT "RSS_BFS: %ld\n", rss_bfs);
        std::printf("[result] bfs_checksum=%zu pr_sum=%.8f cc_components=%zu total=%.4f ingest_bw=%.4fM edges/s\n",
                    bfs_checksum,
                    pr_sum,
                    cc_components,
                    total_seconds,
                    ingest_bandwidth);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        return 1;
    }
}
