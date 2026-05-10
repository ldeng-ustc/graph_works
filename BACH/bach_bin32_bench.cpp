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
    size_t cc_rounds = 10;
    size_t cc_neighbor_rounds = 2;
    int64_t debug_vertex = -1;
    bool reset = false;
    bool compact = false;
};

struct LoadedGraph {
    std::vector<Edge32> edges;
    std::vector<uint32_t> out_degree;
    std::vector<uint32_t> in_degree;
    uint32_t num_vertices = 0;
};

#include "graph_benchmarks.hpp"

struct BACHDbHandles {
    std::shared_ptr<BACH::Options> options;
    std::unique_ptr<BACH::DB> db;
    BACH::label_t vertex_label = 0;
    BACH::label_t edge_out_label = 0;
    BACH::label_t edge_in_label = 0;

    BACHBenchmarkLabels benchmark_labels() const {
        return {edge_out_label, edge_in_label};
    }
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
        << "  --cc-rounds <n>       CC rounds, default 10\n"
        << "  --cc-neighbor-rounds <n>  CC sampled neighbor rounds, default 2\n"
        << "  --debug-vertex <id>   Print out/in neighbors for one vertex and exit after ingest\n"
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
        } else if (arg == "--cc-rounds") {
            opts.cc_rounds = std::stoull(require_value("--cc-rounds"));
        } else if (arg == "--cc-neighbor-rounds") {
            opts.cc_neighbor_rounds = std::stoull(require_value("--cc-neighbor-rounds"));
        } else if (arg == "--debug-vertex") {
            opts.debug_vertex = std::stoll(require_value("--debug-vertex"));
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

void validate_options(const Options& opts) {
    if (opts.ingest_batch_edges == 0) {
        throw std::runtime_error("--ingest-batch-edges must be greater than 0");
    }
    if (opts.num_threads == 0) {
        throw std::runtime_error("--threads must be greater than 0");
    }
    if (opts.algo_threads == 0) {
        throw std::runtime_error("--algo-threads must be greater than 0");
    }
    if (opts.debug_vertex < -1) {
        throw std::runtime_error("--debug-vertex must be >= 0");
    }
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
                throw std::runtime_error(
                    "Storage path already exists and is not a directory: " +
                    storage_dir.string());
            }
            fs::remove(storage_dir);
        }
    }
    fs::create_directories(storage_dir);
}

void log_config(const Options& opts) {
    std::printf("[config] dataset=%s\n", opts.input_path.c_str());
    std::printf("[config] storage=%s\n", opts.storage_dir.c_str());
    std::printf("[config] ingest_batch_edges=%zu threads=%zu algo_threads=%zu bfs_rounds=%zu pr_iters=%zu pr_epsilon=%.6f cc_rounds=%zu cc_neighbor_rounds=%zu compact=%d\n",
                opts.ingest_batch_edges,
                opts.num_threads,
                opts.algo_threads,
                opts.bfs_rounds,
                opts.pr_iterations,
                opts.pr_epsilon,
                opts.cc_rounds,
                opts.cc_neighbor_rounds,
                opts.compact ? 1 : 0);
}

BACHDbHandles open_db(const Options& opts) {
    std::printf("[db] opening storage and creating labels\n");
    BACHDbHandles handles;
    handles.options = std::make_shared<BACH::Options>();
    handles.options->STORAGE_DIR = opts.storage_dir;
    handles.options->NUM_OF_COMPACTION_THREAD = opts.num_threads;
    handles.options->MAX_WORKER_THREAD =
        std::max<size_t>(handles.options->MAX_WORKER_THREAD, opts.num_threads);
    handles.db = std::make_unique<BACH::DB>(handles.options);
    handles.vertex_label = handles.db->AddVertexLabel("v");
    handles.edge_out_label = handles.db->AddEdgeLabel("e_out", "v", "v");
    handles.edge_in_label = handles.db->AddEdgeLabel("e_in", "v", "v");
    std::printf("[db] labels ready vertex_label=%u edge_out_label=%u edge_in_label=%u compaction_threads=%zu\n",
                handles.vertex_label,
                handles.edge_out_label,
                handles.edge_in_label,
                handles.options->NUM_OF_COMPACTION_THREAD);
    return handles;
}

void ingest_vertices(BACH::DB& db, uint32_t num_vertices, BACH::label_t vertex_label) {
    std::printf("[ingest] creating %u vertices\n", num_vertices);
    const auto vertex_begin = std::chrono::steady_clock::now();
    auto tx = db.BeginTransaction();
    for (uint32_t i = 0; i < num_vertices; ++i) {
        const BACH::vertex_t vertex_id = tx.AddVertex(vertex_label);
        if (vertex_id != i) {
            throw std::runtime_error("Vertex id allocation is not contiguous as expected");
        }
    }
    std::printf("[ingest] vertices done time=%.4f\n", seconds_since(vertex_begin));
}

void ingest_edges(BACH::DB& db,
                  const LoadedGraph& loaded,
                  const Options& opts,
                  BACHBenchmarkLabels labels) {
    std::printf("[ingest] loading %zu edges with batch_size=%zu\n",
                loaded.edges.size(),
                opts.ingest_batch_edges);
    const auto ingest_begin = std::chrono::steady_clock::now();
    const size_t total_batches =
        (loaded.edges.size() + opts.ingest_batch_edges - 1) / opts.ingest_batch_edges;

    for (size_t begin = 0, batch_id = 0; begin < loaded.edges.size(); begin += opts.ingest_batch_edges) {
        const size_t end = std::min(begin + opts.ingest_batch_edges, loaded.edges.size());
        const size_t batch_edges = end - begin;
        const size_t batch_threads = clamp_thread_count(opts.num_threads, batch_edges);
        const auto batch_begin = std::chrono::steady_clock::now();
        std::atomic<bool> failed(false);
        std::exception_ptr first_exception;
        std::mutex exception_mutex;

        #pragma omp parallel num_threads(static_cast<int>(batch_threads))
        {
            if (failed.load(std::memory_order_relaxed)) {
                goto ingest_parallel_end;
            }
            try {
                auto tx = db.BeginTransaction();
                #pragma omp for schedule(static)
                for (int64_t i = static_cast<int64_t>(begin); i < static_cast<int64_t>(end); ++i) {
                    const auto& edge = loaded.edges[static_cast<size_t>(i)];
                    tx.PutEdge(edge.src, edge.dst, labels.edge_out_label, 1.0);
                    tx.PutEdge(edge.dst, edge.src, labels.edge_in_label, 1.0);
                }
            } catch (...) {
                failed.store(true, std::memory_order_relaxed);
                std::lock_guard<std::mutex> lock(exception_mutex);
                if (!first_exception) {
                    first_exception = std::current_exception();
                }
            }
        ingest_parallel_end:
            ;
        }

        if (first_exception) {
            std::rethrow_exception(first_exception);
        }

        ++batch_id;
        const double elapsed = seconds_since(ingest_begin);
        const double batch_seconds = seconds_since(batch_begin);
        const double progress = loaded.edges.empty()
                                    ? 100.0
                                    : 100.0 * static_cast<double>(end) /
                                          static_cast<double>(loaded.edges.size());
        const double ingest_rate = elapsed > 0.0 ? static_cast<double>(end) / elapsed / 1e6 : 0.0;
        std::printf("[ingest] batch=%zu/%zu edges=[%zu,%zu) threads=%zu progress=%.2f%% batch_time=%.4f elapsed=%.4f rate=%.4fM edges/s\n",
                    batch_id,
                    total_batches,
                    begin,
                    end,
                    batch_threads,
                    progress,
                    batch_seconds,
                    elapsed,
                    ingest_rate);
    }
}

void maybe_compact(BACH::DB& db, bool compact) {
    if (!compact) {
        return;
    }
    const auto compact_begin = std::chrono::steady_clock::now();
    std::printf("[compact] start\n");
    db.CompactAll();
    std::printf("[compact] done time=%.4f\n", seconds_since(compact_begin));
}

BACHBenchmarkConfig make_benchmark_config(const Options& opts) {
    BACHBenchmarkConfig config;
    config.algo_threads = opts.algo_threads;
    config.pr_iterations = opts.pr_iterations;
    config.pr_epsilon = opts.pr_epsilon;
    config.bfs_rounds = opts.bfs_rounds;
    config.cc_rounds = opts.cc_rounds;
    config.cc_neighbor_rounds = opts.cc_neighbor_rounds;
    return config;
}

void print_summary(double load_seconds,
                   double ingest_seconds,
                   long rss_ingest,
                   const BACHBenchmarkResult& result,
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
    std::printf(EXPOUT "RSS_BFS: %ld\n", result.rss_bfs);
    std::printf("[result] bfs_checksum=%zu pr_sum=%.8f cc_components=%zu total=%.4f ingest_bw=%.4fM edges/s\n",
                result.bfs_checksum,
                result.pr_sum,
                result.cc_components,
                total_seconds,
                ingest_bandwidth);
}

void print_vertex_neighbors(BACH::DB& db,
                            BACHBenchmarkLabels labels,
                            const LoadedGraph& loaded,
                            uint32_t vertex_id) {
    if (vertex_id >= loaded.num_vertices) {
        throw std::runtime_error("debug vertex is out of range");
    }

    auto print_list = [&](const char* name, const std::vector<uint32_t>& values) {
        std::vector<uint32_t> sorted = values;
        std::sort(sorted.begin(), sorted.end());
        std::printf("[debug] vertex=%u %s_count=%zu raw=", vertex_id, name, values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            std::printf("%s%u", i == 0 ? "" : ",", values[i]);
        }
        std::printf("\n");
        std::printf("[debug] vertex=%u %s_sorted=", vertex_id, name);
        for (size_t i = 0; i < sorted.size(); ++i) {
            std::printf("%s%u", i == 0 ? "" : ",", sorted[i]);
        }
        std::printf("\n");
    };

    auto tx = db.BeginReadOnlyTransaction();
    std::vector<uint32_t> out_neighbors;
    std::vector<uint32_t> in_neighbors;

    auto out_edges = tx.GetEdges(vertex_id, labels.edge_out_label);
    out_neighbors.reserve(out_edges->size());
    for (const auto& [dst, property] : *out_edges) {
        (void)property;
        out_neighbors.push_back(dst);
    }

    auto in_edges = tx.GetEdges(vertex_id, labels.edge_in_label);
    in_neighbors.reserve(in_edges->size());
    for (const auto& [src, property] : *in_edges) {
        (void)property;
        in_neighbors.push_back(src);
    }

    std::printf("[debug] vertex=%u loaded_out_degree=%u loaded_in_degree=%u\n",
                vertex_id,
                loaded.out_degree[vertex_id],
                loaded.in_degree[vertex_id]);
    print_list("out_neighbors", out_neighbors);
    print_list("in_neighbors", in_neighbors);
}

int main(int argc, char* argv[]) {
    try {
        const auto bench_begin = std::chrono::steady_clock::now();
        Options opts = parse_args(argc, argv);
        if (opts.storage_dir.empty()) {
            opts.storage_dir = "../data/bach-bench/" + dataset_name_from_path(opts.input_path);
        }
        validate_options(opts);
        log_config(opts);

        const auto load_begin = std::chrono::steady_clock::now();
        std::printf("[load] reading bin32 graph\n");
        LoadedGraph loaded = load_bin32_graph(opts.input_path);
        const double load_seconds = seconds_since(load_begin);
        std::printf("[load] done vertices=%u edges=%zu time=%.4f\n",
                    loaded.num_vertices,
                    loaded.edges.size(),
                    load_seconds);

        prepare_storage_dir(opts.storage_dir, opts.reset);
        BACHDbHandles handles = open_db(opts);

        const auto ingest_begin = std::chrono::steady_clock::now();
        ingest_vertices(*handles.db, loaded.num_vertices, handles.vertex_label);
        ingest_edges(*handles.db, loaded, opts, handles.benchmark_labels());
        maybe_compact(*handles.db, opts.compact);
        const double ingest_seconds = seconds_since(ingest_begin);
        const long rss_ingest = get_rss();

        if (opts.debug_vertex >= 0) {
            print_vertex_neighbors(*handles.db,
                                   handles.benchmark_labels(),
                                   loaded,
                                   static_cast<uint32_t>(opts.debug_vertex));
            return 0;
        }

        const BACHBenchmarkResult result = run_bach_graph_benchmarks_gapbs(
            *handles.db, handles.benchmark_labels(), loaded, make_benchmark_config(opts));
        const double total_seconds = seconds_since(bench_begin);
        print_summary(
            load_seconds, ingest_seconds, rss_ingest, result, loaded.edges.size(), total_seconds);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        return 1;
    }
}
