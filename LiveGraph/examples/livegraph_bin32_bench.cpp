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
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "livegraph.hpp"
#include "../../../scatter_graph/script/query_bench.hpp"

namespace fs = std::filesystem;

#ifndef EXPOUT
#define EXPOUT "[EXPOUT]"
#endif

struct Edge32 {
    uint32_t src;
    uint32_t dst;
};

static_assert(sizeof(Edge32) == sizeof(uint32_t) * 2, "Edge32 must be 8 bytes");

// Streaming chunk size: 16 MiB worth of edges (16 MiB / 8B = 2M edges per chunk)
constexpr size_t kStreamChunkBytes = 16ull << 20;
constexpr size_t kStreamChunkEdges = kStreamChunkBytes / sizeof(Edge32);

struct Options {
    std::string input_path;
    std::string storage_dir;
    size_t num_vertices = 0;  // required, passed from driver via --num-vertices
    size_t batch_size = kStreamChunkEdges;
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
    bool skip_analytics = false;
    size_t max_batch = 0;  // 0 = no limit; otherwise stop after this many 16MiB batches
    query_bench::QueryOptions query;
};

struct LoadedGraph {
    std::vector<uint32_t> out_degree;
    std::vector<uint32_t> in_degree;
    uint32_t num_vertices = 0;
    size_t total_edges = 0;
};

#include "graph_benchmarks.hpp"

void print_usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0 << " -f <graph.bin32> --num-vertices <n> [options]\n"
        << "Options:\n"
        << "  --num-vertices <n>    Vertex count from datasets.py (REQUIRED)\n"
        << "  --storage <dir>       Storage root, default ../data/livegraph-bench/<dataset>\n"
        << "  --batch-size <n>      Edge ingest chunk size, default 2097152 (16 MiB / edge)\n"
        << "  --threads <n>         Worker threads for ingest, default hardware concurrency\n"
        << "  --algo-threads <n>    Worker threads for BFS/PR/CC, default same as --threads\n"
        << "  --pr-iters <n>        PageRank iterations, default 10\n"
        << "  --pr-epsilon <x>      PageRank early-stop epsilon, default 0\n"
        << "  --bfs-rounds <n>      BFS rounds, default 20\n"
        << "  --cc-rounds <n>       CC rounds, default 10\n"
        << "  --cc-neighbor-rounds <n>  CC sampled neighbor rounds, default 2\n"
        << "  --debug-vertex <id>   Print out/in neighbors for one vertex and exit after ingest\n"
        << "  --skip-analytics      Skip BFS/PR/CC after query phase\n"
        << "  --reset               Remove existing storage dir before ingest\n"
        << "  --max-batch <n>       Stop ingest after N 16MiB batches (0=no limit)\n"
        << "  --query-pool-size <n> Point-query vertex pool size, default 10000\n"
        << "  --query-uniform-count <n> Point-Uniform query count, default 1000000\n"
        << "  --query-skew-count <n> Point-Skew query count, default 1000000\n"
        << "  --query-zipf-alpha <x> Point-Skew Zipf alpha, default 1.2\n"
        << "  --query-pool-seed <n> Query pool RNG seed, default 42\n"
        << "  --query-seed-base <n> Query stream seed base, default 43\n";
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
        } else if (arg == "--num-vertices") {
            opts.num_vertices = std::stoull(require_value("--num-vertices"));
        } else if (arg == "--storage") {
            opts.storage_dir = require_value("--storage");
        } else if (arg == "--batch-size") {
            opts.batch_size = std::stoull(require_value("--batch-size"));
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
        } else if (arg == "--skip-analytics") {
            opts.skip_analytics = true;
        } else if (arg == "--max-batch") {
            opts.max_batch = std::stoull(require_value("--max-batch"));
        } else if (arg == "--query-pool-size") {
            opts.query.pool_size = std::stoull(require_value("--query-pool-size"));
        } else if (arg == "--query-uniform-count") {
            opts.query.uniform_count = std::stoull(require_value("--query-uniform-count"));
        } else if (arg == "--query-skew-count") {
            opts.query.skew_count = std::stoull(require_value("--query-skew-count"));
        } else if (arg == "--query-zipf-alpha") {
            opts.query.zipf_alpha = std::stod(require_value("--query-zipf-alpha"));
        } else if (arg == "--query-pool-seed") {
            opts.query.pool_seed = std::stoull(require_value("--query-pool-seed"));
        } else if (arg == "--query-seed-base") {
            opts.query.seed_base = std::stoull(require_value("--query-seed-base"));
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
    if (opts.num_vertices == 0) {
        throw std::runtime_error("Please specify --num-vertices <n> (from datasets.py)");
    }
    if (opts.algo_threads == 0) {
        opts.algo_threads = opts.num_threads;
    }
    opts.query.threads = opts.algo_threads;
    return opts;
}

void validate_options(const Options& opts) {
    if (opts.batch_size == 0) {
        throw std::runtime_error("--batch-size must be > 0");
    }
    if (opts.num_threads == 0) {
        throw std::runtime_error("--threads must be > 0");
    }
    if (opts.algo_threads == 0) {
        throw std::runtime_error("--algo-threads must be > 0");
    }
    if (opts.debug_vertex < -1) {
        throw std::runtime_error("--debug-vertex must be >= 0");
    }
}

std::string dataset_name_from_path(const std::string& path) {
    fs::path p(path);
    return p.stem().empty() ? "dataset" : p.stem().string();
}

LoadedGraph make_empty_loaded_graph(uint32_t num_vertices) {
    LoadedGraph graph;
    graph.num_vertices = num_vertices;
    graph.out_degree.assign(num_vertices, 0);
    graph.in_degree.assign(num_vertices, 0);
    graph.total_edges = 0;
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
    std::printf("[config] batch_size=%zu threads=%zu algo_threads=%zu bfs_rounds=%zu pr_iters=%zu pr_epsilon=%.6f cc_rounds=%zu cc_neighbor_rounds=%zu\n",
                opts.batch_size,
                opts.num_threads,
                opts.algo_threads,
                opts.bfs_rounds,
                opts.pr_iterations,
                opts.pr_epsilon,
                opts.cc_rounds,
                opts.cc_neighbor_rounds);
}

lg::Graph open_graph(const Options& opts, uint32_t num_vertices) {
    const fs::path storage_root(opts.storage_dir);
    const fs::path block_path = storage_root / "block.dat";
    const fs::path wal_path = storage_root / "wal.log";
    const size_t estimated_block_bytes = 1ULL << 40;
    return lg::Graph(
        block_path.string(),
        wal_path.string(),
        estimated_block_bytes,
        std::max<lg::vertex_t>(1, num_vertices));
}

void ingest_vertices(lg::Graph& graph, uint32_t num_vertices) {
    std::printf("[ingest] creating %u vertices\n", num_vertices);
    const auto vertex_begin = std::chrono::steady_clock::now();
    auto tx = graph.begin_batch_loader();
    for (uint32_t i = 0; i < num_vertices; ++i) {
        const auto vertex_id = tx.new_vertex();
        if (vertex_id != i) {
            throw std::runtime_error("Vertex id allocation is not contiguous as expected");
        }
        tx.put_vertex(vertex_id, std::string_view());
    }
    tx.commit();
    std::printf("[ingest] vertices done time=%.4f\n", seconds_since(vertex_begin));
}

void ingest_edges(lg::Graph& graph, LoadedGraph& loaded, const Options& opts) {
    std::printf("[ingest] streaming edges from %s with chunk_edges=%zu (%zu MiB)\n",
                opts.input_path.c_str(),
                opts.batch_size,
                (opts.batch_size * sizeof(Edge32)) >> 20);

    std::FILE* fp = std::fopen(opts.input_path.c_str(), "rb");
    if (!fp) {
        throw std::runtime_error("Failed to open input file: " + opts.input_path);
    }

    // Get file size for progress reporting.
    std::fseek(fp, 0, SEEK_END);
    const long file_bytes = std::ftell(fp);
    std::fseek(fp, 0, SEEK_SET);
    if (file_bytes < 0 || file_bytes % static_cast<long>(sizeof(Edge32)) != 0) {
        std::fclose(fp);
        throw std::runtime_error("Input file is not a valid bin32 edge list");
    }
    const size_t total_edges = static_cast<size_t>(file_bytes) / sizeof(Edge32);
    const size_t total_batches =
        total_edges == 0 ? 0 : (total_edges + opts.batch_size - 1) / opts.batch_size;

    std::vector<Edge32> chunk(opts.batch_size);
    const auto ingest_begin = std::chrono::steady_clock::now();
    size_t cursor = 0;
    size_t batch_id = 0;
    while (true) {
        const size_t want = opts.batch_size;
        const size_t got_edges = std::fread(chunk.data(), sizeof(Edge32), want, fp);
        if (got_edges == 0) break;

        const size_t batch_threads = clamp_thread_count(opts.num_threads, got_edges);
        const auto batch_begin = std::chrono::steady_clock::now();
        std::atomic<bool> failed(false);
        std::exception_ptr first_exception;
        std::mutex exception_mutex;

        // Accumulate degrees and ingest edges in parallel within this chunk.
        // Degree increments are serialised after the parallel ingest to avoid
        // races; degree counters are O(num_vertices) and only touched from one
        // thread per edge anyway, so a sequential pass is plenty fast.
        #pragma omp parallel num_threads(static_cast<int>(batch_threads))
        {
            if (failed.load(std::memory_order_relaxed)) {
                goto ingest_parallel_end;
            }
            try {
                auto tx = graph.begin_batch_loader();
                #pragma omp for schedule(static)
                for (int64_t i = 0; i < static_cast<int64_t>(got_edges); ++i) {
                    const auto& edge = chunk[static_cast<size_t>(i)];
                    tx.put_edge(edge.src, 0, edge.dst, std::string_view());
                    tx.put_edge(edge.dst, 1, edge.src, std::string_view());
                }
                tx.commit();
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
            std::fclose(fp);
            std::rethrow_exception(first_exception);
        }

        // Tally degrees serially after the parallel ingest section.
        for (size_t i = 0; i < got_edges; ++i) {
            const auto& edge = chunk[i];
            if (edge.src < loaded.num_vertices) ++loaded.out_degree[edge.src];
            if (edge.dst < loaded.num_vertices) ++loaded.in_degree[edge.dst];
        }
        loaded.total_edges += got_edges;
        cursor += got_edges;
        ++batch_id;

        const double elapsed = seconds_since(ingest_begin);
        const double batch_seconds = seconds_since(batch_begin);
        const double progress = total_edges == 0
                                    ? 100.0
                                    : 100.0 * static_cast<double>(cursor) /
                                          static_cast<double>(total_edges);
        const double ingest_rate = elapsed > 0.0 ? static_cast<double>(cursor) / elapsed / 1e6 : 0.0;
        std::printf("[ingest] batch=%zu/%zu edges=%zu cursor=%zu threads=%zu progress=%.2f%% batch_time=%.4f elapsed=%.4f rate=%.4fM edges/s\n",
                    batch_id,
                    total_batches,
                    got_edges,
                    cursor,
                    batch_threads,
                    progress,
                    batch_seconds,
                    elapsed,
                    ingest_rate);

        if (got_edges < want) break;  // EOF reached
        if (opts.max_batch != 0 && batch_id >= opts.max_batch) {
            std::printf("[ingest] reached --max-batch=%zu, stopping early\n", opts.max_batch);
            break;
        }
    }

    std::fclose(fp);
}

LiveGraphBenchmarkConfig make_benchmark_config(const Options& opts) {
    LiveGraphBenchmarkConfig config;
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
                   const LiveGraphBenchmarkResult& result,
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

void print_vertex_neighbors(lg::Graph& graph, const LoadedGraph& loaded, uint32_t vertex_id) {
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

    auto tx = graph.begin_read_only_transaction();
    std::vector<uint32_t> out_neighbors;
    std::vector<uint32_t> in_neighbors;

    auto out_it = tx.get_edges(vertex_id, 0);
    while (out_it.valid()) {
        out_neighbors.push_back(static_cast<uint32_t>(out_it.dst_id()));
        out_it.next();
    }

    auto in_it = tx.get_edges(vertex_id, 1);
    while (in_it.valid()) {
        in_neighbors.push_back(static_cast<uint32_t>(in_it.dst_id()));
        in_it.next();
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
            opts.storage_dir = "../data/livegraph-bench/" + dataset_name_from_path(opts.input_path);
        }
        validate_options(opts);
        log_config(opts);

        const auto load_begin = std::chrono::steady_clock::now();
        // Driver supplied num_vertices via --num-vertices, no input scan needed.
        LoadedGraph loaded = make_empty_loaded_graph(static_cast<uint32_t>(opts.num_vertices));
        const double load_seconds = seconds_since(load_begin);
        std::printf("[load] vertices=%u (from --num-vertices) time=%.4f\n",
                    loaded.num_vertices,
                    load_seconds);

        prepare_storage_dir(opts.storage_dir, opts.reset);
        lg::Graph graph = open_graph(opts, loaded.num_vertices);

        const auto ingest_begin = std::chrono::steady_clock::now();
        ingest_vertices(graph, loaded.num_vertices);
        ingest_edges(graph, loaded, opts);
        const double ingest_seconds = seconds_since(ingest_begin);
        const long rss_ingest = get_rss();

        if (opts.debug_vertex >= 0) {
            print_vertex_neighbors(graph, loaded, static_cast<uint32_t>(opts.debug_vertex));
            return 0;
        }

        auto make_query_walker = [&graph]() {
            return [tx = graph.begin_read_only_transaction()](uint32_t v) mutable -> uint64_t {
                auto it = tx.get_edges(v, 0);
                uint64_t degree = 0;
                while (it.valid()) {
                    ++degree;
                    it.next();
                }
                return degree;
            };
        };
        query_bench::run_query_phase_thread_local(
            loaded.num_vertices, make_query_walker, opts.query, std::cout);

        LiveGraphBenchmarkResult result;
        if (!opts.skip_analytics) {
            result = run_livegraph_graph_benchmarks_gapbs(graph, loaded, make_benchmark_config(opts));
        }
        const double total_seconds = seconds_since(bench_begin);
        print_summary(
            load_seconds, ingest_seconds, rss_ingest, result, loaded.total_edges, total_seconds);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        return 1;
    }
}
