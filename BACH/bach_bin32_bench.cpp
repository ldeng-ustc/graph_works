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
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
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
        << "  --threads <n>         Worker threads for ingest experiment, default hardware concurrency\n"
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
    return opts;
}

double seconds_since(const std::chrono::steady_clock::time_point& begin) {
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
}

size_t clamp_thread_count(size_t requested_threads, size_t work_items) {
    if (work_items == 0) {
        return 1;
    }
    return std::max<size_t>(1, std::min(requested_threads, work_items));
}

template <typename Fn>
void parallel_for_ranges(size_t work_items, size_t requested_threads, Fn&& fn) {
    size_t threads = clamp_thread_count(requested_threads, work_items);
    if (threads <= 1) {
        fn(0, work_items, 0);
        return;
    }

    const size_t base = work_items / threads;
    const size_t extra = work_items % threads;
    std::vector<std::thread> workers;
    workers.reserve(threads);
    std::atomic<bool> failed(false);
    std::exception_ptr first_exception;
    std::mutex exception_mutex;

    size_t begin = 0;
    for (size_t tid = 0; tid < threads; ++tid) {
        const size_t size = base + (tid < extra ? 1 : 0);
        const size_t end = begin + size;
        workers.emplace_back([&, begin, end, tid]() {
            if (failed.load(std::memory_order_relaxed)) {
                return;
            }
            try {
                fn(begin, end, tid);
            } catch (...) {
                failed.store(true, std::memory_order_relaxed);
                std::lock_guard<std::mutex> lock(exception_mutex);
                if (!first_exception) {
                    first_exception = std::current_exception();
                }
            }
        });
        begin = end;
    }

    for (auto& worker : workers) {
        worker.join();
    }
    if (first_exception) {
        std::rethrow_exception(first_exception);
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
        std::vector<size_t> local_frontiers(clamp_thread_count(num_threads, num_vertices), 0);
        parallel_for_ranges(num_vertices, num_threads, [&](size_t begin, size_t end, size_t tid) {
            size_t local_frontier = 0;
            if (top_down) {
                for (size_t v = begin; v < end; ++v) {
                    if (status[v] != level) {
                        continue;
                    }
                    for_out_neighbors(tid, static_cast<uint32_t>(v), [&](uint32_t dst) {
                        if (dst < num_vertices && compare_and_swap(status[dst], 0, level + 1)) {
                            ++local_frontier;
                        }
                        return true;
                    });
                }
            } else {
                for (size_t v = begin; v < end; ++v) {
                    if (status[v] != 0) {
                        continue;
                    }
                    for_in_neighbors(tid, static_cast<uint32_t>(v), [&](uint32_t src) {
                        if (src < num_vertices &&
                            status[src] == level &&
                            compare_and_swap(status[v], 0, level + 1)) {
                            ++local_frontier;
                            return false;
                        }
                        return status[v] == 0;
                    });
                }
            }
            local_frontiers[tid] = local_frontier;
        });
        frontier = std::accumulate(local_frontiers.begin(), local_frontiers.end(), size_t{0});
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
    parallel_for_ranges(num_vertices, num_threads, [&](size_t begin, size_t end, size_t) {
        for (size_t n = begin; n < end; ++n) {
            outgoing_contrib[n] =
                out_degree[n] == 0 ? 0.0f : init_score / static_cast<ScoreT>(out_degree[n]);
        }
    });

    for (size_t iter = 0; iter < iterations; ++iter) {
        auto iter_begin = std::chrono::steady_clock::now();
        std::vector<ScoreT> prev_outgoing_contrib = outgoing_contrib;
        std::vector<double> local_errors(clamp_thread_count(num_threads, num_vertices), 0.0);
        parallel_for_ranges(num_vertices, num_threads, [&](size_t begin, size_t end, size_t tid) {
            double local_error = 0.0;
            for (size_t u = begin; u < end; ++u) {
                ScoreT incoming_total = 0.0f;
                for_in_neighbors(tid, static_cast<uint32_t>(u), [&](uint32_t src) {
                    if (src < num_vertices) {
                        incoming_total += prev_outgoing_contrib[src];
                    }
                    return true;
                });
                ScoreT old_score = scores[u];
                scores[u] = base_score + kDamp * incoming_total;
                local_error += std::fabs(scores[u] - old_score);
                outgoing_contrib[u] =
                    out_degree[u] == 0 ? 0.0f : scores[u] / static_cast<ScoreT>(out_degree[u]);
            }
            local_errors[tid] = local_error;
        });
        double error = std::accumulate(local_errors.begin(), local_errors.end(), 0.0);
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

void compress_components(uint32_t num_vertices, uint32_t* comp) {
    for (uint32_t n = 0; n < num_vertices; ++n) {
        while (comp[n] != comp[comp[n]]) {
            comp[n] = comp[comp[n]];
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
    parallel_for_ranges(num_vertices, num_threads, [&](size_t begin, size_t end, size_t) {
        for (size_t v = begin; v < end; ++v) {
            comp[v] = static_cast<uint32_t>(v);
        }
    });

    auto sample_begin = std::chrono::steady_clock::now();
    parallel_for_ranges(num_vertices, num_threads, [&](size_t begin, size_t end, size_t tid) {
        for (size_t u = begin; u < end; ++u) {
            size_t d = 0;
            for_out_neighbors(tid, static_cast<uint32_t>(u), [&](uint32_t v) {
                if (d < neighbor_rounds && v < num_vertices) {
                    link_components(static_cast<uint32_t>(u), v, comp.data());
                    ++d;
                    return d < neighbor_rounds;
                }
                return true;
            });
        }
    });
    compress_components(num_vertices, comp.data());

    uint32_t largest_component = sample_frequent_element(comp.data(), num_vertices);
    std::printf("[cc] sample+compress time=%.4f largest_component=%u\n",
                seconds_since(sample_begin),
                largest_component);
    auto final_begin = std::chrono::steady_clock::now();
    parallel_for_ranges(num_vertices, num_threads, [&](size_t begin, size_t end, size_t tid) {
        for (size_t u = begin; u < end; ++u) {
            if (comp[u] == largest_component) {
                continue;
            }

            size_t d = 0;
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
    });
    compress_components(num_vertices, comp.data());
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

        std::printf("[config] dataset=%s\n", opts.input_path.c_str());
        std::printf("[config] storage=%s\n", opts.storage_dir.c_str());
        std::printf("[config] ingest_batch_edges=%zu threads=%zu bfs_rounds=%zu pr_iters=%zu pr_epsilon=%.6f cc_neighbor_rounds=%zu compact=%d\n",
                    opts.ingest_batch_edges,
                    opts.num_threads,
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
            parallel_for_ranges(batch_edges, opts.num_threads, [&](size_t local_begin, size_t local_end, size_t) {
                auto tx = db.BeginTransaction();
                for (size_t i = begin + local_begin; i < begin + local_end; ++i) {
                    const auto& edge = loaded.edges[i];
                    tx.PutEdge(edge.src, edge.dst, edge_out_label, 1.0);
                    tx.PutEdge(edge.dst, edge.src, edge_in_label, 1.0);
                }
            });
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

        double bfs_seconds = 0.0;
        size_t bfs_checksum = 0;
        for (size_t round = 0; round < opts.bfs_rounds; ++round) {
            uint32_t source = select_bfs_source(round, loaded.out_degree);
            auto t0 = std::chrono::steady_clock::now();
            std::printf("[bfs] round=%zu source=%u start\n", round, source);
            auto bfs_out_tx = std::make_shared<BACH::Transaction>(db.BeginReadOnlyTransaction());
            auto bfs_in_tx = std::make_shared<BACH::Transaction>(db.BeginReadOnlyTransaction());
            auto with_out_neighbors = [&](size_t, uint32_t src, auto&& fn) {
                auto edges = bfs_out_tx->GetEdges(src, edge_out_label);
                for (const auto& [dst, property] : *edges) {
                    (void)property;
                    if (!fn(dst)) {
                        break;
                    }
                }
            };
            auto with_in_neighbors = [&](size_t, uint32_t dst, auto&& fn) {
                auto edges = bfs_in_tx->GetEdges(dst, edge_in_label);
                for (const auto& [src, property] : *edges) {
                    (void)property;
                    if (!fn(src)) {
                        break;
                    }
                }
            };
            size_t visited = run_gapbs_bfs(
                loaded.num_vertices, source, opts.num_threads, with_out_neighbors, with_in_neighbors);
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

        auto pr_t0 = std::chrono::steady_clock::now();
        std::printf("[pr] start iterations=%zu epsilon=%.6f\n", opts.pr_iterations, opts.pr_epsilon);
        auto pr_in_tx = std::make_shared<BACH::Transaction>(db.BeginReadOnlyTransaction());
        auto with_in_neighbors = [&](size_t, uint32_t dst, auto&& fn) {
            auto edges = pr_in_tx->GetEdges(dst, edge_in_label);
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
            opts.num_threads,
            opts.pr_iterations,
            opts.pr_epsilon,
            with_in_neighbors);
        auto pr_t1 = std::chrono::steady_clock::now();
        double pr_seconds = std::chrono::duration<double>(pr_t1 - pr_t0).count();
        std::printf("[pr] done sum=%.8f time=%.4f\n", pr_sum, pr_seconds);

        auto cc_t0 = std::chrono::steady_clock::now();
        std::printf("[cc] start neighbor_rounds=%zu\n", opts.cc_neighbor_rounds);
        auto cc_out_tx = std::make_shared<BACH::Transaction>(db.BeginReadOnlyTransaction());
        auto cc_in_tx = std::make_shared<BACH::Transaction>(db.BeginReadOnlyTransaction());
        auto cc_with_out_neighbors = [&](size_t, uint32_t src, auto&& fn) {
            auto edges = cc_out_tx->GetEdges(src, edge_out_label);
            for (const auto& [dst, property] : *edges) {
                (void)property;
                if (!fn(dst)) {
                    break;
                }
            }
        };
        auto cc_with_in_neighbors = [&](size_t, uint32_t dst, auto&& fn) {
            auto edges = cc_in_tx->GetEdges(dst, edge_in_label);
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
            opts.num_threads,
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

        std::printf(EXPOUT "Dataset: %s\n", opts.input_path.c_str());
        std::printf(EXPOUT "StorageDir: %s\n", opts.storage_dir.c_str());
        std::printf(EXPOUT "Vertex count: %u\n", loaded.num_vertices);
        std::printf(EXPOUT "Edge count: %zu\n", loaded.edges.size());
        std::printf(EXPOUT "Load: %.4f\n", load_seconds);
        std::printf(EXPOUT "Ingest: %.4f\n", ingest_seconds);
        std::printf(EXPOUT "BFS: %.4f\n", bfs_seconds);
        std::printf(EXPOUT "PR: %.4f\n", pr_seconds);
        std::printf(EXPOUT "CC: %.4f\n", cc_seconds);
        std::printf(EXPOUT "BFS_Checksum: %zu\n", bfs_checksum);
        std::printf(EXPOUT "PR_Sum: %.8f\n", pr_sum);
        std::printf(EXPOUT "CC_Components: %zu\n", cc_components);
        std::printf(EXPOUT "Total: %.4f\n", total_seconds);
        std::printf("Ingest bandwidth: %.4fM Edges/s\n", ingest_bandwidth);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        return 1;
    }
}
