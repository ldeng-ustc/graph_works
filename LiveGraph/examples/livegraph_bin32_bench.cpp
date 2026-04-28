#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <numeric>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "livegraph.hpp"

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
    size_t batch_size = 1 << 20;
    size_t num_threads = std::max(1u, std::thread::hardware_concurrency());
    size_t pr_iterations = 10;
    double pr_epsilon = 0.0;
    size_t bfs_rounds = 20;
    size_t cc_neighbor_rounds = 2;
    bool reset = false;
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
        << "  --storage <dir>       Storage root, default ../data/livegraph-bench/<dataset>\n"
        << "  --batch-size <n>      Edge ingest batch size, default 1048576\n"
        << "  --threads <n>         Worker thread count, default hardware concurrency\n"
        << "  --pr-iters <n>        PageRank iterations, default 10\n"
        << "  --pr-epsilon <x>      PageRank early-stop epsilon, default 0\n"
        << "  --bfs-rounds <n>      BFS rounds, default 20\n"
        << "  --cc-neighbor-rounds <n>  CC sampled neighbor rounds, default 2\n"
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
        } else if (arg == "--batch-size") {
            opts.batch_size = std::stoull(require_value("--batch-size"));
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
    if (opts.batch_size == 0) {
        throw std::runtime_error("--batch-size must be > 0");
    }
    if (opts.num_threads == 0) {
        throw std::runtime_error("--threads must be > 0");
    }
    return opts;
}

std::string dataset_name_from_path(const std::string& path) {
    fs::path p(path);
    return p.stem().empty() ? "dataset" : p.stem().string();
}

void log_progress(const char* stage) {
    auto now = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::localtime(&tt);
    std::printf("[progress][%02d:%02d:%02d] %s\n",
                tm.tm_hour,
                tm.tm_min,
                tm.tm_sec,
                stage);
    std::fflush(stdout);
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

size_t clamp_thread_count(size_t requested, size_t work_items) {
    if (work_items == 0) {
        return 1;
    }
    return std::max<size_t>(1, std::min(requested, work_items));
}

template <typename Fn>
void parallel_for_ranges(size_t work_items, size_t num_threads, Fn&& fn) {
    size_t threads = clamp_thread_count(num_threads, work_items);
    if (threads == 1) {
        fn(0, work_items, 0);
        return;
    }

    std::vector<std::thread> workers;
    workers.reserve(threads);
    size_t block = (work_items + threads - 1) / threads;
    for (size_t tid = 0; tid < threads; ++tid) {
        size_t begin = tid * block;
        size_t end = std::min(begin + block, work_items);
        if (begin >= end) {
            break;
        }
        workers.emplace_back([&, begin, end, tid]() { fn(begin, end, tid); });
    }
    for (auto& worker : workers) {
        worker.join();
    }
}

template <typename MakeOutNeighbors, typename MakeInNeighbors>
size_t run_gapbs_bfs(uint32_t num_vertices,
                     uint32_t source,
                     size_t num_threads,
                     MakeOutNeighbors&& make_out_neighbors,
                     MakeInNeighbors&& make_in_neighbors) {
    if (num_vertices == 0) {
        return 0;
    }

    std::vector<uint32_t> status(num_vertices, 0);
    uint32_t level = 1;
    bool top_down = true;
    size_t frontier = 0;
    size_t visited_count = 1;
    status[source] = level;

    std::printf("[bfs] source=%u vertices=%u\n", source, num_vertices);
    std::fflush(stdout);

    do {
        frontier = 0;
        const char* mode = top_down ? "top-down" : "bottom-up";
        std::vector<size_t> local_frontiers(clamp_thread_count(num_threads, num_vertices), 0);
        parallel_for_ranges(num_vertices, num_threads, [&](size_t begin, size_t end, size_t tid) {
            auto for_out_neighbors = make_out_neighbors();
            auto for_in_neighbors = make_in_neighbors();
            size_t local_frontier = 0;
            if (top_down) {
                for (size_t v = begin; v < end; ++v) {
                    if (status[v] != level) {
                        continue;
                    }
                    for_out_neighbors(static_cast<uint32_t>(v), [&](uint32_t dst) {
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
                    for_in_neighbors(static_cast<uint32_t>(v), [&](uint32_t src) {
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
        std::fflush(stdout);
        top_down = !(frontier >= static_cast<size_t>(0.002 * static_cast<double>(num_vertices)));
        ++level;
    } while (frontier);

    return visited_count;
}

template <typename MakeInNeighbors>
double run_gapbs_pr(uint32_t num_vertices,
                    const std::vector<uint32_t>& out_degree,
                    size_t num_threads,
                    size_t iterations,
                    double epsilon,
                    MakeInNeighbors&& make_in_neighbors) {
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
        double error = 0.0;
        std::vector<ScoreT> prev_outgoing_contrib = outgoing_contrib;
        std::vector<double> local_errors(clamp_thread_count(num_threads, num_vertices), 0.0);
        auto iter_begin = std::chrono::steady_clock::now();
        parallel_for_ranges(num_vertices, num_threads, [&](size_t begin, size_t end, size_t tid) {
            auto for_in_neighbors = make_in_neighbors();
            double local_error = 0.0;
            for (size_t u = begin; u < end; ++u) {
                ScoreT incoming_total = 0.0f;
                for_in_neighbors(static_cast<uint32_t>(u), [&](uint32_t src) {
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
        error = std::accumulate(local_errors.begin(), local_errors.end(), 0.0);
        auto iter_end = std::chrono::steady_clock::now();
        std::printf("[pr] iter=%zu error=%.6f time=%.4f\n",
                    iter + 1,
                    error,
                    std::chrono::duration<double>(iter_end - iter_begin).count());
        std::fflush(stdout);
        if (error < epsilon) {
            break;
        }
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

template <typename MakeOutNeighbors, typename MakeInNeighbors>
size_t run_gapbs_cc(uint32_t num_vertices,
                    size_t neighbor_rounds,
                    size_t num_threads,
                    MakeOutNeighbors&& make_out_neighbors,
                    MakeInNeighbors&& make_in_neighbors) {
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
    parallel_for_ranges(num_vertices, num_threads, [&](size_t begin, size_t end, size_t) {
        auto for_out_neighbors = make_out_neighbors();
        for (size_t u = begin; u < end; ++u) {
            size_t d = 0;
            for_out_neighbors(static_cast<uint32_t>(u), [&](uint32_t v) {
                if (d < neighbor_rounds && v < num_vertices) {
                    link_components(static_cast<uint32_t>(u), v, comp.data());
                    ++d;
                    return d < neighbor_rounds;
                }
                return false;
            });
        }
    });
    compress_components(num_vertices, comp.data());
    auto sample_end = std::chrono::steady_clock::now();
    uint32_t largest_component = sample_frequent_element(comp.data(), num_vertices);
    std::printf("[cc] sample+compress time=%.4f largest_component=%u\n",
                std::chrono::duration<double>(sample_end - sample_begin).count(),
                largest_component);
    std::fflush(stdout);

    auto link_begin = std::chrono::steady_clock::now();
    parallel_for_ranges(num_vertices, num_threads, [&](size_t begin, size_t end, size_t) {
        auto for_out_neighbors = make_out_neighbors();
        auto for_in_neighbors = make_in_neighbors();
        for (size_t u = begin; u < end; ++u) {
            if (comp[u] == largest_component) {
                continue;
            }

            size_t d = 0;
            for_out_neighbors(static_cast<uint32_t>(u), [&](uint32_t v) {
                if (d > neighbor_rounds && v < num_vertices) {
                    link_components(static_cast<uint32_t>(u), v, comp.data());
                }
                ++d;
                return true;
            });

            d = 0;
            for_in_neighbors(static_cast<uint32_t>(u), [&](uint32_t v) {
                if (d > neighbor_rounds && v < num_vertices) {
                    link_components(static_cast<uint32_t>(u), v, comp.data());
                }
                ++d;
                return true;
            });
        }
    });
    compress_components(num_vertices, comp.data());
    auto link_end = std::chrono::steady_clock::now();
    std::printf("[cc] final-link+compress time=%.4f\n",
                std::chrono::duration<double>(link_end - link_begin).count());
    std::fflush(stdout);

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
        log_progress("parsed arguments");
        std::printf("[progress] threads=%zu batch_size=%zu\n", opts.num_threads, opts.batch_size);
        std::fflush(stdout);
        if (opts.storage_dir.empty()) {
            opts.storage_dir = "../data/livegraph-bench/" + dataset_name_from_path(opts.input_path);
        }

        log_progress("loading bin32 graph");
        auto ts_load_begin = std::chrono::steady_clock::now();
        LoadedGraph loaded = load_bin32_graph(opts.input_path);
        auto ts_load_end = std::chrono::steady_clock::now();
        std::printf("[progress] loaded graph: vertices=%u edges=%zu\n",
                    loaded.num_vertices,
                    loaded.edges.size());
        std::fflush(stdout);

        fs::path storage_root(opts.storage_dir);
        log_progress("preparing storage directory");
        prepare_storage_dir(storage_root, opts.reset);
        fs::path block_path = storage_root / "block.dat";
        fs::path wal_path = storage_root / "wal.log";

        // LiveGraph's block manager requires a sufficiently large mapped region.
        // For large datasets, the previous heuristic underestimated the space
        // needed by edge-block growth and reverse-edge duplication, causing
        // crashes during put_edge() when the mapped region was exhausted.
        size_t estimated_block_bytes = 1ULL << 40;  // 1 TB virtual mapping

        lg::Graph graph(
            block_path.string(),
            wal_path.string(),
            estimated_block_bytes,
            std::max<lg::vertex_t>(1, loaded.num_vertices));

        log_progress("starting vertex ingest");
        auto ts_ingest_begin = std::chrono::steady_clock::now();
        {
            auto tx = graph.begin_batch_loader();
            for (uint32_t i = 0; i < loaded.num_vertices; ++i) {
                auto vertex_id = tx.new_vertex();
                if (vertex_id != i) {
                    throw std::runtime_error("Vertex id allocation is not contiguous as expected");
                }
                tx.put_vertex(vertex_id, std::string_view());
            }
            tx.commit();
        }
        log_progress("finished vertex ingest");

        size_t progress_every = std::max<size_t>(1, loaded.edges.size() / std::max<size_t>(1, 20));
        size_t next_progress = progress_every;
        log_progress("starting edge ingest");
        for (size_t begin = 0; begin < loaded.edges.size(); begin += opts.batch_size) {
            size_t end = std::min(begin + opts.batch_size, loaded.edges.size());
            std::printf("[progress] edge batch begin: [%zu, %zu)\n", begin, end);
            std::fflush(stdout);
            size_t batch_edges = end - begin;
            parallel_for_ranges(batch_edges, opts.num_threads, [&](size_t local_begin, size_t local_end, size_t) {
                auto tx = graph.begin_batch_loader();
                for (size_t i = begin + local_begin; i < begin + local_end; ++i) {
                    const auto& edge = loaded.edges[i];
                    tx.put_edge(edge.src, 0, edge.dst, std::string_view());
                    tx.put_edge(edge.dst, 1, edge.src, std::string_view());
                }
                tx.commit();
            });
            std::printf("[progress] edge batch done: [%zu, %zu)\n", begin, end);
            std::fflush(stdout);
            if (end >= next_progress || end == loaded.edges.size()) {
                std::printf("[progress] edge ingest: %zu / %zu (%.1f%%)\n",
                            end,
                            loaded.edges.size(),
                            100.0 * static_cast<double>(end) /
                                static_cast<double>(std::max<size_t>(1, loaded.edges.size())));
                std::fflush(stdout);
                next_progress += progress_every;
            }
        }
        auto ts_ingest_end = std::chrono::steady_clock::now();
        log_progress("finished edge ingest");

        double bfs_seconds = 0.0;
        size_t bfs_checksum = 0;
        auto make_out_neighbors = [&graph]() {
            return [tx = graph.begin_read_only_transaction()](uint32_t src, auto&& fn) mutable {
                auto it = tx.get_edges(src, 0);
                while (it.valid()) {
                    uint32_t dst = static_cast<uint32_t>(it.dst_id());
                    if (!fn(dst)) {
                        break;
                    }
                    it.next();
                }
            };
        };
        auto make_in_neighbors = [&graph]() {
            return [tx = graph.begin_read_only_transaction()](uint32_t dst, auto&& fn) mutable {
                auto it = tx.get_edges(dst, 1);
                while (it.valid()) {
                    uint32_t src = static_cast<uint32_t>(it.dst_id());
                    if (!fn(src)) {
                        break;
                    }
                    it.next();
                }
            };
        };

        for (size_t round = 0; round < opts.bfs_rounds; ++round) {
            uint32_t source = select_bfs_source(round, loaded.out_degree);
            std::printf("[progress] bfs round %zu / %zu, source=%u\n",
                        round + 1,
                        opts.bfs_rounds,
                        source);
            std::fflush(stdout);
            auto t0 = std::chrono::steady_clock::now();
            bfs_checksum += run_gapbs_bfs(
                loaded.num_vertices, source, opts.num_threads, make_out_neighbors, make_in_neighbors);
            auto t1 = std::chrono::steady_clock::now();
            bfs_seconds += std::chrono::duration<double>(t1 - t0).count();
        }

        auto pr_t0 = std::chrono::steady_clock::now();
        log_progress("starting pagerank");
        double pr_sum = run_gapbs_pr(
            loaded.num_vertices,
            loaded.out_degree,
            opts.num_threads,
            opts.pr_iterations,
            opts.pr_epsilon,
            make_in_neighbors);
        auto pr_t1 = std::chrono::steady_clock::now();
        double pr_seconds = std::chrono::duration<double>(pr_t1 - pr_t0).count();
        log_progress("finished pagerank");

        auto cc_t0 = std::chrono::steady_clock::now();
        log_progress("starting connected components");
        size_t cc_components = run_gapbs_cc(
            loaded.num_vertices,
            opts.cc_neighbor_rounds,
            opts.num_threads,
            make_out_neighbors,
            make_in_neighbors);
        auto cc_t1 = std::chrono::steady_clock::now();
        double cc_seconds = std::chrono::duration<double>(cc_t1 - cc_t0).count();
        log_progress("finished connected components");

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
        std::printf(EXPOUT "Threads: %zu\n", opts.num_threads);
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
