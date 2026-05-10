#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <omp.h>
#include <random>
#include <sstream>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "livegraph.hpp"

struct LoadedGraph;

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

struct LiveGraphBenchmarkConfig {
    size_t algo_threads = 1;
    size_t pr_iterations = 10;
    double pr_epsilon = 0.0;
    size_t bfs_rounds = 20;
    size_t cc_rounds = 10;
    size_t cc_neighbor_rounds = 2;
};

struct LiveGraphBenchmarkResult {
    double bfs_seconds = 0.0;
    size_t bfs_checksum = 0;
    long rss_bfs = 0;
    double pr_seconds = 0.0;
    double pr_sum = 0.0;
    double cc_seconds = 0.0;
    size_t cc_components = 0;
};

inline bool compare_and_swap(uint32_t& x, uint32_t old_val, uint32_t new_val) {
    return __sync_bool_compare_and_swap(&x, old_val, new_val);
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

    do {
        frontier = 0;
        const char* mode = top_down ? "top-down" : "bottom-up";
        const int threads = static_cast<int>(clamp_thread_count(num_threads, num_vertices));
        if (top_down) {
            #pragma omp parallel num_threads(threads) reduction(+:frontier)
            {
                auto for_out_neighbors = make_out_neighbors();
                #pragma omp for schedule(dynamic, 16384)
                for (int64_t v = 0; v < static_cast<int64_t>(num_vertices); ++v) {
                    if (status[static_cast<size_t>(v)] != level) {
                        continue;
                    }
                    for_out_neighbors(static_cast<uint32_t>(v), [&](uint32_t dst) {
                        if (dst < num_vertices &&
                            compare_and_swap(status[static_cast<size_t>(dst)], 0, level + 1)) {
                            ++frontier;
                        }
                        return true;
                    });
                }
            }
        } else {
            #pragma omp parallel num_threads(threads) reduction(+:frontier)
            {
                auto for_in_neighbors = make_in_neighbors();
                #pragma omp for schedule(dynamic, 16384)
                for (int64_t v = 0; v < static_cast<int64_t>(num_vertices); ++v) {
                    if (status[static_cast<size_t>(v)] != 0) {
                        continue;
                    }
                    for_in_neighbors(static_cast<uint32_t>(v), [&](uint32_t src) {
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
        #pragma omp parallel num_threads(threads) reduction(+:error)
        {
            auto for_in_neighbors = make_in_neighbors();
            #pragma omp for schedule(dynamic, 16384)
            for (int64_t u = 0; u < static_cast<int64_t>(num_vertices); ++u) {
                ScoreT incoming_total = 0.0f;
                bool has_incoming = false;
                for_in_neighbors(static_cast<uint32_t>(u), [&](uint32_t src) {
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
    for (uint32_t i = 0; i < num_vertices; ++i) {
        count[comp[i]] += 1;
    }

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
    const int threads = static_cast<int>(clamp_thread_count(num_threads, num_vertices));
    #pragma omp parallel for num_threads(threads) schedule(static)
    for (int64_t v = 0; v < static_cast<int64_t>(num_vertices); ++v) {
        comp[static_cast<size_t>(v)] = static_cast<uint32_t>(v);
    }

    const auto sample_begin = std::chrono::steady_clock::now();
    #pragma omp parallel num_threads(threads)
    {
        auto for_out_neighbors = make_out_neighbors();
        #pragma omp for schedule(dynamic, 16384)
        for (int64_t u = 0; u < static_cast<int64_t>(num_vertices); ++u) {
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
    }
    compress_components(num_vertices, comp.data(), num_threads);

    const uint32_t largest_component = sample_frequent_element(comp.data(), num_vertices);
    std::printf("[cc] sample+compress time=%.4f largest_component=%u\n",
                seconds_since(sample_begin),
                largest_component);
    const auto final_begin = std::chrono::steady_clock::now();
    #pragma omp parallel num_threads(threads)
    {
        auto for_out_neighbors = make_out_neighbors();
        auto for_in_neighbors = make_in_neighbors();
        #pragma omp for schedule(dynamic, 16384)
        for (int64_t u = 0; u < static_cast<int64_t>(num_vertices); ++u) {
            if (comp[static_cast<size_t>(u)] == largest_component) {
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

inline LiveGraphBenchmarkResult run_livegraph_graph_benchmarks_gapbs(
    lg::Graph& graph, const LoadedGraph& loaded, const LiveGraphBenchmarkConfig& config) {
    LiveGraphBenchmarkResult result;
    auto make_out_neighbors = [&graph]() {
        return [tx = graph.begin_read_only_transaction()](uint32_t src, auto&& fn) mutable {
            auto it = tx.get_edges(src, 0);
            while (it.valid()) {
                const uint32_t dst = static_cast<uint32_t>(it.dst_id());
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
                const uint32_t src = static_cast<uint32_t>(it.dst_id());
                if (!fn(src)) {
                    break;
                }
                it.next();
            }
        };
    };

    for (size_t round = 0; round < config.bfs_rounds; ++round) {
        const uint32_t source =
            loaded.num_vertices == 0 ? 0 : static_cast<uint32_t>(round % loaded.num_vertices);
        const auto round_begin = std::chrono::steady_clock::now();
        std::printf("[bfs] round=%zu source=%u start\n", round, source);
        const size_t visited = run_gapbs_bfs(
            loaded.num_vertices,
            source,
            config.algo_threads,
            make_out_neighbors,
            make_in_neighbors);
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
    std::printf("[pr] start iterations=%zu epsilon=%.6f\n",
                config.pr_iterations,
                config.pr_epsilon);
    result.pr_sum = run_gapbs_pr(
        loaded.num_vertices,
        loaded.out_degree,
        config.algo_threads,
        config.pr_iterations,
        config.pr_epsilon,
        make_in_neighbors);
    result.pr_seconds = seconds_since(pr_begin);
    std::printf("[pr] done sum=%.8f time=%.4f\n", result.pr_sum, result.pr_seconds);

    const auto cc_begin = std::chrono::steady_clock::now();
    for (size_t round = 0; round < config.cc_rounds; ++round) {
        const auto round_begin = std::chrono::steady_clock::now();
        std::printf("[cc] round=%zu start neighbor_rounds=%zu\n",
                    round,
                    config.cc_neighbor_rounds);
        result.cc_components = run_gapbs_cc(
            loaded.num_vertices,
            config.cc_neighbor_rounds,
            config.algo_threads,
            make_out_neighbors,
            make_in_neighbors);
        std::printf("[cc] round=%zu done components=%zu time=%.4f\n",
                    round,
                    result.cc_components,
                    seconds_since(round_begin));
    }
    result.cc_seconds = seconds_since(cc_begin);
    return result;
}
