#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

namespace oneway_bench {

struct GraphMetadata {
  std::vector<uint32_t> out_degree;
  uint32_t num_vertices = 0;
  uint32_t first_active_vertex = 0;
  bool has_active_vertex = false;
  size_t total_edges = 0;
};

struct BenchmarkConfig {
  size_t algo_threads = 1;
  size_t bfs_rounds = 1;
  size_t pr_iterations = 1;
  double pr_epsilon = 0.0;
  size_t cc_rounds = 1;
};

struct BenchmarkResult {
  double bfs_seconds = 0.0;
  uint64_t bfs_checksum = 0;
  double pr_seconds = 0.0;
  double pr_sum = 0.0;
  double cc_seconds = 0.0;
  size_t cc_components = 0;
};

inline double seconds_since(const std::chrono::steady_clock::time_point& begin) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
}

inline size_t clamp_thread_count(size_t requested_threads, size_t work_items) {
  if (work_items == 0) {
    return 1;
  }
  return std::max<size_t>(1, std::min(requested_threads, work_items));
}

inline void validate_edge_endpoints(uint32_t src,
                                    uint32_t dst,
                                    uint32_t num_vertices,
                                    size_t input_edge_index) {
  if (src < num_vertices && dst < num_vertices) {
    return;
  }
  throw std::out_of_range("edge " + std::to_string(input_edge_index) + " has endpoint (" +
                          std::to_string(src) + ", " + std::to_string(dst) +
                          ") outside --num-vertices " + std::to_string(num_vertices));
}

inline bool should_log_bfs_level(size_t level, size_t next_frontier_size) {
  constexpr size_t kInitialLevels = 3;
  constexpr size_t kLogInterval = 100;
  return level < kInitialLevels || level % kLogInterval == 0 || next_frontier_size == 0;
}

template <typename MakeOutNeighbors>
size_t run_forward_bfs(uint32_t num_vertices,
                       uint32_t source,
                       size_t num_threads,
                       MakeOutNeighbors&& make_out_neighbors) {
  if (num_vertices == 0 || source >= num_vertices) {
    return 0;
  }

  const size_t threads = clamp_thread_count(num_threads, num_vertices);
  auto visited = std::make_unique<std::atomic<uint8_t>[]>(num_vertices);
  for (uint32_t vertex = 0; vertex < num_vertices; ++vertex) {
    visited[vertex].store(0, std::memory_order_relaxed);
  }
  visited[source].store(1, std::memory_order_relaxed);

  std::vector<uint32_t> frontier{source};
  size_t visited_count = 1;
  size_t level = 0;
  while (!frontier.empty()) {
    std::vector<std::vector<uint32_t>> local_next(threads);
#pragma omp parallel num_threads(static_cast<int>(threads))
    {
      const size_t tid = static_cast<size_t>(omp_get_thread_num());
      auto for_out_neighbors = make_out_neighbors();
      auto& next = local_next[tid];
#pragma omp for schedule(dynamic, 1024)
      for (size_t index = 0; index < frontier.size(); ++index) {
        for_out_neighbors(frontier[index], [&](uint32_t dst) {
          if (dst < num_vertices &&
              visited[dst].exchange(1, std::memory_order_relaxed) == 0) {
            next.push_back(dst);
          }
          return true;
        });
      }
    }

    size_t next_size = 0;
    for (const auto& next : local_next) {
      next_size += next.size();
    }
    std::vector<uint32_t> next_frontier;
    next_frontier.reserve(next_size);
    for (auto& next : local_next) {
      next_frontier.insert(next_frontier.end(), next.begin(), next.end());
    }
    visited_count += next_frontier.size();
    if (should_log_bfs_level(level, next_frontier.size())) {
      std::printf("[bfs-forward] level=%zu frontier=%zu next=%zu visited=%zu\n",
                  level,
                  frontier.size(),
                  next_frontier.size(),
                  visited_count);
    }
    frontier.swap(next_frontier);
    ++level;
  }
  return visited_count;
}

template <typename ScoreT>
inline void atomic_add(std::atomic<ScoreT>& target, ScoreT delta) {
  ScoreT old_value = target.load(std::memory_order_relaxed);
  while (!target.compare_exchange_weak(
      old_value, old_value + delta, std::memory_order_relaxed, std::memory_order_relaxed)) {
  }
}

template <typename MakeOutNeighbors>
double run_push_pagerank(uint32_t num_vertices,
                         const std::vector<uint32_t>& out_degree,
                         size_t num_threads,
                         size_t iterations,
                         double epsilon,
                         MakeOutNeighbors&& make_out_neighbors) {
  if (num_vertices == 0) {
    return 0.0;
  }

  using ScoreT = float;
  constexpr ScoreT kDamp = 0.85f;
  const ScoreT initial_score = 1.0f / static_cast<ScoreT>(num_vertices);
  const size_t threads = clamp_thread_count(num_threads, num_vertices);
  std::vector<ScoreT> scores(num_vertices, initial_score);
  auto next_scores = std::make_unique<std::atomic<ScoreT>[]>(num_vertices);

  for (size_t iteration = 0; iteration < iterations; ++iteration) {
    const auto iteration_begin = std::chrono::steady_clock::now();
    double dangling_mass = 0.0;
#pragma omp parallel for num_threads(static_cast<int>(threads)) schedule(static) reduction(+ : dangling_mass)
    for (int64_t vertex = 0; vertex < static_cast<int64_t>(num_vertices); ++vertex) {
      if (out_degree[static_cast<size_t>(vertex)] == 0) {
        dangling_mass += scores[static_cast<size_t>(vertex)];
      }
    }

    const ScoreT base_score =
        (1.0f - kDamp + kDamp * static_cast<ScoreT>(dangling_mass)) /
        static_cast<ScoreT>(num_vertices);
#pragma omp parallel for num_threads(static_cast<int>(threads)) schedule(static)
    for (int64_t vertex = 0; vertex < static_cast<int64_t>(num_vertices); ++vertex) {
      next_scores[static_cast<size_t>(vertex)].store(base_score, std::memory_order_relaxed);
    }

#pragma omp parallel num_threads(static_cast<int>(threads))
    {
      auto for_out_neighbors = make_out_neighbors();
#pragma omp for schedule(dynamic, 1024)
      for (int64_t src = 0; src < static_cast<int64_t>(num_vertices); ++src) {
        const uint32_t degree = out_degree[static_cast<size_t>(src)];
        if (degree == 0) {
          continue;
        }
        const ScoreT contribution =
            kDamp * scores[static_cast<size_t>(src)] / static_cast<ScoreT>(degree);
        for_out_neighbors(static_cast<uint32_t>(src), [&](uint32_t dst) {
          if (dst < num_vertices) {
            atomic_add(next_scores[dst], contribution);
          }
          return true;
        });
      }
    }

    double error = 0.0;
#pragma omp parallel for num_threads(static_cast<int>(threads)) schedule(static) reduction(+ : error)
    for (int64_t vertex = 0; vertex < static_cast<int64_t>(num_vertices); ++vertex) {
      const ScoreT next_score =
          next_scores[static_cast<size_t>(vertex)].load(std::memory_order_relaxed);
      error += std::fabs(next_score - scores[static_cast<size_t>(vertex)]);
      scores[static_cast<size_t>(vertex)] = next_score;
    }
    std::printf("[pr-push] iter=%zu error=%.6f dangling=%.6f time=%.4f\n",
                iteration + 1,
                error,
                dangling_mass,
                seconds_since(iteration_begin));
    if (error < epsilon) {
      break;
    }
  }

  return std::accumulate(scores.begin(), scores.end(), 0.0);
}

inline uint32_t find_component_root(const std::atomic<uint32_t>* parent, uint32_t vertex) {
  uint32_t root = vertex;
  while (true) {
    const uint32_t next = parent[root].load(std::memory_order_relaxed);
    if (next == root) {
      return root;
    }
    root = next;
  }
}

inline void union_components(std::atomic<uint32_t>* parent, uint32_t left, uint32_t right) {
  while (true) {
    const uint32_t left_root = find_component_root(parent, left);
    const uint32_t right_root = find_component_root(parent, right);
    if (left_root == right_root) {
      return;
    }

    const uint32_t high = std::max(left_root, right_root);
    const uint32_t low = std::min(left_root, right_root);
    uint32_t expected = high;
    if (parent[high].compare_exchange_weak(
            expected, low, std::memory_order_relaxed, std::memory_order_relaxed)) {
      return;
    }
  }
}

template <typename MakeOutNeighbors>
size_t run_weak_cc(uint32_t num_vertices,
                   size_t num_threads,
                   MakeOutNeighbors&& make_out_neighbors) {
  if (num_vertices == 0) {
    return 0;
  }

  const size_t threads = clamp_thread_count(num_threads, num_vertices);
  auto parent = std::make_unique<std::atomic<uint32_t>[]>(num_vertices);
#pragma omp parallel for num_threads(static_cast<int>(threads)) schedule(static)
  for (int64_t vertex = 0; vertex < static_cast<int64_t>(num_vertices); ++vertex) {
    parent[static_cast<size_t>(vertex)].store(
        static_cast<uint32_t>(vertex), std::memory_order_relaxed);
  }

#pragma omp parallel num_threads(static_cast<int>(threads))
  {
    auto for_out_neighbors = make_out_neighbors();
#pragma omp for schedule(dynamic, 1024)
    for (int64_t src = 0; src < static_cast<int64_t>(num_vertices); ++src) {
      for_out_neighbors(static_cast<uint32_t>(src), [&](uint32_t dst) {
        if (dst < num_vertices) {
          union_components(parent.get(), static_cast<uint32_t>(src), dst);
        }
        return true;
      });
    }
  }

#pragma omp parallel for num_threads(static_cast<int>(threads)) schedule(static)
  for (int64_t vertex = 0; vertex < static_cast<int64_t>(num_vertices); ++vertex) {
    const uint32_t root = find_component_root(parent.get(), static_cast<uint32_t>(vertex));
    parent[static_cast<size_t>(vertex)].store(root, std::memory_order_relaxed);
  }

  size_t components = 0;
#pragma omp parallel for num_threads(static_cast<int>(threads)) schedule(static) reduction(+ : components)
  for (int64_t vertex = 0; vertex < static_cast<int64_t>(num_vertices); ++vertex) {
    if (parent[static_cast<size_t>(vertex)].load(std::memory_order_relaxed) ==
        static_cast<uint32_t>(vertex)) {
      ++components;
    }
  }
  return components;
}

template <typename MakeOutNeighbors>
BenchmarkResult run_oneway_graph_benchmarks(const GraphMetadata& graph,
                                            const BenchmarkConfig& config,
                                            MakeOutNeighbors&& make_out_neighbors) {
  BenchmarkResult result;
  for (size_t round = 0; round < config.bfs_rounds; ++round) {
    const uint32_t source = graph.has_active_vertex
                                ? static_cast<uint32_t>(
                                      (static_cast<uint64_t>(graph.first_active_vertex) + round) %
                                      graph.num_vertices)
                                : 0;
    const auto round_begin = std::chrono::steady_clock::now();
    const size_t visited =
        run_forward_bfs(graph.num_vertices, source, config.algo_threads, make_out_neighbors);
    result.bfs_checksum += visited;
    const double round_seconds = seconds_since(round_begin);
    result.bfs_seconds += round_seconds;
    std::printf("[bfs-forward] round=%zu source=%u visited=%zu time=%.4f\n",
                round,
                source,
                visited,
                round_seconds);
  }

  const auto pr_begin = std::chrono::steady_clock::now();
  result.pr_sum = run_push_pagerank(graph.num_vertices,
                                    graph.out_degree,
                                    config.algo_threads,
                                    config.pr_iterations,
                                    config.pr_epsilon,
                                    make_out_neighbors);
  result.pr_seconds = seconds_since(pr_begin);
  std::printf("[pr-push] done sum=%.8f time=%.4f\n", result.pr_sum, result.pr_seconds);

  const auto cc_begin = std::chrono::steady_clock::now();
  for (size_t round = 0; round < config.cc_rounds; ++round) {
    const auto round_begin = std::chrono::steady_clock::now();
    result.cc_components =
        run_weak_cc(graph.num_vertices, config.algo_threads, make_out_neighbors);
    std::printf("[cc-weak] round=%zu components=%zu time=%.4f\n",
                round,
                result.cc_components,
                seconds_since(round_begin));
  }
  result.cc_seconds = seconds_since(cc_begin);
  return result;
}

}  // namespace oneway_bench
