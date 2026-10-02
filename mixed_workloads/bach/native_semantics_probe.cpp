#include "BACH/db/Transaction.h"
#include "sg/mixed_workload/analytics.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <filesystem>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <vector>

namespace fs = std::filesystem;
using Vid = uint32_t;

struct Graph {
  std::shared_ptr<BACH::Options> options;
  BACH::DB db;
  BACH::label_t vl, el;

  static std::shared_ptr<BACH::Options> make_options(const fs::path& path) {
    if (!fs::create_directories(path)) throw std::runtime_error("runtime must be new");
    auto options = std::make_shared<BACH::Options>();
    options->STORAGE_DIR = path.string();
    options->NUM_OF_COMPACTION_THREAD = 1;
    options->MAX_WORKER_THREAD = 4;
    options->MEMORY_MERGE_NUM = 8;
    // Keep the native elastic policy and normal thresholds. Force only the
    // public PersistenceAll barrier to expose an unmerged older disk layer.
    return options;
  }
  explicit Graph(const fs::path& path) : options(make_options(path)), db(options) {
    vl = db.AddVertexLabel("v");
    el = db.AddEdgeLabel("e", "v", "v");
    auto tx = db.BeginTransaction();
    for (Vid v = 0; v != 8; ++v) {
      if (tx.AddVertex(vl) != v) throw std::runtime_error("non-contiguous vertex");
      tx.PutVertex(vl, v, "alive");
    }
  }
  void put(Vid src, Vid dst, uint64_t seq) {
    auto tx = db.BeginTransaction();
    tx.PutEdge(src, dst, el, static_cast<double>(seq));
  }
  void del(Vid src, Vid dst) {
    auto tx = db.BeginTransaction();
    tx.DelEdge(src, dst, el);
  }
  void del_vertex(Vid v) {
    auto tx = db.BeginTransaction();
    tx.DelVertex(v, vl);
  }
  void put_vertex(Vid v) {
    auto tx = db.BeginTransaction();
    tx.PutVertex(vl, v, "readded");
  }
  void flush() {
    db.Memtable->PersistenceAll();
    db.WaitForBackgroundWork();
    uint64_t files = 0, bytes = 0;
    for (const auto& entry : fs::directory_iterator(options->STORAGE_DIR)) {
      if (entry.is_regular_file()) { ++files; bytes += entry.file_size(); }
    }
    std::cout << "{\"event\":\"forced_flush_completed\",\"files\":" << files
              << ",\"bytes\":" << bytes << "}" << std::endl;
  }
  double point(Vid src, Vid dst) {
    auto tx = db.BeginReadOnlyTransaction();
    return tx.GetEdge(src, dst, el);
  }
  std::vector<std::pair<Vid, double>> neighbors(Vid src) {
    auto tx = db.BeginReadOnlyTransaction();
    return *tx.GetEdges(src, el);
  }
  bool vertex_present(Vid v) {
    auto tx = db.BeginReadOnlyTransaction();
    return bool(tx.GetVertex(v, vl));
  }
};

void print_neighbors(const char* event, Graph& graph, Vid src) {
  const auto edges = graph.neighbors(src);
  std::cout << "{\"event\":\"" << event << "\",\"src\":" << src << ",\"edges\":[";
  bool first = true;
  for (auto [dst, prop] : edges) {
    if (!first) std::cout << ',';
    first = false;
    std::cout << '[' << dst << ',' << prop << ']';
  }
  std::cout << "]}" << std::endl;
}

void observe(const char* name, bool expected_semantics) {
  std::cout << "{\"check\":\"" << name << "\",\"e12_semantics\":"
            << (expected_semantics ? "true" : "false") << "}" << std::endl;
}

void require(bool value, const char* message) {
  if (!value) throw std::runtime_error(message);
}

// Diagnostic only, NOT a production adapter: GetEdges supplies candidates;
// each distinct candidate is checked with native GetEdge. This keeps native
// edge deletion and needs no full-graph edge copy, but disk point lookup is a
// linear scan of one adjacency. Therefore the scan can cost O(sum_v d(v)^2).
class PointFilteredProjection {
 public:
  explicit PointFilteredProjection(Graph& graph) : graph_(graph) {
    for (Vid v = 0; v != 6; ++v) alive_[v] = 1;
  }

  bool visible(Vid src, Vid dst, double property) const {
    return src < 8 && dst < 8 && alive_[src] && alive_[dst] &&
           std::isfinite(property) && property != BACH::TOMBSTONE &&
           property >= static_cast<double>(std::max(birth_[src], birth_[dst]));
  }
  bool insert(Vid src, Vid dst, uint64_t seq) {
    require(seq < (1ULL << 53), "double sequence loses precision");
    if (src >= 8 || dst >= 8 || !alive_[src] || !alive_[dst]) return false;
    if (visible(src, dst, graph_.point(src, dst))) return false;
    graph_.put(src, dst, seq);
    return true;
  }
  bool erase(Vid src, Vid dst) {
    if (src >= 8 || dst >= 8 || !alive_[src] || !alive_[dst]) return false;
    if (!visible(src, dst, graph_.point(src, dst))) return false;
    graph_.del(src, dst);
    return true;
  }
  bool erase_vertex(Vid v) {
    if (v >= 8 || !alive_[v]) return false;
    graph_.del_vertex(v);
    alive_[v] = 0;
    return true;
  }
  bool add_vertex(Vid v, uint64_t seq) {
    if (v >= 8 || alive_[v]) return false;
    graph_.put_vertex(v);
    alive_[v] = 1;
    birth_[v] = seq;
    return true;
  }
  std::vector<Vid> neighbors(Vid src) {
    std::vector<Vid> result;
    if (src >= 8 || !alive_[src]) return result;
    auto tx = graph_.db.BeginReadOnlyTransaction();
    const auto candidates = tx.GetEdges(src, graph_.el);
    Vid previous = UINT32_MAX;
    for (const auto [dst, property] : *candidates) {
      (void)property;
      if (dst == previous) continue;
      previous = dst;
      ++point_probes_;
      const double current = tx.GetEdge(src, dst, graph_.el);
      if (visible(src, dst, current)) result.push_back(dst);
    }
    return result;
  }
  template<class Callback>
  void batch_out_neighbors(std::span<const Vid> sources, Callback&& callback) {
    for (Vid src : sources) {
      const auto targets = neighbors(src);
      callback(src, std::span<const Vid>(targets));
    }
  }
  bool is_alive(Vid v) const { return v < 8 && alive_[v]; }
  uint64_t point_probes() const { return point_probes_; }

 private:
  Graph& graph_;
  std::array<uint8_t, 8> alive_{};
  std::array<uint64_t, 8> birth_{};
  uint64_t point_probes_ = 0;
};

void native_cases(const fs::path& root) {
  {
    Graph graph(root / "native-memtable");
    graph.put(0, 1, 1);
    graph.put(0, 1, 2);
    print_neighbors("same_memtable_duplicate", graph, 0);
    auto edges = graph.neighbors(0);
    observe("same_memtable_duplicate_is_one_latest_edge",
            edges.size() == 1 && edges.front().first == 1 && edges.front().second == 2);
    graph.del(0, 1);
    observe("same_memtable_delete_point", graph.point(0, 1) == BACH::TOMBSTONE);
    observe("same_memtable_delete_scan", graph.neighbors(0).empty());
    graph.del(0, 2);
    observe("delete_missing_scan_empty", graph.neighbors(0).empty());
  }
  {
    Graph graph(root / "native-cross-flush-delete");
    graph.put(0, 1, 1);
    graph.put(0, 2, 2);
    graph.flush();
    print_neighbors("base_after_flush", graph, 0);
    const auto base_edges = graph.neighbors(0);
    observe("base_flush_scan_is_simple", base_edges.size() == 2 &&
            base_edges[0].first == 1 && base_edges[1].first == 2);
    graph.del(0, 1);
    print_neighbors("deleted_in_new_memtable", graph, 0);
    observe("cross_flush_delete_point", graph.point(0, 1) == BACH::TOMBSTONE);
    auto edges = graph.neighbors(0);
    observe("cross_flush_delete_scan", edges.size() == 1 && edges.front().first == 2);
    graph.flush();
    print_neighbors("tombstone_after_second_flush", graph, 0);
    observe("second_flush_delete_point", graph.point(0, 1) == BACH::TOMBSTONE);
    edges = graph.neighbors(0);
    observe("second_flush_delete_scan", edges.size() == 1 && edges.front().first == 2);
  }
  {
    Graph graph(root / "native-cross-flush-duplicate");
    graph.put(0, 1, 1);
    graph.flush();
    graph.put(0, 1, 2);
    print_neighbors("duplicate_in_new_memtable", graph, 0);
    auto edges = graph.neighbors(0);
    observe("cross_flush_duplicate_scan", edges.size() == 1 && edges.front().second == 2);
    observe("cross_flush_duplicate_point", graph.point(0, 1) == 2);
    graph.flush();
    print_neighbors("duplicate_after_second_flush", graph, 0);
    edges = graph.neighbors(0);
    observe("second_flush_duplicate_scan", edges.size() == 1 && edges.front().second == 2);
  }
  {
    Graph graph(root / "native-vertex-lifecycle");
    graph.put(0, 1, 1);
    graph.put(1, 2, 1);
    graph.put(1, 1, 1);
    graph.flush();
    graph.del_vertex(1);
    observe("del_vertex_property_hidden", !graph.vertex_present(1));
    observe("del_vertex_outgoing_hidden", graph.neighbors(1).empty());
    observe("del_vertex_incoming_hidden", graph.neighbors(0).empty());
    graph.put_vertex(1);
    observe("same_id_put_vertex_reactivates_property", graph.vertex_present(1));
    observe("same_id_put_vertex_no_old_outgoing", graph.neighbors(1).empty());
    observe("same_id_put_vertex_no_old_incoming", graph.neighbors(0).empty());
    print_neighbors("readded_vertex_outgoing", graph, 1);
  }
}

void projection_case(const fs::path& root) {
  Graph graph(root / "point-filtered-projection");
  PointFilteredProjection projected(graph);
  require(projected.insert(0, 1, 0), "base insert");
  require(projected.insert(1, 2, 0), "base outgoing");
  require(projected.insert(1, 1, 0), "base self loop");
  require(!projected.insert(0, 1, 0), "duplicate base must be noop");
  graph.flush();
  require(!projected.insert(0, 1, 1), "duplicate persisted edge must be noop");
  require(projected.erase(0, 1), "erase present edge");
  require(!projected.erase(0, 1), "erase deleted edge must be noop");
  require(!projected.erase(0, 3), "erase absent edge must be noop");
  require(projected.neighbors(0).empty(), "point filter must remove old persisted edge");
  graph.flush();
  require(projected.neighbors(0).empty(), "point filter after tombstone flush");
  require(projected.insert(0, 1, 5), "reinsert deleted edge");
  require(projected.neighbors(0) == std::vector<Vid>{1}, "reinsert appears once");
  require(projected.erase_vertex(1), "delete vertex");
  require(!projected.erase_vertex(1), "delete dead vertex noop");
  require(projected.neighbors(0).empty(), "dead incoming filtered");
  require(projected.neighbors(1).empty(), "dead outgoing filtered");
  require(!projected.insert(0, 1, 8), "dead target insert noop");
  require(!projected.insert(1, 3, 9), "dead source insert noop");
  require(!projected.erase(0, 1), "dead endpoint delete noop");
  require(projected.add_vertex(1, 10), "readd vertex");
  require(!projected.add_vertex(1, 11), "duplicate add vertex noop");
  require(projected.neighbors(0).empty(), "readd old incoming does not revive");
  require(projected.neighbors(1).empty(), "readd old outgoing does not revive");
  require(projected.insert(0, 1, 12), "new incarnation incoming");
  require(projected.insert(1, 3, 13), "new incarnation outgoing");
  require(projected.insert(1, 1, 14), "new incarnation self loop");
  require(!projected.insert(1, 7, 15), "reserved but never alive endpoint noop");
  require(projected.add_vertex(7, 16), "first birth of reserved vertex");
  require(projected.insert(1, 7, 17), "edge to newly born vertex");
  graph.flush();
  require(projected.neighbors(0) == std::vector<Vid>{1}, "final incoming");
  require(projected.neighbors(1) == (std::vector<Vid>{1, 3, 7}), "final outgoing/self loop");
  auto metadata = sg::mixed_workload::inspect_visible_graph(projected, 8, 1,
      [&](Vid v) { return projected.is_alive(v); });
  require(metadata.edges.count == 4 && metadata.alive_vertices == 7,
          "shared metadata visible edge and vertex count");
  auto bfs = sg::mixed_workload::run_bfs(projected, 8, 0, 1);
  sg::mixed_workload::ResultDigest expected_bfs;
  expected_bfs.add(0, 0); expected_bfs.add(1, 1);
  expected_bfs.add(3, 2); expected_bfs.add(7, 2);
  require(bfs.distances.count == expected_bfs.count &&
          bfs.distances.xor_value == expected_bfs.xor_value &&
          bfs.distances.sum == expected_bfs.sum && bfs.max_distance == 2,
          "shared BFS exact distance digest");
  auto cc = sg::mixed_workload::run_weak_cc(projected, metadata, 1);
  sg::mixed_workload::ResultDigest expected_cc;
  for (Vid v : {0u, 1u, 3u, 7u}) expected_cc.add(v, 0);
  for (Vid v : {2u, 4u, 5u}) expected_cc.add(v, v);
  require(cc.components == 4 && cc.labels.count == expected_cc.count &&
          cc.labels.xor_value == expected_cc.xor_value && cc.labels.sum == expected_cc.sum,
          "shared CC exact minimum-label digest");
  auto pr = sg::mixed_workload::run_pagerank(projected, metadata, 1, 10);
  require(pr.iterations == 10 && std::abs(pr.sum - 1.0) < 1e-6,
          "shared PR fixed iterations and mass");
  std::cout << "{\"check\":\"shared_e12_algorithms\",\"pass\":true,"
               "\"visible_edges\":" << metadata.edges.count
            << ",\"alive_vertices\":" << metadata.alive_vertices
            << ",\"bfs_count\":" << bfs.distances.count
            << ",\"cc_components\":" << cc.components
            << ",\"pr_iterations\":" << pr.iterations
            << ",\"pr_sum\":" << pr.sum << "}" << std::endl;
  std::cout << "{\"check\":\"point_filtered_projection_all_operations\",\"pass\":true,"
               "\"native_point_probes_in_neighbor_queries\":" << projected.point_probes()
            << ",\"state_bytes\":72,\"global_edge_copy\":false}" << std::endl;
}

int main(int argc, char** argv) {
  rlimit core_limit{0, 0};
  setrlimit(RLIMIT_CORE, &core_limit);
  prctl(PR_SET_DUMPABLE, 0, 0, 0, 0);
  std::cout << std::unitbuf;
  try {
    if (argc != 2) throw std::runtime_error("usage: probe NEW_RUNTIME_ROOT");
    const fs::path root(argv[1]);
    if (!fs::create_directories(root)) throw std::runtime_error("runtime root must be new");
    native_cases(root);
    projection_case(root);
    std::cout << "{\"probe_completed\":true}" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "probe error: " << error.what() << std::endl;
    return 1;
  }
}
