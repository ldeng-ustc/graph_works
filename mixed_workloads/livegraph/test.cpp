#include <fstream>
#include <iostream>
#include <random>
#include <set>
#include "mixed_graph.hpp"
#include "support.hpp"
#include "sg/mixed_workload/analytics.h"

namespace mw = sg::mixed_workload;
using e12_livegraph::MixedGraph;
using E = dcsr::RawEdge32<void>;

static void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
class ReferenceView {
public:
    explicit ReferenceView(const mw::ReferenceGraph& graph) : graph_(graph) {}
    template<class Callback> void batch_out_neighbors(std::span<const uint32_t> sources, Callback&& callback) {
        std::vector<uint32_t> neighbors;
        for (auto source : sources) {
            neighbors.assign(graph_.neighbors(source).begin(), graph_.neighbors(source).end());
            callback(source, std::span<const uint32_t>(neighbors));
        }
    }
private:
    const mw::ReferenceGraph& graph_;
};
static void compare(MixedGraph& graph, const mw::ReferenceGraph& reference, bool algorithms = false) {
    MixedGraph::View view(graph);
    for (uint32_t source = 0; source < graph.capacity(); ++source) {
        require(graph.alive(source) == reference.is_alive(source), "alive mismatch");
        view.batch_out_neighbors(std::span<const uint32_t>(&source, 1),
            [&](uint32_t, std::span<const uint32_t> neighbors) {
                std::vector<uint32_t> actual(neighbors.begin(), neighbors.end());
                std::sort(actual.begin(), actual.end());
                const std::vector<uint32_t> expected(reference.neighbors(source).begin(), reference.neighbors(source).end());
                require(actual == expected, "visible adjacency mismatch or duplicate");
            });
    }
    const auto& stats = graph.statistics();
    require(stats.attempted == reference.statistics().attempted, "attempt count mismatch");
    require(stats.effective == reference.statistics().effective, "effective count mismatch");
    require(stats.noop == reference.statistics().noop, "no-op count mismatch");
    if (!algorithms) return;
    ReferenceView expected_view(reference);
    auto actual = mw::inspect_visible_graph(view, graph.capacity(), 4, [&](uint32_t v) { return graph.alive(v); });
    auto expected = mw::inspect_visible_graph(expected_view, graph.capacity(), 4,
                                             [&](uint32_t v) { return reference.is_alive(v); });
    require(actual.alive_vertices == expected.alive_vertices && actual.edges.count == expected.edges.count &&
            actual.edges.xor_value == expected.edges.xor_value && actual.edges.sum == expected.edges.sum,
            "visible metadata mismatch");
    require(actual.first_source == expected.first_source, "root mismatch");
    auto a = mw::run_bfs(view, graph.capacity(), actual.first_source, 4);
    auto b = mw::run_bfs(expected_view, graph.capacity(), expected.first_source, 4);
    require(a.distances.count == b.distances.count && a.distances.xor_value == b.distances.xor_value &&
            a.distances.sum == b.distances.sum, "BFS distance digest mismatch");
    auto pa = mw::run_pagerank(view, actual, 4, 10);
    auto pb = mw::run_pagerank(expected_view, expected, 4, 10);
    require(std::abs(pa.sum - pb.sum) < 2e-6 && std::abs(pa.weighted_sum - pb.weighted_sum) < 2e-6,
            "PageRank mismatch");
    auto ca = mw::run_weak_cc(view, actual, 4);
    auto cb = mw::run_weak_cc(expected_view, expected, 4);
    require(ca.components == cb.components && ca.labels.xor_value == cb.labels.xor_value &&
            ca.labels.sum == cb.labels.sum, "CC label mismatch");
}

static void probe(const std::filesystem::path& root) {
    const auto runtime = root / "semantic-probe";
    e12_livegraph::create_runtime(runtime);
    std::vector<E> base{{0,1},{0,1},{1,0},{1,1},{2,1},{1,3},{3,2},{4,4}};
    MixedGraph graph(runtime, 6, 8, uint64_t{1} << 32);
    mw::ReferenceGraph reference(8, 6);
    graph.add_base(base, 4); reference.add_base(base); compare(graph, reference, true);
    uint64_t sequence = 0;
    const auto records = mw::semantic_probe(6);
    for (const auto& record : records) {
        graph.apply(std::span<const mw::UpdateRecord>(&record, 1), ++sequence, 4);
        reference.apply(record, sequence);
        compare(graph, reference);
    }
    // Deleting all live vertices leaves an empty analytical vertex domain.
    for (uint32_t v = 0; v < 8; ++v) {
        mw::UpdateRecord record{mw::UpdateKind::DeleteVertex, v, 0, 0};
        graph.apply(std::span<const mw::UpdateRecord>(&record, 1), ++sequence, 4);
        reference.apply(record, sequence);
    }
    compare(graph, reference, true);
    graph.sync_files();
    std::puts("PASS native base duplicates, all 22 semantic prefixes, empty graph, common algorithms");
}

static void random_and_parallel(const std::filesystem::path& root) {
    const auto runtime = root / "random-parallel";
    e12_livegraph::create_runtime(runtime);
    constexpr uint32_t capacity = 96, base_vertices = 64;
    MixedGraph graph(runtime, base_vertices, capacity, uint64_t{1} << 32);
    mw::ReferenceGraph reference(capacity, base_vertices);
    std::mt19937 random(20261002);
    std::vector<E> base;
    for (size_t i = 0; i < 1200; ++i)
        base.push_back({static_cast<uint32_t>(random()%base_vertices),
                        static_cast<uint32_t>(random()%base_vertices)});
    graph.add_base(base, 4); reference.add_base(base);
    uint64_t sequence = 0;
    for (int round = 0; round < 12; ++round) {
        std::vector<mw::UpdateRecord> records;
        const size_t n = round == 0 ? 6000 : 257;
        for (size_t i = 0; i < n; ++i) {
            const uint32_t kind = round == 0 ? 1 + random()%2 : 1 + random()%4;
            const uint32_t source = random()%capacity;
            const uint32_t target = kind < 3 ? random()%capacity : 0;
            records.push_back({static_cast<mw::UpdateKind>(kind), source, target, 0});
        }
        graph.apply(records, sequence + 1, 4, 31);
        for (const auto& record : records) reference.apply(record, ++sequence);
        compare(graph, reference, round == 0 || round == 11);
    }
    require(graph.statistics().parallel_edge_segments != 0, "parallel path was not exercised");
    // Validate rejection before mutation, including malformed vertex target.
    for (const auto record : std::array<mw::UpdateRecord, 4>{{
             {mw::UpdateKind::InsertEdge, capacity, 0, 0},
             {static_cast<mw::UpdateKind>(5), 0, 0, 0},
             {mw::UpdateKind::InsertVertex, 0, 1, 0},
             {mw::UpdateKind::InsertEdge, 0, 0, 1}}}) {
        bool rejected = false;
        try { graph.apply(std::span<const mw::UpdateRecord>(&record, 1), sequence + 1, 4); }
        catch (const std::exception&) { rejected = true; }
        require(rejected, "invalid record was accepted");
    }
    require(graph.last_sequence() == sequence, "invalid record advanced sequence");
    graph.sync_files();
    std::puts("PASS random lifecycle operations, ordered parallel edge segments, invalid records");
}

static void fixture(const std::filesystem::path& root) {
    const std::vector<E> base{{0,1},{0,1},{1,0},{1,1},{2,1},{1,3},{3,2},{4,4}};
    const auto records = mw::semantic_probe(6);
    mw::TraceHeader header;
    header.base_vertices = 6; header.vertex_capacity = 8; header.base_edges = base.size();
    header.seed = 20261002; header.record_count = records.size(); header.counts = mw::kProbeCounts;
    header.probe_records = records.size();
    for (auto edge : base) header.base_checksum += mw::hash_word(mw::edge_key(edge.from, edge.to));
    std::ofstream base_file(root / "base.bin", std::ios::binary);
    base_file.write(reinterpret_cast<const char*>(base.data()), base.size()*sizeof(E));
    base_file.close();
    mw::TraceWriter writer(root / "updates.bin", header);
    for (const auto& record : records) writer.append(record);
    writer.finish();
    mw::ReferenceGraph reference(8, 6); reference.add_base(base);
    uint64_t sequence = 0;
    for (const auto& record : records) reference.apply(record, ++sequence);
    ReferenceView view(reference);
    auto meta = mw::inspect_visible_graph(view, 8, 4, [&](uint32_t v) { return reference.is_alive(v); });
    constexpr uint32_t source = 6; // Explicit source differs from the automatic root.
    auto bfs = mw::run_bfs(view, 8, source, 4);
    auto pr = mw::run_pagerank(view, meta, 4, 10);
    auto cc = mw::run_weak_cc(view, meta, 4);
    e12_livegraph::Json("fixture_expected")
        .number("alive_vertices", meta.alive_vertices).number("visible_edges", meta.edges.count)
        .number("edge_xor", meta.edges.xor_value).number("edge_sum", meta.edges.sum)
        .number("bfs_source", source).number("bfs_visited", bfs.distances.count)
        .number("bfs_distance_xor", bfs.distances.xor_value).number("bfs_distance_sum", bfs.distances.sum)
        .number("pr_sum", pr.sum).number("pr_weighted_sum", pr.weighted_sum)
        .number("cc_components", cc.components).number("cc_xor", cc.labels.xor_value)
        .number("cc_sum", cc.labels.sum).emit();
}
int main(int argc, char** argv) {
    e12_livegraph::disable_cores();
    try {
        if (argc != 2) throw std::invalid_argument("usage: livegraph_mixed_test NEW_RUNTIME_ROOT");
        const std::filesystem::path root(argv[1]);
        e12_livegraph::create_runtime(root);
        omp_set_dynamic(0);
        probe(root);
        random_and_parallel(root);
        fixture(root);
        std::puts("PASS E12 LiveGraph correctness suite");
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "FAIL: %s\n", error.what());
        return 1;
    }
}
