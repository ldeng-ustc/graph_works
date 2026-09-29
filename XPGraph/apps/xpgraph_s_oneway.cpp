// SSD reproduction of XPGraph-S using the native XPGraph log, hierarchy and adjacency APIs.
#include "libxpgraph.h"
#include "../../oneway_basic_benchmarks/oneway_graph_benchmarks.hpp"
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <queue>
#include <filesystem>

using Clock = std::chrono::steady_clock;
struct InputEdge { uint32_t src, dst; };
static uint64_t env_num(const char* key, uint64_t fallback) {
    const char* value = std::getenv(key);
    if (!value) return fallback;
    size_t end = 0; const auto parsed = std::stoull(value, &end);
    if (value[end]) throw std::invalid_argument(std::string("invalid ") + key);
    return parsed;
}
static size_t rss_bytes() {
    std::ifstream stat("/proc/self/statm"); size_t ignored, resident;
    stat >> ignored >> resident;
    return resident * static_cast<size_t>(sysconf(_SC_PAGESIZE));
}
struct Reader {
    XPGraph* graph;
    std::vector<uint32_t> scratch;
    template<class F> void operator()(uint32_t src, F visit) {
        const auto degree = graph->get_out_degree(src);
        if (!degree) return;
        scratch.resize(degree);
        const auto count = graph->get_out_nebrs(src, scratch.data());
        if (count != degree) throw std::runtime_error("XPGraph degree / adjacency mismatch");
        for (const auto dst : scratch) if (!visit(dst)) break;
    }
};
static std::unique_ptr<XPGraph> make_graph(const std::string& dir, uint32_t vertices,
                                          unsigned threads, uint64_t edges, bool test = false) {
    std::filesystem::create_directories(dir);
    const size_t pool_mb = env_num("XPGRAPH_S_VBUF_MB", test ? 64 : 1024);
    const size_t max_buf = env_num("XPGRAPH_S_MAX_VBUF", 4 * env_num("XPGRAPH_S_SSD_GRANULARITY", 4096));
    // Sparse virtual capacity; du measures only pages actually written. Preserve all old files.
    const uint64_t blocks_mb = env_num("XPGRAPH_S_PBLK_MB",
        std::max<uint64_t>(512, (edges * 48 + uint64_t(vertices) * 64) / (1 << 20) + threads * 16 + 256));
    static std::vector<std::string> args{"xpgraph_s", "--ssd", "1", "--oneway", "1", "-v", std::to_string(vertices),
        "-t", std::to_string(threads), "-s", "1", "-numa", "0", "-p0", dir + "/", "--persist", "0",
        "--membulk", "4", "--vbuf_pool_mb", std::to_string(pool_mb), "--pblk_pool_mb", std::to_string(blocks_mb),
        "--minvbuf", "16", "--maxvbuf", std::to_string(max_buf), "--elogsize",
        std::to_string(env_num("XPGRAPH_S_ELOG_SHIFT", test ? 15 : 27)), "--batch_shift",
        std::to_string(env_num("XPGRAPH_S_BATCH_SHIFT", test ? 8 : 16)), "--ssd_granularity",
        std::to_string(env_num("XPGRAPH_S_SSD_GRANULARITY", 4096)), "--merge_threshold",
        std::to_string(env_num("XPGRAPH_S_MERGE_THRESHOLD", 8))};
    static std::vector<const char*> argv;
    for (const auto& arg : args) argv.push_back(arg.c_str());
    std::cout << "[xpgraph-s-command]";
    for (const auto& arg : args) std::cout << ' ' << arg;
    std::cout << '\n';
    return std::make_unique<XPGraph>(argv.size(), argv.data());
}
// Test-only per-vertex observations. No reference edge arrays exist in the benchmark path.
template<class Read> std::vector<int> bfs_levels(unsigned n, unsigned root, Read read) {
    std::vector<int> levels(n, -1); levels[root] = 0; std::queue<unsigned> queue; queue.push(root);
    while (!queue.empty()) { auto src = queue.front(); queue.pop();
        read(src, [&](uint32_t dst) { if (levels[dst] < 0) { levels[dst] = levels[src] + 1; queue.push(dst); } return true; });
    } return levels;
}
template<class Read> std::vector<float> pr_scores(unsigned n, const std::vector<uint32_t>& degrees, Read read) {
    std::vector<float> scores(n, 1.0f / n), next(n);
    for (unsigned it = 0; it < 3; ++it) {
        double dangling = 0; for (unsigned v = 0; v < n; ++v) if (!degrees[v]) dangling += scores[v];
        std::fill(next.begin(), next.end(), (1.0f - .85f + .85f * float(dangling)) / n);
        for (unsigned src = 0; src < n; ++src) if (degrees[src]) {
            const auto contribution = .85f * scores[src] / degrees[src];
            read(src, [&](uint32_t dst) { next[dst] += contribution; return true; });
        } scores.swap(next);
    } return scores;
}
template<class Read> std::vector<unsigned> cc_partition(unsigned n, Read read) {
    std::vector<unsigned> labels(n); std::iota(labels.begin(), labels.end(), 0);
    auto root = [&](unsigned v) { while (labels[v] != v) v = labels[v]; return v; };
    for (unsigned src = 0; src < n; ++src) read(src, [&](uint32_t dst) {
        auto a = root(src), b = root(dst); labels[std::max(a,b)] = std::min(a,b); return true; });
    for (unsigned v = 0; v < n; ++v) labels[v] = root(v);
    return labels;
}
static void require(bool condition, const char* message) { if (!condition) throw std::runtime_error(message); }
static void validate(XPGraph& graph, const std::vector<std::vector<uint32_t>>& reference,
                     const std::string& out, unsigned stage) {
    const unsigned n = reference.size(); std::vector<uint32_t> degrees(n);
    Reader native{&graph, {}};
    auto ref = [&](uint32_t src, auto visit) { for (auto dst : reference[src]) if (!visit(dst)) break; };
    std::ofstream rows(out + "/per-vertex-stage-" + std::to_string(stage) + ".csv");
    rows << "vertex,degree,bfs_0,bfs_9,bfs_100,pr_3,cc_label\n";
    for (unsigned src = 0; src < n; ++src) {
        require(graph.get_in_degree(src) == 0, "unexpected in-edge storage");
        degrees[src] = reference[src].size(); std::vector<uint32_t> actual;
        native(src, [&](uint32_t dst) { actual.push_back(dst); return true; });
        auto expected = reference[src]; std::sort(expected.begin(), expected.end()); std::sort(actual.begin(), actual.end());
        require(expected == actual, "adjacency multiset mismatch");
    }
    std::vector<std::vector<int>> levels;
    for (const unsigned root : {0u, 9u, 100u}) {
        const auto expected = bfs_levels(n, root, ref); auto actual = bfs_levels(n, root, native);
        require(expected == actual, "per-vertex BFS mismatch"); levels.push_back(actual);
        size_t reached = 0; for (auto l : expected) reached += l >= 0;
        require(oneway_bench::run_forward_bfs(n, root, 4, [&]{ return Reader{&graph, {}}; }) == reached, "public BFS mismatch");
    }
    const auto ranks = pr_scores(n, degrees, native), expected_ranks = pr_scores(n, degrees, ref);
    for (unsigned v = 0; v < n; ++v) require(std::abs(ranks[v] - expected_ranks[v]) < 2e-6, "per-vertex PR mismatch");
    const auto components = cc_partition(n, native), expected_components = cc_partition(n, ref);
    require(components == expected_components, "per-vertex weak CC mismatch");
    const double sum = oneway_bench::run_push_pagerank(n, degrees, 4, 3, 0, [&]{ return Reader{&graph, {}}; });
    require(std::abs(sum - std::accumulate(expected_ranks.begin(), expected_ranks.end(), 0.0)) < 2e-6, "public PR mismatch");
    size_t cc_count = 0; for (unsigned v = 0; v < n; ++v) cc_count += components[v] == v;
    require(oneway_bench::run_weak_cc(n, 4, [&]{ return Reader{&graph, {}}; }) == cc_count, "public CC mismatch");
    for (unsigned v = 0; v < n; ++v)
        rows << v << ',' << degrees[v] << ',' << levels[0][v] << ',' << levels[1][v] << ',' << levels[2][v]
             << ',' << std::setprecision(10) << ranks[v] << ',' << components[v] << '\n';
    graph.sync_storage(); graph.print_ssd_stats();
    require(graph.verify_ssd_storage(), "SSD file bytes differ from queried mmap adjacency");
    std::cout << "[xpgraph-s-test] stage=" << stage << " vertices=" << n << " multiplicity,bfs,pr,weak_cc=PASS\n";
}
static int self_test(const std::string& output) {
    constexpr unsigned n = 1024;
    auto graph = make_graph(output + "/graph", n, 16, 1500000, true);
    std::vector<std::vector<uint32_t>> reference(n);
    auto add = [&](unsigned src, unsigned dst) { graph->add_edge(src,dst); reference[src].push_back(dst); };
    for (auto edge : {InputEdge{0,1},{1,2},{2,3},{3,3},{0,1},{9,4},{9,8},{100,101},{511,510}}) add(edge.src, edge.dst);
    // >65535 count in one list exercises widened SSD counters and many circular-log wraps.
    for (unsigned i = 0; i < 70017; ++i) add(100, 200 + i % 100);
    // Grow an existing small vertex through hierarchy levels across separate snapshots.
    graph->wait_quiescent();
    for (unsigned size : {2u, 8u, 64u, 1024u}) {
        for (unsigned i = 0; i < size; ++i) add(511,510);
        graph->buffer_all_edges();
    }
    // Distinct long lists allow real overlapping native reads with the same threshold.
    for (unsigned src = 128; src < 144; ++src)
        for (unsigned i = 0; i < 70017; ++i) add(src,200 + i % 100);
    graph->wait_quiescent();
#pragma omp parallel num_threads(16)
    {
        const unsigned src = 128 + omp_get_thread_num();
        std::vector<uint32_t> scratch(reference[src].size());
#pragma omp barrier
        const auto count = graph->get_out_nebrs(src, scratch.data());
        require(count == reference[src].size(), "parallel large-list query lost neighbors");
    }
    graph->print_ssd_stats();
    validate(*graph, reference, output, 1);
    for (unsigned i = 0; i < 101; ++i) add(100, 201);
    add(4,3); add(101,9); add(1023,1022);
    // Small incremental tail, and return to query after buffers were drained previously.
    graph->wait_quiescent();
    validate(*graph, reference, output, 2);
    std::cout << "[xpgraph-s-test] incremental-tail-and-large-list=PASS\n";
    return 0;
}
int main(int argc, char** argv) {
    prctl(PR_SET_DUMPABLE, 0); const rlimit no_core{0, 0}; setrlimit(RLIMIT_CORE, &no_core);
    std::cout.setf(std::ios::unitbuf); omp_set_dynamic(0);
    try {
        if (argc == 3 && std::string(argv[1]) == "--self-test") return self_test(argv[2]);
        if (argc != 6) { std::cerr << "Usage: xpgraph_s_oneway INPUT OUTPUT_DIR THREADS NUM_VERTICES EXPECTED_EDGES\n"; return 2; }
        const auto wall_begin = Clock::now();
        const std::string input = argv[1], output = argv[2];
        const unsigned threads = std::stoul(argv[3]); const uint64_t vertices64 = std::stoull(argv[4]);
        const uint64_t expected_edges = std::stoull(argv[5]);
        require(threads > 0 && threads <= 128, "threads must be 1..128");
        require(vertices64 > 0 && vertices64 < (uint64_t(1) << 31), "XPGraph delete-bit vertex domain exceeded");
        const uint32_t vertices = vertices64; omp_set_num_threads(threads);
        const auto load_begin = Clock::now();
        const int fd = open(input.c_str(), O_RDONLY); if (fd < 0) throw std::runtime_error("cannot open input");
        struct stat st{}; if (fstat(fd,&st)) throw std::runtime_error("fstat failed");
        require(st.st_size % sizeof(InputEdge) == 0, "input is not bin32 edges");
        const uint64_t edge_count = st.st_size / sizeof(InputEdge);
        require(expected_edges == edge_count, "expected_edges does not match input size");
        const auto* edges = edge_count ? static_cast<const InputEdge*>(mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0)) : nullptr;
        if (edges == MAP_FAILED) throw std::runtime_error("input mmap failed");
        oneway_bench::GraphMetadata metadata;
        metadata.num_vertices = vertices; metadata.total_edges = edge_count; metadata.out_degree.assign(vertices,0);
        metadata.has_active_vertex = edge_count != 0; metadata.first_active_vertex = edge_count ? edges[0].src : 0;
        for (uint64_t i = 0; i < edge_count; ++i) {
            oneway_bench::validate_edge_endpoints(edges[i].src, edges[i].dst, vertices, i);
            require(metadata.out_degree[edges[i].src] != UINT32_MAX, "degree overflow"); ++metadata.out_degree[edges[i].src];
        }
        const double load_seconds = oneway_bench::seconds_since(load_begin);
        const auto init_begin = Clock::now(); auto graph = make_graph(output, vertices, threads, edge_count);
        const double init_seconds = oneway_bench::seconds_since(init_begin);
        const auto ingest_begin = Clock::now();
        for (uint64_t i = 0; i < edge_count; ++i) graph->add_edge(edges[i].src, edges[i].dst);
        const double accept_seconds = oneway_bench::seconds_since(ingest_begin);
        const auto drain_begin = Clock::now(); graph->wait_quiescent();
        const double drain_seconds = oneway_bench::seconds_since(drain_begin);
        const double ingest_seconds = oneway_bench::seconds_since(ingest_begin);
        const size_t ingest_rss = rss_bytes();
        if (edges) munmap(const_cast<InputEdge*>(edges), st.st_size);
        close(fd);
        auto make_reader = [&] { return Reader{graph.get(), {}}; };
        oneway_bench::BenchmarkResult result;
        const auto bfs_begin = Clock::now();
        for (size_t round = 0; round < env_num("SG_BENCH_BFS_ROOTS",1); ++round) {
            const uint32_t root = (uint64_t(metadata.first_active_vertex) + round) % vertices;
            const auto visited = oneway_bench::run_forward_bfs(vertices, root, threads, make_reader);
            result.bfs_checksum += visited;
            std::printf("[bfs-forward] round=%zu source=%u visited=%zu\n",round,root,visited);
        }
        graph->sync_storage(); result.bfs_seconds = oneway_bench::seconds_since(bfs_begin);
        const auto pr_begin = Clock::now();
        result.pr_sum = oneway_bench::run_push_pagerank(vertices,metadata.out_degree,threads,env_num("SG_BENCH_PR_ITERS",1),0,make_reader);
        graph->sync_storage(); result.pr_seconds = oneway_bench::seconds_since(pr_begin);
        const auto cc_begin = Clock::now();
        for (size_t round = 0; round < env_num("SG_BENCH_CC_RUNS",1); ++round)
            result.cc_components = oneway_bench::run_weak_cc(vertices,threads,make_reader);
        graph->sync_storage(); result.cc_seconds = oneway_bench::seconds_since(cc_begin);
        graph->print_ssd_stats();
        std::printf("[EXPOUT] Load: %.9f\n[EXPOUT] Init: %.9f\n[EXPOUT] Ingest: %.9f\n",load_seconds,init_seconds,ingest_seconds);
        std::printf("[EXPOUT] BFS: %.9f\n[EXPOUT] PR: %.9f\n[EXPOUT] CC: %.9f\n[EXPOUT] RSS_Ingest: %zu\n",result.bfs_seconds,result.pr_seconds,result.cc_seconds,ingest_rss);
        std::printf("[xpgraph-s-timing] accept_and_background=%.9f drain_flush_sync=%.9f wall=%.9f\n",accept_seconds,drain_seconds,oneway_bench::seconds_since(wall_begin));
        std::printf("[result] edges=%lu bfs_checksum=%lu pr_sum=%.10f cc_components=%zu\n",edge_count,result.bfs_checksum,result.pr_sum,result.cc_components);
        return 0;
    } catch (const std::exception& e) { std::cerr << "[error] " << e.what() << '\n'; return 1; }
}
