#include <charconv>
#include <fstream>
#include <limits>
#include <map>
#include <sched.h>
#include "mixed_graph.hpp"
#include "support.hpp"
#include "sg/mixed_workload/analytics.h"

namespace mw = sg::mixed_workload;
namespace lg = e12_livegraph;
using Edge = dcsr::RawEdge32<void>;

struct Options {
    std::string input, trace, data, cpus;
    int threads = 16;
    size_t chunk_edges = 2097152, transaction_records = 512, pr_iterations = 10;
    uint64_t mapping_bytes = uint64_t{1} << 40;
    uint32_t bfs_source = UINT32_MAX;
    bool explicit_bfs_source = false;
};

static uint64_t positive_number(std::string_view text) {
    uint64_t value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size() || value == 0)
        throw std::invalid_argument("expected a positive integer");
    return value;
}

static Options parse_options(int argc, char** argv) {
    Options result;
    std::map<std::string, std::string> values;
    for (int i = 1; i < argc; i += 2) {
        if (i + 1 >= argc || !values.emplace(argv[i], argv[i + 1]).second)
            throw std::invalid_argument("options require unique --name value pairs");
    }
    for (const auto& [name, value] : values) {
        if (name == "--input") result.input = value;
        else if (name == "--trace") result.trace = value;
        else if (name == "--data") result.data = value;
        else if (name == "--cpus") result.cpus = value;
        else if (name == "--threads") {
            const uint64_t n = positive_number(value);
            if (n > 1024) throw std::invalid_argument("too many worker threads");
            result.threads = static_cast<int>(n);
        } else if (name == "--chunk-edges") result.chunk_edges = positive_number(value);
        else if (name == "--bfs-source") {
            uint32_t source = 0;
            const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), source);
            if (error != std::errc{} || end != value.data() + value.size())
                throw std::invalid_argument("invalid BFS source");
            result.bfs_source = source;
            result.explicit_bfs_source = true;
        }
        else if (name == "--transaction-records") result.transaction_records = positive_number(value);
        else if (name == "--pr-iterations") result.pr_iterations = positive_number(value);
        else if (name == "--mapping-bytes") result.mapping_bytes = positive_number(value);
        else throw std::invalid_argument("unknown option: " + name);
    }
    if (result.input.empty() || result.trace.empty() || result.data.empty())
        throw std::invalid_argument("--input, --trace and --data are required");
    if (result.chunk_edges > std::numeric_limits<std::streamsize>::max() / sizeof(mw::UpdateRecord))
        throw std::invalid_argument("input chunk is too large");
    return result;
}

static void bind_cpus(const Options& options) {
    if (options.cpus.empty()) return;
    cpu_set_t mask;
    CPU_ZERO(&mask);
    std::istringstream input(options.cpus);
    std::string item;
    size_t count = 0;
    while (std::getline(input, item, ',')) {
        unsigned int cpu = 0;
        const auto [end, error] = std::from_chars(item.data(), item.data() + item.size(), cpu);
        if (error != std::errc{} || end != item.data() + item.size() || cpu >= CPU_SETSIZE || CPU_ISSET(cpu, &mask))
            throw std::invalid_argument("invalid or repeated CPU ID");
        CPU_SET(cpu, &mask);
        ++count;
    }
    if (options.cpus.back() == ',' || count != static_cast<size_t>(options.threads))
        throw std::invalid_argument("CPU count differs from --threads");
    if (::sched_setaffinity(0, sizeof(mask), &mask) != 0) throw std::runtime_error("CPU binding failed");
}

static uint64_t peak_rss_bytes() {
    rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) != 0) throw std::runtime_error("getrusage failed");
    return static_cast<uint64_t>(usage.ru_maxrss) * 1024;
}

int main(int argc, char** argv) {
    lg::disable_cores();
    if (argc == 2 && std::string_view(argv[1]) == "--help") {
        std::puts("livegraph_mixed_bench --input BASE.bin --trace TRACE.bin --data NEW_RUNTIME "
                  "[--threads 16] [--cpus LIST] [--chunk-edges 2097152] "
                  "[--transaction-records 512] [--pr-iterations 10] [--bfs-source ID] "
                  "[--mapping-bytes 1099511627776]");
        return 0;
    }
    try {
        const Options options = parse_options(argc, argv);
        bind_cpus(options);
        omp_set_dynamic(0);
        const auto origin = lg::Clock::now();
        mw::TraceReader trace(options.trace);
        const auto header = trace.header();
        const uint64_t base_bytes = std::filesystem::file_size(options.input);
        if (base_bytes % sizeof(Edge) || base_bytes / sizeof(Edge) != header.base_edges)
            throw std::runtime_error("trace/base edge count mismatch");
        const std::filesystem::path runtime(options.data);
        lg::create_runtime(runtime);
        lg::Json("mixed_config")
            .number("initial_vertices", header.base_vertices).number("vertices", header.vertex_capacity)
            .number("expected_edges", header.base_edges).number("update_records", header.record_count)
            .number("trace_checksum", header.trace_checksum).number("base_checksum", header.base_checksum)
            .number("seed", header.seed).number("threads", options.threads)
            .number("probe_records", header.probe_records).number("candidate_count", header.candidate_count)
            .number("insert_edge_count", header.counts[0]).number("delete_edge_count", header.counts[1])
            .number("insert_vertex_count", header.counts[2]).number("delete_vertex_count", header.counts[3])
            .number("chunk_edges", options.chunk_edges).number("transaction_records", options.transaction_records)
            .number("pr_iterations", options.pr_iterations).number("mapping_bytes", options.mapping_bytes)
            .number("requested_bfs_source", options.bfs_source)
            .text("bfs_source_policy", options.explicit_bfs_source ? "explicit" : "minimum_live_source")
            .text("cpus", options.cpus).text("native_wal", "enabled_fdatasync_per_commit_group")
            .text("mixed_execution", "trace_order_single_writer_native_transactions")
            .text("edge_only_execution", "source_partitioned_parallel_transactions")
            .text("vertex_cascade", "native_vertex_tombstone_plus_alive_birth_edge_sequence_filter")
            .text("load_definition", "update_apply_s+final_drain_s_excludes_preload_and_trace_read").emit();

        const auto base_start = lg::Clock::now();
        std::ifstream input(options.input, std::ios::binary);
        if (!input) throw std::runtime_error("cannot open base input");
        auto edges = std::make_unique<Edge[]>(options.chunk_edges);
        const auto init_start = lg::Clock::now();
        auto graph = std::make_unique<lg::MixedGraph>(runtime, header.base_vertices, header.vertex_capacity,
                                                     options.mapping_bytes);
        const double graph_init_s = lg::seconds(init_start);
        uint64_t loaded = 0, base_checksum = 0;
        double input_read_s = 0, input_validation_s = 0, base_checksum_s = 0, base_add_s = 0;
        while (loaded < header.base_edges) {
            const size_t count = std::min<uint64_t>(options.chunk_edges, header.base_edges - loaded);
            const auto read_start = lg::Clock::now();
            input.read(reinterpret_cast<char*>(edges.get()), count * sizeof(Edge));
            if (input.gcount() != static_cast<std::streamsize>(count * sizeof(Edge)))
                throw std::runtime_error("short base input read");
            input_read_s += lg::seconds(read_start);
            const auto checksum_start = lg::Clock::now();
            uint64_t checksum = 0;
            #pragma omp parallel for num_threads(options.threads) reduction(+:checksum) schedule(static)
            for (size_t i = 0; i < count; ++i)
                checksum += mw::hash_word(mw::edge_key(edges[i].from, edges[i].to));
            base_checksum += checksum;
            base_checksum_s += lg::seconds(checksum_start);
            const auto add_start = lg::Clock::now();
            const double validation_s = graph->add_base(std::span<const Edge>(edges.get(), count), options.threads);
            base_add_s += lg::seconds(add_start) - validation_s;
            input_validation_s += validation_s;
            loaded += count;
            lg::Json("mixed_progress").text("phase", "base").number("edges", loaded)
                .number("elapsed_s", lg::seconds(origin)).emit();
        }
        if (base_checksum != header.base_checksum) throw std::runtime_error("trace/base checksum mismatch");
        input.close();
        edges.reset();
        const auto base_drain_start = lg::Clock::now();
        graph->sync_files();
        const double base_drain_s = lg::seconds(base_drain_start);
        const double initial_load_s = lg::seconds(base_start);
        lg::Json("mixed_preload").number("initial_load_s", initial_load_s)
            .number("input_edges", loaded).number("base_checksum", base_checksum)
            .number("elapsed_s", lg::seconds(origin)).emit();

        auto updates = std::make_unique<mw::UpdateRecord[]>(options.chunk_edges);
        double trace_read_s = 0, update_apply_s = 0;
        const auto updates_start = lg::Clock::now();
        while (true) {
            const uint64_t first_sequence = trace.next_sequence();
            const auto read_start = lg::Clock::now();
            const size_t count = trace.read(std::span<mw::UpdateRecord>(updates.get(), options.chunk_edges));
            trace_read_s += lg::seconds(read_start);
            if (count == 0) break;
            const auto apply_start = lg::Clock::now();
            graph->apply(std::span<const mw::UpdateRecord>(updates.get(), count), first_sequence,
                         options.threads, options.transaction_records);
            update_apply_s += lg::seconds(apply_start);
            lg::Json("mixed_progress").text("phase", "updates").number("operations", trace.records_read())
                .number("update_apply_s", update_apply_s).number("elapsed_s", lg::seconds(origin)).emit();
        }
        trace.finish();
        const auto final_drain_start = lg::Clock::now();
        graph->sync_files();
        const double final_drain_s = lg::seconds(final_drain_start);
        const double updates_wall_s = lg::seconds(updates_start);
        const double preload_and_update_wall_s = initial_load_s + updates_wall_s;
        updates.reset();
        const auto stats = graph->statistics();
        if (graph->last_sequence() != header.record_count || stats.attempted != header.counts)
            throw std::runtime_error("incomplete trace application");
        lg::Json("mixed_updates_done").number("load_s", update_apply_s + final_drain_s)
            .number("update_apply_s", update_apply_s).number("final_drain_s", final_drain_s)
            .number("updates_wall_s", updates_wall_s).number("elapsed_s", lg::seconds(origin)).emit();

        uint64_t scratch_peak = 0;
        const auto metadata_start = lg::Clock::now();
        mw::VisibleMetadata metadata;
        {
            lg::MixedGraph::View view(*graph);
            metadata = mw::inspect_visible_graph(view, header.vertex_capacity, options.threads,
                                                 [&](uint32_t v) { return graph->alive(v); });
            scratch_peak = std::max(scratch_peak, view.scratch_bytes());
        }
        const double metadata_s = lg::seconds(metadata_start);
        const uint32_t bfs_source = options.explicit_bfs_source ? options.bfs_source : metadata.first_source;
        if (options.explicit_bfs_source &&
            (bfs_source >= header.vertex_capacity || !graph->alive(bfs_source)))
            throw std::invalid_argument("BFS source is outside the final live vertex set");
        mw::BfsResult bfs;
        mw::PageRankResult pr;
        mw::CcResult cc;
        double bfs_s, pr_s, cc_s;
        {
            lg::MixedGraph::View view(*graph);
            const auto start = lg::Clock::now();
            bfs = mw::run_bfs(view, header.vertex_capacity, bfs_source, options.threads);
            bfs_s = lg::seconds(start);
            scratch_peak = std::max(scratch_peak, view.scratch_bytes());
        }
        if (options.explicit_bfs_source && bfs.scanned_edges == 0)
            throw std::runtime_error("explicit BFS source scans zero edges; invalid performance sample");
        {
            lg::MixedGraph::View view(*graph);
            const auto start = lg::Clock::now();
            pr = mw::run_pagerank(view, metadata, options.threads, options.pr_iterations);
            pr_s = lg::seconds(start);
            scratch_peak = std::max(scratch_peak, view.scratch_bytes());
        }
        {
            lg::MixedGraph::View view(*graph);
            const auto start = lg::Clock::now();
            cc = mw::run_weak_cc(view, metadata, options.threads);
            cc_s = lg::seconds(start);
            scratch_peak = std::max(scratch_peak, view.scratch_bytes());
        }
        const auto files = lg::footprint(runtime);
        auto summary = lg::Json("mixed_summary");
        summary.number("vertices", header.vertex_capacity).number("initial_vertices", header.base_vertices)
            .number("input_edges", loaded).number("update_records", graph->last_sequence())
            .number("trace_checksum", header.trace_checksum).number("base_checksum", base_checksum)
            .number("threads", options.threads).number("load_s", update_apply_s + final_drain_s)
            .number("preload_and_update_wall_s", preload_and_update_wall_s).number("initial_load_s", initial_load_s)
            .number("graph_init_s", graph_init_s).number("base_add_s", base_add_s).number("base_drain_s", base_drain_s)
            .number("input_read_s", input_read_s).number("input_validation_s", input_validation_s)
            .number("base_checksum_s", base_checksum_s)
            .number("updates_wall_s", updates_wall_s).number("trace_read_s", trace_read_s)
            .number("update_apply_s", update_apply_s).number("final_drain_s", final_drain_s)
            .number("metadata_s", metadata_s).number("bfs_s", bfs_s).number("pr_s", pr_s).number("cc_s", cc_s)
            .number("elapsed_s", lg::seconds(origin)).number("alive_vertices", metadata.alive_vertices)
            .number("visible_edges", metadata.edges.count).number("edge_xor", metadata.edges.xor_value)
            .number("edge_sum", metadata.edges.sum).number("bfs_source", bfs_source)
            .number("bfs_visited", bfs.distances.count).number("bfs_distance_xor", bfs.distances.xor_value)
            .number("bfs_distance_sum", bfs.distances.sum).number("bfs_scanned_edges", bfs.scanned_edges)
            .number("max_distance", bfs.max_distance)
            .number("pr_iterations", pr.iterations).number("pr_sum", pr.sum)
            .number("pr_weighted_sum", pr.weighted_sum).number("pr_last_delta", pr.last_delta)
            .number("cc_components", cc.components).number("cc_xor", cc.labels.xor_value).number("cc_sum", cc.labels.sum)
            .number("physical_bytes", files.physical).number("logical_capacity_bytes", files.logical)
            .number("vertex_state_bytes", graph->lifetime_state_bytes()).number("edge_sequence_property_bytes", 8)
            .number("query_scratch_peak_bytes", scratch_peak).number("peak_rss_bytes", peak_rss_bytes())
            .number("native_write_transactions", stats.write_transactions)
            .number("serial_mixed_transactions", stats.serial_mixed_transactions)
            .number("ordered_vertex_records", stats.ordered_vertex_records)
            .number("parallel_edge_segments", stats.parallel_edge_segments)
            .text("native_gc", "not_invoked_old_incident_edges_retained")
            .text("io_mode", "native_mmap_plus_wal");
        constexpr const char* names[] = {"edge_insert", "edge_erase", "vertex_add", "vertex_erase"};
        for (size_t kind = 0; kind < 4; ++kind) {
            summary.number(std::string(names[kind]) + "_attempts", stats.attempted[kind]);
            summary.number(std::string(names[kind]) + "_effective", stats.effective[kind]);
            summary.number(std::string(names[kind]) + "_noops", stats.noop[kind]);
        }
        summary.emit();
        const auto shutdown_start = lg::Clock::now();
        graph.reset();
        lg::Json("mixed_process_end").number("shutdown_s", lg::seconds(shutdown_start))
            .number("wall_total_s", lg::seconds(origin)).emit();
        return 0;
    } catch (const std::exception& error) {
        lg::Json("error").text("message", error.what()).emit();
        return 1;
    }
}
