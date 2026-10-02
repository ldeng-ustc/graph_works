#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <omp.h>
#include <unistd.h>
#include <tbb/enumerable_thread_specific.h>

#include "core/livegraph.hpp"
#include "sg/mixed_workload/workload.h"

namespace e12_livegraph {
namespace protocol = sg::mixed_workload;
using protocol::UpdateKind;
using protocol::UpdateRecord;

struct Statistics {
    protocol::OperationCounts attempted{}, effective{}, noop{};
    uint64_t write_transactions = 0;
    uint64_t ordered_vertex_records = 0;
    uint64_t serial_mixed_transactions = 0;
    uint64_t parallel_edge_segments = 0;
};

// This adapter adds vertex-lifetime filtering to native LiveGraph. All edges
// and edge deletions remain in LiveGraph; no RAM copy or reverse edge index is
// built. Updates and analytics are phase separated in this E12 entry point.
class MixedGraph {
public:
    MixedGraph(const std::filesystem::path& runtime, uint32_t base_vertices,
               uint32_t capacity, uint64_t mapping_bytes = uint64_t{1} << 40)
        : runtime_(runtime), base_vertices_(base_vertices), capacity_(capacity),
          alive_(std::make_unique<uint8_t[]>(capacity)),
          birth_(std::make_unique<uint64_t[]>(capacity)),
          graph_((runtime / "block.dat").string(), (runtime / "wal.log").string(),
                 mapping_bytes, std::max<uint64_t>(capacity, 1)) {
        if (base_vertices > capacity) throw std::invalid_argument("base exceeds capacity");
        // Physical native slots are reserved once. Logical reserved IDs stay
        // dead until InsertVertex. This cost belongs to initial_load.
        auto txn = graph_.begin_batch_loader();
        for (uint64_t v = 0; v < capacity_; ++v) {
            if (txn.new_vertex() != v) throw std::runtime_error("non-contiguous native ID");
            if (v < base_vertices_) {
                txn.put_vertex(v, std::string_view{});
                alive_[v] = 1;
            }
        }
        txn.commit();
    }

    uint32_t capacity() const noexcept { return capacity_; }
    bool alive(uint32_t v) const { return v < capacity_ && alive_[v] != 0; }
    uint64_t birth(uint32_t v) const { return birth_[v]; }
    uint64_t last_sequence() const noexcept { return last_sequence_; }
    uint64_t lifetime_state_bytes() const noexcept { return uint64_t{capacity_} * 9; }
    const Statistics& statistics() const noexcept { return statistics_; }

    // Return the validation subinterval so the CLI reports disjoint preload
    // components without validating the same input twice.
    double add_base(std::span<const dcsr::RawEdge32<void>> edges, int threads) {
        if (last_sequence_ != 0 || threads < 1)
            throw std::invalid_argument("invalid base phase or worker count");
        const auto validation_start = std::chrono::steady_clock::now();
        for (const auto edge : edges)
            if (edge.from >= base_vertices_ || edge.to >= base_vertices_)
                throw std::out_of_range("base endpoint exceeds initial domain");
        const double validation_s = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - validation_start).count();
        auto errors = std::make_unique<std::exception_ptr[]>(threads);
        #pragma omp parallel num_threads(threads)
        {
            const int tid = omp_get_thread_num();
            try {
                auto txn = graph_.begin_batch_loader();
                const uint64_t sequence = 0;
                const std::string_view data(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
                const size_t begin = edges.size() * tid / omp_get_num_threads();
                const size_t end = edges.size() * (tid + 1) / omp_get_num_threads();
                for (size_t i = begin; i < end; ++i)
                    txn.put_edge(edges[i].from, 0, edges[i].to, data, false);
                txn.commit();
            } catch (...) { errors[tid] = std::current_exception(); }
        }
        for (int tid = 0; tid < threads; ++tid)
            if (errors[tid]) std::rethrow_exception(errors[tid]);
        return validation_s;
    }

    // Mixed batches retain exact trace order within bounded native transactions.
    // This amortizes LiveGraph's per-commit WAL sync without changing any vertex
    // lifetime rule. Pure edge batches can use source ownership in parallel.
    void apply(std::span<const UpdateRecord> records, uint64_t first_sequence,
               int threads, size_t transaction_records = 512) {
        if (threads < 1 || transaction_records == 0 ||
            first_sequence != last_sequence_ + 1 ||
            records.size() > UINT64_MAX - last_sequence_)
            throw std::invalid_argument("invalid trace sequence or worker/batch count");
        for (const auto& record : records) protocol::validate_record(record, capacity_);
        bool has_vertex = false;
        for (const auto& record : records) {
            ++statistics_.attempted[protocol::kind_index(record.kind)];
            has_vertex |= protocol::kind_index(record.kind) >= 2;
        }
        if (!has_vertex) {
            apply_edges_(records, first_sequence, threads, transaction_records);
        } else {
            for (size_t begin = 0; begin < records.size(); begin += transaction_records) {
                auto txn = graph_.begin_transaction();
                const size_t end = std::min(begin + transaction_records, records.size());
                for (size_t i = begin; i < end; ++i) {
                    const auto& record = records[i];
                    const size_t kind = protocol::kind_index(record.kind);
                    const bool changed = kind < 2
                        ? apply_edge_(txn, record, first_sequence + i)
                        : apply_vertex_(txn, record, first_sequence + i);
                    ++(changed ? statistics_.effective[kind] : statistics_.noop[kind]);
                    statistics_.ordered_vertex_records += kind >= 2;
                }
                txn.commit();
                ++statistics_.write_transactions;
                ++statistics_.serial_mixed_transactions;
            }
        }
        last_sequence_ += records.size();
    }

    void sync_files() const {
        // Native commits have completed before this boundary. fdatasync on
        // these files also writes back their dirty MAP_SHARED file pages.
        for (const char* name : {"block.dat", "wal.log"}) {
            const auto file = runtime_ / name;
            const int fd = ::open(file.c_str(), O_RDWR | O_CLOEXEC);
            if (fd < 0) throw std::runtime_error("cannot open graph file for sync");
            const int result = ::fdatasync(fd);
            ::close(fd);
            if (result != 0) throw std::runtime_error("graph file sync failed");
        }
    }

    class View {
    public:
        explicit View(MixedGraph& owner) : owner_(owner) {}
        template<class Callback>
        void batch_out_neighbors(std::span<const uint32_t> sources, Callback&& callback) {
            // Construct and destroy on the actual calling worker: LiveGraph's
            // active read-epoch registry is thread-local.
            auto txn = owner_.graph_.begin_read_only_transaction();
            auto& values = scratch_.local();
            for (uint32_t source : sources) {
                if (source >= owner_.capacity_) throw std::out_of_range("query source");
                values.clear();
                if (owner_.alive(source)) {
                    auto iter = txn.get_edges(source, 0);
                    while (iter.valid()) {
                        const auto target64 = iter.dst_id();
                        if (target64 >= owner_.capacity_)
                            throw std::runtime_error("native target out of range");
                        const auto target = static_cast<uint32_t>(target64);
                        if (owner_.visible_(source, target, iter.edge_data()))
                            values.push_back(target);
                        iter.next();
                    }
                }
                callback(source, std::span<const uint32_t>(values));
            }
        }
        uint64_t scratch_bytes() const {
            uint64_t result = 0;
            for (const auto& values : scratch_) result += values.capacity() * sizeof(uint32_t);
            return result;
        }
    private:
        MixedGraph& owner_;
        tbb::enumerable_thread_specific<std::vector<uint32_t>> scratch_;
    };

private:
    bool visible_(uint32_t source, uint32_t target, std::string_view data) const {
        if (!alive(source) || !alive(target) || data.data() == nullptr) return false;
        if (data.size() != sizeof(uint64_t)) throw std::runtime_error("invalid edge sequence property");
        uint64_t sequence;
        std::memcpy(&sequence, data.data(), sizeof(sequence));
        return sequence >= birth_[source] && sequence >= birth_[target];
    }

    bool apply_vertex_(livegraph::Transaction& txn, const UpdateRecord& record, uint64_t sequence) {
        const uint32_t v = record.source;
        const bool insertion = record.kind == UpdateKind::InsertVertex;
        if (insertion == alive(v)) return false;
        if (insertion) txn.put_vertex(v, std::string_view{});
        else if (!txn.del_vertex(v, false))
            throw std::runtime_error("native vertex deletion disagrees with alive state");
        alive_[v] = insertion;
        if (insertion) birth_[v] = sequence;
        return true;
    }

    bool apply_edge_(livegraph::Transaction& txn, const UpdateRecord& record, uint64_t sequence) {
        const uint32_t source = record.source, target = record.target;
        if (!alive(source) || !alive(target)) return false;
        const auto previous = txn.get_edge(source, 0, target);
        const bool exists = visible_(source, target, previous);
        if (record.kind == UpdateKind::InsertEdge) {
            if (exists) return false;
            const std::string_view data(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
            txn.put_edge(source, 0, target, data, false);
            return true;
        }
        if (!exists) return false;
        if (!txn.del_edge(source, 0, target))
            throw std::runtime_error("native deletion lost a visible edge");
        return true;
    }

    void apply_edges_(std::span<const UpdateRecord> records, uint64_t sequence,
                      int requested_threads, size_t transaction_records) {
        const int threads = records.size() >= static_cast<size_t>(requested_threads) * 128
                                ? requested_threads : 1;
        if (threads > 1) ++statistics_.parallel_edge_segments;
        std::vector<size_t> starts(static_cast<size_t>(threads) + 1, 0);
        for (const auto& record : records) ++starts[record.source % threads + 1];
        for (int tid = 0; tid < threads; ++tid) starts[tid + 1] += starts[tid];
        auto cursor = starts;
        std::vector<size_t> order(records.size());
        for (size_t i = 0; i < records.size(); ++i)
            order[cursor[records[i].source % threads]++] = i;
        auto stats = std::make_unique<Statistics[]>(threads);
        auto errors = std::make_unique<std::exception_ptr[]>(threads);
        #pragma omp parallel for num_threads(threads) schedule(static, 1)
        for (int tid = 0; tid < threads; ++tid) {
            try {
                for (size_t begin = starts[tid]; begin < starts[tid + 1]; begin += transaction_records) {
                    auto txn = graph_.begin_transaction();
                    const size_t end = std::min(begin + transaction_records, starts[tid + 1]);
                    for (size_t pos = begin; pos < end; ++pos) {
                        const size_t index = order[pos];
                        const auto& record = records[index];
                        const bool changed = apply_edge_(txn, record, sequence + index);
                        const size_t kind = protocol::kind_index(record.kind);
                        ++(changed ? stats[tid].effective[kind] : stats[tid].noop[kind]);
                    }
                    txn.commit();
                    ++stats[tid].write_transactions;
                }
            } catch (...) { errors[tid] = std::current_exception(); }
        }
        for (int tid = 0; tid < threads; ++tid) {
            if (errors[tid]) std::rethrow_exception(errors[tid]);
            statistics_.write_transactions += stats[tid].write_transactions;
            for (size_t k = 0; k < 4; ++k) {
                statistics_.effective[k] += stats[tid].effective[k];
                statistics_.noop[k] += stats[tid].noop[k];
            }
        }
    }

    std::filesystem::path runtime_;
    uint32_t base_vertices_, capacity_;
    std::unique_ptr<uint8_t[]> alive_;
    std::unique_ptr<uint64_t[]> birth_;
    livegraph::Graph graph_;
    uint64_t last_sequence_ = 0;
    Statistics statistics_;
};
}  // namespace e12_livegraph
