#include <cstdio>
#include <random>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include "livegraph.hpp"

using RandomEngine = std::mt19937_64;

void load_edges(lg::Graph* g, size_t start, size_t end, size_t v, size_t block_size, unsigned int seed) {
    auto st = std::chrono::steady_clock::now();

    RandomEngine rng(seed);
    std::uniform_int_distribution<size_t> vertex_dist(0, v - 1);
    
    // 按块大小处理分配给当前线程的边
    size_t current_start = start;
    while (current_start < end) {
        size_t current_end = std::min(current_start + block_size, end);
        
        auto tx = g->begin_batch_loader();
        for (size_t i = current_start; i < current_end; i++) {
            auto src = vertex_dist(rng);
            auto dst = vertex_dist(rng);
            tx.put_edge(src, 0, dst, std::string_view());
        }
        tx.commit();
        
        current_start = current_end;
    }
    
    auto ed = std::chrono::steady_clock::now();
    auto dur = std::chrono::duration<double>(ed - st).count();
    printf("Thread %u: Inserted edges from %zu to %zu, duration: %.3f s\n", seed, start, end, dur);
}


int main(int argc, char* argv[]) {

    size_t num_threads = 8;  // 线程数
    size_t b = 1 << 20;      // 每个线程每次处理的边块大小
    
    size_t v = 1ul << 20;    // 1M vertices
    size_t e = 1ul << 26;    // 16M edges
    
    if (argc > 1) {
        num_threads = std::stoul(argv[1]);
    }

    auto g = new lg::Graph(
        "../data/livegraph/block",
        "../data/livegraph/wal"//,
        //v * 32,
        //v
    );

    printf("Thread count: %zu\n", num_threads);

    RandomEngine rng(0);
    std::uniform_int_distribution<size_t> vertex_dist(0, v - 1);

    auto st = std::chrono::steady_clock::now();

    auto tx = g->begin_batch_loader();
    // auto tx = g->begin_transaction();
    for(size_t i = 0; i < v; i++) {
        auto vertex_id = tx.new_vertex();
        tx.put_vertex(vertex_id, std::string_view());
        // printf("Inserted vertex %zu.\n", i);
    }
    auto commit_time = tx.commit();
    auto ed_vertex = std::chrono::steady_clock::now();
    auto dur_vertex = std::chrono::duration<double>(ed_vertex - st).count();
    printf("Inserted %zu vertices.", v);
    printf(" Commit time: %ld ms.", commit_time);
    printf(" Duration: %.3f s.\n", dur_vertex);
    printf("Throughput: %.3f M vertices/s.\n", v / dur_vertex / 1e6);


    // 多线程加载边
    st = std::chrono::steady_clock::now();
    
    std::vector<std::thread> threads;
    
    // 计算每个线程负责的边数
    size_t edges_per_thread = (e + num_threads - 1) / num_threads;
    
    // 创建并启动线程，每个线程处理自己分配的边范围
    for (size_t t = 0; t < num_threads; t++) {
        size_t start_idx = t * edges_per_thread;
        size_t end_idx = std::min((t + 1) * edges_per_thread, e);
        
        threads.emplace_back(load_edges, g, start_idx, end_idx, v, b, t);
    }
    
    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }

    auto et = std::chrono::steady_clock::now();
    auto dur = std::chrono::duration<double>(et - st).count();
    printf("Inserted %zu edges using %zu threads with block size %zu.", e, num_threads, b);
    printf(" Duration: %.3f s.\n", dur);
    printf("Throughput: %.3f M edges/s.\n", e / dur / 1e6);
    
    return 0;
}
