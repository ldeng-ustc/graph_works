#pragma once
#include <omp.h>
#include <random>
#include <utility>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cinttypes>
#include <functional>
#include <chrono>
#include <cassert>

#include <libxpgraph.h>

/*
GAP Benchmark Suite
Kernel: PageRank (PR)
Author: Scott Beamer

Will return pagerank scores for all vertices once total change < epsilon

This PR implementation uses the traditional iterative approach. It performs
updates in the pull direction to remove the need for atomics, and it allows
new values to be immediately visible (like Gauss-Seidel method). The prior PR
implementation is still available in src/pr_spmv.cc.
*/

using ScoreT = float;
using NodeID = vid_t;
const float kDamp = 0.85;

ScoreT* PageRankPullGS(XPGraph* xpgraph, int max_iters, double epsilon=0, bool logging_enabled = false){
    const size_t v_count = xpgraph->get_vcount();
    const ScoreT init_score = 1.0f / v_count;
    const ScoreT base_score = (1.0f - kDamp) / v_count;


    auto scores = new ScoreT[v_count];
    auto outgoing_contrib = new ScoreT[v_count];
    std::fill_n(scores, v_count, init_score);

    #pragma omp parallel for
    for (NodeID n=0; n < v_count; n++)
        outgoing_contrib[n] = init_score / xpgraph->get_out_degree(n);


    // m.stop_time("5.4.2  -get_degree_information");

    constexpr uint8_t NUM_SOCKETS = 2;
    tid_t ncores_per_socket = omp_get_max_threads() / NUM_SOCKETS / 2;
    omp_set_nested(1);
    printf("ncores_per_socket = %d\n", ncores_per_socket);

    for (int iter=0; iter < max_iters; iter++) {
        auto st = std::chrono::high_resolution_clock::now();
        double error = 0;

        #pragma omp parallel num_threads(NUM_SOCKETS)
        {
            tid_t id = omp_get_thread_num();
            #pragma omp parallel num_threads(omp_get_max_threads()/NUM_SOCKETS)
            {
                tid_t tid = omp_get_thread_num() + id * 40;
                xpgraph->bind_cpu(tid, id);

                #pragma omp for reduction(+ : error) schedule(dynamic, 4096)
                for (NodeID u=id; u < v_count; u+=NUM_SOCKETS) {
                    if(u == 1) {
                        printf("tid = %d, u = %d\n", tid, u);
                    }

                    ScoreT incoming_total = 0;

                    degree_t nebr_count = 0;
                    degree_t local_degree = 0;
                    vid_t* local_adjlist;

                    nebr_count = xpgraph->get_in_degree(u);
                    if (0 == nebr_count) continue;

                    local_adjlist = new vid_t[nebr_count];
                    local_degree = xpgraph->get_in_nebrs(u, local_adjlist);
                    assert(local_degree == nebr_count);

                    // traverse the delta adj list
                    for (index_t j = 0; j < local_degree; ++j){
                        NodeID v = local_adjlist[j];
                        incoming_total += outgoing_contrib[v];
                    }

                    ScoreT old_score = scores[u];
                    scores[u] = base_score + kDamp * incoming_total;
                    error += fabs(scores[u] - old_score);
                    outgoing_contrib[u] = scores[u] / xpgraph->get_out_degree(u);
                    delete [] local_adjlist;
                }

                xpgraph->cancel_bind_cpu();
            }
        }

        auto ed = std::chrono::high_resolution_clock::now();
        double dur = std::chrono::duration<double>(ed - st).count();

        if (error < epsilon)
            break;
        printf("PR Iteration %d (error=%.9f, time=%.2fs)\n", iter, error, dur);
    
    }

    delete [] outgoing_contrib;
    return scores;
}


void PrintScores(ScoreT* scores, int64_t N) {
    ScoreT max_score = 0;
    int64_t idx = 0;
    for (int64_t n=0; n < N; n++) {
        if (scores[n] > max_score) {
            idx = n;
            max_score = scores[n];
        }
    }

    for (int64_t n=0; n < 5; n++)
        printf("Score[%ld] = %.9f\n", n, scores[n]);
    printf("Score[%ld] = %.9f (Max)\n", idx, max_score);
}

ScoreT* pr_gapbs(XPGraph* xpgraph, int max_iters=10) {
    auto scores = PageRankPullGS(xpgraph, max_iters);
    return scores;
}
