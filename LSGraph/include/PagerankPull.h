// This code is part of the project "Ligra: A Lightweight Graph Processing
// Framework for Shared Memory", presented at Principles and Practice of 
// Parallel Programming, 2013.
// Copyright (c) 2013 Julian Shun and Guy Blelloch
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights (to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// #include "ligra.h"
#include "math.h"
#pragma once
#include "Map.cpp"
#define newA(__E,__n) (__E*) malloc((__n)*sizeof(__E))

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

template<typename T>
struct PR_Pull_F {
    T* outgoing_contrib;
    T& incoming_total;


    PR_Pull_F(T* _outgoing_contrib, T& _incoming_total) : 
        outgoing_contrib(_outgoing_contrib), incoming_total(_incoming_total) {}

    inline bool update(uint32_t s, uint32_t d) {
        auto old_incoming = incoming_total;
        incoming_total += outgoing_contrib[s];
        // if(isnanf(incoming_total) || isinff(incoming_total)) {
        // if (d == 26743) {
        //     printf("s: %d, d: %d ", s, d);
        //     printf("old_incoming: %g, outgoing_contrib[s]: %g ", old_incoming, outgoing_contrib[s]);
        //     printf("incoming_total: %g\n", incoming_total);
        //     if(isnanf(incoming_total) || isinff(incoming_total)) {
        //         printf("Error: %d\n", d);
        //         exit(0);
        //     }
        // }
        return 1;
    }

    inline bool cond(uint32_t d) { return 1; }

};

template<class TGraph, typename T=float>
T* PR_Pull_S(TGraph& G, int max_iters, double epsilon=0) {
    using ScoreT = T;
    using NodeID = uint32_t;
    const float kDamp = 0.85;

    VertexSubset all(0, G.get_num_vertices(), true);
    VertexSubset useless_ouput;

    const ScoreT init_score = 1.0f / G.get_num_vertices();
    const ScoreT base_score = (1.0f - kDamp) / G.get_num_vertices();
    printf("Init score: %.9f, Base score: %.9f\n", init_score, base_score);

    ScoreT* scores = newA(ScoreT, G.get_num_vertices());
    ScoreT* outgoing_contrib = newA(ScoreT, G.get_num_vertices());

    parallel_for (NodeID n=0; n < G.get_num_vertices(); n++)
        scores[n] = init_score;

    parallel_for (NodeID n=0; n < G.get_num_vertices(); n++)
        outgoing_contrib[n] = init_score / G.out().degree(n);

    for (int iter=0; iter < max_iters; iter++) {
        double error = 0;
        auto st = std::chrono::high_resolution_clock::now();

        // manually map instead of using edgeMap, to avoid extra incoming_total array
        parallel_for(NodeID u=0; u < G.get_num_vertices(); u++) {
            ScoreT incoming_total = 0;
            auto func = PR_Pull_F(outgoing_contrib, incoming_total);
            G.in().map_dense_vs_all(func, all, useless_ouput, u, false);
            ScoreT old_score = scores[u];
            scores[u] = base_score + kDamp * incoming_total;
            error += fabs(scores[u] - old_score);
            outgoing_contrib[u] = scores[u] / G.out().degree(u);
        }

        auto ed = std::chrono::high_resolution_clock::now();
        double dur = std::chrono::duration<double>(ed - st).count();
        printf("Iteration %d: error = %.9f, %.6f seconds\n", iter, error, dur);

        if (error < epsilon)
        break;
    }
    return scores;
}

template<typename T=float>
void PrintScores(T* scores, int64_t N) {
    T max_score = 0;
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


template<class TGraph, typename T=float>
T* PR_GAPBS_S(TGraph& G, int max_iters, double epsilon=0) {
    return PR_Pull_S<TGraph, T>(G, max_iters, epsilon);
}
