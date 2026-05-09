#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include <random>
#include <algorithm>
#include <vector>
#include <chrono>

#include "Map.cpp"

/*
GAP Benchmark Suite
Kernel: Connected Components (CC)
Authors: Michael Sutton, Scott Beamer

Will return comp array labelling each vertex with a connected component ID

This CC implementation makes use of the Afforest subgraph sampling algorithm [1],
which restructures and extends the Shiloach-Vishkin algorithm [2].

[1] Michael Sutton, Tal Ben-Nun, and Amnon Barak. "Optimizing Parallel 
    Graph Connectivity Computation via Subgraph Sampling" Symposium on 
    Parallel and Distributed Processing, IPDPS 2018.

[2] Yossi Shiloach and Uzi Vishkin. "An o(logn) parallel connectivity algorithm"
    Journal of Algorithms, 3(1):57–67, 1982.
*/

using NodeID = uint32_t;

template<typename T>
bool compare_and_swap(T &x, const T &old_val, const T &new_val) {
    // printf("CAS: %d %d %d\n", x, old_val, new_val);
    bool res = __sync_bool_compare_and_swap(&x, old_val, new_val);
    // printf("Result: %d\n", res);
    return res;
}

// Place nodes u and v in same component of lower component ID
void Link(NodeID u, NodeID v, NodeID* comp) {
    // printf("Linking %d and %d\n", u, v);
    NodeID p1 = comp[u];
    NodeID p2 = comp[v];
    // printf("p1: %d, p2: %d\n", p1, p2);
    while (p1 != p2) {
        NodeID high = p1 > p2 ? p1 : p2;
        NodeID low = p1 + (p2 - high);
        NodeID p_high = comp[high];
        // printf("high: %d, low: %d, p_high: %d\n", high, low, p_high);
        // Was already 'low' or succeeded in writing 'low'
        if ((p_high == low) || (p_high == high && compare_and_swap(comp[high], high, low))) {
            // printf("Breaking\n");
            break;
        }

        p1 = comp[comp[high]]; // Union-Find ? path compression?
        p2 = comp[low];
        // printf("New p1: %d, New p2: %d\n", p1, p2);
    }
}

struct CC_Vertex_Compress_F {
    NodeID* comp;
    
    CC_Vertex_Compress_F(NodeID* _comp) : comp(_comp) {}

    inline bool operator() (NodeID i) {
        while (comp[i] != comp[comp[i]]) {
            comp[i] = comp[comp[i]];
        }
        return 1;
    }
};

NodeID SampleFrequentElement(const NodeID* comp, NodeID v_count, bool logging=false, size_t num_samples=1024) {
    std::unordered_map<NodeID, int> sample_counts(32);
    using kvp_type = std::unordered_map<NodeID, int>::value_type;
    // Sample elements from 'comp'
    std::mt19937 gen;
    std::uniform_int_distribution<NodeID> distribution(0, v_count - 1);
    for (NodeID i = 0; i < num_samples; i++) {
        NodeID n = distribution(gen);
        sample_counts[comp[n]]++;
    }
    // Find most frequent element in samples (estimate of most frequent overall)
    auto most_frequent = std::max_element(
        sample_counts.begin(), sample_counts.end(),
        [](const kvp_type& a, const kvp_type& b) { return a.second < b.second; });
    float frac_of_graph = static_cast<float>(most_frequent->second) / num_samples;
    // if(logging) {
        printf("Skipping largest intermediate component (ID: %d, approx. %.2f%% of the graph)\n", most_frequent->first, frac_of_graph * 100);
    //}
    return most_frequent->first;
}

// Returns k pairs with the largest values from list of key-value pairs
template<typename KeyT, typename ValT>
std::vector<std::pair<ValT, KeyT>> TopK(const std::vector<std::pair<KeyT, ValT>> &to_sort, size_t k) {
    std::vector<std::pair<ValT, KeyT>> top_k;
    ValT min_so_far = 0;
    for (auto kvp : to_sort) {
        if ((top_k.size() < k) || (kvp.second > min_so_far)) {
            top_k.push_back(std::make_pair(kvp.second, kvp.first));
            std::sort(top_k.begin(), top_k.end(), std::greater<std::pair<ValT, KeyT>>());
            if (top_k.size() > k)
                top_k.resize(k);
            min_so_far = top_k.back().first;
        }
    }
    return top_k;
}

template<typename NodeID>
void PrintCompStats(const NodeID* comp, size_t v_count) {
    printf("\n");
    std::unordered_map<NodeID, NodeID> count;
    for (size_t i = 0; i < v_count; i++)
        count[comp[i]] += 1;
    int k = 5;
    std::vector<std::pair<NodeID, NodeID>> count_vector;
    count_vector.reserve(count.size());
    for (auto kvp : count)
        count_vector.push_back(kvp);
    std::vector<std::pair<NodeID, NodeID>> top_k = TopK(count_vector, k);
    k = std::min(k, static_cast<int>(top_k.size()));
    printf("%d biggest clusters\n", k);
    for(auto kvp : top_k)
        printf("%d:%d\n", kvp.second, kvp.first);
    printf("There are %d components\n", count.size());
}

struct CC_Vertex_Init_F {
    NodeID* comp;

    CC_Vertex_Init_F(NodeID* _comp) : comp(_comp) {}

    inline bool operator() (NodeID i) {
        // if(i % 1024 == 0) {
        //     printf("Processing node %d\n", i);
        // }
        comp[i] = i;
        return 1;
    }
};

VertexSubset _placeholder;  // useless placeholder, never modified or read, just passed to EdgeMap

struct CC_Edge_Sample_F { // Map Dense
    NodeID* comp;
    // int& k;
    int k;
    int r;
    bool has_sample;

    // CC_Edge_Sample_F(NodeID* _comp, int& _k, int _r) : comp(_comp), k(_k), r(_r) {}
    CC_Edge_Sample_F(NodeID* _comp, int _r) : comp(_comp), k(0), r(_r), has_sample(false) {}

    inline bool update(uint32_t d, uint32_t s) {    // Link the r-th pair, k is init to 0
        if(k == r) {
            // if(s == 0 || d == 0) {
            //     printf("%d -> %d\n", s, d);
            //     exit(1);
            // }
            Link(s, d, comp);
            has_sample = true;
            return 1;
        } else {
            k++;
            return 0;
        }
    }

    inline bool cond(uint32_t d) {  // after Link
        return !has_sample;
    }
};

struct CC_Edge_Sample_Once_F { // Map Dense
    NodeID* comp;
    // int& k;
    int k;
    int r;

    CC_Edge_Sample_Once_F(NodeID* _comp, int _r) : comp(_comp), k(0), r(_r) {}

    inline bool update(uint32_t d, uint32_t s) {    // Link the r-th pair, k is init to 0
        // if(s < 100) {
        //     printf("Linking %d and %d\n", s, d);
        // }
        Link(s, d, comp);
        k ++;
        return 1;
    }

    inline bool cond(uint32_t d) {  // after Link
        return k < r;
    }
};

template <class TGraph>
struct CC_Vertex_Sample_F {
    TGraph& G;
    NodeID* comp;
    int round;

    CC_Vertex_Sample_F(TGraph& _G, NodeID* _comp, int _round) : G(_G), comp(_comp), round(_round) {}

    inline bool operator() (NodeID i) {
        // int k = 0;
        // printf("Processing node %d\n", i);
        CC_Edge_Sample_F func(comp, round);
        G.out().map_dense_vs_all(func, _placeholder, _placeholder, i, false);
        return 1;
    }
};

template <class TGraph>
struct CC_Vertex_Sample_Once_F {
    TGraph& G;
    NodeID* comp;
    int round;

    CC_Vertex_Sample_Once_F(TGraph& _G, NodeID* _comp, int _round) : G(_G), comp(_comp), round(_round) {}

    inline bool operator() (NodeID i) {
        // int k = 0;
        // printf("Processing node %d\n", i);
        CC_Edge_Sample_Once_F func(comp, round);
        G.out().map_dense_vs_all(func, _placeholder, _placeholder, i, false);
        return 1;
    }
};

struct CC_Edge_Finalize_F { // Map Dense
    NodeID* comp;
    // int& k;
    int k;
    int r;

    // CC_Edge_Finalize_F(NodeID* _comp, int& _k, int _r) : comp(_comp), k(_k), r(_r) {}
    CC_Edge_Finalize_F(NodeID* _comp, int _r) : comp(_comp), k(0), r(_r) {}

    inline bool update(uint32_t d, uint32_t s) {    // Link the r-th pair, k is init to 0
        if(k >= r) {
            Link(s, d, comp);
            return 1;
        } else {
            k++;
            return 0;
        }
    }

    inline bool cond(uint32_t d) {  // Link rest of the pairs
        return 1;
    }
};


template <class TGraph>
struct CC_Vertex_Finalize_F {
    TGraph& G;
    NodeID* comp;
    int round;
    NodeID max_comp;

    CC_Vertex_Finalize_F(TGraph& _G, NodeID* _comp, int _round, NodeID _max_comp) : G(_G), comp(_comp), round(_round), max_comp(_max_comp) {}

    inline bool operator() (NodeID i) {
        // if(i > 325550) {
        //     printf("Processing node %d\n", i);
        //     printf("comp[i] = %d, max_comp = %d\n", comp[i], max_comp);
        // }

        if(comp[i] == max_comp) {
            return 0;
        }
        // int k = 0;
        // Skip neighbors in the first few rounds (which were sampled)
        CC_Edge_Finalize_F func(comp, round);
        G.out().map_dense_vs_all(func, _placeholder, _placeholder, i, false);

        // To support directed graphs, process reverse graph completely
        func.k = 0;
        func.r = 0; // all reverse edges
        G.in().map_dense_vs_all(func, _placeholder, _placeholder, i, false);
        return 1;
    }
};

void PrintComp(NodeID* comp, size_t n) {
    printf("Compressed comp: ");
    for(int i = 0; i < n; i++) {
        printf("%d, ", comp[i]);
    }
    printf("\n");
}

template <class TGraph>
NodeID* CC_GAPBS_S(TGraph &G, bool logging, size_t neighbor_rounds=2) {
    auto st = std::chrono::high_resolution_clock::now();
    size_t v_count = G.get_num_vertices();
    NodeID* comp = newA(NodeID, v_count);

    VertexSubset all = VertexSubset(0, v_count, true);
    printf("Number of vertices: %d\n", v_count);

    // Initialize each node to a single-node self-pointing tree
    vertexMap(all, CC_Vertex_Init_F(comp), false);
    // PrintComp(comp, 128);

    auto ts1 = std::chrono::high_resolution_clock::now();
    auto t1 = std::chrono::duration<double>(ts1 - st);
    printf("Initialization time: %.2fs\n", t1.count());

    // Process a sparse sampled subgraph first for approximating components.
    // Sample by processing a fixed number of neighbors for each node (see paper)
    // for (int round = 0; round < neighbor_rounds; round++) {
    //     vertexMap(all, CC_Vertex_Sample_F<TGraph>(G, comp, round), false);
    //     // PrintComp(comp, 128);
    //     vertexMap(all, CC_Vertex_Compress_F(comp), false);
    //     // PrintComp(comp, 128);
    // }
    vertexMap(all, CC_Vertex_Sample_Once_F<TGraph>(G, comp, neighbor_rounds), false);
    vertexMap(all, CC_Vertex_Compress_F(comp), false);


    auto ts2 = std::chrono::high_resolution_clock::now();
    auto t2 = std::chrono::duration<double>(ts2 - ts1);
    printf("Sampling time: %.2fs\n", t2.count());

    NodeID c = SampleFrequentElement(comp, v_count, logging);

    // Directed graph, so we need to process both directions
    vertexMap(all, CC_Vertex_Finalize_F<TGraph>(G, comp, neighbor_rounds, c), false);
    // Finally, 'compress' for final convergence
    vertexMap(all, CC_Vertex_Compress_F(comp), false);

    auto ts3 = std::chrono::high_resolution_clock::now();
    auto t3 = std::chrono::duration<double>(ts3 - ts2);
    printf("Finalization time: %.2fs\n", t3.count());

    // if(logging) {
    //     PrintCompStats(comp, v_count);
    // }
    return comp;
}
