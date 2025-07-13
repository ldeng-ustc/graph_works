#include <chrono>
#include "Map.cpp"
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


struct BFS_F {
  int32_t* Parents;
  BFS_F(int32_t* _Parents) : Parents(_Parents) {}
  inline bool update (uint32_t s, uint32_t d) { //Update
    // thread_local int k = 0;
    // if(k++ < 4)
    //   printf("Parents[%d] = %d\n", d, s);
    if(Parents[d] == -1) { Parents[d] = s; return 1; }
    else return 0;
  }
  inline bool updateAtomic (uint32_t s, uint32_t d){ //atomic version of Update
    return __sync_bool_compare_and_swap(&Parents[d],-1,s);
  }
  //cond function checks if vertex has been visited yet
  inline bool cond (uint32_t d) { return (Parents[d] == -1); } 
};

template <class Graph>
int32_t* BFS_with_edge_map(Graph &G, uint32_t src) {
  long start = src;
  long n = G.get_num_vertices();
  //creates Parents array, initialized to all -1, except for start
  int32_t* Parents = (int32_t *) malloc(n * sizeof(uint32_t));
  parallel_for(long i=0;i<n;i++) Parents[i] = -1;
  Parents[start] = start;
  VertexSubset frontier = VertexSubset(start, n); //creates initial frontier
  
  size_t level = 0;
  while(frontier.not_empty()){ //loop until frontier is empty
    auto st = std::chrono::high_resolution_clock::now();
    // sparse only, normal directed graph
    VertexSubset next_frontier = edgeMap(G, frontier, BFS_F(Parents), true, 0);
    // dense only, only for in-graph (reverse graph)
    // VertexSubset next_frontier = edgeMap(G, frontier, BFS_F(Parents), true, INT_MAX);    
    // dynamic, incorrect within directed graph, only for undirected graphs
    // VertexSubset next_frontier = edgeMap(G, frontier, BFS_F(Parents), true); 
    frontier.del();
    frontier = next_frontier;
    level++;
    auto ed = std::chrono::high_resolution_clock::now();
    double dur = std::chrono::duration<double>(ed - st).count();
    printf("Level = %lu, Frontier Count = %lu, Time = %.2fs\n", level, frontier.get_n(), dur);
  }
  frontier.del();


#if VERIFY
  std::vector<uint32_t> depths(n, UINT32_MAX);
  for (uint32_t j = 0; j < n; j++) {
    uint32_t current_depth = 0;
    int32_t current_parent = j;
    if (Parents[j] < 0) {
      continue;
    }
    while (current_parent != Parents[current_parent]) {
      current_depth += 1;
      current_parent = Parents[current_parent];
    }
    depths[j] = current_depth;
  }
  
  // write out to file
  std::ofstream myfile;
  myfile.open ("bfs.out");
  for (int i = 0; i < n; i++) {
    myfile << depths[i] << "\n";
  }
  myfile.close();
#endif
  return Parents;
}


template <class Graph>
int32_t* BFS_directed_with_edge_map(Graph &G, uint32_t src) {
  long start = src;
  long n = G.get_num_vertices();
  //creates Parents array, initialized to all -1, except for start
  int32_t* Parents = (int32_t *) malloc(n * sizeof(uint32_t));
  parallel_for(long i=0;i<n;i++) Parents[i] = -1;
  Parents[start] = start;
  VertexSubset frontier = VertexSubset(start, n); //creates initial frontier
  
  size_t level = 0;
  while(frontier.not_empty()){ //loop until frontier is empty
    auto st = std::chrono::high_resolution_clock::now();
    // sparse only, normal directed graph
    VertexSubset next_frontier = edgeMap(G, frontier, BFS_F(Parents), true, 0);
    frontier.del();
    frontier = next_frontier;
    level++;
    auto ed = std::chrono::high_resolution_clock::now();
    double dur = std::chrono::duration<double>(ed - st).count();
    printf("Level = %lu, Frontier Count = %lu, Time = %.2fs\n", level, frontier.get_n(), dur);
  }
  frontier.del();

#if VERIFY
  std::vector<uint32_t> depths(n, UINT32_MAX);
  for (uint32_t j = 0; j < n; j++) {
    uint32_t current_depth = 0;
    int32_t current_parent = j;
    if (Parents[j] < 0) {
      continue;
    }
    while (current_parent != Parents[current_parent]) {
      current_depth += 1;
      current_parent = Parents[current_parent];
    }
    depths[j] = current_depth;
  }
  
  // write out to file
  std::ofstream myfile;
  myfile.open ("bfs.out");
  for (int i = 0; i < n; i++) {
    myfile << depths[i] << "\n";
  }
  myfile.close();
#endif
  return Parents;
}

// template <class TGraph>
// int32_t* BFS_with_edge_map_two_way(TGraph &G, uint32_t src) {
//   long start = src;
//   long n = G.get_num_vertices();
//   //creates Parents array, initialized to all -1, except for start
//   int32_t* Parents = (int32_t *) malloc(n * sizeof(uint32_t));
//   parallel_for(long i=0;i<n;i++) Parents[i] = -1;
//   Parents[start] = start;
//   VertexSubset frontier = VertexSubset(start, n); //creates initial frontier

//   const uint64_t threshold = 20;
  
//   size_t level = 0;
//   while(frontier.not_empty()){ //loop until frontier is empty
//     auto st = std::chrono::high_resolution_clock::now();

//     bool top_down;
//     VertexSubset next_frontier;
//     if (G.get_num_vertices() <= frontier.get_n() * threshold) {
//       top_down = false;
//       next_frontier = EdgeMapDense(G.in(), frontier, BFS_F(Parents), true);
//       // next_frontier = EdgeMapSparse(G.in(), frontier, BFS_F(Parents), true);
//     } else {
//       top_down = true;
//       next_frontier = EdgeMapDense(G.out(), frontier, BFS_F(Parents), true);  // both directions use dense, keep consistent with XPGraph's algo
//       // next_frontier = EdgeMapSparse(G.out(), frontier, BFS_F(Parents), true);
//     }

//     frontier.del();
//     frontier = next_frontier;
//     level++;
//     auto ed = std::chrono::high_resolution_clock::now();
//     double dur = std::chrono::duration<double>(ed - st).count();
//     printf("Top down = %d, Level = %lu, Frontier Count = %lu, Time = %.2fs\n", top_down, level, frontier.get_n(), dur);
//   }
//   frontier.del();
//   return Parents;
// }


struct BFS_TopDown_F {
  int32_t* levels;
  int32_t& cur_level;
  int64_t& frontier;
  int32_t thread_frontier[1024][16];  // max 1024 thread, cache line padding

  BFS_TopDown_F(int32_t* levels, int32_t& cur, int64_t& front) : levels(levels), cur_level(cur), frontier(front) {
    memset(thread_frontier, 0, sizeof(thread_frontier));
  }

  inline bool update(uint32_t d, uint32_t s) {   // it's reverse, when use dense in out-graph
    // if(cur_level == 1) {
    //     printf("%u --> %u\n", s, d);
    // }
    if(levels[d] == 0) {
      levels[d] = cur_level + 1;
      // frontier++;
      thread_frontier[getWorkerNum()][0] ++;
      return 1;
    }
    return 0;
  }

  inline bool updateAtomic (uint32_t d, uint32_t s){
    printf("Never reach here\n");
    exit(1);
    return false;
  }

  //cond function checks if vertex has been visited yet
  inline bool cond(uint32_t s) {
    return levels[s] == cur_level;
  }

  inline void reduction() {
    for (int i = 0; i < getWorkers(); i++) {
      frontier += thread_frontier[i][0];
      thread_frontier[i][0] = 0;
    }
  }
};

struct BFS_DownTop_F {
  int32_t* levels;
  int32_t& cur_level;
  int64_t& frontier;
  int32_t thread_frontier[1024];

  BFS_DownTop_F(int32_t* levels, int32_t& cur, int64_t& front) : levels(levels), cur_level(cur), frontier(front) {
    memset(thread_frontier, 0, sizeof(thread_frontier));
  }

  inline bool update (uint32_t s, uint32_t d) { // pull
    if (levels[s] == cur_level) {
      levels[d] = cur_level + 1;
      thread_frontier[getWorkerNum()] ++;
      return 1;
    }
    return 0;
  }

  inline bool updateAtomic (uint32_t s, uint32_t d){ // push
    printf("Never reach here\n");
    exit(1);
    return false;
  }

  //cond function checks if vertex has been visited yet
  inline bool cond(uint32_t d) {
    return levels[d] == 0;
  }

  inline void reduction() {
    for (int i = 0; i < getWorkers(); i++) {
      frontier += thread_frontier[i];
      thread_frontier[i] = 0;
    }
  }

};



template <class TGraph>
int32_t* BFS_xpgraph(TGraph &G, uint32_t root) {
    int level = 1;
    int64_t frontier = 0;
    size_t v_count = G.get_num_vertices();

    auto levels = new int32_t[v_count];
    std::fill(levels, levels + v_count, 0);

    VertexSubset all = VertexSubset(0, v_count, true);
    BFS_TopDown_F topdown_func = BFS_TopDown_F(levels, level, frontier);
    BFS_DownTop_F downtop_func = BFS_DownTop_F(levels, level, frontier);

    levels[root] = level;
    int	top_down = 1;
    do {
        frontier = 0;
        auto st = std::chrono::high_resolution_clock::now();

        if (top_down) {
            EdgeMapDense<BFS_TopDown_F&>(G.out(), all, topdown_func, false);
            topdown_func.reduction();
        } else { //bottom up
            EdgeMapDense<BFS_DownTop_F&>(G.in(), all, downtop_func, false);
            downtop_func.reduction();
        }

        auto ed = std::chrono::high_resolution_clock::now();
        double level_time = std::chrono::duration<double>(ed - st).count();
        printf("Top down = %d, Level = %u, Frontier Count = %d, Time = %.2fs\n", top_down, level, frontier, level_time);

        //Point is to simulate bottom up bfs, and measure the trade-off    
        if (frontier >= 0.002 * v_count) { // same as GraphOne, XPGraph
        // if (20ull * frontier >= v_count) { // better
            top_down = false;
        } else {
            top_down = true;
        }
        level ++;

        // if(level == 8) {
        //     break;
        // }
    } while (frontier);

  return levels;
}