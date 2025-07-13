#pragma once
#include <omp.h>
#include <random>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <cassert>
#include <libxpgraph.h>

inline void print_bfs_summary(uint16_t* status, uint16_t level, vid_t v_count, vid_t root){
    vid_t sum = 0;
    for (int l = 1; l < level; ++l) {
        vid_t vid_count = 0;
        #pragma omp parallel for reduction (+:vid_count) 
        for (vid_t v = 0; v < v_count; ++v) {
            if (status[v] == l) ++vid_count;
            //if (status[v] == l && l == 3) cout << v << endl;
        }
        sum += vid_count;
        std::cout << " Level = " << l << " count = " << vid_count << std::endl;
    }
    std::cout << " bfs_summary of root " << root << " = " << sum << std::endl;
}

index_t test_bfs(XPGraph* xpgraph, index_t root_count){
    std::cout << "test_bfs..." << std::endl;
    vid_t           v_count    = xpgraph->get_vcount();

    srand(0);
    index_t total_count = 0;
    while(root_count--){
        vid_t i1 = 0, root = 0;
        while (1) {
            root = rand() % v_count;
            if (xpgraph->get_out_degree(root) > 20) break;
            i1++;
            if (i1 >= v_count) {
                root = 0;
                break;
            }
        }

        int				level      = 1;
        int				top_down   = 1;
        vid_t			frontier   = 0;
        
        // double start1 = mywtime();
        uint16_t* status = new uint16_t[v_count];
        status[root] = level;
        
        total_count += 1; 
        do {
            frontier = 0;
            // double start = mywtime();
            #pragma omp parallel reduction(+:frontier)
            {
                vid_t vid;
                degree_t nebr_count = 0;
                degree_t local_degree = 0;
                vid_t* local_adjlist = 0;
                
                if (top_down) {
                    #pragma omp for nowait
                    for (vid_t v = 0; v < v_count; v++) {
                        if (status[v] != level) continue;
                        
                        nebr_count = xpgraph->get_out_degree(v);
                        if (0 == nebr_count) continue;
                        
                        local_adjlist = new vid_t[nebr_count];
                        local_degree  = xpgraph->get_out_nebrs(v, local_adjlist);
                        assert(local_degree  == nebr_count);
                        
                        //traverse the delta adj list
                        for (degree_t i = 0; i < local_degree; ++i) {
                            vid = local_adjlist[i];
                            if (status[vid] == 0) {
                                status[vid] = level + 1;
                                ++frontier;
                            }
                        }
                        delete [] local_adjlist;
                    }
                } else { // bottom up
                    #pragma omp for nowait
                    for (vid_t v = 0; v < v_count; v++) {
                        if (status[v] != 0) continue;

                        nebr_count = xpgraph->get_in_degree(v);
                        if (0 == nebr_count) continue;

                        local_adjlist = new vid_t[nebr_count];
                        local_degree  = xpgraph->get_in_nebrs(v, local_adjlist);
                        assert(local_degree  == nebr_count);

                        for (degree_t i = 0; i < local_degree; ++i) {
                            vid = local_adjlist[i];
                            if (status[vid] == level) {
                                status[v] = level + 1;
                                ++frontier;
                                break;
                            }
                        }
                        delete [] local_adjlist;
                    }
                }
            }
            
            // double end = mywtime();
            // std::cout << "Top down = " << top_down
            //      << " Level = " << level
            //      << " Frontier Count = " << frontier
            //      << " Time = " << end - start
            //      << std::endl;

            // Point is to simulate bottom up bfs, and measure the trade-off    
            if (frontier >= 0.002 * v_count) {
                top_down = false;
            } else {
                top_down = true;
            }
            ++level;
            total_count += frontier;
        } while (frontier);

        print_bfs_summary(status, level, v_count, root);
        delete [] status;
    }
    return total_count;
}

index_t test_bfs_numa(XPGraph* xpgraph, index_t root=1){
    std::cout << "test_bfs_numa from" << root << std::endl;
    vid_t           v_count    = xpgraph->get_vcount();

    srand(0);
    index_t total_count = 0;

    index_t root_count = 1;
    while(root_count--){
        int				level      = 1;
        int				top_down   = 1;
        vid_t			frontier   = 0;

        // double start1 = mywtime();
        uint16_t* status = new uint16_t[v_count];
        status[root] = level;

        // uint16_t NUM_SOCKETS = 2;
        // tid_t ncores_per_socket = omp_get_max_threads() / NUM_SOCKETS / 2; //24
        
        index_t total_count = 1; 
        do {
            frontier = 0;
            // double start = mywtime();
            auto st = std::chrono::high_resolution_clock::now();
            #pragma omp parallel reduction(+:frontier)
            {
                vid_t vid;
                degree_t nebr_count = 0;
                degree_t local_degree = 0;
                vid_t* local_adjlist = 0;

                tid_t tid = omp_get_thread_num();

                if (top_down) {
                    // First process vertices in socket 0
                    xpgraph->bind_cpu(tid, 0);
                    #pragma omp for nowait
                    for (vid_t v = 0; v < v_count; v+=2) {
                        if (status[v] != level) continue;
                        
                        nebr_count = xpgraph->get_out_degree(v);
                        if (0 == nebr_count) continue;
                        
                        local_adjlist = new vid_t[nebr_count];
                        local_degree  = xpgraph->get_out_nebrs(v, local_adjlist);
                        assert(local_degree  == nebr_count);
                        
                        //traverse the delta adj list
                        for (degree_t i = 0; i < local_degree; ++i) {
                            vid = local_adjlist[i];
                            if (status[vid] == 0) {
                                status[vid] = level + 1;
                                ++frontier;
                                //cout << " " << sid << endl;
                            }
                        }
                        delete [] local_adjlist;
                    }

                    // Then process vertices in socket 1
                    xpgraph->bind_cpu(tid, 1);
                    #pragma omp for nowait
                    for (vid_t v = 1; v < v_count; v+=2) {
                        if (status[v] != level) continue;
                        
                        nebr_count = xpgraph->get_out_degree(v);
                        if (0 == nebr_count) continue;
                        
                        local_adjlist = new vid_t[nebr_count];
                        local_degree  = xpgraph->get_out_nebrs(v, local_adjlist);
                        assert(local_degree  == nebr_count);
                        
                        //traverse the delta adj list
                        for (degree_t i = 0; i < local_degree; ++i) {
                            vid = local_adjlist[i];
                            if (status[vid] == 0) {
                                status[vid] = level + 1;
                                ++frontier;
                                //cout << " " << sid << endl;
                            }
                        }
                        delete [] local_adjlist;
                    }
                } else { // bottom up
                    // First process vertices in socket 0
                    xpgraph->bind_cpu(tid, 0);
                    #pragma omp for nowait
                    for (vid_t v = 0; v < v_count; v+=2) {
                        if (status[v] != 0) continue;

                        nebr_count = xpgraph->get_in_degree(v);
                        if (0 == nebr_count) continue;

                        local_adjlist = new vid_t[nebr_count];
                        local_degree  = xpgraph->get_in_nebrs(v, local_adjlist);
                        assert(local_degree  == nebr_count);

                        for (degree_t i = 0; i < local_degree; ++i) {
                            vid = local_adjlist[i];
                            if (status[vid] == level) {
                                status[v] = level + 1;
                                ++frontier;
                                break;
                            }
                        }
                        delete [] local_adjlist;
                    }

                    // Then process vertices in socket 1
                    xpgraph->bind_cpu(tid, 1);
                    #pragma omp for nowait
                    for (vid_t v = 1; v < v_count; v+=2) {
                        if (status[v] != 0) continue;

                        nebr_count = xpgraph->get_in_degree(v);
                        if (0 == nebr_count) continue;

                        local_adjlist = new vid_t[nebr_count];
                        local_degree  = xpgraph->get_in_nebrs(v, local_adjlist);
                        assert(local_degree  == nebr_count);

                        for (degree_t i = 0; i < local_degree; ++i) {
                            vid = local_adjlist[i];
                            if (status[vid] == level) {
                                status[v] = level + 1;
                                ++frontier;
                                break;
                            }
                        }
                        delete [] local_adjlist;
                    }
                }
                xpgraph->cancel_bind_cpu();
            }

            auto ed = std::chrono::high_resolution_clock::now();
            double dur = std::chrono::duration<double>(ed - st).count();
            if(root == 1)
                std::cout << "Level = " << level << " Frontier Count = " << frontier << " Time = " << dur << " (Top Down = " << top_down << ")" << std::endl;
            // double end = mywtime();
            // std::cout << "Top down = " << top_down
            //      << " Level = " << level
            //      << " Frontier Count = " << frontier
            //      << " Time = " << end - start
            //      << std::endl;

            // Point is to simulate bottom up bfs, and measure the trade-off    
            if (frontier >= 0.002 * v_count) {
                top_down = false;
            } else {
                top_down = true;
            }
            ++level;
            total_count += frontier;
        } while (frontier);

        // print_bfs_summary(status, level, v_count, root);
        delete [] status;
    }
    return total_count;
}



class Bitmap {
  template<typename T>
  inline  bool compare_and_swap(T &x, const T &old_val, const T &new_val) {
      return __sync_bool_compare_and_swap(&x, old_val, new_val);
  }
 public:
  explicit Bitmap(size_t size) {
    uint64_t num_words = (size + kBitsPerWord - 1) / kBitsPerWord;
    start_ = new uint64_t[num_words];
    end_ = start_ + num_words;
  }

  ~Bitmap() {
    delete[] start_;
  }

  void reset() {
    std::fill(start_, end_, 0);
  }

  void set_bit(size_t pos) {
    start_[word_offset(pos)] |= ((uint64_t) 1l << bit_offset(pos));
  }

  void set_bit_atomic(size_t pos) {
    uint64_t old_val, new_val;
    do {
      old_val = start_[word_offset(pos)];
      new_val = old_val | ((uint64_t) 1l << bit_offset(pos));
    } while (!compare_and_swap(start_[word_offset(pos)], old_val, new_val));
  }

  bool get_bit(size_t pos) const {
    return (start_[word_offset(pos)] >> bit_offset(pos)) & 1l;
  }

  void swap(Bitmap &other) {
    std::swap(start_, other.start_);
    std::swap(end_, other.end_);
  }

 private:
  uint64_t *start_;
  uint64_t *end_;

  static const uint64_t kBitsPerWord = 64;
  static uint64_t word_offset(size_t n) { return n / kBitsPerWord; }
  static uint64_t bit_offset(size_t n) { return n & (kBitsPerWord - 1); }
};



index_t test_bfs_lsgraph(XPGraph* xpgraph, index_t src){
    long start = src;
    long n = xpgraph->get_vcount();
    //creates Parents array, initialized to all -1, except for start
    int32_t* Parents = (int32_t *) malloc(n * sizeof(uint32_t));
    #pragma omp parallel for
    for(long i=0;i<n;i++) Parents[i] = -1;
    Parents[start] = start;
    Bitmap frontier(n); //creates initial frontier
    frontier.set_bit(start);

    const long threshold = 20;
    // const long threshold = 500;

    index_t total_count = 1;
    
    size_t level = 0;
    long frontier_count = 1;
    while(frontier_count){ //loop until frontier is empty
        auto st = std::chrono::high_resolution_clock::now();

        bool top_down;
        if (n <= frontier_count * threshold) {
            top_down = false;
        } else {
            top_down = true;
        }

        Bitmap next_frontier(n);
        size_t next_frontier_count = 0;
        #pragma omp parallel reduction(+:next_frontier_count)
        {
            vid_t vid;
            degree_t nebr_count = 0;
            degree_t local_degree = 0;
            vid_t* local_adjlist = 0;
        

            if (top_down) {
                #pragma omp for
                for (vid_t v = 0; v < n; v++) {
                    if(!frontier.get_bit(v)) continue;
                    
                    nebr_count = xpgraph->get_out_degree(v);
                    if (0 == nebr_count) continue;
                    
                    local_adjlist = new vid_t[nebr_count];
                    local_degree  = xpgraph->get_out_nebrs(v, local_adjlist);
                    // assert(local_degree  == nebr_count);
                    
                    //traverse the delta adj list
                    for (degree_t i = 0; i < local_degree; ++i) {
                        vid = local_adjlist[i];
                        if (Parents[vid] == -1) {
                            Parents[vid] = v;
                            next_frontier.set_bit(vid);
                            ++next_frontier_count;
                        }
                    }
                    delete [] local_adjlist;
                }
            } else { // bottom up
                #pragma omp for
                for (vid_t v = 0; v < n; v++) {
                    if(Parents[v] != -1) continue;

                    nebr_count = xpgraph->get_in_degree(v);
                    if (0 == nebr_count) continue;

                    local_adjlist = new vid_t[nebr_count];
                    local_degree  = xpgraph->get_in_nebrs(v, local_adjlist);
                    // assert(local_degree  == nebr_count);

                    for (degree_t i = 0; i < local_degree; ++i) {
                        vid = local_adjlist[i];
                        if(frontier.get_bit(vid)) {
                            Parents[v] = vid;
                            next_frontier.set_bit(v);
                            ++next_frontier_count;
                            break;
                        }
                    }
                    delete [] local_adjlist;
                }
            }
        }

        frontier.reset();
        frontier.swap(next_frontier);
        frontier_count = next_frontier_count;
        total_count += frontier_count;
        level++;
        auto ed = std::chrono::high_resolution_clock::now();
        double dur = std::chrono::duration<double>(ed - st).count();
        printf("Top down = %d, Level = %lu, Frontier Count = %lu, Time = %.2fs\n", top_down, level, frontier_count, dur);
    }

    free(Parents);

    return total_count;
}