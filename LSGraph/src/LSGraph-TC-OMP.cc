#define ENABLE_LOCK 1
#define WEIGHTED 0
#define VERIFY 0

#include <cstdio>
#include <cstdlib>
#include <cassert>

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <array>
#include <random>
#include <algorithm>
#include <queue>
#include <chrono>

#include "LSGraph.h"
#include "parallel.h"
#include "util.h"
#include "parse_command_line.h"
#include "rmat_util.h"
#include "bench_utils.h"

#include "BFS.h"
#include "PagerankPush.h"
#include "PagerankPull.h"
#include "CC.h"
#include "BC.h"
#include "TC.h"
#include "io_util.h"

// #include "sim_api.h"

using namespace graphstore;

#define LOGGING_TICK (1ULL << 24)
#define BATCH_SIZE (1ULL << 9)

#include	<stdlib.h>

template <class G>
size_t run_tc(G& GA) {
  struct timeval start, prepare;
  struct timezone tzp;
  gettimeofday(&start, &tzp);
  std::vector<std::vector<uint32_t>>mp(GA.get_num_vertices());
  parallel_for(int i = 0; i < GA.get_num_vertices(); ++i){
    std::vector<uint32_t>nei;
    // GA.print(i, nei);
    GA.print_l(i, nei, i);
    mp[i] = nei;
  }
  gettimeofday(&prepare, &tzp);
  printf("Prepare: %.2f\n", cal_time_elapsed(&start, &prepare));

  auto count = TC_gabps(GA, mp);
  return count;
}

template <class G>
double test_tc(G& GA, commandLine& P) {
	struct timeval start, end;
	struct timezone tzp;

  std::cout << "Running TC" << std::endl;

  // with edge map
  gettimeofday(&start, &tzp);
  auto count = run_tc(GA);
  gettimeofday(&end, &tzp);
  printf("TC finished, counted %ld\n", count);
  return cal_time_elapsed(&start, &end);
}

#define EXPOUT "[EXPOUT]"

void run_algorithm(commandLine& P) {
	uint32_t num_nodes;
  uint64_t num_edges;
  auto filename = P.getOptionValue("-f", "none");

  auto ts_begin = std::chrono::high_resolution_clock::now();

  // Load file
  // pair_uint *edges = get_edges_from_binary64_file(filename.c_str(), false, &num_edges, &num_nodes);
  pair_uint *edges = get_edges_from_binary32_file(filename.c_str(), true, &num_edges, &num_nodes);
  auto ts_load = std::chrono::high_resolution_clock::now();

  // Create updates
  size_t batch_size = P.getOptionLongValue("-bs", 5);
  size_t batchs = (num_edges + batch_size - 1) / batch_size;
  size_t info_batch = std::max((size_t)1, 20000000u / batch_size);

  using Sequence = parlay::sequence<std::tuple<uint32_t, uint32_t>>;
  std::vector<Sequence> batch_data;
  batch_data.reserve(batchs);

  for(size_t i=0; i<batchs; i++) {
    size_t sz = std::min(batch_size, num_edges - i*batch_size);
    Sequence batch(sz);
    for(size_t j=0; j<sz; j++) {
      batch[j] = std::make_tuple(edges[i*batch_size+j].x, edges[i*batch_size+j].y);
    }
    batch_data.emplace_back(std::move(batch));

    if(i % info_batch == 0) {
      printf("Transform batch to Sequence %ld/%ld\n", i+1, batchs);
    }
  }
  free(edges);
  auto ts_transform = std::chrono::high_resolution_clock::now();

  // Batch ingest
  LSGraph graph(num_nodes);

  for(size_t i=0; i<batchs; i++) {
    auto& batch = batch_data[i];

    graph.add_edge_batch_sort(batch, batch.size(), num_nodes);

    if(i % info_batch == 0) {
      auto ts = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration<double>(ts - ts_transform).count();
      auto bw = (i+1) * batch_size / t / 1e6;
      printf("Batch %ld/%ld, time: %.3fs, bandwith: %.2fM Edges/s\n", i+1, batchs, t, bw);
    }
  }
  auto ts_ingest = std::chrono::high_resolution_clock::now();

  // Run test
  double t_tc = test_tc(graph, P);

  double t_load = std::chrono::duration<double>(ts_load - ts_begin).count();
  double t_transfrom = std::chrono::duration<double>(ts_transform - ts_load).count();
  double t_ingest = std::chrono::duration<double>(ts_ingest - ts_transform).count();

  printf(EXPOUT "Datset: %s\n", filename.c_str());
  printf(EXPOUT "Vertex count: %d\n", num_nodes);
  printf(EXPOUT "Load: %.4f\n", t_load);
  printf("\tTransform time: %.4f\n", t_transfrom);
  printf(EXPOUT "Ingest: %.4f\n", t_ingest);

  printf(EXPOUT "TC: %.4f\n", t_tc);

  double bw_ingest = num_edges / t_ingest / 1e6;
  printf("Ingest bandwidth: %.4fM Edges/s\n", bw_ingest);
}

/* 
 * ===  FUNCTION  =============================================================
 *         Name:  main
 *  Description:  
 * ============================================================================
 */
int main(int argc, char** argv) {
  srand(time(NULL));
  printf("Num workers: %ld\n", getWorkers());
  commandLine P(argc, argv, "./graph_bm [-t testname -r rounds -f file");
  run_algorithm(P);
}