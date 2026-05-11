#ifndef LSMG_SCAN_HEADER
#define LSMG_SCAN_HEADER

#include <assert.h>
#include <omp.h>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include "algo_factory.h"
#include "common/config.h"
#include "lsmgraph.h"

namespace lsmg {
inline int run_scan(LSMGraph &gStore, const SequenceNumber_t seq = MAX_SEQ_ID) {
  using value_t      = int64_t;
  uint64_t graph_num = gStore.get_max_vertex_num();

  LOG_INFO("graph_num={}", graph_num);
  LOG_INFO("seq={}", seq);

  std::function<void(uint64_t)> F;

  size_t find_edge_num = 0;

  F = [&](uint64_t src) {
    auto it_A = gStore.get_edges(src, seq);
    for (; it_A.valid(); it_A.next()) {
      // find_edge_num++;
    }
  };

  #pragma omp parallel for num_threads(FLAGS_thread_num) schedule(dynamic, 64)
  for (uint64_t v_i = 0; v_i < graph_num; v_i++) {
    F(v_i);
  }

  // std::cout << "find_edge_num=" << find_edge_num << std::endl;

  return 0;
}
REGISTER_ALGORITHM(scan, run_scan)

}  // namespace lsmg
#endif