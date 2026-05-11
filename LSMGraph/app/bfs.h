#ifndef LSMG_BFS_HEADER
#define LSMG_BFS_HEADER

#include <assert.h>
#include <omp.h>
#include <algorithm>
#include <cstdint>
#include "algo_factory.h"
#include "common/config.h"
#include "common/flags.h"
#include "common/utils/atomic.h"
#include "common/utils/bitmap.hpp"
#include "lsmgraph.h"

namespace lsmg {
inline int run_bfs(LSMGraph &gStore, const SequenceNumber_t seq = MAX_SEQ_ID) {
  using value_t = int64_t;

  size_t     graph_num     = gStore.get_max_vertex_num();
  value_t    DEFAULT_VALUE = std::numeric_limits<value_t>::max();
  VertexId_t root          = FLAGS_source;

  LOG_INFO("source={}", root);

  std::vector<value_t> values(graph_num, DEFAULT_VALUE);
  size_t               basic_chunk = 64;
  Bitmap              *visited     = new Bitmap(graph_num);
  Bitmap              *active_out  = new Bitmap(graph_num);
  Bitmap              *active_in   = new Bitmap(graph_num);
  visited->clear();
  visited->set_bit(root);
  active_in->clear();
  active_in->set_bit(root);
  int32_t activated = 1;
  values[root]      = 0;

  std::function<int32_t(VertexId_t)> F = [&](VertexId_t src) {
    int32_t activated = 0;
    auto    it_A      = gStore.get_edges(src, seq);

    for (; it_A.valid(); it_A.next()) {
      VertexId_t dst = it_A.dst_id();
      if (!visited->get_bit(dst)) {
        cas(&values[dst], DEFAULT_VALUE, values[src] + 1);
        visited->set_bit(dst);
        active_out->set_bit(dst);
        activated++;
      }
    }
    return activated;
  };

  uint step = 0;
  while (activated > 0) {
    LOG_INFO("step={}, activated={}", ++step, activated);
    activated = 0;
    active_out->clear();
    assert(step <= graph_num);
#pragma omp parallel for num_threads(FLAGS_thread_num) schedule(dynamic, 64) reduction(+ : activated)
    for (VertexId_t begin_v_i = 0; begin_v_i < graph_num; begin_v_i += basic_chunk) {
      VertexId_t    v_i  = begin_v_i;
      unsigned long word = active_in->data[WORD_OFFSET(v_i)];
      while (word != 0) {
        if (word & 1) {
          activated += F(v_i);
        }
        v_i++;
        word = word >> 1;
      }
    }
    std::swap(active_in, active_out);
  }

  value_t sum = 0;
#pragma omp parallel for num_threads(FLAGS_thread_num) reduction(+ : sum)
  for (uint64_t i = 0; i < graph_num; i++) {
    if (values[i] != DEFAULT_VALUE) {
      sum += values[i];
    }
  }
  LOG_INFO("sum={}", sum);

  delete visited;
  delete active_in;
  delete active_out;
  return 0;
}
REGISTER_ALGORITHM(bfs, run_bfs)
}  // namespace lsmg

#endif