#ifndef LSMG_ARENA_HEADER
#define LSMG_ARENA_HEADER

#include <omp.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include "../flags.h"

using order_t = uint8_t;

namespace lsmg {

template <typename T>
class Arena {
 public:
  Arena() = delete;

  Arena(size_t node_num)
      : nodelist_(node_num)
      , node_num_{node_num} {}

  // support concurrency
  T *GetAnElement() {
    T *pointer = NULL;
    if (free_prt_ >= node_num_) {
      exit(0);
    }
    size_t temp_ptr = __sync_fetch_and_add(&free_prt_, 1);
    pointer         = &nodelist_[temp_ptr];
    assert(pointer != NULL);
    return pointer;
  }
  // support concurrency
  T *GetAnElementById(size_t id) {
    T *pointer = NULL;
    if (id >= node_num_) {
      exit(0);
    }
    assert(id < node_num_);
    pointer = &nodelist_[id];
    assert(pointer != NULL);
    return pointer;
  }

  void Reset() {
    free_prt_ = 0;
// Can be executed in parallel
#pragma omp parallel for num_threads(FLAGS_thread_num)
    for (uint i = 0; i < node_num_; i++) {
      nodelist_[i].reset();
    }
  }

  size_t FreeSize() {
    return node_num_ - free_prt_;
  }

 private:
  std::vector<T> nodelist_;
  size_t         node_num_;
  size_t         free_prt_;
};

}  // namespace lsmg

#endif  // ARENA_H