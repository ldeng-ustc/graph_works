#ifndef LSMG_CHAR_ARENA_HEADER
#define LSMG_CHAR_ARENA_HEADER

#include <cassert>
#include <cstddef>
#include <cstdlib>

#include "common/utils/logger.h"

namespace lsmg {

template <typename T>
class CharArena {
 public:
  CharArena()
      : capacity_(0)
      , data_(nullptr) {}

  CharArena(int capacity)
      : capacity_(capacity)
      , used_size_(0) {
    data_ = (T *)malloc(capacity_);
  }

  void Init(const size_t capacity) {
    capacity_  = capacity;
    used_size_ = 0;
    data_      = (T *)malloc(capacity_);
    if (!data_) {
      LOG_ERROR("Memory allocation failed.");
    }
  }

  // support concurrency
  T *Alloc(const size_t need_size) {
    size_t pointer = 0;
    if (used_size_ + need_size >= capacity_) {
      LOG_ERROR(" no a free node!\n");
      exit(0);
    }
    pointer = __sync_fetch_and_add(&used_size_, need_size);
    assert(pointer < capacity_);
    return data_ + pointer;
  }

  void Resize(size_t size) {
    // pass
  }

  void Reset() {
    used_size_ = 0;
  }

  size_t FreeSize() const {
    assert(used_size_ <= capacity_);
    return capacity_ - used_size_;
  }

  size_t GetUsedSize() const {
    return used_size_;
  }

  ~CharArena() {
    free(data_);
  }

 private:
  size_t capacity_;
  size_t used_size_;
  T     *data_;
};

}  // namespace lsmg

#endif  // CharArena_H