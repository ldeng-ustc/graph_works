#ifndef RW_LATCH_H
#define RW_LATCH_H

#include <shared_mutex>

namespace lsmg {

class ReaderWriterLatch {
 public:
  void WLock() {
    mutex_.lock();
  }

  void WUnlock() {
    mutex_.unlock();
  }

  void RLock() {
    mutex_.lock_shared();
  }

  void RUnlock() {
    mutex_.unlock_shared();
  }

 private:
  std::shared_mutex mutex_;
};

}  // namespace lsmg

#endif