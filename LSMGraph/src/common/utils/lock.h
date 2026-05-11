#ifndef LOCK_H
#define LOCK_H
#include <assert.h>
#include <atomic>
#include <climits>
#include <cstdlib>
#include <shared_mutex>

class Spinlock {
 private:
  std::atomic_flag flag_ = ATOMIC_FLAG_INIT;

 public:
  void lock() {
    while (flag_.test_and_set(std::memory_order_acquire)) {
    }
  }

  void unlock() {
    flag_.clear(std::memory_order_release);
  }
};

class SharedRWClock {
 public:
  void ReadLock() {
    rwLock_.lock_shared();
  }

  void WriteLock() {
    rwLock_.lock();
  }

  void ReadUnlock() {
    rwLock_.unlock_shared();
  }

  void WriteUnlock() {
    rwLock_.unlock();
  }

 private:
  std::shared_mutex rwLock_;
};

class CASRWLock {
 public:
  CASRWLock()
      : lock_(0) {}

  void ReadLock() {
    uint64_t i, n;
    uint64_t old_readers;
    for (;;) {
      old_readers = lock_;
      if (old_readers != WLOCK && __sync_bool_compare_and_swap(&lock_, old_readers, old_readers + 1)) {
        return;
      }
      for (n = 1; n < SPIN; n <<= 1) {
        for (i = 0; i < n; i++) {
          __asm__("pause");
        }
        old_readers = lock_;
        if (old_readers != WLOCK && __sync_bool_compare_and_swap(&lock_, old_readers, old_readers + 1)) {
          return;
        }
      }
      sched_yield();
    }
  }

  void WriteLock() {
    uint64_t i, n;
    for (;;) {
      if (lock_ == 0 && __sync_bool_compare_and_swap(&lock_, 0, WLOCK)) {
        return;
      }
      for (n = 1; n < SPIN; n <<= 1) {
        for (i = 0; i < n; i++) {
          __asm__("pause");
        }
        if (lock_ == 0 && __sync_bool_compare_and_swap(&lock_, 0, WLOCK)) {
          return;
        }
      }
      sched_yield();
    }
  }

  void ReadUnlock() {
    uint64_t old_readers;
    old_readers = lock_;
    if (old_readers == WLOCK) {
      lock_ = 0;
      return;
    }
    for (;;) {
      if (__sync_bool_compare_and_swap(&lock_, old_readers, old_readers - 1)) {
        return;
      }
      old_readers = lock_;
    }
  }

  void WriteUnlock() {
    uint64_t old_readers;
    old_readers = lock_;
    if (old_readers == WLOCK) {
      lock_ = 0;
      return;
    }
    for (;;) {
      if (__sync_bool_compare_and_swap(&lock_, old_readers, old_readers - 1)) {
        return;
      }
      old_readers = lock_;
    }
  }

 private:
  static const uint64_t SPIN  = 2048;
  static const uint64_t WLOCK = ((unsigned long)-1);

  uint64_t lock_;
};

#endif