// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
#define STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_

#include "common/utils/leveldb/port/port.h"
#include "common/utils/leveldb/port/port_stdcxx.h"
#include "common/utils/leveldb/port/thread_annotations.h"

namespace leveldb {

class SCOPED_LOCKABLE MutexLock {
 public:
  explicit MutexLock(port::Mutex *mu) EXCLUSIVE_LOCK_FUNCTION(mu)
      : mu_(mu) {
    this->mu_->Lock();
  }
  ~MutexLock() UNLOCK_FUNCTION() {
    this->mu_->Unlock();
  }

  MutexLock(const MutexLock &)            = delete;
  MutexLock &operator=(const MutexLock &) = delete;

 private:
  port::Mutex *const mu_;
};

}  // namespace leveldb
#endif  // STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
