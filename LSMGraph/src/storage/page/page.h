#ifndef PAGE_H
#define PAGE_H

#include <cstring>

#include "common/config.h"
#include "common/utils/rwlatch.h"

namespace lsmg {

class Page {
  friend class BufferPoolManager;

 public:
  Page() {
    data_ = new char[PAGE_SIZE];
    ResetMemory();
  }

  ~Page() {
    delete[] data_;
  }

  inline char *GetData() {
    return data_;
  }

  inline page_id_t GetPageId() {
    return page_id_;
  }

  inline int GetPinCount() {
    return pin_count_;
  }

  inline bool IsDirty() {
    return is_dirty_;
  }

  inline void WLatch() {
    rwlatch_.WLock();
  }

  inline void WUnlatch() {
    rwlatch_.WUnlock();
  }

  inline void RLatch() {
    rwlatch_.RLock();
  }

  inline void RUnlatch() {
    rwlatch_.RUnlock();
  }

  inline lsn_t GetLSN() {
    return *reinterpret_cast<lsn_t *>(GetData() + OFFSET_LSN);
  }

  inline void SetLSN(lsn_t lsn) {
    memcpy(GetData() + OFFSET_LSN, &lsn, sizeof(lsn_t));
  }

 protected:
  static_assert(sizeof(page_id_t) == 4);
  static_assert(sizeof(lsn_t) == 4);

  static constexpr size_t SIZE_PAGE_HEADER  = 8;
  static constexpr size_t OFFSET_PAGE_START = 0;
  static constexpr size_t OFFSET_LSN        = 4;

 private:
  inline void ResetMemory() {
    memset(data_, OFFSET_PAGE_START, PAGE_SIZE);
  }

  char             *data_;
  page_id_t         page_id_   = INVALID_PAGE_ID;
  int               pin_count_ = 0;
  bool              is_dirty_  = false;
  ReaderWriterLatch rwlatch_;
};

}  // namespace lsmg

#endif