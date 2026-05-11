#ifndef LSMG_DISK_LEVEL_INDEX
#define LSMG_DISK_LEVEL_INDEX

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <list>
#include <mutex>
#include <string>
#include <utility>

#include "cache/block_manager.h"
#include "common/config.h"

namespace lsmg {

template <typename IndexEntry, size_t LENGTH>
class DiskIndexManager {
 public:
  DiskIndexManager(const std::string &path, size_t index_num)
      : free_list_()
      , file_path_(path)
      , block_manager_(path, LENGTH * sizeof(IndexEntry) * index_num)
      , page_num_(0) {
    if (free_list_.empty()) {
      block_manager_.alloc(PAGE_SIZE);
      for (size_t i = 0; i < page_capacity_; ++i) {
        free_list_.push_back(i + page_capacity_ * page_num_);
      }
      ++page_num_;
    }
  }

  ~DiskIndexManager() {}

  void Delete(size_t idx) {
    std::lock_guard lg(mtx_);
    free_list_.push_back(idx);
  }

  template <typename... Args>
  size_t InsertList(Args &&...indexes) {
    static_assert(sizeof...(Args) == LENGTH, "Too many arguments");
    static_assert((std::is_convertible_v<Args, IndexEntry> && ...), "All arguments must be convertible to IndexEntry");

    size_t idx = TryAllocSlot();
    PutIndexListToPage(idx, std::forward<Args>(indexes)...);
    return idx;
  }

  void Update(size_t idx, size_t offset, IndexEntry index) {
    PutIndexToPage(idx, offset, index);
  }

  size_t AllocIndexList() {
    return TryAllocSlot();
  }

  IndexEntry *Get(size_t idx) {
    return GetIndexBegin(idx);
  }

 private:
  size_t TryAllocSlot() {
    std::lock_guard lg(mtx_);
    if (free_list_.empty()) {
      block_manager_.alloc(PAGE_SIZE);
      for (size_t i = 0; i < page_capacity_; ++i) {
        free_list_.push_back(i + page_capacity_ * page_num_);
      }
      ++page_num_;
    }

    size_t idx = free_list_.front();
    free_list_.pop_front();
    return idx;
  }

  size_t Idx2PageOffset(size_t idx) const {
    return (idx % page_capacity_) * sizeof(IndexEntry) * LENGTH + PAGE_SIZE * (idx / page_capacity_);
  }

  template <typename... Args>
  void PutIndexListToPage(size_t idx, Args &&...args) {
    IndexEntry *idx_ptr = GetIndexBegin(idx);

    size_t index = 0;
    ((idx_ptr[index++] = std::forward<Args>(args)), ...);
  }

  void PutIndexToPage(size_t idx, size_t offset, IndexEntry index) {
    IndexEntry *idx_ptr = GetIndexBegin(idx) + offset;

    *idx_ptr = index;
  }

  IndexEntry *GetIndexBegin(size_t idx) {
    size_t      byte_offset    = Idx2PageOffset(idx);
    char       *begin_byte_ptr = reinterpret_cast<char *>(block_manager_.get_data()) + byte_offset;
    IndexEntry *idx_ptr        = reinterpret_cast<IndexEntry *>(begin_byte_ptr);
    return idx_ptr;
  }

  const size_t page_capacity_ = PAGE_SIZE / (LENGTH * sizeof(IndexEntry));

  std::list<uint32_t> free_list_;
  std::string         file_path_;
  BlockManager        block_manager_;
  uint32_t            page_num_;
  uint32_t            entry_num_;
  std::mutex          mtx_;
};
}  // namespace lsmg
#endif