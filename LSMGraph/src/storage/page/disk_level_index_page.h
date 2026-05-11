#ifndef DISK_LEVEL_INDEX_PAGE_H
#define DISK_LEVEL_INDEX_PAGE_H

#include <fmt/core.h>
#include <gflags/gflags_declare.h>
#include <cstdint>
#include <iostream>
#include "common/config.h"

namespace lsmg {

class DiskLevelIndex;

struct IndexEntry {
  uint32_t seg_id_;
  uint32_t offset_;
};

// fixed length
struct DiskLevelIndexChunk {
 public:
  DiskLevelIndexChunk() {
    level_[0] = level_[1] = level_[2] = level_[3] = 0;
  }

  DiskLevelIndexChunk(VertexId_t vid)
      : vid_(vid) {
    level_[0] = level_[1] = level_[2] = level_[3] = 0;
  }

  IndexEntry &At(uint32_t i) {
    return idx_entry_[i];
  }

  VertexId_t VID() const {
    return vid_;
  }

  uint32_t Size() const {
    for (uint32_t i = 0; i < 3; ++i) {
      if (level_[i] == 0) {
        return i + 1;
      }
    }
    return 0;  // invalid format, size is considered to be 0
  };

 private:
  uint16_t   level_[4];  // level[3] is used for alignment
  IndexEntry idx_entry_[3];
  VertexId_t vid_;
};
inline constexpr uint32_t DISK_LEVEL_INDEX_PAGE_SIZE = (PAGE_SIZE - 3 * sizeof(uint32_t)) / sizeof(DiskLevelIndexChunk);

class DiskLevelIndexPage {
  friend class DiskLevelIndex;

 public:
  void Init(uint32_t mid) {
    lower_size_ = 0;
    upper_size_ = DISK_LEVEL_INDEX_PAGE_SIZE - 1;
    mid_        = mid;
  }

  uint32_t Insert(const DiskLevelIndexChunk &idx_chunk);

  VertexId_t Remove(uint32_t pos);

  void Update(uint32_t pos, const DiskLevelIndexChunk &idx_chunk);

  const DiskLevelIndexChunk &At(uint32_t pos) const {
    return index_array_[pos];
  }
  DiskLevelIndexChunk &At(uint32_t pos) {
    return index_array_[pos];
  }

  uint32_t Size() const {
    return lower_size_ + DISK_LEVEL_INDEX_PAGE_SIZE - upper_size_ - 1;
  }

  void Print() const {
    fmt::print("mid={}\n", mid_);
    for (int i = 0; i < lower_size_; ++i) {
      fmt::print("<{}, {}> ", index_array_[i].VID(), i);
    }
    fmt::print("\n");
    for (uint i = upper_size_ + 1; i < DISK_LEVEL_INDEX_PAGE_SIZE; ++i) {
      fmt::print("<{}, {}> ", index_array_[i].VID(), i);
    }
  }

 private:
  int32_t             lower_size_;
  int32_t             upper_size_;
  uint32_t            mid_;
  DiskLevelIndexChunk index_array_[DISK_LEVEL_INDEX_PAGE_SIZE];
};

}  // namespace lsmg
#endif