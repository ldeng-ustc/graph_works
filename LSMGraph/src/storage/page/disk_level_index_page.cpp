#include "storage/page/disk_level_index_page.h"
#include <cstdint>
#include "common/config.h"
#include "common/utils/logger.h"

namespace lsmg {

uint32_t DiskLevelIndexPage::Insert(const DiskLevelIndexChunk &idx_chunk) {
  if (upper_size_ < lower_size_) {
    LOG_DEBUG("ERROR: Insert to full index page");
    return INVALID_OFFSET;
  }

  uint32_t insert_pos;
  if (idx_chunk.VID() >= mid_) {
    insert_pos = upper_size_--;
  } else {
    insert_pos = lower_size_++;
  }
  index_array_[insert_pos] = idx_chunk;
  return insert_pos;
}

VertexId_t DiskLevelIndexPage::Remove(uint32_t pos) {
  uint32_t remove_pos;
  if (index_array_[pos].VID() >= mid_) {
    if (upper_size_ == DISK_LEVEL_INDEX_PAGE_SIZE - 1) {
      return INVALID_VERTEX_ID;
    }
    remove_pos = upper_size_++;
  } else {
    if (lower_size_ == 0) {
      return INVALID_VERTEX_ID;
    }
    remove_pos = lower_size_--;
  }
  VertexId_t vid    = index_array_[pos].VID();
  index_array_[pos] = index_array_[remove_pos];

  return vid;
}

void DiskLevelIndexPage::Update(uint32_t pos, const DiskLevelIndexChunk &idx_chunk) {
  index_array_[pos] = idx_chunk;
}

}  // namespace lsmg