#ifndef LSMG_BUFFER_MANAGER_HEADER
#define LSMG_BUFFER_MANAGER_HEADER

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>
#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace lsmg {

class BufferPoolManager {
 public:
  BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K);

  ~BufferPoolManager();

  size_t GetPoolSize() {
    return pool_size_;
  }

  Page *GetPages() {
    return pages_;
  }

  Page *NewPage(page_id_t *page_id);

  BasicPageGuard NewPageGuarded(page_id_t *page_id);

  Page *FetchPage(page_id_t page_id, AccessType access_type = AccessType::Unknown);

  BasicPageGuard FetchPageBasic(page_id_t page_id);
  ReadPageGuard  FetchPageRead(page_id_t page_id);
  WritePageGuard FetchPageWrite(page_id_t page_id);

  bool UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type = AccessType::Unknown);

  bool FlushPage(page_id_t page_id);

  void FlushAllPages();

  bool DeletePage(page_id_t page_id);

 private:
  const size_t                                          pool_size_;
  std::atomic<page_id_t>                                next_page_id_ = 0;
  Page                                                 *pages_;
  std::unique_ptr<DiskScheduler>                        disk_scheduler_;
  std::unordered_map<page_id_t, frame_id_t>             page_table_;
  std::unique_ptr<LRUKReplacer>                         replacer_;
  std::list<frame_id_t>                                 free_list_;
  std::mutex                                            latch_;
  std::vector<std::pair<std::condition_variable, bool>> avaliable_;

  page_id_t AllocatePage();

  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {}

  void FlushFrame(frame_id_t frame_id);

  void ReadFrame(frame_id_t frame_id);
};
}  // namespace lsmg

#endif