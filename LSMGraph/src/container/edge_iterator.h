#ifndef LSMG_EDGE_ITERATOR_HEADER
#define LSMG_EDGE_ITERATOR_HEADER

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <queue>
#include <string>
#include <unordered_set>
#include <variant>

#include "cache/block_manager.h"
#include "cache/mem_table.h"
#include "cache/sst_data_cache.h"
#include "cache/sst_data_manager.h"
#include "cache/sst_table_cache.h"
#include "common/config.h"
#include "common/flags.h"
#include "common/utils.h"
#include "common/utils/livegraph/futex.hpp"
#include "common/utils/rocksdb/heap.h"
#include "container/edge_iterator_base.h"
#include "container/sst_edge_iterator.h"
#include "graph/edge.h"
#include "index/del_record_manager.h"
#include "version/super_version.h"

namespace lsmg {

class EdgeIteratorNoDelete;

using EdgeIterator = EdgeIteratorNoDelete;

class EdgeIteratorNoDelete {
 public:
  EdgeIteratorNoDelete(const VertexId_t src, MemTable *newmemTable,
                       std::vector<std::vector<SSTableCache *> *> &fileMetaCache, SSTDataManager &sstdata_manager,
                       LevelIndex *vid_to_levelIndex, SuperVersion *sv, DelRecordManage *del_record_manager,
                       int max_level, SequenceNumber_t seq = MAX_SEQ_ID)
      : sv_(sv)
      , seq_(seq)
      , del_record_manager_(del_record_manager) {
    // std::cout << "max_level=" << max_level << std::endl;
    if (max_level < 0) {
      return;
    }

    if (max_level >= 1) {
      FileId_t min_level_0_fid = sv_->findex.get_min_level_0_fid();
      for (auto sst_it : *(sv_->get_version()->GetLevel0Files())) {
        if (seq_ <= sst_it->seq_) {
          continue;
        }
        if (sst_it->header.timeStamp >= min_level_0_fid && src <= sst_it->header.maxKey
            && src >= sst_it->header.minKey) {
          find_iterator_sst(sst_it, src, sstdata_manager);
        }
      }
    }
    if (max_level >= 2) {
      find_iterator_sst_by_levelindex(src, sstdata_manager, vid_to_levelIndex, max_level);
    }
    for (auto tb : sv_->get_memtable()) {
      if (seq_ <= tb->GetStartTime()) {
        continue;
      }
      it = std::shared_ptr<EdgeIteratorBase>(new NeighBors::MemEdgeIterator(tb->get_vertex_adj(src), tb->GetFid()));
      if (it->valid()) {
        it_array.insert(it_array.begin(), it);
      }
    }
    if (!it_array.empty()) {
      findFirstValid();
    } else {
      it = nullptr;
    }
  }

  void find_iterator_sst_by_levelindex(const VertexId_t src, SSTDataManager &sstdata_manager,
                                       LevelIndex *vid_to_levelIndex, int max_level) {
    uint32_t fileID      = 0;
    uint32_t offset      = 0;
    uint32_t next_offset = 0;

    MulLevelIndex &findex = sv_->findex;

    for (int levelID = 0; levelID < max_level - 1; levelID++) {
      if (FLAGS_support_mulversion == false) {
        int         index_id = src * LEVEL_INDEX_SIZE + levelID;
        LevelIndex &findex   = vid_to_levelIndex[index_id];
        fileID               = findex.get_fileID();

        if (fileID == INVALID_File_ID) {
          continue;
        }

        offset      = findex.get_offset();
        next_offset = findex.get_next_offset();
        assert(next_offset >= offset);
      } else {
        fileID = findex.get_fileID(levelID);

        if (fileID == INVALID_File_ID) {
          continue;
        }

        offset      = findex.get_offset(levelID);
        next_offset = findex.get_next_offset(levelID);
        assert(next_offset >= offset);
      }

      uint32_t adj_size = (next_offset - offset) / sizeof(EdgeBody_t);

      // std::cout << " level=" << levelID
      //           << " offset=" << offset
      //           << " next_offset=" << next_offset
      //           << " adj_size=" << adj_size
      //           << std::endl;

      if (FLAGS_OPEN_SSTDATA_CACHE == true) {
        SSTDataCache *sstcache = sstdata_manager.get_data(fileID);
        assert(sstcache != nullptr);
        std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(new SSTEdgeIterator(
            (EdgeBody_t *)(sstcache->GetEdgeData() + offset), sstcache->GetPropertyData(), adj_size, fileID));
        if (it_temp->valid()) {
          it_array.emplace_back(it_temp);
        }
      } else {
        LOG_INFO(" ToDo get_edges...");
      }
    }
  }

  Status find_iterator_sst(SSTableCache *sst_it, VertexId_t src, SSTDataManager &sstdata_manager) {
    int pos = sst_it->get(src);

    if (pos < 0) {
      return Status::kNotFound;
    }

    uint32_t offset      = (sst_it->indexes)[pos].offset;
    uint32_t next_offset = (sst_it->indexes)[pos + 1].offset;

    if (FLAGS_OPEN_SSTDATA_CACHE == true) {
      SSTDataCache *sstcache = sstdata_manager.get_data(sst_it->header.timeStamp);
      assert(sstcache != nullptr);
      uint32_t adj_size = (next_offset - offset) / sizeof(EdgeBody_t);
      assert(next_offset >= offset);

      // std::cout << " +++++++++++ offset=" << offset
      //           << " next_offset=" << next_offset
      //           << " pos=" << pos
      //           << " adj_size=" << adj_size
      //           << std::endl;

      std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(
          new SSTEdgeIterator((EdgeBody_t *)(sstcache->GetEdgeData() + offset), sstcache->GetPropertyData(), adj_size,
                              sst_it->header.timeStamp));
      if (it_temp->valid()) {
        it_array.emplace_back(it_temp);
      }
    } else {
      LOG_ERROR("NOT IMPLEMENTED");
      std::terminate();
    }
    return Status::kOk;
  }

  void init() {}

  bool valid() {
    return !(it == nullptr) && it->valid();
  }

  bool check_entry_valid() {
    assert(it != nullptr && it->valid());
    uint64_t temp_seq = it->sequence();
    if (temp_seq > seq_) {
      return false;
    }
#ifdef DEL_EDGE_SEPARATE
    if (!curr_have_map_) {
      if (it->marker() == true && it->sequence() < seq_) {
        it->next();
        if (!it->IsMemTable()) {
          assert(it != nullptr);
          it->next();
        }
        return it->valid() && check_entry_valid();
      }
    } else {
      SequenceNumber_t eid = it->sequence();
      SequenceNumber_t del_time;
      if (del_record_manager_->get_time(curr_deleted_edge_map_, eid, del_time) && del_time < seq_) {
        it->next();
        if (!it->IsMemTable()) {
          assert(it != nullptr);
          it->next();
        }
        return it->valid() && check_entry_valid();
      } else {
        return true;
      }
    }
#endif
    return true;
  }

  void findFirstValid() {
    if (it_array.size() == 0) {
      it = nullptr;
      return;
    } else {
      it = nullptr;
      do {
        it = it_array.back();
        it_array.pop_back();
#ifdef DEL_EDGE_SEPARATE
        curr_have_map_ = del_record_manager_->find_eidmap(it->get_fid(), curr_deleted_edge_map_);
#endif
        while (valid() && !check_entry_valid()) {
          if (!valid()) {
            break;
          }
          it->next();
        }
      } while ((!valid() && !it_array.empty()));
      return;
    }
  }

  void next() {
    while (true) {
      if (!valid()) {
        return;
      }
      it->next();

      if (!valid()) {
        findFirstValid();
      }

      if (!valid() || check_entry_valid()) {
        break;
      }
    }
  }

  VertexId_t dst_id() {
    return it->dst_id();
  }

  SequenceNumber_t sequence() {
    return it->sequence();
  }

  Marker_t marker() {
    return it->marker();
  }

  EdgeProperty_t edge_data() {
    return it->edge_data();
  }

  bool empty() {
    return it->empty();
  }

  size_t size() {
    return it->size();
  }

  bool compare(const std::shared_ptr<EdgeIteratorBase> r1, const std::shared_ptr<EdgeIteratorBase> r2) {
    if (r1->dst_id() == r2->dst_id()) {
      return r1->sequence() > r2->sequence();
    }
    return r1->dst_id() < r2->dst_id();
  }

  // clear all points
  ~EdgeIteratorNoDelete() {
    it_array.clear();
  }

 private:
  std::shared_ptr<EdgeIteratorBase>              it = nullptr;
  std::vector<std::shared_ptr<EdgeIteratorBase>> it_array;
  SuperVersion                                  *sv_;
  SequenceNumber_t                               seq_;
  EidToTimeMap                                  *curr_deleted_edge_map_;
  DelRecordManage                               *del_record_manager_;
  bool                                           curr_have_map_;
};

class EdgeIteratorTraverse {
 public:
  EdgeIteratorTraverse(const VertexId_t src, MemTable *newmemTable,
                       std::vector<std::vector<SSTableCache *> *> &fileMetaCache, SSTDataManager &sstdata_manager,
                       LevelIndex *vid_to_levelIndex, SuperVersion *sv, DelRecordManage *del_record_manager,
                       int max_level, SequenceNumber_t seq = MAX_SEQ_ID)
      : seq_(seq)
      , sv_(sv) {
    if (FLAGS_support_mulversion == true) {
      if (max_level < 0) {
        return;
      }

      for (auto tb : sv_->get_memtable()) {
        NeighBors *neighbors = tb->get_vertex_adj(src);
        if (neighbors != nullptr) {
          it = std::shared_ptr<EdgeIteratorBase>(new NeighBors::MemEdgeIterator(neighbors, tb->GetFid()));
          it_array.emplace_back(it);
        }
      }
      if (max_level >= 1) {
        FileId_t min_level_0_fid = sv_->findex.get_min_level_0_fid();
        for (auto sst_it : *(sv_->get_version()->GetLevel0Files())) {
          if (sst_it->header.timeStamp >= min_level_0_fid && src <= sst_it->header.maxKey
              && src >= sst_it->header.minKey) {
            find_iterator_sst(sst_it, src, sstdata_manager, max_level);
          }
        }
      }
    } else {
      // memtable
      NeighBors *neighbors = newmemTable->get_vertex_adj(src);
      if (neighbors != nullptr) {
        it = std::shared_ptr<EdgeIteratorBase>(new NeighBors::MemEdgeIterator(neighbors, newmemTable->GetFid()));
        if (it->valid()) {
          it_array.emplace_back(it);
        }
      }
      // sstable
      for (auto sst_it : *fileMetaCache[0]) {
        if (src <= sst_it->header.maxKey && src >= sst_it->header.minKey) {
          find_iterator_sst(sst_it, src, sstdata_manager, max_level);
        }
      }
    }

    // level>0, from levelindex
    if (max_level >= 2) {
      find_iterator_sst_by_levelindex(src, sstdata_manager, vid_to_levelIndex, max_level);
    }
    ptr_num_ = it_array.size();
    if (ptr_num_ > 0) {
      find_smallest();
      if (it != nullptr) {
        findFirstValid();
      }
    } else {
      it = nullptr;
    }
  }

  void find_iterator_sst_by_levelindex(const VertexId_t src, SSTDataManager &sstdata_manager,
                                       LevelIndex *vid_to_levelIndex, int max_level) {
    uint32_t fileID      = 0;
    uint32_t offset      = 0;
    uint32_t next_offset = 0;

    MulLevelIndex &findex = sv_->findex;

    for (int levelID = 0; levelID < max_level - 1; levelID++) {
      if (FLAGS_support_mulversion == false) {
        int         index_id = src * LEVEL_INDEX_SIZE + levelID;
        LevelIndex &findex   = vid_to_levelIndex[index_id];
        fileID               = findex.get_fileID();

        if (fileID == INVALID_File_ID) {
          continue;
        }

        offset      = findex.get_offset();
        next_offset = findex.get_next_offset();
        assert(next_offset >= offset);
      } else {
        fileID = findex.get_fileID(levelID);

        if (fileID == INVALID_File_ID) {
          continue;
        }

        offset      = findex.get_offset(levelID);
        next_offset = findex.get_next_offset(levelID);
        assert(next_offset >= offset);
      }

      uint32_t adj_size = (next_offset - offset) / sizeof(EdgeBody_t);
      assert(next_offset > offset);
      assert(adj_size > 0);
      if (FLAGS_OPEN_SSTDATA_CACHE == true) {
        SSTDataCache *sstcache = sstdata_manager.get_data(fileID);
        assert(sstcache != nullptr);
        std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(new SSTEdgeIterator(
            (EdgeBody_t *)(sstcache->GetEdgeData() + offset), sstcache->GetPropertyData(), adj_size, fileID));
        it_array.emplace_back(it_temp);
      } else {
        LOG_INFO(" ToDo get_edges...");
      }
    }
  }

  Status find_iterator_sst(SSTableCache *sst_it, VertexId_t src, SSTDataManager &sstdata_manager, int max_level) {
    int pos = sst_it->get(src);

    if (pos < 0) {
      return Status::kNotFound;
    }

    uint32_t offset      = (sst_it->indexes)[pos].offset;
    uint32_t next_offset = (sst_it->indexes)[pos + 1].offset;

    if (FLAGS_OPEN_SSTDATA_CACHE == true) {
      SSTDataCache *sstcache = sstdata_manager.get_data(sst_it->header.timeStamp);
      assert(sstcache != nullptr);
      uint32_t adj_size = (next_offset - offset) / sizeof(EdgeBody_t);
      assert(next_offset > offset);
      assert(adj_size > 0);

      std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(
          new SSTEdgeIterator((EdgeBody_t *)(sstcache->GetEdgeData() + offset), sstcache->GetPropertyData(), adj_size,
                              sst_it->header.timeStamp));
      it_array.emplace_back(it_temp);
    } else {
      LOG_INFO(" ToDo get_edges...");
    }
    return Status::kOk;
  }

  void init() {}

  bool valid() {
    return !(it == nullptr) && it->valid();
  }

  void findFirstValid() {
    if (it->sequence() < seq_) {
      if (it->marker() == true) {  // deleted
        saved_key_ = it->dst_id();
        skipping_  = true;
      } else {
        skipping_ = false;
        return;
      }
    }
    next();
  }

  void find_smallest() {
    if (ptr_num_ == 0) {
      it = nullptr;
      return;
    } else if (ptr_num_ == 1) {
      it = nullptr;
      if (it_array[0]->valid()) {
        it = it_array[0];
      }
      return;
    }
    it = nullptr;
    for (int i = 0; i < ptr_num_; i++) {
      std::shared_ptr<EdgeIteratorBase> child = it_array[i];
      if (child->valid()) {
        if (it == nullptr) {
          it = child;
        } else if (compare(child, it)) {
          it = child;
        }
      }
    }
  }

  void next() {
    if (!valid()) {
      return;
    }

    while (true) {
      it->next();
      find_smallest();

      if (it == nullptr) {
        return;
      }

      // check valid
      if (it->sequence() < seq_) {
        if (skipping_ == true && it->dst_id() == saved_key_) {
        } else {
          if (it->marker() == true) {  // deleted
            saved_key_ = it->dst_id();
            skipping_  = true;
          } else {
            skipping_ = false;
            break;
          }
        }
      } else {
        if (it->dst_id() == saved_key_) {
        } else {
          saved_key_ = it->dst_id();
        }
      }
    }
  }

  VertexId_t dst_id() {
    return it->dst_id();
  }

  SequenceNumber_t sequence() {
    return it->sequence();
  }

  Marker_t marker() {
    return it->marker();
  }

  EdgeProperty_t edge_data() {
    return it->edge_data();
  }

  bool empty() {
    return it->empty();
  }

  size_t size() {
    return it->size();
  }

  bool compare(const std::shared_ptr<EdgeIteratorBase> r1, const std::shared_ptr<EdgeIteratorBase> r2) {
    if (r1->dst_id() == r2->dst_id()) {
      return r1->sequence() > r2->sequence();
    }
    return r1->dst_id() < r2->dst_id();
  }

  // clear all points
  ~EdgeIteratorTraverse() {
    it_array.clear();
  }

 private:
  std::shared_ptr<EdgeIteratorBase>              it = nullptr;
  std::vector<std::shared_ptr<EdgeIteratorBase>> it_array;
  VertexId_t                                     saved_key_ = INVALID_VERTEX_ID;
  bool                                           skipping_  = false;
  SequenceNumber_t                               seq_;
  SuperVersion                                  *sv_;
  short                                          ptr_num_ = 0;
};

class EdgeIteratorTraverseOpt {
 public:
  EdgeIteratorTraverseOpt(const VertexId_t src, MemTable *newmemTable,
                          std::vector<std::vector<SSTableCache *> *> &fileMetaCache, SSTDataManager &sstdata_manager,
                          LevelIndex *vid_to_levelIndex, SuperVersion *sv, DelRecordManage *del_record_manager,
                          int max_level, SequenceNumber_t seq = MAX_SEQ_ID)
      : sv_(sv)
      , seq_(seq) {
    if (max_level < 0) {
      return;
    }

    if (max_level >= 1) {
      FileId_t min_level_0_fid = sv_->findex.get_min_level_0_fid();
      for (auto sst_it : *(sv_->get_version()->GetLevel0Files())) {
        if (seq_ <= sst_it->seq_) {
          continue;
        }
        if (sst_it->header.timeStamp >= min_level_0_fid && src <= sst_it->header.maxKey
            && src >= sst_it->header.minKey) {
          find_iterator_sst(sst_it, src, sstdata_manager);
        }
      }
    }
    // level>0, from levelindex
    if (max_level >= 2) {
      find_iterator_sst_by_levelindex(src, sstdata_manager, vid_to_levelIndex, max_level);
    }
    for (auto tb : sv_->get_memtable()) {
      if (seq_ <= tb->GetStartTime()) {
        continue;
      }
      it = std::shared_ptr<EdgeIteratorBase>(new NeighBors::MemEdgeIterator(tb->get_vertex_adj(src), tb->GetFid()));
      if (it->valid()) {
        it_array.insert(it_array.begin(), it);
      }
    }

    if (!it_array.empty()) {
      findFirstValid();
    } else {
      it = nullptr;
    }
  }

  void find_iterator_sst_by_levelindex(const VertexId_t src, SSTDataManager &sstdata_manager,
                                       LevelIndex *vid_to_levelIndex, int max_level) {
    uint32_t fileID      = 0;
    uint32_t offset      = 0;
    uint32_t next_offset = 0;

    MulLevelIndex &findex = sv_->findex;

    for (int levelID = 0; levelID < max_level - 1; levelID++) {
      if (FLAGS_support_mulversion == false) {
        int         index_id = src * LEVEL_INDEX_SIZE + levelID;
        LevelIndex &findex   = vid_to_levelIndex[index_id];
        fileID               = findex.get_fileID();

        if (fileID == INVALID_File_ID) {
          continue;
        }

        offset      = findex.get_offset();
        next_offset = findex.get_next_offset();
        assert(next_offset >= offset);
      } else {
        fileID = findex.get_fileID(levelID);

        if (fileID == INVALID_File_ID) {
          continue;
        }

        offset      = findex.get_offset(levelID);
        next_offset = findex.get_next_offset(levelID);
        assert(next_offset >= offset);
      }

      uint32_t adj_size = (next_offset - offset) / sizeof(EdgeBody_t);
      if (FLAGS_OPEN_SSTDATA_CACHE == true) {
        SSTDataCache *sstcache = sstdata_manager.get_data(fileID);
        assert(sstcache != nullptr);
        std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(new SSTEdgeIterator(
            (EdgeBody_t *)(sstcache->GetEdgeData() + offset), sstcache->GetPropertyData(), adj_size, fileID));
        if (it_temp->valid()) {
          it_array.emplace_back(it_temp);
        }
      } else {
        LOG_INFO(" ToDo get_edges...");
      }
    }
  }

  Status find_iterator_sst(SSTableCache *sst_it, VertexId_t src, SSTDataManager &sstdata_manager) {
    int pos = sst_it->get(src);

    if (pos < 0) {
      return Status::kNotFound;
    }

    uint32_t offset      = (sst_it->indexes)[pos].offset;
    uint32_t next_offset = (sst_it->indexes)[pos + 1].offset;

    if (FLAGS_OPEN_SSTDATA_CACHE == true) {
      SSTDataCache *sstcache = sstdata_manager.get_data(sst_it->header.timeStamp);
      assert(sstcache != nullptr);
      uint32_t adj_size = (next_offset - offset) / sizeof(EdgeBody_t);
      assert(next_offset >= offset);

      std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(
          new SSTEdgeIterator((EdgeBody_t *)(sstcache->GetEdgeData() + offset), sstcache->GetPropertyData(), adj_size,
                              sst_it->header.timeStamp));
      if (it_temp->valid()) {
        it_array.emplace_back(it_temp);
      }
    } else {
      LOG_INFO(" ToDo get_edges...");
    }
    return Status::kOk;
  }

  void init() {}

  bool valid() {
    return !(it == nullptr) && it->valid();
  }

  bool check_entry_valid() {
    assert(it != nullptr && it->valid());

    uint64_t temp_seq = it->sequence();
    if (temp_seq > seq_) {
      return false;
    }

    if (it->marker() != 0) {
      return false;
    }

    VertexId_t dst = it->dst_id();
    if (threadLocalSet.find(dst) != threadLocalSet.end()) {
      return false;
    }
    threadLocalSet.insert(dst);

    return true;
  }

  void findFirstValid() {
    it = nullptr;
    if (it_cnt >= it_array.size()) {
      return;
    }

    do {
      it = it_array[it_cnt++];
      while (valid()) {
        if (check_entry_valid()) {
          return;
        }
        it->next();
      }
    } while (it_cnt < it_array.size());

    if (it_cnt >= it_array.size()) {
      it = nullptr;
    }
  }

  void next() {
    while (true) {
      if (!valid()) {
        return;
      }
      it->next();

      if (!valid()) {
        findFirstValid();
        return;
      } else if (check_entry_valid()) {
        return;
      }
    }
  }

  VertexId_t dst_id() {
    return it->dst_id();
  }

  SequenceNumber_t sequence() {
    return it->sequence();
  }

  Marker_t marker() {
    return it->marker();
  }

  EdgeProperty_t edge_data() {
    return it->edge_data();
  }

  bool empty() {
    return it->empty();
  }

  size_t size() {
    return it->size();
  }

  bool compare(const std::shared_ptr<EdgeIteratorBase> r1, const std::shared_ptr<EdgeIteratorBase> r2) {
    if (r1->dst_id() == r2->dst_id()) {
      return r1->sequence() > r2->sequence();
    }
    return r1->dst_id() < r2->dst_id();
  }

  // clear all points
  ~EdgeIteratorTraverseOpt() {
    it_array.clear();
    threadLocalSet.clear();
  }

 private:
  std::shared_ptr<EdgeIteratorBase>                  it = nullptr;
  std::vector<std::shared_ptr<EdgeIteratorBase>>     it_array;
  uint                                               it_cnt = 0;
  SuperVersion                                      *sv_;
  SequenceNumber_t                                   seq_;
  static thread_local std::unordered_set<VertexId_t> threadLocalSet;

  [[maybe_unused]] bool curr_have_map_;
};

}  // namespace lsmg
#endif