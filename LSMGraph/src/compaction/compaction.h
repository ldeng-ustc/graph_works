#ifndef LSMG_COMPACTION_HEADER
#define LSMG_COMPACTION_HEADER

#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ctime>
#include <mutex>
#include <string>
#include "cache/sst_table_cache.h"

#include "cache/buffer_manager.h"
#include "cache/sst_data_manager.h"
#include "common/config.h"
#include "common/flags.h"
#include "common/utils/concurrent_queue.h"
#include "common/utils/livegraph/futex.hpp"
#include "compaction/compaction_container.h"
#include "index/del_record_manager.h"
#include "version/super_version.h"
#include "version/version_set.h"

using Futex = livegraph::Futex;

namespace lsmg {

struct SuperVersion;

class Compaction {
  using DefaultMulLevelMemDiskIndexManager = MulLevelMemDiskIndexManager<MEM_INDEX_NUM, DISK_INDEX_NUM>;

 public:
  Compaction(std::vector<std::vector<SSTableCache *> *> &_fileMetaCache, std::string &_dataDir, uint64_t &_currentTime,
             std::mutex *level_0_mux_, VersionSet *l0_versionset, std::atomic<SequenceNumber_t> &global_version_id,
             SSTDataManager &sstdata_manager, DelRecordManage &del_record_manager, SuperVersion &sv)
      : fileMetaCache_(_fileMetaCache)
      , dataDir(_dataDir)
      , currentTime(_currentTime)
      , buffer_manager(1, MAX_INDEX_NUM, 0, MAX_INDEX_NUM, 1, BODY_BUFFER_SIZE, 1, PROPERTY_BUFFER_SIZE)
      , state(false)
      , l0_versionset_(l0_versionset)
      , global_version_id_(global_version_id)
      , sstdata_manager_(sstdata_manager)
      , del_record_manager_(del_record_manager)
      , sv_(sv)
      , large_job_(0)
      , max_job_num_(0) {
    for (uint level = 0; level < MAX_LEVEL; level++) {
      compact_pointer_[level] = MAX_GLOBAL_SEQ;
    }
    max_subcompactions_ = FLAGS_max_subcompactions;
    tablecache_mutex_.emplace_back(level_0_mux_);
    for (uint i = 1; i < MAX_LEVEL; i++) {
      tablecache_mutex_.emplace_back(new std::mutex());
    }
  }

  void init(Futex *_vertex_futexes, LevelIndex *_vid_to_levelIndex, RWLock_t *_vertex_rwlocks,
            Level_t *_vertex_max_level, MulLevelIndexSharedArray &_vid_to_mullevelIndex,
            DefaultMulLevelMemDiskIndexManager *vid_to_mem_disk_index) {
    vertex_futexes         = _vertex_futexes;
    vertex_rwlocks_        = _vertex_rwlocks;
    vertex_max_level_      = _vertex_max_level;
    vid_to_levelIndex      = _vid_to_levelIndex;
    vid_to_mullevelIndex_  = _vid_to_mullevelIndex;  // vid to mullevel_index: v_num * 1
    vid_to_mem_disk_index_ = vid_to_mem_disk_index;
  }

  bool GetState() {
    return state.load(std::memory_order_acquire);
  }

  bool SetState(bool old_state, bool new_state) {
    return state.compare_exchange_strong(old_state, new_state);
  }

  uint32_t LargeJob() const {
    return large_job_;
  }

  ~Compaction() {
    RealRemoveFile();
    for (uint i = 1; i < MAX_LEVEL; i++) {
      delete tablecache_mutex_[i];
    }
  }

  bool PickCompactioFile(const uint level, const bool is_grow);
  void GetRange(const std::vector<SSTableCache *> &inputs, VertexId_t &smallest, VertexId_t &largest);
  void GetRange2(const std::vector<SSTableCache *> &inputs1, const std::vector<SSTableCache *> &inputs2,
                 VertexId_t &smallest, VertexId_t &largest);
  void GetOverlappingInputs(const uint level, const VertexId_t begin, const VertexId_t end,
                            std::vector<SSTableCache *> *inputs);
  void SetupOtherInputs(const int pre_level, const bool is_grow);
  void BackgroundCompaction();
  void RemoveCache(const uint level, const int inputs_index, bool free_space = true);
  void RemoveFile();
  void RealRemoveFile();
  void DirectAdjustLevel(const uint level);
  void UpdataLevelIndex(SSTableCache *it, const int level);
  void GenSubcompactionBoundaries(std::vector<WayAnchor> &all_wayAnchors, std::vector<VertexId_t> &boundaries);
  void PreCompaction(const int level, std::vector<Way> &ways);

  void DoCompactionWork_all_priorty(const int level, std::vector<Way> &ways);
  void ProcessCompactionWork(const int level, const int inputs_0_size, const int inputs_1_size,
                             std::vector<EdgeRecord> &edge_cache, std::vector<Way> &ways);
  void DoSubCompactionWork(const int level);
  void DoCompactionWorkTwoWay(const int level, const int inputs_0_size, const int inputs_1_size,
                              std::vector<EdgeRecord> &edge_cache, std::vector<Way> &ways);
  void DoGeneralCompactionWork(const int level);
  void ProcessMultiWaysCompaction(std::vector<Way> &ways, const int level);
  void ProcessMultiWaysCompactionOpt(const uint level, std::vector<WayAnchor> &all_wayAnchors, int anchor_begin,
                                     int anchor_end);

  void MaybeScheduleCompaction();

 private:
  std::vector<std::vector<SSTableCache *> *> &fileMetaCache_;  // Each level has some
  LevelIndex                                 *vid_to_levelIndex;
  MulLevelIndexSharedArray                    vid_to_mullevelIndex_;
  DefaultMulLevelMemDiskIndexManager         *vid_to_mem_disk_index_;
  Futex                                      *vertex_futexes;
  RWLock_t                                   *vertex_rwlocks_;
  Level_t                                    *vertex_max_level_;
  FileId_t                                    min_level_0_fid_;
  std::string                                &dataDir;
  uint64_t                                   &currentTime;
  SequenceNumber_t                            MAX_GLOBAL_SEQ = std::numeric_limits<SequenceNumber_t>::max();
  std::vector<Way>                            ways;
  std::vector<EdgeRecord>                     edge_cache;
  int                                         cache_max_size = 0;
  BufferManager                               buffer_manager;
  uint32_t                                    max_subcompactions_;
  std::vector<std::mutex *>                   tablecache_mutex_;
  std::set<FileId_t>                          merged_fids_[2];
  grape::BlockingQueue<FileId_t>              delete_fids_set_;
  std::vector<SSTableCache *>                 delete_filemeta_set_;
  std::vector<SSTableCache *>                 inputs_[2];  // The two sets of inputs
  SequenceNumber_t                            compact_pointer_[MAX_LEVEL];
  VertexId_t                                  last_merge_max_key_ = 0;
  std::atomic<bool>                           state;
  VersionSet                                 *l0_versionset_;
  std::atomic<SequenceNumber_t>              &global_version_id_;

  VersionEdit      version_edit_;
  SSTDataManager  &sstdata_manager_;
  DelRecordManage &del_record_manager_;
  SuperVersion    &sv_;

  std::atomic<uint32_t> large_job_;
  std::atomic<uint32_t> max_job_num_;
};

}  // namespace lsmg

#endif