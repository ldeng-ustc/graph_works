#ifndef LSMG_SSTABLE_WRITER_HEADER
#define LSMG_SSTABLE_WRITER_HEADER

#include <sys/types.h>
#include <cstdint>
#include <fstream>
#include <unordered_set>
#include <vector>

#include "cache/buffer_manager.h"
#include "cache/sst_table_cache.h"
#include "common/utils/livegraph/futex.hpp"
#include "compaction/compaction_container.h"
#include "index/index.h"
#include "version/version_set.h"

namespace lsmg {

using Futex = livegraph::Futex;

class SSTableWriter {
  using DefaultMulLevelMemDiskIndexManager = MulLevelMemDiskIndexManager<MEM_INDEX_NUM, DISK_INDEX_NUM>;

 public:
  SSTableWriter(std::vector<std::vector<SSTableCache *> *> &fileMetaCache, LevelIndex *vid_to_levelIndex,
                MulLevelIndexSharedArray &vid_to_mullevelIndex, Futex *vertex_futexes, RWLock_t *vertex_rwlocks,
                Level_t *vertex_max_level, FileId_t min_level_0_fid, int level, BufferManager &buffer_manager,
                std::vector<std::mutex *> &tablecache_mutex, VersionEdit &version_edit, SSTDataManager &sstdata_manager,
                DefaultMulLevelMemDiskIndexManager &vid_to_mem_disk_index)
      : level_(level)
      , fileMetaCache_(fileMetaCache)
      , vid_to_levelIndex_(vid_to_levelIndex)
      , vid_to_mullevelIndex_(vid_to_mullevelIndex)
      , vertex_rwlocks_(vertex_rwlocks)
      , vertex_max_level_(vertex_max_level)
      , min_level_0_fid_(min_level_0_fid)
      , vid_to_mem_disk_index_(vid_to_mem_disk_index)
      , buffer_manager_(buffer_manager)
      , tablecache_mutex_(tablecache_mutex)
      , version_edit_(version_edit)
      , vertex_futexes_(vertex_futexes)
      , sstdata_manager_(sstdata_manager) {
    edge_body_buffer_  = buffer_manager_.GetEdgeBodyBuffer();
    edge_index_buffer_ = buffer_manager_.GetMaxIndexBuffer();
    p_file_content_    = buffer_manager_.GetPropertyBuffer();
  }

  void Init(const std::string &efile_name, const std::string &pfile_name, VertexId_t first_src,
            const uint64_t timestamp) {
    // init data
    timestamp_   = timestamp;
    edge_ptr_    = 0;
    edge_cnt_    = 0;
    index_cnt_   = 0;
    p_ptr_       = 0;
    p_file_size_ = 0;
    bloom_filter_.reset();
    cur_src_vtx_ = INVALID_VERTEX_ID;
    first_src_   = first_src;

    e_file_fd = -1;
    p_file_fd = -1;
    path_     = std::move(efile_name);
    e_file_fd = open(path_.c_str(), O_WRONLY | O_CREAT, 0644);
    p_file_fd = open(pfile_name.c_str(), O_WRONLY | O_CREAT, 0644);

    assert(e_file_fd > 0);
    assert(p_file_fd > 0);
  }

  ~SSTableWriter() {
    buffer_manager_.FreeEdgeBodyBuffer(edge_body_buffer_);
    buffer_manager_.FreeMaxIndexBuffer(edge_index_buffer_);
    buffer_manager_.FreePropertyBuffer(p_file_content_);
  };

  // if e_file full before Write, then return false, else return true
  auto Write(EdgeRecord &edge_record) -> bool;
  void WriteEnd();

  static void get_edge_frome_file(std::ifstream &file, Edge &edge, uint32_t obj_offset, std::string &efile_path,
                                  size_t all_edge_num, std::string *property);

  static void print_ssTable(std::string path, uint print_size = 20);

  bool IsClear() {
    return edge_ptr_ == 0;
  }

  void InsertInput0Fid(FileId_t fid) {
    input_0_fidset_.insert(fid);
  }

 private:
  int e_file_fd;
  int p_file_fd;

  EdgeBody_t *edge_body_buffer_;
  uint64_t    edge_ptr_ = 0;
  uint64_t    edge_cnt_ = 0;

  Index   *edge_index_buffer_;
  uint64_t index_cnt_ = 0;

  char                *p_file_content_;
  EdgePropertyOffset_t p_ptr_       = 0;
  EdgePropertyOffset_t p_file_size_ = 0;

  BloomFilter bloom_filter_;
  VertexId_t  cur_src_vtx_ = INVALID_VERTEX_ID;

  VertexId_t first_src_;
  uint64_t   timestamp_;

  std::string                                 path_;
  int                                         level_;
  std::vector<std::vector<SSTableCache *> *> &fileMetaCache_;  // Each level has some
  LevelIndex                                 *vid_to_levelIndex_;
  MulLevelIndexSharedArray                    vid_to_mullevelIndex_;
  RWLock_t                                   *vertex_rwlocks_;
  Level_t                                    *vertex_max_level_;
  FileId_t                                    min_level_0_fid_;

  DefaultMulLevelMemDiskIndexManager &vid_to_mem_disk_index_;

  BufferManager             &buffer_manager_;
  std::vector<std::mutex *> &tablecache_mutex_;

  std::unordered_set<int> input_0_fidset_;

  [[maybe_unused]] VersionEdit &version_edit_;
  [[maybe_unused]] Futex       *vertex_futexes_;

  SSTDataManager &sstdata_manager_;

  auto CurSize() const -> size_t {
    return BLOOM_FILTER_SIZE + HEADER_SIZE + (edge_cnt_ + 1) * sizeof(EdgeBody_t) + (index_cnt_ + 1) * sizeof(Index);
  }
};
}  // namespace lsmg

#endif