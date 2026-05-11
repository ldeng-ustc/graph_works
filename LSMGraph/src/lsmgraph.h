#ifndef LSMGRAPH_KVSTORE_H
#define LSMGRAPH_KVSTORE_H

#include <tbb/concurrent_queue.h>
#include <fstream>
#include <list>
#include <mutex>
#include <string>

#include "cache/block_manager.h"
#include "cache/mem_table.h"
#include "cache/sst_data_manager.h"
#include "cache/sst_table_cache.h"
#include "common/config.h"
#include "common/utils/leveldb/port/port_stdcxx.h"
#include "common/utils/livegraph/allocator.hpp"
#include "common/utils/livegraph/futex.hpp"
#include "common/utils/thread_pool.h"
#include "compaction/compaction.h"
#include "container/edge_iterator.h"
#include "graph/edge.h"
#include "index/del_record_manager.h"
#include "version/version_set.h"

using Futex = livegraph::Futex;

namespace lsmg {

class LSMGraph {
  using DefaultMulLevelMemDiskIndexManager = MulLevelMemDiskIndexManager<MEM_INDEX_NUM, DISK_INDEX_NUM>;

 private:
  std::atomic<MemTable *>                  memTable_;
  leveldb::port::Mutex                     mu_;
  leveldb::port::CondVar memtabl_state_cv_ GUARDED_BY(mu_);

  std::vector<MemTable *> memTable_list_;
  std::queue<MemTable *>  free_menTables;
  std::mutex              memtable_mux_;
  std::mutex              memtable_insert_mux_;
  std::condition_variable memtable_cv_;

  std::mutex                                 level_0_mux_;   // update level-0 file in fileMetaCache[0]
  std::vector<std::vector<SSTableCache *> *> fileMetaCache;  // Each level has some SSTableCache
  std::vector<std::list<SSTableCache *>>     index2cache_;
  std::vector<std::unordered_map<SSTableCache *, std::list<SSTableCache *>::iterator>> cache2index_;
  uint64_t                                                                             currentTime;  // file id
  std::string                                                                          dataDir;
  SequenceNumber_t                                                                     global_seq;  // edge time
  SequenceNumber_t        MAX_GLOBAL_SEQ = std::numeric_limits<SequenceNumber_t>::max();
  ThreadPool              worker_pool;
  const size_t            max_vertex_num_;  // max vertex_id supported by the system
  std::atomic<VertexId_t> vertex_id_;       // max vertex_id actually used
  BlockManager            block_manager_;
  SSTDataManager          sstdata_manager_;
  DelRecordManage         del_record_manager_;

  std::unordered_map<std::string, std::ifstream *> file_handle_cache_;  // cache read file head

  // level file index
  livegraph::SparseArrayAllocator<void> array_allocator;
  Futex                                *vertex_futexes_;
  RWLock_t                             *vertex_rwlocks_;     // read and update level_index
  LevelIndex                           *vid_to_levelIndex_;  // vid to level_index: v_num*(level-1)
  MulLevelIndexSharedArray              vid_to_mullevelIndex_;
  DefaultMulLevelMemDiskIndexManager    vid_to_mem_disk_index_;

  // vertex max level
  Level_t *vertex_max_level_;  // max level of a vertex's edge exist, here, mem is level-0, eq., L_0=1

  VersionSet  *l0_versionset_;
  SuperVersion sv_;

  std::atomic<SequenceNumber_t>        global_version_id_ = 0;
  static thread_local SequenceNumber_t local_version_id_;
  static thread_local SuperVersion     local_sv_;

  Compaction compactor_;

 public:
  static void open(const std::string &dir, const size_t max_vertex_num, LSMGraph **db);

  LSMGraph(const std::string &dir, const size_t max_vertex_num, int num_threads, int memtable_num);

  ~LSMGraph();

  void valid_index(uint max_vertex_id);

  Status del_edge(VertexId_t src, VertexId_t dst);

  Status del_edge_sep(VertexId_t src, VertexId_t dis);

  VertexId_t new_vertex(bool use_recycled_vertex = false);

  void put_vertex(VertexId_t vertex_id, std::string_view data);

  void put_edge(VertexId_t src, VertexId_t dst, const EdgeProperty_t &s);

  Status get_edge(VertexId_t src, VertexId_t dst, std::string *property);

  Status get_edge(VertexId_t src, VertexId_t dst, FileId_t &fid, SequenceNumber_t &seq);

  Status find_edge(VertexId_t src, VertexId_t dst, std::string *property, SSTableCache *it);

  Status find_edge(VertexId_t src, VertexId_t dst, SequenceNumber_t &seq, SSTableCache *it);

  Status find_edge_by_levelindex(VertexId_t src, VertexId_t dst, std::string *property, SuperVersion &sv);

  Status find_edge_by_levelindex(VertexId_t src, VertexId_t dst, FileId_t &fid, SequenceNumber_t &seq,
                                 SuperVersion &local_sv);

  bool get_edges(VertexId_t src, std::vector<Edge> &edges);

  EdgeIterator get_edges(VertexId_t src, SequenceNumber_t seq = MAX_SEQ_ID, bool use_disk_index = true);

  void print_memTable(std::string label);

  SequenceNumber_t automic_get_global_seq(SequenceNumber_t num);

  SequenceNumber_t LastSequence();

  void SetLastSequence(SequenceNumber_t last_sequence);

  bool find(VertexId_t target, uint32_t s_offset, uint32_t e_offset, uint32_t step_offset, std::ifstream &file,
            uint32_t &obj_offset);

  Status find_edge_from_file_with_cache(VertexId_t target, uint32_t s_offset, uint32_t e_offset, std::ifstream *file,
                                        std::string &path, std::string *property);

  Status find_edge_from_file_with_cache_by_directed_IO(VertexId_t src, VertexId_t dst, uint32_t s_offset,
                                                       uint32_t e_offset, uint32_t fileID, SequenceNumber_t &seq);

  Status find_edge_from_file_with_cache(VertexId_t target, uint32_t s_offset, uint32_t e_offset, std::ifstream *file,
                                        std::string &path, SequenceNumber_t &seq);

  Status find_edges_from_file_with_cache(uint32_t s_offset, uint32_t e_offset, std::ifstream *file, SSTableCache *sst,
                                         std::vector<Edge> &edges);

  Status find_edge_from_sstdata_cache(VertexId_t src, VertexId_t dst, uint32_t s_offset, uint32_t e_offset,
                                      uint32_t fileID, std::string *property);

  Status find_edge_from_sstdata_cache(VertexId_t src, VertexId_t dst, uint32_t s_offset, uint32_t e_offset,
                                      uint32_t fileID, SequenceNumber_t &seq);

  Status find_edges_from_sstdata_cache(uint32_t s_offset, uint32_t e_offset, uint32_t fileID, std::vector<Edge> &edges);

  MemTable *get_newmemTable();

  void recycle_memTable(MemTable *table);

  void check_vertex_id(VertexId_t vertex_id);

  void load_sstdatacache();

  VertexId_t get_max_vertex_num();

  void compact();

  bool GetCompactionState();

  void print_all_file_info();

  void static_edge_distribution();
  void printSpaceInfo();
  void printLevelInfo();

  void get_superversion(SuperVersion &sv);

  void get_superversion();

  SequenceNumber_t get_sequence() const;

  void debug();

  void PrintSSTable();

  void AwaitWrite();

  bool CheckState() const;
};

}  // namespace lsmg
#endif