#ifndef LSMG_MEM_TABLE_HEADER
#define LSMG_MEM_TABLE_HEADER
#include <iostream>
#include <vector>
#include "cache/sst_data_manager.h"
#include "common/config.h"
#include "common/utils/arena.h"
#include "common/utils/livegraph/allocator.hpp"
#include "compaction/compaction.h"
#include "container/neighbors.h"
#include "graph/edge.h"
#include "index/del_record_manager.h"
#include "version/version_set.h"

class SuperVersion;

namespace lsmg {

class MemTable {
 private:
  uintptr_t       *vertex_adjs;
  uint64_t         listLength;  // the number of edges
  Arena<NeighBors> edge_arena;
  const size_t     max_edge_num =
      (MAX_TABLE_SIZE - HEADER_SIZE - BLOOM_FILTER_SIZE)
      / sizeof(
          EdgeBody_t);  // For allocating space, an error will occur if MemTable's number of edges exceeds this number!
  size_t                                max_vertex_num;
  std::atomic<VertexId_t>              &vertex_id_;  // max vertex_id actually used
  size_t                                src_vertex_num = 0;
  std::mutex                           &level_0_mux_;
  VersionSet                           *l0_versionset_;
  Futex                                *vertex_futexes_;
  Level_t                              *vertex_max_level_;
  Compaction                           &compactor_;
  SuperVersion                         &sv_;
  std::atomic<SequenceNumber_t>        &global_version_id_;
  SSTDataManager                       &sstdata_manager_;
  std::atomic<int32_t>                  refs;
  bool                                  islive  = false;  // can read if free=true
  bool                                  isflash = false;  // can read if free=false
  livegraph::SparseArrayAllocator<void> array_allocator;
  FileId_t                              fid_;
  SequenceNumber_t                      start_time_;
  [[maybe_unused]] DelRecordManage     &del_record_manager_;

 public:
  int64_t remain_capacity;
  MemTable(std::mutex &level_0_mux, const size_t _max_vertex_num, std::atomic<VertexId_t> &vertex_id,
           Futex *vertex_futexes, Level_t *vertex_max_level, VersionSet *l0_versionset, Compaction &compactor,
           SuperVersion &sv, SSTDataManager &sstdata_manager, DelRecordManage &del_record_manager,
           std::atomic<SequenceNumber_t> &global_version_id,
           size_t _max_edge_num = (MAX_TABLE_SIZE - HEADER_SIZE - BLOOM_FILTER_SIZE) / sizeof(EdgeBody_t))
      : listLength(0)
      , edge_arena(_max_edge_num + 1)
      , max_edge_num(_max_edge_num)
      , max_vertex_num(_max_vertex_num)
      , vertex_id_(vertex_id)
      , level_0_mux_(level_0_mux)
      , l0_versionset_(l0_versionset)
      , vertex_futexes_(vertex_futexes)
      , vertex_max_level_(vertex_max_level)
      , compactor_(compactor)
      , sv_(sv)
      , global_version_id_(global_version_id)
      , sstdata_manager_(sstdata_manager)
      , refs(0)
      , array_allocator()
      , fid_(0)
      , del_record_manager_(del_record_manager) {
    auto pointer_allocater = std::allocator_traits<decltype(array_allocator)>::rebind_alloc<uintptr_t>(array_allocator);
    vertex_adjs            = pointer_allocater.allocate(max_vertex_num);

    remain_capacity = max_edge_num;
    clear_vertex_adj();
  }

  ~MemTable() {
    // delete[] vertex_adjs;
    auto pointer_allocater = std::allocator_traits<decltype(array_allocator)>::rebind_alloc<uintptr_t>(array_allocator);
    pointer_allocater.deallocate(vertex_adjs, max_vertex_num);
  };

  void put_edge(VertexId_t src, VertexId_t dis, const EdgeProperty_t &s, Marker_t marker, SequenceNumber_t seq,
                size_t id);

  Status get(VertexId_t src_, VertexId_t dst_, std::string *property);

  Status get(VertexId_t src, VertexId_t dst, FileId_t &fid, SequenceNumber_t &seq);

  void get_edges(VertexId_t src, std::vector<Edge> &edges);

  void save2eSSTable(const std::string &dir, uint64_t &currentTime, std::vector<SSTableCache *> *level_0_cache);

  void save2eSSTable_split_property(const std::string &dir, uint64_t &currentTime,
                                    std::vector<SSTableCache *> *level_0_cache);

  void save2eSSTable_split(const std::string &dir, uint64_t &currentTime, std::vector<SSTableCache *> *level_0_cache);

  void     print(std::string label);
  uint64_t GetListLength();
  template <typename T>
  void             AutomicAdd(T &a, T b);
  uint64_t         GetMaxEdgeNum();
  void             SetStartTime(SequenceNumber_t start_time);
  SequenceNumber_t GetStartTime();
  void             SetFid(FileId_t fid);
  FileId_t         GetFid() const;
  void             reset();
  void             clear_vertex_adj();
  NeighBors       *get_vertex_adj(VertexId_t src);

  bool checkIsFinishWrite();

  void    Ref();
  void    Unref();
  int32_t Getref();
  void    SetLive(bool _islive);
  void    SetFlash(bool _isflash);
  bool    IsLive();
  bool    IsFlash();

  void init_vertex_adjs(VertexId_t vid) {
    vertex_adjs[vid] = NULLPOINTER;
  }
};
}  // namespace lsmg

#endif
