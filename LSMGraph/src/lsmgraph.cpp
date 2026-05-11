#include "lsmgraph.h"
#include <gflags/gflags.h>
#include <cstdlib>
#include <exception>
#include <random>
#include "common/config.h"
#include "common/utils/logger.h"
#include "compaction/sstable_writer.h"
#include "index/index.h"

namespace lsmg {

thread_local SequenceNumber_t               LSMGraph::local_version_id_ = 0;
thread_local SuperVersion                   LSMGraph::local_sv_         = SuperVersion();
thread_local std::unordered_set<VertexId_t> EdgeIteratorTraverseOpt::threadLocalSet;

LSMGraph::LSMGraph(const std::string &dir, const size_t max_vertex_num, int thread_num, int memtable_num)
    : memtabl_state_cv_(&mu_)
    , worker_pool(thread_num)
    , max_vertex_num_(max_vertex_num)
    , vertex_id_(0)
    , block_manager_(FLAGS_mmap_path)
    , vid_to_mem_disk_index_(dir + "/disk_index_mmap_file", max_vertex_num)
    , l0_versionset_(new VersionSet(level_0_mux_))
    , compactor_(fileMetaCache, dataDir, currentTime, &level_0_mux_, l0_versionset_, global_version_id_,
                 sstdata_manager_, del_record_manager_, sv_) {
  if (dir[dir.length()] == '/')
    dataDir = dir.substr(0, dir.length() - 1);
  else
    dataDir = dir;
  currentTime = 0;
  global_seq  = 0;

  if (FLAGS_LOAD_OLD_DATA == false) {
    LOG_INFO(" *clean up the old sstable file...");
    // clean up the old sstable file
    if (utils::dirExists(dataDir)) {
      int s = utils::rmdir(dataDir.c_str());
      assert(s != -1);
      LOG_INFO(" delete: {} {} file(s).", dataDir, s);
    }
    utils::mkdir(dataDir.c_str());
    LOG_INFO(" save dataDir={}", dataDir);
    assert(utils::dirExists(dataDir));
    utils::mkdir((dataDir).c_str());
  }

  // init lock
  auto futex_allocater = std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Futex>(array_allocator);
  vertex_futexes_      = futex_allocater.allocate(max_vertex_num_);

  auto rwlock_allocater = std::allocator_traits<decltype(array_allocator)>::rebind_alloc<RWLock_t>(array_allocator);
  vertex_rwlocks_       = rwlock_allocater.allocate(max_vertex_num_);

  auto vertex_max_level_allocater =
      std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Level_t>(array_allocator);
  vertex_max_level_ = vertex_max_level_allocater.allocate(max_vertex_num_);

  // init level_index
  // size_t init_vertex_num = 1ul << 32;
  if (FLAGS_support_mulversion == false) {
    size_t real_vector_size = LEVEL_INDEX_SIZE * max_vertex_num_;
    auto   levelIndex_allocater =
        std::allocator_traits<decltype(array_allocator)>::rebind_alloc<LevelIndex>(array_allocator);
    vid_to_levelIndex_ = levelIndex_allocater.allocate(real_vector_size);
    LOG_INFO(" levelIndex space: {}GB", (real_vector_size * sizeof(LevelIndex)) / 1024.0 / 1024 / 1024);
  } else {
    VertexId_t tmp_max_vertex_num = MMAP_INITIAL_SIZE >> 6;
    if (FLAGS_LOAD_OLD_DATA == true) {
      std::string   file_info_path = dataDir + "/file.info";
      std::ifstream file(file_info_path, std::ios::binary);
      file.read((char *)&tmp_max_vertex_num, sizeof(tmp_max_vertex_num));
      file.close();
    }

    size_t real_vector_size = max_vertex_num_;
    vid_to_mullevelIndex_   = MulLevelIndexSharedArray{dataDir, tmp_max_vertex_num};  // mmap size = vertex_num * 64B
    LOG_INFO(" levelIndex space: {}GB", (real_vector_size * sizeof(MulLevelIndex)) / 1024.0 / 1024 / 1024);
  }
  compactor_.init(vertex_futexes_, vid_to_levelIndex_, vertex_rwlocks_, vertex_max_level_, vid_to_mullevelIndex_,
                  &vid_to_mem_disk_index_);

  // init memtable
  memTable_list_.reserve(memtable_num);
  for (int i = 0; i < memtable_num; i++) {
    MemTable *tb =
        new MemTable(level_0_mux_, max_vertex_num, vertex_id_, vertex_futexes_, vertex_max_level_, l0_versionset_,
                     compactor_, sv_, sstdata_manager_, del_record_manager_, global_version_id_);
    memTable_list_.emplace_back(tb);
    free_menTables.push(tb);
  }
  MemTable *mem = get_newmemTable();
  memTable_.store(mem, std::memory_order_release);
  mem->SetLive(true);
  mem->SetFid(__sync_fetch_and_add(&currentTime, 1));
  mem->SetStartTime(automic_get_global_seq(mem->GetMaxEdgeNum()));

  // init superversion
  l0_versionset_->VersionLock();
  std::shared_ptr<VersionAndMemTable> new_vm = std::make_shared<VersionAndMemTable>();
  new_vm->set_vs(l0_versionset_->GetCurrent());
  new_vm->insert_tb(mem);
  {
    std::unique_lock w_lock(sv_.vm_rw_mtx);
    sv_.version_memtable = new_vm;
  }
  global_version_id_.fetch_add(1, std::memory_order_acquire);
  l0_versionset_->VersionUnLock();

  // init fileMetaCache
  fileMetaCache.reserve(MAX_LEVEL);
  for (uint i = 0; i < MAX_LEVEL; i++) {
    fileMetaCache.push_back(new std::vector<SSTableCache *>());
  }

  // load old data for db
  if (FLAGS_LOAD_OLD_DATA == true) {
    LOG_ERROR("Not supported.");
  }

  // Iterate over all GFlags variables and output their names and values
  if (false) {
    std::vector<google::CommandLineFlagInfo> all_flags;
    GetAllFlags(&all_flags);
    for (const gflags::CommandLineFlagInfo &flag_info : all_flags) {
      LOG_INFO("FLAGS_{}={}", flag_info.name, flag_info.current_value);
    }
  }
}

VertexId_t LSMGraph::new_vertex(bool use_recycled_vertex) {
  VertexId_t vertex_id = vertex_id_.fetch_add(1, std::memory_order_relaxed);

  // Initialize all resources related to this vertex
  // must be initialized manually
  if (FLAGS_support_mulversion == false) {
    for (uint level_id = 0; level_id < LEVEL_INDEX_SIZE; level_id++) {
      vid_to_levelIndex_[vertex_id * LEVEL_INDEX_SIZE + level_id].init();
    }
  } else if (FLAGS_LOAD_OLD_DATA == false) {
    vid_to_mullevelIndex_[vertex_id].init();
  }
  vertex_futexes_[vertex_id].clear();

  if (FLAGS_LOAD_OLD_DATA == false) {
    vertex_max_level_[vertex_id] = -1;
  } else {
    vertex_max_level_[vertex_id] = MAX_LEVEL;
  }

  for (auto tb : memTable_list_) {
    tb->init_vertex_adjs(vertex_id);
  }
  return vertex_id;
}

void LSMGraph::put_vertex(VertexId_t vertex_id, std::string_view data) {}

LSMGraph::~LSMGraph() {
  while (GetCompactionState()) {
    LOG_INFO(" wait for the compaction to complete...");
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  MemTable *mem_ = memTable_.load(std::memory_order_relaxed);

  if (mem_->GetListLength() > 0) {
    LOG_INFO(" save data before close db.");
    mem_->save2eSSTable(dataDir, currentTime, fileMetaCache[0]);
  }

  // save file information of every level
  if (FLAGS_LOAD_OLD_DATA == false) {
    if (FLAGS_support_mulversion == true) {
      // update version
      l0_versionset_->VersionLock();
      fileMetaCache[0] = l0_versionset_->GetCurrent()->GetLevel0Files();
      l0_versionset_->VersionUnLock();
    }

    std::string filename = dataDir + "/file.info";
    LOG_INFO("\nwrite to: {}", filename);
    std::ofstream outFile(filename, std::ios::binary | std::ios::out);

    int        levelNum       = fileMetaCache.size();
    int        file_num       = 0;
    VertexId_t max_vertex_num = vertex_id_.load() + 1;
    outFile.write(reinterpret_cast<char *>(&max_vertex_num), sizeof(max_vertex_num));
    outFile.write(reinterpret_cast<char *>(&levelNum), sizeof(levelNum));
    size_t offset_part_size = 0;
    for (int i = 0; i < levelNum; ++i) {
      int level_file_num = fileMetaCache[i]->size();
      outFile.write(reinterpret_cast<char *>(&level_file_num), sizeof(level_file_num));
      for (auto it = fileMetaCache[i]->begin(); it != fileMetaCache[i]->end(); ++it) {
        offset_part_size += (*it)->header.index_size * sizeof(Index);
        outFile.write(reinterpret_cast<char *>(&(*it)->header.timeStamp), sizeof(uint64_t));
        ++file_num;
      }
    }
    outFile.close();
  }

  for (auto it1 = fileMetaCache.begin(); it1 != fileMetaCache.end(); ++it1) {
    for (auto it2 = (*it1)->begin(); it2 != (*it1)->end(); ++it2) delete (*it2);
  }

  for (auto tb : memTable_list_) {
    delete tb;
  }

  for (auto &f_handle : file_handle_cache_) {
    f_handle.second->close();
    delete f_handle.second;
  }

  sv_.~SuperVersion();
  delete l0_versionset_;

  auto futex_allocater = std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Futex>(array_allocator);
  futex_allocater.deallocate(vertex_futexes_, max_vertex_num_);

  auto rwlock_allocater = std::allocator_traits<decltype(array_allocator)>::rebind_alloc<RWLock_t>(array_allocator);
  rwlock_allocater.deallocate(vertex_rwlocks_, max_vertex_num_);

  auto vertex_max_level_allocater =
      std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Level_t>(array_allocator);
  vertex_max_level_allocater.deallocate(vertex_max_level_, max_vertex_num_);

  if (FLAGS_support_mulversion == false) {
    auto levelIndex_allocater =
        std::allocator_traits<decltype(array_allocator)>::rebind_alloc<LevelIndex>(array_allocator);
    levelIndex_allocater.deallocate(vid_to_levelIndex_, max_vertex_num_ * LEVEL_INDEX_SIZE);
  }
  LOG_INFO("closed LSMStore.");
}

static void Sleep(double t) {
  timespec req;
  req.tv_sec  = (int)t;
  req.tv_nsec = (int64_t)(1e9 * (t - (int64_t)t));
  assert(req.tv_nsec >= 0);
  nanosleep(&req, NULL);
}

void LSMGraph::put_edge(VertexId_t src, VertexId_t dst, const EdgeProperty_t &s) {
  check_vertex_id(src);
  check_vertex_id(dst);
  Marker_t marker = false;
  if (s == "~DELETED~") {
    marker = true;
  }

#ifdef WRITE_STALL
  AwaitWrite();
#endif

  int64_t remain_capacity = 0;

  MemTable *old_mem = memTable_.load(std::memory_order_acquire);
  remain_capacity   = __sync_fetch_and_sub(&old_mem->remain_capacity, 1);
  auto timeout      = std::chrono::milliseconds(10);
  while (remain_capacity <= 0) {
    std::unique_lock<std::mutex> locker(memtable_insert_mux_);
    old_mem         = memTable_.load(std::memory_order_acquire);
    remain_capacity = __sync_fetch_and_sub(&old_mem->remain_capacity, 1);
    if (remain_capacity > 0) {
      break;
    }
    memtable_cv_.wait_for(locker, timeout);
    old_mem         = memTable_.load(std::memory_order_acquire);
    remain_capacity = __sync_fetch_and_sub(&old_mem->remain_capacity, 1);
  }

  assert(remain_capacity > 0);
  size_t           id  = old_mem->GetMaxEdgeNum() - remain_capacity;
  SequenceNumber_t seq = old_mem->GetStartTime() + id;

  // insert edge
  old_mem->put_edge(src, dst, s, marker, seq, id);

  if (remain_capacity > 1) {
    return;
  }

  // switch memtable to im_mem
  MemTable *null_memtable = get_newmemTable();
  null_memtable->SetStartTime(automic_get_global_seq(null_memtable->GetMaxEdgeNum()));
  null_memtable->SetLive(true);
  null_memtable->SetFid(__sync_fetch_and_add(&currentTime, 1));
  memTable_.store(null_memtable, std::memory_order_release);

#ifdef DEL_EDGE_SEPARATE
  { del_record_manager_.clean(); }
#endif

  memtable_cv_.notify_all();

  l0_versionset_->VersionLock();
  std::shared_ptr<VersionAndMemTable> old_vm;
  {
    std::shared_lock r_lock(sv_.vm_rw_mtx);
    old_vm = sv_.version_memtable;
  }
  std::shared_ptr<VersionAndMemTable> new_vm = std::make_shared<VersionAndMemTable>();
  new_vm->batch_insert_tb(old_vm->menTables);
  new_vm->set_vs(old_vm->current_);
  new_vm->insert_tb(null_memtable);
  {
    std::unique_lock w_lock(sv_.vm_rw_mtx);
    sv_.version_memtable = new_vm;
  }
  global_version_id_.fetch_add(1, std::memory_order_acquire);
  l0_versionset_->VersionUnLock();

  bool open_extra_compression_thread = true;
  if (open_extra_compression_thread) {
    auto save = [this](MemTable *immemTable) {
      immemTable->save2eSSTable(dataDir, currentTime, fileMetaCache[0]);
      recycle_memTable(immemTable);
      compactor_.MaybeScheduleCompaction();
    };
    worker_pool.enqueue(save, old_mem);
  } else {
    old_mem->save2eSSTable(dataDir, currentTime, fileMetaCache[0]);
    recycle_memTable(old_mem);
    compactor_.MaybeScheduleCompaction();
  }
}

Status LSMGraph::find_edge(VertexId_t src, VertexId_t dst, std::string *property, SSTableCache *it) {
  Status rs  = Status::kNotFound;
  int    pos = it->get(src, dst);
  if (pos < 0) return rs;

  uint32_t offset      = (it->indexes)[pos].offset;
  uint32_t next_offset = (it->indexes)[pos + 1].offset;

  if (FLAGS_OPEN_SSTDATA_CACHE == true) {
    rs = find_edge_from_sstdata_cache(src, dst, offset, next_offset, it->header.timeStamp, property);
  } else {
    std::ifstream *file;
    if (file_handle_cache_.find(it->path) != file_handle_cache_.end()) {
      file = file_handle_cache_[it->path];
    } else {
      file = new std::ifstream(it->path, std::ios::binary | std::ios::in);
      file_handle_cache_.emplace(it->path, file);
    }
    if (!file) {
      printf("Lost file: %s", (it->path).c_str());
      exit(-1);
    }

    rs = find_edge_from_file_with_cache(dst, offset, next_offset, file, it->path, property);
  }
  return rs;
}

Status LSMGraph::find_edge(VertexId_t src, VertexId_t dst, SequenceNumber_t &seq, SSTableCache *it) {
  Status rs  = Status::kNotFound;
  int    pos = it->get(src, dst);
  if (pos < 0) return rs;

  uint32_t offset      = (it->indexes)[pos].offset;
  uint32_t next_offset = (it->indexes)[pos + 1].offset;

  if (FLAGS_OPEN_SSTDATA_CACHE == true) {
    rs = find_edge_from_sstdata_cache(src, dst, offset, next_offset, it->header.timeStamp, seq);
  } else {
    std::ifstream *file;
    if (file_handle_cache_.find(it->path) != file_handle_cache_.end()) {
      file = file_handle_cache_[it->path];
    } else {
      file = new std::ifstream(it->path, std::ios::binary | std::ios::in);
      file_handle_cache_.emplace(it->path, file);
    }
    if (!file) {
      printf("Lost file: %s", (it->path).c_str());
      exit(-1);
    }

    rs = find_edge_from_file_with_cache(dst, offset, next_offset, file, it->path, seq);
  }
  return rs;
}

Status LSMGraph::find_edge_by_levelindex(VertexId_t src, VertexId_t dst, std::string *property,
                                         SuperVersion &local_sv) {
  Status rs = Status::kNotFound;

  uint32_t fileID      = 0;
  uint32_t offset      = 0;
  uint32_t next_offset = 0;

  MulLevelIndex &findex = local_sv.findex;
  // FileId_t min_level_0_fid = 0;

  for (uint levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
    if (FLAGS_support_mulversion == false) {
      int         index_id = src * LEVEL_INDEX_SIZE + levelID;
      LevelIndex &findex   = vid_to_levelIndex_[index_id];
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

    if (FLAGS_OPEN_SSTDATA_CACHE == true) {
      rs = find_edge_from_sstdata_cache(src, dst, offset, next_offset, fileID, property);
    } else {
      std::string    path = dataDir + "/" + std::to_string(fileID) + ".sst";
      std::ifstream *file;
      if (file_handle_cache_.find(path) != file_handle_cache_.end()) {
        file = file_handle_cache_[path];
      } else {
        file = new std::ifstream(path, std::ios::binary | std::ios::in);
        file_handle_cache_.emplace(path, file);
      }
      if (!file) {
        printf("Lost file: %s", path.c_str());
        exit(-1);
      }

      rs = find_edge_from_file_with_cache(dst, offset, next_offset, file, path, property);
    }
    if (rs != Status::kNotFound) {  // not found in the edge list of this file
      break;
    }
  }

  return rs;
}

Status LSMGraph::find_edge_by_levelindex(VertexId_t src, VertexId_t dst, FileId_t &fid, SequenceNumber_t &seq,
                                         SuperVersion &local_sv) {
  Status rs = Status::kNotFound;

  uint32_t fileID      = 0;
  uint32_t offset      = 0;
  uint32_t next_offset = 0;

  MulLevelIndex &findex = local_sv.findex;
  // FileId_t min_level_0_fid = 0;

  for (uint levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
    if (FLAGS_support_mulversion == false) {
      int         index_id = src * LEVEL_INDEX_SIZE + levelID;
      LevelIndex &findex   = vid_to_levelIndex_[index_id];
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

    fid = fileID;

    if (FLAGS_OPEN_SSTDATA_CACHE == true) {
      rs = find_edge_from_file_with_cache_by_directed_IO(src, dst, offset, next_offset, fileID, seq);
    } else {
      std::string    path = dataDir + "/" + std::to_string(fileID) + ".sst";
      std::ifstream *file;
      if (file_handle_cache_.find(path) != file_handle_cache_.end()) {
        file = file_handle_cache_[path];
      } else {
        file = new std::ifstream(path, std::ios::binary | std::ios::in);
        file_handle_cache_.emplace(path, file);
      }
      if (!file) {
        printf("Lost file: %s", path.c_str());
        exit(-1);
      }

      rs = find_edge_from_file_with_cache(dst, offset, next_offset, file, path, seq);
    }
    if (rs != Status::kNotFound) {  // not found in the edge list of this file
      break;
    }
  }

  return rs;
}

void LSMGraph::get_superversion(SuperVersion &local_sv) {
  std::shared_lock r_lock(sv_.vm_rw_mtx);
  local_sv.version_memtable = sv_.version_memtable;
}

void LSMGraph::get_superversion() {
  SequenceNumber_t v_id = global_version_id_.load(std::memory_order_relaxed);
  if (local_version_id_ < v_id) {
    local_version_id_ = v_id;
    {
      std::shared_lock r_lock(sv_.vm_rw_mtx);
      local_sv_.version_memtable = sv_.version_memtable;
    }
  }
}

/**
 * Returns the property of the given edge.
 * An empty string indicates not found.
 */
Status LSMGraph::get_edge(VertexId_t src, VertexId_t dst, std::string *property) {
  if (FLAGS_support_mulversion == true) {
    get_superversion(local_sv_);
  }

  Status rs = kNotFound;

  if (FLAGS_support_mulversion == false) {
    MemTable *mem = memTable_.load(std::memory_order_acquire);
    Status    rs  = mem->get(src, dst, property);
    if (rs != Status::kNotFound) {
      return rs;
    }
  } else {
    bool found = false;
    for (auto tb : local_sv_.version_memtable->menTables) {
      if (found == false) {
        rs = tb->get(src, dst, property);
        if (rs != Status::kNotFound) {
          found = true;
        }
      }
    }
    if (rs != Status::kNotFound) {
      local_sv_.version_memtable = nullptr;
      return rs;
    }
  }

  // When cannot find in memTable, try to find in SSTables.
  // level-0
  if (FLAGS_support_mulversion == false) {
    for (auto it = fileMetaCache[0]->begin(); it != fileMetaCache[0]->end(); ++it) {
      if (src <= ((*it)->header).maxKey && src >= ((*it)->header).minKey) {
        rs = find_edge(src, dst, property, *it);
        if (rs == Status::kNotFound) {  // not found in the edge list of this file
          continue;
        } else {
          return rs;
        }
      }
    }
  } else {
    {
      vertex_rwlocks_[src].ReadLock();
      MulLevelIndex temp = vid_to_mem_disk_index_[src].ToMulLevelIndex();
      memcpy(&local_sv_.findex, &temp, sizeof(MulLevelIndex));
      vertex_rwlocks_[src].ReadUnlock();
    }
    // fileID >= min_level_0_fid
    FileId_t min_level_0_fid = local_sv_.findex.get_min_level_0_fid();
    for (auto it : *(local_sv_.version_memtable->current_->GetLevel0Files())) {
      if (it->header.timeStamp >= min_level_0_fid && src <= it->header.maxKey && src >= it->header.minKey) {
        rs = find_edge(src, dst, property, it);
        if (rs == Status::kNotFound) {  // not found in the edge list of this file
          continue;
        } else {
          local_sv_.version_memtable = nullptr;
          return rs;
        }
      }
    }
  }

  // level>=1
  local_sv_.version_memtable = nullptr;
  rs                         = find_edge_by_levelindex(src, dst, property, local_sv_);
  return rs;
}

/**
 * Returns the fid and seq(eid) of the given edge.
 */
Status LSMGraph::get_edge(VertexId_t src, VertexId_t dst, FileId_t &fid, SequenceNumber_t &seq) {
  if (FLAGS_support_mulversion == true) {
    // get_superversion();
    get_superversion(local_sv_);
  }

  Status rs = kNotFound;

  if (FLAGS_support_mulversion == false) {
    MemTable *mem = memTable_.load(std::memory_order_acquire);
    Status    rs  = mem->get(src, dst, fid, seq);
    if (rs != Status::kNotFound) {
      return rs;
    }
  } else {
    bool found = false;
    for (auto tb : local_sv_.version_memtable->menTables) {
      if (found == false) {
        rs = tb->get(src, dst, fid, seq);
        if (rs != Status::kNotFound) {
          found = true;
        }
      }
    }
    if (rs != Status::kNotFound) {
      local_sv_.version_memtable = nullptr;
      return rs;
    }
  }

  // When cannot find in memTable, try to find in SSTables.
  // level-0
  if (FLAGS_support_mulversion == false) {
    for (auto it = fileMetaCache[0]->begin(); it != fileMetaCache[0]->end(); ++it) {
      if (src <= ((*it)->header).maxKey && src >= ((*it)->header).minKey) {
        fid = ((*it)->header).timeStamp;
        rs  = find_edge(src, dst, seq, *it);
        if (rs == Status::kNotFound) {  // not found in the edge list of this file
          continue;
        } else {
          return rs;
        }
      }
    }
  } else {
    {
      vertex_rwlocks_[src].ReadLock();
      MulLevelIndex temp = vid_to_mem_disk_index_[src].ToMulLevelIndex();
      memcpy(&local_sv_.findex, &temp, sizeof(MulLevelIndex));
      vertex_rwlocks_[src].ReadUnlock();
    }
    // fileID >= min_level_0_fid
    FileId_t min_level_0_fid = local_sv_.findex.get_min_level_0_fid();
    for (auto it : *(local_sv_.version_memtable->current_->GetLevel0Files())) {
      if (it->header.timeStamp >= min_level_0_fid && src <= it->header.maxKey && src >= it->header.minKey) {
        fid = it->header.timeStamp;
        rs  = find_edge(src, dst, seq, it);
        if (rs == Status::kNotFound) {  // not found in the edge list of this file
          continue;
        } else {
          local_sv_.version_memtable = nullptr;
          return rs;
        }
      }
    }
  }

  // level>=1
  local_sv_.version_memtable = nullptr;
  rs                         = find_edge_by_levelindex(src, dst, fid, seq, local_sv_);
  return rs;
}

/// Binary search for the target destination vertex in the body part of the edge file.
bool LSMGraph::find(VertexId_t target, uint32_t s_offset, uint32_t e_offset, uint32_t step_offset, std::ifstream &file,
                    uint32_t &obj_offset) {
  if (s_offset >= e_offset) return false;
  obj_offset = e_offset;
  uint32_t   mid_offset;
  VertexId_t temp_k;
  while (s_offset < e_offset) {
    mid_offset = s_offset + (e_offset - s_offset) / 2 / step_offset * step_offset;  // Guaranteed to round down
    file.seekg(mid_offset);
    file.read((char *)(&temp_k), sizeof(VertexId_t));
    if (temp_k == target) {
      e_offset = mid_offset;
    } else if (temp_k < target) {
      s_offset = mid_offset + step_offset;
    } else if (temp_k > target) {
      e_offset = mid_offset;
    }
  }
  // bound is left closed right open: [)
  if (s_offset >= obj_offset) return false;
  VertexId_t dis_ = 0;
  file.seekg(s_offset);
  file.read((char *)(&dis_), sizeof(VertexId_t));
  if (dis_ != target) return false;
  obj_offset = s_offset;
  return true;
}

Status LSMGraph::find_edge_from_sstdata_cache(VertexId_t src, VertexId_t dst, uint32_t s_offset, uint32_t e_offset,
                                              uint32_t fileID, SequenceNumber_t &seq) {
  Status        result   = Status::kNotFound;
  SSTDataCache *sstcache = sstdata_manager_.get_data(fileID);

  //-------------------------------------------
  if ((reinterpret_cast<SSTableCache *>(sstcache->GetSSTableCache()))->bloomFilter->contains(src, dst) == false) {
    return result;
  }

  uint32_t edge_body_size = sizeof(EdgeBody_t);
  uint32_t adj_size       = (e_offset - s_offset) / edge_body_size;
  char    *body_buffer    = sstcache->GetEdgeData();
  uint     low            = s_offset;
  uint     high           = e_offset;

  if (adj_size < BINARY_THRESHOLD_FIND_EDGE) {
    for (; low < high; low += edge_body_size) {
      EdgeBody_t *body = reinterpret_cast<EdgeBody_t *>(body_buffer + low);
      if (body->get_dst() == dst) {
        if (!body->get_marker()) {
          seq    = body->get_seq();
          result = Status::kOk;
        } else {
          result = Status::kDelete;
        }
        break;
      }
    }
  } else {
    uint mid = 0;
    while (low < high) {
      // mid = (low + high) >> 1;
      mid = low + (high - low) / 2 / edge_body_size * edge_body_size;
      if (reinterpret_cast<EdgeBody_t *>(body_buffer + mid)->get_dst() >= dst)
        high = mid;
      else {
        low = mid + edge_body_size;
      }
    }
    if (low < e_offset) {
      EdgeBody_t *body = reinterpret_cast<EdgeBody_t *>(body_buffer + low);
      if (body->get_dst() == dst) {
        if (!body->get_marker()) {
          seq    = body->get_seq();
          result = Status::kOk;
        } else {
          result = Status::kDelete;
        }
      }
    }
  }

  return result;
}

Status LSMGraph::find_edge_from_sstdata_cache(VertexId_t src, VertexId_t dst, uint32_t s_offset, uint32_t e_offset,
                                              uint32_t fileID, std::string *property) {
  Status        result   = Status::kNotFound;
  SSTDataCache *sstcache = sstdata_manager_.get_data(fileID);

  //-------------------------------------------
  if ((reinterpret_cast<SSTableCache *>(sstcache->GetSSTableCache()))->bloomFilter->contains(src, dst) == false) {
    return result;
  }
  //-------------------------------------------

  uint32_t edge_body_size = sizeof(EdgeBody_t);
  uint32_t adj_size       = (e_offset - s_offset) / edge_body_size;
  char    *body_buffer    = sstcache->GetEdgeData();
  uint     low            = s_offset;
  uint     high           = e_offset;

  if (adj_size < BINARY_THRESHOLD_FIND_EDGE) {
    for (; low < high; low += edge_body_size) {
      EdgeBody_t *body = reinterpret_cast<EdgeBody_t *>(body_buffer + low);
      if (body->get_dst() == dst) {
        if (!body->get_marker()) {
          result = Status::kOk;
        } else {
          result = Status::kDelete;
        }
        break;
      }
    }
  } else {
    uint mid = 0;
    while (low < high) {
      // mid = (low + high) >> 1;
      mid = low + (high - low) / 2 / edge_body_size * edge_body_size;
      if (reinterpret_cast<EdgeBody_t *>(body_buffer + mid)->get_dst() >= dst)
        high = mid;
      else {
        low = mid + edge_body_size;
      }
    }
    if (low < e_offset) {
      EdgeBody_t *body = reinterpret_cast<EdgeBody_t *>(body_buffer + low);
      if (body->get_dst() == dst) {
        if (!body->get_marker()) {
          result = Status::kOk;
        } else {
          result = Status::kDelete;
        }
      }
    }
  }
  // load property
  if (result == Status::kOk) {
    char *property_buffer = sstcache->GetPropertyData();

    EdgePropertyOffset_t property_offset = reinterpret_cast<EdgeBody_t *>(body_buffer + low)->get_prop_pointer();
    uint32_t             length =
        reinterpret_cast<EdgeBody_t *>(body_buffer + low + edge_body_size)->get_prop_pointer() - property_offset;
    assert(length < 1024);
    if (length > 0) {
      property->assign(property_buffer + property_offset, length);
    }
  }

  return result;
}

Status LSMGraph::find_edges_from_sstdata_cache(uint32_t s_offset, uint32_t e_offset, uint32_t fileID,
                                               std::vector<Edge> &edges) {
  Status        result   = Status::kNotFound;
  SSTDataCache *sstcache = sstdata_manager_.get_data(fileID);
  assert(sstcache != nullptr);
  uint32_t edge_body_size = sizeof(EdgeBody_t);

  uint32_t adj_size    = (e_offset - s_offset) / edge_body_size;
  char    *body_buffer = sstcache->GetEdgeData();
  int      low         = s_offset;
  int      high        = e_offset;

  // e1, e2, e3, e4,
  VertexId_t saved_key = INVALID_VERTEX_ID;
  bool       skipping  = false;
  edges.reserve(edges.size() + adj_size);
  for (; low < high; low += edge_body_size) {
    EdgeBody_t *body = reinterpret_cast<EdgeBody_t *>(body_buffer + low);
    // body->print();
    if (!(skipping && body->get_dst() == saved_key)) {
      if (body->get_marker() == 0) {
        edges.emplace_back(*body);
      } else {
        saved_key = body->get_dst();
        skipping  = true;
      }
    }
  }

  return result;
}

Status LSMGraph::find_edge_from_file_with_cache(VertexId_t target, uint32_t s_offset, uint32_t e_offset,
                                                std::ifstream *file, std::string &path, std::string *property) {
  if (s_offset >= e_offset) return Status::kNotFound;
  uint32_t edge_body_size = sizeof(EdgeBody_t);

  // load whole adjlist of the target node
  uint32_t    adj_size    = (e_offset - s_offset) / edge_body_size;
  EdgeBody_t *body_buffer = new EdgeBody_t[adj_size + 1];
  auto        need_bytes  = (adj_size + 1) * edge_body_size;
  file->seekg(s_offset);
  auto read_bytes = file->readsome((char *)body_buffer, need_bytes);  // cache all adj edges of the target node
  assert(need_bytes == read_bytes);

  Status result = Status::kNotFound;
  uint   low    = 0;

  if (adj_size < BINARY_THRESHOLD_FIND_EDGE) {
    for (low = 0; low < adj_size; low++) {
      if (body_buffer[low].get_dst() == target) {
        if (!body_buffer[low].get_marker()) {
          result = Status::kOk;
        } else {
          result = Status::kDelete;
        }
        break;
      }
    }
  } else {
    uint high = adj_size;
    uint mid  = 0;
    while (low < high) {
      mid = (low + high) / 2;
      if (body_buffer[mid].get_dst() >= target)
        high = mid;
      else {
        low = mid + 1;
      }
    }
    if (low < adj_size && body_buffer[low].get_dst() == target) {
      if (!body_buffer[low].get_marker()) {
        result = Status::kOk;
      } else {
        result = Status::kDelete;
      }
    }
  }

  // load property
  if (result == Status::kOk) {
    std::ifstream *p_file;
    std::string    p_path = path + +"_p";
    if (file_handle_cache_.find(p_path) != file_handle_cache_.end()) {
      p_file = file_handle_cache_[p_path];
    } else {
      p_file = new std::ifstream(p_path, std::ios::binary | std::ios::in);
      file_handle_cache_.emplace(p_path, p_file);
    }

    EdgePropertyOffset_t property_offset = body_buffer[low].get_prop_pointer();
    uint32_t             length          = body_buffer[low + 1].get_prop_pointer() - property_offset;
    assert(length < 1024);
    if (length > 0) {
      property->resize(length);
      p_file->seekg(property_offset);
      p_file->read(&(*property)[0], length);
    }
  }

  delete[] body_buffer;
  return result;
}

Status LSMGraph::find_edge_from_file_with_cache(VertexId_t target, uint32_t s_offset, uint32_t e_offset,
                                                std::ifstream *file, std::string &path, SequenceNumber_t &seq) {
  return Status::kNotFound;
  uint32_t edge_body_size = sizeof(EdgeBody_t);

  // load whole adjlist of the target node
  uint32_t    adj_size    = (e_offset - s_offset) / edge_body_size;
  EdgeBody_t *body_buffer = new EdgeBody_t[adj_size + 1];
  auto        need_bytes  = (adj_size + 1) * edge_body_size;
  file->seekg(s_offset);
  auto read_bytes = file->readsome((char *)body_buffer, need_bytes);  // cache all adj edges of the target node
  assert(need_bytes == read_bytes);

  Status   result = Status::kNotFound;
  uint32_t low    = 0;

  if (adj_size < BINARY_THRESHOLD_FIND_EDGE) {
    for (low = 0; low < adj_size; low++) {
      if (body_buffer[low].get_dst() == target) {
        if (!body_buffer[low].get_marker()) {
          seq    = body_buffer[low].get_seq();
          result = Status::kOk;
        } else {
          result = Status::kDelete;
        }
        break;
      }
    }
  } else {
    uint32_t high = adj_size;
    uint32_t mid  = 0;
    while (low < high) {
      mid = (low + high) / 2;
      if (body_buffer[mid].get_dst() >= target)
        high = mid;
      else {
        low = mid + 1;
      }
    }
    if (low < adj_size && body_buffer[low].get_dst() == target) {
      if (!body_buffer[low].get_marker()) {
        seq    = body_buffer[low].get_seq();
        result = Status::kOk;
      } else {
        result = Status::kDelete;
      }
    }
  }

  delete[] body_buffer;
  return result;
}

Status LSMGraph::find_edge_from_file_with_cache_by_directed_IO(VertexId_t src, VertexId_t dst, uint32_t s_offset,
                                                               uint32_t e_offset, uint32_t fileID,
                                                               SequenceNumber_t &seq) {
  if (s_offset >= e_offset) return Status::kNotFound;
  Status        result   = Status::kNotFound;
  SSTDataCache *sstcache = sstdata_manager_.get_data(fileID);

  // use filter to check if the target is in the file
  if ((reinterpret_cast<SSTableCache *>(sstcache->GetSSTableCache()))->bloomFilter->contains(src, dst) == false) {
    return result;
  }
  //-------------------------------------------

  int fd = sstcache->GetEFileFD();
  if (fd == -1) {
    throw std::runtime_error("read file not exit: fileID=" + std::to_string(fileID)
                             + " offset=" + std::to_string(s_offset) + " " + std::to_string(e_offset));
  }

  uint32_t edge_body_size = sizeof(EdgeBody_t);
  // load whole adjlist of the target node
  uint32_t    adj_size    = (e_offset - s_offset) / edge_body_size;
  EdgeBody_t *body_buffer = new EdgeBody_t[adj_size + 1];
  auto        need_bytes  = (adj_size + 1) * edge_body_size;

  auto read_bytes = pread(fd, body_buffer, need_bytes, s_offset);
  // assert(read_bytes == need_bytes);
  if (read_bytes != need_bytes) {
    throw std::runtime_error("read data size error: fileID=" + std::to_string(fileID) + " read_bytes="
                             + std::to_string(read_bytes) + " need_bytes=" + std::to_string(need_bytes));
  }

  uint low = 0;

  if (adj_size < BINARY_THRESHOLD_FIND_EDGE) {
    for (low = 0; low < adj_size; low++) {
      if (body_buffer[low].get_dst() == dst) {
        if (!body_buffer[low].get_marker()) {
          seq    = body_buffer[low].get_seq();
          result = Status::kOk;
        } else {
          result = Status::kDelete;
        }
        break;
      }
    }
  } else {
    uint high = adj_size;
    uint mid  = 0;
    while (low < high) {
      mid = (low + high) / 2;
      if (body_buffer[mid].get_dst() >= dst)
        high = mid;
      else {
        low = mid + 1;
      }
    }
    if (low < adj_size && body_buffer[low].get_dst() == dst) {
      if (!body_buffer[low].get_marker()) {
        seq    = body_buffer[low].get_seq();
        result = Status::kOk;
      } else {
        result = Status::kDelete;
      }
    }
  }

  delete[] body_buffer;
  return result;
}

Status LSMGraph::find_edges_from_file_with_cache(uint32_t s_offset, uint32_t e_offset, std::ifstream *file,
                                                 SSTableCache *sst, std::vector<Edge> &edges) {
  if (s_offset >= e_offset) return Status::kNotFound;
  uint32_t edge_body_size       = sizeof(EdgeBody_t);
  uint32_t last_edge_end_offset = edge_body_size * sst->header.size;
  bool     not_file_end         = (e_offset < last_edge_end_offset);

  // load whole adjlist of the target node
  uint32_t    adj_size    = (e_offset - s_offset) / edge_body_size;
  EdgeBody_t *body_buffer = new EdgeBody_t[adj_size + not_file_end];
  auto        need_bytes  = (adj_size + not_file_end) * edge_body_size;
  file->seekg(s_offset);
  auto read_bytes = file->readsome((char *)body_buffer, need_bytes);  // cache all adj edges of the target node
  assert(need_bytes == read_bytes);

  for (auto &e : edges) {
    e.print();
  }

  return Status::kOk;
}

/**
 * Returns an array of all outgoing edges of a vertex.
 */
bool LSMGraph::get_edges(VertexId_t src, std::vector<Edge> &edges) {
  MemTable *mem = memTable_.load(std::memory_order_acquire);
  mem->get_edges(src, edges);

  int levelNum = fileMetaCache.size();
  for (int i = 0; i < levelNum; ++i) {
    // find keys in SSTableCaches, from the one with biggest timestamp
    for (auto it = fileMetaCache[i]->begin(); it != fileMetaCache[i]->end(); ++it) {
      // check if the src is in the range of the sstablecache
      if (src <= ((*it)->header).maxKey && src >= ((*it)->header).minKey) {
        int pos = (*it)->get(src);
        if (pos < 0 && i == 0) {
          continue;
        } else if (pos < 0) {
          break;
        }

        uint32_t offset      = ((*it)->indexes)[pos].offset;
        uint32_t next_offset = ((*it)->indexes)[pos + 1].offset;

        if (FLAGS_OPEN_SSTDATA_CACHE == true) {
          find_edges_from_sstdata_cache(offset, next_offset, (*it)->header.timeStamp, edges);
        } else {
          std::ifstream *file;
          if (file_handle_cache_.find((*it)->path) != file_handle_cache_.end()) {
            file = file_handle_cache_[(*it)->path];
          } else {
            file = new std::ifstream((*it)->path, std::ios::binary | std::ios::in);
            file_handle_cache_.emplace((*it)->path, file);
          }
          if (!file) {
            printf("Lost file: %s", ((*it)->path).c_str());
            exit(-1);
          }

          find_edges_from_file_with_cache(offset, next_offset, file, *it, edges);
        }

        if (it + 1 != fileMetaCache[i]->end()
            && src == ((*(it + 1))->header).minKey) {  // The edges of the large vertex may be squeezed to the next sst.
          continue;
        } else {
          break;
        }
      }
    }
  }
  return true;
}

EdgeIterator LSMGraph::get_edges(VertexId_t src, const SequenceNumber_t seq, bool use_disk_index) {
  MemTable *mem       = nullptr;
  int       max_level = -1;
  if (FLAGS_support_mulversion == true) {
    {
      get_superversion(local_sv_);
      vertex_rwlocks_[src].ReadLock();
      MulLevelIndex temp = vid_to_mem_disk_index_[src].ToMulLevelIndex();
      memcpy(&local_sv_.findex, &temp, sizeof(MulLevelIndex));
      vertex_rwlocks_[src].ReadUnlock();
      max_level = vertex_max_level_[src];
    }
  } else {
    mem = memTable_.load(std::memory_order_acquire);
  }

  return EdgeIterator{
      src, mem, fileMetaCache, sstdata_manager_, vid_to_levelIndex_, &local_sv_, &del_record_manager_, max_level, seq};
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
Status LSMGraph::del_edge(VertexId_t src, VertexId_t dis) {
  std::string res_property;
  Status      res = get_edge(src, dis, &res_property);
  if (res != Status::kOk) {
    return res;
  }
  res_property = "~DELETED~";
  put_edge(src, dis, res_property);
  return Status::kOk;
}

// Insert and delete edges separately
Status LSMGraph::del_edge_sep(VertexId_t src, VertexId_t dis) {
  FileId_t         fid = INVALID_File_ID;
  SequenceNumber_t eid = MAX_SEQ_ID;
  // get eid, fid
  Status res = get_edge(src, dis, fid, eid);
  if (res != Status::kOk) {
    return res;
  }

  SequenceNumber_t del_time = automic_get_global_seq(1);

  del_record_manager_.put_fid_and_record(fid, eid, del_time);

  return Status::kOk;
}

void LSMGraph::print_memTable(std::string label) {
  MemTable *mem = memTable_.load(std::memory_order_acquire);
  mem->print(label);
}

SequenceNumber_t LSMGraph::automic_get_global_seq(SequenceNumber_t num) {
  SequenceNumber_t temp_global_seq = __sync_fetch_and_add(&global_seq, num);
  return temp_global_seq;
}

SequenceNumber_t LSMGraph::LastSequence() {
  return global_seq;
}

void LSMGraph::SetLastSequence(SequenceNumber_t last_sequence) {
  global_seq = last_sequence;
}

MemTable *LSMGraph::get_newmemTable() {
  while (true) {
    if (!free_menTables.empty()) {
      std::unique_lock<std::mutex> lk(memtable_mux_);
      auto                         table = free_menTables.front();
      if (FLAGS_support_mulversion == true) {
        if ((table->IsFlash() == true || table->IsLive() == false) && table->Getref() == 0) {
          table->reset();
          free_menTables.pop();
          return table;
        }
      } else {
        table->reset();
        free_menTables.pop();
        return table;
      }
    }
    Sleep(0.0000001);
  }
}

void LSMGraph::recycle_memTable(MemTable *table) {
  {
    std::unique_lock<std::mutex> lk(memtable_mux_);
    free_menTables.push(table);
  }
}

void LSMGraph::check_vertex_id(VertexId_t vertex_id) {
  if (vertex_id >= vertex_id_.load(std::memory_order_relaxed)) throw std::invalid_argument("The vertex id is invalid.");
}

// open
void LSMGraph::open(const std::string &dir, size_t max_vertex_num, LSMGraph **db) {
  max_vertex_num += 10;
  *db = new LSMGraph(dir, max_vertex_num, 1, FLAGS_memtable_num);
}

void LSMGraph::valid_index(uint max_vertex_id) {
  for (uint i = 0; i < max_vertex_id; ++i) {
    auto &multi_level    = vid_to_mullevelIndex_[i];
    auto &mem_disk_index = vid_to_mem_disk_index_[i];
    auto  temp           = mem_disk_index.ToMulLevelIndex();
    if (!(temp == multi_level)) {
      LOG_ERROR("unmatch index");
      LOG_ERROR("i={}", i);
      temp.print();
      multi_level.print();
      std::terminate();
    }
  }
}

void LSMGraph::load_sstdatacache() {
  LOG_INFO("build sst_data_cache...");

  if (FLAGS_support_mulversion == true) {
    // update version
    l0_versionset_->VersionLock();
    fileMetaCache[0] = l0_versionset_->GetCurrent()->GetLevel0Files();
    l0_versionset_->VersionUnLock();
  }

  int levelNum = fileMetaCache.size();
  int file_num = 0;
  for (int i = 0; i < levelNum; ++i) {
    for (auto it = fileMetaCache[i]->begin(); it != fileMetaCache[i]->end(); ++it) {
      SSTDataCache *sst =
          new SSTDataCache((*it)->path, ((*it)->header).size, (*it)->path + "_p", reinterpret_cast<uintptr_t>(*it));
      sstdata_manager_.put_data((*it)->header.timeStamp, sst);
      ++file_num;
    }
  }
  LOG_INFO("  levelNum={} file_num={}", levelNum, file_num);
}

void LSMGraph::static_edge_distribution() {
  LOG_INFO("-------------------------------------------------");
  // in file
  std::vector<int> filenum_of_vertex;
  std::vector<int> vertex_num_of_level(MAX_LEVEL + 2, 0);
  int              memtable_vertex_num = 0;
  MemTable        *mem                 = memTable_.load(std::memory_order_acquire);
  for (VertexId_t vid = 0; vid < vertex_id_.load(std::memory_order_relaxed); vid++) {
    uint cnt = 0;
    // memtable
    if (mem->get_vertex_adj(vid) != nullptr) {
      memtable_vertex_num++;
      cnt++;
      vertex_num_of_level[0]++;
    }
    // level-0
    for (auto it = fileMetaCache[0]->begin(); it != fileMetaCache[0]->end(); ++it) {
      if (vid <= ((*it)->header).maxKey && vid >= ((*it)->header).minKey) {
        int rs = (*it)->find(vid, 0, (*it)->indexes.size() - 1);
        if (rs != -1) {
          cnt++;
          vertex_num_of_level[1]++;
        }
      }
    }
    // level>=1
    if (FLAGS_support_mulversion == false) {
      for (uint levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
        int         index_id = vid * LEVEL_INDEX_SIZE + levelID;
        LevelIndex &findex   = vid_to_levelIndex_[index_id];
        FileId_t    fileID   = findex.get_fileID();
        if (fileID == INVALID_File_ID) {
          continue;
        }
        cnt++;
        vertex_num_of_level[1 + levelID + 1]++;
      }
    } else {
      auto &multi_index = vid_to_mem_disk_index_[vid];
      for (uint levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
        auto index = multi_index.Get(levelID);
        if (index == nullptr) {
          continue;
        }
        cnt++;
        vertex_num_of_level[1 + levelID + 1]++;
      }
    }
    if (cnt >= filenum_of_vertex.size()) {
      filenum_of_vertex.resize(cnt + 1);
    }
    filenum_of_vertex[cnt]++;
  }
  LOG_INFO(" file num of each vertex:");
  for (uint i = 0; i < filenum_of_vertex.size(); i++) {
    LOG_INFO("   file_num={} vertex_num={} rate={}", i, filenum_of_vertex[i],
             (filenum_of_vertex[i] * 1.0 / vertex_id_.load(std::memory_order_relaxed)));
  }
  LOG_INFO("   memtable/all_vertex={}", (memtable_vertex_num * 1.0 / vertex_id_.load(std::memory_order_relaxed)));

  LOG_INFO("\n vertex num of each level: (-1 is memtable):");
  for (uint i = 0; i < vertex_num_of_level.size(); i++) {
    LOG_INFO("   level={} vertex_num={} rate={}", (i - 1), vertex_num_of_level[i],
             (vertex_num_of_level[i] * 1.0 / vertex_id_.load(std::memory_order_relaxed)));
  }

  // count max level
  {
    LOG_INFO("\n max_level info of each vertex:");
    std::vector<VertexId_t> cnt_max_level(MAX_LEVEL + 1, 0);
    for (VertexId_t i = 0; i < vertex_id_; i++) {
      cnt_max_level[vertex_max_level_[i] + 1]++;
    }
    for (uint i = 0; i < MAX_LEVEL + 1; i++) {
      LOG_INFO("   max_level={} num={} rate={}", (i - 1), cnt_max_level[i],
               cnt_max_level[i] * 1.0 / vertex_id_.load(std::memory_order_relaxed));
    }
  }

  LOG_INFO("-------------------------------------------------");
}

void LSMGraph::printSpaceInfo() {
  LOG_INFO("vertex_id_={}", vertex_id_.load());

  if (FLAGS_support_mulversion == false) {
    int real_vector_size = vertex_id_.load() * LEVEL_INDEX_SIZE;
    printf(" @levelIndex space: %.2fGB\n", (real_vector_size * sizeof(LevelIndex)) / 1024.0 / 1024 / 1024);
  } else {
    int real_vector_size = vertex_id_.load();
    printf(" @levelIndex space: %.2fGB\n", (real_vector_size * sizeof(MulLevelIndex)) / 1024.0 / 1024 / 1024);
  }

  printf(" @Futex space: %0.2fGB\n", vertex_id_.load() * sizeof(Futex) / 1024.0 / 1024 / 1024);
  printf(" @RWLock_t space: %0.2fGB\n", vertex_id_.load() * sizeof(RWLock_t) / 1024.0 / 1024 / 1024);
  LOG_INFO(" @sizeof(Edge): {}", sizeof(Edge));
  LOG_INFO(" @sizeof(MulLevelIndex): {}", sizeof(MulLevelIndex));
  LOG_INFO(" @MAX_LEVEL: {}", MAX_LEVEL);
  LOG_INFO(" @NeighBors: {}", typeid(NeighBors).name());
  LOG_INFO(" @EdgeIterator: {}", typeid(EdgeIterator).name());
  LOG_INFO(" @MULTI_LEVEL: close");
  LOG_INFO(" @USE_BLOOMFILTER: open");
  LOG_INFO(" @USE_DIRECTEDIO: open");

  {
    LOG_INFO("-------------------- start count mem cost------------------");
    double     all_mem_cost   = 0;
    VertexId_t max_vertex_num = vertex_id_.load() + 1;

    // memtable
    double memtable_cost   = 0;
    size_t max_edge_num    = (MAX_TABLE_SIZE - HEADER_SIZE - BLOOM_FILTER_SIZE) / sizeof(EdgeBody_t);
    size_t skiplist        = sizeof(NeighBors) + (sizeof(Edge) + 8) * FLAGS_reserve_node;  // reserve-node
    size_t a_memtable_size = max_edge_num * skiplist                                       // all skiplist
                             + max_vertex_num * 8;
    memtable_cost = a_memtable_size * memTable_list_.size();

    // cache
    double fileMateCache_cost = 0;
    double level_0            = max_edge_num * sizeof(Index);  // index
    fileMateCache_cost        = level_0 * memTable_list_.size();

    // compaction
    double compact_cost = 1280 * (1 << 20);

    // lock
    double lock_cost = 0;
    lock_cost        = max_vertex_num * sizeof(Futex) + max_vertex_num * sizeof(RWLock_t);

    // level_index
    double level_index_cost = 0;
    level_index_cost        = max_vertex_num * sizeof(MulLevelIndex);

    all_mem_cost = memtable_cost + fileMateCache_cost + compact_cost + lock_cost + level_index_cost;
    printf(" @all_mem_cost: %0.3fGB\n", all_mem_cost / 1024.0 / 1024 / 1024);
    printf("  memtable_cost: %0.3fGB\n", memtable_cost / 1024.0 / 1024 / 1024);
    printf("  fileMateCache_cost: %0.3fGB\n", fileMateCache_cost / 1024.0 / 1024 / 1024);
    printf("  compact_cost: %0.3fGB\n", compact_cost / 1024.0 / 1024 / 1024);
    printf("  lock_cost: %0.3fGB\n", lock_cost / 1024.0 / 1024 / 1024);
    printf("  level_index real cost: %0.3fGB\n", level_index_cost / 1024.0 / 1024 / 1024);
    LOG_INFO("-------------------- end count mem cost--------------------");
  }
}

void LSMGraph::printLevelInfo() {
  // save file information of every level
  {
    LOG_INFO("----------------- information of every level---------------");
    if (FLAGS_support_mulversion == true) {
      LOG_INFO("\ncurr_level0_version:");
      int file_num = 0;
      for (auto filemeta : *l0_versionset_->GetCurrent()->GetLevel0Files()) {
        LOG_INFO(" level={} minkey={} maxkey={} fid={} ref={}", 0, filemeta->header.minKey, filemeta->header.maxKey,
                 filemeta->header.timeStamp, filemeta->Getref());
        ++file_num;
      }
      LOG_INFO(" level_id={} file_num={}", 0, file_num);
    }
  }
}

// return max vertex_id actually used
VertexId_t LSMGraph::get_max_vertex_num() {
  LOG_INFO("vertex_id_={}", vertex_id_.load(std::memory_order_relaxed));
  return vertex_id_.load(std::memory_order_relaxed);
}

bool LSMGraph::GetCompactionState() {
  return compactor_.GetState();
}

void LSMGraph::print_all_file_info() {
  int levelNum = fileMetaCache.size();
  int file_num = 0;
  for (int i = 0; i < levelNum; ++i) {
    LOG_INFO(" level_id={} file_num={}", i, fileMetaCache[i]->size());
    for (auto it = fileMetaCache[i]->begin(); it != fileMetaCache[i]->end(); ++it) {
      LOG_INFO("  level={} minkey={} maxkey={} fid={}", i, (*it)->header.minKey, (*it)->header.maxKey,
               (*it)->header.timeStamp);
      ++file_num;
    }
  }
  LOG_INFO("@file_num: {}", file_num);
}

SequenceNumber_t LSMGraph::get_sequence() const {
  MemTable *old_mem = memTable_.load(std::memory_order_acquire);
  return (old_mem->GetStartTime() + old_mem->GetMaxEdgeNum() - old_mem->remain_capacity + 1);
}

void LSMGraph::debug() {}

void LSMGraph::PrintSSTable() {
  int file_num = 0;
  for (uint i = 0; i < fileMetaCache.size(); ++i) {
    int level_file_num = fileMetaCache[i]->size();
    for (auto it = fileMetaCache[i]->begin(); it != fileMetaCache[i]->end(); ++it) {
      LOG_INFO("  level={} minkey={} maxkey={} fid={} ref={} index_num={}", i, (*it)->header.minKey,
               (*it)->header.maxKey, (*it)->header.timeStamp, (*it)->Getref(), (*it)->header.index_size);
    }
    file_num += level_file_num;
    LOG_INFO(" level_id={} file_num={}", i, fileMetaCache[i]->size());
  }
  LOG_INFO("@file_num: {}", file_num);
}

void LSMGraph::AwaitWrite() {
  if (CheckState()) {
    // busy loop
    for (uint32_t tries = 0; tries < 120; ++tries) {
      if (!CheckState()) {
        return;
      }
      asm volatile("pause");
    }
  }
}

bool LSMGraph::CheckState() const {
  return fileMetaCache[0]->size() >= 2;
}

}  // namespace lsmg