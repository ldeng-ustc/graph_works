#include <string.h>
#include <tbb/tbb.h>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "cache/mem_table.h"
#include "common/utils.h"
#include "common/utils/atomic.h"
#include "common/utils/file.h"
#include "index/vary_size_bloom_filter.h"

namespace lsmg {

void MemTable::put_edge(VertexId_t src, VertexId_t dst, const EdgeProperty_t &s, Marker_t marker, SequenceNumber_t seq,
                        size_t id) {
  vertex_futexes_[src].lock();
  if (vertex_adjs[src] == NULLPOINTER) {
    NeighBors *tempEdge_ = edge_arena.GetAnElementById(id);
    tempEdge_->put_edge(dst, seq, marker, s);
    vertex_adjs[src] = reinterpret_cast<uintptr_t>(tempEdge_);
    vertex_futexes_[src].unlock();
    __sync_fetch_and_add(&src_vertex_num, 1);
    write_max(&vertex_max_level_[src], Level_t(0));
  } else {
    (reinterpret_cast<NeighBors *>(vertex_adjs[src]))->put_edge(dst, seq, marker, s);
    vertex_futexes_[src].unlock();
  }

  __sync_fetch_and_add(&listLength, 1);
  assert(listLength <= max_edge_num);  // An error will occur if MemTable's number of edges exceeds this number!
}

// Status MemTable::get(Edge& edge, std::string* property){
Status MemTable::get(VertexId_t src, VertexId_t dst, std::string *property) {
  auto prt = vertex_adjs[src];
  if (prt == NULLPOINTER) {
    return Status::kNotFound;
  } else {
    Status temp = (reinterpret_cast<NeighBors *>(prt))->get(dst, property);
    return temp;
  }
}

// Status MemTable::get(Edge& edge, std::string* property){
Status MemTable::get(VertexId_t src, VertexId_t dst, FileId_t &fid, SequenceNumber_t &seq) {
  auto prt = vertex_adjs[src];
  if (prt == NULLPOINTER) {
    return Status::kNotFound;
  } else {
    fid         = this->fid_;
    Status temp = (reinterpret_cast<NeighBors *>(prt))->get(dst, seq);
    return temp;
  }
}

void MemTable::get_edges(VertexId_t src, std::vector<Edge> &edges) {
  if (vertex_adjs[src] != NULLPOINTER) {
    (reinterpret_cast<NeighBors *>(vertex_adjs[src]))->get_edges(src, edges);
  }
}

void MemTable::save2eSSTable(const std::string &dir, uint64_t &currentTime,
                             std::vector<SSTableCache *> *level_0_cache) {
  // LOG_INFO("start save sst to disk");
  // select method of save2esst
  // select_1:
  // save2eSSTable_split_property(dir, currentTime, level_0_cache);

  // select_2:
  save2eSSTable_split(dir, currentTime, level_0_cache);

  // LOG_INFO("finish save sst to disk.");
}

void MemTable::save2eSSTable_split_property(const std::string &dir, uint64_t &currentTime,
                                            std::vector<SSTableCache *> *level_0_cache) {
  uint64_t temp_currentTime = this->GetFid();

  uint64_t listLength = 0;
  if (remain_capacity > 0) {
    listLength = GetMaxEdgeNum() - remain_capacity;
  } else {
    listLength = GetMaxEdgeNum();
    checkIsFinishWrite();
  }

  size_t write_all_edge_num = listLength;

  // build cache and buffers
  SSTableCache *cache  = new SSTableCache(sstdata_manager_);
  BloomFilter  *filter = cache->bloomFilter;
  cache->indexes.resize(src_vertex_num);

  BloomFilterVarySize *bloomFilterVarySize = new BloomFilterVarySize();
  cache->bloomFilterVarySize               = bloomFilterVarySize;

  uint32_t EdgeBody_size   = sizeof(EdgeBody_t);
  uint32_t index_pair_size = (sizeof(VertexId_t) + 4);
  uint32_t sizeof_vid      = sizeof(VertexId_t);

  // setting of parallel build sstable
  uint                      chunk_num  = FLAGS_max_subcompactions;
  uint                      chunk_size = write_all_edge_num / chunk_num;
  std::vector<int>          chunk_src_ids;
  std::vector<EdgeOffset_t> chunk_edge_offset;
  std::vector<EdgeOffset_t> chunk_index_offset;
  std::vector<EdgeOffset_t> chunk_property_offset;

  chunk_src_ids.reserve(chunk_num + 1);
  chunk_edge_offset.reserve(chunk_num);
  chunk_index_offset.reserve(chunk_num);
  chunk_property_offset.reserve(chunk_num);
  chunk_src_ids.push_back(0);
  chunk_edge_offset.push_back(0);
  chunk_index_offset.push_back(0);
  chunk_property_offset.push_back(0);

  // get all source nodes
  VertexId_t  i                 = 0;
  size_t      edge_num          = 0;
  size_t      edge_num_sum      = 0;
  size_t      property_size_sum = 0;
  VertexId_t *vertex_set        = new VertexId_t[src_vertex_num];

  VertexId_t temp_max_v_num = vertex_id_.load(std::memory_order_relaxed);

  // std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
  for (VertexId_t id = 0; id < temp_max_v_num; ++id) {
    auto item = vertex_adjs[id];
    if (item != NULLPOINTER) {
      auto   adjs    = reinterpret_cast<NeighBors *>(item);
      size_t adj_num = adjs->getEdgeNum();
      if (edge_num + adj_num > chunk_size) {
        edge_num_sum += edge_num;
        chunk_src_ids.push_back(i);
        chunk_edge_offset.push_back(edge_num_sum * EdgeBody_size);
        chunk_index_offset.push_back(i * index_pair_size);
        chunk_property_offset.push_back(property_size_sum);
        edge_num = 0;
      }
      edge_num += adj_num;
      property_size_sum += adjs->getPropertySize();
      vertex_set[i++] = id;
      if (i >= src_vertex_num) {
        break;
      }
    }
  }

  chunk_src_ids.push_back(i);
  edge_num_sum += edge_num;

  if (!(i == src_vertex_num && i > 0)) {
    LOG_ERROR("i={} src_vertex_num={}", i, src_vertex_num);
    exit(0);
  }
  if (edge_num_sum != listLength) {
    LOG_ERROR("edge_num_sum={} listLength={}", edge_num_sum, listLength);
    exit(0);
  }

  // uint64_t property_size = property_buffer.GetUsedSize();
  uint64_t property_size   = property_size_sum;
  uint64_t real_efile_size = HEADER_SIZE + BLOOM_FILTER_SIZE + EdgeBody_size * (write_all_edge_num + 1)
                             + (src_vertex_num + 1) * index_pair_size;
  char *efile_buffer = new char[real_efile_size];
  char *pfile_buffer = new char[property_size];

  // write head
  (cache->header).timeStamp  = temp_currentTime;
  (cache->header).size       = write_all_edge_num + 1;  // Add an end flag
  (cache->header).index_size = src_vertex_num + 1;      // Add an end flag
  (cache->header).minKey     = vertex_set[0];
  (cache->header).maxKey     = vertex_set[i - 1];

  cache->seq_ = GetStartTime();

  char        *index           = efile_buffer + sizeof(EdgeBody_t) * (write_all_edge_num + 1);
  EdgeOffset_t body_offset     = 0;
  char        *body            = efile_buffer + body_offset;
  EdgeOffset_t property_offset = 0;

  auto build_sstable = [&](uint chunk_id, BloomFilter *filter, BloomFilterVarySize *bloomFilterVarySize) {
    char        *temp_index           = index + chunk_index_offset[chunk_id];
    char        *temp_body            = body + chunk_edge_offset[chunk_id];
    EdgeOffset_t temp_body_offset     = body_offset + chunk_edge_offset[chunk_id];
    EdgeOffset_t temp_property_offset = chunk_property_offset[chunk_id];
    assert(chunk_src_ids.size() > chunk_id + 1);
    for (int v_index = chunk_src_ids[chunk_id]; v_index < chunk_src_ids[chunk_id + 1]; v_index++) {
      VertexId_t cur_vid = vertex_set[v_index];
      auto       mem_ptr = this->get_vertex_adj(cur_vid);
      mem_ptr->sort();
      NeighBors::MemEdgeIterator it = NeighBors::MemEdgeIterator(mem_ptr);
      write_max(&vertex_max_level_[cur_vid], Level_t(1));

      // write edge index
      *(VertexId_t *)temp_index = cur_vid;
      temp_index += sizeof(VertexId_t);
      *(EdgeOffset_t *)temp_index = temp_body_offset;
      temp_index += sizeof(EdgeOffset_t);
      // (cache->indexes).emplace_back(cur_vid, temp_body_offset);
      cache->indexes[v_index].key    = cur_vid;
      cache->indexes[v_index].offset = temp_body_offset;
      // write edge body
      for (; it.valid(); it.next()) {
        filter->add(cur_vid, it.dst_id());
        bloomFilterVarySize->add(cur_vid, it.dst_id());

        EdgeOffset_t newOffset = temp_body_offset + EdgeBody_size;
        if (newOffset > sizeof(EdgeBody_t) * write_all_edge_num) {
          LOG_ERROR("eBuffer Overflow in memtable to sstable");
          exit(-1);
        }
        temp_body_offset = newOffset;
        EdgeBody_t edgebody(it.dst_id(), it.sequence(), temp_property_offset, it.marker());
        *(EdgeBody_t *)temp_body = edgebody;
        temp_body += EdgeBody_size;

        // write property
        EdgeOffset_t strLen              = it.edge_data().size();
        EdgeOffset_t new_property_offset = temp_property_offset + strLen;
        if (new_property_offset > property_size) {
          LOG_ERROR("pBuffer Overflow in memtable to sstable");
          exit(-1);
        }
        // it.key().print();
        memcpy(pfile_buffer + temp_property_offset, it.edge_data().data(), strLen);
        temp_property_offset = new_property_offset;
      }
    }
  };

  if (FLAGS_max_subcompactions > 1) {
    std::vector<std::thread>           threads;
    int                                threads_num = chunk_src_ids.size() - 1;
    std::vector<BloomFilter>           filters(threads_num - 1);
    std::vector<BloomFilterVarySize *> bloomFilterVarySizes;
    for (int i = 0; i < threads_num - 1; i++) {
      bloomFilterVarySizes.push_back(new BloomFilterVarySize());
    }

    threads.reserve(threads_num - 1);
    for (int i = 0; i < threads_num - 1; i++) {
      threads.emplace_back(build_sstable, i, &filters[i], bloomFilterVarySizes[i]);
      // std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    build_sstable(threads_num - 1, filter, bloomFilterVarySize);
    for (int i = 0; i < threads_num - 1; i++) {
      threads[i].join();
    }
    for (int i = 0; i < threads_num - 1; i++) {
      filter->merge(filters[i]);
      bloomFilterVarySize->merge(*(bloomFilterVarySizes[i]));
      delete bloomFilterVarySizes[i];
    }
  } else {
    // edge/index wirte to buffer
    for (uint v_index = 0; v_index < src_vertex_num; v_index++) {
      VertexId_t cur_vid = vertex_set[v_index];
      write_max(&vertex_max_level_[cur_vid], Level_t(1));

      // write edge index
      *(VertexId_t *)index = cur_vid;
      index += sizeof(VertexId_t);
      *(EdgeOffset_t *)index = body_offset;
      index += sizeof(EdgeOffset_t);
      // (cache->indexes).emplace_back(last_srcId, body_offset);
      cache->indexes[v_index].key    = cur_vid;
      cache->indexes[v_index].offset = body_offset;

      NeighBors::MemEdgeIterator it = NeighBors::MemEdgeIterator(this->get_vertex_adj(cur_vid));
      for (; it.valid(); it.next()) {
        filter->add(cur_vid, it.dst_id());
        bloomFilterVarySize->add(cur_vid, it.dst_id());

        // write edge body
        EdgeOffset_t newOffset = body_offset + EdgeBody_size;
        if (newOffset > sizeof(EdgeBody_t) * write_all_edge_num) {
          LOG_ERROR("eBuffer Overflow in memtable to sstable");
          exit(-1);
        }
        body_offset = newOffset;
        EdgeBody_t edgebody(it.dst_id(), it.sequence(), property_offset, it.marker());
        *(EdgeBody_t *)body = edgebody;
        body += EdgeBody_size;

        // write property
        EdgeOffset_t strLen              = it.edge_data().size();
        EdgeOffset_t new_property_offset = property_offset + strLen;
        if (new_property_offset > property_size) {
          LOG_ERROR("pBuffer Overflow in memtable to sstable");
          exit(-1);
        }
        // it.key().print();
        memcpy(pfile_buffer + property_offset, it.edge_data().data(), strLen);
        property_offset = new_property_offset;
      }
    }
  }

  // Add an end flag
  char *index_cur = efile_buffer + EdgeBody_size * (write_all_edge_num + 1) + index_pair_size * src_vertex_num;
  *(VertexId_t *)(index_cur) = INVALID_VERTEX_ID;
  index_cur += sizeof_vid;
  *(EdgeOffset_t *)index_cur = EdgeBody_size * write_all_edge_num;
  index_cur += sizeof(EdgeOffset_t);
  (cache->indexes).push_back(Index(INVALID_VERTEX_ID, EdgeBody_size * write_all_edge_num));

  // Add an end flag
  body_offset = EdgeBody_size * write_all_edge_num;
  EdgeBody_t edgebody(INVALID_VERTEX_ID, INVALID_VERTEX_ID, property_size, 0);  // just property_offset is valid
  char      *body_cur     = efile_buffer + body_offset;
  *(EdgeBody_t *)body_cur = edgebody;
  body_cur += EdgeBody_size;

  // wirte head
  *(Header *)(efile_buffer + real_efile_size - HEADER_SIZE) = cache->header;

  // write filter
  filter->save2Buffer(efile_buffer + real_efile_size - HEADER_SIZE - BLOOM_FILTER_SIZE);

  std::string e_filename = utils::eFileName(temp_currentTime);
  cache->path            = e_filename;

  FileIO file(e_filename);
  if (file.Open(O_WRONLY | O_CREAT | O_TRUNC) == false) {
    LOG_ERROR("Failed to open file for writing. file name={}", e_filename);
    exit(1);
  }
  file.Write(efile_buffer, real_efile_size);

  // std::string p_filename = e_filename + "_p";
  std::string p_filename = utils::pFileName(temp_currentTime);
  FileIO      p_outFile(p_filename);
  if (p_outFile.Open(O_WRONLY | O_CREAT | O_TRUNC) == false) {
    LOG_ERROR("Failed to open file for writing. file name={}", p_filename);
    exit(1);
  }
  p_outFile.Write(pfile_buffer, property_size);

  delete[] efile_buffer;
  delete[] pfile_buffer;
  delete[] vertex_set;  // Frees array memory allocated on the heap

  sstdata_manager_.put_data(temp_currentTime, cache->header.size, reinterpret_cast<uintptr_t>(cache));

  uint        level_0_max_sst_num_ = 2;
  VersionEdit edit;
  edit.AddFile(cache);
  int try_compaction = 3;

  while (true) {
    if (FLAGS_support_mulversion == true) {
      uint curr_level_0file_num = l0_versionset_->GetCurrent()->GetLevel0Files()->size();
      if (curr_level_0file_num >= level_0_max_sst_num_) {
        try_compaction--;
        if (try_compaction == 0) {
          compactor_.MaybeScheduleCompaction();
          try_compaction = 3;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }

      Version *v = new Version(l0_versionset_);

      l0_versionset_->VersionLock();
      l0_versionset_->LogAndApply(edit, v);
      level_0_cache = l0_versionset_->GetCurrent()->GetLevel0Files();
      this->SetFlash(true);
      std::shared_ptr<VersionAndMemTable> old_vm;
      {
        std::shared_lock r_lock(sv_.vm_rw_mtx);
        old_vm = sv_.version_memtable;
      }
      std::shared_ptr<VersionAndMemTable> new_vm = std::make_shared<VersionAndMemTable>();
      new_vm->batch_insert_tb(old_vm->menTables);
      new_vm->remove_tb(this);
      new_vm->set_vs(l0_versionset_->GetCurrent());
      {
        std::unique_lock w_lock(sv_.vm_rw_mtx);
        sv_.version_memtable = new_vm;
      }
      global_version_id_.fetch_add(1, std::memory_order_acquire);

      l0_versionset_->VersionUnLock();

      break;
    } else {
      std::unique_lock<std::mutex> lk(level_0_mux_);
      LOG_INFO("level_0_cache->size()={}", level_0_cache->size());
      if (level_0_cache->size() >= level_0_max_sst_num_) {
        lk.unlock();
        try_compaction--;
        if (try_compaction == 0) {
          compactor_.MaybeScheduleCompaction();
          try_compaction = 3;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }
      level_0_cache->push_back(cache);
      std::sort(level_0_cache->begin(), level_0_cache->end(), cacheTimeCompare);
      break;
    }
  }
}

bool MemTable::checkIsFinishWrite() {
  while (GetListLength() != GetMaxEdgeNum()) {
    std::this_thread::sleep_for(std::chrono::microseconds(5));
  }
  return true;
}

void MemTable::Ref() {
  refs.fetch_add(1);
}

void MemTable::Unref() {
  assert(refs.load(std::memory_order_acquire) >= 0);
  refs.fetch_sub(1);
}

int32_t MemTable::Getref() {
  return refs.load(std::memory_order_acquire);
}

void MemTable::SetLive(bool _islive) {
  islive = _islive;
}

void MemTable::SetFid(FileId_t fid) {
  fid_ = fid;
}

FileId_t MemTable::GetFid() const {
  return fid_;
}

void MemTable::SetFlash(bool _isflash) {
  isflash = _isflash;
}

bool MemTable::IsLive() {
  return islive;
}

bool MemTable::IsFlash() {
  return isflash;
}

void MemTable::save2eSSTable_split(const std::string &dir, uint64_t &currentTime,
                                   std::vector<SSTableCache *> *level_0_cache) {
  int cut_num = MAX_TABLE_SIZE / MAX_EFILE_SiZE;
  if (cut_num < 2) {
    save2eSSTable_split_property(dir, currentTime, level_0_cache);
    return;
  }

  assert(cut_num >= 2);
}

void MemTable::print(std::string label = "") {
  LOG_INFO("===========SkipList::print({})======", label);

  std::vector<VertexId_t> vertex_set(src_vertex_num);
  uint                    i = 0;
  for (size_t id = 0; id < vertex_id_.load(std::memory_order_relaxed); id++) {
    if (vertex_adjs[id] != NULLPOINTER) {
      vertex_set[i++] = id;
    }
  }
  LOG_INFO(" src_vertex_num={}", src_vertex_num);
  LOG_INFO(" edge_num={}", listLength);
  assert(i == src_vertex_num && i > 0);
  std::sort(vertex_set.begin(), vertex_set.begin() + src_vertex_num);

  for (uint v_index = 0; v_index < src_vertex_num; v_index++) {
    VertexId_t cur_vid = vertex_set[v_index];
    LOG_INFO("src={}", cur_vid);

    NeighBors::MemEdgeIterator it = NeighBors::MemEdgeIterator(this->get_vertex_adj(cur_vid));
    for (; it.valid(); it.next()) {
      it.key().print();
    }
  }

  LOG_INFO("=========================================\n");
}

uint64_t MemTable::GetListLength() {
  return listLength;
}

template <typename T>
void MemTable::AutomicAdd(T &a, T b) {
  __sync_fetch_and_add(&a, b);
}

uint64_t MemTable::GetMaxEdgeNum() {
  return max_edge_num;
}

void MemTable::SetStartTime(SequenceNumber_t start_time) {
  start_time_ = start_time;
}

SequenceNumber_t MemTable::GetStartTime() {
  return start_time_;
  ;
}

void MemTable::reset() {
  clear_vertex_adj();
  listLength = 0;
  refs.store(0);
  remain_capacity = max_edge_num;
  edge_arena.Reset();
  SetLive(false);
  SetFlash(false);
}

void MemTable::clear_vertex_adj() {
  src_vertex_num = 0;
#pragma omp parallel for num_threads(FLAGS_thread_num)
  for (size_t i = 0; i < vertex_id_.load(std::memory_order_relaxed); i++) {
    vertex_adjs[i] = NULLPOINTER;
  }
}

NeighBors *MemTable::get_vertex_adj(VertexId_t src) {
  assert(vertex_id_.load(std::memory_order_relaxed) >= src);
  return reinterpret_cast<NeighBors *>(vertex_adjs[src]);
}
}  // namespace lsmg