#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cstdint>
#include <exception>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <thread>

#include "cache/sst_data_cache.h"
#include "cache/sst_table_cache.h"
#include "common/utils.h"
#include "common/utils/atomic.h"
#include "compaction/compaction.h"
#include "compaction/sstable_writer.h"

#define DEL_EDGE_SEPARATE
namespace lsmg {

bool Compaction::PickCompactioFile(const uint level, const bool is_grow) {
  bool is_reverse = false;
  for (auto f : *fileMetaCache_[level]) {
    if (compact_pointer_[level] == MAX_GLOBAL_SEQ || f->header.minKey > compact_pointer_[level]) {
      inputs_[0].push_back(f);
      break;
    }
  }

  if (inputs_[0].empty()) {
    inputs_[0].push_back((*fileMetaCache_[level])[0]);
    is_reverse = true;
  }

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    VertexId_t smallest, largest;
    GetRange(inputs_[0], smallest, largest);
    GetOverlappingInputs(0, smallest, largest, &inputs_[0]);
    assert(!inputs_[0].empty());
  }

  if (fileMetaCache_.size() > level + 1) {
    SetupOtherInputs(level, is_grow);
  } else {
    VertexId_t smallest, largest;
    GetRange(inputs_[0], smallest, largest);
    compact_pointer_[level] = largest;  // update pointer
  }
  return is_reverse;
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void Compaction::GetRange2(const std::vector<SSTableCache *> &inputs1, const std::vector<SSTableCache *> &inputs2,
                           VertexId_t &smallest, VertexId_t &largest) {
  std::vector<SSTableCache *> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void Compaction::GetRange(const std::vector<SSTableCache *> &inputs, VertexId_t &smallest, VertexId_t &largest) {
  assert(!inputs.empty());
  for (size_t i = 0; i < inputs.size(); i++) {
    SSTableCache *f = inputs[i];
    if (i == 0) {
      smallest = f->header.minKey;
      largest  = f->header.maxKey;
    } else {
      if (f->header.minKey < smallest) {
        smallest = f->header.minKey;
      }
      if (f->header.maxKey > largest) {
        largest = f->header.maxKey;
      }
    }
  }
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Compaction::GetOverlappingInputs(const uint level, const VertexId_t begin, const VertexId_t end,
                                      std::vector<SSTableCache *> *inputs) {
  assert(level < MAX_LEVEL);
  inputs->clear();
  VertexId_t user_begin = begin, user_end = end;

  std::vector<SSTableCache *> &fileMetaCache_level = *fileMetaCache_[level];
  for (size_t i = 0; i < fileMetaCache_[level]->size();) {
    SSTableCache    *f          = fileMetaCache_level[i++];
    const VertexId_t file_start = f->header.minKey;
    const VertexId_t file_limit = f->header.maxKey;
    if (file_limit < user_begin) {
      // "f" is completely before specified range; skip it
    } else if (file_start > user_end) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) {
        if (file_start < user_begin) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (file_limit > user_end) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

// find files in level n+1 and update files in level n
void Compaction::SetupOtherInputs(const int pre_level, const bool is_grow) {
  assert(pre_level >= 0);
  VertexId_t smallest, largest;
  GetRange(inputs_[0], smallest, largest);

  GetOverlappingInputs(pre_level + 1, smallest, largest, &inputs_[1]);

  // Get entire range covered by compaction
  VertexId_t all_start, all_limit;
  GetRange2(inputs_[0], inputs_[1], all_start, all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (is_grow && !inputs_[1].empty()) {
    std::vector<SSTableCache *> expanded0;
    GetOverlappingInputs(pre_level, all_start, all_limit, &expanded0);
    if (expanded0.size() > inputs_[0].size()) {
      VertexId_t new_start, new_limit;
      GetRange(expanded0, new_start, new_limit);
      std::vector<SSTableCache *> expanded1;
      GetOverlappingInputs(pre_level + 1, new_start, new_limit, &expanded1);
      if (expanded1.size() == inputs_[1].size()) {
        smallest   = new_start;
        largest    = new_limit;
        inputs_[0] = expanded0;
        inputs_[1] = expanded1;
        GetRange2(inputs_[0], inputs_[1], all_start, all_limit);
      }
    }
  }

  compact_pointer_[pre_level] = largest;
  last_merge_max_key_         = std::max(largest, all_limit);
}

void Compaction::RemoveCache(const uint level, const int inputs_index, bool free_space) {
  if (level >= fileMetaCache_.size() || merged_fids_[inputs_index].size() == 0) {
    return;
  }
  // level-0 delete merged files
  if (FLAGS_support_mulversion == true && level == 0 && inputs_index == 0) {
    return;
  }
  std::lock_guard<std::mutex> lock(*tablecache_mutex_[level]);
  auto                        iter = fileMetaCache_[level]->begin();
  while (iter != fileMetaCache_[level]->end()) {
    bool is_del = false;
    if (merged_fids_[inputs_index].find((*iter)->header.timeStamp) != merged_fids_[inputs_index].end()) {
      is_del = true;
    }
    if (is_del) {
      if (free_space) {
        delete *iter;
      }
      iter = fileMetaCache_[level]->erase(iter);
    } else {
      ++iter;
    }
  }
}

void Compaction::RemoveFile() {
  // remove all merged file
  for (auto del_fid : merged_fids_[0]) {
    delete_fids_set_.Put(del_fid);
  }
  for (auto del_fid : merged_fids_[1]) {
    delete_fids_set_.Put(del_fid);
  }
}

void Compaction::RealRemoveFile() {
  // remove all merged file
  while (delete_fids_set_.Size() > 0) {
    FileId_t fid = -1;
    delete_fids_set_.Get(fid);
    auto rt = utils::rmfile(utils::eFileName(fid).c_str());
    assert(rt == 0);
    rt = utils::rmfile(utils::pFileName(fid).c_str());
    assert(rt == 0);
  }
}

void Compaction::DoCompactionWorkTwoWay(const int level,

                                        const int inputs_0_size, const int inputs_1_size,
                                        std::vector<EdgeRecord> &edge_cache, std::vector<Way> &ways) {
  assert(level >= 1);
  assert(inputs_0_size >= 1);

  // [0, inputs_0_size) [inputs_0_size, inputs_1_size)
  int way_num            = inputs_0_size + inputs_1_size;
  int level_way_ptr      = 0;
  int next_level_way_ptr = inputs_0_size;
  int work_way           = way_num;

  edge_cache.resize(2);
  if (inputs_0_size > 0) {
    ways[level_way_ptr].Init();
    edge_cache[0] = ways[level_way_ptr].NextEdgeRecord().value();
  }
  if (inputs_1_size > 0) {
    ways[next_level_way_ptr].Init();
    edge_cache[1] = ways[next_level_way_ptr].NextEdgeRecord().value();
  }

  // find min edge
  int min_edge_index = 0;
  int min_way_index  = level_way_ptr;
  if (inputs_1_size > 0 && edge_cache[1] < edge_cache[0]) {
    min_edge_index = 1;
    min_way_index  = next_level_way_ptr;
  }

  auto writer = SSTableWriter(fileMetaCache_, vid_to_levelIndex, vid_to_mullevelIndex_, vertex_futexes, vertex_rwlocks_,
                              vertex_max_level_, min_level_0_fid_, level, buffer_manager, tablecache_mutex_,
                              version_edit_, sstdata_manager_, *vid_to_mem_disk_index_);
  uint64_t temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
  writer.Init(utils::eFileName(temp_currentTime), utils::pFileName(temp_currentTime), edge_cache[min_edge_index].src_,
              temp_currentTime);
  for (int i = 0; i < inputs_0_size; i++) {
    writer.InsertInput0Fid(ways[i].GetFildId());
  }

#ifdef DEL_EDGE_SEPARATE
  std::vector<bool>           way_have_del_edge(way_num, false);
  std::vector<EidToTimeMap *> way_map(way_num, nullptr);
  for (uint i = 0; i < ways.size(); i++) {
    EidToTimeMap *deleted_edge_map;
    bool          have_del_edge = del_record_manager_.find_eidmap(ways[i].GetFildId(), deleted_edge_map);
    if (have_del_edge == true) {
      way_have_del_edge[i] = true;
      way_map[i]           = deleted_edge_map;
    } else {
    }
  }
#endif

  while (work_way > 0) {
    auto &edge_record = edge_cache[min_edge_index];

#ifdef DEL_EDGE_SEPARATE
    if (way_have_del_edge[min_way_index] == true && edge_record.marker_ == false) {
      SequenceNumber_t eid = edge_record.seq_;
      SequenceNumber_t del_time;
      if (del_record_manager_.get_time(way_map[min_way_index], eid, del_time)) {
        del_record_manager_.del_eid_frome_eidmap(way_map[min_way_index], eid);

        edge_record.marker_ = true;
      }
    }
#endif

    // write edge to output file
    if (!writer.Write(edge_record)) {  // if output file full
      temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
      writer.Init(utils::eFileName(temp_currentTime), utils::pFileName(temp_currentTime), edge_record.src_,
                  temp_currentTime);
      bool rt = writer.Write(edge_record);
      assert(rt == true);
    }

    auto new_edge_record_opt = ways[min_way_index].NextEdgeRecord();

    if (new_edge_record_opt == std::nullopt) {
      ways[min_way_index].FreeBuffer();
      work_way--;
      if (work_way <= 0) {
        break;
      }
      level_way_ptr += (min_edge_index == 0);
      next_level_way_ptr += (min_edge_index == 1);
      if (min_edge_index == 0 && level_way_ptr < inputs_0_size) {
        ways[level_way_ptr].Init();
        edge_cache[0] = ways[level_way_ptr].NextEdgeRecord().value();
      } else if (min_edge_index == 1 && next_level_way_ptr < way_num) {
        assert(inputs_1_size > 0);
        ways[next_level_way_ptr].Init();
        edge_cache[1] = ways[next_level_way_ptr].NextEdgeRecord().value();
      }
    } else {
      edge_cache[min_edge_index] = new_edge_record_opt.value();
    }

    // find min edge
    if (level_way_ptr == inputs_0_size) {
      min_edge_index = 1;
      min_way_index  = next_level_way_ptr;
      assert(inputs_1_size > 0);
    } else if (next_level_way_ptr == way_num) {
      min_edge_index = 0;
      min_way_index  = level_way_ptr;
    } else {
      assert(inputs_1_size > 0);
      min_edge_index = 0;
      min_way_index  = level_way_ptr;
      if (edge_cache[1] < edge_cache[0]) {
        min_edge_index = 1;
        min_way_index  = next_level_way_ptr;
      }
    }
  }

  if (!writer.IsClear()) {
    writer.WriteEnd();
  }
  edge_cache.clear();
}

// The goal is to find some boundary keys so that we can evenly partition
// the compaction input data into max_subcompactions ranges.
// For every input file, we ask TableReader to estimate 128 anchor points
// that evenly partition the input file into 128 ranges and the range
// sizes. This can be calculated by scanning index blocks of the file.
// Once we have the anchor points for all the input files, we merge them
// together and try to find keys dividing ranges evenly.
// For example, if we have two input files, and each returns following
// ranges:
//   File1: (a1, 1000), (b1, 1200), (c1, 1100)
//   File2: (a2, 1100), (b2, 1000), (c2, 1000)
// We total sort the keys to following:
//  (a1, 1000), (a2, 1100), (b1, 1200), (b2, 1000), (c1, 1100), (c2, 1000)
// We calculate the total size by adding up all ranges' size, which is 6400.
// If we would like to partition into 2 subcompactions, the target of the
// range size is 3200. Based on the size, we take "b1" as the partition key
// since the first three ranges would hit 3200.
//
// Note that the ranges are actually overlapping. For example, in the example
// above, the range ending with "b1" is overlapping with the range ending with
// "b2". So the size 1000+1100+1200 is an underestimation of data size up to
// "b1". In extreme cases where we only compact N L0 files, a range can
// overlap with N-1 other ranges. Since we requested a relatively large number
// (128) of ranges from each input files, even N range overlapping would
// cause relatively small inaccuracy.
// ref: rocksdb/db/compaction/compaction_job.cc
void Compaction::GenSubcompactionBoundaries(std::vector<WayAnchor>  &all_wayAnchors,
                                            std::vector<VertexId_t> &boundaries) {
  std::vector<Anchor> all_anchors;
  uint64_t            total_size  = 0;
  int                 segment_num = 16;
  // level=0
  for (auto it : inputs_[0]) {
    size_t edge_num = it->header.size;
    total_size += edge_num;
    size_t              index_num           = it->header.index_size;
    size_t              edge_num_per_anchor = edge_num / segment_num;
    size_t              range_size          = 0;
    std::vector<Index> &indexes             = it->indexes;
    EdgeOffset_t        offset              = indexes[0].offset;
    VertexId_t          src                 = indexes[0].key;
    for (uint j = 0; j < index_num; j++) {
      EdgeOffset_t now_offset = indexes[j].offset;
      assert(now_offset >= offset);
      range_size = (now_offset - offset) / sizeof(EdgeBody_t);
      if (range_size > edge_num_per_anchor) {
        all_anchors.emplace_back(src, range_size);
        range_size = 0;
        offset     = now_offset;
        src        = indexes[j].key;
      }
    }
    if (range_size > 0) {
      all_anchors.emplace_back(src, range_size);
    }
  }

  // level=1
  for (auto it : inputs_[1]) {
    size_t edge_num = it->header.size;
    total_size += edge_num;
    size_t       index_num           = it->header.index_size;
    size_t       edge_num_per_anchor = edge_num / segment_num;
    size_t       range_size          = 0;
    char        *indexBuf            = it->GetIndex();
    EdgeOffset_t offset              = *(EdgeOffset_t *)(indexBuf + 8);
    VertexId_t   src                 = *(VertexId_t *)(indexBuf);
    for (uint j = 0; j < index_num; j++) {
      EdgeOffset_t now_offset = *(EdgeOffset_t *)(indexBuf + 12 * j + 8);
      range_size              = (now_offset - offset) / sizeof(EdgeBody_t);
      if (range_size > edge_num_per_anchor) {
        all_anchors.emplace_back(src, range_size);
        range_size = 0;
        offset     = now_offset;
        src        = *(VertexId_t *)(indexBuf + 12 * j);
      }
    }
    if (range_size > 0) {
      all_anchors.emplace_back(src, range_size);
    }
    delete[] indexBuf;
  }

  std::sort(all_anchors.begin(), all_anchors.end(),
            [](const Anchor &a, const Anchor &b) { return a.begin_ < b.begin_; });

  // Remove duplicated entries from boundaries.
  all_anchors.erase(
      std::unique(all_anchors.begin(), all_anchors.end(), [](Anchor &a, Anchor &b) { return a.begin_ == b.begin_; }),
      all_anchors.end());

  uint64_t num_planned_subcompactions = max_subcompactions_;

  assert(num_planned_subcompactions > 1);

  // Group the ranges into subcompactions
  size_t max_edge_num_per_file =
      (MAX_EFILE_SiZE - HEADER_SIZE - BLOOM_FILTER_SIZE) / (sizeof(EdgeBody_t) + sizeof(Index));
  uint64_t target_range_size = std::max(total_size / num_planned_subcompactions, max_edge_num_per_file);
  assert(target_range_size < total_size);

  uint64_t next_threshold            = target_range_size;
  uint64_t cumulative_size           = 0;
  uint64_t num_actual_subcompactions = 1U;
  for (Anchor &anchor : all_anchors) {
    cumulative_size += anchor.range_size_;
    if (cumulative_size > next_threshold) {
      next_threshold += target_range_size;
      num_actual_subcompactions++;
      boundaries.push_back(anchor.begin_);
    }
    if (num_actual_subcompactions == num_planned_subcompactions) {
      break;
    }
  }

  // level=0
  for (auto it : inputs_[0]) {
    if (it->header.index_size <= 1) {
      continue;
    }
    size_t       range_size    = 0;
    EdgeOffset_t offset        = it->indexes[0].offset;
    VertexId_t   src           = it->indexes[0].key;
    uint         j             = 0;
    uint         last_index_id = 0;
    for (auto bd : boundaries) {
      j = it->low_bound(bd, 0, it->indexes.size() - 1);
      if (j == 0 || bd < src || j == last_index_id) {
        continue;
      }
      // assert(bd <= it->indexes[j].key);
      VertexId_t   now_key    = it->indexes[j].key;
      EdgeOffset_t now_offset = it->indexes[j].offset;
      range_size              = now_offset - offset;  // / sizeof(EdgeBody_t);
      // [begin, end]
      assert(j >= 1);
      VertexId_t end_key = it->indexes[j - 1].key;
      all_wayAnchors.emplace_back(src,
                                  // now_key,
                                  end_key, offset, last_index_id * sizeof(Index) + it->header.size * sizeof(EdgeBody_t),
                                  j - last_index_id, range_size, it->path, it->header.timeStamp, true);
      assert(range_size > 0);
      offset        = now_offset;
      src           = now_key;
      last_index_id = j;
      if (j == it->header.index_size - 1) {
        break;
      }
    }
    if (j < it->header.index_size - 1) {  // || last_index_id == 0) {
      j          = it->header.index_size - 1;
      range_size = (it->indexes[j].offset - offset);  // / sizeof(EdgeBody_t);
      assert(j >= 1);
      VertexId_t end_key = it->indexes[j - 1].key;
      all_wayAnchors.emplace_back(src,
                                  // VertexId_t(it->indexes[j].key),
                                  end_key, offset, last_index_id * sizeof(Index) + it->header.size * sizeof(EdgeBody_t),
                                  j - last_index_id, range_size, it->path, it->header.timeStamp, true);
      assert(range_size > 0);
    }
  }
  // level=1
  for (auto it : inputs_[1]) {
    if (it->header.index_size <= 1) {
      continue;
    }
    size_t       range_size    = 0;
    char        *indexBuf      = it->GetIndex();
    VertexId_t   src           = *(VertexId_t *)(indexBuf);
    EdgeOffset_t offset        = *(EdgeOffset_t *)(indexBuf + 8);
    uint         j             = 0;
    uint         last_index_id = 0;
    for (auto bd : boundaries) {
      j = it->low_bound(bd, 0, it->header.index_size - 1, indexBuf);
      if (j == 0 || bd < src || j == last_index_id) {
        continue;
      }
      // assert(bd <= it->indexes[j].key);
      VertexId_t   now_key    = *(VertexId_t *)(indexBuf + 12 * j);
      EdgeOffset_t now_offset = *(EdgeOffset_t *)(indexBuf + 12 * j + 8);
      range_size              = (now_offset - offset);  // / sizeof(EdgeBody_t);
      assert(j >= 1);
      VertexId_t end_key = *(VertexId_t *)(indexBuf + 12 * (j - 1));
      all_wayAnchors.emplace_back(src,
                                  // now_key,
                                  end_key, offset, last_index_id * sizeof(Index) + it->header.size * sizeof(EdgeBody_t),
                                  j - last_index_id, range_size, it->path, it->header.timeStamp, false);
      assert(range_size > 0);
      offset        = now_offset;
      src           = now_key;
      last_index_id = j;
      if (j == it->header.index_size - 1) {
        break;
      }
    }
    if (j < it->header.index_size - 1) {  // || last_index_id == 0) {
      j                       = it->header.index_size - 1;
      EdgeOffset_t now_offset = *(EdgeOffset_t *)(indexBuf + 12 * j + 8);
      range_size              = (now_offset - offset);  // / sizeof(EdgeBody_t);
      assert(j >= 1);
      VertexId_t end_key = *(VertexId_t *)(indexBuf + 12 * (j - 1));
      all_wayAnchors.emplace_back(src,
                                  // now_key,
                                  end_key, offset, last_index_id * sizeof(Index) + it->header.size * sizeof(EdgeBody_t),
                                  j - last_index_id, range_size, it->path, it->header.timeStamp, false);
      assert(range_size > 0);
    }
    delete[] indexBuf;
  }

  std::sort(all_wayAnchors.begin(), all_wayAnchors.end(),
            [](const WayAnchor &a, const WayAnchor &b) { return a.begin_ < b.begin_; });
}

void Compaction::ProcessMultiWaysCompaction(std::vector<Way> &ways, const int level) {
  int way_num = ways.size();
  assert(way_num > 0);
  int                     level_way_ptr = 0;
  std::vector<bool>       way_status(way_num, true);
  int                     work_way = way_num;
  std::vector<EdgeRecord> edge_cache;
  edge_cache.reserve(way_num);

  for (int i = 0; i < way_num; i++) {
    ways[i].Init();
    edge_cache.emplace_back(ways[i].NextEdgeRecord().value());
  }

  // find min edge
  int min_way_index = level_way_ptr;
  int ptr           = level_way_ptr + 1;
  while (ptr < way_num) {
    if (edge_cache[ptr] < edge_cache[min_way_index]) {
      min_way_index = ptr;
    }
    ptr++;
  }

  uint64_t temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
  auto writer = SSTableWriter(fileMetaCache_, vid_to_levelIndex, vid_to_mullevelIndex_, vertex_futexes, vertex_rwlocks_,
                              vertex_max_level_, min_level_0_fid_, level, buffer_manager, tablecache_mutex_,
                              version_edit_, sstdata_manager_, *vid_to_mem_disk_index_);
  writer.Init(utils::eFileName(temp_currentTime), utils::pFileName(temp_currentTime), edge_cache[min_way_index].src_,
              temp_currentTime);

  for (uint i = 0; i < ways.size(); i++) {
    if (ways[i].Is_input_0()) {
      writer.InsertInput0Fid(ways[i].GetFildId());
    }
  }

#ifdef DEL_EDGE_SEPARATE
  std::vector<bool>           way_have_del_edge(way_num, false);
  std::vector<EidToTimeMap *> way_map(way_num, nullptr);
  for (uint i = 0; i < ways.size(); i++) {
    EidToTimeMap *deleted_edge_map;
    bool          have_del_edge = del_record_manager_.find_eidmap(ways[i].GetFildId(), deleted_edge_map);
    if (have_del_edge == true) {
      way_have_del_edge[i] = true;
      way_map[i]           = deleted_edge_map;
    } else {
    }
  }
#endif

  while (work_way > 0) {
    auto &edge_record = edge_cache[min_way_index];

#ifdef DEL_EDGE_SEPARATE
    // Merge the deleted records of this edge, if they exist.
    // DelRecordManage& del_record_manager;
    if (way_have_del_edge[min_way_index] == true) {
      SequenceNumber_t eid = edge_record.seq_;
      SequenceNumber_t del_time;
      if (del_record_manager_.get_time(way_map[min_way_index], eid, del_time)) {
        del_record_manager_.del_eid_frome_eidmap(way_map[min_way_index], eid);

        edge_record.marker_ = true;
      }
    }
#endif

    // write edge to output file
    if (!writer.Write(edge_record)) {  // if output file full
      temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
      writer.Init(utils::eFileName(temp_currentTime), utils::pFileName(temp_currentTime), edge_record.src_,
                  temp_currentTime);
      bool rt = writer.Write(edge_record);
      assert(rt == true);
    }

    auto new_edge_record_opt = ways[min_way_index].NextEdgeRecord();
    if (new_edge_record_opt == std::nullopt) {
      way_status[min_way_index] = false;
      ways[min_way_index].FreeBuffer();
      work_way--;
      if (work_way <= 0) {
        break;
      }
    } else {
      edge_cache[min_way_index] = new_edge_record_opt.value();
    }

    // find min edge
    int  ptr      = level_way_ptr;
    bool is_first = true;
    while (ptr < way_num) {
      if (is_first && way_status[ptr]) {
        min_way_index = ptr;
        ptr++;
        is_first = false;
        continue;
      }
      if (way_status[ptr] && edge_cache[ptr] < edge_cache[min_way_index]) {
        min_way_index = ptr;
      }
      ptr++;
    }
    assert(way_status[min_way_index] == true);
  }

  if (!writer.IsClear()) {
    writer.WriteEnd();
  }
  edge_cache.clear();
}

void Compaction::ProcessMultiWaysCompactionOpt(const uint level, std::vector<WayAnchor> &all_wayAnchors,
                                               int anchor_begin, int anchor_end) {
  assert(level == 0);  // If level >= 1, it is better to use two-way compaction
  assert(all_wayAnchors.size() > 0);
  assert(anchor_begin < anchor_end);
  std::vector<VertexId_t>       end_per_way;
  std::vector<std::vector<Way>> mulways;
  for (int i = anchor_begin; i < anchor_end; i++) {
    auto &anchor = all_wayAnchors[i];
    bool  found  = false;
    for (uint way_id = 0; way_id < end_per_way.size(); way_id++) {
      if (anchor.begin_ > end_per_way[way_id]) {
        // anchor.print();
        end_per_way[way_id] = anchor.end;
        mulways[way_id].emplace_back(buffer_manager, anchor.index_num + 1, anchor.index_start_offset,
                                     anchor.range_size / sizeof(EdgeBody_t) + 1, anchor.path, anchor.fid,
                                     anchor.is_input_0);
        found = true;
        break;
      }
    }
    if (found == false) {
      end_per_way.push_back(anchor.end);
      mulways.push_back(std::vector<Way>());
      mulways[mulways.size() - 1].emplace_back(buffer_manager, anchor.index_num + 1, anchor.index_start_offset,
                                               anchor.range_size / sizeof(EdgeBody_t) + 1, anchor.path, anchor.fid,
                                               anchor.is_input_0);
    }
  }

  uint way_num = mulways.size();
  assert(way_num > 0);
  assert(way_num <= utils::getLevelMaxSize(level) + 1);
  int                     level_way_ptr = 0;
  std::vector<bool>       way_status(way_num, true);
  std::vector<uint>       way_ptr(way_num, 0);
  int                     work_way = way_num;
  std::vector<EdgeRecord> edge_cache;
  edge_cache.reserve(way_num);

  for (uint i = 0; i < way_num; i++) {
    mulways[i][way_ptr[i]].Init();
    edge_cache.emplace_back(mulways[i][way_ptr[i]].NextEdgeRecord().value());
  }

  // find min edge
  uint min_way_index = level_way_ptr;
  uint ptr           = level_way_ptr + 1;
  while (ptr < way_num) {
    if (edge_cache[ptr] < edge_cache[min_way_index]) {
      min_way_index = ptr;
    }
    ptr++;
  }

  uint64_t temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
  auto writer = SSTableWriter(fileMetaCache_, vid_to_levelIndex, vid_to_mullevelIndex_, vertex_futexes, vertex_rwlocks_,
                              vertex_max_level_, min_level_0_fid_, level, buffer_manager, tablecache_mutex_,
                              version_edit_, sstdata_manager_, *vid_to_mem_disk_index_);
  writer.Init(utils::eFileName(temp_currentTime), utils::pFileName(temp_currentTime), edge_cache[min_way_index].src_,
              temp_currentTime);

  std::vector<std::vector<int>> level_1_index(mulways.size());

  for (uint i = 0; i < mulways.size(); i++) {
    for (uint j = 0; j < mulways[i].size(); ++j) {
      if (mulways[i][j].Is_input_0()) {
        writer.InsertInput0Fid(mulways[i][j].GetFildId());
      } else {
        level_1_index[i].emplace_back(j);
      }
    }
  }

#ifdef DEL_EDGE_SEPARATE
  std::vector<bool>           way_have_del_edge(way_num, false);
  std::vector<EidToTimeMap *> way_map(way_num, nullptr);
  for (uint i = 0; i < ways.size(); i++) {
    EidToTimeMap *deleted_edge_map;
    bool          have_del_edge = del_record_manager_.find_eidmap(ways[i].GetFildId(), deleted_edge_map);
    if (have_del_edge == true) {
      way_have_del_edge[i] = true;
      way_map[i]           = deleted_edge_map;
    } else {
    }
  }
#endif

  while (work_way > 0) {
    uint32_t cur_vertex = edge_cache[min_way_index].src_;
    while (work_way > 0 && cur_vertex == edge_cache[min_way_index].src_) {
      auto &edge_record = edge_cache[min_way_index];

#ifdef DEL_EDGE_SEPARATE
      // Merge the deleted records of this edge, if they exist.
      if (way_have_del_edge[min_way_index] == true) {
        SequenceNumber_t eid = edge_record.seq_;
        SequenceNumber_t del_time;
        if (del_record_manager_.get_time(way_map[min_way_index], eid, del_time)) {
          del_record_manager_.del_eid_frome_eidmap(way_map[min_way_index], eid);

          edge_record.marker_ = true;
        }
      }
#endif

      // write edge to output file
      if (!writer.Write(edge_record)) {  // if output file full
        temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
        writer.Init(utils::eFileName(temp_currentTime), utils::pFileName(temp_currentTime), edge_record.src_,
                    temp_currentTime);
        bool rt = writer.Write(edge_record);
        assert(rt == true);
      }

      int  curr_ptr            = way_ptr[min_way_index];
      auto new_edge_record_opt = mulways[min_way_index][curr_ptr].NextEdgeRecord();
      if (new_edge_record_opt == std::nullopt) {
        mulways[min_way_index][curr_ptr].FreeBuffer();
        way_ptr[min_way_index]++;
        if (way_ptr[min_way_index] == mulways[min_way_index].size()) {
          way_status[min_way_index] = false;
          work_way--;
          if (work_way <= 0) {
            break;
          }
        } else {
          mulways[min_way_index][way_ptr[min_way_index]].Init();
          edge_cache[min_way_index] = mulways[min_way_index][way_ptr[min_way_index]].NextEdgeRecord().value();
        }
      } else {
        edge_cache[min_way_index] = new_edge_record_opt.value();
      }

      // find min edge
      uint ptr      = level_way_ptr;
      bool is_first = true;
      while (ptr < way_num) {
        if (is_first && way_status[ptr]) {
          min_way_index = ptr;
          ptr++;
          is_first = false;
          continue;
        }
        if (way_status[ptr] && edge_cache[ptr] < edge_cache[min_way_index]) {
          min_way_index = ptr;
        }
        ptr++;
      }
      assert(way_status[min_way_index] == true);
    }
  }

  if (!writer.IsClear()) {
    writer.WriteEnd();
  }
  assert(edge_cache.size() == way_num);
  edge_cache.clear();
}

void Compaction::DoSubCompactionWork(const int level) {
  assert(level == 0);

  std::vector<WayAnchor>  all_wayAnchors;
  std::vector<VertexId_t> boundaries;  // the boundaries for each subcompaction
  GenSubcompactionBoundaries(all_wayAnchors, boundaries);

  if (FLAGS_support_mulversion == false) {
    RemoveCache(level, 0, true);
    RemoveCache(level + 1, 1, true);
    RemoveFile();
  } else {
    RemoveCache(level, 0, false);
    RemoveCache(level + 1, 1, false);
  }

  //---------------------------------------------------------------------------/
  // build subcompaction thread
  const size_t num_threads = boundaries.size();  // real: num_threads + 1(main)
  assert(num_threads > 0);
  assert(num_threads <= max_subcompactions_);

  // Launch a thread for each of subcompactions 1...num_threads-1
  std::vector<std::thread> thread_pool;
  thread_pool.reserve(num_threads);
  std::vector<std::vector<Way>> sub_ways(num_threads);

  bool opt = true;
  if (opt == false) {
    int cnt      = 0;
    int thead_id = 0;
    for (VertexId_t bd : boundaries) {
      for (uint i = cnt; i < all_wayAnchors.size(); i++, cnt++) {
        WayAnchor &anchor = all_wayAnchors[i];
        if (anchor.begin_ >= bd) {
          break;
        }
        sub_ways[thead_id].emplace_back(buffer_manager, anchor.index_num + 1, anchor.index_start_offset,
                                        anchor.range_size / sizeof(EdgeBody_t) + 1, anchor.path, anchor.fid,
                                        anchor.is_input_0);
      }
      thread_pool.emplace_back(&Compaction::ProcessMultiWaysCompaction, this, std::ref(sub_ways[thead_id++]), level);
    }

    for (uint i = cnt; i < all_wayAnchors.size(); i++, cnt++) {
      WayAnchor &anchor = all_wayAnchors[i];
      ways.emplace_back(buffer_manager, anchor.index_num + 1, anchor.index_start_offset,
                        anchor.range_size / sizeof(EdgeBody_t) + 1, anchor.path, anchor.fid, anchor.is_input_0);
    }
    // Always schedule the first subcompaction (whether or not there are also
    // others) in the current thread to be efficient with resources
    ProcessMultiWaysCompaction(ways, level);
  } else {
    int cnt          = 0;
    int anchor_begin = 0;
    int anchor_end   = 0;
    // [begin, end)
    for (VertexId_t bd : boundaries) {
      anchor_begin = cnt;
      for (uint i = cnt; i < all_wayAnchors.size(); i++, cnt++) {
        WayAnchor &anchor = all_wayAnchors[i];
        anchor_end        = i;
        if (anchor.begin_ >= bd) {
          break;
        }
      }
      thread_pool.emplace_back(&Compaction::ProcessMultiWaysCompactionOpt, this, level, std::ref(all_wayAnchors),
                               anchor_begin, anchor_end);
    }

    ProcessMultiWaysCompactionOpt(level, all_wayAnchors, cnt, all_wayAnchors.size());
  }

  for (auto &thread : thread_pool) {
    thread.join();
  }
  // LOG_INFO("finish subcompacton.");
}

void Compaction::ProcessCompactionWork(const int level, const int inputs_0_size, const int inputs_1_size,
                                       std::vector<EdgeRecord> &edge_cache, std::vector<Way> &ways) {
  if (level >= 1) {
    int max_level = fileMetaCache_.size() - 1;
    while (max_level > 0) {
      if (!fileMetaCache_[max_level]->empty()) {
        break;
      }
      --max_level;
    }
    DoCompactionWorkTwoWay(level, inputs_0_size, inputs_1_size, edge_cache, ways);
    return;
  }

  int               way_num            = inputs_0_size + inputs_1_size;
  int               level_way_ptr      = 0;
  int               next_level_way_ptr = inputs_0_size;
  std::vector<bool> way_status(way_num, true);
  int               work_way = way_num;

  for (int i = 0; i < inputs_0_size; i++) {
    ways[i].Init();
    edge_cache.emplace_back(ways[i].NextEdgeRecord().value());
  }
  if (inputs_1_size > 0) {
    ways[next_level_way_ptr].Init();
    edge_cache.emplace_back(ways[next_level_way_ptr].NextEdgeRecord().value());
  }

  // find min edge
  int min_way_index = level_way_ptr;
  int ptr           = level_way_ptr + 1;
  while (ptr < inputs_0_size) {
    if (edge_cache[ptr] < edge_cache[min_way_index]) {
      min_way_index = ptr;
    }
    ptr++;
  }
  if (next_level_way_ptr < way_num && edge_cache[next_level_way_ptr] < edge_cache[min_way_index]) {
    min_way_index = next_level_way_ptr;
  }

  auto writer = SSTableWriter(fileMetaCache_, vid_to_levelIndex, vid_to_mullevelIndex_, vertex_futexes, vertex_rwlocks_,
                              vertex_max_level_, min_level_0_fid_, level, buffer_manager, tablecache_mutex_,
                              version_edit_, sstdata_manager_, *vid_to_mem_disk_index_);
  uint64_t temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
  writer.Init(utils::eFileName(temp_currentTime), utils::pFileName(temp_currentTime), edge_cache[min_way_index].src_,
              temp_currentTime);
  for (int i = 0; i < inputs_0_size; i++) {
    writer.InsertInput0Fid(ways[i].GetFildId());
  }

#ifdef DEL_EDGE_SEPARATE
  std::vector<bool>           way_have_del_edge(way_num, false);
  std::vector<EidToTimeMap *> way_map(way_num, nullptr);
  for (uint i = 0; i < ways.size(); i++) {
    EidToTimeMap *deleted_edge_map;
    bool          have_del_edge = del_record_manager_.find_eidmap(ways[i].GetFildId(), deleted_edge_map);
    if (have_del_edge == true) {
      way_have_del_edge[i] = true;
      way_map[i]           = deleted_edge_map;
    } else {
    }
  }
#endif

  while (work_way > 0) {
    VertexId_t cur_vertex = edge_cache[min_way_index].src_;

    if (next_level_way_ptr < way_num && edge_cache[next_level_way_ptr].src_ == cur_vertex
        && (*vid_to_mem_disk_index_)[cur_vertex].Get(1)->fileID == INVALID_File_ID) {
      while (edge_cache[next_level_way_ptr].src_ == cur_vertex) {
        auto new_edge_record_opt = ways[next_level_way_ptr].NextEdgeRecord();
        if (edge_cache[next_level_way_ptr].seq_ == 1217) {
          LOG_INFO("{}\n{}", next_level_way_ptr, ways[next_level_way_ptr].GetFildId());
        }
        if (new_edge_record_opt == std::nullopt) {
          way_status[next_level_way_ptr] = false;
          ways[next_level_way_ptr].FreeBuffer();
          work_way--;
          if (work_way <= 0) {
            break;
          }
          next_level_way_ptr++;
          if (next_level_way_ptr < way_num) {
            ways[next_level_way_ptr].Init();
            edge_cache.emplace_back(ways[next_level_way_ptr].NextEdgeRecord().value());
          }
          break;
        } else {
          edge_cache[next_level_way_ptr] = new_edge_record_opt.value();
        }
      }
      // find min edge
      int  ptr      = level_way_ptr;
      bool is_first = true;
      while (ptr < inputs_0_size) {
        if (is_first && way_status[ptr]) {
          min_way_index = ptr;
          ptr++;
          is_first = false;
          continue;
        }
        if (way_status[ptr] && edge_cache[ptr] < edge_cache[min_way_index]) {
          min_way_index = ptr;
        }
        ptr++;
      }
      if (next_level_way_ptr < way_num && way_status[next_level_way_ptr]) {
        if (is_first || edge_cache[next_level_way_ptr] < edge_cache[min_way_index]) {
          min_way_index = next_level_way_ptr;
        }
      }
      continue;
    }
    while (work_way > 0 && edge_cache[min_way_index].src_ == cur_vertex) {
      auto &edge_record = edge_cache[min_way_index];
#ifdef DEL_EDGE_SEPARATE
      if (way_have_del_edge[min_way_index] == true) {
        SequenceNumber_t eid = edge_record.seq_;
        SequenceNumber_t del_time;
        if (del_record_manager_.get_time(way_map[min_way_index], eid, del_time)) {
          del_record_manager_.del_eid_frome_eidmap(way_map[min_way_index], eid);

          edge_record.marker_ = true;
        }
      }
#endif

      if (!writer.Write(edge_record)) {  // if output file full
        temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
        writer.Init(utils::eFileName(temp_currentTime), utils::pFileName(temp_currentTime), edge_record.src_,
                    temp_currentTime);
        bool rt = writer.Write(edge_record);
        assert(rt == true);
      }
      auto new_edge_record_opt = ways[min_way_index].NextEdgeRecord();

#ifdef DEL_EDGE_SEPARATE
      if (edge_record.marker_ == true) {
        assert(new_edge_record_opt != std::nullopt);
        edge_cache[min_way_index] = new_edge_record_opt.value();
        continue;
      }
#endif

      if (new_edge_record_opt == std::nullopt) {
        way_status[min_way_index] = false;
        ways[min_way_index].FreeBuffer();
        work_way--;
        if (work_way <= 0) {
          break;
        }
        if (min_way_index == next_level_way_ptr) {
          next_level_way_ptr++;
          if (next_level_way_ptr < way_num) {
            ways[next_level_way_ptr].Init();
            edge_cache.emplace_back(ways[next_level_way_ptr].NextEdgeRecord().value());
          }
        }
      } else {
        edge_cache[min_way_index] = new_edge_record_opt.value();
      }

      // find min edge
      int  ptr      = level_way_ptr;
      bool is_first = true;
      while (ptr < inputs_0_size) {
        if (is_first && way_status[ptr]) {
          min_way_index = ptr;
          ptr++;
          is_first = false;
          continue;
        }
        if (way_status[ptr] && edge_cache[ptr] < edge_cache[min_way_index]) {
          min_way_index = ptr;
        }
        ptr++;
      }
      if (next_level_way_ptr < way_num && way_status[next_level_way_ptr]) {
        if (is_first || edge_cache[next_level_way_ptr] < edge_cache[min_way_index]) {
          min_way_index = next_level_way_ptr;
        }
      }
      assert(way_status[min_way_index] == true);
    }
  }

  if (!writer.IsClear()) {
    writer.WriteEnd();
  }
  edge_cache.clear();
}

void Compaction::UpdataLevelIndex(SSTableCache *it, const int level) {
  std::ifstream file(it->path, std::ios::binary);
  if (!file) {
    printf("Fail to open file %s\n", it->path.c_str());
    exit(-1);
  }
  int64_t index_length = it->header.index_size;
  char   *indexBuf     = new char[index_length * 12];
  file.seekg(it->header.size * sizeof(EdgeBody_t), std::ios::beg);

  file.read(indexBuf, index_length * 12);
  // update levelindex
  assert(level > 0);
  for (int32_t i = 0; i < index_length - 1; ++i) {
    VertexId_t key = *(uint64_t *)(indexBuf + 12 * i);
    write_max(&vertex_max_level_[key], Level_t(level + 2));
    if (FLAGS_support_mulversion == false) {
      int index_id = key * LEVEL_INDEX_SIZE + level - 1;
      memcpy(&vid_to_levelIndex[index_id + 1], &vid_to_levelIndex[index_id], sizeof(LevelIndex));
      vid_to_levelIndex[index_id].set_fileID(INVALID_File_ID);
    } else {
      vertex_rwlocks_[key].WriteLock();

      auto &multi_index = (*vid_to_mem_disk_index_)[key];

      MemDiskIndex *index = multi_index.Get(level);

      index->level = level + 1;
      vertex_rwlocks_[key].WriteUnlock();
    }
  }

  delete[] indexBuf;
  file.close();
}

void Compaction::DirectAdjustLevel(const uint level) {
  assert(level >= 1);
  assert(level + 1 < fileMetaCache_.size());
  // add inputs[0] to level+1
  for (auto it : inputs_[0]) {
    fileMetaCache_[level + 1]->emplace_back(it);
    // build file levelindex
    UpdataLevelIndex(it, level);
  }
  std::sort(fileMetaCache_[level + 1]->begin(), fileMetaCache_[level + 1]->end(),
            [](const SSTableCache *a, const SSTableCache *b) {
              return (a->header).minKey < (b->header).minKey;  // Sort minkey from small to large
            });
  RemoveCache(level, 0, false);
}

void Compaction::PreCompaction(const int level, std::vector<Way> &ways) {
  for (auto it : inputs_[0]) {
    ways.emplace_back(it, buffer_manager);
  }
  for (auto it : inputs_[1]) {
    ways.emplace_back(it, buffer_manager);
  }

  if (FLAGS_support_mulversion == false) {
    RemoveCache(level, 0, true);
    RemoveCache(level + 1, 1, true);
    RemoveFile();
  } else {
    RemoveCache(level, 0, false);
    RemoveCache(level + 1, 1, false);
  }
}

void Compaction::DoGeneralCompactionWork(const int level) {
  int reseve_size = inputs_[0].size() + inputs_[1].size();
  assert(reseve_size > 0);
  if (cache_max_size < reseve_size) {
    cache_max_size = reseve_size;
    ways.reserve(cache_max_size);
  }

  int inputs_0_num = inputs_[0].size();
  int inputs_1_num = inputs_[1].size();

  PreCompaction(level, ways);

  if ((!(max_subcompactions_ > 1 && (fileMetaCache_[level]->size() > utils::getLevelMaxSize(level))) || level == 0)) {
    ProcessCompactionWork(level, inputs_0_num, inputs_1_num, edge_cache, ways);
  } else {
    assert(level > 0);

    std::vector<std::thread> thread_pool;
    thread_pool.reserve(max_subcompactions_);

    VertexId_t all_start, all_limit;
    GetRange2(inputs_[0], inputs_[1], all_start, all_limit);
    VertexId_t first_compaction_min_key = all_start;

    std::vector<std::vector<Way>>        sub_ways;
    std::vector<std::vector<EdgeRecord>> sub_edge_caches;
    std::vector<int>                     inputs_0_num_vec;
    std::vector<int>                     inputs_1_num_vec;
    sub_ways.reserve(max_subcompactions_);
    sub_edge_caches.reserve(max_subcompactions_);
    inputs_0_num_vec.reserve(max_subcompactions_);
    inputs_1_num_vec.reserve(max_subcompactions_);
    uint thread_id = 0;

    thread_pool.emplace_back(&Compaction::ProcessCompactionWork, this, level, inputs_0_num, inputs_1_num,
                             std::ref(edge_cache), std::ref(ways));

    while (fileMetaCache_[level]->size() > utils::getLevelMaxSize(level) && thread_id < max_subcompactions_) {
      compact_pointer_[level] = last_merge_max_key_;

      inputs_[0].clear();
      inputs_[1].clear();
      merged_fids_[0].clear();
      merged_fids_[1].clear();
      bool is_reverse = false;

      {
        std::lock_guard<std::mutex> lock(*tablecache_mutex_[level]);
        std::lock_guard<std::mutex> lock2(*tablecache_mutex_[level + 1]);
        is_reverse = PickCompactioFile(level, false);
      }

      {
        VertexId_t all_start, all_limit;
        GetRange2(inputs_[0], inputs_[1], all_start, all_limit);
        if (is_reverse && all_limit < first_compaction_min_key) {
          // LOG_INFO("skip the result.");
          break;
        }
      }
      sub_ways.emplace_back(std::vector<Way>());
      sub_edge_caches.emplace_back(std::vector<EdgeRecord>());

      for (auto it : inputs_[0]) {
        merged_fids_[0].insert(it->header.timeStamp);
        if (FLAGS_support_mulversion == true) {
          delete_filemeta_set_.push_back(it);
        }
      }
      for (auto it : inputs_[1]) {
        merged_fids_[1].insert(it->header.timeStamp);
        if (FLAGS_support_mulversion == true) {
          delete_filemeta_set_.push_back(it);
        }
      }

      int inputs_0_num = inputs_[0].size();
      int inputs_1_num = inputs_[1].size();

      PreCompaction(level, sub_ways[thread_id]);

      inputs_0_num_vec.emplace_back(inputs_0_num);
      inputs_1_num_vec.emplace_back(inputs_1_num);

      thread_id++;
    }

    for (uint t_id = 0; t_id < thread_id; t_id++) {
      thread_pool.emplace_back(&Compaction::ProcessCompactionWork, this, level, inputs_0_num_vec[t_id],
                               inputs_1_num_vec[t_id], std::ref(sub_edge_caches[t_id]), std::ref(sub_ways[t_id]));
    }

    for (auto &thread : thread_pool) {
      thread.join();
    }
  }
}

void Compaction::BackgroundCompaction() {
  // LOG_INFO("Do compaction work");
  uint level = 0;

  if (FLAGS_support_mulversion == true && level == 0) {
    l0_versionset_->VersionLock();
    fileMetaCache_[0] = l0_versionset_->GetCurrent()->GetLevel0Files();
    l0_versionset_->VersionUnLock();
  }

  if (FLAGS_support_mulversion == false && delete_fids_set_.Size() > 100) {
    LOG_INFO("Note: delete_fids_set_.size()={}", delete_fids_set_.Size());
    RealRemoveFile();
  }

  while (true) {
    if (level >= fileMetaCache_.size() || fileMetaCache_[level]->size() < utils::getLevelMaxSize(level)
        || level + 1 >= MAX_LEVEL) {
      break;
    }

    inputs_[0].clear();
    inputs_[1].clear();
    ways.clear();
    merged_fids_[0].clear();
    merged_fids_[1].clear();
    delete_filemeta_set_.clear();
    bool need_del_file = true;
    min_level_0_fid_   = 0;

    if (FLAGS_support_mulversion == true && level == 0) {
      l0_versionset_->VersionLock();
      fileMetaCache_[0] = l0_versionset_->GetCurrent()->GetLevel0Files();
      l0_versionset_->VersionUnLock();
    }

    {
      std::lock_guard<std::mutex> lock(*tablecache_mutex_[level]);
      std::lock_guard<std::mutex> lock2(*tablecache_mutex_[level + 1]);
      PickCompactioFile(level, true);
    }

    for (auto it : inputs_[0]) {
      assert(it->Getref() > 0);
      merged_fids_[0].insert(it->header.timeStamp);
      if (FLAGS_support_mulversion == true && level == 0) {
        version_edit_.RemoveFile(it->header.timeStamp);
        if (min_level_0_fid_ < it->header.timeStamp) {
          min_level_0_fid_ = it->header.timeStamp;
        }
      }
      if (FLAGS_support_mulversion == true) {
        delete_filemeta_set_.push_back(it);
      }
    }
    for (auto it : inputs_[1]) {
      assert(it->Getref() > 0);
      merged_fids_[1].insert(it->header.timeStamp);
      if (FLAGS_support_mulversion == true) {
        delete_filemeta_set_.push_back(it);
      }
    }

    if (level > 0 && inputs_[1].size() == 0) {
      DirectAdjustLevel(level);
      need_del_file = false;
    }
#ifndef MERGE_LARGE_VERTEX
    else if (level == 0 && max_subcompactions_ > 1) {
      // LOG_INFO("DoSubCompactionWork, level={}", level);
      DoSubCompactionWork(level);
    }
#endif
    else {
      DoGeneralCompactionWork(level);
    }

    if (FLAGS_support_mulversion == true && level == 0) {
      // update version
      Version *v = new Version(l0_versionset_);

      l0_versionset_->VersionLock();

      l0_versionset_->LogAndApply(version_edit_, v);
      fileMetaCache_[0] = l0_versionset_->GetCurrent()->GetLevel0Files();

      std::shared_ptr<VersionAndMemTable> old_vm = std::atomic_load(&sv_.version_memtable);
      std::shared_ptr<VersionAndMemTable> new_vm = std::make_shared<VersionAndMemTable>();
      new_vm->batch_insert_tb(old_vm->menTables);
      new_vm->set_vs(l0_versionset_->GetCurrent());
      std::atomic_store(&sv_.version_memtable, new_vm);

      global_version_id_.fetch_add(1, std::memory_order_acquire);

      l0_versionset_->VersionUnLock();
    }

    if (FLAGS_support_mulversion == true && need_del_file == true) {
      for (auto it : delete_filemeta_set_) {
        it->Unref();
      }
    }

    if (fileMetaCache_[level]->size() > utils::getLevelMaxSize(level)) {
      continue;
    }
    level++;
  }
}

void Compaction::MaybeScheduleCompaction() {
  uint curr_level_0file_num = 0;
  if (FLAGS_support_mulversion == true) {
    curr_level_0file_num = l0_versionset_->GetCurrent()->GetLevel0Files()->size();
  } else {
    curr_level_0file_num = fileMetaCache_[0]->size();
  }

  bool state = GetState();
  if (curr_level_0file_num >= utils::getLevelMaxSize(0) && !state && SetState(false, true)) {
    BackgroundCompaction();

    SetState(true, false);
    // LOG_INFO("Finish DoCompactionWork.");
  }
}

}  // namespace lsmg