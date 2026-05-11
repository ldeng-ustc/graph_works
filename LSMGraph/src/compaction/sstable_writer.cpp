#include "compaction/sstable_writer.h"
#include <sys/types.h>
#include "common/flags.h"
#include "common/utils/atomic.h"
#include "common/utils/logger.h"

#include <algorithm>
#include <cstdint>
#include <exception>

namespace lsmg {

bool SSTableWriter::Write(EdgeRecord &edge_record) {
  auto const src = edge_record.src_;
  bloom_filter_.add(src, edge_record.dst_);

  if (src != cur_src_vtx_) {
    if (CurSize() >= MAX_EFILE_SiZE) {
      WriteEnd();
      return false;
    }
    cur_src_vtx_                     = src;
    edge_index_buffer_[index_cnt_++] = Index{src, (EdgeOffset_t)(edge_cnt_ * sizeof(EdgeBody_t))};
    assert(index_cnt_ < MAX_INDEX_NUM);
  }

  if (p_ptr_ + edge_record.prop_.size() >= PROPERTY_BUFFER_SIZE) {
    [[maybe_unused]] auto write_bytes = write(p_file_fd, p_file_content_, p_ptr_);
    p_ptr_                            = 0;
  }

  assert(p_ptr_ + edge_record.prop_.size() < PROPERTY_BUFFER_SIZE);
  memcpy(p_file_content_ + p_ptr_, edge_record.prop_.data(), edge_record.prop_.size());
  p_ptr_ += edge_record.prop_.size();
  assert(p_ptr_ < PROPERTY_BUFFER_SIZE);

  edge_body_buffer_[edge_ptr_++] = EdgeBody_t{edge_record.dst_, edge_record.seq_, p_file_size_, edge_record.marker_};
  if (edge_ptr_ >= BODY_BUFFER_SIZE - 1) {
    [[maybe_unused]] auto write_bytes = write(e_file_fd, edge_body_buffer_, edge_ptr_ * sizeof(EdgeBody_t));
    edge_ptr_                         = 0;
  }

  p_file_size_ += edge_record.prop_.size();
  edge_cnt_++;

  return true;
}

void SSTableWriter::WriteEnd() {
  assert(edge_ptr_ + 1 < BODY_BUFFER_SIZE);
  edge_body_buffer_[edge_ptr_++] = EdgeBody_t{INVALID_VERTEX_ID, 0, p_file_size_, 0};
  auto write_bytes               = write(e_file_fd, edge_body_buffer_, edge_ptr_ * sizeof(EdgeBody_t));

  assert(index_cnt_ + 1 < MAX_INDEX_NUM);
  edge_index_buffer_[index_cnt_++] = Index{INVALID_VERTEX_ID, (EdgeOffset_t)(edge_cnt_ * sizeof(EdgeBody_t))};
  write_bytes                      = write(e_file_fd, edge_index_buffer_, index_cnt_ * sizeof(Index));

  write_bytes = write(e_file_fd, &bloom_filter_, BLOOM_FILTER_SIZE);

  SSTableCache *temp_filemeta_cache = new SSTableCache(sstdata_manager_);

  *(temp_filemeta_cache->bloomFilter) = bloom_filter_;

  temp_filemeta_cache->path = path_;

  temp_filemeta_cache->header.size       = edge_cnt_ + 1;
  temp_filemeta_cache->header.index_size = index_cnt_;
  temp_filemeta_cache->header.timeStamp  = timestamp_;
  temp_filemeta_cache->header.minKey     = first_src_;
  temp_filemeta_cache->header.maxKey     = cur_src_vtx_;
  write_bytes                            = write(e_file_fd, &temp_filemeta_cache->header, sizeof(Header));
  assert(write_bytes != -1);
  write_bytes = write(p_file_fd, p_file_content_, p_ptr_);
  assert(write_bytes != -1);

  close(e_file_fd);
  close(p_file_fd);

  sstdata_manager_.put_data(timestamp_, temp_filemeta_cache->header.size,
                            reinterpret_cast<uintptr_t>(temp_filemeta_cache));

  if (FLAGS_support_mulversion == false) {
    for (uint32_t i = 0; i < index_cnt_ - 1; ++i) {
      int index_id = edge_index_buffer_[i].key * LEVEL_INDEX_SIZE + level_;
      if (level_ > 0) {
        FileId_t input0_fid = vid_to_levelIndex_[index_id - 1].get_fileID();
        if (input_0_fidset_.find(input0_fid) != input_0_fidset_.end()) {
          vid_to_levelIndex_[index_id - 1].set_fileID(INVALID_File_ID);
        }
      }
      LevelIndex &findex = vid_to_levelIndex_[index_id];
      FileId_t    fid    = timestamp_;
      findex.set_fileID(fid);
      findex.set_offset(edge_index_buffer_[i].offset);
      findex.set_next_offset(edge_index_buffer_[i + 1].offset);
      write_max(&vertex_max_level_[edge_index_buffer_[i].key], Level_t(level_ + 2));
    }
  } else {
    for (uint32_t i = 0; i < index_cnt_ - 1; ++i) {
      vertex_rwlocks_[edge_index_buffer_[i].key].WriteLock();

      auto    &multi_index = vid_to_mem_disk_index_[edge_index_buffer_[i].key];
      FileId_t fid         = timestamp_;

      if (level_ > 0) {
        MemDiskIndex *index = multi_index.Get(level_);
        if (index != nullptr && input_0_fidset_.find(index->fileID) != input_0_fidset_.end()) {
          multi_index.Delete(level_);
        }
      } else {
        multi_index.SetL0FileID(min_level_0_fid_ + 1);
      }
      MemDiskIndex *index = multi_index.Get(level_ + 1);
      if (index == nullptr) {
        multi_index.Insert(
            MemDiskIndex{(uint32_t)level_ + 1, fid, edge_index_buffer_[i].offset, edge_index_buffer_[i + 1].offset});
      } else {
        *index =
            MemDiskIndex{(uint32_t)level_ + 1, fid, edge_index_buffer_[i].offset, edge_index_buffer_[i + 1].offset};
      }

      write_max(&vertex_max_level_[edge_index_buffer_[i].key], Level_t(level_ + 2));
      vertex_rwlocks_[edge_index_buffer_[i].key].WriteUnlock();
    }
  }

  std::lock_guard<std::mutex> lock(*tablecache_mutex_[level_ + 1]);
  fileMetaCache_[level_ + 1]->push_back(temp_filemeta_cache);
  std::sort(fileMetaCache_[level_ + 1]->begin(), fileMetaCache_[level_ + 1]->end(),
            [](const SSTableCache *a, const SSTableCache *b) {
              return (a->header).minKey < (b->header).minKey;  // Sort minkey from small to large
            });
}

void SSTableWriter::get_edge_frome_file(std::ifstream &file, Edge &edge, uint32_t obj_offset, std::string &efile_path,
                                        size_t all_edge_num, std::string *property) {
  uint32_t edge_body_size = sizeof(EdgeBody_t);

  std::ifstream p_file(efile_path + "_p", std::ios::binary);
  if (!p_file) {
    printf("Lost file: %s_p", (efile_path.c_str()));
    exit(-1);
  }

  file.seekg(obj_offset);

  EdgeBody_t edge_body;
  file.read(reinterpret_cast<char *>(&edge_body), sizeof(EdgeBody_t));
  EdgePropertyOffset_t prop_pointer_ = edge_body.get_prop_pointer();

  edge.set_destination(edge_body.get_dst());
  edge.set_sequence(edge_body.get_seq());
  edge.set_marker(edge_body.get_marker());

  EdgeBody_t next_edgebody;
  file.seekg(obj_offset + edge_body_size);
  file.read(reinterpret_cast<char *>(&next_edgebody), sizeof(EdgeBody_t));
  LOG_INFO("       next edge: ");
  next_edgebody.print();
  EdgePropertyOffset_t next_prop_pointer_ = next_edgebody.get_prop_pointer();

  uint32_t length = next_prop_pointer_ - prop_pointer_;
  if (length > 0) {
    property->resize(length);
    p_file.seekg(prop_pointer_);
    p_file.read(&(*property)[0], length);
    edge.set_property(*property);
  }

  p_file.close();
}

void SSTableWriter::print_ssTable(std::string path, uint print_size) {
  LOG_INFO("\n---------------(read sst from file)----------------");
  LOG_INFO(" print_size={}", print_size);
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    printf("Fail to open file %s\n", path.c_str());
    exit(-1);
  }
  LOG_INFO("open sst file: {}", path);

  // load head
  Header   header;
  uint64_t head_offset = -(HEADER_SIZE);
  file.seekg(head_offset, std::ios::end);
  file.read((char *)&header, sizeof(Header));

  LOG_INFO("Head: ");
  LOG_INFO("  header.timeStamp:{}", header.timeStamp);
  LOG_INFO("  header.size:{}", header.size);
  LOG_INFO("  header.index_size:{}", header.index_size);
  LOG_INFO("  header.minKey:{}", header.minKey);
  LOG_INFO("  header.maxKey:{}", header.maxKey);

  file.seekg(header.size * sizeof(EdgeBody_t), std::ios::beg);
  LOG_INFO("Index:");
  std::vector<Index> index_list;
  for (uint i = 1; i <= header.index_size && i < print_size; i++) {
    LOG_INFO("  index_{}: ", i);
    Index index;
    file.read((char *)&index, sizeof(Index));
    index_list.push_back(index);
    LOG_INFO(" type.size={} srcId:{} body_offset:{}", sizeof(Index), static_cast<unsigned long>(index.key),
             static_cast<unsigned int>(index.offset));
  }

  // read edge including property
  LOG_INFO("Edge:");
  for (size_t i = 0; i < index_list.size() - 1 && i < print_size; i++) {
    uint32_t e_start = index_list[i].offset;
    uint32_t e_end   = index_list[i + 1].offset;
    LOG_INFO(" src={}", static_cast<unsigned long>(index_list[i].key));
    file.seekg(e_start, std::ios::beg);
    for (uint32_t adr = e_start; adr < e_end; adr += sizeof(EdgeBody_t)) {
      std::string p;
      Edge        edge(index_list[i].key);
      get_edge_frome_file(file, edge, adr, path, header.size, &p);
      edge.print();
    }
  }

  LOG_INFO("---------------finish------------\n");

  file.close();
}

}  // namespace lsmg