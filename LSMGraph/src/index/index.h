#ifndef LSMG_INEDEX_HEADER
#define LSMG_INEDEX_HEADER

#include <fcntl.h>
#include <fmt/core.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cassert>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <queue>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/utils/concurrent_hash_map.h"
#include "common/utils/logger.h"
#include "disk_level_index.h"
#include "graph/edge.h"

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)
#define LEVEL_NUM_MASK 0xE000000000000000
#define INDEX_MASK     0x1FFFFFFFFFFFFFFF

namespace lsmg {

#define MMAP_LEVEL_INDEX  // read form mmap array

// cold/hot index
#define INDEX_MAP_TYPE 1  // 0: array, 1: hashmap, 2: unordermap
#define COLD_LEVEL_NUM 2  // disk
#define HOT_LEVEL_NUM  4  // memory
// #define MMAP_HOT
// #define MMAP_COLD_HOT_LEVEL_INDEX

struct Header {
  uint64_t timeStamp;   // file id
  uint64_t size;        // edge num
  uint64_t index_size;  // src  num
  uint64_t minKey, maxKey;

  Header()
      : timeStamp(0)
      , size(0)
      , index_size(0)
      , minKey(0)
      , maxKey(0) {}
};

struct __attribute__((__packed__)) Index {
  VertexId_t   key;
  EdgeOffset_t offset;

  Index(uint64_t k = 0, uint32_t o = 0)
      : key(k)
      , offset(o) {}
};

struct __attribute__((__packed__)) File_Index {
  uint32_t fileID;
  uint32_t offset;
  uint32_t next_offset;

  File_Index(uint32_t _fileID = 0, EdgeOffset_t _offset = 0, EdgeOffset_t _next_offset = 0)
      : fileID(_fileID)
      , offset(_offset)
      , next_offset(_next_offset) {}
};

struct Range {
  uint64_t min, max;

  Range(const uint64_t &i, const uint64_t &a)
      : min(i)
      , max(a) {}
};

class LevelIndex {
 public:
  LevelIndex()
      : fileID(INVALID_File_ID)
      , offset(INVALID_OFFSET)
      , next_offset(INVALID_OFFSET) {}

  LevelIndex(uint32_t _fid, uint32_t _offset, uint32_t _next_offset)
      : fileID(_fid)
      , offset(_offset)
      , next_offset(_next_offset) {}

  void init() {
    fileID      = INVALID_File_ID;
    offset      = INVALID_OFFSET;
    next_offset = INVALID_OFFSET;
  }

  void set_fileID(uint32_t fid) {
    this->fileID = fid;
  }

  uint32_t get_fileID() const {
    return this->fileID;
  }

  void set_offset(uint32_t offset) {
    this->offset = offset;
  }

  uint32_t get_offset() const {
    return this->offset;
  }

  void set_next_offset(uint32_t next_offset) {
    this->next_offset = next_offset;
  }

  uint32_t get_next_offset() const {
    return this->next_offset;
  }

 private:
  uint32_t fileID      = INVALID_File_ID;
  uint32_t offset      = INVALID_OFFSET;
  uint32_t next_offset = INVALID_OFFSET;
};

class MulLevelIndex {
 public:
  struct LevelData {
    FileId_t fileID      = INVALID_File_ID;
    uint32_t offset      = INVALID_OFFSET;
    uint32_t next_offset = INVALID_OFFSET;
    bool     operator==(const LevelData &other) const {
          return fileID == other.fileID
                 && ((fileID == INVALID_File_ID) || (offset == other.offset && next_offset == other.next_offset));
    }
  };

  void init() {
    min_level_0_fid = 0;
    for (uint i = 0; i < LEVEL_INDEX_SIZE; i++) {
      set_fileID(i, INVALID_File_ID);
      levels[i].offset      = INVALID_OFFSET;
      levels[i].next_offset = INVALID_OFFSET;
    }
  }

  void set_level_data(int level, FileId_t fid, uint32_t offset, uint32_t next_offset) {
    levels[level].fileID      = fid;
    levels[level].offset      = offset;
    levels[level].next_offset = next_offset;
  }

  const LevelData &get_level_data(int level) const {
    return levels[level];
  }

  const LevelData *get_level_data_ptr(int level) const {
    return levels;
  }

  void set_min_level_0_fid(FileId_t fid) {
    min_level_0_fid = fid;
  }

  FileId_t get_min_level_0_fid() const {
    return min_level_0_fid;
  }

  void set_fileID(int level, FileId_t fid) {
    levels[level].fileID = fid;
  }

  FileId_t get_fileID(int level) const {
    return levels[level].fileID;
  }

  bool readable(int level) const {
    return get_fileID(level) != INVALID_File_ID;
  }

  void set_offset(int level, uint32_t offset) {
    levels[level].offset = offset;
  }

  uint32_t get_offset(int level) const {
    return levels[level].offset;
  }

  void set_next_offset(int level, uint32_t next_offset) {
    levels[level].next_offset = next_offset;
  }

  uint32_t get_next_offset(int level) const {
    return levels[level].next_offset;
  }

  void copy_from_ptr(const MulLevelIndex *ptr, int level = LEVEL_INDEX_SIZE) {
    if (!ptr) return;
    const int n           = std::max(0, std::min(level, static_cast<int>(LEVEL_INDEX_SIZE)));
    this->min_level_0_fid = ptr->min_level_0_fid;
    if constexpr (std::is_trivially_copyable<LevelData>::value) {
      std::memcpy(this->levels, ptr->levels, static_cast<size_t>(n) * sizeof(LevelData));
    } else {
      std::copy_n(ptr->levels, n, this->levels);
    }
    for (size_t i = n; i < LEVEL_INDEX_SIZE; ++i) this->levels[i] = LevelData{};
  }

  void copy_from_obj(const MulLevelIndex &obj, int level = LEVEL_INDEX_SIZE) {
    const int n           = std::max(0, std::min(level, static_cast<int>(LEVEL_INDEX_SIZE)));
    this->min_level_0_fid = obj.min_level_0_fid;
    if constexpr (std::is_trivially_copyable<LevelData>::value) {
      std::memcpy(this->levels, obj.levels, static_cast<size_t>(n) * sizeof(LevelData));
    } else {
      std::copy_n(obj.levels, n, this->levels);
    }
    for (size_t i = n; i < LEVEL_INDEX_SIZE; ++i) this->levels[i] = LevelData{};
  }

  void print() const {
    LOG_INFO("min l0 fid={}", min_level_0_fid);
    for (uint i = 0; i < LEVEL_INDEX_SIZE; i++) {
      LOG_INFO("i={}, fid={}, offset={}, next_offset={}", i, get_fileID(i), get_level_data(i).offset,
               get_level_data(i).next_offset);
    }
  }

  bool operator==(const MulLevelIndex &other) const {
    if (min_level_0_fid != other.min_level_0_fid) {
      return false;
    }
    for (uint i = 0; i < LEVEL_INDEX_SIZE; ++i) {
      auto &level_this  = this->levels[i];
      auto &level_other = other.levels[i];
      if (!(level_this == level_other)) {
        return false;
      }
    }
    return true;
  }

 private:
  [[maybe_unused]] int *level_fd;
  LevelData             levels[LEVEL_INDEX_SIZE];
  uint32_t              min_level_0_fid = 0;  // minimum fid for readability
};

static_assert(std::is_same<FileId_t, uint32_t>::value, "FileId_t must be a uint32_t");
static_assert(sizeof(MulLevelIndex) == 64, "MulLevelIndex size exceeds the expected limit!");

class MulLevelIndexSharedArray {
 private:
  struct MulLevelIndexArray {
    uint64_t       vertex_num_;
    MulLevelIndex *data_;
    int            fd_;
    std::mutex     mutex;
    std::string    db_dir_;

    MulLevelIndexArray(const std::string &db_dir, uint64_t vertex_num)
        : vertex_num_(vertex_num)
        , data_(nullptr)
        , fd_(-1)
        , mutex() {
      db_dir_         = db_dir + "/multi-level";
      size_t capacity = vertex_num_ * sizeof(MulLevelIndex);

      LOG_INFO("multi-level index path={} vertex_num={}", (db_dir + "/multi-level").c_str(), vertex_num);

      fd_ = open((db_dir + "/multi-level").c_str(), O_RDWR | O_CREAT, 0666);
      if (fd_ == -1) {
        perror("Open: ");
        throw std::runtime_error("Unable to open file");
      }

      lseek(fd_, 0, SEEK_SET);
      if (ftruncate(fd_, capacity) == -1) {
        close(fd_);
        perror("Adjust mmap file error: ");
        throw std::runtime_error("Error adjusting file size");
      }

      data_ = static_cast<MulLevelIndex *>(mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
      if (data_ == MAP_FAILED) {
        close(fd_);
        perror("mmap: ");
        throw std::runtime_error("Error mapping file to memory");
      }

      if (madvise(data_, capacity, MADV_WILLNEED) != 0) throw std::runtime_error("madvise block error.");
    }

    MulLevelIndexArray &operator=(const MulLevelIndexArray &rhs) = delete;

    MulLevelIndexArray &operator=(MulLevelIndexArray &&rhs) = delete;

    ~MulLevelIndexArray() {
      if (data_ != nullptr) {
        msync(data_, vertex_num_ * sizeof(MulLevelIndex), MS_SYNC);
        munmap(data_, vertex_num_ * sizeof(MulLevelIndex));
      }
      if (fd_ != -1) {
        close(fd_);
      }
      // printf("Size of MulLevelIndexSharedArray mmap: %lu MB\n", ((vertex_num_ * sizeof(MulLevelIndex)) >> 20));
    }

    void adjust_file_size(uint64_t new_size) {
      std::lock_guard<std::mutex> lock(mutex);
      if (new_size <= vertex_num_) {
        return;
      }
      size_t new_length = new_size * sizeof(MulLevelIndex);

      vertex_num_ = new_size;
      if (fd_ != -1 && ftruncate(fd_, new_length) != 0) {
        close(fd_);
        perror("Adjust mmap file error: ");
        throw std::runtime_error("Error adjusting file size");
      }
      data_ = static_cast<MulLevelIndex *>(mmap(nullptr, new_length, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
      if (data_ == MAP_FAILED) {
        close(fd_);
        perror("mmap: ");
        throw std::runtime_error("Error mapping file to memory");
      }

      LOG_INFO("Adjusting Level Index File Size to {} MB", ((new_length) >> 20));
    }

    void copy_index_from_file(VertexId_t src, MulLevelIndex *index) {
      size_t index_offset = sizeof(MulLevelIndex) * src;

      int fd = open(db_dir_.c_str(), O_RDONLY);
      if (fd == -1) {
        perror("open file: ");
        throw std::runtime_error(db_dir_);
      }

      ssize_t size = pread(fd, reinterpret_cast<char *>(index), sizeof(MulLevelIndex), index_offset);

      if (size <= 0) {
        close(fd_);
        perror("mmap: ");
        throw std::runtime_error("Error index.sieze <= 0");
      }

      close(fd);
    }
  };

  std::shared_ptr<MulLevelIndexArray> arr_ptr_;

 public:
  MulLevelIndexSharedArray()
      : arr_ptr_(nullptr) {}

  MulLevelIndexSharedArray(const std::string &db_dir, uint64_t vertex_num)
      : arr_ptr_(new MulLevelIndexArray(db_dir, vertex_num)) {}

  MulLevelIndex &operator[](size_t index) {
    if (unlikely(index >= arr_ptr_->vertex_num_)) {
      uint64_t file_size = ((index << 6) / MMAP_CHUNK_SIZE + 1) * MMAP_CHUNK_SIZE;  // vertex_num << 6 = file size
      arr_ptr_->adjust_file_size(file_size >> 6);                                   // file_size >> 6 = vertex_num
    }
    return arr_ptr_->data_[index];
  }

  void copy_index_from_file(VertexId_t src, MulLevelIndex *index) {
    arr_ptr_->copy_index_from_file(src, index);
  }
};

struct MemDiskIndex {
  uint32_t level       = INVALID_LEVEL;
  FileId_t fileID      = INVALID_File_ID;
  uint32_t offset      = INVALID_OFFSET;
  uint32_t next_offset = INVALID_OFFSET;
};

template <size_t MEM_INDEX_LENGTH, size_t DISK_INDEX_LENGTH>
class MulLevelMemDiskIndex {
 public:
  explicit MulLevelMemDiskIndex(DiskIndexManager<MemDiskIndex, DISK_INDEX_LENGTH> *disk_index_manager)
      : disk_index_manager_(disk_index_manager)
      , mem_index_()
      , disk_page_idx_(INVALID_OFFSET)
      , mem_index_num_(0)
      , disk_index_num_(0)
      , l0_min_fid_(0)
      , expand_to_disk(false) {}

  bool Update(MemDiskIndex index_entry) {
    if (index_entry.level == 0) {
      l0_min_fid_ = index_entry.fileID;
      return true;
    }

    bool   is_disk_index = false;
    size_t pos           = INVALID_OFFSET;

    bool founded = GetLevelIndexPosition(index_entry.level, pos, is_disk_index);

    if (!founded) {
      return false;
    }

    if (is_disk_index) {
      *GetDiskIndex(pos) = index_entry;
    } else {
      mem_index_[pos] = index_entry;
    }

    return true;
  }

  void Delete(uint32_t level) {
    if (level == 0) {
      l0_min_fid_ = INVALID_File_ID;
      return;
    }
    bool   is_disk_index = false;
    size_t pos           = INVALID_OFFSET;

    bool founded = GetLevelIndexPosition(level, pos, is_disk_index);
    assert(founded);

    if (!is_disk_index) {
      RemoveMemIndex(pos);
      if (!expand_to_disk) {
        return;
      }
      mem_index_[mem_index_num_] = *GetDiskIndex(0);

      *GetDiskIndex(0) = MemDiskIndex{};

      ++mem_index_num_;
      pos = 0;
    }

    RemoveDiskIndex(pos);
    if (disk_index_num_ > 0) {
      return;
    }

    assert(mem_index_num_ == MEM_INDEX_LENGTH);

    *GetDiskIndex(0) = MemDiskIndex{};

    disk_index_manager_->Delete(disk_page_idx_);

    disk_index_num_ = 0;
    disk_page_idx_  = INVALID_OFFSET;
    expand_to_disk  = false;
  }

  void Insert(MemDiskIndex index_entry) {
    if (index_entry.level == 0) {
      l0_min_fid_ = index_entry.fileID;
      return;
    }
    if (!expand_to_disk && mem_index_num_ < MEM_INDEX_LENGTH) {
      mem_index_[mem_index_num_] = index_entry;
      ++mem_index_num_;
      return;
    }

    if (disk_index_num_ == 0) {
      disk_page_idx_ = disk_index_manager_->AllocIndexList();
      expand_to_disk = true;
    }

    assert(disk_index_num_ < DISK_INDEX_LENGTH);
    *GetDiskIndex(disk_index_num_) = index_entry;
    ++disk_index_num_;
  }

  FileId_t GetL0FileID() const {
    return l0_min_fid_;
  }

  void SetL0FileID(FileId_t fid) {
    l0_min_fid_ = fid;
  }

  MemDiskIndex *Get(size_t level) {
    if (level == 0) {
      return nullptr;
    }
    bool   is_disk_index = false;
    size_t pos           = INVALID_OFFSET;

    bool founded = GetLevelIndexPosition(level, pos, is_disk_index);
    if (!founded) {
      return nullptr;
    }
    if (is_disk_index) {
      return disk_index_manager_->Get(disk_page_idx_) + pos;
    }
    return mem_index_ + pos;
  }

  MulLevelIndex ToMulLevelIndex() {
    MulLevelIndex result;
    result.set_min_level_0_fid(l0_min_fid_);
    for (uint i = 0; i < mem_index_num_; ++i) {
      result.set_level_data(mem_index_[i].level - 1, mem_index_[i].fileID, mem_index_[i].offset,
                            mem_index_[i].next_offset);
    }

    if (expand_to_disk) {
      for (uint i = 0; i < disk_index_num_; ++i) {
        MemDiskIndex *disk_index_ = GetDiskIndex(i);
        result.set_level_data(disk_index_->level - 1, disk_index_->fileID, disk_index_->offset,
                              disk_index_->next_offset);
      }
    }

    return result;
  }

 private:
  DiskIndexManager<MemDiskIndex, DISK_INDEX_LENGTH> *disk_index_manager_ = nullptr;

  MemDiskIndex mem_index_[MEM_INDEX_LENGTH];  // begin from Level 1
  uint32_t     disk_page_idx_;
  uint32_t     mem_index_num_  = 1;  // L0 must have index entry
  uint32_t     disk_index_num_ = 0;
  FileId_t     l0_min_fid_     = 0;
  bool         expand_to_disk  = false;

  MemDiskIndex *GetDiskIndex(size_t idx) {
    return disk_index_manager_->Get(disk_page_idx_) + idx;
  }

  bool GetLevelIndexPosition(size_t level, size_t &pos, bool &is_disk_index) {
    for (uint i = 0; i < mem_index_num_ && mem_index_[i].level != INVALID_LEVEL; ++i) {
      if (mem_index_[i].level == level) {
        pos           = i;
        is_disk_index = false;
        return true;
      }
    }

    if (expand_to_disk) {
      MemDiskIndex *disk_index = disk_index_manager_->Get(disk_page_idx_);
      for (uint i = 0; i < disk_index_num_ && disk_index[i].level != INVALID_LEVEL; ++i) {
        if (disk_index[i].level == level) {
          pos           = i;
          is_disk_index = true;
          return true;
        }
      }
    }
    return false;
  }

  void RemoveMemIndex(size_t begin_idx) {
    --mem_index_num_;
    for (uint i = begin_idx; i < mem_index_num_; ++i) {
      mem_index_[i] = mem_index_[i + 1];
    }
    mem_index_[mem_index_num_] = MemDiskIndex{};
  }

  void RemoveDiskIndex(size_t begin_idx) {
    --disk_index_num_;
    MemDiskIndex *disk_index = disk_index_manager_->Get(disk_page_idx_);
    for (uint i = begin_idx; i < disk_index_num_; ++i) {
      disk_index[i] = disk_index[i + 1];
    }
    disk_index[disk_index_num_] = MemDiskIndex{};
  }
};

template <size_t MEM_INEDEX_LENGTH, size_t DISK_INDEX_LENGTH>
class MulLevelMemDiskIndexManager {
 public:
  using MultiLevelMemDiskIndex = MulLevelMemDiskIndex<MEM_INEDEX_LENGTH, DISK_INDEX_LENGTH>;
  explicit MulLevelMemDiskIndexManager(const std::string &db_path, uint64_t vertex_num)
      : disk_index_manager_(db_path, vertex_num)
      , mem_disk_index_(vertex_num, MultiLevelMemDiskIndex(&disk_index_manager_))
      , vertex_num_(vertex_num) {}

  size_t Size() const {
    return vertex_num_;
  }

  MultiLevelMemDiskIndex &operator[](size_t idx) {
    return mem_disk_index_[idx];
  }

 private:
  DiskIndexManager<MemDiskIndex, DISK_INDEX_LENGTH> disk_index_manager_;

  std::vector<MulLevelMemDiskIndex<MEM_INEDEX_LENGTH, DISK_INDEX_LENGTH>> mem_disk_index_;

  VertexId_t vertex_num_;
};

constexpr size_t MEM_INDEX_NUM  = 1;
constexpr size_t DISK_INDEX_NUM = MAX_LEVEL - MEM_INDEX_NUM - 1;  // MAX_LEVEL - L0 - MEM_INDEX

constexpr uint64_t HEADER_SIZE      = sizeof(Header);
constexpr uint64_t MAX_INDEX_NUM    = (MAX_TABLE_SIZE - HEADER_SIZE - BLOOM_FILTER_SIZE) / sizeof(EdgeBody_t) + 1;
const uint64_t     BODY_BUFFER_SIZE = (4 * (1 << 20) / sizeof(EdgeBody_t));  // 1024000;
}  // namespace lsmg

#endif