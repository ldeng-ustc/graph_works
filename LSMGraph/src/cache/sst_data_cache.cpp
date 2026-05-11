#include "cache/sst_data_cache.h"

namespace lsmg {
void SSTDataCache::LoadEdgesByBlockManager(const std::string &path, const size_t edge_num,
                                           BlockManager &block_manager) {
  edgebody_size_ = sizeof(EdgeBody_t) * edge_num;
  edgebody_ptr_  = reinterpret_cast<char *>(block_manager.get_data()) + block_manager.alloc(edgebody_size_);
  std::ifstream file(path, std::ios::binary | std::ios::in);
  file.seekg(0);
  auto read_bytes = file.readsome((char *)edgebody_ptr_, edgebody_size_);
  assert(edgebody_size_ == static_cast<size_t>(read_bytes));
  file.close();
}

void SSTDataCache::LoadPropertyByBlockManager(const std::string &path, BlockManager &block_manager) {
  property_size_ = GetFileSize(path.data());
  property_ptr_  = reinterpret_cast<char *>(block_manager.get_data()) + block_manager.alloc(property_size_);
  std::ifstream file(path, std::ios::binary | std::ios::in);
  auto          read_bytes = file.readsome((char *)property_ptr_, property_size_);
  assert(property_size_ == static_cast<size_t>(read_bytes));
  file.close();
}
void SSTDataCache::LoadFile(const std::string &e_path, const std::string &p_path, const size_t edge_num) {
  edgebody_size_ = sizeof(EdgeBody_t) * edge_num;
  property_size_ = GetFileSize(p_path.data());

  {
    e_fd = open(e_path.c_str(), O_RDONLY);
    if (e_fd == EMPTY_FD) throw std::runtime_error("open error. e_path=" + e_path);
    lseek(e_fd, 0, SEEK_SET);
    edgebody_ptr_ = reinterpret_cast<char *>(mmap(nullptr, edgebody_size_, PROT_READ, MAP_PRIVATE, e_fd, 0));
    if (data == MAP_FAILED) throw std::runtime_error("mmap edgebody error.");
  }

  {
    p_fd = open(p_path.c_str(), O_RDONLY);
    if (p_fd == EMPTY_FD) throw std::runtime_error("open error. p_path=" + p_path);
    lseek(p_fd, 0, SEEK_SET);
    property_ptr_ = reinterpret_cast<char *>(mmap(nullptr, property_size_, PROT_READ, MAP_PRIVATE, p_fd, 0));
    if (data == MAP_FAILED) throw std::runtime_error("mmap property error.");
  }
}

void SSTDataCache::LoadFileAndMerge(const std::string &e_path, const std::string &p_path, const size_t edge_num) {
  edgebody_size_ = sizeof(EdgeBody_t) * edge_num;
  property_size_ = GetFileSize(p_path.data());
  capacity       = edgebody_size_ + property_size_;

  data = mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (data == MAP_FAILED) throw std::runtime_error("mmap file error.");

  {
    edgebody_ptr_ = reinterpret_cast<char *>(data);
    int e_fd      = open(e_path.c_str(), O_RDONLY);
    if (e_fd == -1) throw std::runtime_error("open error. e_path=" + e_path);
    lseek(e_fd, 0, SEEK_SET);
    auto read_bytes = read(e_fd, (char *)edgebody_ptr_, edgebody_size_);
    assert(edgebody_size_ == static_cast<size_t>(read_bytes));
    close(e_fd);
  }

  {
    property_ptr_ = reinterpret_cast<char *>(data) + edgebody_size_;
    int p_fd      = open(p_path.c_str(), O_RDONLY);
    if (p_fd == -1) throw std::runtime_error("open error. p_path=" + p_path);
    lseek(p_fd, 0, SEEK_SET);
    auto read_bytes = read(p_fd, (char *)property_ptr_, property_size_);
    assert(property_size_ == static_cast<size_t>(read_bytes));
    close(p_fd);
  }
}

}  // namespace lsmg