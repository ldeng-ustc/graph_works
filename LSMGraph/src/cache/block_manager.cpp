#include "cache/block_manager.h"
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <exception>
#include "common/utils/logger.h"

namespace lsmg {

BlockManager::BlockManager(const std::string path, size_t capacity)
    : capacity_(capacity)
    , mutex_() {
  if (path.empty()) {
    fd_   = EMPTY_FD;
    data_ = mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);

    if (data_ == MAP_FAILED) throw std::runtime_error("mmap block error.");
  } else {
    LOG_INFO("mmap path:{} ", path);
    fd_ = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0640);
    if (fd_ == EMPTY_FD) {
      LOG_ERROR("open block file error. path={}, errno={}, strerror:{}", path, errno, strerror(errno));
      std::terminate();
    }
    if (ftruncate(fd_, FILE_TRUNC_SIZE) != 0) {
      LOG_ERROR("ftruncate block file error.");
      throw std::runtime_error("ftruncate block file error.");
    }
    data_ = mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (data_ == MAP_FAILED) {
      LOG_ERROR("mmap block error.");
      throw std::runtime_error("mmap block error.");
    }
  }

  if (madvise(data_, capacity, MADV_RANDOM) != 0) throw std::runtime_error("madvise block error.");

  file_size_ = FILE_TRUNC_SIZE;
  used_size_ = 0;
}

BlockManager::~BlockManager() {
  msync(data_, capacity_, MS_SYNC);
  munmap(data_, capacity_);
  if (fd_ != EMPTY_FD) close(fd_);
}

uintptr_t BlockManager::alloc(size_t block_size) {
  uintptr_t pointer = used_size_.fetch_add(block_size);

  if (pointer + block_size >= file_size_) {
    auto                        new_file_size = ((pointer + block_size) / FILE_TRUNC_SIZE + 1) * FILE_TRUNC_SIZE;
    std::lock_guard<std::mutex> lock(mutex_);
    if (new_file_size >= file_size_) {
      if (fd_ != EMPTY_FD) {
        if (ftruncate(fd_, new_file_size) != 0) {
          LOG_ERROR("ftruncate block file error.");
          throw std::runtime_error("ftruncate block file error.");
        }
      }
      file_size_ = new_file_size;
    }
  }

  return pointer;
}

}  // namespace lsmg