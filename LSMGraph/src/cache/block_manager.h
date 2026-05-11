#ifndef LSMG_BLOCK_MANAGER_HEADER
#define LSMG_BLOCK_MANAGER_HEADER

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>

namespace lsmg {
class BlockManager {
 public:
  constexpr static uintptr_t NULLPOINTER = 0;  // UINTPTR_MAX;

  BlockManager(const std::string path, size_t capacity = 1ul << 40);

  ~BlockManager();

  uintptr_t alloc(size_t block_size);

  void *get_data() {
    return data_;
  }

 private:
  constexpr static int    EMPTY_FD        = -1;
  constexpr static size_t FILE_TRUNC_SIZE = 1ul << 30;  // 1GB

  const size_t        capacity_;
  int                 fd_;
  void               *data_;
  std::mutex          mutex_;
  std::atomic<size_t> used_size_, file_size_;
};

}  // namespace lsmg

#endif