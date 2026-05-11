#ifndef SST_DATA_CACHE_H
#define SST_DATA_CACHE_H

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cassert>
#include <cstddef>
#include <fstream>
#include "cache/block_manager.h"
#include "common/utils/logger.h"
#include "graph/edge.h"

namespace lsmg {
class SSTDataCache {
 public:
  SSTDataCache() {}

  SSTDataCache(const std::string &e_path, const size_t edge_num, const std::string &p_path, BlockManager &block_manager)
      : it_(NULLPOINTER) {
    LoadEdgesByBlockManager(e_path, edge_num, block_manager);
    LoadPropertyByBlockManager(p_path, block_manager);
  }

  SSTDataCache(const std::string &e_path, const size_t edge_num, const std::string &p_path, uintptr_t it)
      : it_(it) {
    LoadFile(e_path, p_path, edge_num);
  }

  void LoadEdgesByBlockManager(const std::string &path, const size_t edge_num, BlockManager &block_manager);

  void LoadPropertyByBlockManager(const std::string &path, BlockManager &block_manager);

  void LoadFile(const std::string &e_path, const std::string &p_path, const size_t edge_num);

  void LoadFileAndMerge(const std::string &e_path, const std::string &p_path, const size_t edge_num);

  char *GetEdgeData() {
    return edgebody_ptr_;
  }

  char *GetPropertyData() {
    return property_ptr_;
  }

  size_t GetPropertySize() {
    return property_size_;
  }

  size_t GetFileSize(const char *fileName) {
    if (fileName == NULL) {
      return 0;
    }
    struct stat statbuf;
    stat(fileName, &statbuf);
    return statbuf.st_size;
  }

  uintptr_t GetSSTableCache() {
    return it_;
  }

  int GetEFileFD() {
    return e_fd;
  }

  ~SSTDataCache() {
    if (data != nullptr) {
      munmap(data, capacity);
      LOG_INFO(" ~munmap");
    }
    if (e_fd != EMPTY_FD) {
      munmap(edgebody_ptr_, edgebody_size_);
      close(e_fd);
    }
    if (p_fd != EMPTY_FD) {
      munmap(property_ptr_, property_size_);
      close(p_fd);
    }
  }

 private:
  char                *edgebody_ptr_;
  char                *property_ptr_;
  size_t               edgebody_size_;
  size_t               property_size_;
  size_t               capacity = 0;
  void                *data     = nullptr;
  constexpr static int EMPTY_FD = -1;
  int                  e_fd     = EMPTY_FD;
  int                  p_fd     = EMPTY_FD;
  uintptr_t            it_;
};

}  // namespace lsmg

#endif