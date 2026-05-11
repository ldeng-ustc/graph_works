#ifndef DISK_MANAGER_H
#define DISK_MANAGER_H

#include <fstream>
#include <future>
#include <mutex>
#include <string>
#include "common/config.h"

namespace lsmg {

class DiskManager {
 public:
  explicit DiskManager(const std::string &db_file);

  DiskManager() = default;

  virtual ~DiskManager() = default;

  void ShutDown();

  virtual void WritePage(page_id_t page_id, const char *page_data);

  virtual void ReadPage(page_id_t page_id, char *page_data);

  void WriteLog(char *log_data, int size);

  bool ReadLog(char *log_data, int size, int offset);

  int GetNumFlushes() const;

  bool GetFlushState() const;

  int GetNumWrites() const;

  inline void SetFlushLogFuture(std::future<void> *f) {
    flush_log_f_ = f;
  }

  bool HasFlushLogFuture() {
    return flush_log_f_ != nullptr;
  }

 protected:
  int GetFileSize(const std::string &file_name);

  std::fstream       db_io_;
  std::string        file_name_;
  int                num_flushes_{0};
  int                num_writes_{0};
  bool               flush_log_{false};
  std::future<void> *flush_log_f_{nullptr};
  std::mutex         db_io_latch_;
};

}  // namespace lsmg

#endif