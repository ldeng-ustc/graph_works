#ifndef DISK_SCHEDULER_H
#define DISK_SCHEDULER_H

#include <condition_variable>
#include <cstddef>
#include <future>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

#include "common/utils/channel.h"
#include "storage/disk/disk_manager.h"

#define CONCURRENT_IO
namespace lsmg {

struct DiskRequest {
  bool               is_write_;
  char              *data_;
  page_id_t          page_id_;
  std::promise<bool> callback_;
};

class SingleThreadScheduler {
 public:
  explicit SingleThreadScheduler(DiskManager *disk_manager);
  ~SingleThreadScheduler();

  void Schedule(DiskRequest r);

  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;

  DiskSchedulerPromise CreatePromise() {
    return {};
  }

 private:
  DiskManager                        *disk_manager_;
  Channel<std::optional<DiskRequest>> request_queue_;
  std::optional<std::thread>          background_thread_;
};
class ConcurrentScheduler {
 public:
  explicit ConcurrentScheduler(DiskManager *disk_manager);
  ~ConcurrentScheduler();

  void Schedule(DiskRequest r);

  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;

  DiskSchedulerPromise CreatePromise() {
    return {};
  }

 private:
  DiskManager             *disk_manager_;
  size_t                   thread_num_;
  std::vector<std::thread> workers_;
  std::queue<DiskRequest>  request_queue_;
  std::mutex               queue_mtx_;
  std::condition_variable  condition_;
  bool                     stop_{false};
};

// using DiskScheduler = SingleThreadScheduler;
using DiskScheduler = ConcurrentScheduler;
}  // namespace lsmg

#endif