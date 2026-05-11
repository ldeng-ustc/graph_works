#include "storage/disk/disk_scheduler.h"
#include <cstddef>
#include <cstdio>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>
#include "storage/disk/disk_manager.h"

namespace lsmg {

SingleThreadScheduler::SingleThreadScheduler(DiskManager *disk_manager)
    : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

SingleThreadScheduler::~SingleThreadScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void SingleThreadScheduler::Schedule(DiskRequest r) {
  request_queue_.Put(std::optional<DiskRequest>(std::move(r)));
}

void SingleThreadScheduler::StartWorkerThread() {
  std::optional<DiskRequest> disk_request;
  while (true) {
    disk_request = request_queue_.Get();
    if (!disk_request.has_value()) {
      return;
    }
    if (disk_request->is_write_) {
      disk_manager_->WritePage(disk_request->page_id_, disk_request->data_);
      disk_request->callback_.set_value(true);
    } else {
      disk_manager_->ReadPage(disk_request->page_id_, disk_request->data_);
      disk_request->callback_.set_value(true);
    }
  }
}

ConcurrentScheduler::ConcurrentScheduler(DiskManager *disk_manager)
    : disk_manager_(disk_manager) {
  thread_num_ = 16;
  workers_.reserve(thread_num_);
  for (size_t i = 0; i < thread_num_; ++i) {
    workers_.emplace_back([&] { StartWorkerThread(); });
  }
}

ConcurrentScheduler::~ConcurrentScheduler() {
  {
    std::unique_lock lock(queue_mtx_);
    stop_ = true;
  }
  condition_.notify_all();
  for (auto &worker : workers_) {
    worker.join();
  }
}

void ConcurrentScheduler::Schedule(DiskRequest r) {
  {
    std::unique_lock lock(queue_mtx_);
    request_queue_.push(std::move(r));
  }
  condition_.notify_one();
}

void ConcurrentScheduler::StartWorkerThread() {
  while (true) {
    DiskRequest disk_request;
    {
      std::unique_lock lock(queue_mtx_);
      condition_.wait(lock, [this]() { return !request_queue_.empty() || stop_; });
      if (stop_) {
        return;
      }
      disk_request = std::move(request_queue_.front());
      request_queue_.pop();
    }
    if (disk_request.is_write_) {
      disk_manager_->WritePage(disk_request.page_id_, disk_request.data_);
      disk_request.callback_.set_value(true);
    } else {
      disk_manager_->ReadPage(disk_request.page_id_, disk_request.data_);
      disk_request.callback_.set_value(true);
    }
  }
}

}  // namespace lsmg
