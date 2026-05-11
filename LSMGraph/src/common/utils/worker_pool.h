#ifndef WORKER_POOL_H
#define WORKER_POOL_H
#include <cassert>
#include <functional>
#include <vector>

#include "thread_pool.h"

typedef std::function<void()> worker_task;

class WorkerPool : ThreadPool {
  std::vector<std::future<void>> futures;

 public:
  using ThreadPool::ThreadPool;
  void launch(worker_task &task) {
    assert(futures.size() == 0);

    for (uint i = 0; i < workers.size(); i++) {
      futures.push_back(enqueue(task));
    }
  }
  void wait_all() {
    for (auto &future : futures) {
      future.wait();
    }

    futures.clear();
  }
};

class DynamicWorkerPool {
  std::vector<std::thread> workers;

 public:
  void launch(worker_task &task) {
    for (uint i = 0; i < workers.size(); i++) {
      workers.emplace_back(task);
    }
  }
  void wait_all(void) {
    for (auto &worker : workers) {
      worker.join();
    }
  }
};

#endif