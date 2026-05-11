#ifndef CHANNEL_H
#define CHANNEL_H

#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <queue>
#include <utility>

namespace lsmg {

template <class T>
class Channel {
 public:
  Channel()  = default;
  ~Channel() = default;

  void Put(T element) {
    std::unique_lock<std::mutex> lk(m_);
    q_.push(std::move(element));
    lk.unlock();
    cv_.notify_all();
  }

  T Get() {
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait(lk, [&]() { return !q_.empty(); });
    T element = std::move(q_.front());
    q_.pop();
    return element;
  }

 private:
  std::mutex              m_;
  std::condition_variable cv_;
  std::queue<T>           q_;
};
}  // namespace lsmg

#endif