#ifndef PAGE_GUARD_H
#define PAGE_GUARD_H

#include <cstdio>
#include <utility>
#include "storage/page/page.h"

namespace lsmg {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

class BasicPageGuard {
 public:
  BasicPageGuard() = default;

  BasicPageGuard(BufferPoolManager *bpm, Page *page)
      : bpm_(bpm)
      , page_(page) {}

  BasicPageGuard(const BasicPageGuard &)            = delete;
  BasicPageGuard &operator=(const BasicPageGuard &) = delete;

  BasicPageGuard(BasicPageGuard &&that) noexcept;

  void Drop();

  BasicPageGuard &operator=(BasicPageGuard &&that) noexcept;

  ~BasicPageGuard();

  ReadPageGuard UpgradeRead();

  WritePageGuard UpgradeWrite();

  page_id_t PageId() {
    return page_->GetPageId();
  }

  const char *GetData() {
    return page_->GetData();
  }

  template <class T>
  const T *As() {
    return reinterpret_cast<const T *>(GetData());
  }

  char *GetDataMut() {
    is_dirty_ = true;
    return page_->GetData();
  }

  template <class T>
  T *AsMut() {
    return reinterpret_cast<T *>(GetDataMut());
  }

 private:
  friend class ReadPageGuard;
  friend class WritePageGuard;

  BufferPoolManager *bpm_{nullptr};
  Page              *page_{nullptr};
  bool               is_dirty_{false};
};

class ReadPageGuard {
 public:
  ReadPageGuard() = default;

  ReadPageGuard(BufferPoolManager *bpm, Page *page)
      : guard_(bpm, page) {}

  explicit ReadPageGuard(BasicPageGuard &&guard)
      : guard_(std::move(guard)) {}

  ReadPageGuard(const ReadPageGuard &) = delete;

  ReadPageGuard &operator=(const ReadPageGuard &) = delete;

  ReadPageGuard(ReadPageGuard &&that) noexcept;

  ReadPageGuard &operator=(ReadPageGuard &&that) noexcept;

  void Drop();

  ~ReadPageGuard();

  page_id_t PageId() {
    return guard_.PageId();
  }

  const char *GetData() {
    return guard_.GetData();
  }

  template <class T>
  const T *As() {
    return guard_.As<T>();
  }

 private:
  BasicPageGuard guard_;
};

class WritePageGuard {
 public:
  WritePageGuard() = default;

  WritePageGuard(BufferPoolManager *bpm, Page *page)
      : guard_(bpm, page) {
    guard_.is_dirty_ = true;
  }

  explicit WritePageGuard(BasicPageGuard &&guard)
      : guard_(std::move(guard)) {
    guard_.is_dirty_ = true;
  }

  WritePageGuard(const WritePageGuard &) = delete;

  WritePageGuard &operator=(const WritePageGuard &) = delete;

  WritePageGuard(WritePageGuard &&that) noexcept;

  WritePageGuard &operator=(WritePageGuard &&that) noexcept;

  void Drop();

  ~WritePageGuard();

  page_id_t PageId() {
    return guard_.PageId();
  }

  const char *GetData() {
    return guard_.GetData();
  }

  template <class T>
  const T *As() {
    return guard_.As<T>();
  }

  char *GetDataMut() {
    return guard_.GetDataMut();
  }

  template <class T>
  T *AsMut() {
    return guard_.AsMut<T>();
  }

 private:
  BasicPageGuard guard_;
};

}  // namespace lsmg

#endif