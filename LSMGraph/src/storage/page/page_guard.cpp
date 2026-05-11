#include "storage/page/page_guard.h"
#include <cstdio>
#include <utility>
#include "buffer/buffer_pool_manager.h"

namespace lsmg {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_)
    , page_(that.page_)
    , is_dirty_(that.is_dirty_) {
  that.bpm_      = nullptr;
  that.page_     = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_      = nullptr;
  page_     = nullptr;
  is_dirty_ = false;
}

BasicPageGuard &BasicPageGuard::operator=(BasicPageGuard &&that) noexcept {
  if (this != &that) {
    Drop();
    bpm_           = that.bpm_;
    page_          = that.page_;
    is_dirty_      = that.is_dirty_;
    that.bpm_      = nullptr;
    that.page_     = nullptr;
    that.is_dirty_ = false;
  }

  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
};  // NOLINT

ReadPageGuard BasicPageGuard::UpgradeRead() {
  page_->RLatch();
  return ReadPageGuard{std::move(*this)};
}

WritePageGuard BasicPageGuard::UpgradeWrite() {
  page_->WLatch();
  is_dirty_ = true;
  return WritePageGuard{std::move(*this)};
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  Drop();
  guard_ = std::move(that.guard_);
};

ReadPageGuard &ReadPageGuard::operator=(ReadPageGuard &&that) noexcept {
  if (this != &that) {
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  if (guard_.page_ != nullptr) {
    // guard_.Drop();
    guard_.page_->RUnlatch();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  Drop();
  guard_ = std::move(that.guard_);
}

WritePageGuard &WritePageGuard::operator=(WritePageGuard &&that) noexcept {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
}  // NOLINT

}  // namespace lsmg
