#include "version/version_set.h"
#include <algorithm>

namespace lsmg {

void Version::Ref() {
  refs_.fetch_add(1);
}

void Version::Unref() {
  assert(this != &belong_vset_->dummy_versions_);
  assert(refs_ >= 1);
  int32_t previous_refs = refs_.fetch_sub(1);
  assert(previous_refs >= 1);
  if (previous_refs > 1) {
    return;
  }
  for (auto meta : l0_filemetas_) {
    meta->Unref();
  }

  belong_vset_->DeleteVersion(this);
  delete this;
}

Status Version::GetEdge(VertexId_t src, VertexId_t dst, std::string *property) {
  // TODO
  return Status::kNotFound;
}

std::vector<SSTableCache *> *Version::GetLevel0Files() {
  return &l0_filemetas_;
}

void Version::PrintFilesMetaInfo() {
  LOG_INFO(" file_num={}", l0_filemetas_.size());
  for (auto it : l0_filemetas_) {
    LOG_INFO(" fid={} min_key={} max_key={} ref={}", it->header.timeStamp, it->header.minKey, it->header.maxKey,
             it->Getref());
  }
}

// VersionSet
VersionSet::VersionSet(std::mutex &version_mu)
    : current_(nullptr)
    , dummy_versions_(this)
    , version_mu_(version_mu) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
}

Version *VersionSet::GetCurrent() {
  return current_;
}

void VersionSet::AppendVersion(Version *v) {
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  mu_.lock();
  v->next_                     = dummy_versions_.next_;
  dummy_versions_.next_->prev_ = v;
  dummy_versions_.next_        = v;
  v->prev_                     = &dummy_versions_;
  mu_.unlock();
}

void VersionSet::DeleteVersion(Version *v) {
  mu_.lock();
  v->prev_->next_ = v->next_;
  v->next_->prev_ = v->prev_;
  mu_.unlock();
}

void VersionSet::VersionLock() {
  version_mu_.lock();
}

void VersionSet::VersionUnLock() {
  version_mu_.unlock();
}

bool VersionSet::LogAndApply(VersionEdit &version_edit, Version *v) {
  int reserve_size =
      current_->l0_filemetas_.size() + version_edit.new_files_.size() - version_edit.deleted_files_.size();
  if (reserve_size > 0) {
    v->l0_filemetas_.reserve(reserve_size);
  }

  for (auto filemeta : current_->l0_filemetas_) {
    if (version_edit.deleted_files_.count(filemeta->header.timeStamp) == 0) {
      filemeta->Ref();
      v->l0_filemetas_.push_back(filemeta);
    }
  }

  for (auto filemeta : version_edit.new_files_) {
    if (version_edit.deleted_files_.count(filemeta->header.timeStamp) == 0) {
      filemeta->Ref();
      v->l0_filemetas_.push_back(filemeta);
    }
  }

  if (v->l0_filemetas_.size() > 1) {
    std::sort(v->l0_filemetas_.begin(), v->l0_filemetas_.end(), cacheTimeCompare);
  }

  AppendVersion(v);
  version_edit.Clear();

  return true;
}

}  // namespace lsmg
