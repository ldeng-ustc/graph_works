#ifndef LSMG_VERSION_SET_HEADER
#define LSMG_VERSION_SET_HEADER

#include <assert.h>
#include <atomic>
#include <mutex>
#include <set>
#include <vector>
#include "cache/sst_table_cache.h"

namespace lsmg {

class Version;
class VersionSet;

class Version {
 public:
  Version                    *next_;
  Version                    *prev_;
  VersionSet                 *belong_vset_;
  std::atomic<int32_t>        refs_;
  std::vector<SSTableCache *> l0_filemetas_;

  Version(VersionSet *vset)
      : next_(this)
      , prev_(this)
      , belong_vset_(vset)
      , refs_(0) {}

  ~Version() {
    assert(refs_ <= 0);
  }

  void Ref();

  void Unref();

  Status                       GetEdge(VertexId_t src, VertexId_t dst, std::string *property);
  std::vector<SSTableCache *> *GetLevel0Files();

  void PrintFilesMetaInfo();
};

class VersionEdit {
 public:
  VersionEdit() {
    Clear();
  }
  ~VersionEdit() = default;

  void Clear() {
    new_files_.clear();
    deleted_files_.clear();
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  void AddFile(SSTableCache *f) {
    new_files_.push_back(f);
  }

  // Delete the specified "file" from level-0.
  void RemoveFile(FileId_t fid) {
    deleted_files_.insert(fid);
  }

 private:
  friend class VersionSet;
  std::vector<SSTableCache *> new_files_;
  std::set<FileId_t>          deleted_files_;
};

class VersionSet {
 public:
  VersionSet(std::mutex &version_mu_);
  VersionSet(const VersionSet &)            = delete;
  VersionSet &operator=(const VersionSet &) = delete;
  Version    *GetCurrent();
  void        AppendVersion(Version *v);
  void        DeleteVersion(Version *v);

  bool LogAndApply(VersionEdit &version_edit, Version *v);

  void VersionLock();
  void VersionUnLock();

  ~VersionSet();

 private:
  friend class Version;
  Version    *current_;
  Version     dummy_versions_;
  std::mutex  mu_;
  std::mutex &version_mu_;
};

}  // namespace lsmg

#endif