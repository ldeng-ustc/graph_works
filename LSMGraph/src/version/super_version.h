#ifndef LSMG_SUPER_VERSION_HEADER
#define LSMG_SUPER_VERSION_HEADER
#include <assert.h>
#include <vector>
#include "index/index.h"
#include "version/version_set.h"

namespace lsmg {

class MemTable;

struct VersionAndMemTable {
  std::vector<MemTable *> menTables;
  Version                *current_;
  ~VersionAndMemTable();
  void set_vs(Version *current);
  void batch_insert_tb(std::vector<MemTable *> &_menTables);
  void insert_tb(MemTable *tb);
  void remove_tb(MemTable *tb);
};

struct SuperVersion {
  std::shared_ptr<VersionAndMemTable> version_memtable;
  std::shared_mutex                   vm_rw_mtx;
  MulLevelIndex                       findex;

  SuperVersion()
      : version_memtable(nullptr) {}
  ~SuperVersion() {}

  Version *get_version() {
    return version_memtable->current_;
  }

  std::vector<MemTable *> &get_memtable() {
    return version_memtable->menTables;
  }
};
}  // namespace lsmg
#endif