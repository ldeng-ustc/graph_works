#include "version/super_version.h"
#include <algorithm>
#include "cache/mem_table.h"

namespace lsmg {

template <typename T>
inline void vectorDel(std::vector<T> &vec, T a) {
  auto it = std::find(vec.begin(), vec.end(), a);
  vec.erase(it);
}

VersionAndMemTable::~VersionAndMemTable() {
  current_->Unref();
  for (auto tb : menTables) {
    tb->Unref();
  }
}
void VersionAndMemTable::set_vs(Version *current) {
  current->Ref();
  current_ = current;
}
void VersionAndMemTable::batch_insert_tb(std::vector<MemTable *> &_menTables) {
  for (auto tb : _menTables) {
    tb->Ref();
    menTables.push_back(tb);
  }
}
void VersionAndMemTable::insert_tb(MemTable *tb) {
  tb->Ref();
  menTables.push_back(tb);
}
void VersionAndMemTable::remove_tb(MemTable *tb) {
  vectorDel<MemTable *>(menTables, tb);
  tb->Unref();
}
}  // namespace lsmg
