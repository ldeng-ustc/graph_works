#ifndef SST_DATA_MANAGER_H
#define SST_DATA_MANAGER_H

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <tbb/concurrent_hash_map.h>
#include <unistd.h>
#include <vector>
#include "common/config.h"

namespace lsmg {
class SSTDataCache;
class SSTDataManagerHash;
using SSTDataManager = SSTDataManagerHash;

typedef tbb::concurrent_hash_map<FileId_t, SSTDataCache *> HashMap;
typedef typename HashMap::const_accessor                   HashMapConstAccessor;
typedef typename HashMap::accessor                         HashMapAccessor;
typedef typename HashMap::iterator                         HashMapIterator;
typedef HashMap::value_type                                HashMapValuePair;

class SSTDataManagerHash {
 public:
  explicit SSTDataManagerHash() {}

  void put_data(const FileId_t fid, SSTDataCache *data);

  void put_data(const FileId_t fid, const size_t edge_num, uintptr_t it);

  SSTDataCache *get_data(const FileId_t fid);

  void del_data(const FileId_t fid);

  ~SSTDataManagerHash() noexcept;

 private:
  HashMap hash_map_;  // cache sstable data
};

}  // namespace lsmg
#endif