#ifndef SSTABLE_H
#define SSTABLE_H

#include <cassert>
#include <string>
#include <vector>
#include "cache/sst_data_manager.h"
#include "common/config.h"
#include "index/bloom_filter.h"
#include "index/index.h"
#include "index/vary_size_bloom_filter.h"

namespace lsmg {

class SSTableCache {
 public:
  Header               header;
  BloomFilter         *bloomFilter;
  BloomFilterVarySize *bloomFilterVarySize;
  SequenceNumber_t     seq_;
  SSTDataManager      &sstdata_manager_;

  std::vector<Index> indexes;
  std::string        path;

  std::atomic<int32_t> refs = {1};

  ~SSTableCache() {
    delete bloomFilter;
    if (bloomFilterVarySize != nullptr) {
      delete bloomFilterVarySize;
    }
  }
  SSTableCache(SSTDataManager &sstdata_manager)
      : bloomFilter(new BloomFilter())
      , bloomFilterVarySize(nullptr)
      , sstdata_manager_(sstdata_manager) {
    // in_compaction_ = false;
  }
  SSTableCache(const std::string dir, SSTDataManager &sstdata_manager);
  Header readHeadFromFile(const std::string dir);

  int   get(const uint64_t &src);
  int   get(const uint64_t &src, const uint64_t &dst);
  int   find(const uint64_t &key, int start, int end);
  int   low_bound(const uint64_t &key, int start, int last);
  int   low_bound(const uint64_t &key, int start, int last, char *indexBuf);
  char *GetIndex();

  void    Ref();
  void    Unref();
  int32_t Getref();
};

inline bool cacheTimeCompare(SSTableCache *a, SSTableCache *b) {
  return (a->header).timeStamp > (b->header).timeStamp;  // Sort from largest to smallest
}

inline bool cacheKeyCompare(SSTableCache *a, SSTableCache *b) {
  return (a->header).minKey < (b->header).minKey;  // Sort minkey from small to large
}

inline bool haveIntersection(const SSTableCache *cache, const std::vector<Range> &ranges) {
  uint64_t min = (cache->header).minKey, max = (cache->header).maxKey;
  for (auto it = ranges.begin(); it != ranges.end(); ++it) {
    if (!(((*it).max < min) || ((*it).min > max))) {
      return true;
    }
  }
  return false;
}

}  // namespace lsmg

#endif  // SSTABLE_H
