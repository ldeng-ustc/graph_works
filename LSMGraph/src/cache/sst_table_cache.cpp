
#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>

#include "cache/sst_table_cache.h"
#include "common/config.h"
#include "common/utils.h"
#include "index/index.h"

namespace lsmg {

SSTableCache::SSTableCache(const std::string dir, SSTDataManager &sstdata_manager)
    : sstdata_manager_(sstdata_manager) {
  path = dir;
  std::ifstream file(dir, std::ios::binary);
  if (!file) {
    printf("Fail to open file %s\n", dir.c_str());
    exit(-1);
  }
  // load header
  uint64_t offset = -(HEADER_SIZE);
  file.seekg(offset, std::ios::end);
  file.read((char *)&header, sizeof(Header));
  int64_t index_length = header.index_size;

  // load bloom filter
  char *filterBuf = new char[BLOOM_FILTER_SIZE];
  offset          = -(HEADER_SIZE + BLOOM_FILTER_SIZE);
  file.seekg(offset, std::ios::end);
  file.read(filterBuf, BLOOM_FILTER_SIZE);
  bloomFilter = new BloomFilter(filterBuf);

  char *indexBuf = new char[index_length * 12];
  // file.seekg(-int(index_length) * 12 - (HEADER_SIZE + BLOOM_FILTER_SIZE), std::ios::end);
  file.seekg(header.size * sizeof(EdgeBody_t), std::ios::beg);

  file.read(indexBuf, index_length * 12);
  for (int32_t i = 0; i < index_length; ++i) {
    indexes.push_back(Index(*(uint64_t *)(indexBuf + 12 * i), *(uint32_t *)(indexBuf + 12 * i + 8)));
  }

  sstdata_manager_.put_data(header.timeStamp, header.size, reinterpret_cast<uintptr_t>(this));

  delete[] filterBuf;
  delete[] indexBuf;
  file.close();
}

Header SSTableCache::readHeadFromFile(const std::string dir) {
  std::ifstream file(dir, std::ios::binary);
  if (!file) {
    printf("Fail to open file %s\n", dir.c_str());
    exit(-1);
  }
  // load header
  Header   header;
  uint64_t offset = -(HEADER_SIZE);
  file.seekg(offset, std::ios::end);
  file.read((char *)&header, sizeof(Header));
  return header;
}

// query the offset of the src
int SSTableCache::get(const uint64_t &src) {
  return find(src, 0, indexes.size() - 1);
}

// First determine whether the target edge exists, and then query the offset of the src
int SSTableCache::get(const uint64_t &src, const uint64_t &dst) {
#ifdef USE_BLOOMFILTER
  if (!bloomFilter->contains(src, dst)) {
    return -1;
  }
#endif

  return find(src, 0, indexes.size() - 1);
}

// return location of <key, offset> in indexes, not location of edge
int SSTableCache::find(const uint64_t &key, int start, int last) {
  while (start <= last) {
    int mid = start + ((last - start) >> 1);
    if (indexes[mid].key == key) {
      return mid;
    } else if (indexes[mid].key < key) {
      start = mid + 1;
    } else {
      last = mid - 1;
    }
  }
  return -1;
}

// Returns a position of iterator pointing to the first element in the range
// [start,last) which does not compare less than key.
int SSTableCache::low_bound(const uint64_t &key, int start, int last) {
  auto it = std::lower_bound(indexes.begin() + start, indexes.begin() + last, key,
                             [](const Index &index, uint64_t key) { return index.key < key; });
  return it - indexes.begin();
}

// Returns a position of iterator pointing to the first element in the range
// [start,last) which does not compare less than key.
int SSTableCache::low_bound(const uint64_t &key, int start, int last, char *indexBuf) {
  while (start < last) {
    int        mid     = (start + last) / 2;
    VertexId_t mid_val = *(uint64_t *)(indexBuf + mid * 12);
    if (mid_val >= key) {
      last = mid;
    } else {
      start = mid + 1;  // sizeof(index)=8+4
    }
  }
  return start;
}

char *SSTableCache::GetIndex() {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    printf("Fail to open file %s\n", path.c_str());
    exit(-1);
  }
  int64_t index_length = header.index_size;
  char   *indexBuf     = new char[index_length * 12];
  file.seekg(header.size * sizeof(EdgeBody_t), std::ios::beg);

  file.read(indexBuf, index_length * 12);
  file.close();
  return indexBuf;
}

void SSTableCache::Ref() {
  refs.fetch_add(1);
}

void SSTableCache::Unref() {
  int32_t old_refs = refs.fetch_sub(1);
  assert(old_refs >= 1);
  if (old_refs == 1) {  // means refs = 0
    sstdata_manager_.del_data(header.timeStamp);
    auto rt = utils::rmfile(utils::eFileName(header.timeStamp).c_str());
    assert(rt == 0);
    rt = utils::rmfile(utils::pFileName(header.timeStamp).c_str());
    assert(rt == 0);
    delete this;
  }
}

int32_t SSTableCache::Getref() {
  return refs.load(std::memory_order_acquire);
}

}  // namespace lsmg