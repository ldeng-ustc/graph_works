#ifndef LSMG_BLOOMFILTER_HEADER
#define LSMG_BLOOMFILTER_HEADER

#include <bitset>
#include "common/config.h"

namespace lsmg {

constexpr uint64_t FILTER_LENGTH = (BLOOM_FILTER_SIZE * 8);

class BloomFilter {
 private:
  std::bitset<FILTER_LENGTH> bit_set_;

 public:
  BloomFilter() {
    bit_set_.reset();
  }
  BloomFilter(char *buf);
  BloomFilter(const BloomFilter &other);
  BloomFilter &operator=(const BloomFilter &other);

  void add(const uint64_t &key);
  void add(const uint64_t &key1, const uint64_t &key2);
  bool contains(const uint64_t &key);
  bool contains(const uint64_t &key1, const uint64_t &key2);
  void save2Buffer(char *buf);
  void reset() {
    bit_set_.reset();
  }
  void merge(const BloomFilter &other);

  std::bitset<FILTER_LENGTH> *getSet() {
    return &bit_set_;
  }
};

}  // namespace lsmg
#endif  // BLOOMFILTER_H
