#include "index/bloom_filter.h"
#include <string.h>
#include "common/utils/MurmurHash3.h"

namespace lsmg {
BloomFilter::BloomFilter(char *buf) {
  memcpy((char *)&bit_set_, buf, FILTER_LENGTH / 8);
}

BloomFilter::BloomFilter(const BloomFilter &other) {
  bit_set_ = other.bit_set_;
}

BloomFilter &BloomFilter::operator=(const BloomFilter &other) {
  if (this == &other) {
    return *this;
  }

  // Copy the contents of the other BloomFilter
  bit_set_ = other.bit_set_;

  return *this;
}

void BloomFilter::add(const uint64_t &key) {
  uint32_t hashVal[4] = {0};
  MurmurHash3_x64_128(&key, sizeof(key), 1, &hashVal);
  bit_set_.set(hashVal[0] % FILTER_LENGTH);
  bit_set_.set(hashVal[1] % FILTER_LENGTH);
  bit_set_.set(hashVal[2] % FILTER_LENGTH);
  bit_set_.set(hashVal[3] % FILTER_LENGTH);
}

void BloomFilter::add(const uint64_t &key1, const uint64_t &key2) {
  const uint64_t key        = (key1 % 4294967291) << 32 | (key2 % 4294967291);
  uint32_t       hashVal[4] = {0};
  MurmurHash3_x64_128(&key, sizeof(key), 1, &hashVal);
  bit_set_.set(hashVal[0] % FILTER_LENGTH);
  bit_set_.set(hashVal[1] % FILTER_LENGTH);
  bit_set_.set(hashVal[2] % FILTER_LENGTH);
  bit_set_.set(hashVal[3] % FILTER_LENGTH);
}

bool BloomFilter::contains(const uint64_t &key) {
  uint32_t hashVal[4] = {0};
  MurmurHash3_x64_128(&key, sizeof(key), 1, &hashVal);
  return (bit_set_[hashVal[0] % FILTER_LENGTH] && bit_set_[hashVal[1] % FILTER_LENGTH]
          && bit_set_[hashVal[2] % FILTER_LENGTH] && bit_set_[hashVal[3] % FILTER_LENGTH]);
}

bool BloomFilter::contains(const uint64_t &key1, const uint64_t &key2) {
  const uint64_t key        = (key1 % 4294967291) << 32 | (key2 % 4294967291);
  uint32_t       hashVal[4] = {0};
  MurmurHash3_x64_128(&key, sizeof(key), 1, &hashVal);
  return (bit_set_[hashVal[0] % FILTER_LENGTH] && bit_set_[hashVal[1] % FILTER_LENGTH]
          && bit_set_[hashVal[2] % FILTER_LENGTH] && bit_set_[hashVal[3] % FILTER_LENGTH]);
}

void BloomFilter::save2Buffer(char *buf) {
  memcpy(buf, (char *)&bit_set_, FILTER_LENGTH / 8);
}

void BloomFilter::merge(const BloomFilter &other) {
  bit_set_ |= other.bit_set_;
}
}  // namespace lsmg