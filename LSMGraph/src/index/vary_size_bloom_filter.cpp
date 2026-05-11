#include "index/vary_size_bloom_filter.h"
#include <string.h>
#include "common/utils/MurmurHash3.h"

namespace lsmg {

BloomFilterVarySize::BloomFilterVarySize(char *buf) {
  memcpy((char *)&bitSet, buf, BITSETSIZE / 8);
}

BloomFilterVarySize::BloomFilterVarySize(const BloomFilterVarySize &other) {
  bitSet = other.bitSet;
}

BloomFilterVarySize &BloomFilterVarySize::operator=(const BloomFilterVarySize &other) {
  if (this == &other) {
    return *this;
  }
  bitSet = other.bitSet;
  return *this;
}

void BloomFilterVarySize::add(const uint64_t &key) {
  uint32_t hashVal[4] = {0};
  MurmurHash3_x64_128(&key, sizeof(key), 1, &hashVal);
  bitSet.set(hashVal[0] % BITSETSIZE);
  bitSet.set(hashVal[1] % BITSETSIZE);
  bitSet.set(hashVal[2] % BITSETSIZE);
  bitSet.set(hashVal[3] % BITSETSIZE);
}

void BloomFilterVarySize::add(const uint64_t &key1, const uint64_t &key2) {
  const uint64_t key        = (key1 % 4294967291) << 32 | (key2 % 4294967291);
  uint32_t       hashVal[4] = {0};
  MurmurHash3_x64_128(&key, sizeof(key), 1, &hashVal);
  bitSet.set(hashVal[0] % BITSETSIZE);
  bitSet.set(hashVal[1] % BITSETSIZE);
  bitSet.set(hashVal[2] % BITSETSIZE);
  bitSet.set(hashVal[3] % BITSETSIZE);
}

bool BloomFilterVarySize::contains(const uint64_t &key) {
  uint32_t hashVal[4] = {0};
  MurmurHash3_x64_128(&key, sizeof(key), 1, &hashVal);
  return (bitSet[hashVal[0] % BITSETSIZE] && bitSet[hashVal[1] % BITSETSIZE] && bitSet[hashVal[2] % BITSETSIZE]
          && bitSet[hashVal[3] % BITSETSIZE]);
}

bool BloomFilterVarySize::contains(const uint64_t &key1, const uint64_t &key2) {
  const uint64_t key        = (key1 % 4294967291) << 32 | (key2 % 4294967291);
  uint32_t       hashVal[4] = {0};
  MurmurHash3_x64_128(&key, sizeof(key), 1, &hashVal);
  return (bitSet[hashVal[0] % BITSETSIZE] && bitSet[hashVal[1] % BITSETSIZE] && bitSet[hashVal[2] % BITSETSIZE]
          && bitSet[hashVal[3] % BITSETSIZE]);
}

void BloomFilterVarySize::save2Buffer(char *buf) {
  memcpy(buf, (char *)&bitSet, BITSETSIZE / 8);
}

void BloomFilterVarySize::merge(const BloomFilterVarySize &other) {
  bitSet |= other.bitSet;
}

}  // namespace lsmg