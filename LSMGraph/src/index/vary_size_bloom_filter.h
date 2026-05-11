#ifndef BloomFilterVarySize_H
#define BloomFilterVarySize_H

#include <bitset>
#include <cstdint>

namespace lsmg {

#define BITSETSIZE 26150110

class BloomFilterVarySize {
 private:
  std::bitset<BITSETSIZE> bitSet;

 public:
  BloomFilterVarySize() {
    bitSet.reset();
  }
  BloomFilterVarySize(char *buf);
  BloomFilterVarySize(const BloomFilterVarySize &other);
  BloomFilterVarySize &operator=(const BloomFilterVarySize &other);
  void                 add(const uint64_t &key);
  void                 add(const uint64_t &key1, const uint64_t &key2);
  bool                 contains(const uint64_t &key);
  bool                 contains(const uint64_t &key1, const uint64_t &key2);
  void                 save2Buffer(char *buf);
  void                 reset() {
                    bitSet.reset();
  }
  void                     merge(const BloomFilterVarySize &other);
  std::bitset<BITSETSIZE> *getSet() {
    return &bitSet;
  }
};

}  // namespace lsmg

#endif  // BloomFilterVarySize_H
