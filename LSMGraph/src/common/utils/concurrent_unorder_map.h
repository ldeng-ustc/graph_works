#ifndef CONCURRENT_UNORDER_MAP_H
#define CONCURRENT_UNORDER_MAP_H

#include <tbb/concurrent_unordered_map.h>

// TODO: rename function
namespace lsmg {

template <typename KeyType, typename ValueType>
class ConcurrentUnorderedMap {
 private:
  using HashMap              = tbb::concurrent_unordered_map<KeyType, ValueType>;
  using HashMapIterator      = typename HashMap::iterator;
  using HashMapConstIterator = typename HashMap::const_iterator;
  using HashMapValueaPair    = typename HashMap::value_type;

 public:
  ConcurrentUnorderedMap(ValueType default_value)
      : DEFAULT_VALUE(default_value) {}

  size_t size() {
    return hash_map_.size();
  }

  bool insert(const KeyType &key, const ValueType &value) {
    std::pair<HashMapIterator, bool> result = hash_map_.insert(HashMapValuePair(key, value));
    return result.second;
  }

  bool erase(const KeyType &key) {
    printf("Not support concurrent erasure\n");
    return false;
  }

  void clear() {
    hash_map_.clear();
  }

  bool find(const KeyType &key, ValueType &value) {
    HashMapConstIterator it = hash_map_.find(key);
    if (it != hash_map_.end()) {
      value = it->second;
      return true;
    }
    value = DEFAULT_VALUE;
    return false;
  }

  inline const ValueType &operator[](const KeyType &key) const {
    HashMapConstIterator it = hash_map_.find(key);
    if (it != hash_map_.end()) {
      return it->second;
    }
    return DEFAULT_VALUE;
  }

  // ValueType& operator[](const KeyType& key) {
  //     return hashMap[key];
  // }

  HashMapIterator begin() {
    return hash_map_.begin();
  }

  HashMapIterator end() {
    return hash_map_.end();
  }

 private:
  HashMap   hash_map_;
  ValueType DEFAULT_VALUE;
};

}  // namespace lsmg

#endif