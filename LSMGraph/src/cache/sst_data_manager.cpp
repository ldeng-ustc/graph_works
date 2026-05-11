#include "cache/sst_data_manager.h"
#include "cache/sst_data_cache.h"
#include "common/utils.h"

namespace lsmg {

// SSTDataManagerHash
SSTDataManagerHash::~SSTDataManagerHash() noexcept {
  // printf("~sstdatacache.size=%ld\n", hash_map_.size());
  for (HashMapIterator iterator1 = hash_map_.begin(); iterator1 != hash_map_.end(); ++iterator1) {
    if (iterator1->second != nullptr) {
      delete iterator1->second;
    }
  }
}

void SSTDataManagerHash::put_data(const FileId_t fid, SSTDataCache *data) {
  HashMapValuePair hashMapValuePair(fid, data);
  hash_map_.insert(hashMapValuePair);
}

void SSTDataManagerHash::put_data(const FileId_t fid, const size_t edge_num, uintptr_t it) {
  SSTDataCache    *sst = new SSTDataCache(utils::eFileName(fid), edge_num, utils::pFileName(fid), it);
  HashMapValuePair hashMapValuePair(fid, sst);
  bool             rt = hash_map_.insert(hashMapValuePair);
  assert(rt == true);
}

SSTDataCache *SSTDataManagerHash::get_data(const FileId_t fid) {
  HashMapConstAccessor hashAccessor;
  if (hash_map_.find(hashAccessor, fid)) {
    return hashAccessor->second;
  } else {
    throw std::runtime_error("No found sstdatacache, fid=" + std::to_string(fid));
    return nullptr;
  }
}

void SSTDataManagerHash::del_data(const FileId_t fid) {
  HashMapConstAccessor hashAccessor;
  if (hash_map_.find(hashAccessor, fid)) {
    delete hashAccessor->second;
    hash_map_.erase(hashAccessor);
  } else {
    throw std::runtime_error("The deleted fid does not exist, fid=" + std::to_string(fid));
  }
}
}  // namespace lsmg
