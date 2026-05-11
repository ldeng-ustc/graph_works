#ifndef LSMG_DEL_RECORD_MANAGER_HEADER
#define LSMG_DEL_RECORD_MANAGER_HEADER

#include <tbb/concurrent_hash_map.h>
#include <cassert>
#include <iostream>
#include "common/config.h"

namespace lsmg {

using EidToTimeMap   = tbb::concurrent_hash_map<SequenceNumber_t, SequenceNumber_t>;
using FidToRecordMap = tbb::concurrent_hash_map<FileId_t, EidToTimeMap *>;

class DelRecordManage {
 private:
  FidToRecordMap fidMap;

 public:
  DelRecordManage() {}

  ~DelRecordManage() {
    for (auto &pair : fidMap) {
      delete pair.second;
    }
  }

  size_t size() {
    return fidMap.size();
  }

  void put_fid_and_record(FileId_t fid, SequenceNumber_t eid, SequenceNumber_t time) {
    FidToRecordMap::accessor a;  // For thread-safe access
    bool                     isNewInsertion = fidMap.insert(a, fid);
    if (isNewInsertion) {
      // New element inserted, create new EidToTimeMap
      a->second = new EidToTimeMap();
    }
    // Insert record into the EidToTimeMap
    a->second->insert(std::make_pair(eid, time));
  }

  bool find_eidmap(FileId_t fid, EidToTimeMap *&mp) {
    FidToRecordMap::const_accessor ca;
    if (!fidMap.find(ca, fid) || ca->second->size() == 0) {
      return false;
    }
    mp = ca->second;
    return true;
  }

  bool get_time(EidToTimeMap *mp, SequenceNumber_t eid, SequenceNumber_t &time) {
    assert(mp != nullptr);
    EidToTimeMap::const_accessor ca;
    if (!mp->find(ca, eid)) {
      return false;
    }
    time = ca->second;
    return true;
  }

  void del_eid_frome_eidmap(EidToTimeMap *mp, SequenceNumber_t eid) {
    assert(mp != nullptr);
    mp->erase(eid);
  }

  void clean() {}

  void prinf() {
    size_t fild_size = 0;
    size_t eid_size  = 0;
    for (auto it = fidMap.begin(); it != fidMap.end(); ++it) {
      FidToRecordMap::const_accessor ca;
      fild_size++;
      if (fidMap.find(ca, it->first)) {
        eid_size += ca->second->size();
      }
    }
    LOG_INFO("fild_size={} eid_size={}", fild_size, eid_size);
    LOG_INFO("fild_size={} eid_size={}", fild_size, eid_size);
  }
};

}  // namespace lsmg
#endif