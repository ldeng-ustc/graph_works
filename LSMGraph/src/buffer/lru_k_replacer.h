#ifndef LSMG_LRU_K_REPLACER_HEADER
#define LSMG_LRU_K_REPLACER_HEADER

#include <cstddef>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace lsmg {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 public:
  explicit LRUKNode(size_t k = 0);

  bool operator<(const LRUKNode &rhs) const;

  void Init(size_t k);

  bool Evictable() const {
    return is_evictable_;
  }

  void SetEvictable(bool set_evictable) {
    is_evictable_ = set_evictable;
  }

  bool Valid() const;

  void SetInvalid();

  size_t KDistance() const {
    return k_distance_;
  }

  void Access(size_t timestamp);

 private:
  std::vector<size_t> history_;
  size_t              start_{0};
  size_t              end_{0};
  size_t              k_distance_;
  size_t              k_;
  bool                is_evictable_{false};
};

class LRUKImpl {
 public:
  LRUKImpl(size_t node_num, std::vector<LRUKNode> &node_ref);

  void Push(frame_id_t frame_id);

  frame_id_t Pop();

  void Remove(frame_id_t frame_id);

 private:
  struct ListNode {
    ListNode  *prev_;
    ListNode  *next_;
    frame_id_t frame_id_;
  };

  struct List {
    ListNode *head_{nullptr};
    ListNode *tail_{nullptr};
    size_t    size_{0};

    ~List() {
      auto curr = head_;
      while (curr != nullptr) {
        auto temp = curr;
        curr      = curr->next_;
        delete temp;
      }
    }
  };

  union Pos {
    ListNode *list_ptr_;
    uint64_t  heap_pos_;
  };

  std::vector<LRUKNode>  &node_ref_;        // reference to node store
  std::vector<frame_id_t> r_heap_;          // lru heap. heap_[0] is a sentinel element, indicate the size of heap;
  List                    f_list_;          // lfu list.
  std::vector<Pos>        frame_pos_map_;   // map frame id to posistion in heap/list
  std::vector<char>       frame_pos_type_;  // true is list, false is heap

  void Push2List(frame_id_t frame_id);

  ListNode *ListEvict();

  void RemoveFromList(const ListNode *node_ptr);

  void Push2Heap(frame_id_t frame_id, bool new_entry);

  size_t HeapEvict();

  void RemoveFromHeap(size_t pos);

  void AmendHeap(size_t pos);
};

class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  ~LRUKReplacer() = default;

  bool Evict(frame_id_t *frame_id);

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  size_t Size();

  bool Evictable(frame_id_t frame_id) {
    return node_store_[frame_id].Evictable();
  }

 private:
  std::vector<LRUKNode> node_store_;
  LRUKImpl              node_heap_;
  size_t                current_timestamp_{0};
  size_t                curr_size_{0};
  size_t                replacer_size_;  // max size
  size_t                k_;
  std::mutex            latch_;
};

}  // namespace lsmg

#endif