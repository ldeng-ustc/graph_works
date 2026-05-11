#ifndef LMSG_NEIGHBORS_HEADEr
#define LMSG_NEIGHBORS_HEADEr

#include <stdlib.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <set>
#include <utility>
#include "common/config.h"
#include "common/flags.h"
#include "container/edge_iterator_base.h"
#include "graph/edge.h"

// #define SORTED_ARRAY

namespace lsmg {

class NewEdgeSL;
class NewEdgeArray;
template <typename K, class Cmp>
class SkipList;

using NeighBors = NewEdgeSL;

const uint16_t kMaxHeight_              = 4;  // SKIPLIST_MAX_HEIGHT
const uint16_t kBranching_              = 4;
const uint32_t kScaledInverseBranching_ = 2147483647L / kBranching_;

template <typename K, class Cmp>
class SkipList {
 public:
  class MemEdgeIterator;

  explicit SkipList()
      : head_(NewNode(0, kMaxHeight_))
      , max_height_(1)
      , prev_height_(1)
      , size_(0)
      , property_size_(0) {
    assert(kMaxHeight_ > 0 && kMaxHeight_ == static_cast<uint32_t>(kMaxHeight_));
    assert(kBranching_ > 0 && kBranching_ == static_cast<uint32_t>(kBranching_));
    assert(kScaledInverseBranching_ > 0);
    prev_ = (Node **)malloc(sizeof(Node *) * kMaxHeight_);
    for (int i = 0; i < kMaxHeight_; i++) {
      head_->SetNext(i, nullptr);
      prev_[i] = head_;
    }
    if (FLAGS_reserve_node > 0) {
      dst_ = new Node[FLAGS_reserve_node];
    }
  }

  ~SkipList() {
    if (FLAGS_reserve_node > 0) {
      delete[] dst_;
    }
    if (prev_ != nullptr) {
      free(prev_);
    }
    for (auto mem : recycle_mem) {
      if (mem != nullptr) {
        clear_node_key(mem);
        free(mem);
      }
    }
  }

  void clear_node_key(char *mem) {
    Node *temp_node = reinterpret_cast<Node *>(mem);
    temp_node->~Node();
  }

  void reset() {
    max_height_    = 1;
    size_          = 0;
    property_size_ = 0;
    prev_height_   = 1;
    for (int i = 0; i < kMaxHeight_; i++) {
      head_->SetNext(i, nullptr);
      prev_[i] = head_;
    }
    for (auto mem : recycle_mem) {
      if (mem != nullptr && mem != reinterpret_cast<char *>(head_)) {
        clear_node_key(mem);
        free(mem);
        mem = nullptr;
      }
    }
    recycle_mem.clear();
  }

  SkipList(const SkipList &other) = delete;

  void operator=(const SkipList &other) = delete;

  Status get(VertexId_t dst, std::string *property) {
    Edge  key = Edge(dst, MAX_SEQ_ID);
    Node *x   = FindGreaterOrEqual(key);
    if (x != nullptr && key.destination() == x->key.destination()) {
      if (key.marker()) {
        return Status::kDelete;
      }
      property->assign(x->key.property().data(), x->key.property().size());
      return Status::kOk;
      ;
    } else {
      return Status::kNotFound;
    }
  }

  Status get(VertexId_t dst, SequenceNumber_t &seq) {
    Edge  key = Edge(dst, MAX_SEQ_ID);
    Node *x   = FindGreaterOrEqual(key);
    if (x != nullptr && key.destination() == x->key.destination()) {
      if (key.marker()) {
        return Status::kDelete;
      }
      seq = x->key.sequence();
      return Status::kOk;
      ;
    } else {
      return Status::kNotFound;
    }
  }

  void get_edges(VertexId_t src, std::vector<Edge> &edges) {
    edges.resize(size_);
    uint32_t idx(0);
    Node    *x     = head_;
    int      level = GetMaxHeight() - 1;
    while (true) {
      Node *next = x->Next(level);
      if (next == nullptr) {
        if (level == 0) {
          return;
        } else {
          level--;
        }
      } else {
        edges[idx++] = x->key;
        x            = next;
      }
    }
  }

  void put_edge(VertexId_t dst, SequenceNumber_t seq, Marker_t marker, const EdgeProperty_t &property) {
    int   height = 1;
    Node *x;
    if (size_ < FLAGS_reserve_node) {
      x = GetOldNode(size_);
      x->SetNext(0, nullptr);
    } else {
      height = RandomHeight();
      x      = GetNewNode(height);
    }

    Edge &key = x->key;
    key.set_destination(dst);
    key.set_sequence(seq);
    key.set_marker(marker);
    key.set_property(property);

    // fast path for sequential insertion
    if (!KeyIsAfterNode(key, prev_[0]->NoBarrier_Next(0)) && (prev_[0] == head_ || KeyIsAfterNode(key, prev_[0]))) {
      assert(prev_[0] != head_ || (prev_height_ == 1 && GetMaxHeight() == 1));

      for (uint i = 1; i < prev_height_; i++) {
        prev_[i] = prev_[0];
      }
    } else {
      FindLessThan(key, prev_);
    }

    // Our data structure does not allow duplicate insertion
    assert(prev_[0]->Next(0) == nullptr || !Equal(key, prev_[0]->Next(0)->key));
    if (height > GetMaxHeight()) {
      for (int i = GetMaxHeight(); i < height; i++) {
        prev_[i] = head_;
      }
      // It is ok to mutate max_height_ without any synchronization
      // with concurrent readers.
      max_height_.store(height, std::memory_order_relaxed);
    }

    for (int i = 0; i < height; i++) {
      x->NoBarrier_SetNext(i, prev_[i]->NoBarrier_Next(i));
      prev_[i]->SetNext(i, x);
    }
    prev_[0]     = x;
    prev_height_ = height;
    size_++;
    property_size_ += property.size();
  }

  bool Contains(const K &key) const {
    Node *x = FindGreaterOrEqual(key);
    if (x != nullptr && Equal(key, x->key)) {
      return true;
    } else {
      return false;
    }
  }

  uint64_t EstimateCount(const K &key) const {
    uint64_t count = 0;

    Node *x     = head_;
    int   level = GetMaxHeight() - 1;
    while (true) {
      assert(x == head_ || Cmp::compare(x->key, key) < 0);
      Node *next = x->Next(level);
      if (next == nullptr || Cmp::compare(next->key, key) >= 0) {
        if (level == 0) {
          return count;
        } else {
          // Switch to next list
          count *= kBranching_;
          level--;
        }
      } else {
        x = next;
        count++;
      }
    }
  }

  size_t getEdgeNum() const {
    return size_;
  }

  size_t getPropertySize() const {
    return property_size_;
  }

  void print() {
    printf("-----------------print list----------------\n");
    printf("---%ld:\n", getEdgeNum());
    MemEdgeIterator it = MemEdgeIterator(this);
    for (; it.valid(); it.next()) {
      LOG_INFO(" read next edge...");
      it.key().print();
    }
    printf("-------------------------------------------\n");
  }

  void prefetch() {
    __builtin_prefetch(head_, 0, 0);
  }

  size_t size() const {
    return size_;
  }

  MemEdgeIterator begin(const FileId_t fid) const {
    return MemEdgeIterator{this, fid};
  }

  void sort() {
    // no need to sort.
  }

 private:
  struct Node;

  std::vector<char *> recycle_mem;
  Node *const         head_;
  // Used for optimizing sequential insert patterns.
  Node           **prev_;
  std::atomic<int> max_height_;  // Height of the entire list
  uint32_t         prev_height_;
  uint32_t         size_;
  uint32_t         property_size_;
  Node            *dst_;

  Node *NewNode(const K &key, int height) {
    char *mem = (char *)malloc(sizeof(Node) + sizeof(std::atomic<Node *>) * (height - 1));
    recycle_mem.push_back(mem);
    return new (mem) Node(key);
  }

  Node *GetNewNode(const int height) {
    char *mem = (char *)malloc(sizeof(Node) + sizeof(std::atomic<Node *>) * (height - 1));
    recycle_mem.push_back(mem);
    return new (mem) Node();
  }

  Node *GetOldNode(int id) {
    return dst_ + id;
  }

  int RandomHeight() {
    uint height = 1;
    while (height < kMaxHeight_ && static_cast<uint32_t>(rand()) < kScaledInverseBranching_) height++;

    assert(height > 0);
    assert(height <= kMaxHeight_);
    return height;
  }

  bool KeyIsAfterNode(const K &key, Node *n) const {
    // nullptr n is considered infinite
    return (n != nullptr) && (Cmp::compare(n->key, key) < 0);
  }

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  bool Equal(const K &a, const K &b) const {
    return (Cmp::compare(a, b) == 0);
  }

  bool LessThan(const K &a, const K &b) const {
    return (Cmp::compare(a, b) < 0);
  }

  // Returns the earliest node with a key >= key.
  // Return nullptr if there is no such node.
  Node *FindGreaterOrEqual(const K &key) const {
    Node *x           = head_;
    int   level       = GetMaxHeight() - 1;
    Node *last_bigger = nullptr;
    while (true) {
      assert(x != nullptr);
      Node *next = x->Next(level);
      // Make sure the lists are sorted
      assert(x == head_ || next == nullptr || KeyIsAfterNode(next->key, x));
      // Make sure we haven't overshot during our search
      assert(x == head_ || KeyIsAfterNode(key, x));
      int cmp = (next == nullptr || next == last_bigger) ? 1 : Cmp::compare(next->key, key);
      if (cmp == 0 || (cmp > 0 && level == 0)) {
        return next;
      } else if (cmp < 0) {
        // Keep searching in this list
        x = next;
      } else {
        // Switch to next list, reuse Cmp::compare() result
        last_bigger = next;
        level--;
      }
    }
  }

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node *FindLessThan(const K &key, Node **prev = nullptr) const {
    Node *x     = head_;
    int   level = GetMaxHeight() - 1;
    // KeyIsAfter(key, last_not_after) is definitely false
    Node *last_not_after = nullptr;
    while (true) {
      assert(x != nullptr);
      Node *next = x->Next(level);
      assert(x == head_ || next == nullptr || KeyIsAfterNode(next->key, x));
      assert(x == head_ || KeyIsAfterNode(key, x));
      if (next != last_not_after && KeyIsAfterNode(key, next)) {
        // Keep searching in this list
        x = next;
      } else {
        if (prev != nullptr) {
          prev[level] = x;
        }
        if (level == 0) {
          return x;
        } else {
          // Switch to next list, reuse KeyIUsAfterNode() result
          last_not_after = next;
          level--;
        }
      }
    }
  }

  Node *FindLast() const {
    Node *x     = head_;
    int   level = GetMaxHeight() - 1;
    while (true) {
      Node *next = x->Next(level);
      if (next == nullptr) {
        if (level == 0) {
          return x;
        } else {
          // Switch to next list
          level--;
        }
      } else {
        x = next;
      }
    }
  }
};

template <typename K, class Cmp>
struct SkipList<K, Cmp>::Node {
  K key;

  explicit Node(K k)
      : key(std::move(k))
      , next_{nullptr} {}

  explicit Node()
      : key()
      , next_{nullptr} {}

  ~Node() {
    next_[0] = nullptr;
  }

  void cleal_Key() {
    key.~K();
  }

  Node *Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return next_[n].load(std::memory_order_acquire);
  }

  void SetNext(int n, Node *next) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node
    next_[n].store(next, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node *NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }

  void NoBarrier_SetNext(int n, Node *x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  std::atomic<Node *> next_[1];
};

template <typename K, class Cmp>
class SkipList<K, Cmp>::MemEdgeIterator : public EdgeIteratorBase {
 public:
  // Initialize an iterator over the specified list.
  // The returned iterator is not valid.
  explicit MemEdgeIterator(const SkipList *list, FileId_t fid = INVALID_File_ID)
      : fid_(fid) {
    SetList(list);
  }

  explicit MemEdgeIterator()
      : list_(nullptr)
      , node_(nullptr)
      , fid_(INVALID_File_ID) {}

  bool operator==(const MemEdgeIterator &rhs) const {
    return node_ == rhs.node_;
  }

  void SetList(const SkipList *list) {
    list_ = list;
    node_ = nullptr;
    if (list_ == nullptr) {
      return;
    }
    SeekToFirst();
  }

  FileId_t get_fid() const {
    return fid_;
  }

  bool valid() const {
    return node_ != nullptr;
  }

  // Returns the key at the current position.
  const K &key() {
    assert(valid());
    return node_->key;
  }

  VertexId_t dst_id() const {
    assert(valid());
    return node_->key.destination();
  }

  SequenceNumber_t sequence() const {
    assert(valid());
    return node_->key.sequence();
  }

  Marker_t marker() const {
    assert(valid());
    return node_->key.marker();
  }

  EdgeProperty_t edge_data() const {
    assert(valid());
    return node_->key.property();
  }

  bool empty() const {
    assert(valid());
    return list_->getEdgeNum() == 0;
  }

  size_t size() const {
    assert(valid());
    return list_->getEdgeNum();
  }

  void next() {
    assert(valid());
    node_ = node_->Next(0);
  }

  void Prev() {
    assert(valid());
    node_ = list_->FindLessThan(node_->key);
    if (node_ == list_->head_) {
      node_ = nullptr;
    }
  }

  // Advance to the first entry with a key >= target
  void Seek(const K &target) {
    node_ = list_->FindGreaterOrEqual(target);
  }

  // Retreat to the last entry with a key <= target
  void SeekForPrev(const K &target) {
    Seek(target);
    if (!valid()) {
      SeekToLast();
    }
    while (valid() && list_->LessThan(target, key())) {
      Prev();
    }
  }

  void SeekToFirst() {
    node_ = list_->head_->Next(0);
  }

  void SeekToLast() {
    node_ = list_->FindLast();
    if (node_ == list_->head_) {
      node_ = nullptr;
    }
  }

  bool IsMemTable() const {
    return is_mem_table;
  }

 private:
  const SkipList *list_;
  Node           *node_;
  FileId_t        fid_;

  bool is_mem_table = true;
};

class NewEdgeSL {
 private:
  bool                            full_;
  Edge                           *dst_;
  uint32_t                        cnt_;
  size_t                          property_size_;
  SkipList<Edge, EdgeComparator> *list_;

 public:
  class MemEdgeIterator;

  NewEdgeSL()
      : full_(false)
      , dst_(nullptr)
      , cnt_(0)
      , property_size_(0)
      , list_(nullptr) {
    if (FLAGS_reserve_node > 0) {
      dst_ = new Edge[FLAGS_reserve_node];
    }
  }

  ~NewEdgeSL() {
    if (FLAGS_reserve_node > 0) {
      delete[] dst_;
    }
    if (full_) {
      delete list_;
    }
  }

  void put_edge(VertexId_t dst, SequenceNumber_t seq, Marker_t marker, const EdgeProperty_t &property) {
    if (!full_) {
      if (cnt_ == FLAGS_reserve_node) {
        list_ = new SkipList<Edge, EdgeComparator>;
        for (uint i = 0; i < FLAGS_reserve_node; i++) {
          list_->put_edge(dst_[i].destination(), dst_[i].sequence(), dst_[i].marker(), dst_[i].property());
        }
        list_->put_edge(dst, seq, marker, property);
        full_ = true;
      } else {
        Edge edge(dst, seq, marker, property);
#ifdef SORTED_ARRAY
#else
        dst_[cnt_++] = edge;
#endif
      }
    } else {
      list_->put_edge(dst, seq, marker, property);
    }
    property_size_ += property.size();
  }

  Status get(VertexId_t dst, std::string *property) {
    if (!full_) {
      uint i = 0;
      while (i < this->cnt_) {
        if (this->dst_[i].destination() == dst) {
          if (this->dst_[i].marker()) return Status::kDelete;
          property->assign(this->dst_[i].property().data(), this->dst_[i].property().size());
          return Status::kOk;
        }
        i++;
      }
      return Status::kNotFound;
    } else {
      return list_->get(dst, property);
    }
  }

  Status get(VertexId_t dst, SequenceNumber_t &seq) {
    if (!full_) {
      uint i = 0;
      while (i < this->cnt_) {
        if (this->dst_[i].destination() == dst) {
          if (this->dst_[i].marker()) return Status::kDelete;
          seq = this->dst_[i].sequence();
          return Status::kOk;
          ;
        }
        i++;
      }
      return Status::kNotFound;
    } else {
      return list_->get(dst, seq);
    }
  }

  void get_edges(VertexId_t src, std::vector<Edge> &edges) {
    edges.reserve(edges.size() + this->cnt_);
    if (!full_) {
      VertexId_t saved_key = INVALID_VERTEX_ID;
      bool       skipping  = false;
      for (uint i = 0; i < this->cnt_; i++) {
        if (!(skipping && this->dst_[i].destination() == saved_key)) {
          if (this->dst_[i].marker() == 0) {
            edges.emplace_back(this->dst_[i]);
          } else {
            saved_key = this->dst_[i].destination();
            skipping  = true;
          }
        }
      }
    } else {
      list_->get_edges(src, edges);
    }
  }

  Edge getvector(uint i) {
    if (i + 1 > cnt_) {
      LOG_INFO("error");
      return {0, 0};
    } else
      return dst_[i];
  }

  int getVectorNum() const {
    return cnt_;
  }

  int getEdgeNum() const {
    if (!full_) {
      return cnt_;
    } else {
      return list_->size();
    }
  }

  size_t getPropertySize() const {
    return property_size_;
  }

  bool isfull() const {
    return full_;
  }

  SkipList<Edge, EdgeComparator>::MemEdgeIterator getListHead(const FileId_t fid) const {
    return list_->begin(fid);
  }

  const Edge *getArrayHead() const {
    return dst_;
  }

  void reset() {
    if (full_) {
      delete list_;
    }
    full_          = false;
    cnt_           = 0;
    property_size_ = 0;
  }

  void sort() {
#ifndef SORTED_ARRAY
    if (!full_) {
      std::sort(dst_, dst_ + cnt_, [](const Edge &lhs, const Edge &rhs) -> bool {
        if (lhs.destination() != rhs.destination()) {
          return lhs.destination() < rhs.destination();
        } else {
          return lhs.sequence() > rhs.sequence();
        }
      });
    }
#endif
  }
};

class NewEdgeSL::MemEdgeIterator : public EdgeIteratorBase {
 public:
  explicit MemEdgeIterator(const NewEdgeSL *edges, FileId_t fid = INVALID_File_ID)
      : edges_(edges)
      , fid_(fid) {
    init();
  }

  void init() {
    if (edges_ == nullptr) {
      return;
    }
    if (!edges_->isfull()) {
      array_it_ = edges_->getArrayHead();
    } else {
      skip_list_it_ = SkipList<Edge, EdgeComparator>::MemEdgeIterator(edges_->list_, fid_);
    }
  }

  bool valid() const override {
    if (edges_ == nullptr) {
      return false;
    }
    if (!edges_->isfull()) {
      return array_it_ != edges_->getArrayHead() + edges_->getVectorNum();
    } else {
      return skip_list_it_.valid();
    }
  }

  const Edge &key() {
    assert(valid());
    if (!edges_->isfull()) {
      return *array_it_;
    } else {
      return skip_list_it_.key();
    }
  }

  void next() override {
    if (!edges_->isfull()) {
      array_it_++;
    } else {
      skip_list_it_.next();
    }
  }

  VertexId_t dst_id() const override {
    if (!edges_->isfull()) {
      return array_it_->destination();
    } else {
      return skip_list_it_.dst_id();
    }
  }

  Marker_t marker() const override {
    if (!edges_->isfull()) {
      return array_it_->marker();
    } else {
      return skip_list_it_.marker();
    }
  }

  SequenceNumber_t sequence() const override {
    if (!edges_->isfull()) {
      return array_it_->sequence();
    } else {
      return skip_list_it_.sequence();
    }
  }

  EdgeProperty_t edge_data() const override {
    if (!edges_->isfull()) {
      return array_it_->property();
    } else {
      return skip_list_it_.edge_data();
    }
  }

  bool empty() const override {
    return !edges_->getEdgeNum();
  }

  size_t size() const override {
    return edges_->getEdgeNum();
  }

  bool IsMemTable() const override {
    return is_mem_table;
  }

  FileId_t get_fid() const override {
    return fid_;
  }

 private:
  const NewEdgeSL                                *edges_;
  SkipList<Edge, EdgeComparator>::MemEdgeIterator skip_list_it_;
  const Edge                                     *array_it_;
  FileId_t                                        fid_;
  bool                                            is_mem_table = true;
};

}  // namespace lsmg
#endif