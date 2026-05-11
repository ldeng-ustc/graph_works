#ifndef LSMG_EDGE_HEADER
#define LSMG_EDGE_HEADER

#include <memory.h>
#include <cassert>
#include <iostream>
#include "common/config.h"
#include "common/utils/fix_string.h"
#include "common/utils/logger.h"

namespace lsmg {

class EdgeBody24_t;
struct EdgeBody16_t;

using EdgeBody_t = EdgeBody16_t;

class Edge_string;
class Edge_fixstring;

using Edge = Edge_string;

class EdgeBody24_t {
 private:
  VertexId_t           dst_;
  SequenceNumber_t     seq_;
  EdgePropertyOffset_t prop_pointer_;
  Marker_t             marker_;

 public:
  EdgeBody24_t(VertexId_t dst = INVALID_VERTEX_ID, SequenceNumber_t seq = 0, EdgePropertyOffset_t prop_pointer = 0x00,
               Marker_t marker = false)
      : dst_{dst}
      , seq_{seq}
      , prop_pointer_{prop_pointer}
      , marker_{marker} {}

  friend bool operator<(const EdgeBody24_t &rhs1, const EdgeBody24_t &rhs2) {
    if (rhs1.dst_ < rhs2.dst_) {
      return true;
    } else if (rhs1.dst_ == rhs2.dst_) {
      return rhs1.seq_ > rhs2.seq_;  // The newer the time, the higher the front.
    }
    return false;
  }

  void set_dst(VertexId_t dst) {
    dst_ = dst;
  }

  VertexId_t get_dst() const {
    return dst_;
  }

  void set_seq(VertexId_t seq) {
    seq_ = seq;
  }

  VertexId_t get_seq() const {
    return seq_;
  }

  void set_prop_pointer(EdgePropertyOffset_t _prop_pointer) {
    prop_pointer_ = _prop_pointer;
  }

  EdgePropertyOffset_t get_prop_pointer() const {
    return prop_pointer_;
  }

  void set_marker(bool marker) {
    marker_ = marker;
  }

  bool get_marker() const {
    return marker_;
  }

  void print(std::string label = "") const {
    if (label != "") printf("print edge information (%s):\n", label.c_str());
    LOG_INFO("  dst_: {}  seq_: {}  marker_: {}  prop_pointer_: {}", get_dst(), get_seq(), get_marker(),
             get_prop_pointer());
  }
};

struct EdgeBody16_t {
 private:
  uint32_t prop_pointer;
  uint32_t dst_low;
  uint32_t seq_low;
  uint16_t dst_high;
  uint16_t seq_high;

 public:
  EdgeBody16_t(VertexId_t dst = INVALID_VERTEX_ID, SequenceNumber_t seq = 0, EdgePropertyOffset_t _prop_pointer = 0x00,
               Marker_t marker = false) {
    set_dst(dst);
    set_seq(seq);
    set_prop_pointer(_prop_pointer);
    set_marker(marker);
  }

  void set_dst(VertexId_t dst) {
    assert(dst <= ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX);
    dst_high = (dst >> 32) & UINT16_MAX;
    dst_low  = dst & UINT32_MAX;
  }

  VertexId_t get_dst() const {
    return ((VertexId_t)dst_high << 32) + (VertexId_t)dst_low;
  }

  void set_seq(VertexId_t seq) {
    assert(seq <= ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX);
    seq_high = (seq >> 32) & UINT16_MAX;
    seq_low  = seq & UINT32_MAX;
  }

  VertexId_t get_seq() const {
    return ((VertexId_t)seq_high << 32) + (VertexId_t)seq_low;
  }

  void set_prop_pointer(EdgePropertyOffset_t _prop_pointer) {
    assert(_prop_pointer <= (UINT32_MAX >> 1));
    prop_pointer = (_prop_pointer & 0x7FFFFFFF) | (prop_pointer & 0x80000000);
  }

  EdgePropertyOffset_t get_prop_pointer() {
    return prop_pointer & 0x7FFFFFFF;  // prop_pointer & 0x7FFFFFFF
  }

  void set_marker(bool marker) {
    prop_pointer = (prop_pointer & 0x7FFFFFFF) | (marker << 31);
  }

  bool get_marker() const {
    return (prop_pointer >> 31) & 0x1;
  }

  friend bool operator<(const EdgeBody16_t &rhs1, const EdgeBody16_t &rhs2) {
    if (rhs1.get_dst() < rhs2.get_dst()) {
      return true;
    } else if (rhs1.get_dst() == rhs2.get_dst()) {
      return rhs1.get_seq() > rhs2.get_seq();  // The newer the time, the higher the front.
    }
    return false;
  }

  // debug
  void print(std::string label = "") const {
    if (label != "") printf("print edge information (%s):\n", label.c_str());
    LOG_INFO("  dst_: {}  seq_: {}  marker_: {}  prop_pointer_: {}", get_dst(), get_seq(), get_marker(), prop_pointer);
  }
};
static_assert(sizeof(EdgeBody16_t) == 16);

class Edge_fixstring {
 public:
  Edge_fixstring(VertexId_t dst = INVALID_VERTEX_ID, SequenceNumber_t seq = 0, Marker_t marker = false,
                 std::string property = "") {
    set_destination(dst);
    set_sequence(seq);
    set_marker(marker);
    set_property(property);
  }

  ~Edge_fixstring(){};

  Edge_fixstring(const Edge_fixstring &other) {
    dst_      = other.dst_;
    seq_      = other.seq_;
    property_ = other.property_;
  }

  Edge_fixstring(Edge_fixstring &&other) {
    dst_      = std::move(other.dst_);
    seq_      = std::move(other.seq_);
    property_ = std::move(other.property_);
  }

  Edge_fixstring(EdgeBody_t &other) {
    dst_ = other.get_dst();
    seq_ = other.get_seq();
  }

  void reset() {
    set_destination(INVALID_VERTEX_ID);
    set_sequence(0);
    set_marker(false);
    set_property("");
  }

  Edge_fixstring &operator=(const Edge_fixstring &other) {
    if (this != &other) {
      dst_      = other.dst_;
      seq_      = other.seq_;
      property_ = other.property_;
    }
    return *this;
  }

  Edge_fixstring &operator=(Edge_fixstring &&other) {
    if (this != &other) {
      dst_      = std::move(other.dst_);
      seq_      = std::move(other.seq_);
      property_ = std::move(other.property_);
    }
    return *this;
  }

  bool operator==(const Edge_fixstring &other) const {
    return dst_ == other.dst_;
  }

  bool operator!=(const Edge_fixstring &other) const {
    return !(*this == other);
  }

  bool operator<(const Edge_fixstring &rhs) {
    if (dst_ < rhs.dst_) {
      return true;
    } else if (dst_ == rhs.dst_) {
      return seq_ > rhs.seq_;  // The newer the time, the higher the front.
    }
    return false;
  }
  friend bool operator<(const Edge_fixstring &rhs1, const Edge_fixstring &rhs2) {
    if (rhs1.dst_ < rhs2.dst_) {
      return true;
    } else if (rhs1.dst_ == rhs2.dst_) {
      return rhs1.seq_ > rhs2.seq_;  // The newer the time, the higher the front.
    }
    return false;
  }
  bool operator>(const Edge_fixstring &rhs) {
    if (dst_ > rhs.dst_) {
      return true;
    } else if (dst_ == rhs.dst_) {
      return seq_ < rhs.seq_;  // The newer the time, the higher the front.
    }
    return false;
  }

  bool Equal(const Edge_fixstring &rhs) const {
    return dst_ == rhs.dst_;
  }
  // Used for comparison in lookups
  bool LessThan(const Edge_fixstring &rhs) {
    if (dst_ < rhs.dst_) {
      return true;
    }
    return false;
  }

  VertexId_t destination() const {
    return dst_;
  }
  void set_destination(VertexId_t dst) {
    dst_ = dst;
  }

  SequenceNumber_t sequence() const {
    return seq_ >> 1;
  }
  void set_sequence(SequenceNumber_t seq) {
    assert(seq <= (INT64_MAX >> 1));
    seq_ = (seq << 1) | (seq_ & 0x1);
  }

  Marker_t marker() const {
    return seq_ & 0x1;
  }
  void set_marker(Marker_t marker) {
    seq_ = (seq_ & (~(static_cast<SequenceNumber_t>(1)))) | marker;
  }

  void set_property(const std::string &property) {
    property_ = property;
  }
  const FixString &property() const {
    return property_;
  }
  void set_property(FixString &property) {
    property_ = property;
  }

  uint32_t get_body_size() const {
    return sizeof(EdgeBody_t);
  }

  uint32_t propertySize() const {
    return property_.size();
  }

  void print(std::string label = "") const {
    if (label != "") printf("print edge information (%s):\n", label.c_str());
    // LOG_INFO("  dst_: {}  seq_: {}  marker_: {}  property_: {}", destination(), sequence(), marker(), property());
  }

 private:
  VertexId_t       dst_;
  SequenceNumber_t seq_;
  FixString        property_;
};

class Edge_string {
 public:
  Edge_string(VertexId_t dst = INVALID_VERTEX_ID, SequenceNumber_t seq = 0, Marker_t marker = false,
              EdgeProperty_t property = "")
      : dst_{dst}
      , seq_{seq}
      , marker_{marker}
      , property_{property} {}

  ~Edge_string(){};

  Edge_string(const Edge_string &other) {
    dst_      = other.dst_;
    seq_      = other.seq_;
    marker_   = other.marker_;
    property_ = other.property_;
  }

  Edge_string(Edge_string &&other) {
    dst_      = std::move(other.dst_);
    seq_      = std::move(other.seq_);
    marker_   = std::move(other.marker_);
    property_ = std::move(other.property_);
  }

  Edge_string(EdgeBody_t &other) {
    dst_      = other.get_dst();
    seq_      = other.get_seq();
    marker_   = other.get_marker();
    property_ = "no get property";
  }

  void reset() {
    dst_      = INVALID_VERTEX_ID;
    seq_      = 0;
    marker_   = false;
    property_ = "";
  }

  Edge_string &operator=(const Edge_string &other) {
    if (this != &other) {
      dst_      = other.dst_;
      seq_      = other.seq_;
      marker_   = other.marker_;
      property_ = other.property_;
    }
    return *this;
  }

  Edge_string &operator=(Edge_string &&other) {
    if (this != &other) {
      dst_      = std::move(other.dst_);
      seq_      = std::move(other.seq_);
      marker_   = std::move(other.marker_);
      property_ = std::move(other.property_);
    }
    return *this;
  }

  bool operator==(const Edge_string &other) const {
    return dst_ == other.dst_;
  }

  bool operator!=(const Edge_string &other) const {
    return !(*this == other);
  }

  bool operator<(const Edge_string &rhs) {
    if (dst_ < rhs.dst_) {
      return true;
    } else if (dst_ == rhs.dst_) {
      return seq_ > rhs.seq_;  // The newer the time, the higher the front.
    }
    return false;
  }
  friend bool operator<(const Edge_string &rhs1, const Edge_string &rhs2) {
    if (rhs1.dst_ < rhs2.dst_) {
      return true;
    } else if (rhs1.dst_ == rhs2.dst_) {
      return rhs1.seq_ > rhs2.seq_;  // The newer the time, the higher the front.
    }
    return false;
  }
  bool operator>(const Edge_string &rhs) {
    if (dst_ > rhs.dst_) {
      return true;
    } else if (dst_ == rhs.dst_) {
      return seq_ < rhs.seq_;  // The newer the time, the higher the front.
    }
    return false;
  }

  bool Equal(const Edge_string &rhs) const {
    return dst_ == rhs.dst_;
  }
  bool LessThan(const Edge_string &rhs) {
    if (dst_ < rhs.dst_) {
      return true;
    }
    return false;
  }

  VertexId_t destination() const {
    return dst_;
  }
  void set_destination(VertexId_t dst) {
    dst_ = dst;
  }

  SequenceNumber_t sequence() const {
    return seq_;
  }
  void set_sequence(SequenceNumber_t seq) {
    seq_ = seq;
  }

  Marker_t marker() const {
    return marker_;
  }
  void set_marker(Marker_t marker) {
    marker_ = marker;
  }

  EdgeProperty_t property() const {
    return property_;
  }
  void set_property(EdgeProperty_t property) {
    property_ = property;
  }

  uint32_t get_body_size() const {
    return sizeof(EdgeBody_t);  // + property_.size();
  }

  uint32_t propertySize() const {
    return property_.size();
  }

  void print(std::string label = "") const {
    if (label != "") printf("print edge information (%s):\n", label.c_str());
    LOG_INFO("  dst_: {}  seq_: {}  marker_: {}  property_: {}", dst_, seq_, marker_, property_);
  }

 private:
  VertexId_t       dst_;       // VertexId_t src_;
  SequenceNumber_t seq_;       // EdgeOffset_t prev_pointer_;
  Marker_t         marker_;    // EdgePropertyOffset_t prop_pointer_;
  EdgeProperty_t   property_;  // In memory, edge properties stay with the edge
};

struct EdgeComparator {
  static int compare(const Edge &a, const Edge &b) {
    if (a.destination() < b.destination()) {
      return -1;
    } else if (a.destination() > b.destination()) {
      return +1;  // The newer the time, the higher the front.
    } else {
      if (a.sequence() < b.sequence()) {
        return +1;
      } else if (a.sequence() > b.sequence()) {
        return -1;
      } else {
        return 0;
      }
    }
  }
};

}  // namespace lsmg

#endif  // EDGE_H