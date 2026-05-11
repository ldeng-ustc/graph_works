#ifndef VERTEX_H
#define VERTEX_H

#include <array>
#include <cstring>
#include <iostream>
#include "common/config.h"

namespace lsmg {

class Vertex {
 public:
  Vertex() = default;
  Vertex(VertexId_t id, SequenceNumber_t seq, VertexOffset_t prev_pointer, VertexProperty_t property)
      : id_(id)
      , seq_(seq)
      , prev_pointer_(prev_pointer)
      , property_(property) {}

  ~Vertex() noexcept {}

  Vertex(const Vertex &other) {
    id_           = other.id_;
    seq_          = other.seq_;
    prev_pointer_ = other.prev_pointer_;
    property_     = other.property_;
  }

  Vertex(Vertex &&other) noexcept {
    id_           = std::move(other.id_);
    seq_          = std::move(other.seq_);
    prev_pointer_ = std::move(other.prev_pointer_);
    property_     = std::move(other.property_);
  }

  Vertex &operator=(const Vertex &other) {
    if (this != &other) {
      id_           = other.id_;
      seq_          = other.seq_;
      prev_pointer_ = other.prev_pointer_;
      property_     = other.property_;
    }
    return *this;
  }

  Vertex &operator=(Vertex &&other) noexcept {
    if (this != &other) {
      id_           = std::move(other.id_);
      seq_          = std::move(other.seq_);
      prev_pointer_ = std::move(other.prev_pointer_);
      property_     = std::move(other.property_);
    }
    return *this;
  }

  bool operator==(const Vertex &other) const {
    return id_ == other.id_ && seq_ == other.seq_ && prev_pointer_ == other.prev_pointer_
           && property_ == other.property_;
  }

  bool operator!=(const Vertex &other) const {
    return !(*this == other);
  }

  bool operator<(const Vertex &rhs) const {
    if (id_ == rhs.id_) {
      return seq_ < rhs.seq_;
    }
    return id_ < rhs.id_;
  }

  void Serialize(char *buffer) const {
    memcpy(buffer, this, sizeof(Vertex));
  }

  void Deserialize(const char *buffer) {
    memcpy(this, buffer, sizeof(Vertex));
  }

  VertexId_t id() const {
    return id_;
  }

  void set_id(VertexId_t id) {
    id_ = id;
  }

  SequenceNumber_t sequence() const {
    return seq_;
  }
  void set_sequence(SequenceNumber_t seq) {
    seq_ = seq;
  }

  VertexOffset_t prev_pointer() const {
    return prev_pointer_;
  }
  void set_prev_pointer(VertexOffset_t prev_pointer) {
    prev_pointer_ = prev_pointer;
  }

  VertexProperty_t property() const {
    return property_;
  }
  void set_property(VertexProperty_t property) {
    property_ = property;
  }

  void print() const {
    printf("print vertex information:\n");
    LOG_INFO("  id: {}", id_);
    LOG_INFO("  seq: {}", seq_);
    LOG_INFO("  prev_pointer: {}", prev_pointer_);
    LOG_INFO("  spropertyeq: ");
    for (const auto &s : property_) LOG_INFO("{}", s);
  }

 private:
  VertexId_t       id_;
  SequenceNumber_t seq_;           // timestamp
  VertexOffset_t   prev_pointer_;  // vertex offset
  VertexProperty_t property_;      // property data
};

}  // namespace lsmg

#endif  // VERTEX_H