#pragma once

#include "common/config.h"
#include "container/edge_iterator_base.h"
#include "container/slice.h"
#include "graph/edge.h"

namespace lsmg {
class SSTEdgeIterator : public EdgeIteratorBase {
 public:
  SSTEdgeIterator(EdgeBody_t *body_data, char *property_data, size_t body_num, FileId_t fid = INVALID_File_ID)
      : body_data_(body_data)
      , property_data_(property_data)
      , body_num_(body_num)
      , body_cursor_(body_data)
      , body_end_(body_data_ + body_num_)
      , fid_(fid) {}

  SSTEdgeIterator() = default;

  void init() {}

  bool valid() const override {
    return body_cursor_ < body_end_;
  }

  void next() override {
    body_cursor_++;
  }

  VertexId_t dst_id() const override {
    return body_cursor_->get_dst();
  }

  Marker_t marker() const override {
    return body_cursor_->get_marker();
  }

  SequenceNumber_t sequence() const override {
    return body_cursor_->get_seq();
  }

  EdgeProperty_t edge_data() const override {
    if (!valid()) {
      return EdgeProperty_t();
    } else {
      return EdgeProperty_t(property_data_ + body_cursor_->get_prop_pointer(),
                            (body_cursor_ + 1)->get_prop_pointer() - body_cursor_->get_prop_pointer());
    }
  }

  Slice edge_slice_data() {
    if (!valid()) {
      return EdgeProperty_t();
    } else {
      return Slice(property_data_ + body_cursor_->get_prop_pointer(),
                   (body_cursor_ + 1)->get_prop_pointer() - body_cursor_->get_prop_pointer());
    }
  }

  bool empty() const override {
    return !body_data_;
  }

  size_t size() const override {
    return body_num_;  // we need to merge before we can count
  }

  FileId_t get_fid() const override {
    return fid_;
  }

  bool IsMemTable() const override {
    return is_mem_table;
  }

 private:
  EdgeBody_t *body_data_;
  char       *property_data_;
  size_t      body_num_;
  EdgeBody_t *body_cursor_;
  EdgeBody_t *body_end_;
  FileId_t    fid_;
  bool        is_mem_table = false;
};
}  // namespace lsmg