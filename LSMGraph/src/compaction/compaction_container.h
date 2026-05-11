#ifndef LSMG_COMPACTION_CONTIANTER_HEADER
#define LSMG_COMPACTION_CONTIANTER_HEADER

#include "cache/buffer_manager.h"
#include "cache/sst_table_cache.h"
#include "common/config.h"
#include "container/slice.h"
#include "container/sst_edge_iterator.h"
#include "graph/edge.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <optional>

namespace lsmg {

struct EdgeRecord {
  VertexId_t       src_;
  VertexId_t       dst_;
  SequenceNumber_t seq_;
  Slice            prop_;
  Marker_t         marker_;

  EdgeRecord() {}

  EdgeRecord(VertexId_t src, VertexId_t dst, SequenceNumber_t seq, Slice prop, Marker_t marker)
      : src_(src)
      , dst_(dst)
      , seq_(seq)
      , prop_(prop)
      , marker_(marker) {}

  EdgeRecord(VertexId_t src, const EdgeBody_t &edge_body, const Slice &prop)
      : src_(src)
      , dst_(edge_body.get_dst())
      , seq_(edge_body.get_seq())
      , prop_(prop)
      , marker_(edge_body.get_marker()) {}

  void SetValue(VertexId_t src, VertexId_t dst, SequenceNumber_t seq, Slice prop, Marker_t marker) {
    src_    = src;
    dst_    = dst;
    seq_    = seq;
    prop_   = prop;
    marker_ = marker;
  }

  bool operator<(const EdgeRecord &rhs) const {
    if (src_ < rhs.src_) {
      return true;
    } else if (src_ == rhs.src_) {
      if (dst_ < rhs.dst_) {
        return true;
      } else if (dst_ == rhs.dst_) {
        return seq_ > rhs.seq_;  // The newer the time, the higher the front.
      }
    }
    return false;
  }

  bool operator>(const SSTEdgeIterator &rhs) const {
    if (dst_ > rhs.dst_id()) {
      return true;
    } else if (dst_ == rhs.dst_id()) {
      return seq_ < rhs.sequence();  // The newer the time, the higher the front.
    }
    return false;
  }
};

struct Anchor {
  VertexId_t begin_;
  size_t     range_size_;

  Anchor() {}

  Anchor(VertexId_t begin, size_t range_size)
      : begin_(begin)
      , range_size_(range_size) {}
};

struct WayAnchor {
  VertexId_t   begin_;
  VertexId_t   end;
  EdgeOffset_t offset;  // edge offset

  VertexId_t  index_start_offset;  // index id
  size_t      index_num;           // index num
  size_t      range_size;          // edge_offset range size
  std::string path;                // file path
  FileId_t    fid;
  bool        is_input_0;

  WayAnchor() {}

  WayAnchor(VertexId_t _begin, VertexId_t _end, EdgeOffset_t _offset, VertexId_t _index_start_offset, size_t _index_num,
            size_t _range_size, std::string _path, FileId_t _fid, bool _is_input_0)
      : begin_(_begin)
      , end(_end)
      , offset(_offset)
      , index_start_offset(_index_start_offset)
      , index_num(_index_num)
      , range_size(_range_size)
      , path(_path)
      , fid(_fid)
      , is_input_0(_is_input_0) {}

  void print() {
    LOG_INFO(
        " WayAnchor: begin={} end={} offset={} _index_start_offset={} _index_num={} range_size={} _fid={} "
        "is_input_0={}",
        begin_, end, offset, index_start_offset, index_num, (range_size / sizeof(EdgeBody_t)), fid, is_input_0);
  }
};

class Way {
 public:
  Way(SSTableCache *c, BufferManager &buffer_manager)
      : total_edge_cnt_(c->header.size)
      , total_index_cnt_(c->header.index_size)
      , edge_start_offset_(0)
      , property_start_offset_(0)
      , index_start_offset_(total_edge_cnt_ * sizeof(EdgeBody_t))
      , remain_edge_cnt_(c->header.size)
      , path_(c->path)
      , fid_(c->header.timeStamp)
      , buffer_manager_(buffer_manager) {}

  Way(BufferManager &buffer_manager, size_t index_num, EdgeOffset_t index_offset, size_t edge_num, std::string &path,
      FileId_t fid, bool is_input_0)
      : total_edge_cnt_(edge_num)
      , total_index_cnt_(index_num)
      , index_start_offset_(index_offset)
      , remain_edge_cnt_(edge_num)
      , path_(path)
      , fid_(fid)
      , buffer_manager_(buffer_manager)
      , is_input_0_(is_input_0) {}

  void Init() {
    assert(total_index_cnt_ <= MAX_INDEX_NUM);

    e_file_fd = open(path_.c_str(), O_RDONLY);
    p_file_fd = open((path_ + "_p").c_str(), O_RDONLY);
    assert(e_file_fd >= 0);
    assert(p_file_fd >= 0);

    index_buffer_ = new Index[total_index_cnt_];
    ReadIndex(total_index_cnt_);

    assert(total_index_cnt_ > 0);
    edge_start_offset_ = index_buffer_[0].offset;

    body_buffer_ = buffer_manager_.GetEdgeBodyBuffer();
    FillBodyBuffer();

    assert(total_edge_cnt_ > 0);
    property_start_offset_ = body_buffer_[0].get_prop_pointer();

    property_buffer_ = buffer_manager_.GetPropertyBuffer();
    FillPropertyBuffer();
  }

  void FreeBuffer() {
    if (index_buffer_ != nullptr) {
      delete[] index_buffer_;
      buffer_manager_.FreeEdgeBodyBuffer(body_buffer_);
      buffer_manager_.FreePropertyBuffer(property_buffer_);
      index_buffer_    = nullptr;
      body_buffer_     = nullptr;
      property_buffer_ = nullptr;
    }
    if (e_file_fd != -1) {
      close(e_file_fd);
      close(p_file_fd);
      e_file_fd = -1;
      p_file_fd = -1;
    }
    has_released = true;
  }

  bool IsReleased() const {
    return has_released;
  }

  ~Way() {
    if (e_file_fd != -1) {
      close(e_file_fd);
      close(p_file_fd);
    }
    if (index_buffer_ != nullptr) {
      delete[] index_buffer_;
      buffer_manager_.FreeEdgeBodyBuffer(body_buffer_);
      buffer_manager_.FreePropertyBuffer(property_buffer_);
      LOG_INFO(" error: don't freebuffer in way.");
    }
  }

  std::optional<EdgeRecord> NextEdgeRecord() {
    if (EdgeBodyOffset() >= index_buffer_[index_ptr_ + 1].offset) {
      // need to change src vertex
      index_ptr_++;
      if (index_ptr_ >= total_index_cnt_ - 1) {
        return std::nullopt;
      }
    }
    auto edge_body = CurEdgeBody();
    remain_edge_cnt_--;
    if (body_ptr_ == BODY_BUFFER_SIZE - 1) {
      FillBodyBuffer();
    } else {
      body_ptr_++;
    }

    if (remain_edge_cnt_ == 0) {
      lseek(p_file_fd, 0L, SEEK_END);
    }

    size_t prop_len = CurEdgeBody().get_prop_pointer() - edge_body.get_prop_pointer();
    if (prop_buffer_ptr_ + prop_len > PROPERTY_BUFFER_SIZE) {
      FillPropertyBuffer();
    }
    assert(prop_len < 100000);
    auto prop = Slice(property_buffer_ + prop_buffer_ptr_, prop_len);
    prop_read_cnt_ += prop_len;
    prop_buffer_ptr_ += prop_len;
    return std::optional<EdgeRecord>{std::in_place, CurSrcVertex(), edge_body, prop};  // OPTME:
  }

  FileId_t GetFildId() const {
    return fid_;
  }

  bool Is_input_0() const {
    return is_input_0_;
  }

 private:
  auto CurSrcVertex() const -> VertexId_t {
    return index_buffer_[index_ptr_].key;
  }

  auto EdgeBodyOffset() const -> EdgeOffset_t {
    return edge_start_offset_ + (total_edge_cnt_ - remain_edge_cnt_) * sizeof(EdgeBody_t);
  }

  auto CurEdgeBody() const -> EdgeBody_t {
    assert(body_ptr_ < BODY_BUFFER_SIZE);
    return body_buffer_[body_ptr_];
  }

  void ReadIndex(size_t index_cnt) {
    index_ptr_ = 0;
    lseek(e_file_fd, index_start_offset_, SEEK_SET);
    uint read_bytes = read(e_file_fd, index_buffer_, index_cnt * sizeof(Index));
    assert(read_bytes == index_cnt * sizeof(Index));
  }

  void FillBodyBuffer() {
    auto body_cnt = std::min(BODY_BUFFER_SIZE, remain_edge_cnt_);
    lseek(e_file_fd, (total_edge_cnt_ - remain_edge_cnt_) * sizeof(EdgeBody_t) + edge_start_offset_, SEEK_SET);
    uint read_bytes = read(e_file_fd, (char *)body_buffer_, body_cnt * sizeof(EdgeBody_t));

    assert(read_bytes == sizeof(EdgeBody_t) * body_cnt);
    body_ptr_ = 0;
  }

  void FillPropertyBuffer() {
    lseek(p_file_fd, prop_read_cnt_ + property_start_offset_, SEEK_SET);
    auto read_bytes = read(p_file_fd, (char *)property_buffer_, PROPERTY_BUFFER_SIZE);
    assert(read_bytes != 0);
    prop_buffer_ptr_ = 0;
  }

 private:
  int e_file_fd = -1;
  int p_file_fd = -1;

  Index      *index_buffer_ = nullptr;
  size_t      index_ptr_;
  EdgeBody_t *body_buffer_ = nullptr;
  size_t      body_ptr_;

  char  *property_buffer_ = nullptr;
  size_t prop_buffer_ptr_ = 0;

  const size_t total_edge_cnt_;
  const size_t total_index_cnt_;

  EdgeOffset_t       edge_start_offset_;
  EdgeOffset_t       property_start_offset_;
  const EdgeOffset_t index_start_offset_;

  size_t remain_edge_cnt_;
  size_t prop_read_cnt_ = 0;

  std::string    path_;
  FileId_t       fid_;
  BufferManager &buffer_manager_;

  bool is_input_0_  = false;
  bool has_released = false;
};  // Way
}  // namespace lsmg

// for debug
template <>
struct fmt::formatter<lsmg::EdgeRecord> {
  constexpr auto parse(format_parse_context &ctx) {
    return ctx.begin();
  }

  auto format(const lsmg::EdgeRecord &er, format_context &ctx) const {
    return fmt::format_to(ctx.out(), "EdgeRecord(src={},dst={},seq={},mark={},prop={})", er.src_, er.dst_, er.marker_,
                          er.prop_.ToString());
  }
};

template <>
struct fmt::formatter<lsmg::Anchor> {
  constexpr auto parse(format_parse_context &ctx) {
    return ctx.begin();
  }

  auto format(const lsmg::Anchor &anc, format_context &ctx) const {
    return fmt::format_to(ctx.out(), "Anchor(begin={},range={})", anc.begin_, anc.range_size_);
  }
};

template <>
struct fmt::formatter<lsmg::WayAnchor> {
  constexpr auto parse(format_parse_context &ctx) {
    return ctx.begin();
  }

  auto format(const lsmg::WayAnchor &wa, format_context &ctx) const {
    return fmt::format_to(
        ctx.out(), "WayAnchor(begin={},end={},offset={},index_start_offset={},index_num,range={},fid={},is_input_0={})",
        wa.begin_, wa.end, wa.offset, wa.index_start_offset, wa.index_num, wa.range_size, wa.fid, wa.is_input_0);
  }
};

#endif
