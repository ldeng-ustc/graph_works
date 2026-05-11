#ifndef EDGE_ITERATOR_BASE_H
#define EDGE_ITERATOR_BASE_H

#include "common/config.h"
namespace lsmg {

class EdgeIteratorBase {
 public:
  virtual ~EdgeIteratorBase(){};
  virtual bool             valid() const      = 0;
  virtual void             next()             = 0;
  virtual VertexId_t       dst_id() const     = 0;
  virtual SequenceNumber_t sequence() const   = 0;
  virtual Marker_t         marker() const     = 0;
  virtual bool             empty() const      = 0;
  virtual size_t           size() const       = 0;
  virtual EdgeProperty_t   edge_data() const  = 0;
  virtual FileId_t         get_fid() const    = 0;
  virtual bool             IsMemTable() const = 0;
};

}  // namespace lsmg

#endif