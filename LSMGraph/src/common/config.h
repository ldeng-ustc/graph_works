#ifndef CONFIG_H
#define CONFIG_H

#include <array>
#include <cstdint>
#include <limits>
#include <string>
#include "common/utils/lock.h"

namespace lsmg {

#define DEL_EDGE_SEPARATE

using VertexId_t       = uint64_t;             // vertex id
using VertexOffset_t   = uint32_t;             // vertex address offset_
using VertexProperty_t = std::array<char, 8>;  // vertex property
using Marker_t         = bool;                 // marker bit
using SequenceNumber_t = uint64_t;             // timestamp

using EdgeOffset_t         = uint32_t;     // edge address offset_
using EdgeProperty_t       = std::string;  // edge property
using EdgePropertyOffset_t = uint32_t;     // edge property address offset_

using FileId_t = uint32_t;
using Level_t  = int8_t;

using frame_id_t    = int32_t;  // frame id type
using page_id_t     = int32_t;  // page id type
using txn_id_t      = int64_t;  // transaction id type
using lsn_t         = int32_t;  // log sequence number type
using slot_offset_t = size_t;   // slot offset type
using oid_t         = uint16_t;

using RWLock_t = CASRWLock;

static constexpr int      INVALID_PAGE_ID  = -1;                                    // invalid page id
static constexpr int      INVALID_TXN_ID   = -1;                                    // invalid transaction id
static constexpr int      INVALID_LSN      = -1;                                    // invalid log sequence number
static constexpr uint64_t INVALID_OFFSET   = 0xFFFFFFFF;                            // invalid offset
static constexpr int      HEADER_PAGE_ID   = 0;                                     // the header page id
static constexpr int      PAGE_SIZE        = 4096;                                  // size of a data page in byte
static constexpr int      BUFFER_POOL_SIZE = 10;                                    // size of buffer pool
static constexpr int      LOG_BUFFER_SIZE  = ((BUFFER_POOL_SIZE + 1) * PAGE_SIZE);  // size of a log buffer in byte
static constexpr int      BUCKET_SIZE      = 50;                                    // size of extendible hash bucket
static constexpr int      LRUK_REPLACER_K  = 10;  // lookback window for lru-k replacer

static constexpr uintptr_t NULLPOINTER = 0;

static constexpr uint64_t MAX_LEVEL         = 5;
static constexpr uint64_t LEVEL_INDEX_SIZE  = (MAX_LEVEL - 1);
static constexpr uint64_t MMAP_INITIAL_SIZE = (1ul << 20);
static constexpr uint64_t MMAP_CHUNK_SIZE   = (1ul << 29);

enum Status : unsigned char { kOk = 0, kDelete = 1, kNotFound = 2 };

constexpr VertexId_t       INVALID_VERTEX_ID = ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX;
constexpr FileId_t         INVALID_File_ID   = std::numeric_limits<FileId_t>::max() >> 1;
constexpr SequenceNumber_t MAX_SEQ_ID        = ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX;
constexpr uint32_t         INVALID_LEVEL     = UINT32_MAX;

#ifdef USE_BLOOMFILTER
constexpr uint64_t BLOOM_FILTER_SIZE = (26150110 / 8);
#else
constexpr uint64_t BLOOM_FILTER_SIZE = 1024;
#endif

constexpr uint64_t   MAX_TABLE_SIZE             = (64 * (1 << 20) / 1);
const size_t         PROPERTY_BUFFER_SIZE       = (4 * (1 << 20));
const size_t         MAX_EFILE_SiZE             = 64 * (1 << 20) / 1;
const int            LEVEL_FILE_RATIO           = 10;
static constexpr int BINARY_THRESHOLD_FIND_EDGE = 20;

};  // namespace lsmg
#endif