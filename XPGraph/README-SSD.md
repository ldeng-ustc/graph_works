# XPGraph-S SSD reproduction for E2

This is our reproduction of the SSD variant described in **Scalable and High-Performance
Large-Scale Dynamic Graph Storage and Processing System**, ACM TOS 21(2), Article 18,
2025, DOI [10.1145/3715332](https://doi.org/10.1145/3715332).
It extends the existing XPGraph source in `graph_works` (HPCA snapshot
`33ef9153cae46ad647a0ee8770646ef26329fbca`); it is not an author-released SSD implementation.
The original PMEM executable and block format remain separate from the SSD build.

The [author PDF](https://shuibing9420.github.io/assets/pdf/XPGraph-TOS25.pdf), §4.3
(PDF page 16), §5.7 / Figure 24 (page 26), and §5.8 / Figure 26 (page 28), were
checked against the user's Zotero attachment (item `U4CZF7SN`, PDF SHA256
`a71d34790bcd4b02f4e4070295586805158a3849e36d0c12ff7021aa1442ff8d`).

| Paper mechanism | Original source | This reproduction |
| --- | --- | --- |
| SSD-mapped circular log and persistent adjacency; vertex buffers in DRAM | `nvm-common.hpp`, `levelgraph.hpp`, `mem_bulk.hpp` | Explicit `--ssd 1` mapping through ordinary `MAP_SHARED` files, `msync(MS_SYNC)` plus `fdatasync`; DRAM metadata and vertex-buffer pool |
| Adjust buffer hierarchy and high-degree limit to storage granularity | `graph.hpp`, `blocks.hpp`, `args_config.hpp` | Degree-dependent power-of-two buffers; configurable granularity, maximum buffer, and total pool; large persistent allocations rounded to the configured granularity |
| Merge a queried vertex at low concurrency; bypass at high concurrency | `graph.hpp::compact_vbuf_pblks`, `levelgraph.hpp::query_nebrs` | Count overlapping neighborhood reads; merge fragmented native lists when the count is below the threshold; preserve native lists otherwise |

The paper does not specify a full buffer-sizing formula or SSD implementation. The
following sizes, actual-overlap counter, query-time trigger, striped reader locks,
and durability boundaries are **reproduction assumptions**. In particular, 4 KiB and
16 KiB are not claimed to be the author's parameters.

## Build and run

Requirements: C++17, OpenMP, libnuma, libpmem / libpmemobj, zlib. No Optane device is
needed for the SSD target. Build and includes use the local source tree; no library
or header is copied into a system directory.

```bash
make -C related_works/XPGraph -j4 oneway
# Standalone interface: input.bin new-output-dir threads vertex-count expected-edge-count
# Example only; give every run a new dedicated NVMe directory.
timeout 120s related_works/XPGraph/bin/xpgraph_s_oneway \
    input.bin output/tmp/xpgraph-s-example-unique 16 1024 5000
```

The executable has an RPATH to its own `src/api/lib/libxpgraph_s.so`. `make all`
still builds `bin/main` and the original `libxpgraph.so`. The SSD target compiles
with `XPGRAPH_S`, widening block counts to 32 bits for large merged adjacency lists
while retaining 32-bit vertex IDs. The original PMEM build keeps 16-bit block counts.
These two libraries and disk formats must not be mixed. PMEM was build checked;
PMEM hardware execution and recovery were not validated by this task.

`O_EXCL` file creation rejects an existing graph rather than overwriting it. The
program retains graph files when exiting, and suppresses core dumps. The E2 wrapper
owns run-directory cleanup and supplies an empty dedicated directory.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `XPGRAPH_S_VBUF_MB` | 1024 | DRAM vertex-buffer pool in MiB; excludes metadata and page cache |
| `XPGRAPH_S_MAX_VBUF` | `4 * SSD_GRANULARITY`, normally 16384 | Largest per-vertex buffer in bytes; a power of two, at most 4 MiB |
| `XPGRAPH_S_SSD_GRANULARITY` | 4096 | Allocation rounding for persistent lists at least this large |
| `XPGRAPH_S_MERGE_THRESHOLD` | 8 | Merge only when overlapping native neighborhood reads are less than this value |
| `XPGRAPH_S_ELOG_SHIFT` | 27 | Circular log capacity is `2^shift` edges (default 1 GiB) |
| `XPGRAPH_S_BATCH_SHIFT` | 16 | Background archive trigger is `2^shift` edges |
| `XPGRAPH_S_PBLK_MB` | From input size, see below | Sparse persistent-address-space reservation in MiB |
| `SG_BENCH_BFS_ROOTS` | 1 | Consecutive roots starting with the original first edge's source |
| `SG_BENCH_PR_ITERS` | 1 | Public E2 push PageRank iterations |
| `SG_BENCH_CC_RUNS` | 1 | Complete weak-CC invocations |

The minimum DRAM vertex buffer is 16 B. Buffers grow by degree through powers of
two up to the limit. Persistent lists below the configured granularity retain
small power-of-two allocations, avoiding a full page per small vertex. Persistent
lists above it are rounded to the granularity. SSD allocation bulks are 4 MiB;
a large merged list gets its own sufficiently large bulk. The sparse file capacity
is `max(512, floor((48*E + 64*V)/2^20) + 16*threads + 256)` MiB unless overridden.
`ftruncate` reserves address space, not all disk blocks; report `du` separately.
Pool exhaustion fails the run, never reports successful ingestion.

There is one input producer and an OpenMP archival team of the requested size;
the archive coordinator participates in that team. Thus E2's 16 archive threads
can run alongside one producer. Analytics uses 16 computation threads while the
coordinator is idle. SSD mode uses one storage mapping domain and does not bind to
hard-coded two-socket CPU numbers.

## Native storage, concurrency and scope

The native circular log, range classification / workload division, DRAM hierarchy,
linked persistent adjacency blocks, and vertex metadata remain the data path.
`--oneway 1` creates only the out-graph and does not materialize reverse edges or
an in-graph vertex index. The small existing in-range bookkeeping arrays remain
unused. No whole-graph CSR or alternate analytics graph is constructed.

The active-read counter measures native adjacency-read calls that have acquired
their reader lock, including their merge and copy time. It does **not** measure
outstanding NVMe commands or assume a 16-thread OpenMP region produces 16 I/Os.
Page-cache hits are included. A fragmented vertex is merged only when this observed
count is below 8; otherwise its original linked lists are read. The adapter then
visits the thread-private scratch buffer required by the original API. Calls for
the same vertex are serialized using 4096 striped locks. There are no concurrent
updates and queries in E2.

SSD merges use the original `compact_vbuf_pblks` allocation/copy path. The new merged
list replaces the first/last pointers; cleared old blocks are detached instead of
being linked back and merged again on every read. The original allocator does not
reclaim these old blocks. Their allocation and disk footprint remain included.
Incremental updates can append native blocks again and be merged on a later query.

The log publishes an edge after its contents have been written. The background
worker uses a condition predicate and a joined shutdown; `wait_quiescent()` handles
sub-batch tails, waits for the worker to finish, flushes all vertex buffers, and
synchronizes the log plus allocated adjacency ranges. It also synchronizes before
releasing circular-log space after a full flush. Repeated insert–drain–query cycles
are supported. This provides the measured phase-boundary file synchronization;
it does not claim per-edge crash consistency or recovery of the DRAM vertex index.
Deletion, persistence recovery, and update/query overlap are outside this E2 adapter.

## Workload, timing and resource accounting

`apps/xpgraph_s_oneway.cpp` calls the shared algorithms in
`../oneway_basic_benchmarks/oneway_graph_benchmarks.hpp`: directed BFS,
float push PageRank with 0.85 damping and dangling redistribution, and weak CC.
Edges keep their order and multiplicity. Roots use the first original input source
and then increment modulo the configured vertex domain. Isolated vertices remain.

- `Load`: read-only input mapping, endpoint validation, degree metadata, original
  first-source selection. The input is not rewritten or sorted.
- `Init`: empty native graph, log / persistent mapping, metadata and pool setup.
- `Ingest`: every `add_edge` plus the overlapped background work, final partial-batch
  drain, all DRAM-buffer flushing, `msync` and `fdatasync`. It excludes Load and Init.
  The log also reports `accept_and_background` and `drain_flush_sync` subphases.
- `BFS`, `PR`, `CC`: the respective public algorithm plus query-triggered native
  merging and synchronization of changed files at that algorithm's end. No free
  full-graph compaction is performed before analytics.
- `RSS_Ingest`: `/proc/self/statm` RSS in bytes, sampled after synchronized ingestion
  and before input unmapping. It includes resident input and SSD file pages as well
  as anonymous metadata / buffers. Full-process peak RSS is independently supplied
  by `/usr/bin/time -v`.
- `[result]`: `edges`, `bfs_checksum`, `pr_sum`, `cc_components`. Whole-run wall time
  is also logged; the external wrapper includes teardown in its total elapsed time.

Graph files reside in the caller's NVMe directory. Ordinary mmap uses the OS page
cache. No cache drop or unreported warmup is performed between ingestion, BFS, PR,
and CC. These analytics are not claimed to be cold SSD reads or O_DIRECT. The
synchronous end-of-ingestion policy must be retained when comparing timings with
systems that only buffer or asynchronously write at that boundary.

## Verification and concurrency probe

```bash
# All checks write into a new directory and every executable is wrapped in timeout.
make -C related_works/XPGraph -j4 oneway ssd-allocator-test
timeout 120s related_works/XPGraph/bin/ssd_allocator_test \
    output/tmp/xpgraph-s-pressure-UNIQUE
python3 related_works/XPGraph/tests/test_ssd_oneway.py \
    --output output/tmp/xpgraph-s-validation-UNIQUE

make -C related_works/XPGraph -j4 ssd-probe
timeout 30s related_works/XPGraph/bin/ssd_mmap_probe \
    output/tmp/xpgraph-s-probe-UNIQUE > output/tmp/xpgraph-s-probe-UNIQUE.csv
```

The self-test checks adjacency **multisets**, per-vertex BFS levels for three roots,
three-iteration PR scores, weak-CC labels, and the shared algorithms' aggregate
results. It covers isolated and dangling vertices, sparse active IDs, self-loops,
duplicate edges, opposite-numbered edges, more than 65535 neighbors, many log wraps,
and two insert/query stages with a short final batch. Sixteen concurrent native
reads of separate large vertices exercise both merge decisions at threshold 8.
Counters verify buffer growth. A second configuration tests 1 MiB maximum buffers
and 16 KiB allocation granularity, exceeding the original 16-bit block capacity.
`pread` compares synchronized file bytes against every active native block and
log bytes. The production five-argument driver also processes tiny and empty graphs.
Per-vertex results and all logs are retained under the new test directory.
The allocator test performs 20 rounds of concurrent pool exhaustion and a deterministic
full-block fallback using a two-buffer pool. Pool allocation now checks the returned
pointer under the existing allocator mutex, and a flush that exactly fills a block
uses the native single-edge archiver to obtain the next block for the following edge.

Initial validation evidence is under ScatterGraph
`output/tmp/xpgraph-s-dev-20260930/` and `output/tmp/xpgraph-s-selftest-20260930-c/`.
After the allocator boundary fixes, the full suite passed again in
`output/tmp/xpgraph-s-validation-20260930-c/`; the pool-pressure log is
`output/tmp/xpgraph-s-dev-20260930/pressure-a.txt`.
A syscall trace (`sync-trace-c.txt`) records successful `msync(MS_SYNC)` and
`fdatasync` calls; a separate untraced run observed `peak=16`, `low=484`, `high=9`,
18 merged vertices and 12 buffer upgrades. These counts can vary with scheduling.
The LiveJournal wrapper check also matched the existing E2 reference exactly:
BFS checksum 4,400,347, CC 1,876, PR sum 1.0000000526. Its raw record is
`output/tmp/e2-added-baselines-20260930-021956/xpgraph-s-livejournal-01.txt`.

The probe initializes and syncs a new 128 MiB file and runs two repetitions at
1, 2, 4, 8 and 16 threads. Before each pass it uses `MADV_DONTNEED` followed by
`POSIX_FADV_DONTNEED`; `mincore` records whether pages really are nonresident.
Sequential accesses use `MADV_SEQUENTIAL`, random accesses use `MADV_RANDOM`;
major-fault counts and checksums are reported. This measures this mmap/fault path
including readahead, not a hardware-only bandwidth limit or a graph merge cost.

On 2026-09-30, all 20 passes had zero resident pages before the pass; each random
pass produced 32,768 major faults. Random throughput at 1/2/4/8/16 threads was
approximately 37/73/143/270/486 MiB/s. It kept increasing through 16, so this
measurement did **not** identify an optimal merge crossover. The E2 threshold
remains **8 as a paper reference / reproduction assumption**, not a calibrated
optimum. CSV: `output/tmp/xpgraph-s-dev-20260930/mmap-probe.csv`.
