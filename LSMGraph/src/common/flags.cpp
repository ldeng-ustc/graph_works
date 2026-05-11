#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

DEFINE_bool(OPEN_SSTDATA_CACHE, true, "load sstable to mmap file");
DEFINE_bool(LOAD_OLD_DATA, false, "load old sstdata before running");
DEFINE_bool(open_rw_concurrent, false, "open read and write concurrent");
DEFINE_bool(directed, true, "if directed graph is ture, else false");
DEFINE_bool(shuffle_edge, true, "if shuffle_edge");
DEFINE_double(fresh_rate, 1, "the freshness of the queried dataset");

DEFINE_string(get_edges_type, "iterator", "vector/iterator: return type of test_edges()");
DEFINE_string(dataset_name, "google", "dataset name");
DEFINE_string(dataset_path, "/data/path", "path of data");
DEFINE_string(db_path, "/data/path", "path of database store data");
DEFINE_string(alg_app, "bfs", "graph algorithm name");
DEFINE_string(mmap_path, "/data/lsmgraph_mmap/block.mmap", "mmap file path");

DEFINE_uint32(max_subcompactions, 1, "max_subcompactions thread num");
DEFINE_uint32(multi_level_merge_degree, 5, "multi level merege small degree vertex's degree");
DEFINE_uint32(thread_num, 1, "app thread num");
DEFINE_uint32(memtable_num, 2, "memtable num");
DEFINE_uint32(khop, 1, "khop num");
DEFINE_uint32(khop_num, 1000, "khop query num");
DEFINE_uint32(test_times, 1, "khop num");
DEFINE_uint64(read_p, 10, "read edges of 1/p all edges");
DEFINE_uint32(del_rate, 0, "delete edges of 1/p all edges");
DEFINE_uint32(reserve_node, 0, "reserve node num of skiplist");
DEFINE_uint64(source, 0, "source node of the app");

DEFINE_uint32(large_vertex, 50, "large degree vertex threshold");

DEFINE_bool(support_mulversion, true, "support multiple versions");