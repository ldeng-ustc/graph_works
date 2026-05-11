#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gflags/gflags.h>
#include <omp.h>
#include <cstdint>
#include <exception>
#include <thread>
#include "bench_utils.h"
#include "bfs.h"
#include "cc.h"
#include "scan.h"
#include "sssp.h"

using vertex_t   = uint64_t;
using time_point = std::chrono::steady_clock::time_point;

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  std::string dataset_name   = FLAGS_dataset_name;
  std::string input_filename = FLAGS_dataset_path;
  std::string db_path        = FLAGS_db_path;

  double load_dataset_before = lsmg::utils::get_memory_usage();

  lsmg::utils::GraphData<vertex_t> graphdata;
  graphdata.load(input_filename);
  LOG_INFO("node_num={}, edge_num={}", graphdata.node_num, graphdata.edge_num);
  if (FLAGS_shuffle_edge == true && dataset_name.find("sf") == std::string::npos && FLAGS_LOAD_OLD_DATA == false) {
    graphdata.shuffle_edge();
  } else {
    LOG_INFO("no shuffle edges.");
  }
  double load_dataset_over = lsmg::utils::get_memory_usage();
  LOG_INFO("Load dataset Memory usage(GB): {:.3}", load_dataset_over - load_dataset_before);
  LOG_INFO("ID: {}, Max threads: {}, Num threads: {}", omp_get_thread_num(), omp_get_max_threads(),
           omp_get_num_threads());
  lsmg::LSMGraph *gStore;

  double open_before = lsmg::utils::get_memory_usage();

  lsmg::LSMGraph::open(db_path, graphdata.node_num, &gStore);  // ssd

  double open_over = lsmg::utils::get_memory_usage();
  LOG_INFO("LSMGraph memory usage(GB): {:.3}", open_before - open_over);
  // insert vertex
  {
    LOG_INFO("Start inserting vertex");
    double mem_before_inesrt_vertex = lsmg::utils::get_memory_usage();
    for (uint i = 0; i < graphdata.node_num; i++) {
      std::string data = std::to_string(i);
      auto        src  = gStore->new_vertex();

      gStore->put_vertex(src, data);
    }
    double mem_after_inesrt_vertex = lsmg::utils::get_memory_usage();
    LOG_INFO("vertex memory usage(GB): {:.3}", mem_after_inesrt_vertex - mem_before_inesrt_vertex);
  }

  // insert edge
  {
    double mem_before_insert_edge = lsmg::utils::get_memory_usage();
    size_t insert_edge_num        = graphdata.edges_pairs.size();

    int write_thread_num = FLAGS_thread_num/2;
    LOG_INFO("write_thread_num: {}", write_thread_num);

    time_point start_insert_ts = std::chrono::steady_clock::now();
#pragma omp parallel for num_threads(write_thread_num)
    for (size_t i = 0; i < insert_edge_num; i++) {
      auto &e = graphdata.edges_pairs[i];

      lsmg::VertexId_t src      = e.first;
      lsmg::VertexId_t dst      = e.second;
      std::string      property = std::to_string((src + dst) % 64);

      gStore->put_edge(src, dst, property);

      if (FLAGS_directed == false) {
        property = std::to_string((src + dst) % 64);
        gStore->put_edge(dst, src, property);
      }

      if (i % 100000000 == 0) {
        LOG_INFO("inserted edges: {}/{}, {:.3}", i, insert_edge_num, i*1.0/insert_edge_num);
      }
    }

    time_point end_insert_ts = std::chrono::steady_clock::now();

    std::chrono::duration<double> time_span =
        std::chrono::duration_cast<std::chrono::duration<double>>(end_insert_ts - start_insert_ts);
    LOG_INFO("Insert edge cost time: {:.2}", time_span.count());
    LOG_INFO("Insert {} edges", insert_edge_num);
    LOG_INFO("Insert edge QPS: {}", insert_edge_num / time_span.count());
    double mem_after_insert_edge = lsmg::utils::get_memory_usage();
    LOG_INFO("Edge Memory usage(GB):{:.3}", mem_after_insert_edge - mem_before_insert_edge);
  }
  LOG_INFO("Waiting for the write task to complete");
  while (gStore->GetCompactionState()) {
    std::this_thread::sleep_for(std::chrono::microseconds(20));
  }
  std::this_thread::sleep_for(std::chrono::seconds(5));

  /******************************** Test APP ********************************/
  std::string              alg_app     = FLAGS_alg_app;
  std::vector<std::string> alg_app_set = {"sssp", "scan", "bfs", "cc"};
  if (alg_app != "all") {
    alg_app_set.clear();
    std::stringstream ss(alg_app);
    char              delimiter = ',';
    std::string       item;
    while (std::getline(ss, item, delimiter)) {
      if (!item.empty()) {
        alg_app_set.push_back(item);
      }
    }
  }

  for (auto &alg_app : alg_app_set) {
    LOG_INFO("run {}.", alg_app);
    time_point t1 = std::chrono::steady_clock::now();

    auto algo_func_ptr = lsmg::AlgoFactory::GetExcutor(alg_app);
    if (algo_func_ptr == nullptr) {
      LOG_ERROR("Unsupport algorithm: {}", alg_app);
      std::terminate();
    }
    (*algo_func_ptr)(*gStore, lsmg::MAX_SEQ_ID);

    time_point t2 = std::chrono::steady_clock::now();

    std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
    LOG_INFO("Run time(sec): {}", time_span.count());
  }

  LOG_INFO("finish.");

  delete gStore;
  google::ShutDownCommandLineFlags();
  return 0;
}