#ifndef LSMG_BENCH_UTILS_HEADER
#define LSMG_BENCH_UTILS_HEADER
#include <fmt/core.h>
#include <fmt/format.h>
#include <string.h>
#include <sys/stat.h>
#include <algorithm>
#include <cassert>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include "common/utils/logger.h"

namespace lsmg::utils {

template <typename VERTEX_T>
class GraphData {
 public:
  GraphData() {}

  GraphData(std::string input_filename, std::string type = "") {
    input_filename_ = input_filename;
    get_type(type);
    double load_time = clock();
    LOG_INFO("Loading graph, type={}, path={}", type, input_filename);

    if (type == "txt") {
      LoadGraphDataTxtFile(input_filename_);
    } else if (type == "binary") {
      LoadGraphDataBinaryFile(input_filename_);
    } else {
      LOG_ERROR("Unsupported dataset file type");
    }

    LOG_INFO("node_num={},edge_num={},ave_degree={}", node_num, edge_num, (edge_num * 1.0 / node_num));
    LOG_INFO("Load dataset time cost {} sec", (clock() - load_time) / CLOCKS_PER_SEC);

    if (type == "txt") {
      WriteToBinaryFile(input_filename_ + ".bin");
    }
  }

  void get_type(std::string &type) {
    if (type == "" && input_filename_.length() >= 2
        && (input_filename_.substr(input_filename_.length() - 4) == ".bin"
            || input_filename_.substr(input_filename_.length() - 2) == ".b")) {
      type = "binary";
    } else {
      type = "txt";
    }
  }

  void load(std::string input_filename = "", std::string type = "") {
    input_filename_ = input_filename;
    get_type(type);
    double load_time = clock();
    LOG_INFO("Loading graph, type={}, path={}", type, input_filename);

    if (type == "txt") {
      LoadGraphDataTxtFile(input_filename_);
    } else if (type == "binary") {
      LoadGraphDataBinaryFile(input_filename_);
    } else {
      LOG_ERROR("Unsupported dataset file type");
    }

    LOG_INFO("node_num={},edge_num={},ave_degree={}", node_num, edge_num, (edge_num * 1.0 / node_num));
    LOG_INFO("Load dataset time cost {} sec", (clock() - load_time) / CLOCKS_PER_SEC);
    if (type == "txt") {
      WriteToBinaryFile(input_filename_ + ".bin");
    }
  }

  void LoadGraphDataTxtFile(const std::string &input_filename = "") {
    // read graphdata from file
    std::ifstream infile(input_filename);

    if (!infile) {
      LOG_ERROR("Failed to open file:{}", input_filename);
      exit(0);
    }

    VERTEX_T    max_vid = 0;
    VERTEX_T    u, v;
    bool        directed = true;
    std::string line;
    while (std::getline(infile, line)) {
      if (line.empty() || line[0] == '%' || line[0] == '#') {
        continue;  // skip lines starting with '%' or '#'
      }
      std::istringstream iss(line);
      iss >> u >> v;
      edges_pairs.emplace_back(std::make_pair(u, v));
      max_vid = max_vid < u ? u : max_vid;
      max_vid = max_vid < v ? v : max_vid;
      if (directed == false) {
        edges_pairs.emplace_back(std::make_pair(v, u));
      }
    }
    infile.close();

    // build graph
    node_num = max_vid + 1;
    edge_num = edges_pairs.size();
  }

  void LoadGraphDataBinaryFile(const std::string &input_filename = "") {
    // read graphdata from file
    size_t        file_size = GetFileSize(input_filename.data());
    std::ifstream file(input_filename, std::ios::binary);

    edge_num = file_size / 2 / sizeof(VERTEX_T);

    if (!file) {
      LOG_ERROR("Failed to open file:{}", input_filename);
      exit(-1);
    }

    const size_t vertex_pair_size = 2 * sizeof(VERTEX_T);
    // 4M
    const size_t buffer_size = (4096 * 1024 / vertex_pair_size) * vertex_pair_size;
    char        *edge_buffer = new char[buffer_size];
    uint64_t     offset      = 0;
    VERTEX_T     max_vid     = 0;
    VERTEX_T     u, v;
    LOG_INFO("dataset memory cost(MB): {}", (sizeof(std::pair<VERTEX_T, VERTEX_T>) * edge_num / 1024.0 / 1024));
    edges_pairs.reserve(edge_num);

    while (offset < file_size) {
      size_t read_size = std::min(buffer_size, file_size - offset);
      file.read(edge_buffer, read_size);

      for (size_t i = 0; i < read_size / vertex_pair_size; i++) {
        u = *(VERTEX_T *)(edge_buffer + i * vertex_pair_size);
        v = *(VERTEX_T *)(edge_buffer + (i * 2 + 1) * sizeof(VERTEX_T));
        edges_pairs.emplace_back(std::make_pair(u, v));
        max_vid = std::max({max_vid, u, v});
      }

      offset += read_size;
    }

    file.close();
    delete[] edge_buffer;

    // build graph
    node_num = max_vid + 1;
    edge_num = edges_pairs.size();
  }

  void LoadGraphDataBinaryFileOnce(std::string input_filename = "") {
    // read graphdata from file
    size_t        file_size = GetFileSize(input_filename.data());
    std::ifstream file(input_filename, std::ios::binary);

    edge_num = file_size / 2 / sizeof(VERTEX_T);

    if (!file) {
      LOG_ERROR("Failed to open file:{}", input_filename);
      exit(-1);
    }
    char *edge_buffer = new char[file_size];
    file.read(edge_buffer, file_size);
    int64_t  offset  = 0;
    VERTEX_T max_vid = 0;
    VERTEX_T u, v;
    LOG_INFO("dataset memory cost(MB): {}", (sizeof(std::pair<VERTEX_T, VERTEX_T>) * edge_num / 1024.0 / 1024));
    edges_pairs.reserve(edge_num);
    for (size_t i = 0; i < edge_num; i++) {
      u = *(VERTEX_T *)(edge_buffer + offset);
      offset += sizeof(VERTEX_T);
      v = *(VERTEX_T *)(edge_buffer + offset);
      offset += sizeof(VERTEX_T);
      edges_pairs.emplace_back(std::make_pair(u, v));
      max_vid = max_vid < u ? u : max_vid;
      max_vid = max_vid < v ? v : max_vid;
    }
    file.close();
    delete[] edge_buffer;

    // build graph
    node_num = max_vid + 1;
    edge_num = edges_pairs.size();
  }

  void WriteToBinaryFileOnce(std::string path) {
    double write_time = clock();

    std::ofstream outfile(path, std::ios::out | std::ios::binary);
    int64_t       length = edges_pairs.size() * sizeof(VERTEX_T) * 2;
    LOG_INFO("length={}", length);

    char   *edge_buffer = new char[length];
    int64_t offset      = 0;
    int64_t edge_num    = 0;
    for (auto &edge : edges_pairs) {
      VERTEX_T u                          = edge.first;
      VERTEX_T v                          = edge.second;
      *(VERTEX_T *)(edge_buffer + offset) = u;
      offset += sizeof(VERTEX_T);
      *(VERTEX_T *)(edge_buffer + offset) = v;
      offset += sizeof(VERTEX_T);
      edge_num++;
    }
    LOG_INFO(" write edge_num={}", edge_num);
    outfile.write(edge_buffer, offset);
    delete[] edge_buffer;

    outfile.close();
    LOG_INFO("write to:{}", path);
    LOG_INFO("WriteToBinaryFile time:{} sec", (clock() - write_time) / CLOCKS_PER_SEC);
  }

  void WriteToBinaryFile(std::string path) {
    double write_time = clock();

    std::ofstream outfile(path, std::ios::out | std::ios::binary);
    uint64_t      length           = edges_pairs.size() * sizeof(VERTEX_T) * 2;
    const size_t  vertex_pair_size = 2 * sizeof(VERTEX_T);
    int64_t       max_buffer_size  = (4096 * 1024 / vertex_pair_size) * vertex_pair_size;
    char         *edge_buffer      = new char[max_buffer_size];
    LOG_INFO(" length={}  write_buffer={}", length, max_buffer_size);

    size_t write_size = 0;
    size_t edge_id    = 0;
    while (edge_id < edges_pairs.size()) {
      int64_t offset = 0;
      while (offset < max_buffer_size && edge_id < edges_pairs.size()) {
        auto    &edge                       = edges_pairs[edge_id++];
        VERTEX_T u                          = edge.first;
        VERTEX_T v                          = edge.second;
        *(VERTEX_T *)(edge_buffer + offset) = u;
        offset += sizeof(VERTEX_T);
        *(VERTEX_T *)(edge_buffer + offset) = v;
        offset += sizeof(VERTEX_T);
        write_size += sizeof(VERTEX_T) * 2;
      }
      outfile.write(edge_buffer, offset);
    }
    if (edge_id != edges_pairs.size() || write_size != length) {
      LOG_INFO(" error: write edge_num={} write byte={}", edge_id, write_size);
      exit(0);
    }

    outfile.close();
    delete[] edge_buffer;
    LOG_INFO("write to:{}", path);
    LOG_INFO("WriteToBinaryFile time:{} sec", (clock() - write_time) / CLOCKS_PER_SEC);
  }

  size_t GetFileSize(const char *fileName) {
    if (fileName == NULL) {
      return 0;
    }
    struct stat statbuf;
    stat(fileName, &statbuf);
    return statbuf.st_size;
  }

  void shuffle_edge() {
    if (input_filename_.size() > 0 && input_filename_.find(".random") != std::string::npos) {
      LOG_WARN("file contains *.random, don't  need to shuffle edge");
      return;
    }
    LOG_INFO("shuffle edge");

    std::mt19937 g(0);
    std::shuffle(edges_pairs.begin(), edges_pairs.end(), g);
  }

  void count_info() {
    LOG_INFO("\nInfo of the graph:");

    VERTEX_T u, v;
    edges_adjlist.resize(node_num);
    std::vector<size_t> degree(node_num + 1, 0);

    for (size_t i = 0; i < edges_pairs.size(); i++) {
      u = edges_pairs[i].first;
      degree[u]++;
    }

    // count info of degree
    size_t all_degree        = 0;
    size_t max_degree        = 0;
    size_t min_degree        = 0xffffffff;
    size_t min_degree_k      = 0;
    size_t max_degree_vertex = 0;
    size_t min_degree_vertex = 0;
    // for (auto p : edges_adjlist) {
    for (size_t i = 0; i < degree.size(); i++) {
      size_t d = degree[i];
      all_degree += d;
      if (min_degree > d) {
        min_degree        = d;
        min_degree_vertex = i;
      }
      if (max_degree < d) {
        max_degree        = d;
        max_degree_vertex = i;
      }
      if (d < 10) {
        min_degree_k += 1;
      }
    }
    assert(all_degree == edges_pairs.size());
    LOG_INFO(R"(Graph info:
              ave_degree={}
              min_degree={}
              max_degree={}
              min_degree_vertex={}
              max_degree_vertex={}
              min_degree_k={})",
             (all_degree / node_num), min_degree, max_degree, min_degree_vertex, max_degree_vertex,
             (min_degree_k * 1.0 / node_num));
  }

  void print_graph() {
    LOG_INFO("node_num={}", node_num);
    LOG_INFO("edges:", edge_num);
    for (auto pair : edges_pairs) {
      LOG_INFO("{} {}", pair.first, pair.second);
    }
  }

 public:
  VERTEX_T                                   node_num;
  uint64_t                                   edge_num;
  std::vector<std::pair<VERTEX_T, VERTEX_T>> edges_pairs;
  std::vector<std::vector<VERTEX_T>>         edges_adjlist;
  std::string                                input_filename_;
};
}  // namespace lsmg::utils

#endif