#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <fstream>
#include <ios>
#include <iostream>
#include <random>
#include <string>
#include <vector>

struct edge64 {
  uint64_t src;
  uint64_t dst;
};

int main() {
  std::string path_prefix              = "/dataset/";
  std::string dataset_name             = "wikipedia";
  std::string base_path                = path_prefix + dataset_name + '/' + dataset_name;
  std::string input_file_path          = base_path + ".txt";
  std::string output_file_path         = base_path + ".bin";
  std::string output_shuffle_file_path = base_path + "_rand.bin";

  std::vector<edge64> edges;
  uint64_t            max_vertex_id = 0;

  {
    std::ifstream input_file(input_file_path, std::ios_base::in);
    if (!input_file.is_open()) {
      std::cout << "Failed to open file.\n";
      std::terminate();
    }
    std::cout << "opened\n";
    uint64_t    src, dst;
    std::string line;
    std::getline(input_file, line);
    std::cout << "line:" << line << std::endl;
    uint cnt(0);
    while (input_file >> src >> dst) {
      uint64_t bigger = std::max(src, dst);
      if (bigger > max_vertex_id) {
        max_vertex_id = bigger;
      }
      edges.push_back({src, dst});
      ++cnt;
      if (!(cnt % (1000 * 1000))) {
        std::cout << cnt << std::endl;
      }
    }
  }
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();

  std::default_random_engine rng(seed);

  {
    std::ofstream output_file(output_file_path, std::ios_base::binary);
    for (const auto e : edges) {
      output_file.write(reinterpret_cast<const char *>(&e), sizeof(e));
    }
  }

  // shuffle
  {
    std::shuffle(edges.begin(), edges.end(), rng);
    std::ofstream output_file(output_shuffle_file_path, std::ios_base::binary);
    for (const auto e : edges) {
      output_file.write(reinterpret_cast<const char *>(&e), sizeof(e));
    }
  }

  return 0;
}