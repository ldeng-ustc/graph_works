#include <cstdint>
#include <fstream>
#include <ios>
#include <iostream>
struct edge64 {
  uint64_t src;
  uint64_t dst;
};

int main() {
  std::string path_prefix              = "dataset/";
  std::string dataset_name             = "wikipedia";
  std::string base_path                = path_prefix + dataset_name + '/' + dataset_name;
  std::string input_file_path          = base_path + ".txt";
  std::string output_file_path         = base_path + ".bin";
  std::string output_shuffle_file_path = base_path + "_rand.bin";
  {
    std::ifstream input(output_file_path, std::ios::binary);
    if (!input.is_open()) {
      std::cout << "Failed to open file.\n";
      std::terminate();
    }
    edge64   e;
    uint32_t cnt(0);
    while (cnt < 20 && input.read((char *)(&e), sizeof(e))) {
      std::cout << e.src << ' ' << e.dst << '\n';
      ++cnt;
    }
  }
  {
    std::ifstream input(output_shuffle_file_path, std::ios::binary);
    if (!input.is_open()) {
      std::cout << "Failed to open file.\n";
      std::terminate();
    }
    edge64   e;
    uint32_t cnt(0);
    while (cnt < 20 && input.read((char *)(&e), sizeof(e))) {
      std::cout << e.src << ' ' << e.dst << '\n';
      ++cnt;
    }
  }
  return 0;
}