// E2 adapter for MBFGraph. Original algorithms are in benchmarks/driver.cpp.
#include <sys/prctl.h>
#include <sys/resource.h>
#include "../core/sg_driver.hpp"
#include "../format/type_2.hpp"
#include "../algorithms/e2/e2.hpp"
int main(int argc, const char** argv) {
  prctl(PR_SET_DUMPABLE, 0);
  rlimit limit{0, 0}; setrlimit(RLIMIT_CORE, &limit);
  setup_options(argc, argv);
  init_graph_desc(vm["graph"].as<std::string>());
  using F = format::type2::format_utils;
  std::string algo = vm["benchmark"].as<std::string>();
  if (algo == "e2_index") algorithm::scatter_gather<e2::algorithm<0>, F>()();
  else if (algo == "e2_bfs") algorithm::scatter_gather<e2::algorithm<1>, F>()();
  else if (algo == "e2_pr") algorithm::scatter_gather<e2::algorithm<2>, F>()();
  else if (algo == "e2_cc") algorithm::scatter_gather<e2::algorithm<3>, F>()();
  else return 2;
}
