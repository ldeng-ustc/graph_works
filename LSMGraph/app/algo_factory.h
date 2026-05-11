#ifndef LSMG_ALGO_FACTORY_HEADER
#define LSMG_ALGO_FACTORY_HEADER

#include <functional>
#include <map>
#include <string>

#include "lsmgraph.h"

namespace lsmg {
class AlgoFactory {
 public:
  using AlgoExecutor = std::function<int(LSMGraph &, SequenceNumber_t)>;

  AlgoFactory() = delete;

  AlgoFactory(const AlgoFactory &) = delete;
  AlgoFactory(AlgoFactory &&)      = delete;

  AlgoFactory &operator=(const AlgoFactory &) = delete;
  AlgoFactory &operator=(AlgoFactory &&)      = delete;

  static std::unique_ptr<AlgoExecutor> GetExcutor(const std::string &algo_name) {
    if (algo_executors_.find(algo_name) != algo_executors_.end())
      return std::make_unique<AlgoExecutor>(algo_executors_[algo_name]);
    return nullptr;
  }

  static void RegisterAlgo(const std::string &algo_name, AlgoExecutor algo_executor) {
    algo_executors_[algo_name] = algo_executor;
  }

 private:
  inline static std::map<std::string, AlgoExecutor> algo_executors_;
};

#define REGISTER_ALGORITHM(ALGO_NAME, FUNC)      \
  namespace {                                    \
  const auto _register_##ALGO_NAME = []() {      \
    AlgoFactory::RegisterAlgo(#ALGO_NAME, FUNC); \
    return 0;                                    \
  }();                                           \
  }

}  // namespace lsmg

#endif