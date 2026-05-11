#ifndef LOGGER_H
#define LOGGER_H

#include <fmt/core.h>
#include <spdlog/common.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <cstdint>
#include <memory>

namespace lsmg {

static constexpr const char *ENV_LOG_LEVEL = "LSMGLOG_LEVEL";

enum class LogLevel : uint32_t {
  kTrace = 0,
  kDebug = 1,
  kInfo  = 2,
  kWarn  = 3,
  kError = 4,
  kOff   = 6,
};

inline LogLevel GetEnvLogLevel() {
  const char *env_level = std::getenv(ENV_LOG_LEVEL);
  if (env_level == nullptr) {
    return LogLevel::kInfo;
  }
  return LogLevel::kInfo;
}

class LSMGLogger {
 public:
  static LSMGLogger *GetInstance() noexcept {
    static LSMGLogger logger;
    return &logger;
  }

  ~LSMGLogger() noexcept = default;

  void Init() noexcept {}

  spdlog::logger *GetLogger() {
    return default_logger.get();
  }

 private:
  static constexpr const char *kDefaultLoggerFileName = "lsmg_default.log";

  LSMGLogger(LogLevel level = LogLevel::kInfo)
      : level_(level)
      , default_logger(nullptr) {
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();

    // [2023-11-02 10:00:00] [info] [main.cpp:10] [main] log info
    console_sink->set_pattern("[%Y-%m-%d %H:%M:%S] [%l] [%s:%#] [%!] %v");

    default_logger = std::make_shared<spdlog::logger>("LSMGLogger", spdlog::sinks_init_list{console_sink});

    default_logger->set_level(static_cast<spdlog::level::level_enum>(level_));

    spdlog::set_default_logger(default_logger);
  }

  LogLevel                        level_;
  std::shared_ptr<spdlog::logger> default_logger;
};

#define LOG_TRACE(...) \
  SPDLOG_LOGGER_CALL(lsmg::LSMGLogger::GetInstance()->GetLogger(), spdlog::level::trace, __VA_ARGS__)
#define LOG_DEBUG(...) \
  SPDLOG_LOGGER_CALL(lsmg::LSMGLogger::GetInstance()->GetLogger(), spdlog::level::debug, __VA_ARGS__)
#define LOG_INFO(...)  SPDLOG_LOGGER_CALL(lsmg::LSMGLogger::GetInstance()->GetLogger(), spdlog::level::info, __VA_ARGS__)
#define LOG_WARN(...)  SPDLOG_LOGGER_CALL(lsmg::LSMGLogger::GetInstance()->GetLogger(), spdlog::level::warn, __VA_ARGS__)
#define LOG_ERROR(...) SPDLOG_LOGGER_CALL(lsmg::LSMGLogger::GetInstance()->GetLogger(), spdlog::level::err, __VA_ARGS__)
#define LOG_CRITICAL(...) \
  SPDLOG_LOGGER_CALL(lsmg::LSMGLogger::GetInstance()->GetLogger(), spdlog::level::critical, __VA_ARGS__)

}  // namespace lsmg
#endif