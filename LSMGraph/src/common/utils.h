#ifndef LSMG_UTILS_HEADER
#define LSMG_UTILS_HEADER

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/flags.h"
#include "common/utils/logger.h"

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#include <stdio.h>
#include <windows.h>
#endif
#if defined(__linux__) || defined(__MINGW32__) || defined(__APPLE__)
#include <dirent.h>
#include <unistd.h>
#include <cstring>
#include <filesystem>
#endif

namespace lsmg {
namespace utils {

inline uint32_t getLevelMaxSize(int level) {
  int s = 2;
  for (int i = 0; i < level; i++) {
    s *= LEVEL_FILE_RATIO;
  }
  return s;
}

inline std::string eFileName(uint64_t currentTime) {
  return fmt::format("{}/{}.sst", FLAGS_db_path, std::to_string(currentTime));
}

inline std::string pFileName(uint64_t currentTime) {
  return fmt::format("{}/{}.sst_p", FLAGS_db_path, std::to_string(currentTime));
}

static inline void use_madvise(void *addr, int length, bool use_byte = false, int advice = MADV_NORMAL) {
  if (use_byte == true) {
    madvise(addr, length, MADV_WILLNEED);
  } else {
    madvise(addr, (length / 4096) * 4096, MADV_WILLNEED);
  }
}

static inline bool dirExists(std::string path) {
  struct stat st;
  int         ret = stat(path.c_str(), &st);
  return ret == 0 && st.st_mode & S_IFDIR;
}

#if defined(_WIN32) && !defined(__MINGW32__)
static inline int scanDir(std::string path, std::vector<std::string> &ret) {
  std::string extendPath;
  if (path[path.size() - 1] == '/') {
    extendPath = path + "*";
  } else {
    extendPath = path + "/*";
  }
  WIN32_FIND_DATAA fd;
  HANDLE           h = FindFirstFileA(extendPath.c_str(), &fd);
  if (h == INVALID_HANDLE_VALUE) {
    return 0;
  }
  while (true) {
    std::string ss(fd.cFileName);
    if (ss[0] != '.') {
      ret.push_back(ss);
    }
    if (FindNextFile(h, &fd) == false) {
      break;
    }
  }
  FindClose(h);
  return ret.size();
}
#endif
#if defined(__linux__) || defined(__MINGW32__) || defined(__APPLE__)
static inline int scanDir(std::string path, std::vector<std::string> &ret) {
  DIR           *dir;
  struct dirent *rent;
  dir = opendir(path.c_str());
  char s[100];
  while ((rent = readdir(dir))) {
    strcpy(s, rent->d_name);
    if (s[0] != '.') {
      ret.push_back(s);
    }
  }
  closedir(dir);
  return ret.size();
}
#endif

static inline int _mkdir(const char *path) {
#ifdef _WIN32
  return ::_mkdir(path);
#else
  return ::mkdir(path, 0775);
#endif
}

static inline int mkdir(const std::string &path) {
  std::string       currentPath = "";
  std::string       dirName;
  std::stringstream ss(path);

  while (std::getline(ss, dirName, '/')) {
    if (dirName.size() == 0) {  // root path
      currentPath += "/";
      continue;
    }
    currentPath += dirName;
    if (!dirExists(currentPath) && _mkdir(currentPath.c_str()) != 0) {
      return -1;
    }
    currentPath += "/";
  }
  return 0;
}

static inline int rmdir(const char *path) {
#ifdef _WIN32
  return ::_rmdir(path);
#else
  return std::filesystem::remove_all(path);  // delete non-empty directory
#endif
}

static inline int rmfile(const char *path) {
#ifdef _WIN32
  return ::_unlink(path);
#else
  return ::unlink(path);
#endif
}

static inline uint64_t EncodeIndex(uint16_t levelID, uint32_t fileID, uint32_t offset) {
  uint64_t encoded = 0;
  encoded |= static_cast<uint64_t>(levelID & 0xF) << 60;
  encoded |= static_cast<uint64_t>(fileID & 0x0FFFFFFF) << 32;
  encoded |= static_cast<uint64_t>(offset & 0xFFFFFFFF);
  return encoded;
}

static inline void DecodeIndex(uint64_t encoded, uint16_t &levelID, uint32_t &fileID, uint32_t &offset) {
  levelID = static_cast<uint16_t>((encoded >> 60) & 0xF);
  fileID  = static_cast<uint32_t>((encoded >> 32) & 0x0FFFFFFF);
  offset  = static_cast<uint32_t>(encoded & 0xFFFFFFFF);
}

class SmapsParser {
 public:
  static std::unordered_map<std::string, unsigned long> getMemorySizes(const std::string &pid) {
    std::string                                    smapsPath = "/proc/" + pid + "/smaps";
    std::unordered_map<std::string, unsigned long> memorySizes;

    std::ifstream smapsFile(smapsPath);
    std::string   line;
    std::string   currentCategory;

    if (smapsFile.is_open()) {
      size_t line_num = 0;
      while (std::getline(smapsFile, line)) {
        line_num++;
        if (line.compare(0, 5, "Size:") == 0) {
          memorySizes["Size"] += getSizeInKb(line);
        } else if (line.compare(0, 4, "Rss:") == 0) {
          memorySizes["Rss"] += getSizeInKb(line);
        } else if (line.compare(0, 13, "Shared_Clean:") == 0) {
          memorySizes["Shared_Clean"] += getSizeInKb(line);
        } else if (line.compare(0, 13, "Shared_Dirty:") == 0) {
          memorySizes["Shared_Dirty"] += getSizeInKb(line);
        } else if (line.compare(0, 14, "Private_Clean:") == 0) {
          memorySizes["Private_Clean"] += getSizeInKb(line);
        } else if (line.compare(0, 14, "Private_Dirty:") == 0) {
          memorySizes["Private_Dirty"] += getSizeInKb(line);
        } else if (line.compare(0, 5, "Swap:") == 0) {
          memorySizes["Swap"] += getSizeInKb(line);
        } else if (line.compare(0, 4, "Pss:") == 0) {
          memorySizes["Pss"] += getSizeInKb(line);
        }
      }
      LOG_INFO("file line num={}", line_num);
      smapsFile.close();
    } else {
      LOG_ERROR("error: Failed to open smaps file.");
    }
    return memorySizes;
  }

 private:
  static unsigned long getSizeInKb(const std::string &line) {
    std::string sizeStr = line.substr(line.find_first_of("0123456789"));
    return std::stoul(sizeStr);
  }
};

// return actual memory used by current process (GB)
static inline double get_memory_usage() {
  std::ifstream statusFile("/proc/self/status");
  std::string   line;

  while (std::getline(statusFile, line)) {
    if (line.compare(0, 6, "VmRSS:") == 0) {
      unsigned long resMemory = std::stoul(line.substr(6));
      double        res_mem   = double(resMemory) / (1024 * 1024);  // GB
#ifndef TEST_MEM_TIME_QPS
      LOG_DEBUG("[RES {} GB ]\n", res_mem);
#endif
      return res_mem;
    }
  }
  LOG_ERROR("not found, RES 0 GB.");
  return 0;
}

// read io info
static inline std::string get_io_info(std::string info = "") {
  static std::map<std::string, long long> previousDataMap;
  std::string                             pid             = std::to_string(getpid());
  std::string                             ioStatsFilePath = "/proc/" + pid + "/io";

  auto                             file = std::ifstream(ioStatsFilePath);
  std::map<std::string, long long> currentDataMap;

  fmt::memory_buffer buf;  // A buffer to efficiently build the string

  if (file.is_open()) {
    std::string line;
    while (std::getline(file, line)) {
      size_t delimiterPos = line.find(':');
      if (delimiterPos != std::string::npos) {
        std::string key         = line.substr(0, delimiterPos);
        std::string valueString = line.substr(delimiterPos + 1);
        long long   value       = std::stoull(valueString);
        currentDataMap[key]     = value;
      }
    }

    file.close();

    // Add a header for clarity, maybe with a timestamp
    fmt::format_to(fmt::appender(buf), "[{:%Y-%m-%d %H:%M:%S}] Metric Report for: {}\n",
                   fmt::localtime(std::time(nullptr)), info);
    fmt::format_to(fmt::appender(buf), "{:-<50}\n", "");  // Separator line

    for (const auto &entry : currentDataMap) {
      const std::string &key = entry.first;

      double currentValueGB = static_cast<double>(entry.second) / 1024.0 / 1024 / 1024;

      fmt::format_to(fmt::appender(buf), "  {:<40}(GB): {:>8.2f}\n", key, currentValueGB);

      if (previousDataMap.count(key) > 0) {
        long long diffBytes = entry.second - previousDataMap.at(key);
        double    diffGB    = static_cast<double>(diffBytes) / 1024.0 / 1024 / 1024;
        // Use different formatting for positive/negative diffs for visual clarity
        if (diffGB >= 0) {
          fmt::format_to(fmt::appender(buf), "    Δ {:<36}(GB): +{:>7.2f}\n", key, diffGB);
        } else {
          fmt::format_to(fmt::appender(buf), "    Δ {:<36}(GB): {:>8.2f}\n", key, diffGB);
        }
      } else {
        // If no previous data, treat current value as the "initial" increase
        fmt::format_to(fmt::appender(buf), "    Δ {:<36}(GB): +{:>7.2f} (initial)\n", key, currentValueGB);
      }
    }
    fmt::format_to(fmt::appender(buf), "{:-<50}\n", "");  // Footer separator
    previousDataMap = currentDataMap;
  } else {
    LOG_ERROR("Failed to open file: {}", ioStatsFilePath);
  }
  return fmt::to_string(buf);
}

inline double GetCurrentTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec / 1000000.0;
}

}  // namespace utils
}  // namespace lsmg

#endif